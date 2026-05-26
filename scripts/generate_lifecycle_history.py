"""
generate_lifecycle_history.py — time-partitioned lifecycle backfill from real fact_bookings

────────────────────────────────────────────────────────────────────────
DESIGN — ONE SWEEP, FOUR BUCKETS, ZERO SYNTHESIS
────────────────────────────────────────────────────────────────────────
This script processes every row in `fact_bookings` exactly once and
routes it into one of four buckets, based purely on how its booking
timestamp and stay dates relate to the `--sim-today` anchor:

                   booking_ts < sim-today          booking_ts >= sim-today
                ┌──────────────────────────────┐  ┌─────────────────────┐
checkout <  S   │ COMPLETED                    │  │     (impossible)    │
                │  history: BOOKING + CI + CO  │  │   booking_ts <      │
                │  (or BOOKING + CANCELLATION  │  │   checkin_date is   │
                │   if is_cancelled)           │  │   a guard rule.     │
                ├──────────────────────────────┤  ├─────────────────────┤
checkin < S     │ IN-PROGRESS                  │  │     (impossible)    │
≤ checkout      │  history: BOOKING + CHECKIN  │  │                     │
                │  sim_open: CHECKED_IN        │  │                     │
                ├──────────────────────────────┤  ├─────────────────────┤
checkin >= S    │ BOOKED                       │  │ FUTURE — reserved   │
                │  history: BOOKING            │  │  for the stream.    │
                │  sim_open: BOOKED            │  │  No event written.  │
                └──────────────────────────────┘  └─────────────────────┘

The crucial property: **the open backlog is real**. Every row in
`sim_open_bookings` is a booking that actually exists in `fact_bookings`
— the simulator advances real customers staying at real hotels with
real reservations. Nothing is invented.

The FUTURE bucket is exactly the bookings the stream simulator will
emit later. The earlier version of this script synthesised a small
backlog and then immediately stopped — leaving the stream nothing real
to advance into. The new model keeps the future runway intact.

────────────────────────────────────────────────────────────────────────
WHY HISTORY AND STREAM SHARE ONE TABLE
────────────────────────────────────────────────────────────────────────
`fact_booking_events` is a single events ledger. A `source` column
('history' | 'stream') is the only thing that distinguishes a
backfilled event from one the live producer emitted. Analytics queries
do not care which; ops queries can filter on source when needed.

────────────────────────────────────────────────────────────────────────
THE --sim-today ANCHOR
────────────────────────────────────────────────────────────────────────
The default anchor is **2025-06-01**. Real fact_bookings runs to
~2026-05, so the anchor leaves roughly 11–12 months of real bookings
ahead of it — enough runway for the stream simulator to replay the
future without ever running out of work.

────────────────────────────────────────────────────────────────────────
REALISM GUARDS (skip-or-fix, never silently corrupt)
────────────────────────────────────────────────────────────────────────
Every booking is validated before it's exploded:

  - checkout_date > checkin_date (gap >= 1 night) ........ skip on fail
  - 1 <= nights_from_gap <= 60 .......................... skip on fail
  - booking_ts <= checkin_date (booked at-or-before in) . skip on fail
  - 1 <= num_guests <= 10 ............................... skip on fail
  - nights_stayed disagrees with checkout-checkin ........ COUNT only;
    we trust the gap and recompute revenue = nightly_rate * gap.

A row that fails any skip guard is dropped whole (no half-bookings) and
counted by reason in the stats block.

────────────────────────────────────────────────────────────────────────
IDEMPOTENT RERUNS (--reset)
────────────────────────────────────────────────────────────────────────
`--reset` (the default) deletes ONLY this script's output:
  DELETE FROM fact_booking_events WHERE source = 'history';
  TRUNCATE sim_open_bookings;
Anything with source='stream' is NEVER touched, so re-running this
script during development cannot wipe live-emitted events. The RNG seed
is pinned, so a clean rerun reproduces identical counts.

────────────────────────────────────────────────────────────────────────
Usage
────────────────────────────────────────────────────────────────────────
  python -m scripts.generate_lifecycle_history
  python -m scripts.generate_lifecycle_history --sim-today 2025-06-01
  python -m scripts.generate_lifecycle_history --no-reset    # append, do not wipe

Reads DB config from .env (POSTGRES_HOST / PORT / DB / USER / PASSWORD).
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
import time
import uuid
from datetime import date, datetime, time as dtime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Force UTF-8 stdout on Windows so the stats block renders cleanly.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

load_dotenv()

# ──────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# Anchor default — picked so ~11–12 months of real fact_bookings remain
# AFTER it as the stream's future runway (data runs to ~2026-05).
DEFAULT_SIM_TODAY = date(2025, 6, 1)

# Plausible distributions used to fill columns fact_bookings does not
# carry (payment_mode) and to choose a cancellation_reason that real
# data does not record. Probabilities are project conventions, not
# tuned numbers.
PAYMENT_MODES         = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "WALLET"]
PAYMENT_WEIGHTS       = [0.40, 0.25, 0.15, 0.10, 0.10]
CANCEL_REASONS        = ["customer_cancelled", "no_show"]
CANCEL_REASON_WEIGHTS = [0.70, 0.30]

# Bulk-insert chunk for fact_booking_events. 5,000 keeps memory bounded
# and matches the natural psycopg2 page-size sweet spot for execute_values.
INSERT_CHUNK = 5_000

# Pinned RNG seed → deterministic reruns (jitter on event timestamps,
# cancellation reason picks, payment mode picks).
RNG_SEED = 20260601

# Acceptance threshold for the CONTINUITY test: the latest history
# event_date must sit within this many days of sim-today. 7 days is the
# spec value — any larger gap suggests we left a void of unconverted
# data near the anchor.
CONTINUITY_MAX_GAP_DAYS = 7


# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    The script intentionally exposes only two knobs:
      --sim-today : the calendar anchor (default DEFAULT_SIM_TODAY).
      --reset / --no-reset : wipe-then-write vs append (default reset).

    The older `--history-bookings` and `--open-backlog` flags were
    dropped: there is no longer any sampling and no longer any
    synthesised backlog. Every row comes from fact_bookings.

    Returns:
        argparse.Namespace with `sim_today: date` and `reset: bool`.
    """
    p = argparse.ArgumentParser(
        description="Time-partitioned lifecycle backfill from real fact_bookings.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--sim-today",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=DEFAULT_SIM_TODAY,
        help="Calendar anchor; history sits before it, future bookings sit after it.",
    )
    p.add_argument(
        "--reset", dest="reset", action="store_true", default=True,
        help="Delete only this script's prior rows before writing.",
    )
    p.add_argument(
        "--no-reset", dest="reset", action="store_false",
        help="Append; do NOT wipe prior history/open-backlog rows.",
    )
    return p.parse_args()


# ──────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────

def connect():
    """
    Open a Postgres connection using DB_PARAMS read from .env.

    Returns:
        psycopg2.connection. Caller owns close().
    """
    return psycopg2.connect(**DB_PARAMS)


def load_hotel_to_city(conn) -> dict[str, str]:
    """
    Build a hotel_id -> city map by joining hotel_master to dim_location.

    Used to denormalise `city` onto every event row so per-event
    queries (and the Kafka partition-by-city key) do not need a runtime
    join to `dim_location`.

    Args:
        conn: open Postgres connection.

    Returns:
        Dict mapping each hotel_id (str) to its city (str).
    """
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT h.hotel_id, l.city
            FROM   hotel_master h
            JOIN   dim_location l ON h.location_id = l.location_id
        """)
        for hid, city in cur:
            out[hid] = city
    return out


# ──────────────────────────────────────────────────────────────────────
# Reset
# ──────────────────────────────────────────────────────────────────────

def reset_script_rows(conn) -> None:
    """
    Delete ONLY this script's output, never anyone else's.

    Removes every fact_booking_events row with source='history' and
    truncates sim_open_bookings. Stream-side rows (source='stream') are
    deliberately left alone — that's live producer data and must never
    be wiped by a generator rerun.

    Args:
        conn: open Postgres connection (caller commits).
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM fact_booking_events WHERE source = 'history'")
        deleted_events = cur.rowcount
        cur.execute("TRUNCATE sim_open_bookings")
    conn.commit()
    print(f"[reset] deleted {deleted_events:,} history events; truncated sim_open_bookings")


# ──────────────────────────────────────────────────────────────────────
# Realism guard
# ──────────────────────────────────────────────────────────────────────

def validate_booking(booking_ts, checkin_date, checkout_date, nights_stored, num_guests) -> tuple[int | None, str | None, bool]:
    """
    Validate a single fact_bookings row and decide whether to keep it.

    Implements the realism guard from the spec:
      - checkout > checkin (gap >= 1 night)
      - 1 <= nights_from_gap <= 60
      - booking_ts <= checkin_date (booked at-or-before check-in)
      - 1 <= num_guests <= 10
      - if `nights_stored != gap`, count the mismatch but trust the gap

    Args:
        booking_ts: datetime when the booking was made.
        checkin_date: planned check-in date.
        checkout_date: planned check-out date.
        nights_stored: the nights_stayed value persisted in fact_bookings.
        num_guests: planned guest count.

    Returns:
        Tuple of (nights_to_use, skip_reason, nights_mismatched):
          - nights_to_use: the gap in days (None if the row failed a guard).
          - skip_reason: one of {'gap_lt_1', 'nights_out_of_range',
            'booked_after_checkin', 'num_guests_out_of_range'} when the
            row is dropped, else None.
          - nights_mismatched: True if stored nights disagreed with gap
            but the row was otherwise valid (drives the mismatch stat).
    """
    gap_days = (checkout_date - checkin_date).days
    if gap_days < 1:
        return None, "gap_lt_1", False
    if gap_days > 60:
        return None, "nights_out_of_range", False
    if booking_ts.date() > checkin_date:
        return None, "booked_after_checkin", False
    if num_guests is None or num_guests < 1 or num_guests > 10:
        return None, "num_guests_out_of_range", False
    mismatched = nights_stored is not None and int(nights_stored) != gap_days
    return gap_days, None, mismatched


# ──────────────────────────────────────────────────────────────────────
# Event row builder
# ──────────────────────────────────────────────────────────────────────

_EVENT_COLUMNS = (
    "event_id", "event_type", "booking_id", "customer_id", "hotel_id", "city",
    "room_type_id", "event_ts", "event_date",
    "checkin_date", "checkout_date", "nights", "num_guests",
    "nightly_rate_inr", "revenue_inr", "booking_source", "payment_mode",
    "cancellation_reason", "source",
)

_INSERT_EVENTS_SQL = f"""
    INSERT INTO fact_booking_events ({", ".join(_EVENT_COLUMNS)})
    VALUES %s
"""


def build_event_row(
    *, event_type: str, booking_id, customer_id, hotel_id, city,
    room_type_id, event_dt: datetime,
    checkin_date, checkout_date, nights, num_guests,
    nightly_rate_inr, revenue_inr, booking_source, payment_mode,
    cancellation_reason,
) -> tuple:
    """
    Assemble a tuple in _EVENT_COLUMNS order for execute_values insert.

    All keyword-only so callers self-document at the call site. The
    `revenue_inr` invariant (`== nightly_rate * nights`) is the caller's
    responsibility — by the time we build the tuple the math is fixed.

    Args:
        Every column on fact_booking_events that the script populates
        (the REVIEW / PRICE_CHANGE reserved columns are omitted and
        default to NULL on the INSERT).

    Returns:
        A 19-item tuple ready for execute_values against _INSERT_EVENTS_SQL.
    """
    return (
        str(uuid.uuid4()), event_type,
        str(booking_id) if booking_id is not None else None,
        customer_id, hotel_id, city,
        str(room_type_id) if room_type_id is not None else None,
        event_dt, event_dt.date(),
        checkin_date, checkout_date,
        int(nights) if nights is not None else None,
        int(num_guests) if num_guests is not None else None,
        float(nightly_rate_inr) if nightly_rate_inr is not None else None,
        float(revenue_inr) if revenue_inr is not None else None,
        booking_source, payment_mode, cancellation_reason,
        "history",
    )


def flush_events(conn, batch: list[tuple]) -> None:
    """
    Bulk-insert a buffered batch of event rows into fact_booking_events.

    Uses psycopg2.extras.execute_values with page_size=1000 to keep each
    network round-trip bounded. Commits are handled by the outer driver
    (one commit per full sweep) — calling flush_events does not commit.

    Args:
        conn: open Postgres connection.
        batch: list of tuples shaped per _EVENT_COLUMNS.
    """
    if not batch:
        return
    with conn.cursor() as cur:
        execute_values(cur, _INSERT_EVENTS_SQL, batch, page_size=1000)


# ──────────────────────────────────────────────────────────────────────
# Routing engine — the core sweep
# ──────────────────────────────────────────────────────────────────────

def route_and_write(conn, sim_today: date, rng: random.Random) -> dict:
    """
    Stream every fact_bookings row and write the right events / state.

    For each row, applies the realism guard (validate_booking). Survivors
    are routed by date into one of:

      - COMPLETED   (checkout_date < sim_today)
            → BOOKING@booking_ts and either
                BOOKING + CHECKIN + CHECKOUT  (not cancelled), or
                BOOKING + CANCELLATION       (cancelled — timestamp
                drawn uniformly between booking_ts+1h and checkin-1h).
            No sim_open_bookings row.

      - IN_PROGRESS (checkin_date < sim_today <= checkout_date)
            → BOOKING + CHECKIN as history. sim_open_bookings state
              CHECKED_IN. Cancellation flag is_cancelled is ignored
              here — the customer already checked in, so the booking
              is genuinely "open" at sim-today and the simulator can
              decide its fate later.

      - BOOKED      (checkin_date >= sim_today, booking_ts < sim_today)
            → BOOKING@booking_ts as history. sim_open_bookings state
              BOOKED. Same is_cancelled treatment as IN_PROGRESS.

      - FUTURE      (booking_ts >= sim_today)
            → SKIPPED. Counted. Reserved for the stream simulator to
              emit when live time advances past sim-today.

    All distributions for the stats block are collected during the
    sweep (no second pass).

    Args:
        conn: open Postgres connection (caller commits).
        sim_today: anchor date driving the routing.
        rng: seeded random.Random for deterministic jitter / picks.

    Returns:
        A stats dict consumed by print_stats / acceptance.
    """
    hotel_to_city = load_hotel_to_city(conn)

    stats = {
        "rows_total":              0,
        "rows_processed":          0,
        "rows_skipped":            0,
        "skip_reasons":            {
            "gap_lt_1": 0,
            "nights_out_of_range": 0,
            "booked_after_checkin": 0,
            "num_guests_out_of_range": 0,
        },
        "nights_mismatched":       0,

        "bucket_completed":        0,
        "bucket_in_progress":      0,
        "bucket_booked":           0,
        "bucket_future":           0,

        "bookings_completed":      0,
        "bookings_cancelled":      0,

        "future_min_booking_ts":   None,
        "future_max_booking_ts":   None,

        "events_total":            0,
        "events_by_type":          {"BOOKING": 0, "CHECKIN": 0, "CHECKOUT": 0, "CANCELLATION": 0},
        "min_event_date":          None,
        "max_event_date":          None,

        "distinct_hotels":         set(),
        "distinct_customers":      set(),

        # Realism distributions — sampled per kept booking.
        "nights":                  [],
        "lead_time_days":          [],
        "num_guests":              [],
        "nightly_rate":            [],

        "open_booked":             0,
        "open_checkedin":          0,
    }

    select_sql = """
        SELECT booking_id, hotel_id, customer_id, room_type_id,
               checkin_date, checkout_date, nights_stayed, num_guests,
               nightly_rate_inr, booking_source, is_cancelled, booking_ts
        FROM   fact_bookings
        ORDER  BY booking_ts
    """

    print(f"[sweep] streaming all fact_bookings rows; sim_today={sim_today} ...")
    t0 = time.time()

    event_batch:    list[tuple] = []
    sim_open_batch: list[tuple] = []

    with conn.cursor(name="fb_sweep") as cur:
        cur.itersize = 10_000
        cur.execute(select_sql)

        for row in cur:
            stats["rows_total"] += 1
            (booking_id, hotel_id, customer_id, room_type_id,
             checkin_date, checkout_date, nights_stored, num_guests,
             nightly_rate_inr, booking_source, is_cancelled, booking_ts) = row

            # ── FUTURE bucket: short-circuit before the realism guard.
            #    Future bookings are owned by the stream simulator; we
            #    just observe them and report their range.
            booking_dt = booking_ts.replace(tzinfo=timezone.utc) if booking_ts.tzinfo is None else booking_ts
            if booking_dt.date() >= sim_today:
                stats["bucket_future"] += 1
                if stats["future_min_booking_ts"] is None or booking_dt < stats["future_min_booking_ts"]:
                    stats["future_min_booking_ts"] = booking_dt
                if stats["future_max_booking_ts"] is None or booking_dt > stats["future_max_booking_ts"]:
                    stats["future_max_booking_ts"] = booking_dt
                continue

            # ── Realism guard. Skip-or-fix.
            nights, skip_reason, mismatched = validate_booking(
                booking_ts, checkin_date, checkout_date, nights_stored, num_guests,
            )
            if skip_reason is not None:
                stats["rows_skipped"] += 1
                stats["skip_reasons"][skip_reason] += 1
                continue
            if mismatched:
                stats["nights_mismatched"] += 1

            # Trust the gap; recompute revenue so the invariant holds
            # even when the stored revenue/nights disagreed.
            revenue_inr = round(float(nightly_rate_inr) * nights, 2)

            city         = hotel_to_city.get(hotel_id)
            payment_mode = rng.choices(PAYMENT_MODES, weights=PAYMENT_WEIGHTS, k=1)[0]

            stats["rows_processed"] += 1
            stats["distinct_hotels"].add(hotel_id)
            stats["distinct_customers"].add(customer_id)
            stats["nights"].append(nights)
            stats["lead_time_days"].append((checkin_date - booking_dt.date()).days)
            stats["num_guests"].append(int(num_guests))
            stats["nightly_rate"].append(float(nightly_rate_inr))

            # Always emit BOOKING — same for every non-future bucket.
            event_batch.append(build_event_row(
                event_type="BOOKING", booking_id=booking_id,
                customer_id=customer_id, hotel_id=hotel_id, city=city,
                room_type_id=room_type_id, event_dt=booking_dt,
                checkin_date=checkin_date, checkout_date=checkout_date,
                nights=nights, num_guests=num_guests,
                nightly_rate_inr=nightly_rate_inr, revenue_inr=revenue_inr,
                booking_source=booking_source, payment_mode=payment_mode,
                cancellation_reason=None,
            ))

            # ── Bucket routing by stay dates.
            if checkout_date < sim_today:
                stats["bucket_completed"] += 1
                if is_cancelled:
                    # CANCELLATION timestamp: uniform between
                    # (booking_ts + 1h) and (checkin_date - 1h). If the
                    # interval collapses (booked same day as check-in),
                    # fall back to booking_ts + 30 min.
                    ci_dt = datetime.combine(checkin_date, dtime(hour=11), tzinfo=timezone.utc)
                    low   = booking_dt + timedelta(hours=1)
                    high  = ci_dt - timedelta(hours=1)
                    if high <= low:
                        cancel_dt = booking_dt + timedelta(minutes=30)
                    else:
                        cancel_dt = low + timedelta(seconds=rng.randrange(int((high - low).total_seconds()) + 1))
                    reason = rng.choices(CANCEL_REASONS, weights=CANCEL_REASON_WEIGHTS, k=1)[0]
                    event_batch.append(build_event_row(
                        event_type="CANCELLATION", booking_id=booking_id,
                        customer_id=customer_id, hotel_id=hotel_id, city=city,
                        room_type_id=room_type_id, event_dt=cancel_dt,
                        checkin_date=checkin_date, checkout_date=checkout_date,
                        nights=nights, num_guests=num_guests,
                        nightly_rate_inr=nightly_rate_inr, revenue_inr=revenue_inr,
                        booking_source=booking_source, payment_mode=payment_mode,
                        cancellation_reason=reason,
                    ))
                    stats["bookings_cancelled"] += 1
                else:
                    # CHECKIN afternoon-jittered; CHECKOUT morning-jittered.
                    checkin_dt = datetime.combine(
                        checkin_date,
                        dtime(hour=14, minute=rng.randrange(0, 60)),
                        tzinfo=timezone.utc,
                    )
                    # Same-day check-in after the booking time would otherwise
                    # put CHECKIN's event_ts before BOOKING's. Bump forward by
                    # 15-90 min so the wall-clock order matches the legal
                    # BOOKING -> CHECKIN sequence. event_date stays
                    # checkin_date (business date); event_ts is allowed to
                    # spill across midnight when it has to.
                    if checkin_dt <= booking_dt:
                        checkin_dt = booking_dt + timedelta(
                            minutes=rng.randrange(15, 91)
                        )
                    checkout_dt = datetime.combine(
                        checkout_date,
                        dtime(hour=11, minute=rng.randrange(0, 30)),
                        tzinfo=timezone.utc,
                    )
                    event_batch.append(build_event_row(
                        event_type="CHECKIN", booking_id=booking_id,
                        customer_id=customer_id, hotel_id=hotel_id, city=city,
                        room_type_id=room_type_id, event_dt=checkin_dt,
                        checkin_date=checkin_date, checkout_date=checkout_date,
                        nights=nights, num_guests=num_guests,
                        nightly_rate_inr=nightly_rate_inr, revenue_inr=revenue_inr,
                        booking_source=booking_source, payment_mode=payment_mode,
                        cancellation_reason=None,
                    ))
                    event_batch.append(build_event_row(
                        event_type="CHECKOUT", booking_id=booking_id,
                        customer_id=customer_id, hotel_id=hotel_id, city=city,
                        room_type_id=room_type_id, event_dt=checkout_dt,
                        checkin_date=checkin_date, checkout_date=checkout_date,
                        nights=nights, num_guests=num_guests,
                        nightly_rate_inr=nightly_rate_inr, revenue_inr=revenue_inr,
                        booking_source=booking_source, payment_mode=payment_mode,
                        cancellation_reason=None,
                    ))
                    stats["bookings_completed"] += 1

            elif checkin_date < sim_today:
                # IN_PROGRESS — guest is mid-stay at sim_today.
                stats["bucket_in_progress"] += 1
                checkin_dt = datetime.combine(
                    checkin_date,
                    dtime(hour=14, minute=rng.randrange(0, 60)),
                    tzinfo=timezone.utc,
                )
                # Same monotonicity guard as the COMPLETED branch — see
                # comment there. Prevents CHECKIN.event_ts < BOOKING.event_ts
                # when the booking is made same-day after 2pm.
                if checkin_dt <= booking_dt:
                    checkin_dt = booking_dt + timedelta(
                        minutes=rng.randrange(15, 91)
                    )
                event_batch.append(build_event_row(
                    event_type="CHECKIN", booking_id=booking_id,
                    customer_id=customer_id, hotel_id=hotel_id, city=city,
                    room_type_id=room_type_id, event_dt=checkin_dt,
                    checkin_date=checkin_date, checkout_date=checkout_date,
                    nights=nights, num_guests=num_guests,
                    nightly_rate_inr=nightly_rate_inr, revenue_inr=revenue_inr,
                    booking_source=booking_source, payment_mode=payment_mode,
                    cancellation_reason=None,
                ))
                sim_open_batch.append((
                    str(booking_id), customer_id, hotel_id, city, str(room_type_id),
                    checkin_date, checkout_date, nights, num_guests,
                    float(nightly_rate_inr), payment_mode, booking_source,
                    "CHECKED_IN", booking_dt, "history",
                ))
                stats["open_checkedin"] += 1

            else:
                # BOOKED — checkin is on/after sim_today, booking already in.
                stats["bucket_booked"] += 1
                sim_open_batch.append((
                    str(booking_id), customer_id, hotel_id, city, str(room_type_id),
                    checkin_date, checkout_date, nights, num_guests,
                    float(nightly_rate_inr), payment_mode, booking_source,
                    "BOOKED", booking_dt, "history",
                ))
                stats["open_booked"] += 1

            # Stream-flush the event batch in chunks; the named cursor
            # holds an open server-side portal, so don't open additional
            # cursors on the same connection while it's mid-fetch — keep
            # writes to a single execute_values per chunk.
            if len(event_batch) >= INSERT_CHUNK:
                flush_events(conn, event_batch)
                _tally_events(event_batch, stats)
                event_batch.clear()

        # Final partial chunk for events.
        if event_batch:
            flush_events(conn, event_batch)
            _tally_events(event_batch, stats)
            event_batch.clear()

    # sim_open rows are independent — insert in one shot after the
    # named cursor has been released (its `with` block exited above).
    if sim_open_batch:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO sim_open_bookings (
                    booking_id, customer_id, hotel_id, city, room_type_id,
                    checkin_date, checkout_date, nights, num_guests,
                    nightly_rate_inr, payment_mode, booking_source, state,
                    booked_event_ts, source
                ) VALUES %s
                """,
                sim_open_batch,
                page_size=1000,
            )

    conn.commit()
    stats["elapsed_seconds"] = time.time() - t0
    print(f"[sweep] wrote {stats['events_total']:,} events "
          f"+ {stats['open_booked'] + stats['open_checkedin']:,} open backlog rows "
          f"in {stats['elapsed_seconds']:.1f}s")
    return stats


def _tally_events(batch: list[tuple], stats: dict) -> None:
    """
    Update per-type counters and event_date range from a flushed batch.

    The batch tuples follow _EVENT_COLUMNS, so event_type is index 1
    and event_date is index 8.

    Args:
        batch: tuples that were just bulk-inserted.
        stats: stats dict mutated in place.
    """
    for r in batch:
        et = r[1]
        ed = r[8]
        stats["events_by_type"][et] = stats["events_by_type"].get(et, 0) + 1
        stats["events_total"] += 1
        if stats["min_event_date"] is None or ed < stats["min_event_date"]:
            stats["min_event_date"] = ed
        if stats["max_event_date"] is None or ed > stats["max_event_date"]:
            stats["max_event_date"] = ed


# ──────────────────────────────────────────────────────────────────────
# Stats reporting
# ──────────────────────────────────────────────────────────────────────

def _summary_stats(xs: list) -> tuple:
    """
    Compute (min, median, mean, max) of a numeric list, robust to empty.

    Args:
        xs: numeric values (ints or floats).

    Returns:
        Tuple of four floats (or four None if xs is empty).
    """
    if not xs:
        return (None, None, None, None)
    return (min(xs), statistics.median(xs), statistics.mean(xs), max(xs))


# Histogram buckets for the nights distribution. Surfaces the real
# long-stay tail (15-30, 31-60) without drowning it in the dominant
# short-stay band.
NIGHTS_BUCKETS = [(1, 3), (4, 7), (8, 14), (15, 30), (31, 60)]


def _nights_histogram(nights: list[int]) -> list[tuple[str, int, float]]:
    """
    Bucket the per-booking nights values into NIGHTS_BUCKETS.

    Args:
        nights: list of integer night counts (one per kept booking).

    Returns:
        List of (label, count, pct) tuples in NIGHTS_BUCKETS order.
        `pct` is share of the total, rounded for display.
    """
    total = len(nights)
    out: list[tuple[str, int, float]] = []
    for lo, hi in NIGHTS_BUCKETS:
        c = sum(1 for n in nights if lo <= n <= hi)
        pct = (100.0 * c / total) if total else 0.0
        out.append((f"{lo:>2}-{hi:<2}", c, pct))
    return out


def print_stats(conn, sim_today: date, stats: dict) -> None:
    """
    Print the human-readable stats block.

    Pulls live row counts from Postgres so the "final row counts"
    section reflects what's actually persisted (including any
    pre-seeded source='stream' dummy that was preserved through the
    reset).

    Args:
        conn: open Postgres connection.
        sim_today: the anchor used for this run.
        stats: the stats dict returned by route_and_write.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT source, COUNT(*) FROM fact_booking_events GROUP BY source ORDER BY source")
        events_by_source = dict(cur.fetchall())
        cur.execute("SELECT COUNT(*) FROM sim_open_bookings")
        sim_open_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM hotel_master")
        hotel_master_count = cur.fetchone()[0]

    nights_s    = _summary_stats(stats["nights"])
    lead_s      = _summary_stats(stats["lead_time_days"])
    guests_s    = _summary_stats(stats["num_guests"])
    rate_s      = _summary_stats(stats["nightly_rate"])
    distinct_h  = len(stats["distinct_hotels"])
    distinct_c  = len(stats["distinct_customers"])

    print("\n" + "═" * 72)
    print("  GENERATOR STATS")
    print("═" * 72)
    print(f"  sim-today anchor:                 {sim_today}")
    print(f"  fact_bookings rows seen:          {stats['rows_total']:>10,}")
    print(f"  rows processed (kept):            {stats['rows_processed']:>10,}")
    print(f"  rows skipped (realism guard):     {stats['rows_skipped']:>10,}")
    for reason, n in stats["skip_reasons"].items():
        print(f"      {reason:<28} {n:>10,}")
    print(f"  rows where stored nights disagreed with gap (trusted gap, recomputed revenue):"
          f"  {stats['nights_mismatched']:,}")
    print()
    print(f"  Bucket routing:")
    print(f"    COMPLETED (BOOKING + CI + CO, or BOOKING + CANCEL):  {stats['bucket_completed']:>10,}")
    print(f"        of which completed:                              {stats['bookings_completed']:>10,}")
    print(f"        of which cancelled:                              {stats['bookings_cancelled']:>10,}")
    print(f"    IN_PROGRESS (BOOKING + CHECKIN -> sim_open CHECKED_IN): {stats['bucket_in_progress']:>10,}")
    print(f"    BOOKED      (BOOKING only         -> sim_open BOOKED):  {stats['bucket_booked']:>10,}")
    print(f"    FUTURE      (skipped — reserved for the stream):    {stats['bucket_future']:>10,}")
    if stats["bucket_future"]:
        print(f"        future booking_ts range: "
              f"{stats['future_min_booking_ts']} -> {stats['future_max_booking_ts']}")
    print()
    print(f"  Events written by type:")
    for t in ("BOOKING", "CHECKIN", "CHECKOUT", "CANCELLATION"):
        print(f"    {t:<14} {stats['events_by_type'].get(t, 0):>12,}")
    print(f"    {'TOTAL':<14} {stats['events_total']:>12,}")
    if stats["min_event_date"]:
        gap_days = (sim_today - stats["max_event_date"]).days
        print(f"  History event_date range:       "
              f"{stats['min_event_date']} -> {stats['max_event_date']}  "
              f"(latest is {gap_days} day(s) before sim-today)")
    print()
    print(f"  Open backlog written (sim_open_bookings):")
    print(f"    BOOKED      (awaiting CHECKIN):                       {stats['open_booked']:>10,}")
    print(f"    CHECKED_IN  (awaiting CHECKOUT):                      {stats['open_checkedin']:>10,}")
    print(f"    TOTAL                                                : "
          f"{stats['open_booked'] + stats['open_checkedin']:>10,}")
    print()
    print(f"  REALISM distributions (per kept booking):")
    print(f"    nights         min/median/mean/max: "
          f"{nights_s[0]} / {nights_s[1]} / {nights_s[2]:.2f} / {nights_s[3]}")
    print(f"    lead time (d)  min/median/mean/max: "
          f"{lead_s[0]} / {lead_s[1]} / {lead_s[2]:.2f} / {lead_s[3]}")
    print(f"    num_guests     min/median/max:      "
          f"{guests_s[0]} / {guests_s[1]} / {guests_s[3]}")
    print(f"    nightly_rate   min/median/max:      "
          f"{rate_s[0]:.2f} / {rate_s[1]:.2f} / {rate_s[3]:.2f}")
    print()
    print(f"    nights histogram (kept bookings):")
    for label, count, pct in _nights_histogram(stats["nights"]):
        bar = "▎" * max(1, int(round(pct / 2))) if count else ""
        print(f"      {label} nights  {count:>10,}  {pct:>5.1f}%  {bar}")
    print()
    print(f"  Distinct hotels covered:        "
          f"{distinct_h:,} / {hotel_master_count:,} "
          f"({100*distinct_h/max(hotel_master_count,1):.1f}%)")
    print(f"  Distinct customers covered:     {distinct_c:,}")
    print()
    print(f"  Final row counts:")
    for src, cnt in events_by_source.items():
        print(f"    fact_booking_events  source={src:<8}  {cnt:>12,}")
    print(f"    sim_open_bookings                       {sim_open_total:>12,}")
    print("═" * 72 + "\n")


# ──────────────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────────────

def main() -> int:
    """
    Entry point — open a connection, optionally reset, sweep, print stats.

    Returns:
        Process exit code (0 on success).
    """
    args = parse_args()
    rng = random.Random(RNG_SEED)

    print(f"Connecting to Postgres at {DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']} ...")
    conn = connect()
    try:
        if args.reset:
            reset_script_rows(conn)
        stats = route_and_write(conn, args.sim_today, rng)
        print_stats(conn, args.sim_today, stats)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
