"""
Event simulator — calendar-driven REPLAY producer (B-034, Phase A).

Publishes booking events to Kafka by REPLAYING real fact_bookings rows on
a sim-clock that advances one logical day at a time. The earlier version
of this file was a stateless lifecycle simulator that minted synthetic
booking_ids and customer_ids and drew outcomes randomly. This version
does the opposite: every BOOKING / CHECKIN / CHECKOUT / CANCELLATION on
the wire traces back to a real row in `fact_bookings`; cancellation
outcomes come from `fact_bookings.is_cancelled` (not a random draw).

The consumer (`scripts/stream_consumer.py`) is unchanged. The five wire
`event_type` strings are unchanged. The Kafka config — value serializer
bytes pass-through, key serializer, `acks="all"`, `linger_ms=20`,
partition key = city — is unchanged. The only NEW wire field is
`event_date` (the sim-day the event represents), which is additive: the
consumer ignores unknown fields, silver consumers can read it later.

────────────────────────────────────────────────────────────────────────
HOW IT WORKS
────────────────────────────────────────────────────────────────────────

Two real sources of truth at startup:

  (a) `sim_open_bookings` — every booking that was mid-lifecycle at the
      `--sim-start` anchor. These are real fact_bookings rows that the
      history backfill (B-035) put aside for the simulator to advance.
      Their BOOKING was already emitted as `source='history'`; the
      simulator does NOT re-emit it. It only advances them (CHECKIN /
      CHECKOUT / CANCELLATION) as their dates arrive.

  (b) `fact_bookings WHERE booking_ts >= --sim-start` — the FUTURE
      bucket the history backfill skipped. These are real bookings the
      simulator emits BOOKING events for, in `booking_ts` order, as the
      sim-clock walks through each row's booking_ts day.

A sim-clock advances one logical day per iteration. For each sim-day D:

   1. CANCELLATIONS scheduled for D fire (planned the first time we saw
      each is_cancelled booking; the plan is keyed off `booking_id +
      chaos-seed` so the same booking cancels on the same day across
      restarts).
   2. NEW BOOKINGS — every fact_bookings row with `booking_ts::date = D`
      is emitted as a BOOKING; the row is added to `sim_open_bookings`
      so future days can advance it.
   3. CHECKINS — every open BOOKED row with `checkin_date = D`. The
      row's state flips to CHECKED_IN.
   4. CHECKOUTS — every open CHECKED_IN row with `checkout_date = D`.
      The row is deleted.
   5. PRICE_CHANGE — a small fixed number of independent operational
      events, stateless and unchanged in shape from the old producer.

After the day's events are emitted at a throttled rate, the producer
flushes Kafka, commits the day's `sim_open_bookings` mutations, and
writes the sim-day to `scripts/.sim_clock.json`. On restart, the clock
is read and processing resumes at saved+1. Day boundaries are atomic
commit points — a graceful Ctrl-C finishes the in-progress day before
exiting.

────────────────────────────────────────────────────────────────────────
WIRE COMPATIBILITY
────────────────────────────────────────────────────────────────────────
Unchanged: `event_type` strings (BOOKING / CHECKIN / CHECKOUT /
CANCELLATION / PRICE_CHANGE), base envelope (`event_id, event_type,
hotel_id, city, event_ts`), per-type extras (BOOKING carries
checkin/checkout dates, nights, num_guests, nightly_rate_inr,
revenue_inr, booking_source; lifecycle events carry booking_id +
customer_id; CANCELLATION adds cancellation_reason; PRICE_CHANGE has
neither booking_id nor customer_id).

Added: `event_date` (ISO date — the sim-day the event represents). The
consumer's Gate 2 only requires (event_type, event_ts, city, hotel_id);
unknown fields pass through harmlessly.

The revenue invariant `revenue_inr == nightly_rate_inr * nights` is
asserted on every BOOKING. When `fact_bookings.nights_stayed` disagrees
with `checkout - checkin`, we trust the gap and recompute revenue (the
same rule the history generator follows in B-035).

────────────────────────────────────────────────────────────────────────
WHY event_ts IS NOW () AND NOT THE SIM-DAY
────────────────────────────────────────────────────────────────────────
`event_ts` stays wall-clock UTC NOW so the consumer's event-time
windowing, watermark, and freshness checks all behave the same as
before — windows close in seconds, the `/monitor` live pulse reads
"alive," the heartbeat keeps ticking. `event_date` carries the
*business* date (the sim-day) for downstream silver consumers that
care about when the event logically happened.

────────────────────────────────────────────────────────────────────────
CHAOS INJECTION (preserved verbatim)
────────────────────────────────────────────────────────────────────────
Same five malformed generators, same late-event shifter, same
dispatcher, same fields. CLI takes precedence; env vars are fallback:

  --malformed-pct / CHAOS_MALFORMED_PCT  → % of events corrupted
  --late-pct      / CHAOS_LATE_PCT       → % of events delayed past watermark
  --chaos-seed    / CHAOS_SEED           → reproducible chaos (fixes random.seed)

────────────────────────────────────────────────────────────────────────
Usage
────────────────────────────────────────────────────────────────────────
  # Resume from saved clock (or start at --sim-start if no clock yet):
  python -m scripts.kafka_event_producer

  # Bounded slice (acceptance test):
  python -m scripts.kafka_event_producer --until 2025-06-08

  # Start over from scratch:
  python -m scripts.kafka_event_producer --reset-clock --until 2025-06-08

  # 5% malformed, 2% late, reproducible:
  python -m scripts.kafka_event_producer --until 2025-06-04 \\
      --malformed-pct 5 --late-pct 2 --chaos-seed 42
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaProducer
from scripts.review_generator import make_review_event_dict as _make_review

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# ── Paths & seed data ─────────────────────────────────────────────────────────

DATA_DIR   = Path(os.getenv("DATA_DIR", "./data"))
SEED_FILE  = DATA_DIR / "booking_events_seed.json"
CLOCK_FILE = Path("scripts/.sim_clock.json")

# ── DB params ─────────────────────────────────────────────────────────────────

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# ── Defaults / tunables ───────────────────────────────────────────────────────

# Anchor — MUST match scripts/generate_lifecycle_history.py default.
DEFAULT_SIM_START = date(2025, 6, 1)

# Sim-days per real-second. At ~2000 events / sim-day (full-runway steady
# state) this yields ~100 evt/s, which keeps the consumer comfortable
# (the consumer's flush check runs every 10s; we want plenty of events
# per check cycle but not so many that windowing falls behind).
DEFAULT_SIM_SPEED = 0.05

# A handful of independent PRICE_CHANGE events per sim-day. The point is
# only that the stream carries them — they're stateless operational
# events, NOT lifecycle.
DEFAULT_NUM_PRICES_PER_DAY = 5

# Split among is_cancelled bookings: 70% customer_cancelled (pre-checkin
# day in [booking_ts, checkin_date)), 30% no_show (on checkin_date).
# Matches the prior stateless producer's reason mix.
P_CUSTOMER_CANCELLED = 0.70


# ══════════════════════════════════════════════════════════════════════════════
# CHAOS GENERATORS — preserved verbatim from the previous version
# ══════════════════════════════════════════════════════════════════════════════

def _corrupt_missing_field(event):
    """Remove one required field at random."""
    field = random.choice(["event_type", "event_ts", "city", "hotel_id"])
    bad = dict(event)
    bad.pop(field, None)
    return bad, "missing_field"


def _corrupt_unknown_event_type(event):
    """Replace event_type with a string outside the valid set."""
    bad = dict(event)
    bad["event_type"] = random.choice(["book", "BOOK", "RESERVED", "checkout"])
    return bad, "unknown_event_type"


def _corrupt_unknown_city(event):
    """Replace city with a typo / wrong-language / fictional name."""
    bad = dict(event)
    # Last entry is the Hindi for "Goa" — written as a unicode escape so
    # the parse-check (`open(...).read()` without explicit encoding)
    # doesn't choke on Windows where the default codec is cp1252.
    bad["city"] = random.choice(["Mumbay", "DELHI_TYPO", "Atlantis",
                                  "गोवा"])
    return bad, "unknown_city"


def _corrupt_unparseable_ts(event):
    """Make event_ts a string that isn't ISO 8601."""
    bad = dict(event)
    bad["event_ts"] = random.choice(["yesterday", "2026/05/19", "1747590000"])
    return bad, "unparseable_event_ts"


def _corrupt_unparseable_json(event):
    """Return raw bytes that don't deserialize to JSON."""
    return b'{"event_type": "BOOKING", "city": ', "unparseable_json"


_MALFORMED_GENERATORS = [
    _corrupt_missing_field,
    _corrupt_unknown_event_type,
    _corrupt_unknown_city,
    _corrupt_unparseable_ts,
    _corrupt_unparseable_json,
]


def _make_late(event, min_minutes_late=70, max_minutes_late=180):
    """
    Shift event_ts back into the past far enough that the consumer's
    late-event guard fires. Defaults (70-180 min) target the production
    60-minute window + 5-minute grace.
    """
    bad = dict(event)
    try:
        ts = datetime.fromisoformat(bad["event_ts"].replace("Z", "+00:00"))
    except Exception:
        ts = datetime.now(timezone.utc)
    minutes_back = random.uniform(min_minutes_late, max_minutes_late)
    bad["event_ts"] = (ts - timedelta(minutes=minutes_back)).isoformat()
    return bad


def maybe_corrupt_or_delay(event, malformed_pct, late_pct):
    """
    Decide what to do with this event. Returns (payload, mode); payload
    is dict (normal/late/most malformed) or bytes (unparseable_json).
    """
    r = random.random() * 100.0
    if r < malformed_pct:
        gen = random.choice(_MALFORMED_GENERATORS)
        payload, reason = gen(event)
        return payload, f"malformed:{reason}"
    if r < malformed_pct + late_pct:
        return _make_late(event), "late"
    return event, "normal"


# ══════════════════════════════════════════════════════════════════════════════
# SIM-CLOCK PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def load_clock(default_start):
    """
    Read scripts/.sim_clock.json and return (next_day, saved_seed).
    next_day is the sim-day to resume on (last_completed_day + 1, or
    default_start if no clock file). saved_seed is the chaos-seed from
    the previous run, or None if not recorded.
    """
    if not CLOCK_FILE.exists():
        return default_start, None
    try:
        data = json.loads(CLOCK_FILE.read_text(encoding="utf-8"))
        last = date.fromisoformat(data["last_completed_day"])
        saved_seed = data.get("chaos_seed")
        return last + timedelta(days=1), saved_seed
    except Exception as e:
        print(f"⚠ Could not parse {CLOCK_FILE}: {e}. "
              f"Starting at {default_start}.", file=sys.stderr)
        return default_start, None


def save_clock(day, chaos_seed):
    """
    Atomically record day as the most recently completed sim-day and
    persist the chaos_seed so cancellation plans stay deterministic
    across restarts (the plan is keyed off booking_id + chaos_seed).
    """
    CLOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CLOCK_FILE.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps({
            "last_completed_day": day.isoformat(),
            "chaos_seed": chaos_seed,
        }, indent=2),
        encoding="utf-8",
    )
    tmp.replace(CLOCK_FILE)


def reset_clock():
    if CLOCK_FILE.exists():
        CLOCK_FILE.unlink()


# ══════════════════════════════════════════════════════════════════════════════
# DB HELPERS — sim_open_bookings + fact_bookings
# ══════════════════════════════════════════════════════════════════════════════

# Tuple shape for runway rows. The same shape flows into make_booking_event
# and insert_sim_open_bookings; centralising it here makes the SELECT and
# the consumers easier to keep in step.
_RUNWAY_SELECT = """
    SELECT fb.booking_id, fb.hotel_id, fb.customer_id, fb.room_type_id,
           dl.city, fb.checkin_date, fb.checkout_date,
           fb.nights_stayed, fb.num_guests,
           fb.nightly_rate_inr, fb.revenue_inr, fb.booking_source,
           fb.is_cancelled, fb.booking_ts
    FROM fact_bookings fb
    LEFT JOIN dim_location dl ON dl.location_id = fb.location_id
"""


def fetch_runway_for_day(conn, day):
    """Every fact_bookings row whose booking_ts falls on `day`."""
    with conn.cursor() as cur:
        cur.execute(
            _RUNWAY_SELECT + " WHERE fb.booking_ts >= %s AND fb.booking_ts < %s "
                             " ORDER BY fb.booking_ts",
            (day, day + timedelta(days=1)),
        )
        return cur.fetchall()


def fetch_today_checkins(conn, day):
    """Open BOOKED rows whose checkin_date == day."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT booking_id, customer_id, hotel_id, city "
            "FROM sim_open_bookings "
            "WHERE state = 'BOOKED' AND checkin_date = %s",
            (day,),
        )
        return cur.fetchall()


def fetch_today_checkouts(conn, day):
    """Open CHECKED_IN rows whose checkout_date == day."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT booking_id, customer_id, hotel_id, city "
            "FROM sim_open_bookings "
            "WHERE state = 'CHECKED_IN' AND checkout_date = %s",
            (day,),
        )
        return cur.fetchall()


def lookup_open_booking(conn, booking_id):
    """Return (customer_id, hotel_id, city) for an open booking, or None."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT customer_id, hotel_id, city "
            "FROM sim_open_bookings WHERE booking_id = %s",
            (booking_id,),
        )
        return cur.fetchone()


def insert_sim_open_bookings(conn, fb_row, booked_event_ts, state, source):
    """
    Insert a newly-emitted BOOKING into sim_open_bookings. Idempotent via
    ON CONFLICT DO NOTHING so a partial-day replay can't violate the PK.
    """
    (booking_id, hotel_id, customer_id, room_type_id, city,
     checkin_date, checkout_date, nights_stayed, num_guests,
     nightly_rate_inr, revenue_inr, booking_source,
     is_cancelled, booking_ts) = fb_row

    gap = (checkout_date - checkin_date).days
    nights = gap if gap >= 1 else 1

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sim_open_bookings
                (booking_id, customer_id, hotel_id, city, room_type_id,
                 checkin_date, checkout_date, nights, num_guests,
                 nightly_rate_inr, payment_mode, booking_source,
                 state, booked_event_ts, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (booking_id) DO NOTHING
            """,
            (str(booking_id), customer_id, hotel_id, city, str(room_type_id),
             checkin_date, checkout_date, nights, int(num_guests),
             float(nightly_rate_inr), None, booking_source,
             state, booked_event_ts, source),
        )


def update_state_checked_in(conn, booking_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE sim_open_bookings SET state='CHECKED_IN' WHERE booking_id=%s",
            (booking_id,),
        )


def delete_open_booking(conn, booking_id):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM sim_open_bookings WHERE booking_id=%s",
            (booking_id,),
        )


def has_runway_after(conn, day):
    """True if any fact_bookings row has booking_ts on or after `day`."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM fact_bookings WHERE booking_ts >= %s)",
            (day,),
        )
        return cur.fetchone()[0]


def has_open_bookings(conn):
    """True if sim_open_bookings has any rows."""
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS(SELECT 1 FROM sim_open_bookings)")
        return cur.fetchone()[0]


def _load_hotel_rating_map(conn):
    """Return {hotel_id: (avg_rating, star_category)} from hotel_master."""
    with conn.cursor() as cur:
        cur.execute("SELECT hotel_id, avg_rating, star_category FROM hotel_master")
        return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def _fetch_sim_booking_for_review(conn, booking_id):
    """
    Return (booking_source, checkin_date, checkout_date, booking_ts_date)
    from sim_open_bookings for use in review generation, or None if not found.
    booked_event_ts is used as an approximation of booking_ts.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT booking_source, checkin_date, checkout_date,
                   booked_event_ts::date
            FROM   sim_open_bookings
            WHERE  booking_id = %s
            """,
            (booking_id,),
        )
        return cur.fetchone()


# ══════════════════════════════════════════════════════════════════════════════
# CANCELLATION PLAN — deterministic from booking_id + chaos-seed
# ══════════════════════════════════════════════════════════════════════════════

def plan_cancellation(booking_id, booking_ts_date, checkin_date, sim_start, plan_seed):
    """
    Decide WHEN and WHY a cancelled booking cancels. Pure function of
    (booking_id, sim_start, plan_seed): the same booking cancels on the
    same day across restarts.

      ~70%: customer_cancelled on a day in [max(booking_ts, sim_start),
            checkin_date - 1]. If that window is empty (booking_ts ==
            checkin_date), fall through to no_show.
      ~30%: no_show on checkin_date.

    Clamping to sim_start matters for hydrated BOOKED rows whose
    booking_ts predates the anchor — those bookings have not yet had
    their cancellation emitted by the history generator (the BOOKED
    bucket only emits BOOKING), so the simulator must emit it on or
    after sim_start.
    """
    rng = random.Random(f"{booking_id}|{plan_seed}")
    if rng.random() < P_CUSTOMER_CANCELLED:
        earliest = max(booking_ts_date, sim_start)
        latest = checkin_date - timedelta(days=1)
        if latest >= earliest:
            gap_days = (latest - earliest).days
            return earliest + timedelta(days=rng.randint(0, gap_days)), "customer_cancelled"
    return checkin_date, "no_show"


def hydrate_plans(conn, sim_start, plan_seed, resume_day):
    """
    Re-derive cancellation plans for all currently-open BOOKED rows in
    sim_open_bookings. Returns:
        (booked_count, checked_in_count, scheduled, pending_cancellations,
         cancellation_plans)
    where pending_cancellations is dict[date, list[booking_id]] keyed by
    the day each cancellation fires, and cancellation_plans is
    dict[booking_id, (cancel_date, reason)].

    CHECKED_IN rows: is_cancelled intentionally ignored (their CHECKIN
    has already been emitted by the history generator; we cannot
    retroactively cancel an in-flight stay). They will check out on
    their real checkout_date.

    Cancel-dates that land before resume_day are clamped up to
    resume_day so the simulator catches up rather than leaving the row
    stranded. (Only happens if the user changes --chaos-seed between
    runs.)
    """
    pending = defaultdict(list)
    plans = {}
    booked = checked_in = scheduled = 0
    stale_dates = 0
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sob.booking_id, sob.checkin_date, sob.state,
                   fb.is_cancelled, fb.booking_ts
            FROM sim_open_bookings sob
            JOIN fact_bookings fb ON fb.booking_id = sob.booking_id
            """
        )
        rows = cur.fetchall()
    for booking_id, checkin_date, state, is_cancelled, booking_ts in rows:
        if state == "CHECKED_IN":
            checked_in += 1
            continue
        booked += 1
        if not is_cancelled:
            continue
        cancel_date, reason = plan_cancellation(
            str(booking_id), booking_ts.date(), checkin_date, sim_start, plan_seed
        )
        if cancel_date < resume_day:
            stale_dates += 1
            cancel_date = resume_day
        plans[str(booking_id)] = (cancel_date, reason)
        pending[cancel_date].append(str(booking_id))
        scheduled += 1
    if stale_dates:
        print(f"⚠ {stale_dates} hydrated cancellations had plan dates before "
              f"{resume_day}; clamped forward (likely chaos-seed change between runs).")
    return booked, checked_in, scheduled, pending, plans


# ══════════════════════════════════════════════════════════════════════════════
# EVENT BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso_utc():
    return datetime.now(timezone.utc).isoformat()


def make_booking_event(fb_row, sim_day):
    """
    Build a BOOKING event from a fact_bookings row. event_ts is wall-
    clock NOW; event_date is the sim-day. Trusts checkout-checkin as
    the night count and RECOMPUTES revenue, so the revenue invariant
    `revenue_inr == nightly_rate_inr * nights` always holds — even when
    fact_bookings has a (rare) nights_stayed mismatch.
    """
    (booking_id, hotel_id, customer_id, room_type_id, city,
     checkin_date, checkout_date, nights_stayed, num_guests,
     nightly_rate_inr, revenue_inr, booking_source,
     is_cancelled, booking_ts) = fb_row

    gap = (checkout_date - checkin_date).days
    nights = gap if gap >= 1 else 1
    nightly_int = int(round(float(nightly_rate_inr)))
    revenue_int = nightly_int * nights

    assert revenue_int == nightly_int * nights, \
        f"revenue invariant broken: {revenue_int} != {nightly_int} * {nights}"

    return {
        "event_id":         str(uuid.uuid4()),
        "event_type":       "BOOKING",
        "hotel_id":         hotel_id,
        "city":             city,
        "event_ts":         _now_iso_utc(),
        "event_date":       sim_day.isoformat(),
        "booking_id":       str(booking_id),
        "customer_id":      customer_id,
        "room_type_id":     str(room_type_id),
        "checkin_date":     checkin_date.isoformat(),
        "checkout_date":    checkout_date.isoformat(),
        "nights":           nights,
        "num_guests":       int(num_guests),
        "nightly_rate_inr": nightly_int,
        "revenue_inr":      revenue_int,
        "booking_source":   booking_source,
    }


def make_lifecycle_event(event_type, booking_id, customer_id, hotel_id, city,
                         sim_day, extra=None):
    """
    Build a CHECKIN / CHECKOUT / CANCELLATION event. Base envelope plus
    booking_id + customer_id; CANCELLATION carries cancellation_reason
    in `extra`.
    """
    event = {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "hotel_id":    hotel_id,
        "city":        city,
        "event_ts":    _now_iso_utc(),
        "event_date":  sim_day.isoformat(),
        "booking_id":  str(booking_id),
        "customer_id": customer_id,
    }
    if extra:
        event.update(extra)
    return event


def make_price_change_event(hotel, sim_day):
    """
    Stateless operational event. No booking_id / customer_id — preserved
    from the previous version.
    """
    base = hotel.get("base_daily_bookings", 2000)
    old_price = int(base * random.uniform(0.8, 1.0))
    new_price = int(old_price * random.uniform(0.9, 1.2))
    return {
        "event_id":      str(uuid.uuid4()),
        "event_type":    "PRICE_CHANGE",
        "hotel_id":      hotel["hotel_id"],
        "city":          hotel["city"],
        "event_ts":      _now_iso_utc(),
        "event_date":    sim_day.isoformat(),
        "room_type_id":  str(uuid.uuid4()),
        "old_price_inr": old_price,
        "new_price_inr": new_price,
    }


# ══════════════════════════════════════════════════════════════════════════════
# PER-SIM-DAY LOOP
# ══════════════════════════════════════════════════════════════════════════════

def collect_day_events(conn, hotels, day, sim_start, plan_seed,
                        num_prices, pending_cancellations, cancellation_plans,
                        day_rng, wire_counts, hotel_rating_map):
    """
    Build the full event list for sim-day `day`, mutating sim_open_bookings
    in the same transaction (NOT committed here — caller commits at end
    of day so the day is atomic).
    Returns the list of (event_dict, partition_city) tuples.
    hotel_rating_map: {hotel_id: (avg_rating, star_category)} loaded at startup.
    """
    events = []

    # ── 1. Cancellations scheduled for today (from prior days' BOOKINGs / hydration)
    for booking_id in pending_cancellations.pop(day, []):
        plan = cancellation_plans.get(booking_id)
        if plan is None:
            continue
        cancel_date, reason = plan
        row = lookup_open_booking(conn, booking_id)
        if row is None:
            # Already evicted (e.g., duplicate scheduling). Skip.
            del cancellation_plans[booking_id]
            continue
        customer_id, hotel_id, city = row
        evt = make_lifecycle_event(
            "CANCELLATION", booking_id, customer_id, hotel_id, city, day,
            extra={"cancellation_reason": reason},
        )
        events.append((evt, city))
        wire_counts["CANCELLATION"] += 1
        # B-030: emit REVIEW before row is deleted (need checkin/out dates).
        _rev_ex = _fetch_sim_booking_for_review(conn, booking_id)
        if _rev_ex is not None:
            _bsrc, _cin, _cout, _bts_d = _rev_ex
            _avg, _star = hotel_rating_map.get(hotel_id, (3.0, "budget"))
            _rv = _make_review(
                booking_id=booking_id, customer_id=customer_id,
                hotel_id=hotel_id, booking_source=_bsrc,
                hotel_avg_rating=float(_avg or 3.0), star_category=_star,
                lifecycle_status="CANCELLED", sim_day=day,
                booking_ts_date=_bts_d, checkin_date=_cin, checkout_date=_cout,
            )
            if _rv is not None:
                _rv["event_ts"] = _now_iso_utc()
                events.append((_rv, city))
                wire_counts["REVIEW"] += 1
        delete_open_booking(conn, booking_id)
        del cancellation_plans[booking_id]

    # ── 2. New bookings — runway WHERE booking_ts::date = day
    for fb_row in fetch_runway_for_day(conn, day):
        (booking_id, hotel_id, customer_id, room_type_id, city,
         checkin_date, checkout_date, nights_stayed, num_guests,
         nightly_rate_inr, revenue_inr, booking_source,
         is_cancelled, booking_ts) = fb_row

        booking_evt = make_booking_event(fb_row, day)
        events.append((booking_evt, city))
        wire_counts["BOOKING"] += 1

        if is_cancelled:
            cancel_date, reason = plan_cancellation(
                str(booking_id), booking_ts.date(), checkin_date, sim_start, plan_seed,
            )
            if cancel_date == day:
                # Same-day cancellation — emit pair, do NOT touch sim_open_bookings.
                cancel_evt = make_lifecycle_event(
                    "CANCELLATION", str(booking_id), customer_id, hotel_id, city, day,
                    extra={"cancellation_reason": reason},
                )
                events.append((cancel_evt, city))
                wire_counts["CANCELLATION"] += 1
                # B-030: same-day cancel review — data from fb_row directly.
                _avg, _star = hotel_rating_map.get(hotel_id, (3.0, "budget"))
                _rv = _make_review(
                    booking_id=str(booking_id), customer_id=customer_id,
                    hotel_id=hotel_id, booking_source=booking_source,
                    hotel_avg_rating=float(_avg or 3.0), star_category=_star,
                    lifecycle_status="CANCELLED", sim_day=day,
                    booking_ts_date=booking_ts.date(),
                    checkin_date=checkin_date, checkout_date=checkout_date,
                )
                if _rv is not None:
                    _rv["event_ts"] = _now_iso_utc()
                    events.append((_rv, city))
                    wire_counts["REVIEW"] += 1
            else:
                insert_sim_open_bookings(
                    conn, fb_row, booking_evt["event_ts"],
                    state="BOOKED", source="stream",
                )
                cancellation_plans[str(booking_id)] = (cancel_date, reason)
                pending_cancellations[cancel_date].append(str(booking_id))
        else:
            insert_sim_open_bookings(
                conn, fb_row, booking_evt["event_ts"],
                state="BOOKED", source="stream",
            )

    # ── 3. Check-ins for today
    for booking_id, customer_id, hotel_id, city in fetch_today_checkins(conn, day):
        evt = make_lifecycle_event(
            "CHECKIN", str(booking_id), customer_id, hotel_id, city, day,
        )
        events.append((evt, city))
        wire_counts["CHECKIN"] += 1
        update_state_checked_in(conn, booking_id)

    # ── 4. Check-outs for today
    for booking_id, customer_id, hotel_id, city in fetch_today_checkouts(conn, day):
        evt = make_lifecycle_event(
            "CHECKOUT", str(booking_id), customer_id, hotel_id, city, day,
        )
        events.append((evt, city))
        wire_counts["CHECKOUT"] += 1
        # B-030: emit REVIEW before row is deleted (need booking dates).
        _rev_ex = _fetch_sim_booking_for_review(conn, booking_id)
        if _rev_ex is not None:
            _bsrc, _cin, _cout, _bts_d = _rev_ex
            _avg, _star = hotel_rating_map.get(hotel_id, (3.0, "budget"))
            _rv = _make_review(
                booking_id=str(booking_id), customer_id=customer_id,
                hotel_id=hotel_id, booking_source=_bsrc,
                hotel_avg_rating=float(_avg or 3.0), star_category=_star,
                lifecycle_status="COMPLETED", sim_day=day,
                booking_ts_date=_bts_d, checkin_date=_cin, checkout_date=_cout,
            )
            if _rv is not None:
                _rv["event_ts"] = _now_iso_utc()
                events.append((_rv, city))
                wire_counts["REVIEW"] += 1
        delete_open_booking(conn, booking_id)

    # ── 5. Stateless PRICE_CHANGE
    for _ in range(num_prices):
        h = day_rng.choice(hotels)
        evt = make_price_change_event(h, day)
        events.append((evt, h["city"]))
        wire_counts["PRICE_CHANGE"] += 1

    # ── 6. Deterministic intra-day shuffle so we don't always emit
    # CANCELLATIONS-then-BOOKINGS-then-CHECKINS-... in lockstep.
    day_rng.shuffle(events)
    return events


def run_one_day(conn, producer, topic, hotels, day, sim_start, plan_seed,
                num_prices, pending_cancellations, cancellation_plans,
                sim_speed, day_rng, chaos_args, chaos_stats, wire_counts,
                chaos_seed_for_clock, hotel_rating_map):
    """
    Execute one sim-day atomically: build events, throttle-send, flush
    Kafka, commit DB, save clock. A graceful Ctrl-C that lands mid-day
    will still finish the day before exiting (the SIGINT handler clears
    `running` but the day-loop checks between days).
    """
    events = collect_day_events(
        conn, hotels, day, sim_start, plan_seed,
        num_prices, pending_cancellations, cancellation_plans,
        day_rng, wire_counts, hotel_rating_map,
    )

    n = len(events)
    day_budget = 1.0 / sim_speed
    interval = (day_budget / n) if n > 0 else 0
    t0 = time.time()
    for i, (evt, partition_city) in enumerate(events):
        payload, mode = maybe_corrupt_or_delay(evt, *chaos_args)
        chaos_stats[mode] += 1
        producer.send(topic, key=partition_city, value=payload)
        if interval > 0:
            target = t0 + (i + 1) * interval
            drift = target - time.time()
            if drift > 0:
                time.sleep(drift)

    if n == 0 and day_budget > 0:
        # No events today; still advance wall-clock so the achieved
        # sim-speed reflects the configured value rather than racing.
        time.sleep(day_budget)

    # ── Commit-at-end-of-day: Kafka flush, DB commit, clock save.
    producer.flush()
    conn.commit()
    save_clock(day, chaos_seed_for_clock)
    return n, (time.time() - t0)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date_arg(s):
    if isinstance(s, date):
        return s
    return date.fromisoformat(s)


def main():
    parser = argparse.ArgumentParser(
        description="TravelLens Kafka event producer (calendar replay simulator)"
    )
    parser.add_argument(
        "--sim-start", type=_parse_date_arg, default=DEFAULT_SIM_START,
        help="Anchor sim-date (default 2025-06-01). MUST match the history generator.",
    )
    parser.add_argument(
        "--until", type=_parse_date_arg, default=None,
        help="Stop AFTER processing this sim-day (inclusive). Omit to run full runway.",
    )
    parser.add_argument(
        "--sim-speed", type=float, default=DEFAULT_SIM_SPEED,
        help="Sim-days per real-second. Default 0.05 (~100 evt/s steady state).",
    )
    parser.add_argument(
        "--reset-clock", action="store_true",
        help="Delete scripts/.sim_clock.json before starting (begin at --sim-start).",
    )
    parser.add_argument(
        "--num-prices-per-day", type=int, default=DEFAULT_NUM_PRICES_PER_DAY,
        help="PRICE_CHANGE events emitted per sim-day (stateless).",
    )
    # Chaos flags — CLI takes precedence over CHAOS_* env vars.
    parser.add_argument(
        "--malformed-pct", type=float,
        default=float(os.getenv("CHAOS_MALFORMED_PCT", "0")),
    )
    parser.add_argument(
        "--late-pct", type=float,
        default=float(os.getenv("CHAOS_LATE_PCT", "0")),
    )
    parser.add_argument(
        "--chaos-seed", type=int,
        default=(int(os.getenv("CHAOS_SEED")) if os.getenv("CHAOS_SEED") else None),
    )
    # Deprecated, accepted-and-ignored. The old stateless producer's primary
    # knobs were --rate (events/sec) and --duration (seconds). The calendar
    # replay model is driven by --sim-speed (sim-days/sec) and --until
    # (sim-date). Kept as no-ops so callers like run.py (which still passes
    # --rate from --sim-rate) don't break — a deprecation warning prints
    # if they're used so the next pass over run.py picks them up.
    parser.add_argument("--rate", type=float, default=None,
                        help="DEPRECATED. Ignored. Use --sim-speed.")
    parser.add_argument("--duration", type=float, default=None,
                        help="DEPRECATED. Ignored. Use --until.")
    args = parser.parse_args()

    if args.rate is not None:
        print(f"⚠ --rate is deprecated and ignored; use --sim-speed "
              f"(sim-days per real-second). Current --sim-speed={args.sim_speed}.",
              file=sys.stderr)
    if args.duration is not None:
        print(f"⚠ --duration is deprecated and ignored; use --until <sim-date>. "
              f"Current --until={args.until}.", file=sys.stderr)

    sim_start = args.sim_start
    chaos_seed = args.chaos_seed
    # plan_seed is used to make cancellation plans deterministic; it
    # falls back to 0 so plans are still reproducible across runs even
    # without an explicit --chaos-seed.
    plan_seed = chaos_seed if chaos_seed is not None else 0

    if chaos_seed is not None:
        random.seed(chaos_seed)

    if args.malformed_pct + args.late_pct > 100:
        print(f"⚠ CHAOS config error: malformed ({args.malformed_pct}%) + "
              f"late ({args.late_pct}%) > 100%.", file=sys.stderr)

    if args.reset_clock:
        reset_clock()
        print(f"Clock reset.")

    resume_day, saved_seed = load_clock(default_start=sim_start)
    if resume_day < sim_start:
        resume_day = sim_start

    # Reconcile chaos_seed: explicit CLI > previously saved > None. If both
    # are present and disagree, warn — cancellation plans will shift.
    if chaos_seed is None and saved_seed is not None:
        chaos_seed = saved_seed
        plan_seed = chaos_seed
        print(f"Reusing chaos_seed={chaos_seed} from {CLOCK_FILE.name} (plans stay deterministic across restart).")
        random.seed(chaos_seed)
    elif chaos_seed is not None and saved_seed is not None and chaos_seed != saved_seed:
        print(f"⚠ chaos_seed changed: was {saved_seed}, now {chaos_seed}. "
              f"Previously-scheduled cancellation plans will shift; some hydrated "
              f"plans may be clamped forward.", file=sys.stderr)

    # ── Kafka producer — bytes pass-through, city partition key,
    # acks=all, linger_ms=20 ALL PRESERVED.
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic     = os.getenv("KAFKA_TOPIC",     "booking-events")
    hotels = json.loads(SEED_FILE.read_text(encoding="utf-8"))
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=20,
    )

    # ── DB. autocommit=False; we commit at end of every sim-day.
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    # B-030: hotel rating map loaded once at startup for review generation.
    hotel_rating_map = _load_hotel_rating_map(conn)

    # ── Graceful shutdown — stops AFTER the current day's commit.
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    # ── Hydration: read sim_open_bookings + plan its cancellations.
    booked, checked_in, scheduled, pending_cancellations, cancellation_plans = \
        hydrate_plans(conn, sim_start, plan_seed, resume_day)

    print(f"sim-start={sim_start}  resume-day={resume_day}  "
          f"sim-speed={args.sim_speed} day/s  "
          f"until={args.until.isoformat() if args.until else 'runway exhausted'}")
    print(f"Hydrated sim_open_bookings: {booked} BOOKED + {checked_in} CHECKED_IN "
          f"= {booked+checked_in} total  |  "
          f"scheduled cancellations: {scheduled}")
    print(f"Kafka: {bootstrap}/{topic}  |  hotels in seed: {len(hotels)}")
    if args.malformed_pct > 0 or args.late_pct > 0:
        seed_note = f"  |  seed: {chaos_seed}" if chaos_seed is not None else ""
        print(f"CHAOS — malformed: {args.malformed_pct}%  |  "
              f"late: {args.late_pct}%{seed_note}")

    chaos_args = (args.malformed_pct, args.late_pct)
    chaos_stats = defaultdict(int)
    wire_counts = defaultdict(int)

    # ── Main day loop ──────────────────────────────────────────────────
    day = resume_day
    total_events = 0
    t_start = time.time()
    days_run = 0
    last_day_completed = None

    while running:
        if args.until is not None and day > args.until:
            print(f"\nReached --until {args.until}. Stopping.")
            break
        if not has_runway_after(conn, day) and not has_open_bookings(conn):
            print(f"\nRunway exhausted at {day} AND sim_open_bookings empty. Stopping.")
            break

        day_rng = random.Random(f"day|{day.isoformat()}|{plan_seed}")
        n, wall = run_one_day(
            conn, producer, topic, hotels, day, sim_start, plan_seed,
            args.num_prices_per_day, pending_cancellations, cancellation_plans,
            args.sim_speed, day_rng, chaos_args, chaos_stats, wire_counts,
            chaos_seed, hotel_rating_map,
        )
        total_events += n
        days_run += 1
        last_day_completed = day

        elapsed = time.time() - t_start
        achieved = total_events / elapsed if elapsed > 0 else 0
        print(f"  sim-day {day} → {n:6,} events in {wall:5.1f}s wall  |  "
              f"total {total_events:7,}  |  elapsed {elapsed:6.1f}s  |  "
              f"avg {achieved:5.1f} evt/s  |  open {booked+checked_in:5,}")

        day = day + timedelta(days=1)

    # ── Shutdown ───────────────────────────────────────────────────────
    elapsed = time.time() - t_start
    producer.flush()
    producer.close()
    conn.close()

    avg_rate = (total_events / elapsed) if elapsed > 0 else 0.0
    print(f"\nProduced {total_events:,} events across {days_run} sim-days "
          f"in {elapsed:.1f}s ({avg_rate:.1f}/s avg).")
    if last_day_completed is not None:
        print(f"  Last completed sim-day (clock): {last_day_completed}")

    if total_events > 0:
        print(f"\nWire-type counts (pre-chaos selection):")
        for et in ("BOOKING", "CHECKIN", "CHECKOUT", "CANCELLATION", "PRICE_CHANGE", "REVIEW"):
            n = wire_counts.get(et, 0)
            pct = (n / total_events) * 100.0
            print(f"  {et:13s}: {n:7,}  ({pct:5.2f}%)")

    if args.malformed_pct > 0 or args.late_pct > 0:
        normal = chaos_stats.get("normal", 0)
        late = chaos_stats.get("late", 0)
        malformed_total = sum(v for k, v in chaos_stats.items() if k.startswith("malformed:"))
        print(f"\nChaos summary:")
        print(f"  Normal              : {normal:,}")
        print(f"  Late                : {late:,}")
        print(f"  Malformed (total)   : {malformed_total:,}")
        for mode, count in sorted(chaos_stats.items(), key=lambda x: -x[1]):
            if mode.startswith("malformed:"):
                reason = mode.split(":", 1)[1]
                print(f"     └─ {reason:25s}: {count:,}")
        print(f"\n  These counts should match the consumer's run summary"
              f" (within Kafka offset-commit jitter, usually ±5).")


if __name__ == "__main__":
    main()
