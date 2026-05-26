"""Gold lifecycle updater for TravelLens (B-040).

Runs as a continuous ~1-minute micro-batch process, decoupled from the consumer
so gold latency never back-pressures the silver accept path.

Each iteration:
  1. Reads new rows from fact_booking_events WHERE ingest_seq > watermark,
     ordered by ingest_seq, in bounded batches of GOLD_BATCH_SIZE.
  2. Applies each event FORWARD-ONLY to fact_booking_lifecycle (one row per
     booking_id), following the lifecycle state machine:
       BOOKING      → status BOOKED,      seeds booking facts
       CHECKIN      → status CHECKED_IN,  records checkin_ts / checkin_date
       CHECKOUT     → status COMPLETED,   records checkout_ts / checkout_date
       CANCELLATION → status CANCELLED,   records reason, outcome = cancelled|no_show
       PRICE_CHANGE → SKIP  (no booking_id; excluded by the query filter)
       REVIEW       → SKIP  (hotel-level event, not a per-booking lifecycle step)
  3. Out-of-order arrivals set illegal_transition_flag but never regress status.
  4. Persists the watermark (last processed ingest_seq) atomically with the
     UPSERT so the updater is resumable with no double-processing.

Status machine ranks:
    BOOKED=0  CHECKED_IN=1  COMPLETED=2  CANCELLED=2

Forward-only rule: apply if target_rank > current_rank; flag if out-of-order;
never move to a lower-ranked state.

source_mix tracks provenance:
    history  = all contributing events have source='history'
    stream   = all contributing events have source='stream'
    mixed    = both sources contributed (expected for open-backlog bookings
               whose BOOKING/CHECKIN landed as history and CHECKOUT later
               arrived from the live calendar-replay stream)

Run with:
    python -m scripts.gold_lifecycle_updater

Config (all env-overridable):
    GOLD_INTERVAL_SECONDS   default 60   — sleep between caught-up polls
    GOLD_BATCH_SIZE         default 5000 — silver rows per batch iteration
"""

import os
import sys
import time
import logging
from collections import defaultdict

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Register UUID codec so psycopg2 returns uuid.UUID objects for UUID columns
# and encodes Python uuid.UUID / lists-of-uuid.UUID correctly (avoids the
# "operator does not exist: uuid = text" error on ANY(%s) against a uuid column).
psycopg2.extras.register_uuid()

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

GOLD_INTERVAL_SECONDS = int(os.getenv("GOLD_INTERVAL_SECONDS", "60"))
GOLD_BATCH_SIZE       = int(os.getenv("GOLD_BATCH_SIZE",       "5000"))

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# ── Status model ──────────────────────────────────────────────────────────────

STATUS_RANK = {"BOOKED": 0, "CHECKED_IN": 1, "COMPLETED": 2, "CANCELLED": 2}

# Fixed integer key for pg_try_advisory_lock — unique to this process.
# If another instance already holds it, we exit rather than deadlock.
GOLD_ADVISORY_LOCK_KEY = 7_400_040

# Maps event_type to the lifecycle status it produces
EVENT_TO_STATUS = {
    "BOOKING":      "BOOKED",
    "CHECKIN":      "CHECKED_IN",
    "CHECKOUT":     "COMPLETED",
    "CANCELLATION": "CANCELLED",
}

# Which prior statuses are "expected" before each advancing event type.
# An event arriving with the booking NOT in the expected prior status is
# out-of-order and will set illegal_transition_flag.
EXPECTED_PRIOR = {
    "CHECKIN":      frozenset({"BOOKED"}),
    "CHECKOUT":     frozenset({"CHECKED_IN"}),
    "CANCELLATION": frozenset({"BOOKED", "CHECKED_IN"}),
}

# Apply lifecycle events in this order within each booking_id for a given batch,
# regardless of ingest_seq, so insertion-order quirks in the history backfill
# cannot produce false illegal_transition_flag positives.
_LIFECYCLE_SORT_KEY = {
    "BOOKING": 0, "CHECKIN": 1, "CHECKOUT": 2, "CANCELLATION": 2,
    "PRICE_CHANGE": 9, "REVIEW": 9,
}

# ── Lifecycle row helpers ─────────────────────────────────────────────────────


def _empty_row(booking_id) -> dict:
    """Return a zeroed lifecycle dict for a booking not yet in gold."""
    return {
        "booking_id":              booking_id,
        "customer_id":             None,
        "hotel_id":                None,
        "room_type_id":            None,
        "city":                    None,
        "booking_ts":              None,
        "booking_date":            None,
        "checkin_ts":              None,
        "checkin_date":            None,
        "checkout_ts":             None,
        "checkout_date":           None,
        "cancellation_ts":         None,
        "cancellation_date":       None,
        "nights":                  None,
        "num_guests":              None,
        "nightly_rate_inr":        None,
        "revenue_inr":             None,
        "booking_source":          None,
        "payment_mode":            None,
        "cancellation_reason":     None,
        "current_status":          "BOOKED",
        "outcome":                 None,
        "illegal_transition_flag": False,
        "source_mix":              None,
        "event_count":             0,
        "first_event_ts":          None,
        "last_event_ts":           None,
        # Internal keys stripped before upsert:
        "_seeded":                 False,  # True once the BOOKING event has been applied
        "_sources":                set(),  # source values seen in this batch for this booking
    }


def _apply_event(row: dict, event: dict) -> None:
    """Apply one silver event to a lifecycle dict in-place. Forward-only."""
    et = event["event_type"]
    if et not in EVENT_TO_STATUS:
        return  # PRICE_CHANGE, REVIEW — excluded by query but guard defensively

    # Accumulate event tracking
    row["event_count"] += 1
    ev_ts = event["event_ts"]
    if row["first_event_ts"] is None:
        row["first_event_ts"] = ev_ts
    if row["last_event_ts"] is None or ev_ts > row["last_event_ts"]:
        row["last_event_ts"] = ev_ts

    if et == "BOOKING":
        if row["_seeded"]:
            # Duplicate BOOKING for this booking_id — flag if status already advanced
            if row["current_status"] != "BOOKED":
                row["illegal_transition_flag"] = True
            return
        # Seed the row with booking facts. Do NOT regress status if a CHECKIN/
        # CHECKOUT/CANCELLATION event already arrived first (insertion-order
        # artefact from the history bulk INSERT).
        row.update({
            "customer_id":      event.get("customer_id"),
            "hotel_id":         event.get("hotel_id"),
            "room_type_id":     event.get("room_type_id"),
            "city":             event.get("city"),
            "booking_ts":       ev_ts,
            "booking_date":     event.get("event_date"),
            "nights":           event.get("nights"),
            "num_guests":       event.get("num_guests"),
            "nightly_rate_inr": event.get("nightly_rate_inr"),
            "revenue_inr":      event.get("revenue_inr"),
            "booking_source":   event.get("booking_source"),
            "payment_mode":     event.get("payment_mode"),
            "_seeded":          True,
        })
        # Only set BOOKED status if not already advanced; always set outcome if
        # not yet determined.
        if STATUS_RANK.get(row["current_status"], 0) == 0:
            row["current_status"] = "BOOKED"
            row["outcome"] = "in_progress"
        return

    # ── Non-BOOKING events ────────────────────────────────────────────────────

    target_status = EVENT_TO_STATUS[et]
    target_rank   = STATUS_RANK[target_status]
    current_rank  = STATUS_RANK.get(row["current_status"], 0)

    # Flag illegal ONLY for genuine business-domain inversions (event_ts contradicts
    # recorded lifecycle timestamps). Processing-order artifacts (e.g. a CHECKOUT
    # event getting a lower ingest_seq than its BOOKING because the history generator
    # wrote event types in separate bulk INSERT passes) must NOT set this flag —
    # those are queue artifacts, not data quality issues.
    if et == "CHECKIN":
        # Illegal if event_ts is before booking_ts (physically impossible)
        bts = row.get("booking_ts")
        if bts is not None and ev_ts < bts:
            row["illegal_transition_flag"] = True
    elif et == "CHECKOUT":
        # Illegal if event_ts is before checkin_ts (checked out before checking in),
        # or if no CHECKIN was ever seen for a booking we definitely know about.
        # Guard with _seeded so cross-batch artifacts (CHECKOUT ingest_seq < BOOKING
        # ingest_seq) don't fire when BOOKING simply hasn't arrived yet.
        cts = row.get("checkin_ts")
        if cts is not None and ev_ts < cts:
            row["illegal_transition_flag"] = True
        elif cts is None and row["_seeded"]:
            row["illegal_transition_flag"] = True
        # Also illegal if a CANCELLATION was already recorded (mutually exclusive outcomes)
        if row.get("cancellation_ts") is not None:
            row["illegal_transition_flag"] = True
    elif et == "CANCELLATION":
        # Illegal if a CHECKOUT was already recorded (booking was already completed)
        if row.get("checkout_ts") is not None:
            row["illegal_transition_flag"] = True

    if et == "CHECKIN":
        # Always record the timestamp when first seen, even if status can't advance
        if row["checkin_ts"] is None:
            row["checkin_ts"]   = ev_ts
            row["checkin_date"] = event.get("event_date")
        if target_rank > current_rank:
            row["current_status"] = "CHECKED_IN"
            row["outcome"]        = "in_progress"

    elif et == "CHECKOUT":
        if row["checkout_ts"] is None:
            row["checkout_ts"]   = ev_ts
            row["checkout_date"] = event.get("event_date")
        if target_rank > current_rank:
            row["current_status"] = "COMPLETED"
            row["outcome"]        = "completed"

    elif et == "CANCELLATION":
        reason = event.get("cancellation_reason") or "customer_cancelled"
        if row["cancellation_ts"] is None:
            row["cancellation_ts"]     = ev_ts
            row["cancellation_date"]   = event.get("event_date")
            row["cancellation_reason"] = reason
        if target_rank > current_rank:
            row["current_status"] = "CANCELLED"
            row["outcome"] = "no_show" if reason == "no_show" else "cancelled"


def _merge_source_mix(existing_mix: str | None, batch_sources: set) -> str:
    """Combine existing source_mix with sources seen in the current batch."""
    if existing_mix == "mixed":
        return "mixed"
    all_src = set(batch_sources)
    if existing_mix:
        all_src.add(existing_mix)
    if "history" in all_src and "stream" in all_src:
        return "mixed"
    return next(iter(all_src), "stream")


# ── SQL ───────────────────────────────────────────────────────────────────────

_READ_BATCH_SQL = """
    SELECT event_type, booking_id, customer_id, hotel_id, city, room_type_id,
           event_ts, event_date, nights, num_guests,
           nightly_rate_inr, revenue_inr, booking_source, payment_mode,
           cancellation_reason, source, ingest_seq
    FROM   fact_booking_events
    WHERE  ingest_seq > %s
      AND  event_type NOT IN ('PRICE_CHANGE', 'REVIEW')
      AND  booking_id IS NOT NULL
    ORDER  BY ingest_seq
    LIMIT  %s
"""

_READ_EXISTING_SQL = """
    SELECT booking_id, customer_id, hotel_id, room_type_id, city,
           booking_ts, booking_date,
           checkin_ts, checkin_date,
           checkout_ts, checkout_date,
           cancellation_ts, cancellation_date,
           nights, num_guests, nightly_rate_inr, revenue_inr,
           booking_source, payment_mode, cancellation_reason,
           current_status, outcome, illegal_transition_flag,
           source_mix, event_count, first_event_ts, last_event_ts
    FROM   fact_booking_lifecycle
    WHERE  booking_id = ANY(%s)
"""

# execute_values placeholder — the VALUES %s is replaced by psycopg2
_UPSERT_SQL = """
    INSERT INTO fact_booking_lifecycle (
        booking_id, customer_id, hotel_id, room_type_id, city,
        booking_ts,      booking_date,
        checkin_ts,      checkin_date,
        checkout_ts,     checkout_date,
        cancellation_ts, cancellation_date,
        nights, num_guests, nightly_rate_inr, revenue_inr,
        booking_source, payment_mode, cancellation_reason,
        current_status, outcome, illegal_transition_flag,
        source_mix, event_count, first_event_ts, last_event_ts, last_updated
    ) VALUES %s
    ON CONFLICT (booking_id) DO UPDATE SET
        -- Dimension references: use COALESCE so a BOOKING arriving in a later
        -- batch does not overwrite values set by an earlier CHECKIN/CHECKOUT.
        customer_id          = COALESCE(EXCLUDED.customer_id,      fact_booking_lifecycle.customer_id),
        hotel_id             = COALESCE(EXCLUDED.hotel_id,         fact_booking_lifecycle.hotel_id),
        room_type_id         = COALESCE(EXCLUDED.room_type_id,     fact_booking_lifecycle.room_type_id),
        city                 = COALESCE(EXCLUDED.city,             fact_booking_lifecycle.city),
        -- Stage timestamps: also COALESCE-safe
        booking_ts           = COALESCE(EXCLUDED.booking_ts,       fact_booking_lifecycle.booking_ts),
        booking_date         = COALESCE(EXCLUDED.booking_date,     fact_booking_lifecycle.booking_date),
        checkin_ts           = COALESCE(EXCLUDED.checkin_ts,       fact_booking_lifecycle.checkin_ts),
        checkin_date         = COALESCE(EXCLUDED.checkin_date,     fact_booking_lifecycle.checkin_date),
        checkout_ts          = COALESCE(EXCLUDED.checkout_ts,      fact_booking_lifecycle.checkout_ts),
        checkout_date        = COALESCE(EXCLUDED.checkout_date,    fact_booking_lifecycle.checkout_date),
        cancellation_ts      = COALESCE(EXCLUDED.cancellation_ts,  fact_booking_lifecycle.cancellation_ts),
        cancellation_date    = COALESCE(EXCLUDED.cancellation_date, fact_booking_lifecycle.cancellation_date),
        cancellation_reason  = COALESCE(EXCLUDED.cancellation_reason, fact_booking_lifecycle.cancellation_reason),
        -- Booking facts (only in BOOKING events)
        nights               = COALESCE(EXCLUDED.nights,           fact_booking_lifecycle.nights),
        num_guests           = COALESCE(EXCLUDED.num_guests,       fact_booking_lifecycle.num_guests),
        nightly_rate_inr     = COALESCE(EXCLUDED.nightly_rate_inr, fact_booking_lifecycle.nightly_rate_inr),
        revenue_inr          = COALESCE(EXCLUDED.revenue_inr,      fact_booking_lifecycle.revenue_inr),
        booking_source       = COALESCE(EXCLUDED.booking_source,   fact_booking_lifecycle.booking_source),
        payment_mode         = COALESCE(EXCLUDED.payment_mode,     fact_booking_lifecycle.payment_mode),
        -- State machine fields — Python is the authoritative engine
        current_status           = EXCLUDED.current_status,
        outcome                  = EXCLUDED.outcome,
        illegal_transition_flag  = EXCLUDED.illegal_transition_flag,
        source_mix               = EXCLUDED.source_mix,
        event_count              = EXCLUDED.event_count,
        first_event_ts           = EXCLUDED.first_event_ts,
        last_event_ts            = EXCLUDED.last_event_ts,
        last_updated             = now()
"""

_GET_WATERMARK_SQL = "SELECT last_ingest_seq FROM gold_watermark WHERE id = 1"
_SET_WATERMARK_SQL = (
    "UPDATE gold_watermark SET last_ingest_seq = %s, updated_at = now() WHERE id = 1"
)


# ── Batch processing ──────────────────────────────────────────────────────────


def _row_to_tuple(row: dict) -> tuple:
    """Convert a lifecycle dict to the column-ordered INSERT tuple."""
    from datetime import datetime, timezone
    return (
        row["booking_id"],
        row.get("customer_id"),
        row.get("hotel_id"),
        row.get("room_type_id"),
        row.get("city"),
        row.get("booking_ts"),
        row.get("booking_date"),
        row.get("checkin_ts"),
        row.get("checkin_date"),
        row.get("checkout_ts"),
        row.get("checkout_date"),
        row.get("cancellation_ts"),
        row.get("cancellation_date"),
        row.get("nights"),
        row.get("num_guests"),
        row.get("nightly_rate_inr"),
        row.get("revenue_inr"),
        row.get("booking_source"),
        row.get("payment_mode"),
        row.get("cancellation_reason"),
        row.get("current_status", "BOOKED"),
        row.get("outcome"),
        row.get("illegal_transition_flag", False),
        row.get("source_mix"),
        row.get("event_count", 0),
        row.get("first_event_ts"),
        row.get("last_event_ts"),
        datetime.now(timezone.utc),
    )


def run_once(conn) -> int:
    """One micro-batch: read silver → upsert gold → advance watermark.

    Returns the number of silver events processed (0 = caught up).
    Commits atomically; rolls back on any error.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # ── Read watermark ────────────────────────────────────────────────────
        cur.execute(_GET_WATERMARK_SQL)
        row = cur.fetchone()
        watermark = row["last_ingest_seq"] if row else 0

        # ── Read next batch from silver ───────────────────────────────────────
        cur.execute(_READ_BATCH_SQL, (watermark, GOLD_BATCH_SIZE))
        events = cur.fetchall()
        if not events:
            return 0

        max_seq     = events[-1]["ingest_seq"]
        booking_ids = list({ev["booking_id"] for ev in events})

        # ── Fetch existing lifecycle rows for affected booking_ids ─────────────
        cur.execute(_READ_EXISTING_SQL, (booking_ids,))
        existing = {r["booking_id"]: dict(r) for r in cur.fetchall()}

    # ── Build / update lifecycle rows in Python ───────────────────────────────
    # Group events by booking_id; sort within each group by lifecycle order
    # (BOOKING < CHECKIN < CHECKOUT/CANCELLATION) so insertion-order artefacts
    # from the history bulk INSERT cannot produce false illegal_transition_flag.
    grouped: dict = defaultdict(list)
    for ev in events:
        grouped[ev["booking_id"]].append(dict(ev))

    lifecycle: dict = {}
    for bid, book_events in grouped.items():
        book_events.sort(
            key=lambda e: (_LIFECYCLE_SORT_KEY.get(e["event_type"], 9), e["ingest_seq"])
        )
        if bid in existing:
            r = dict(existing[bid])
            r["_seeded"]  = r.get("booking_ts") is not None
            r["_sources"] = set()
            r["illegal_transition_flag"] = bool(r.get("illegal_transition_flag", False))
        else:
            r = _empty_row(bid)
        lifecycle[bid] = r

        for ev in book_events:
            r["_sources"].add(ev["source"])
            _apply_event(r, ev)

    # Finalise source_mix and strip internal keys before upsert
    for bid, r in lifecycle.items():
        batch_sources = r.pop("_sources")
        r.pop("_seeded", None)
        existing_mix  = existing.get(bid, {}).get("source_mix")
        r["source_mix"] = _merge_source_mix(existing_mix, batch_sources)

    # ── Batch upsert + watermark advance (single commit) ─────────────────────
    tuples = [_row_to_tuple(r) for r in lifecycle.values()]
    with conn.cursor() as wcur:
        psycopg2.extras.execute_values(wcur, _UPSERT_SQL, tuples, page_size=len(tuples))
        wcur.execute(_SET_WATERMARK_SQL, (max_seq,))

    conn.commit()
    return len(events)


# ── Main loop ─────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [gold] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("gold")
    log.info(
        "Gold lifecycle updater starting — interval %ds, batch %d",
        GOLD_INTERVAL_SECONDS, GOLD_BATCH_SIZE,
    )

    conn = psycopg2.connect(**DB_PARAMS)

    with conn.cursor() as _cur:
        _cur.execute("SELECT pg_try_advisory_lock(%s)", (GOLD_ADVISORY_LOCK_KEY,))
        _lock_acquired = _cur.fetchone()[0]
    if not _lock_acquired:
        log.error(
            "Another gold_lifecycle_updater instance holds the advisory lock "
            "(key=%d) — exiting to prevent deadlock. Kill stale instances first.",
            GOLD_ADVISORY_LOCK_KEY,
        )
        conn.close()
        sys.exit(1)
    log.info("Advisory lock acquired (key=%d).", GOLD_ADVISORY_LOCK_KEY)

    try:
        while True:
            try:
                # Drain all available events before sleeping
                total = 0
                while True:
                    n = run_once(conn)
                    if n == 0:
                        break
                    total += n
                    log.info("Batch: %d events → %d total this pass", n, total)
                    if n < GOLD_BATCH_SIZE:
                        break  # caught up within this batch
                if total:
                    log.info("Pass complete: %d events processed", total)
                else:
                    log.debug("No new events — sleeping %ds", GOLD_INTERVAL_SECONDS)
                time.sleep(GOLD_INTERVAL_SECONDS)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.error("Error in batch: %s — rolling back, retrying in 10s", exc, exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(10)
    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
