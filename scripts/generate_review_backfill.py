"""
B-030: History review backfill.

Sweeps all fact_bookings WHERE booking_ts < SIM_TODAY (default 2025-06-01),
derives each booking's lifecycle status, and calls make_review_event_dict()
from scripts.review_generator to decide whether to emit a review.

Design choices:
  - Idempotent: review_id is uuid5(REVIEW_NS, booking_id) — same booking
    always produces the same review_id; ON CONFLICT (review_id) DO NOTHING
    ignores re-runs.
  - --reset deletes ALL record_source='history' rows before re-running so
    you can regenerate cleanly after changing generation parameters.
  - event_ts is set to midnight UTC on the event_date (approximate; history
    reviews have no wall-clock origin).
  - Kaggle seed texts (record_source='seed') are bucketed low/mid/high and
    passed to pick_text() for the 40 % sampled + 25 % blended modes.
  - Batch INSERT with page_size 1000; progress log every 10 000 bookings.

Usage:
    python -m scripts.generate_review_backfill
    python -m scripts.generate_review_backfill --reset            # wipe + regenerate
    python -m scripts.generate_review_backfill --sim-today 2025-07-01
    python -m scripts.generate_review_backfill --dry-run          # count only
"""

import argparse
import sys
from datetime import date, datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

# ── DB connection (mirrors stream_consumer pattern) ───────────────────────────
import os
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

SIM_TODAY_DEFAULT = date(2025, 6, 1)
BATCH_SIZE        = 1000
LOG_EVERY         = 10_000


def _load_seed_texts(conn):
    """
    Bucket the 30 K Kaggle reviews (record_source='seed') into low/mid/high
    by integer rating so pick_text() can sample realistic text.
    Returns dict with keys 'low', 'mid', 'high'.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT rating, review_text
        FROM   reviews_raw
        WHERE  record_source = 'seed'
          AND  review_text IS NOT NULL
          AND  rating      IS NOT NULL
        """
    )
    buckets = {"low": [], "mid": [], "high": []}
    for rating, text in cur.fetchall():
        r = int(rating)
        if   r <= 2: buckets["low"].append(text)
        elif r == 3: buckets["mid"].append(text)
        else:        buckets["high"].append(text)
    cur.close()
    return buckets


def _derive_lifecycle_status(row, sim_today):
    """
    Infer lifecycle_status from fact_bookings columns vs sim_today.

    Mirrors the logic in generate_lifecycle_history.py:
      CANCELLED   — is_cancelled is True
      COMPLETED   — checkout_date < sim_today and not cancelled
      IN_PROGRESS — checkin_date <= sim_today < checkout_date (and not cancelled)
      BOOKED      — checkin_date > sim_today (and not cancelled)

    'FUTURE' rows (booking_ts >= sim_today) are excluded by the WHERE
    clause in _stream_bookings(), so this function never sees them.
    """
    is_cancelled  = row["is_cancelled"]
    checkin_date  = row["checkin_date"]
    checkout_date = row["checkout_date"]

    if is_cancelled:
        return "CANCELLED"
    if checkout_date < sim_today:
        return "COMPLETED"
    if checkin_date <= sim_today:
        return "IN_PROGRESS"
    return "BOOKED"


def _stream_bookings(conn, sim_today):
    """
    Stream all bookings with booking_ts < sim_today in chunks.
    Yields one dict per row.
    """
    FETCH_SIZE = 5000
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            fb.booking_id,
            fb.customer_id,
            fb.hotel_id,
            fb.booking_source,
            fb.booking_ts::date   AS booking_ts_date,
            fb.checkin_date,
            fb.checkout_date,
            fb.is_cancelled,
            hm.avg_rating,
            hm.star_category
        FROM   fact_bookings   fb
        JOIN   hotel_master    hm ON hm.hotel_id = fb.hotel_id
        WHERE  fb.booking_ts < %s
        ORDER  BY fb.booking_ts
        """,
        (sim_today,),
    )
    cols = [d[0] for d in cur.description]
    while True:
        rows = cur.fetchmany(FETCH_SIZE)
        if not rows:
            break
        for raw in rows:
            yield dict(zip(cols, raw))
    cur.close()


def run_backfill(args):
    from scripts.review_generator import make_review_event_dict

    sim_today = args.sim_today
    dry_run   = args.dry_run

    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False

    if args.reset and not dry_run:
        print("  --reset: deleting existing record_source='history' reviews …", flush=True)
        cur = conn.cursor()
        cur.execute("DELETE FROM reviews_raw WHERE record_source = 'history'")
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        print(f"  Deleted {deleted:,} history rows.", flush=True)

    print("  Loading Kaggle seed texts for blending …", flush=True)
    seed_texts = _load_seed_texts(conn)
    total_seed = sum(len(v) for v in seed_texts.values())
    print(f"  Seed texts: low={len(seed_texts['low'])}, "
          f"mid={len(seed_texts['mid'])}, high={len(seed_texts['high'])} "
          f"(total {total_seed:,})", flush=True)

    print(f"  Streaming bookings with booking_ts < {sim_today} …", flush=True)

    batch         = []
    total_seen    = 0
    total_reviews = 0
    total_skipped = 0

    def _flush_batch(b):
        nonlocal total_reviews
        if not b:
            return
        rows = []
        for ev in b:
            from datetime import datetime as _dt, timezone as _tz
            # Approximate event_ts: midnight UTC on the event_date.
            ed = ev["event_date"]           # already an ISO date string
            event_ts = _dt.fromisoformat(f"{ed}T00:00:00+00:00").isoformat()
            rows.append((
                ev["review_id"],
                ev["hotel_id"],
                None,                       # reviewer_name
                ev.get("review_text"),
                ev["rating"],
                ev["event_date"],           # review_date
                ev.get("review_channel"),   # source
                None,                       # travel_type
                None,                       # embedding
                ev["booking_id"],
                ev["customer_id"],
                ev["review_stage"],
                ev["review_channel"],
                event_ts,                   # event_ts
                ev["event_date"],           # event_date
                "history",                  # record_source
            ))
        ins_cur = conn.cursor()
        execute_values(
            ins_cur,
            """
            INSERT INTO reviews_raw
                (review_id, hotel_id, reviewer_name, review_text, rating,
                 review_date, source, travel_type, embedding,
                 booking_id, customer_id, review_stage, review_channel,
                 event_ts, event_date, record_source)
            VALUES %s
            ON CONFLICT (review_id) DO NOTHING
            """,
            rows,
            page_size=BATCH_SIZE,
        )
        inserted = ins_cur.rowcount
        conn.commit()
        ins_cur.close()
        total_reviews += inserted

    # Use a separate streaming connection so the backfill INSERT
    # doesn't share the same transaction as the SELECT cursor.
    # autocommit=False is required for named (server-side) cursors.
    read_conn = psycopg2.connect(**DB_PARAMS)
    read_conn.autocommit = False

    for row in _stream_bookings(read_conn, sim_today):
        total_seen += 1

        lifecycle = _derive_lifecycle_status(row, sim_today)
        ev = make_review_event_dict(
            booking_id        = row["booking_id"],
            customer_id       = row["customer_id"],
            hotel_id          = row["hotel_id"],
            booking_source    = row["booking_source"],
            hotel_avg_rating  = float(row["avg_rating"] or 3.0),
            star_category     = row["star_category"],
            lifecycle_status  = lifecycle,
            sim_day           = sim_today,
            booking_ts_date   = row["booking_ts_date"],
            checkin_date      = row["checkin_date"],
            checkout_date     = row["checkout_date"],
            seed_texts_by_bucket = seed_texts,
        )

        if ev is None:
            total_skipped += 1
        else:
            batch.append(ev)
            if len(batch) >= BATCH_SIZE and not dry_run:
                _flush_batch(batch)
                batch = []

        if total_seen % LOG_EVERY == 0:
            print(f"  … {total_seen:,} bookings processed, "
                  f"{total_reviews:,} reviews inserted so far", flush=True)

    read_conn.close()

    if not dry_run:
        _flush_batch(batch)

    conn.close()

    review_rate = total_reviews / total_seen * 100 if total_seen else 0
    print(f"\nBackfill complete.")
    print(f"  Bookings scanned  : {total_seen:,}")
    print(f"  Reviews inserted  : {total_reviews:,}  ({review_rate:.1f}%)")
    print(f"  Reviews skipped   : {total_skipped:,}  (negativity-bias draw)")
    if dry_run:
        print("  DRY RUN — nothing written.")


def main():
    parser = argparse.ArgumentParser(description="B-030 history review backfill")
    parser.add_argument(
        "--sim-today", type=date.fromisoformat,
        default=SIM_TODAY_DEFAULT,
        help="Sim anchor date (default 2025-06-01); backfills bookings BEFORE this date",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete all record_source='history' rows before re-running",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count only; do not write to DB",
    )
    args = parser.parse_args()
    run_backfill(args)


if __name__ == "__main__":
    main()
