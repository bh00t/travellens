"""
B-030a: Read-only review corpus diagnostics.

Prints health checks for reviews_raw with NO writes to DB or filesystem.
Safe to run at any time — use it as the "check reviews" command until
B-030b lights the monitor tiles.

Checks:
  1. Counts by record_source (seed / history / stream)
  2. Stage distribution (non-seed rows)
  3. Overall review rate = booking-tied reviews / eligible bookings
  4. hotel_id consistency vs fact_bookings (expect 0 mismatches)
  5. no_show stage count (expect 0 — stage is never emitted)
  6. Avg rating by hotel star_category (sentiment-anchor gradient)

Usage:
    python -m scripts.review_stats
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# Must match generate_review_backfill.py and generate_lifecycle_history.py
SIM_TODAY = "2025-06-01"


def run(conn):
    cur = conn.cursor()

    # -- 1. Counts by record_source --------------------------------------------
    cur.execute(
        "SELECT record_source, COUNT(*) FROM reviews_raw GROUP BY 1 ORDER BY 1"
    )
    source_rows = cur.fetchall()
    print("-- Review counts by record_source ----------------------------------")
    total_reviews = 0
    booking_tied  = 0
    for source, count in source_rows:
        print(f"  {source:<10}: {count:>8,}")
        total_reviews += count
        if source in ("history", "stream"):
            booking_tied += count
    print(f"  {'TOTAL':<10}: {total_reviews:>8,}")

    # -- 2. Stage distribution (non-seed) -------------------------------------
    cur.execute(
        """
        SELECT record_source, review_stage, COUNT(*)
        FROM   reviews_raw
        WHERE  record_source <> 'seed'
        GROUP  BY 1, 2
        ORDER  BY 1, 3 DESC
        """
    )
    stage_rows = cur.fetchall()
    print("\n-- Stage distribution (non-seed) -----------------------------------")
    if stage_rows:
        for source, stage, count in stage_rows:
            print(f"  {source:<10}  {(stage or 'NULL'):<15}: {count:>8,}")
    else:
        print("  (no non-seed rows yet)")

    # -- 3. Overall review rate ------------------------------------------------
    cur.execute(
        "SELECT COUNT(*) FROM fact_bookings WHERE booking_ts < %s",
        (SIM_TODAY,),
    )
    eligible = cur.fetchone()[0]
    rate = booking_tied / eligible * 100 if eligible else 0.0
    print("\n-- Overall review rate ---------------------------------------------")
    print(f"  Booking-tied reviews  : {booking_tied:>8,}  (history + stream)")
    print(f"  Eligible bookings     : {eligible:>8,}  (booking_ts < {SIM_TODAY})")
    print(f"  Overall review rate   : {rate:>8.1f}%")

    # -- 4. hotel_id consistency -----------------------------------------------
    cur.execute(
        """
        SELECT COUNT(*)
        FROM   reviews_raw r
        JOIN   fact_bookings b USING (booking_id)
        WHERE  r.hotel_id <> b.hotel_id
        """
    )
    mismatches = cur.fetchone()[0]
    status = "OK  0 mismatches" if mismatches == 0 else f"FAIL  {mismatches:,} MISMATCHES"
    print("\n-- hotel_id consistency (vs fact_bookings) -------------------------")
    print(f"  {status}")

    # -- 5. no_show stage count ------------------------------------------------
    cur.execute(
        "SELECT COUNT(*) FROM reviews_raw WHERE review_stage = 'no_show'"
    )
    no_show = cur.fetchone()[0]
    ns_status = "OK  0" if no_show == 0 else f"FAIL  {no_show:,} unexpected no_show rows"
    print("\n-- no_show stage count (expect 0) ----------------------------------")
    print(f"  {ns_status}")

    # -- 6. Avg rating by star_category ---------------------------------------
    cur.execute(
        """
        SELECT h.star_category,
               ROUND(AVG(r.rating), 2) AS avg_rating,
               COUNT(*)                AS reviews
        FROM   reviews_raw r
        JOIN   hotel_master h USING (hotel_id)
        WHERE  r.record_source <> 'seed'
        GROUP  BY 1
        ORDER  BY 1
        """
    )
    star_rows = cur.fetchall()
    print("\n-- Avg rating by hotel star_category (sentiment-anchor gradient) ---")
    prev_avg      = None
    gradient_ok   = True
    for star, avg_rat, count in star_rows:
        if prev_avg is not None and avg_rat < prev_avg:
            gradient_ok = False
            arrow = " FAIL (breaks gradient)"
        elif prev_avg is not None:
            arrow = " ^"
        else:
            arrow = ""
        print(f"  {star}★  avg={avg_rat}  n={count:>8,}{arrow}")
        prev_avg = avg_rat
    if star_rows:
        g_status = "OK  monotonically increasing" if gradient_ok else "FAIL  gradient broken"
        print(f"  Gradient: {g_status}")
    else:
        print("  (no non-seed rows yet)")

    cur.close()


def main():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True   # read-only; no transaction needed
    try:
        run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
