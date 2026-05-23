"""
populate_opened_year.py — one-off backfill for hotel_master.opened_year
======================================================================
Part of: TravelLens entity-count prompt fix (migration 006 follow-up)
File:    scripts/populate_opened_year.py

What this file does:
    Backfills the new `hotel_master.opened_year` column for all 2000 hotels
    with realistic synthetic values, then verifies two invariants the
    spec calls out:

      1. Every hotel has a non-NULL opened_year.
      2. For every hotel that has bookings, opened_year <= the earliest
         booking year (a hotel cannot be booked before it opened).

    The range is 1975-2023, correlated with star_category:

        star 5 (luxury, established)  → 1975-2010   mean ~1992
        star 4                         → 1980-2015
        star 3                         → 1990-2020
        star 2                         → 1995-2023
        star 1                         → 2000-2023
        star 0 (uncategorised)         → 2005-2023
        chain ILIKE 'oyo%' OVERRIDE   → 2010-2023   (OYO founded 2013)

    Then capped at the hotel's MIN(dim_date.year) from fact_bookings, if any.

    Seeded with a fixed RNG so re-runs produce the same values — important
    so the dashboard / DAG outputs are reproducible across environments.

Run with:
    python -m scripts.populate_opened_year

Idempotent — re-running produces identical values. The script unconditionally
overwrites opened_year. If you don't want that, gate the UPDATE on WHERE
opened_year IS NULL before re-running.
"""

import os
import random
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

SEED = 20260523  # fixed for reproducible synthetic data

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# Star → (min, max) opened-year range. Tighter at the high end (5-star
# luxury skews older) and looser at the low end (uncategorised / budget
# skew newer). These ranges OVERLAP intentionally — real-world hotel
# vintage doesn't cleanly partition by star rating.
STAR_RANGE: dict[int, tuple[int, int]] = {
    5: (1975, 2010),
    4: (1980, 2015),
    3: (1990, 2020),
    2: (1995, 2023),
    1: (2000, 2023),
    0: (2005, 2023),   # uncategorised — treat like budget
}

# OYO is a budget chain founded 2013. Independent of star_category, OYO
# properties shouldn't predate the chain's existence. Override applies
# AFTER the star-based range is chosen.
OYO_RANGE: tuple[int, int] = (2010, 2023)


def main() -> None:
    rng = random.Random(SEED)

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        # ── Step 1: pull every hotel + its earliest booking year (NULL if none) ─
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    h.hotel_id,
                    h.star_category,
                    h.chain_name,
                    MIN(d.year) AS earliest_booking_year
                FROM hotel_master h
                LEFT JOIN fact_bookings b ON b.hotel_id = h.hotel_id
                LEFT JOIN dim_date d      ON d.date_id  = b.date_id
                GROUP BY h.hotel_id, h.star_category, h.chain_name
            """)
            rows = cur.fetchall()

        print(f"Fetched {len(rows)} hotels.")

        # ── Step 2: compute opened_year per hotel ─────────────────────────
        updates: list[tuple[int, str]] = []   # (opened_year, hotel_id)
        no_booking_count = 0

        for hotel_id, star_category, chain_name, earliest_booking_year in rows:
            # Default range from star_category (treat NULL star like 0)
            low, high = STAR_RANGE[star_category or 0]

            # OYO override
            if chain_name and chain_name.lower().startswith("oyo"):
                low, high = OYO_RANGE

            # Hard cap from earliest booking year, if any
            if earliest_booking_year is not None:
                # If the band's low end already exceeds the cap (rare with
                # 2024 minimum here, but defensive), clamp to a one-year
                # range ending at the cap.
                if low > earliest_booking_year:
                    low = earliest_booking_year
                high = min(high, earliest_booking_year)
            else:
                no_booking_count += 1

            opened_year = rng.randint(low, high)
            updates.append((opened_year, hotel_id))

        print(
            f"Computed: {len(updates)} years. "
            f"Hotels without bookings (no cap applied): {no_booking_count}."
        )

        # ── Step 3: batch UPDATE ──────────────────────────────────────────
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                UPDATE hotel_master AS h
                   SET opened_year = v.opened_year
                  FROM (VALUES %s) AS v(opened_year, hotel_id)
                 WHERE h.hotel_id = v.hotel_id
                """,
                updates,
                template="(%s::SMALLINT, %s)",
                page_size=500,
            )
        conn.commit()
        print(f"Updated {len(updates)} rows.")

        # ── Step 4: verify invariants ─────────────────────────────────────
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM hotel_master WHERE opened_year IS NULL")
            null_count = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM hotel_master h
                JOIN (
                    SELECT b.hotel_id, MIN(d.year) AS first_year
                    FROM fact_bookings b
                    JOIN dim_date d ON d.date_id = b.date_id
                    GROUP BY b.hotel_id
                ) earliest ON earliest.hotel_id = h.hotel_id
                WHERE h.opened_year > earliest.first_year
            """)
            violations = cur.fetchone()[0]

            cur.execute("""
                SELECT (opened_year / 10) * 10 AS decade, COUNT(*)
                FROM hotel_master
                GROUP BY (opened_year / 10) * 10
                ORDER BY decade
            """)
            decades = cur.fetchall()

        print()
        print(f"INVARIANT — null opened_year:                       {null_count}  (expected 0)")
        print(f"INVARIANT — opened_year > earliest_booking_year:    {violations}  (expected 0)")
        print()
        print("Distribution by decade:")
        for decade, n in decades:
            print(f"  {decade}s: {n}")

        if null_count != 0 or violations != 0:
            print("INVARIANT VIOLATED — exiting non-zero")
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
