"""Phase 1 bulk loader — loads all 12 CSVs into Postgres in FK-safe order."""

import os
import sys
import time
from pathlib import Path

# Force UTF-8 stdout/stderr so checkmark characters render on Windows
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "host":     os.environ["POSTGRES_HOST"],
    "port":     int(os.environ["POSTGRES_PORT"]),
    "dbname":   os.environ["POSTGRES_DB"],
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}
DATA_DIR = Path(os.environ.get("DATA_DIR", "./data"))

# Load order: reference → dimension → fact (no agg CSVs exist)
LOAD_ORDER = [
    ("ref_price_tiers.csv",    "ref_price_tiers"),
    ("ref_state_centroids.csv","ref_state_centroids"),
    ("india_states_zones.csv", "india_states_zones"),
    ("public_holidays.csv",    "public_holidays"),
    ("dim_date.csv",           "dim_date"),
    ("dim_location.csv",       "dim_location"),
    ("dim_customer.csv",       "dim_customer"),
    ("hotel_master.csv",       "hotel_master"),
    ("dim_room_type.csv",      "dim_room_type"),
    ("fact_bookings.csv",      "fact_bookings"),
    ("fact_price_events.csv",  "fact_price_events"),
    ("reviews_raw.csv",        "reviews_raw"),
]

# All tables in truncate order (facts first, then dims, then refs)
TRUNCATE_ORDER = [
    "fact_bookings", "fact_price_events", "reviews_raw",
    "agg_hourly_city_stats", "agg_daily_hotel_kpi",
    "dim_room_type", "hotel_master",
    "dim_customer", "dim_location", "dim_date",
    "public_holidays", "india_states_zones",
    "ref_state_centroids", "ref_price_tiers",
]

POST_LOAD_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fact_bookings_hotel_date    ON fact_bookings (hotel_id, date_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_bookings_date          ON fact_bookings (date_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_bookings_customer      ON fact_bookings (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_price_events_hotel_ts  ON fact_price_events (hotel_id, event_ts)",
    "CREATE INDEX IF NOT EXISTS idx_reviews_raw_hotel           ON reviews_raw (hotel_id)",
    "CREATE INDEX IF NOT EXISTS idx_dim_room_type_hotel         ON dim_room_type (hotel_id)",
    "CREATE INDEX IF NOT EXISTS idx_hotel_master_location       ON hotel_master (location_id)",
    "CREATE INDEX IF NOT EXISTS idx_agg_hourly_city_window      ON agg_hourly_city_stats (city, window_start DESC)",
]


def load():
    t0 = time.time()
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Disable FK checks for bulk load
        cur.execute("SET session_replication_role = replica;")
        print("FK checks disabled.")

        # Truncate all tables (cascade handles cross-table deps)
        print("Truncating all tables …")
        for tbl in TRUNCATE_ORDER:
            cur.execute(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE;")
        print("  ✓ Truncate complete.")

        # Bulk load each CSV
        for csv_name, table in LOAD_ORDER:
            csv_path = DATA_DIR / csv_name
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing: {csv_path}")
            t1 = time.time()
            # reviews_raw has an extra 'embedding' column not present in the CSV
            if table == "reviews_raw":
                col_list = "(review_id, hotel_id, reviewer_name, review_text, rating, review_date, source, travel_type)"
                copy_sql = f"COPY {table} {col_list} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
            else:
                copy_sql = f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')"
            with open(csv_path, "r", encoding="utf-8") as fh:
                cur.copy_expert(copy_sql, fh)
            elapsed = time.time() - t1
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            rows = cur.fetchone()[0]
            print(f"  ✓ {table}: {rows:,} rows  ({elapsed:.1f}s)")

        # Re-enable FK checks — if any orphan exists this will raise
        print("Re-enabling FK checks …")
        cur.execute("SET session_replication_role = DEFAULT;")

        # Validate FKs by running a quick check
        cur.execute("""
            SELECT COUNT(*) FROM fact_bookings b
            LEFT JOIN hotel_master h ON b.hotel_id = h.hotel_id
            WHERE h.hotel_id IS NULL
        """)
        orphans = cur.fetchone()[0]
        if orphans:
            raise RuntimeError(f"FK violation: {orphans} bookings with no matching hotel")

        # Commit before creating indexes (indexes outside the data transaction)
        conn.commit()
        print("  ✓ FK checks passed. Data committed.")

        # Post-load indexes (outside main transaction for speed)
        conn.autocommit = True
        print("Creating indexes …")
        for ddl in POST_LOAD_INDEXES:
            idx_name = ddl.split("idx_")[1].split(" ")[0]
            cur.execute(ddl)
            print(f"  ✓ idx_{idx_name}")

        total = time.time() - t0
        print(f"\n✓ COMMIT — Phase 1 load complete  ({total:.1f}s total)")

    except Exception as exc:
        conn.rollback()
        print(f"\n✗ ERROR — rolled back: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    load()
