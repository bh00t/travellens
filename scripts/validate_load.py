"""Phase 1 integrity validator — 20 checks, exits 0 if all pass."""

import os
import sys
from dataclasses import dataclass
from typing import Callable

import psycopg2
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

load_dotenv()

DB_PARAMS = {
    "host":     os.environ["POSTGRES_HOST"],
    "port":     int(os.environ["POSTGRES_PORT"]),
    "dbname":   os.environ["POSTGRES_DB"],
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}


@dataclass
class Check:
    name:    str
    sql:     str
    pass_if: Callable
    detail:  str


CHECKS = [
    Check(
        name="ref_price_tiers count",
        sql="SELECT COUNT(*) FROM ref_price_tiers",
        pass_if=lambda v: v == 4,
        detail="expected 4",
    ),
    Check(
        name="public_holidays count",
        sql="SELECT COUNT(*) FROM public_holidays",
        pass_if=lambda v: v == 147,
        detail="expected 147",
    ),
    Check(
        name="dim_date count",
        sql="SELECT COUNT(*) FROM dim_date",
        pass_if=lambda v: v == 2557,
        detail="expected 2557",
    ),
    Check(
        name="dim_customer count",
        sql="SELECT COUNT(*) FROM dim_customer",
        pass_if=lambda v: v == 20000,
        detail="expected 20000",
    ),
    Check(
        name="hotel_master count",
        sql="SELECT COUNT(*) FROM hotel_master",
        pass_if=lambda v: v == 2000,
        detail="expected 2000",
    ),
    Check(
        name="dim_room_type count",
        sql="SELECT COUNT(*) FROM dim_room_type",
        pass_if=lambda v: v == 5542,
        detail="expected 5542",
    ),
    Check(
        name="fact_bookings count",
        sql="SELECT COUNT(*) FROM fact_bookings",
        pass_if=lambda v: v == 1000000,
        detail="expected 1000000",
    ),
    Check(
        name="fact_price_events count",
        sql="SELECT COUNT(*) FROM fact_price_events",
        pass_if=lambda v: v == 86650,
        detail="expected 86650",
    ),
    Check(
        name="reviews_raw count",
        sql="SELECT COUNT(*) FROM reviews_raw",
        pass_if=lambda v: v == 30000,
        detail="expected 30000",
    ),
    Check(
        name="Orphan bookings → hotels",
        sql="""
            SELECT COUNT(*) FROM fact_bookings b
            LEFT JOIN hotel_master h ON b.hotel_id = h.hotel_id
            WHERE h.hotel_id IS NULL
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 orphans",
    ),
    Check(
        name="Orphan bookings → customers",
        sql="""
            SELECT COUNT(*) FROM fact_bookings b
            LEFT JOIN dim_customer c ON b.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 orphans",
    ),
    Check(
        name="Orphan bookings → dim_date",
        sql="""
            SELECT COUNT(*) FROM fact_bookings b
            LEFT JOIN dim_date d ON b.date_id = d.date_id
            WHERE d.date_id IS NULL
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 orphans",
    ),
    Check(
        name="Orphan reviews → hotels",
        sql="""
            SELECT COUNT(*) FROM reviews_raw r
            LEFT JOIN hotel_master h ON r.hotel_id = h.hotel_id
            WHERE h.hotel_id IS NULL
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 orphans",
    ),
    Check(
        name="Orphan room_types → hotels",
        sql="""
            SELECT COUNT(*) FROM dim_room_type rt
            LEFT JOIN hotel_master h ON rt.hotel_id = h.hotel_id
            WHERE h.hotel_id IS NULL
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 orphans",
    ),
    Check(
        name="No negative revenue (non-cancelled)",
        sql="""
            SELECT COUNT(*) FROM fact_bookings
            WHERE revenue_inr <= 0 AND NOT is_cancelled
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 rows",
    ),
    Check(
        name="Cancellation rate in range",
        sql="""
            SELECT ROUND(
                100.0 * SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) / COUNT(*), 1
            ) FROM fact_bookings
        """,
        pass_if=lambda v: 8 <= float(v) <= 15,
        detail="expected 8.0–15.0 %",
    ),
    Check(
        name="Ratings in valid range",
        sql="""
            SELECT COUNT(*) FROM reviews_raw
            WHERE rating < 1.0 OR rating > 5.0
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 out-of-range ratings",
    ),
    Check(
        name="No embeddings yet (Phase 3 work)",
        sql="SELECT COUNT(*) FROM reviews_raw WHERE embedding IS NOT NULL",
        pass_if=lambda v: v == 0,
        detail="expected 0 populated embeddings",
    ),
    Check(
        name="All hotels linked to location",
        sql="""
            SELECT COUNT(*) FROM hotel_master h
            LEFT JOIN dim_location l ON h.location_id = l.location_id
            WHERE l.location_id IS NULL
        """,
        pass_if=lambda v: v == 0,
        detail="expected 0 orphans",
    ),
    Check(
        name="Total revenue in plausible range",
        sql="SELECT SUM(revenue_inr) / 10000000.0 FROM fact_bookings",
        pass_if=lambda v: 1800 <= float(v) <= 2500,
        detail="expected 1800–2500 crore INR",
    ),
]


def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    passed = 0
    failed = 0

    for i, chk in enumerate(CHECKS, start=1):
        cur.execute(chk.sql)
        value = cur.fetchone()[0]
        ok = chk.pass_if(value)
        marker = "✓" if ok else "✗"
        print(f"  {marker} [{i:02d}] {chk.name}: {value}  ({chk.detail})")
        if ok:
            passed += 1
        else:
            failed += 1

    conn.close()

    print()
    if failed == 0:
        print(f"PASSED: {passed} / {passed + failed}")
        sys.exit(0)
    else:
        print(f"FAILED: {failed} checks  (passed {passed} / {passed + failed})")
        sys.exit(1)


if __name__ == "__main__":
    main()
