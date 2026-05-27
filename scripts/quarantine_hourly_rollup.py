"""
quarantine_hourly_rollup.py — B-044
=====================================
Background process that rolls up MinIO quarantine counts into
quarantine_hourly_summary (migration 013), one row per UTC hour.

Design
------
  Watermark   Derived from MAX(summary_date, summary_hour) WHERE is_final=TRUE.
              No separate cursor table.

  Grace       GRACE_MINUTES=10.  An hour is sealed (is_final=TRUE) only once
              now_utc >= end_of_hour + GRACE.  The current hour and any hour
              still inside the grace window stay is_final=FALSE and are
              re-counted next cycle, so a consumer burst that arrives just
              before the hour boundary isn't missed.

  S3 count    list_objects_v2 with Prefix=day/hour path.  KeyCount is summed
              across pages — the key list is never materialised (RAM-safe).
              Re-counts from S3 every cycle; never increments stale values.

  Empty hours Explicit 0/0 is_final=TRUE rows are written so the watermark
              advances contiguously and empty hours are never re-probed.

  Crash-safe  Only is_final=TRUE rows advance the watermark; a partial write
              is always re-counted at the next restart.

  Backfill    First run on an empty table walks the year=/month=/day=/hour=
              virtual dirs to find the earliest quarantine hour, then
              processes every hour forward to now.

  Singleton   pg_try_advisory_lock(7400060).  Second instance exits code 1.

  Eager start First rollup cycle runs immediately on startup so the /monitor
              tiles are populated before the first 5-min sleep.

Run
---
    python -m scripts.quarantine_hourly_rollup
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

LOCK_KEY         = 7400060
CYCLE_SECONDS    = 300           # 5-minute loop
GRACE_MINUTES    = 10

S3_BUCKET        = os.getenv("S3_BUCKET",             "travellens-data")
PREFIX_MALFORMED = os.getenv("S3_PREFIX_MALFORMED",   "malformed_events/")
PREFIX_LATE      = os.getenv("S3_PREFIX_LATE",        "late_events/")


# ── S3 / DB helpers ───────────────────────────────────────────────────────────

def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(connect_timeout=5, read_timeout=30,
                      retries={"max_attempts": 1}),
    )


def _get_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST",     "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB",     "travellens"),
        user=os.getenv("POSTGRES_USER",     "travellens"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# ── S3 virtual-dir walk ───────────────────────────────────────────────────────

def _list_common_prefixes(s3, bucket, prefix):
    """Return all virtual sub-directory prefixes (delimiter='/') under prefix."""
    out = []
    kwargs = dict(Bucket=bucket, Prefix=prefix, Delimiter="/")
    while True:
        resp = s3.list_objects_v2(**kwargs)
        out += [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        if not resp.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = resp["NextContinuationToken"]
    return out


def _discover_earliest_hour(s3, bucket):
    """Walk year=/month=/day=/hour= dirs to find the earliest quarantine hour.

    Takes the sorted-first entry at each level (S3 returns prefixes in
    lexicographic order) — O(4 API calls per prefix) = O(8) total.
    Returns a UTC datetime or None if no objects exist.
    """
    best = None
    for root in (PREFIX_MALFORMED, PREFIX_LATE):
        try:
            year_pfxs = _list_common_prefixes(s3, bucket, root)
            if not year_pfxs:
                continue
            month_pfxs = _list_common_prefixes(s3, bucket, year_pfxs[0])
            if not month_pfxs:
                continue
            day_pfxs = _list_common_prefixes(s3, bucket, month_pfxs[0])
            if not day_pfxs:
                continue
            hour_pfxs = _list_common_prefixes(s3, bucket, day_pfxs[0])
            if not hour_pfxs:
                continue
            rel = hour_pfxs[0][len(root):]          # year=.../month=.../day=.../hour=.../
            parts = rel.rstrip("/").split("/")
            t = (int(parts[0].split("=")[1]), int(parts[1].split("=")[1]),
                 int(parts[2].split("=")[1]), int(parts[3].split("=")[1]))
            if best is None or t < best:
                best = t
        except (IndexError, ValueError, Exception) as exc:
            log.warning("_discover_earliest_hour failed for %s: %s", root, exc)
    return None if best is None else datetime(*best, tzinfo=timezone.utc)


def _count_hour(s3, bucket, root_prefix, d, h):
    """Count S3 objects under root_prefix/year=Y/month=M/day=D/hour=H/.
    Sums KeyCount across paginator pages — never materialises the key list.
    Returns 0 if the prefix is absent.
    """
    prefix = (
        f"{root_prefix}"
        f"year={d.year:04d}/month={d.month:02d}/"
        f"day={d.day:02d}/hour={h:02d}/"
    )
    total = 0
    try:
        pag = s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=bucket, Prefix=prefix):
            total += page.get("KeyCount", 0)
    except Exception as exc:
        log.warning("_count_hour failed for %s: %s", prefix, exc)
    return total


# ── Core rollup cycle ─────────────────────────────────────────────────────────

def run_cycle(conn, s3):
    """Process all hours from watermark+1 up to now. Returns count of rows written."""
    now_utc      = datetime.now(timezone.utc)
    current_hour = now_utc.replace(minute=0, second=0, microsecond=0)

    # Derive watermark from the latest finalised row (no separate cursor table).
    with conn.cursor() as cur:
        cur.execute("""
            SELECT summary_date, summary_hour
            FROM   quarantine_hourly_summary
            WHERE  is_final = TRUE
            ORDER  BY summary_date DESC, summary_hour DESC
            LIMIT  1
        """)
        row = cur.fetchone()

    if row is not None:
        watermark = datetime(row[0].year, row[0].month, row[0].day,
                             int(row[1]), tzinfo=timezone.utc)
        start = watermark + timedelta(hours=1)
    else:
        start = _discover_earliest_hour(s3, S3_BUCKET)
        if start is None:
            log.info("No quarantine objects in S3 — nothing to roll up.")
            return 0
        log.info("First run — backfilling from %s UTC.", start.strftime("%Y-%m-%d %H:00"))

    processed = 0
    dt = start.replace(minute=0, second=0, microsecond=0)
    while dt <= current_hour:
        d, h = dt.date(), dt.hour
        mal  = _count_hour(s3, S3_BUCKET, PREFIX_MALFORMED, d, h)
        late = _count_hour(s3, S3_BUCKET, PREFIX_LATE,      d, h)

        # Finalise only once now >= end-of-hour + GRACE.
        hour_end = dt + timedelta(hours=1)
        is_final = now_utc >= hour_end + timedelta(minutes=GRACE_MINUTES)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quarantine_hourly_summary
                    (summary_date, summary_hour,
                     malformed_count, late_count, is_final, computed_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (summary_date, summary_hour) DO UPDATE
                    SET malformed_count = EXCLUDED.malformed_count,
                        late_count      = EXCLUDED.late_count,
                        is_final        = EXCLUDED.is_final,
                        computed_at     = EXCLUDED.computed_at
                """,
                (d, h, mal, late, is_final),
            )
        conn.commit()
        log.debug("  %s h=%02d  mal=%d  late=%d  final=%s", d, h, mal, late, is_final)
        processed += 1
        dt += timedelta(hours=1)

    return processed


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    conn = _get_db()

    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
        if not cur.fetchone()[0]:
            log.info(
                "Another quarantine_hourly_rollup holds lock %d — exiting.", LOCK_KEY
            )
            conn.close()
            sys.exit(1)
    log.info("quarantine_hourly_rollup started (lock %d).", LOCK_KEY)

    s3 = _get_s3()

    # Eager first cycle — populate tiles before the first sleep.
    try:
        n = run_cycle(conn, s3)
        log.info("Initial cycle: %d hour(s) processed.", n)
    except Exception as exc:
        log.error("Initial cycle failed: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass

    while True:
        time.sleep(CYCLE_SECONDS)
        try:
            n = run_cycle(conn, s3)
            if n:
                log.info("Cycle: %d hour(s) processed.", n)
        except Exception as exc:
            log.error("Cycle failed: %s", exc)
            try:
                conn.rollback()
            except Exception:
                pass
            # Attempt reconnect; re-acquire lock on new session.
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = _get_db()
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
                    if not cur.fetchone()[0]:
                        log.info("Lock %d not reacquired after reconnect — exiting.", LOCK_KEY)
                        sys.exit(1)
                log.info("Reconnected and reacquired lock %d.", LOCK_KEY)
            except Exception as e:
                log.error("Reconnect failed: %s — exiting.", e)
                sys.exit(1)


if __name__ == "__main__":
    main()
