"""
Pure-Python Kafka consumer for TravelLens windowed aggregation.

This consumer is the downstream half of the streaming pipeline. It reads
events published by scripts/kafka_event_producer.py from a Kafka topic,
validates them, aggregates by city in tumbling event-time windows, and
dual-sinks the aggregates to Postgres (live OLAP) and S3 Parquet (archive).

────────────────────────────────────────────────────────────────────────
WHY PURE PYTHON, NOT FLINK
────────────────────────────────────────────────────────────────────────
The original Phase 2 plan called for PyFlink. We pivoted to pure Python
because the PyFlink runtime is unstable on Windows + Python 3.11 (Java
gateway crashes, JNI shutdown errors). For TravelLens's scale — tens of
thousands of events per run, single-node — a hand-rolled consumer is
correct *and* educational. The trade-off you accept by leaving Flink:
no built-in checkpointing, no exactly-once semantics, no automatic
backpressure. None of those matter here.

────────────────────────────────────────────────────────────────────────
EVENT-TIME WINDOWING (CRITICAL CONCEPT)
────────────────────────────────────────────────────────────────────────
Every event carries its own timestamp (event_ts) — when the booking
actually happened. Windows are bucketed by event_ts, NOT by wall-clock
arrival time. So an event with event_ts=14:23 always belongs to the
14:00-15:00 window, regardless of whether it arrives at the consumer
at 14:23:01 or at 15:47:09.

This is "event-time" semantics. It's the correct model for any system
where the meaning of "when did X happen" is fixed at source — financial
trades, IoT sensor readings, hotel bookings.

Windows must close eventually so we can emit the aggregate. We use a
watermark + grace mechanism:
  - max_event_ts is the largest event_ts seen so far.
  - watermark = max_event_ts - WATERMARK_GRACE_SECONDS.
  - Any window whose end is before the watermark is closed and flushed.

If an event arrives AFTER its window's watermark has passed, it's "late."
Late events are NOT silently dropped (that was the bug we fixed) — they're
quarantined to S3 for a separate reconciliation job to merge in later.

────────────────────────────────────────────────────────────────────────
FAILURE MODES (TWO QUARANTINE PATHS)
────────────────────────────────────────────────────────────────────────
The event loop has explicit gates. Anything that fails a gate goes to
S3 with full context — nothing is silently dropped.

  1. Unparseable JSON       → s3://.../malformed_events/  reason=unparseable_json
  2. Missing required field → s3://.../malformed_events/  reason=missing_field
  3. Unknown event_type     → s3://.../malformed_events/  reason=unknown_event_type
  4. Unknown city           → s3://.../malformed_events/  reason=unknown_city
  5. Unparseable event_ts   → s3://.../malformed_events/  reason=unparseable_event_ts
  6. Late event (window closed) → s3://.../late_events/   (recoverable)

Counters track each reason. Shutdown summary prints them. If the
malformed count is > 0, the summary explicitly says "TODO for team" —
this is the team's signal that something upstream is wrong.

────────────────────────────────────────────────────────────────────────
SHUTDOWN MODES
────────────────────────────────────────────────────────────────────────
Three ways the consumer exits, all of which run the graceful flush:

  1. SIGINT (Ctrl-C in the same terminal) — interactive use.
  2. SIGTERM (docker stop, kill <pid>)    — process supervisor.
  3. --max-runtime exceeded                — CLI-driven, bounded run.

Mode 3 exists because cross-process SIGINT delivery on Windows is
fragile (different consoles, AttachConsole / GenerateConsoleCtrlEvent
quirks). For automated test runs or batch jobs, passing --max-runtime
lets the consumer self-terminate after a known duration, run shutdown
cleanly, and exit. The test runner then just waits for the subprocess
to finish — no signal injection needed.

Default --max-runtime=0 means "run forever" (interactive / overnight
mode where you Ctrl-C when you're done, or let it run continuously).

────────────────────────────────────────────────────────────────────────
Usage
────────────────────────────────────────────────────────────────────────
  # Run forever (default — interactive / overnight)
  python scripts/stream_consumer.py

  # Bounded run for automated testing — exits after 420s
  python scripts/stream_consumer.py --max-runtime 420

  # Overnight with a 12h safety cap
  python scripts/stream_consumer.py --max-runtime 43200

Reads broker/DB/S3 config from .env. Run alongside scripts/kafka_event_producer.py.
"""

import argparse
import json
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3
import psycopg2
from psycopg2.extras import execute_values
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer

sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG (env-driven for infra; CLI args for run-shape decisions)
# ══════════════════════════════════════════════════════════════════════════════

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",     "booking-events")

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL",      "http://localhost:9000")
S3_KEY_ID   = os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin")
S3_SECRET   = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_REGION   = os.getenv("AWS_DEFAULT_REGION",    "us-east-1")
S3_BUCKET   = os.getenv("S3_BUCKET",             "travellens-data")
S3_PREFIX   = os.getenv("S3_PREFIX_PROCESSED",   "processed/")

# Window size: events are bucketed into 60-minute tumbling windows.
# Env-overridable so fast dev/test runs can use a small window WITHOUT
# editing this file — e.g. set WINDOW_SIZE_MINUTES=2 in .env to make a
# window close (and flush) within a couple of minutes instead of ~65.
# The DEFAULT IS UNCHANGED at 60: if the env var is unset, behaviour is
# byte-for-byte identical to before. Remember the S3 key-collision
# artifact with sub-hour windows (see LESSONS LEARNED) — dev-only.
WINDOW_SIZE_MINUTES = int(os.getenv("WINDOW_SIZE_MINUTES", "60"))

# Grace period before a window is considered closed. This is the
# trade-off between freshness (short grace = fast aggregates) and
# correctness (long grace = fewer late drops). 300s is the production
# default. In dev, your producer emits chronologically, so you'll never
# see lateness unless you enable CHAOS_LATE_PCT.
# Env-overridable for the same fast-test reason as the window size; the
# DEFAULT IS UNCHANGED at 300. Pair a small window with a small grace
# (e.g. WATERMARK_GRACE_SECONDS=30) so a 2-min window actually closes
# during a short run instead of waiting 5 extra minutes for the grace.
WATERMARK_GRACE_SECONDS = int(os.getenv("WATERMARK_GRACE_SECONDS", "300"))

# How often we wake up to check if any windows are ready to flush.
# Independent of window size — this is just the polling cadence.
FLUSH_CHECK_SECONDS = 10

# Max records pulled per poll() cycle in the main loop (Bug A fix).
# This BOUNDS how long the loop stays in event-processing before it
# falls through to the flush check and the max-runtime check. Without a
# bound, a continuous stream keeps the consumer busy forever and those
# checks never run. Smaller = snappier flush cadence + safer against
# broker eviction; larger = marginally higher throughput. 200 suits the
# project's ~50 evt/s rate. Env-overridable; default 200.
INNER_BATCH_MAX = int(os.getenv("INNER_BATCH_MAX", "200"))

# Bronze buffer cap. The consumer buffers accepted events in memory and
# flushes them to s3://.../raw_events/ as one JSONL file per batch, with
# many events per file. The cap bounds (a) memory in the consumer process
# and (b) the worst-case event-loss window if MinIO is unreachable —
# a flush failure drops AT MOST this many events. Mirrors INNER_BATCH_MAX's
# rationale; chosen larger so a typical "10s tick" at 50-100 evt/s lands
# in a single file rather than producing 2-3 trivial files per tick.
BRONZE_BUFFER_CAP = int(os.getenv("BRONZE_BUFFER_CAP", "500"))

# Silver buffer cap. Same rationale as bronze — bounds memory and the
# worst-case loss on Postgres outage. INSERT batching via execute_values
# means one round-trip per flush regardless of batch size (within reason),
# so larger batches are cheap on the database; matching bronze's 500 keeps
# the two layers in lock-step and makes per-flush log lines easy to read.
SILVER_BUFFER_CAP = int(os.getenv("SILVER_BUFFER_CAP", "500"))

# Event types that pass validation. Anything outside this set lands in
# malformed_events with reason "unknown_event_type" — the producer must
# emit one of these five exact strings or it's a producer bug.
# CHECKOUT joined the family in Chunk 2 of the live-metrics work so the
# hourly aggregate can count guest arrivals AND departures; it is a
# valid event even before the producer starts emitting it (Chunk 3).
VALID_EVENT_TYPES = {
    "BOOKING", "CANCELLATION", "CHECKIN", "CHECKOUT", "PRICE_CHANGE",
    "REVIEW",   # B-030: booking-tied review events; routed to bronze + reviews_raw
}

# Of the six valid types, these four contribute to the city aggregate.
# PRICE_CHANGE and REVIEW are both silently filtered at Gate 3 — valid
# wire types, just not aggregated per (city, window). REVIEW is captured
# separately via the REVIEW branch between Gate 4 and silver.
PROCESSED_EVENT_TYPES = {"BOOKING", "CANCELLATION", "CHECKIN", "CHECKOUT"}

# Review sink buffer cap. Same rationale as SILVER_BUFFER_CAP.
REVIEW_BUFFER_CAP = int(os.getenv("REVIEW_BUFFER_CAP", "500"))


# ══════════════════════════════════════════════════════════════════════════════
# S3 CLIENT (one global, reused across all sink calls)
# ══════════════════════════════════════════════════════════════════════════════
# Why module-level? The original code created a fresh boto3 client inside
# every s3_sink() call. That works but is wasteful — each instantiation
# parses credentials, sets up auth, opens an HTTPS connection. For a
# stream consumer that flushes hundreds of windows, reusing one client
# is a meaningful speed-up and avoids hitting MinIO's connection limits.

def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY_ID,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
    )

S3 = make_s3_client()


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def load_known_cities():
    """
    Load the city allow-list from dim_location at startup.

    Cities live in dim_location, NOT hotel_master. hotel_master references
    location via location_id FK. We could JOIN to get the same set, but
    dim_location.city is the source of truth and a simpler query.

    Loaded once, kept in memory — ~44 strings, trivially small.
    If the producer ever emits an event with a city not in this set,
    the validator catches it as 'unknown_city'.

    Note: this returns ALL cities in dim_location, including any that
    may not currently have hotels. For strict "city where a real hotel
    exists" use the JOIN: SELECT DISTINCT l.city FROM hotel_master h
    JOIN dim_location l ON h.location_id = l.location_id. For TravelLens
    it doesn't matter — the producer only emits cities that have hotels,
    so the looser check still catches every chaos-injected bad city.
    """
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT city FROM dim_location")
    cities = {row[0] for row in cur.fetchall()}
    conn.close()
    return cities


def validate_event(event, known_cities):
    """
    Return None if the event is valid, else a short reason string.

    Design choice: FAIL-FAST. First problem encountered wins. We do NOT
    collect every error — if the event is missing both event_ts AND city,
    we only report 'missing_field' for whichever the loop hits first.
    Rationale:
      - Simpler code, easier to reason about.
      - The raw event is in the quarantine file anyway; an engineer
        opening it sees ALL the problems at a glance.
      - Per-reason counters tell you which validator rule fires most —
        that's the actionable signal, not per-event error lists.

    Reason vocabulary (KEEP STABLE — these strings appear in S3 keys,
    counter dicts, and team triage workflows):
      missing_field         — required field absent or empty
      unknown_event_type    — event_type not in VALID_EVENT_TYPES
      unknown_city          — city not in dim_location
      unparseable_event_ts  — event_ts present but not ISO 8601

    REVIEW events (B-030): different required fields; no city in the wire
    format (hotel_id is the routing anchor). Validated separately below.
    """
    # event_type and event_ts are always required regardless of type.
    for f in ("event_type", "event_ts"):
        if not event.get(f):
            return "missing_field"

    # event_type allow-list check — covers all six types including REVIEW.
    if event["event_type"] not in VALID_EVENT_TYPES:
        return "unknown_event_type"

    if event["event_type"] == "REVIEW":
        # REVIEW wire format: no city. Required: hotel_id, review_id,
        # booking_id, customer_id, review_stage, review_channel, rating.
        # rating is numeric 1-5; treat as truthy if present and non-zero.
        for f in ("hotel_id", "review_id", "booking_id", "customer_id",
                  "review_stage", "review_channel"):
            if not event.get(f):
                return "missing_field"
        if event.get("rating") is None:
            return "missing_field"
        # Skip the city allow-list check for REVIEW — city is not in the
        # wire format. hotel_id is the lookup key instead.
    else:
        # All non-REVIEW event types require city and hotel_id.
        for f in ("city", "hotel_id"):
            if not event.get(f):
                return "missing_field"
        if event["city"] not in known_cities:
            return "unknown_city"

    # ISO 8601 timestamp parse check (applies to all types).
    # Two-step: first confirm it's a string (a number or list would crash
    # fromisoformat with TypeError, not ValueError), then try parsing.
    # .replace("Z", "+00:00") normalizes the "Z" suffix that
    # datetime.fromisoformat doesn't accept before Python 3.11.
    ts_val = event["event_ts"]
    if not isinstance(ts_val, str):
        return "unparseable_event_ts"
    try:
        datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
    except ValueError:
        return "unparseable_event_ts"

    return None   # all gates passed


# ══════════════════════════════════════════════════════════════════════════════
# SINKS — Postgres (live OLAP) and S3 (archive)
# ══════════════════════════════════════════════════════════════════════════════

def postgres_sink(row):
    """
    UPSERT one aggregate row to agg_hourly_city_stats.

    Why UPSERT and not INSERT? Because a window can be flushed more than
    once in dev mode (2-min windows that share an hour-bucket key in S3,
    or future re-runs of a windowed period). The unique key is
    (city, window_start); on conflict, every metric column is overwritten
    with the new computed value, and ingestion_ts is bumped so we can
    see when the latest write happened.

    Each call opens a fresh connection, commits, closes. Wasteful at
    scale, fine at TravelLens scale. A pooled connection would be the
    right answer if windows flushed in the thousands per second.
    """
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur  = conn.cursor()
        # Chunk 2 widening: total_checkins / total_checkouts /
        # total_cancellations are NEW count columns (migration 007)
        # that the consumer now populates so the hourly aggregate
        # captures every event class the producer emits, not just
        # bookings. cancellation_rate (the ratio) is still derived
        # separately in build_row and stays in the UPSERT unchanged.
        # total_reviews exists on the table but is intentionally NOT
        # written here — no REVIEW events flow through this consumer.
        cur.execute(
            """
            INSERT INTO agg_hourly_city_stats
                (city, window_start, window_end, total_bookings,
                 total_revenue_inr, avg_occupancy_rate, cancellation_rate,
                 total_checkins, total_checkouts, total_cancellations)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city, window_start)
            DO UPDATE SET
                window_end          = EXCLUDED.window_end,
                total_bookings      = EXCLUDED.total_bookings,
                total_revenue_inr   = EXCLUDED.total_revenue_inr,
                avg_occupancy_rate  = EXCLUDED.avg_occupancy_rate,
                cancellation_rate   = EXCLUDED.cancellation_rate,
                total_checkins      = EXCLUDED.total_checkins,
                total_checkouts     = EXCLUDED.total_checkouts,
                total_cancellations = EXCLUDED.total_cancellations,
                ingestion_ts        = CURRENT_TIMESTAMP
            """,
            (
                row["city"],
                row["window_start"],
                row["window_end"],
                row["total_bookings"],
                row["total_revenue_inr"],
                row["avg_occupancy_rate"],
                row["cancellation_rate"],
                row["total_checkins"],
                row["total_checkouts"],
                row["total_cancellations"],
            ),
        )
        conn.commit()
        conn.close()
        print(f"  ✓ Postgres upsert: {row['city']}@{row['window_start']}")
    except Exception as exc:
        # We log and continue rather than crash. A transient Postgres
        # blip shouldn't take down the whole consumer — the S3 sink
        # below still gets a chance, and the data lives in MinIO as a
        # backstop. In production we'd add proper retry logic; here,
        # the goal is "don't die silently."
        print(f"  ✗ Postgres sink error: {exc}", file=sys.stderr)


def s3_processed_sink(row):
    """
    Write one aggregate row as a Parquet file to S3 under processed/.

    S3 key pattern:
        processed/agg/hourly_city_stats/year=YYYY/month=MM/day=DD/hour=HH_{city}.parquet

    The Hive-style partitioning (year=/month=/day=/hour=) is the
    standard convention for data-lake layouts — DuckDB, Spark, Athena,
    Trino all auto-detect these partitions and can prune scans based
    on WHERE clauses on the partition columns. Critical for Phase 4/5
    when we read these files for analytics.

    Note the key does NOT include the minute — so in dev mode with
    2-min windows, three windows in the same hour overwrite each other.
    This is a deliberate dev-only artifact; production 60-min windows
    have one file per hour per city naturally.
    """
    try:
        ws  = datetime.fromisoformat(row["window_start"])
        key = (
            f"{S3_PREFIX}agg/hourly_city_stats/"
            f"year={ws.year:04d}/month={ws.month:02d}/"
            f"day={ws.day:02d}/hour={ws.hour:02d}_{row['city']}.parquet"
        )
        # Build a one-row PyArrow table → serialize to Parquet → upload.
        # We write to an in-memory buffer rather than a temp file to
        # avoid filesystem I/O on the hot path.
        table = pa.Table.from_pylist([row])
        buf   = pa.BufferOutputStream()
        pq.write_table(table, buf)
        S3.put_object(Bucket=S3_BUCKET, Key=key, Body=bytes(buf.getvalue()))
        print(f"  ✓ S3 archive: s3://{S3_BUCKET}/{key}")
    except Exception as exc:
        print(f"  ✗ S3 sink error: {exc}", file=sys.stderr)


def quarantine_event(prefix, payload):
    """
    Write a JSON quarantine record to S3 under late_events/ or malformed_events/.

    Quarantine files use JSON (not Parquet) deliberately:
      - Easy to inspect — open in any text editor or `mc cat`.
      - One file per event, not batched — each problem stays isolated.
      - Schema is loose — different reasons have different context fields.
    Parquet would be over-engineering for what is by definition exceptional
    low-volume data.

    Key pattern groups by hour of consumer-side time (when the event was
    caught), not by event-time. Reason: late events have fictional past
    event_ts values; grouping by those creates confusing back-dated dirs.
    The triaging engineer wants "what broke in last night's run", which
    is consumer time.

    A short UUID suffix prevents collisions when multiple events are
    quarantined in the same second.
    """
    try:
        now = datetime.now(timezone.utc)
        key = (
            f"{prefix}/"
            f"year={now.year:04d}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/"
            f"{now.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
        )
        S3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            # default=str handles datetime and other non-JSON-native types
            # gracefully — if the producer sent something weird, we still
            # serialize it as best we can rather than crashing.
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:
        # If quarantine write itself fails — disk full on MinIO, network
        # blip — we log it and keep going. Losing visibility into a
        # malformed event is bad, but crashing the consumer is worse.
        print(f"  ✗ Quarantine write error ({prefix}): {exc}", file=sys.stderr)


def dual_sink(row):
    """Flush one aggregate row to both Postgres and S3."""
    print(f"  → SINK FIRED for {row.get('city')} window {row.get('window_start')}")
    postgres_sink(row)
    s3_processed_sink(row)


# ══════════════════════════════════════════════════════════════════════════════
# BRONZE SINK (B-038) — durable raw archive of every accepted event
# ══════════════════════════════════════════════════════════════════════════════
# Why a bronze layer at all? The agg UPSERT is LOSSY (it derives totals;
# it discards the per-event payload), the Parquet archive is LOSSY (it
# stores aggregate rows, not events), and quarantine only captures the
# events that FAILED a gate. There was previously no immutable copy of
# the events that succeeded — i.e. of the raw stream itself. Bronze
# fills that gap: every accepted payload (whatever passed the four
# gates and made it into the window accumulator) is appended to a
# buffer and flushed to S3 as JSONL.
#
# Format: JSONL — one event per line, many events per file. NOT
# Parquet (overkill for raw bytes + would force a schema on per-type
# variant fields like cancellation_reason/old_price_inr); NOT one
# file per event (would generate tens of thousands of tiny S3 objects
# for a single run, kill list performance, and waste bytes on JSON
# overhead per object).
#
# Partition by INGEST wall-clock time, mirroring quarantine_event's
# convention (NOT event-time, the way the agg Parquet does). Rationale:
# bronze is "what arrived" — operationally indexed. The triaging
# engineer asks "what came in during the last hour" using
# `now()`-relative paths. Event-time partitioning would put late
# chaos events into past-dated folders, which is the same trap the
# quarantine code already chose to avoid.
#
# Isolation: bronze writes are wrapped in their own try/except. A
# bronze failure increments a counter, logs to stderr, and continues —
# it MUST NEVER crash the consumer or affect agg / heartbeat. Lost
# events are accepted; bronze is best-effort-durable, not a gate.

def bronze_flush(buffer, metrics):
    """
    Flush the bronze buffer to S3 as one JSONL file under raw_events/.

    Args:
        buffer:  list of accepted event dicts (caller passes the actual
                 in-memory buffer; this function does NOT mutate it).
        metrics: dict carrying counters (events_bronzed, files_written,
                 flushes_failed) — updated in place.

    Returns:
        An empty list, intended for the caller to reassign over its
        local buffer reference. Even on failure we clear the buffer:
        bronze is best-effort, retrying indefinitely would leak memory
        when MinIO is genuinely unreachable.
    """
    if not buffer:
        return []
    try:
        now = datetime.now(timezone.utc)
        key = (
            f"raw_events/"
            f"year={now.year:04d}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/"
            f"{now.strftime('%H%M%S')}_{uuid.uuid4().hex[:8]}.jsonl"
        )
        # JSONL = newline-delimited JSON. Each line is one event. The
        # trailing newline keeps the file POSIX-text-clean so `wc -l`
        # gives an honest event count.
        # default=str handles any non-JSON-native value the producer
        # might add (datetime, Decimal, etc.) without raising — same
        # defensive default the quarantine code uses.
        body = (
            "\n".join(json.dumps(e, default=str) for e in buffer) + "\n"
        ).encode("utf-8")
        S3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )
        metrics["bronze_events"]  += len(buffer)
        metrics["bronze_files"]   += 1
        metrics["bronze_bytes"]   += len(body)
    except Exception as exc:
        # Same posture as quarantine_event and the heartbeat: log,
        # increment a failure counter so the operator sees the issue
        # in the shutdown summary, and continue. Crashing the consumer
        # to "save" a batch of raw archive is the wrong trade.
        metrics["bronze_failures"] += 1
        print(
            f"  ✗ Bronze sink error ({len(buffer)} events dropped): {exc}",
            file=sys.stderr,
        )
    return []


# ══════════════════════════════════════════════════════════════════════════════
# SILVER SINK (B-039) — typed per-event ledger in fact_booking_events
# ══════════════════════════════════════════════════════════════════════════════
# Why a silver layer on top of bronze? Bronze gives us a durable raw archive
# in S3 but bronze is JSON-on-object-storage — a wrong shape for per-event
# analytics. Silver is "the same events, typed, indexed, queryable" — a
# Postgres-side ledger sitting in the SAME `fact_booking_events` table the
# history backfill writes (B-035). `source='history'` and `source='stream'`
# share the schema; the column is the only thing that distinguishes them.
# Once silver is in place, gold (B-040 — per-booking lifecycle
# reconstruction with `illegal_transition_flag`) becomes a SQL query, not
# a JSONL parser.
#
# Dedup story: `event_id` is the table's PRIMARY KEY (migration 008), so
# `ON CONFLICT (event_id) DO NOTHING` short-circuits re-inserts at the
# database. This covers Kafka redelivery cleanly (a duplicate batch
# arrives → 0 new rows). It does NOT cover producer crash-replay, which
# would mint NEW event_ids for re-emitted events — that's a known gap;
# making event_id deterministic per-emit (e.g. hash of booking_id + type
# + sim_day) is the long-term path to exactly-once, tracked separately.
#
# Type coverage: silver writes ALL five wire types — BOOKING / CHECKIN /
# CHECKOUT / CANCELLATION / PRICE_CHANGE. PRICE_CHANGE rows have NULL
# booking_id and customer_id by design (a price change is an operational
# event on a (hotel, room_type), not on a booking). This is why silver is
# placed BEFORE the Gate-3 PROCESSED_EVENT_TYPES filter — bronze sits
# after that filter and so excludes PRICE_CHANGE, but silver is the
# durable per-event truth and must include every accepted event type.
# (See AUDIT in this session's docs for why bronze isn't moved to match.)
#
# Isolation: same posture as bronze and the heartbeat. Wrapped in its own
# try/except; failures log + increment a counter + drop the batch. The
# agg UPSERT, bronze, heartbeat, quarantine sinks, and window logic are
# untouched and unaffected.

_SILVER_COLUMNS = (
    "event_id", "event_type",
    "booking_id", "customer_id", "hotel_id", "city", "room_type_id",
    "event_ts", "event_date",
    "checkin_date", "checkout_date", "nights", "num_guests",
    "nightly_rate_inr", "revenue_inr", "booking_source", "payment_mode",
    "cancellation_reason",
    "old_price_inr", "new_price_inr",
    "source",
)


def _silver_row(event):
    """
    Map one wire event dict to the fact_booking_events row tuple.

    Per-type field mapping (all source='stream'):
      BOOKING      : full booking payload (rate/revenue/nights/guests/dates).
      CHECKIN      : base envelope + booking_id + customer_id.
      CHECKOUT     : same as CHECKIN.
      CANCELLATION : same + cancellation_reason.
      PRICE_CHANGE : hotel_id + room_type_id + old/new price; booking_id
                     and customer_id INTENTIONALLY NULL (operational event,
                     not booking-scoped).

    Returns a tuple matching _SILVER_COLUMNS order; psycopg2 / execute_values
    handles type coercion (ISO string → TIMESTAMPTZ / DATE; int → SMALLINT /
    NUMERIC). Fields not relevant to the row's event_type are simply None.
    """
    et = event.get("event_type")
    is_booking      = (et == "BOOKING")
    is_cancellation = (et == "CANCELLATION")
    is_price        = (et == "PRICE_CHANGE")
    return (
        event.get("event_id"),
        et,
        event.get("booking_id"),
        event.get("customer_id"),
        event.get("hotel_id"),
        event.get("city"),
        event.get("room_type_id"),
        event.get("event_ts"),
        event.get("event_date"),
        event.get("checkin_date")    if is_booking else None,
        event.get("checkout_date")   if is_booking else None,
        event.get("nights")          if is_booking else None,
        event.get("num_guests")      if is_booking else None,
        event.get("nightly_rate_inr") if is_booking else None,
        event.get("revenue_inr")     if is_booking else None,
        event.get("booking_source")  if is_booking else None,
        event.get("payment_mode")    if is_booking else None,
        event.get("cancellation_reason") if is_cancellation else None,
        event.get("old_price_inr")   if is_price else None,
        event.get("new_price_inr")   if is_price else None,
        "stream",
    )


def silver_flush(buffer, metrics):
    """
    Flush the silver buffer to fact_booking_events.

    Uses psycopg2.extras.execute_values for one round-trip per flush,
    with ON CONFLICT (event_id) DO NOTHING for idempotent re-inserts.
    cur.rowcount post-execute reports rows actually inserted (excludes
    DO NOTHING short-circuits), so we can tell durable inserts apart
    from dedup hits.

    Args:
        buffer:  list of accepted event dicts.
        metrics: dict carrying counters (silver_attempted, silver_inserted,
                 silver_duplicates, silver_flushes, silver_failures) —
                 updated in place.

    Returns:
        Empty list. Same best-effort posture as bronze_flush: on failure
        we drop the batch rather than retry-leak memory under sustained
        Postgres outage.
    """
    if not buffer:
        return []
    try:
        rows = [_silver_row(e) for e in buffer]
        conn = psycopg2.connect(**DB_PARAMS)
        try:
            with conn.cursor() as cur:
                cols = ",".join(_SILVER_COLUMNS)
                # page_size=len(rows) forces execute_values to send the
                # whole batch as ONE statement, so cur.rowcount reflects
                # the entire flush. With the default page_size=100, a
                # 500-row batch gets split into 5 sub-queries and
                # cur.rowcount returns only the LAST one's count — which
                # silently undercounts new-inserts (and overcounts
                # dedupes by the same delta) on every flush. The bigger
                # the batch, the worse the lie. Tested against a
                # synthetic 500-row insert: default page_size returned
                # rowcount=100, custom page_size=500 returned 500.
                execute_values(
                    cur,
                    (
                        f"INSERT INTO fact_booking_events ({cols}) VALUES %s "
                        f"ON CONFLICT (event_id) DO NOTHING"
                    ),
                    rows,
                    page_size=len(rows),
                )
                inserted = cur.rowcount
            conn.commit()
        finally:
            conn.close()
        attempted = len(buffer)
        duplicates = attempted - inserted
        metrics["silver_attempted"]  += attempted
        metrics["silver_inserted"]   += inserted
        metrics["silver_duplicates"] += duplicates
        metrics["silver_flushes"]    += 1
    except Exception as exc:
        metrics["silver_failures"] += 1
        print(
            f"  ✗ Silver sink error ({len(buffer)} rows dropped): {exc}",
            file=sys.stderr,
        )
    return []


# ══════════════════════════════════════════════════════════════════════════════
# REVIEW SINK (B-030) — writes REVIEW wire events to reviews_raw
# ══════════════════════════════════════════════════════════════════════════════
# REVIEW events must NOT enter fact_booking_events (silver) or the city-revenue
# aggregate (accumulator). They have their own dedicated Postgres sink here.
# Same best-effort-durable posture as bronze and silver: failure logs + drops
# the batch + continues; never crashes the consumer.

def _review_row(event):
    """
    Map a REVIEW wire event to a (16-tuple) reviews_raw insert row.

    Column order matches the INSERT below:
        review_id, hotel_id, reviewer_name, review_text, rating,
        review_date, source, travel_type, embedding,
        booking_id, customer_id, review_stage, review_channel,
        event_ts, event_date, record_source
    """
    return (
        event["review_id"],           # review_id  UUID
        event["hotel_id"],            # hotel_id   VARCHAR FK
        None,                         # reviewer_name NULL (not in wire format)
        event.get("review_text"),     # review_text nullable TEXT
        event["rating"],              # rating NUMERIC — accepts int 1-5
        event.get("event_date"),      # review_date DATE (sim-day / calendar day)
        event.get("review_channel"),  # source (channel doubles as source)
        None,                         # travel_type NULL (not in wire format; derive from dim_customer in B-030b)
        None,                         # embedding NULL (B-030b fills via batch embed)
        event["booking_id"],          # booking_id UUID
        event["customer_id"],         # customer_id VARCHAR
        event["review_stage"],        # review_stage
        event["review_channel"],      # review_channel
        event.get("event_ts"),        # event_ts TIMESTAMPTZ
        event.get("event_date"),      # event_date DATE
        "stream",                     # record_source
    )


def review_flush(buffer, metrics):
    """
    Flush the review buffer to reviews_raw.

    ON CONFLICT (review_id) DO NOTHING: deterministic review_id (uuid5 in
    the producer/generator) makes re-delivery idempotent — the same review
    is ignored on the second delivery.

    Same isolation posture as silver_flush: any exception logs to stderr,
    increments review_failures, and returns an empty list; never raises.
    """
    if not buffer:
        return []

    rows     = [_review_row(e) for e in buffer]
    inserted = 0
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        try:
            cur = conn.cursor()
            execute_values(
                cur,
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
                page_size=len(rows),
            )
            inserted = cur.rowcount
            conn.commit()
        finally:
            conn.close()
        attempted  = len(buffer)
        duplicates = attempted - inserted
        metrics["review_attempted"]  += attempted
        metrics["review_inserted"]   += inserted
        metrics["review_duplicates"] += duplicates
        metrics["review_flushes"]    += 1
    except Exception as exc:
        metrics["review_failures"] += 1
        print(
            f"  ✗ Review sink error ({len(buffer)} rows dropped): {exc}",
            file=sys.stderr,
        )
    return []


# ══════════════════════════════════════════════════════════════════════════════
# WINDOWING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def floor_to_window(dt):
    """
    Truncate a UTC datetime down to its window-start boundary.

    Examples (with WINDOW_SIZE_MINUTES=60):
        14:23:45  → 14:00:00
        14:59:59  → 14:00:00
        15:00:00  → 15:00:00

    Implementation: convert to epoch seconds, divide by 60 to get
    epoch minutes, floor-divide by window size, multiply back. Pure
    integer math — no timezone surprises, no off-by-one bugs from
    daylight saving (because we're in UTC throughout).
    """
    epoch_minutes = int(dt.timestamp()) // 60
    window_minute = (epoch_minutes // WINDOW_SIZE_MINUTES) * WINDOW_SIZE_MINUTES
    return datetime.fromtimestamp(window_minute * 60, tz=timezone.utc)


def build_row(window_start, city, acc):
    """
    Convert an in-memory accumulator into the row dict that gets sinked.

    The accumulator is whatever we've been adding to during the window's
    lifetime. This function computes derived metrics (rates, percentages)
    once at flush time rather than maintaining them incrementally — fewer
    chances for floating-point drift.

    Occupancy: bookings / 50 * 100, capped at 100%. The 50 is a placeholder
    for "assumed total rooms per city" — would come from dim_city in a
    real model. Good enough for the streaming demo; Phase 4 SQL queries
    will use the actual dim tables for accurate occupancy.

    Cancellation rate: cancels / (bookings + cancels). Defined this way
    so it's bounded in [0, 1] even when bookings=0 (the if/else handles
    the zero-division case).
    """
    window_end    = window_start + timedelta(minutes=WINDOW_SIZE_MINUTES)
    bookings      = acc["bookings"]
    cancels       = acc["cancellations"]
    # Chunk 2: per-window check-in / check-out counts.
    # The accumulator started tracking these in this chunk so the row
    # can carry total_checkins / total_checkouts straight through to
    # the agg_hourly_city_stats UPSERT. They will read 0 for CHECKOUT
    # until the producer learns to emit it in Chunk 3 — that's expected.
    checkins      = acc["checkins"]
    checkouts     = acc["checkouts"]
    revenue       = acc["revenue"]
    total         = bookings + cancels
    cancel_rate   = round(cancels / total, 4) if total else 0.0
    occupancy_pct = min(bookings / 50.0 * 100, 100.0)
    return {
        "city":                 city,
        "window_start":         window_start.isoformat(),
        "window_end":           window_end.isoformat(),
        "total_bookings":       bookings,
        "total_revenue_inr":    round(revenue, 2),
        "avg_occupancy_rate":   round(occupancy_pct, 2),
        "cancellation_rate":    round(cancel_rate, 4),
        "total_checkins":       checkins,
        "total_checkouts":      checkouts,
        "total_cancellations":  cancels,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # ── CLI args ─────────────────────────────────────────────────────────
    # Only one arg — runtime shape. Everything else is .env so the same
    # script behaves the same way across dev, test, and prod environments
    # without code changes.
    parser = argparse.ArgumentParser(description="TravelLens Kafka stream consumer")
    parser.add_argument(
        "--max-runtime",
        type=int,
        default=0,
        help="Max runtime in seconds. 0 = run forever (default, interactive/overnight). "
             "Non-zero = self-exit gracefully after this many seconds elapsed. "
             "Used by test orchestrators to avoid cross-process SIGINT on Windows."
    )
    args = parser.parse_args()

    MAX_RUNTIME_SECS = args.max_runtime

    print(f"Stream consumer starting — {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}")
    print(f"  Window: {WINDOW_SIZE_MINUTES}m  |  Watermark grace: {WATERMARK_GRACE_SECONDS}s  |  Flush check: {FLUSH_CHECK_SECONDS}s")
    if MAX_RUNTIME_SECS:
        print(f"  Max runtime: {MAX_RUNTIME_SECS}s (will self-exit gracefully after this)")
    else:
        print(f"  Max runtime: unbounded (Ctrl-C or SIGTERM to exit)")

    # Load the city allow-list ONCE at startup. If dim_location changes
    # mid-run (a city is added), this consumer won't see it until restart.
    # Acceptable — the producer's hotel pool is loaded at its own startup,
    # so they stay consistent for the duration of a run.
    known_cities = load_known_cities()
    print(f"  Loaded {len(known_cities)} known cities from dim_location")

    # ── Robust deserializer ─────────────────────────────────────────────
    # The default `json.loads` raises on bad bytes. If we don't catch it,
    # kafka-python's behavior is version-dependent (sometimes drops the
    # message, sometimes kills the iterator). Either way it's silent.
    #
    # Our solution: never let the deserializer raise. Return a sentinel
    # dict carrying the raw bytes and the exception. The main loop checks
    # for this sentinel and routes it to quarantine with reason
    # 'unparseable_json'. This way, the failure is observable.
    def _deserialize(b):
        try:
            return json.loads(b.decode("utf-8"))
        except Exception as exc:
            return {
                "__unparseable__": True,
                "__raw__":          b.decode("utf-8", errors="replace"),
                "__error__":        str(exc),
            }

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="travellens-python-consumer",
        # 'latest' = only events published after the consumer starts.
        # NOTE: this only applies if the consumer group has NO committed
        # offset. Once any run commits offsets, this setting is ignored
        # on restart and the consumer resumes from the committed offset.
        # If tests are accumulating phantom history, reset the group:
        #   docker exec travellens-kafka kafka-consumer-groups \
        #     --bootstrap-server localhost:9092 \
        #     --group travellens-python-consumer \
        #     --reset-offsets --to-latest --topic booking-events --execute
        # (Group must have no active members for reset to succeed.)
        auto_offset_reset="latest",
        # Poll timeout — if no messages arrive in 1s, the iterator
        # yields control so we can run the flush check and runtime check.
        consumer_timeout_ms=1000,
        value_deserializer=_deserialize,
        # Auto-commit offsets every few seconds. For at-least-once
        # semantics this is fine; for exactly-once you'd commit
        # manually after each successful sink. TravelLens accepts
        # the rare double-count from a crash mid-flush.
        enable_auto_commit=True,
        # ── Bug B fix: survive slow batches without broker eviction ──
        # These were previously UNSET, so kafka-python used its defaults
        # (session 10s, heartbeat 3s, max-poll-interval 5min). Under
        # chaos load the consumer spends a long time between polls doing
        # blocking S3 quarantine writes (one put_object per bad event).
        # If the gap between polls exceeds max_poll_interval_ms, the
        # broker assumes the consumer died and evicts it from the group
        # ("no active members"), lag balloons, and nothing ever flushes.
        # We raise the ceilings so a slow batch can't trigger eviction.
        # Combined with the bounded poll() batch in the main loop, the
        # gap between polls is now both CAPPED (max_poll_records) and
        # TOLERATED (max_poll_interval_ms). Defaults preserved otherwise.
        max_poll_records=INNER_BATCH_MAX,   # cap fetch size to bound per-poll work
        max_poll_interval_ms=600000,        # 10 min headroom for slow S3 batches (was 5 min default)
        session_timeout_ms=30000,           # 30s before the broker calls us dead (was 10s default)
        heartbeat_interval_ms=10000,        # 10s heartbeat — must be <= 1/3 of session_timeout_ms
    )

    # ── In-memory window state ──────────────────────────────────────────
    # Keyed by (window_start_iso_string, city). Value is an accumulator
    # dict that grows as events stream in for that window+city.
    #
    # defaultdict means accessing a new key auto-creates a zero-init
    # accumulator. This was the source of the late-event bug we fixed:
    # without the late guard, accessing state[key] for an already-flushed
    # window would silently recreate it with zero state and corrupt the
    # aggregate. The fix is the guard further down in the event loop.
    state = defaultdict(lambda: {
        "bookings":      0,
        "revenue":       0.0,
        "cancellations": 0,
        # Chunk 2: dedicated buckets for CHECKIN / CHECKOUT so the
        # hourly aggregate can report guest arrivals/departures
        # alongside booking activity. They must be initialised here —
        # without these keys, the accumulator block below would
        # KeyError the first time it tried to increment them.
        "checkins":      0,
        "checkouts":     0,
        "total_events":  0,
        "last_event_ts": 0.0,
    })

    # ── Run metrics ─────────────────────────────────────────────────────
    # All counters live in one dict so the shutdown summary code is
    # straightforward. malformed_by_reason is a defaultdict(int) so we
    # can do `+=1` on unseen keys without checking existence.
    run_metrics = {
        "events_consumed":      0,
        "windows_flushed":      0,
        "late_dropped":         0,
        "malformed_dropped":    0,
        # Chunk 2: cumulative run-totals for BOOKING and CANCELLATION
        # events, incremented in the accumulator block alongside the
        # per-window counts. These power the pipeline_metrics
        # heartbeat (and via deltas, the live throughput tile on
        # /monitor). Per-window totals already live inside `state`;
        # these are the run-lifetime totals an outside observer needs.
        "bookings":             0,
        "cancellations":        0,
        "malformed_by_reason":  defaultdict(int),
        # B-038: bronze sink counters. bronze_events is incremented
        # only inside bronze_flush() on a SUCCESSFUL put — so it
        # reports actually-durable events, not events that hit the
        # buffer. bronze_failures counts failed flush batches (each
        # batch failure drops <=BRONZE_BUFFER_CAP events; see
        # bronze_flush comment). bronze_bytes is for the shutdown
        # summary so file sizes are visible at a glance.
        "bronze_events":        0,
        "bronze_files":         0,
        "bronze_bytes":         0,
        "bronze_failures":      0,
        # B-039: silver sink counters. silver_inserted counts rows that
        # actually landed (cur.rowcount post-execute_values, which excludes
        # ON CONFLICT DO NOTHING short-circuits). silver_attempted is the
        # raw batch size — useful for spotting "everything is dedup" vs
        # "nothing inserts." silver_duplicates = attempted - inserted is
        # how the acceptance dedup test (C) shows itself.
        "silver_attempted":     0,
        "silver_inserted":      0,
        "silver_duplicates":    0,
        "silver_flushes":       0,
        "silver_failures":      0,
        # B-030: review sink counters (mirrors silver pattern)
        "review_attempted":     0,
        "review_inserted":      0,
        "review_duplicates":    0,
        "review_flushes":       0,
        "review_failures":      0,
    }

    # B-038 / B-039 / B-030: in-memory buffers for the three parallel sinks.
    # Single-threaded by design; no locking.
    bronze_buffer = []
    silver_buffer = []
    review_buffer = []

    # max_event_ts tracks the largest event_ts seen so far (in epoch
    # seconds). This IS the watermark generator — every time we see a
    # newer event, the watermark advances. Older events are "late."
    max_event_ts = 0.0

    # ── Graceful shutdown ───────────────────────────────────────────────
    # SIGINT (Ctrl-C) and SIGTERM (docker stop, kill) both flip `running`
    # to False, which makes the outer loop exit on the next iteration.
    # Then we flush any windows still in state and write the summary.
    running = True
    def _stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    t_start      = time.time()
    t_last_flush = t_start

    # ════════════════════════════════════════════════════════════════════
    # MAIN EVENT LOOP
    # ════════════════════════════════════════════════════════════════════
    # The loop has three phases:
    #   1. Drain the consumer iterator — process events until poll timeout.
    #   2. Check if any windows are ready to flush; flush them.
    #   3. Check if --max-runtime has elapsed; if so, exit gracefully.
    # We can't do everything in one go because the consumer iterator
    # blocks for up to consumer_timeout_ms.

    while running:
        # ── Phase 1: poll a BOUNDED batch ────────────────────────────
        # Bug A fix. This was previously `for msg in consumer:`, which
        # iterates the KafkaConsumer as an ENDLESS generator that only
        # yields control back to us after consumer_timeout_ms (1s) of
        # SILENCE on the topic. Under a continuous stream — especially
        # `--chaos`, where every bad event triggers a blocking S3 write
        # so the loop is always busy — the topic never goes silent for a
        # full second, the inner loop never ended, and the flush check
        # (Phase 2) and max-runtime check (Phase 3) below NEVER RAN. The
        # consumer just ground on until the broker evicted it, and no
        # window was ever flushed to Postgres.
        #
        # consumer.poll() fixes this: it returns control every cycle —
        # after timeout_ms (1s) OR once max_records have been fetched,
        # whichever comes first. We flatten the {partition: [records]}
        # result into one list and process each record with the EXACT
        # SAME gate/accumulate logic as before (the loop body below is
        # unchanged). The only behavioural difference is that Phase 2/3
        # are now guaranteed a turn on every cycle, even under load.
        # No event is dropped: records we don't fetch this cycle stay on
        # the topic and are returned by the next poll().
        _polled = consumer.poll(timeout_ms=1000, max_records=INNER_BATCH_MAX)
        _batch = [m for _records in _polled.values() for m in _records]
        for msg in _batch:
            if not running:
                break

            event = msg.value
            run_metrics["events_consumed"] += 1

            # ── Gate 1: JSON deserialize ─────────────────────────────
            # The sentinel dict returned by _deserialize() when bytes
            # don't parse. We catch it here and quarantine.
            if isinstance(event, dict) and event.get("__unparseable__"):
                quarantine_event("malformed_events", {
                    "raw_value":       event.get("__raw__"),
                    "deserialize_err": event.get("__error__"),
                    "reason":          "unparseable_json",
                    "kafka_offset":    msg.offset,
                    "kafka_partition": msg.partition,
                    "consumer_ts":     datetime.now(timezone.utc).isoformat(),
                })
                run_metrics["malformed_dropped"] += 1
                run_metrics["malformed_by_reason"]["unparseable_json"] += 1
                continue

            # ── Gate 2: Schema validation ────────────────────────────
            # Returns None if valid, else a reason string. Fail-fast.
            reason = validate_event(event, known_cities)
            if reason:
                quarantine_event("malformed_events", {
                    "raw_value":       event,
                    "reason":          reason,
                    "kafka_offset":    msg.offset,
                    "kafka_partition": msg.partition,
                    "consumer_ts":     datetime.now(timezone.utc).isoformat(),
                })
                run_metrics["malformed_dropped"] += 1
                run_metrics["malformed_by_reason"][reason] += 1
                continue

            # ── Parse timestamp (safe — validator already checked it) ─
            # Moved above the Gate-3 type filter in B-039 so the late
            # guard (Gate 4) sees ALL valid event types — previously
            # Gate 3 silently dropped PRICE_CHANGE before Gate 4 had a
            # chance to look at lateness. The order change is otherwise
            # invisible: every non-PRICE_CHANGE event sees identical
            # behavior; late PRICE_CHANGE now goes to late_events/
            # (was silently dropped at Gate 3 previously) — closing a
            # silent-drop gap the original code didn't intend.
            ts_str  = event["event_ts"].replace("Z", "+00:00")
            event_dt = datetime.fromisoformat(ts_str)
            event_ts_secs = event_dt.timestamp()
            max_event_ts  = max(max_event_ts, event_ts_secs)

            # ── Compute window for this event ────────────────────────
            window_start    = floor_to_window(event_dt)
            window_end_secs = window_start.timestamp() + WINDOW_SIZE_MINUTES * 60
            watermark_secs  = max_event_ts - WATERMARK_GRACE_SECONDS

            # ── Gate 4: Late-event guard ─────────────────────────────
            # This is THE bug we set out to fix. Without this check,
            # accessing state[key] below would resurrect a flushed
            # window via defaultdict and corrupt the aggregate when
            # it inevitably gets re-flushed.
            #
            # An event is "late" if its window's end is already past
            # the watermark. We quarantine to late_events/ — these
            # are recoverable by the reconciliation job, unlike
            # malformed events which need human triage.
            if window_end_secs < watermark_secs:
                quarantine_event("late_events", {
                    "raw_value":       event,
                    "window_start":    window_start.isoformat(),
                    "watermark_secs":  watermark_secs,
                    "kafka_offset":    msg.offset,
                    "kafka_partition": msg.partition,
                    "consumer_ts":     datetime.now(timezone.utc).isoformat(),
                })
                run_metrics["late_dropped"] += 1
                # Throttled progress log — first one, then every 100th.
                # Helps you see late events without flooding the console
                # if chaos rate is high.
                if run_metrics["late_dropped"] % 100 == 1:
                    _loc = event.get("city", event.get("hotel_id", "N/A"))
                    print(f"  ⚠ Late event quarantined: {_loc}@{window_start.isoformat()} (total: {run_metrics['late_dropped']})")
                continue

            # ── REVIEW routing (B-030) ───────────────────────────────
            # REVIEW passes Gates 1-2-4 above but must NOT enter silver
            # (fact_booking_events), agg, or gold — it goes to bronze
            # raw archive + reviews_raw only, then we skip the rest.
            if event["event_type"] == "REVIEW":
                bronze_buffer.append(event)
                if len(bronze_buffer) >= BRONZE_BUFFER_CAP:
                    bronze_buffer = bronze_flush(bronze_buffer, run_metrics)
                review_buffer.append(event)
                if len(review_buffer) >= REVIEW_BUFFER_CAP:
                    review_buffer = review_flush(review_buffer, run_metrics)
                continue

            # ── Silver archive (B-039) ────────────────────────────────
            # Event has passed Gates 1 (JSON), 2 (schema), and 4 (late).
            # Buffer it for the typed fact_booking_events ledger. Unlike
            # bronze (which sits AFTER Gate 3 and so excludes
            # PRICE_CHANGE), silver sits BEFORE Gate 3 so every accepted
            # event type lands in the ledger, including PRICE_CHANGE —
            # the brief's "this row MUST be written (no type filter)"
            # requirement. Flush trigger mirrors bronze.
            silver_buffer.append(event)
            if len(silver_buffer) >= SILVER_BUFFER_CAP:
                silver_buffer = silver_flush(silver_buffer, run_metrics)

            # ── Gate 3: Event type filter ────────────────────────────
            # CHECKIN and PRICE_CHANGE are valid but irrelevant to the
            # city-revenue aggregate. Silent filter — not malformed,
            # not counted as dropped, just not aggregated. Bronze sits
            # AFTER this filter (so bronze excludes PRICE_CHANGE — see
            # the AUDIT note for why we didn't move bronze to match
            # silver: the brief explicitly forbids touching bronze).
            # (Note as of B-034A's B-032 Chunk 2: PROCESSED_EVENT_TYPES
            # already includes CHECKIN; this comment kept the old
            # "CHECKIN and PRICE_CHANGE are valid but irrelevant" line
            # for historical context. The actual filter today only
            # excludes PRICE_CHANGE.)
            if event["event_type"] not in PROCESSED_EVENT_TYPES:
                continue

            # ── Bronze archive (B-038) ────────────────────────────────
            # The event has now passed every gate (JSON parse, schema,
            # late guard, type filter) and will be accumulated below.
            # Append the raw payload to the bronze buffer; flush when
            # the buffer hits BRONZE_BUFFER_CAP so high-throughput runs
            # don't grow it without bound. The periodic flush-check
            # (Phase 2) handles the low-throughput case where the cap
            # is never reached but a small batch is still due.
            #
            # Wrapped only by bronze_flush's own try/except — appending
            # to a list can't fail in practice. The flush call below
            # is the only thing that touches S3, and any exception
            # there is swallowed inside bronze_flush.
            bronze_buffer.append(event)
            if len(bronze_buffer) >= BRONZE_BUFFER_CAP:
                bronze_buffer = bronze_flush(bronze_buffer, run_metrics)

            # ── Accumulate (all gates passed) ────────────────────────
            # Chunk 2: the if/else became if/elif/elif/elif because
            # PROCESSED_EVENT_TYPES now spans four types, not two.
            # BOOKING and CANCELLATION accumulator semantics are
            # UNCHANGED — same fields, same revenue handling. We
            # additionally:
            #   - bump dedicated CHECKIN / CHECKOUT counters on the
            #     window accumulator (feeds the new total_checkins /
            #     total_checkouts columns on agg_hourly_city_stats).
            #   - bump cumulative run-totals for BOOKING / CANCELLATION
            #     on run_metrics (feeds the pipeline_metrics heartbeat).
            # total_events still counts all PROCESSED types — that
            # invariant is exactly why the increment lives outside the
            # if-chain, untouched.
            key = (window_start.isoformat(), event["city"])
            acc = state[key]
            if event["event_type"] == "BOOKING":
                acc["bookings"] += 1
                # `or 0` handles the case where revenue_inr is None
                # (legitimate for some event variants in the future).
                acc["revenue"]  += float(event.get("revenue_inr", 0) or 0)
                run_metrics["bookings"] += 1
            elif event["event_type"] == "CANCELLATION":
                acc["cancellations"] += 1
                run_metrics["cancellations"] += 1
            elif event["event_type"] == "CHECKIN":
                acc["checkins"] += 1
            elif event["event_type"] == "CHECKOUT":
                acc["checkouts"] += 1
            acc["total_events"]  += 1
            acc["last_event_ts"] = max(acc["last_event_ts"], event_ts_secs)

            # Periodic progress log every 100 events.
            if run_metrics["events_consumed"] % 100 == 0:
                print(
                    f"  Consumed {run_metrics['events_consumed']:,} events  |"
                    f"  active windows: {len(state)}  |"
                    f"  max_ts: {datetime.fromtimestamp(max_event_ts, tz=timezone.utc).isoformat()}"
                )

        # ── Phase 2: Flush check ──────────────────────────────────────
        # Runs after each consumer poll timeout. We check periodically
        # rather than on every event because most events don't change
        # which windows are flushable — only events with new max_event_ts
        # do, and checking once every FLUSH_CHECK_SECONDS is plenty.
        now = time.time()
        if (now - t_last_flush) >= FLUSH_CHECK_SECONDS:
            t_last_flush = now

            # ── Pipeline heartbeat (Chunk 2 / B-032 / L-015) ─────────
            # Write one cumulative-metrics snapshot to pipeline_metrics
            # every flush-check tick (~FLUSH_CHECK_SECONDS). The
            # /monitor read path derives events/sec as the delta
            # between successive rows — no rate column needed.
            #
            # Same connect/execute/commit/close cadence as
            # postgres_sink: wasteful at scale, fine at TravelLens's
            # tens-of-windows-per-run volume.
            #
            # CRITICAL: wrapped in its OWN try/except. A heartbeat
            # failure (DB blip, duplicate metric_ts, network hiccup)
            # MUST NEVER crash the consumer loop — losing one snapshot
            # is observability noise; losing the consumer is data loss.
            # We log to stderr and continue, by design.
            #
            # active_windows = len(state) reflects the OPEN windows
            # held in memory at the moment of the heartbeat, BEFORE
            # this tick's flush has had a chance to prune closed ones.
            # That ordering is intentional — it matches the brief's
            # "after t_last_flush = now" placement and gives the
            # monitor a stable cadence-based signal independent of
            # whether any window happened to close on this tick.
            # consumer_lag is intentionally NULL: the column is
            # reserved for a future Kafka-AdminClient lookup.
            try:
                mt_conn = psycopg2.connect(**DB_PARAMS)
                mt_cur  = mt_conn.cursor()
                mt_cur.execute(
                    """
                    INSERT INTO pipeline_metrics
                        (metric_ts, events_consumed, bookings, cancellations,
                         malformed, late, active_windows, max_event_ts, consumer_lag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        datetime.now(timezone.utc),
                        run_metrics["events_consumed"],
                        run_metrics["bookings"],
                        run_metrics["cancellations"],
                        run_metrics["malformed_dropped"],
                        run_metrics["late_dropped"],
                        len(state),
                        (datetime.fromtimestamp(max_event_ts, tz=timezone.utc)
                         if max_event_ts else None),
                        None,  # consumer_lag — reserved, not yet computed
                    ),
                )
                mt_conn.commit()
                mt_conn.close()
            except Exception as exc:
                print(f"  ✗ pipeline_metrics heartbeat error: {exc}", file=sys.stderr)

            # ── Periodic bronze drain (B-038) ─────────────────────────
            # Catches the low-throughput case where the cap is not hit
            # between ticks (e.g. <500 events in 10s). Without this,
            # bronze freshness would degrade to "next time a heavy
            # batch fills the cap" — the opposite of "near-real-time."
            # Bronze flushes BEFORE the window flush below for two
            # reasons: (a) it's smaller and faster (single S3 put_object
            # for many events, vs one put_object per closed window
            # in dual_sink), so we get the raw archive out the door
            # first; (b) bronze_flush is fully isolated — its failure
            # path doesn't affect the watermark window flush that
            # follows.
            bronze_buffer = bronze_flush(bronze_buffer, run_metrics)

            # ── Periodic silver drain (B-039) ─────────────────────────
            # Same rationale as bronze. Silver runs immediately after
            # bronze so both archive layers reach the same logical
            # cadence and neither blocks on the other. Silver writes
            # to Postgres (different external system from bronze's
            # MinIO), so its failure modes are independent — and its
            # own try/except keeps the loop running through DB blips.
            silver_buffer = silver_flush(silver_buffer, run_metrics)

            # ── Periodic review drain (B-030) ─────────────────────────
            # Mirrors the bronze/silver drain pattern. review_flush
            # writes to reviews_raw (Postgres); its own try/except
            # isolates failures from the rest of the flush cycle.
            review_buffer = review_flush(review_buffer, run_metrics)

            watermark_secs = max_event_ts - WATERMARK_GRACE_SECONDS

            # Identify all windows whose end is before the watermark —
            # they're "closed" and ready to emit. List comprehension
            # because we can't delete from a dict while iterating it.
            keys_to_flush = [
                k for k in state
                if (datetime.fromisoformat(k[0]).timestamp()
                    + WINDOW_SIZE_MINUTES * 60) < watermark_secs
            ]

            for k in keys_to_flush:
                ws_iso, city = k
                window_start = datetime.fromisoformat(ws_iso)
                row = build_row(window_start, city, state[k])
                dual_sink(row)
                # CRITICAL: delete the key after flushing. If we left
                # it in state, the late-event guard would still
                # protect us (because the watermark moved past it),
                # but we'd leak memory holding zombie accumulators.
                del state[k]
                run_metrics["windows_flushed"] += 1

        # ── Phase 3: Max-runtime check ────────────────────────────────
        # Self-terminate if --max-runtime is configured and elapsed.
        # This is the bounded-run path that test orchestrators rely on
        # to avoid cross-process SIGINT on Windows. Setting `running`
        # to False here triggers the same graceful-shutdown path as
        # a real SIGINT — all remaining windows still get flushed.
        if MAX_RUNTIME_SECS and (now - t_start) >= MAX_RUNTIME_SECS:
            print(f"\n  Max runtime ({MAX_RUNTIME_SECS}s) reached — initiating graceful shutdown")
            running = False

    # ════════════════════════════════════════════════════════════════════
    # GRACEFUL SHUTDOWN
    # ════════════════════════════════════════════════════════════════════
    # On SIGINT/SIGTERM or max-runtime hit, flush every remaining window
    # even if their watermarks haven't been crossed. This is a deliberate
    # trade-off: we accept partial aggregates for the most recent windows
    # in exchange for not losing data on shutdown. The producer will
    # re-emit nothing — these are the last numbers we have.

    print(f"\nShutting down — flushing {len(state)} remaining window(s) …")
    for (ws_iso, city), acc in list(state.items()):
        window_start = datetime.fromisoformat(ws_iso)
        row = build_row(window_start, city, acc)
        dual_sink(row)
        run_metrics["windows_flushed"] += 1

    # B-038: drain whatever's left in the bronze buffer so the most
    # recent accepted events make it to S3 before we exit. Final flush
    # is force-on-empty-too via the no-op early return in bronze_flush.
    if bronze_buffer:
        print(f"  → final bronze drain: {len(bronze_buffer)} events")
    bronze_buffer = bronze_flush(bronze_buffer, run_metrics)

    # B-039: drain silver too. Same pattern as bronze — no-op when empty,
    # one final Postgres round-trip when not.
    if silver_buffer:
        print(f"  → final silver drain: {len(silver_buffer)} events")
    silver_buffer = silver_flush(silver_buffer, run_metrics)

    # B-030: drain review buffer on shutdown.
    if review_buffer:
        print(f"  → final review drain: {len(review_buffer)} events")
    review_buffer = review_flush(review_buffer, run_metrics)

    consumer.close()

    # ════════════════════════════════════════════════════════════════════
    # RUN SUMMARY
    # ════════════════════════════════════════════════════════════════════
    # Always print the four core counters. Add the TODO breakdown only
    # when something needs human attention — keeps clean runs visually
    # quiet, makes broken runs visually loud.

    print(f"\nConsumer closed.")
    print(f"  Events consumed     : {run_metrics['events_consumed']:,}")
    print(f"  Windows flushed     : {run_metrics['windows_flushed']}")
    print(f"  Late dropped        : {run_metrics['late_dropped']:,}")
    print(f"  Malformed dropped   : {run_metrics['malformed_dropped']:,}")
    # B-038: bronze sink summary. bronze_events should match the
    # accept-path total (events_consumed - malformed - late - any
    # silent type-filter rejects, currently only PRICE_CHANGE).
    bronze_kb = run_metrics["bronze_bytes"] / 1024
    print(f"  Bronze archived     : {run_metrics['bronze_events']:,} events"
          f"  ({run_metrics['bronze_files']} files, {bronze_kb:,.1f} KB)")
    if run_metrics["bronze_failures"] > 0:
        print(f"     ↑ {run_metrics['bronze_failures']} bronze flush(es) failed — "
              f"events dropped; check s3://{S3_BUCKET}/raw_events/ for gaps")
    # B-039: silver sink summary. silver_inserted = silver_attempted on
    # a fresh run; on a redelivered run, silver_duplicates climbs while
    # silver_inserted stays flat (the ON CONFLICT DO NOTHING result).
    print(f"  Silver inserted     : {run_metrics['silver_inserted']:,} rows"
          f"  ({run_metrics['silver_flushes']} flushes, "
          f"{run_metrics['silver_duplicates']:,} dedup'd)")
    if run_metrics["silver_failures"] > 0:
        print(f"     ↑ {run_metrics['silver_failures']} silver flush(es) failed — "
              f"rows dropped; check fact_booking_events for gaps")
    # B-030: review sink summary.
    print(f"  Reviews inserted    : {run_metrics['review_inserted']:,} rows"
          f"  ({run_metrics['review_flushes']} flushes, "
          f"{run_metrics['review_duplicates']:,} dedup'd)")
    if run_metrics["review_failures"] > 0:
        print(f"     ↑ {run_metrics['review_failures']} review flush(es) failed — "
              f"rows dropped; check reviews_raw for gaps")

    if run_metrics["malformed_dropped"] > 0:
        # This is the signal a teammate sees the next morning. They
        # go to S3 with this exact path, sort by reason, find files,
        # diagnose the producer bug. The 'TODO' marker is intentional —
        # it should look like an action item, not a passive log line.
        print(f"     ↑ non-zero = TODO for team, check s3://{S3_BUCKET}/malformed_events/")
        # Sort by frequency descending so the dominant failure mode
        # appears first.
        for reason, count in sorted(run_metrics["malformed_by_reason"].items(),
                                    key=lambda x: -x[1]):
            print(f"     └─ {reason:25s}: {count:,}")

    if run_metrics["late_dropped"] > 0:
        # Late events are recoverable — the reconcile job exists for
        # exactly this case. Point the operator at the right script
        # so they don't have to remember the name.
        print(f"     ↑ {run_metrics['late_dropped']:,} late events quarantined to s3://{S3_BUCKET}/late_events/ — run reconcile_late_events.py to merge them")


if __name__ == "__main__":
    main()