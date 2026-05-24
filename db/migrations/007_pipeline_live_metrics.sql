-- Migration 007 — pipeline live metrics + extended hourly stream counts
-- Part of: TravelLens Phase 7 / Phase 2 hardening (B-032 live throughput; resolves L-015)
-- File:    db/migrations/007_pipeline_live_metrics.sql
--
-- What this creates:
--
--   1. Extends `agg_hourly_city_stats` with four new event-count columns so the
--      hourly stream aggregate captures every event type the consumer sees, not
--      just bookings (resolves the data gap behind L-014 once the consumer is
--      taught to populate them):
--
--        total_checkins      — CHECKIN events landed in the window
--        total_checkouts     — CHECKOUT events landed in the window
--        total_cancellations — explicit CANCELLATION events (separate from
--                              cancellation_rate, which is the booking ratio)
--        total_reviews       — REVIEW events landed in the window
--
--      `cancellation_rate` and `ingestion_ts` already exist on this table from
--      Phase 2; they are re-declared here with IF NOT EXISTS purely so this
--      single migration file is a complete record of the columns the live
--      pipeline depends on (no-op on an existing DB, schema-creating on a
--      freshly bootstrapped one).
--
--   2. Creates `pipeline_metrics` — an append-only heartbeat table the consumer
--      writes one row to every ~5–10s. Powers the monitor's LIVE THROUGHPUT
--      section (events/sec, lag, consumer-alive). This is the queryable
--      mid-run snapshot that today's in-memory `run_metrics` counters can't
--      provide (the L-015 root cause).
--
--        metric_ts        — heartbeat timestamp, PRIMARY KEY
--        events_consumed  — cumulative consumed-since-start
--        bookings         — cumulative BOOKING count
--        cancellations    — cumulative CANCELLATION count
--        malformed        — cumulative malformed (Gate 1/2) drops
--        late             — cumulative late-watermark drops
--        active_windows   — open windows held in memory at heartbeat time
--        max_event_ts     — latest event timestamp seen so far
--        consumer_lag     — Kafka consumer lag (nullable; may be unavailable)
--
--      Events/sec is derived in the read path as the delta between the latest
--      two rows divided by their interval — no need to store a rate column.
--
-- Why:
--   Today the consumer's counters live only in memory and print only at
--   shutdown, so the monitor can't show real-time throughput — it infers
--   activity from what landed in the hourly aggregate, which only moves when
--   a window flushes. This migration is the storage half of the fix; the
--   consumer write-path and the monitor read-path are separate chunks.
--
-- Run with:
--   docker exec -i travellens-postgres psql -U travellens -d travellens \
--     < db/migrations/007_pipeline_live_metrics.sql
--
-- Idempotent — safe to re-run (ADD COLUMN IF NOT EXISTS, CREATE TABLE IF NOT EXISTS).

ALTER TABLE agg_hourly_city_stats
    ADD COLUMN IF NOT EXISTS total_checkins      INTEGER,
    ADD COLUMN IF NOT EXISTS total_checkouts     INTEGER,
    ADD COLUMN IF NOT EXISTS total_cancellations INTEGER,
    ADD COLUMN IF NOT EXISTS total_reviews       INTEGER,
    ADD COLUMN IF NOT EXISTS cancellation_rate   NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS ingestion_ts        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_ts       TIMESTAMP   PRIMARY KEY,
    events_consumed BIGINT      NOT NULL DEFAULT 0,
    bookings        BIGINT      NOT NULL DEFAULT 0,
    cancellations   BIGINT      NOT NULL DEFAULT 0,
    malformed       BIGINT      NOT NULL DEFAULT 0,
    late            BIGINT      NOT NULL DEFAULT 0,
    active_windows  INTEGER     NOT NULL DEFAULT 0,
    max_event_ts    TIMESTAMP,
    consumer_lag    BIGINT
);

-- Verify
SELECT 'migration 007 applied'                              AS status,
       (SELECT COUNT(*) FROM agg_hourly_city_stats)         AS hourly_rows,
       (SELECT COUNT(*) FROM pipeline_metrics)              AS metric_rows;
