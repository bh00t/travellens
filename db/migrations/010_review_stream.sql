-- 010_review_stream.sql
--
-- Extends reviews_raw to support booking-tied stream and history reviews
-- (B-030: REVIEW as a stream event).
--
-- New columns (all nullable on new inserts except record_source):
--   booking_id      UUID        — FK semantics → fact_bookings (no hard FK for ingest speed)
--   customer_id     VARCHAR(12) — FK semantics → dim_customer
--   review_stage    VARCHAR(20) — booking state at review time: booked|checked_in|checked_out|cancelled
--   review_channel  VARCHAR(100)— where the review was posted (OTA app name or direct channel)
--   event_ts        TIMESTAMPTZ — wall-clock UTC when the review event was emitted
--   event_date      DATE        — sim-day or calendar day the review represents
--   record_source   VARCHAR(10) — 'seed' (original Kaggle corpus) | 'history' (backfill) | 'stream' (live)
--
-- Existing 30 K Kaggle seed rows → record_source = 'seed' (via DEFAULT), all new columns NULL.
--
-- Append-only migration; never modifies any previously applied migration.

ALTER TABLE reviews_raw
    ADD COLUMN IF NOT EXISTS booking_id     UUID,
    ADD COLUMN IF NOT EXISTS customer_id    VARCHAR(12),
    ADD COLUMN IF NOT EXISTS review_stage   VARCHAR(20),
    ADD COLUMN IF NOT EXISTS review_channel VARCHAR(100),
    ADD COLUMN IF NOT EXISTS event_ts       TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS event_date     DATE,
    ADD COLUMN IF NOT EXISTS record_source  VARCHAR(10) NOT NULL DEFAULT 'seed';

-- Index for booking-scoped lookups (consistency checks, B-030b embedding backfill).
CREATE INDEX IF NOT EXISTS idx_reviews_raw_booking_id
    ON reviews_raw (booking_id)
    WHERE booking_id IS NOT NULL;

-- Index for record_source — separates seed / history / stream slices quickly.
CREATE INDEX IF NOT EXISTS idx_reviews_raw_record_source
    ON reviews_raw (record_source);
