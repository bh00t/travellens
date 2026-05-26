-- Migration 008 — lifecycle events (silver) + open-bookings simulator state
-- Part of: TravelLens Phase 2 / Phase 6 — booking lifecycle data foundation
-- File:    db/migrations/008_lifecycle_events.sql
--
-- What this creates:
--
--   1. `fact_booking_events` — SILVER one-row-per-event table for the full
--      booking lifecycle (BOOKING / CHECKIN / CHECKOUT / CANCELLATION today;
--      PRICE_CHANGE / REVIEW reserved for later). History (exploded from
--      fact_bookings via scripts/generate_lifecycle_history.py) AND the live
--      stream both write here. The `source` column ('history' | 'stream')
--      is the only thing that distinguishes them; downstream queries treat
--      a closed booking the same whichever source landed it.
--
--   2. `sim_open_bookings` — durable simulator state. One row per booking
--      that is NOT yet CHECKED_OUT or CANCELLED. The stream simulator
--      consumes this table to decide which open booking to advance on each
--      tick (CHECKIN → CHECKED_IN → CHECKOUT, or BOOKED → CANCELLATION).
--      It is the producer's working set — what's "live in the lobby."
--
-- Why two tables and not one:
--   `fact_booking_events` is an APPEND-ONLY ledger (immutable history of
--   what happened). `sim_open_bookings` is MUTABLE state (rows are deleted
--   when a booking checks out or cancels). Mixing them would lose the
--   audit trail. The same pattern as event-sourcing + a materialised
--   projection.
--
-- Why nullable type-specific columns (cancellation_reason, rating, etc.):
--   A single events table is the simplest schema that supports every
--   event type; nullable columns hold per-type payload. The alternative
--   (one table per type) would force a UNION ALL on every analytical
--   query — measurably worse and harder to teach.
--
-- Run with:
--   docker exec -i travellens-postgres psql -U travellens -d travellens \
--     < db/migrations/008_lifecycle_events.sql
--
-- Idempotent — safe to re-run (CREATE TABLE IF NOT EXISTS, CREATE INDEX
-- IF NOT EXISTS). Re-running does NOT delete rows; the generator script
-- handles its own --reset.

CREATE TABLE IF NOT EXISTS fact_booking_events (
    event_id            UUID         PRIMARY KEY,
    event_type          VARCHAR(20)  NOT NULL,
    booking_id          UUID,
    customer_id         VARCHAR(20),
    hotel_id            VARCHAR(20),
    city                VARCHAR(50),
    room_type_id        UUID,

    event_ts            TIMESTAMPTZ  NOT NULL,
    event_date          DATE         NOT NULL,

    checkin_date        DATE,
    checkout_date       DATE,
    nights              SMALLINT,
    num_guests          SMALLINT,
    nightly_rate_inr    NUMERIC(10,2),
    revenue_inr         NUMERIC(10,2),
    booking_source      VARCHAR(50),
    payment_mode        VARCHAR(20),

    cancellation_reason VARCHAR(40),

    rating              NUMERIC(3,1),
    review_channel      VARCHAR(40),
    review_text         TEXT,

    old_price_inr       NUMERIC(10,2),
    new_price_inr       NUMERIC(10,2),

    source              TEXT         NOT NULL,
    ingested_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT fact_booking_events_event_type_chk
        CHECK (event_type IN ('BOOKING','CHECKIN','CHECKOUT','CANCELLATION','PRICE_CHANGE','REVIEW')),
    CONSTRAINT fact_booking_events_source_chk
        CHECK (source IN ('history','stream'))
);

CREATE INDEX IF NOT EXISTS idx_fact_booking_events_booking_id  ON fact_booking_events (booking_id);
CREATE INDEX IF NOT EXISTS idx_fact_booking_events_event_date  ON fact_booking_events (event_date);
CREATE INDEX IF NOT EXISTS idx_fact_booking_events_hotel_id    ON fact_booking_events (hotel_id);
CREATE INDEX IF NOT EXISTS idx_fact_booking_events_source      ON fact_booking_events (source);


CREATE TABLE IF NOT EXISTS sim_open_bookings (
    booking_id        UUID         PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    hotel_id          VARCHAR(20)  NOT NULL,
    city              VARCHAR(50),
    room_type_id      UUID         NOT NULL,
    checkin_date      DATE         NOT NULL,
    checkout_date     DATE         NOT NULL,
    nights            SMALLINT     NOT NULL,
    num_guests        SMALLINT     NOT NULL,
    nightly_rate_inr  NUMERIC(10,2) NOT NULL,
    payment_mode      VARCHAR(20),
    booking_source    VARCHAR(50),
    state             TEXT         NOT NULL,
    booked_event_ts   TIMESTAMPTZ  NOT NULL,
    source            TEXT         NOT NULL DEFAULT 'history',

    CONSTRAINT sim_open_bookings_state_chk
        CHECK (state IN ('BOOKED','CHECKED_IN'))
);

CREATE INDEX IF NOT EXISTS idx_sim_open_bookings_checkin   ON sim_open_bookings (checkin_date);
CREATE INDEX IF NOT EXISTS idx_sim_open_bookings_checkout  ON sim_open_bookings (checkout_date);
CREATE INDEX IF NOT EXISTS idx_sim_open_bookings_state     ON sim_open_bookings (state);

-- Verify
SELECT 'migration 008 applied'                       AS status,
       (SELECT COUNT(*) FROM fact_booking_events)    AS event_rows,
       (SELECT COUNT(*) FROM sim_open_bookings)      AS open_rows;
