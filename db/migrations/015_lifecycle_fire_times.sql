-- B-047 (Stage 2a): per-row lifecycle fire-time stamps so each new BOOKING in
-- sim_open_bookings carries the exact wall-clock moment its CHECKIN /
-- CHECKOUT / (optional) CANCELLATION / REVIEW events should fire under the
-- per-event-type IST hour distributions (see scripts/kafka_event_producer.py).
--
-- The forward loop's "any row due now?" check is `SELECT … WHERE
-- {state-appropriate}_fire_ts <= NOW()` — each fire_ts col is indexed by a
-- partial index narrowed to the lifecycle state where it's the live signal.
--
-- Catch-up rule: when a fire_ts is already in the past at restart (producer
-- was off), the event fires as soon as the bucket has capacity with event_ts
-- = NOW().  Original stamped time is never written to the wire.
--
-- A new state 'REVIEW_PENDING' lets a booking outlive CHECKOUT/CANCELLATION
-- until its deferred REVIEW fires; the row is deleted on REVIEW emit
-- (or sooner if review_fire_ts stays NULL because the negativity-bias draw
-- declined to generate a review).

ALTER TABLE sim_open_bookings
    ADD COLUMN IF NOT EXISTS checkin_fire_ts  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS checkout_fire_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancel_fire_ts   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS review_fire_ts   TIMESTAMPTZ;

ALTER TABLE sim_open_bookings
    DROP CONSTRAINT IF EXISTS sim_open_bookings_state_chk;
ALTER TABLE sim_open_bookings
    ADD CONSTRAINT sim_open_bookings_state_chk
    CHECK (state IN ('BOOKED', 'CHECKED_IN', 'REVIEW_PENDING'));

CREATE INDEX IF NOT EXISTS idx_sim_open_checkin_fire
    ON sim_open_bookings (checkin_fire_ts)
    WHERE state = 'BOOKED' AND checkin_fire_ts IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sim_open_checkout_fire
    ON sim_open_bookings (checkout_fire_ts)
    WHERE state = 'CHECKED_IN' AND checkout_fire_ts IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sim_open_cancel_fire
    ON sim_open_bookings (cancel_fire_ts)
    WHERE state = 'BOOKED' AND cancel_fire_ts IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sim_open_review_fire
    ON sim_open_bookings (review_fire_ts)
    WHERE review_fire_ts IS NOT NULL;

COMMENT ON COLUMN sim_open_bookings.checkin_fire_ts  IS 'B-047 forward generator: wall-clock IST time at which the CHECKIN event should fire (sampled from CHECKIN hour distribution on checkin_date).';
COMMENT ON COLUMN sim_open_bookings.checkout_fire_ts IS 'B-047 forward generator: same for CHECKOUT on checkout_date.';
COMMENT ON COLUMN sim_open_bookings.cancel_fire_ts   IS 'B-047 forward generator: NULL unless the deterministic cancel-decision (Random(booking_id|cancel|daily_seed) < P_CANCEL) selects this booking; if so, time in [booking_ts, checkin_date] from CANCELLATION distribution.';
COMMENT ON COLUMN sim_open_bookings.review_fire_ts   IS 'B-047 forward generator: NULL until CHECKOUT or CANCELLATION emits; then set to a time tonight in 20-23 IST from REVIEW distribution (deferred review).';
