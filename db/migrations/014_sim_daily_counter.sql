-- B-047 (Stage 2a): per-day TOTAL-EVENTS guardrail for the forward generator.
--
-- daily_cap = 1_000_000 (10 lakh) × rate_multiplier
-- Both knobs scale together so a high-x run does not exhaust the cap before
-- noon and "go silent" mid-day (the diurnal integral at x=1 is ~868K events
-- per UTC-IST day; the 1M cap leaves ~13% headroom; at x=5 daily integral is
-- ~4.32M against a cap of 5M, same 13% headroom).
--
-- On cap-hit, the producer stops emitting new BOOKING and PRICE_CHANGE and
-- sleeps until midnight IST.  Lifecycle catch-up resumes the next day with
-- event_ts = NOW() (never backdated) — see scripts/kafka_event_producer.py.

CREATE TABLE IF NOT EXISTS sim_daily_counter (
    counter_date   DATE        PRIMARY KEY,
    events_emitted INTEGER     NOT NULL DEFAULT 0,
    cap            INTEGER     NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  sim_daily_counter         IS 'B-047 forward generator: per-day total-event guardrail. counter_date is in IST.';
COMMENT ON COLUMN sim_daily_counter.cap     IS '1_000_000 × rate_multiplier at the row''s creation; not updated on later sessions of the same day.';
