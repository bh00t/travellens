-- B-047 cap-upsert semantic change (follow-on to migration 014).
--
-- Migration 014 created `sim_daily_counter`.  The producer's startup UPSERT
-- in `scripts/kafka_event_producer.py:ensure_daily_counter_row` originally
-- used:
--     ON CONFLICT (counter_date) DO NOTHING
--
-- That made `cap` STICKY — the first session of the day wrote cap, and any
-- later session at a different `--rate-multiplier` could not change today's
-- ceiling without an IST-midnight rollover.  Owner override (2026-05-28):
-- caps must be LAST-WRITE-WINS across same-day sessions, so a follow-up
-- `python run.py --rate-multiplier 3` over a bare `python run.py` (=1)
-- raises today's cap immediately to 3M.  `events_emitted` is intentionally
-- NOT overwritten — it continues to accumulate across all sessions in the
-- IST day (correct, since the cap is a cumulative ceiling check).
--
-- The actual change ships in scripts/kafka_event_producer.py — the INSERT
-- there now reads:
--     ON CONFLICT (counter_date) DO UPDATE
--       SET cap = EXCLUDED.cap, updated_at = NOW()
-- (events_emitted intentionally absent from the SET clause).  This migration
-- only refreshes the COMMENT on the cap column so the migration ledger
-- surfaces the semantic flip to anyone reading `db/migrations/`.

COMMENT ON COLUMN sim_daily_counter.cap IS
  'B-047 + migration 016: 1_000_000 × rate_multiplier; LAST-WRITE-WINS '
  'across same-day sessions. Every producer-startup UPSERT overwrites cap '
  'with EXCLUDED.cap so a follow-on --rate-multiplier change takes effect '
  'immediately. events_emitted is NOT overwritten and continues to '
  'accumulate.';
