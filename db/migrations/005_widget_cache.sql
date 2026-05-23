-- Migration 005 — widget cache + frozen SQL
-- Part of: TravelLens Phase 5 — Dashboard (B-022 cache + frozen SQL)
-- File:    db/migrations/005_widget_cache.sql
--
-- What this creates:
--   Two new columns on dashboard_widgets that decouple the dashboard read path
--   from compute (LLM + SQL) on every load.
--
--     generated_sql      — the SQL Ollama produced when the widget was pinned.
--                          Frozen at pin time; every refresh runs THIS SQL,
--                          never regenerates from the prompt. Eliminates
--                          non-deterministic SQL drift between refreshes.
--     last_result_json   — JSONB cache of the most recent result, including
--                          the widget config the renderer expects. Lets the
--                          dashboard read path embed cached results without
--                          touching Ollama or Postgres data queries.
--
--   last_refreshed_at already exists from migration 003 and is reused as the
--   cache freshness timestamp.
--
-- Production swap (Phase 6, B-024): last_result_json → Redis keyed by widget_id,
-- TTL = refresh_interval_minutes. JSONB column is a deliberate scoped-down
-- stand-in for Redis, not the intended production answer.
--
-- Run with:
--   docker exec travellens-postgres psql -U travellens -d travellens \
--     -f /db/migrations/005_widget_cache.sql
--
-- Idempotent — safe to run multiple times (IF NOT EXISTS).

ALTER TABLE dashboard_widgets
    ADD COLUMN IF NOT EXISTS generated_sql    TEXT,
    ADD COLUMN IF NOT EXISTS last_result_json JSONB;

-- Verify
SELECT 'migration 005 applied' AS status,
       COUNT(*) AS widget_count
FROM dashboard_widgets;
