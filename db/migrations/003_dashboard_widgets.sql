-- Migration 003 — dashboard_widgets table
-- Part of: TravelLens Phase 5 — Dashboard
-- File:    db/migrations/003_dashboard_widgets.sql
--
-- What this creates:
--   The dashboard_widgets table stores every pinned widget — the prompt that
--   created it, the widget type, the refresh interval, and timestamps.
--   This is the only Postgres table that the render layer touches directly.
--   All data queries go through ai.main.answer().
--
-- Run with:
--   docker exec travellens-postgres psql -U travellens -d travellens \
--     -f /db/migrations/003_dashboard_widgets.sql
--
-- Idempotent — safe to run multiple times (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS dashboard_widgets (
    widget_id                SERIAL PRIMARY KEY,
    prompt                   TEXT NOT NULL,
    widget_type              VARCHAR(20) NOT NULL
                             CHECK (widget_type IN (
                                 'bar_chart', 'line_chart', 'stat_card',
                                 'table', 'semantic'
                             )),
    title                    VARCHAR(200),
    refresh_interval_minutes INTEGER NOT NULL DEFAULT 0,
    pinned_at                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_refreshed_at        TIMESTAMP,
    display_order            INTEGER NOT NULL DEFAULT 0
);

-- Index for ordered dashboard display
CREATE INDEX IF NOT EXISTS idx_dashboard_widgets_order
    ON dashboard_widgets (display_order ASC, pinned_at DESC);

-- Verify
SELECT
    'dashboard_widgets created' AS status,
    COUNT(*) AS existing_widgets
FROM dashboard_widgets;
