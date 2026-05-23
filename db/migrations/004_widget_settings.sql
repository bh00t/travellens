-- Migration 004 — widget settings columns
-- Part of: TravelLens Phase 5 — Dashboard enhancements
-- File:    db/migrations/004_widget_settings.sql
--
-- Adds the `width` column used by the settings popup (Normal / Full row).
-- refresh_interval_minutes already exists from migration 003 and is reused.
--
-- Run with:
--   docker exec travellens-postgres psql -U travellens -d travellens \
--     -f /db/migrations/004_widget_settings.sql
--
-- Idempotent — safe to run multiple times (IF NOT EXISTS).

ALTER TABLE dashboard_widgets
    ADD COLUMN IF NOT EXISTS width VARCHAR(10) NOT NULL DEFAULT 'normal'
    CHECK (width IN ('normal', 'full'));

-- Verify
SELECT 'migration 004 applied' AS status,
       COUNT(*) AS widget_count
FROM dashboard_widgets;
