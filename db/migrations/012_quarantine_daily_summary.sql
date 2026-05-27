-- Migration 012 — quarantine_daily_summary
-- B-033: one row per UTC calendar day; populated by the
-- quarantine_daily_rollup Airflow DAG.  Enables the monitor's
-- _monitor_quarantine() to do an O(1) Postgres range SUM instead of an
-- unbounded S3 bucket scan on every page load.

CREATE TABLE IF NOT EXISTS quarantine_daily_summary (
    summary_date    DATE        PRIMARY KEY,
    malformed_count INT         NOT NULL DEFAULT 0,
    late_count      INT         NOT NULL DEFAULT 0,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Verify
SELECT 'migration 012 applied' AS status, COUNT(*) AS existing_rows
FROM quarantine_daily_summary;
