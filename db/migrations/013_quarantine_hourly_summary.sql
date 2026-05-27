-- Migration 013 — quarantine_hourly_summary
-- B-044: supersedes migration 012 (quarantine_daily_summary, dropped here).
-- One row per UTC hour, populated by scripts/quarantine_hourly_rollup.py
-- (run.py background proc, 5-min loop, pg_try_advisory_lock 7400060).
--
-- is_final=TRUE once now >= end_of_hour + 10-min grace; the derived
-- watermark (MAX final row) drives incremental backfill so no separate
-- cursor table is needed.
--
-- Monitor reads: SELECT summary_date, SUM(malformed_count), SUM(late_count)
--   FROM quarantine_hourly_summary WHERE summary_date BETWEEN %s AND %s
--   GROUP BY summary_date;   ← O(1), no S3 on the request path.

DROP TABLE IF EXISTS quarantine_daily_summary;

CREATE TABLE IF NOT EXISTS quarantine_hourly_summary (
    summary_date    DATE        NOT NULL,
    summary_hour    SMALLINT    NOT NULL,       -- 0–23 UTC
    malformed_count INT         NOT NULL DEFAULT 0,
    late_count      INT         NOT NULL DEFAULT 0,
    is_final        BOOLEAN     NOT NULL DEFAULT FALSE,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (summary_date, summary_hour)
);

-- Verify
SELECT 'migration 013 applied' AS status,
       COUNT(*) AS hourly_rows,
       (SELECT CASE WHEN to_regclass('quarantine_daily_summary') IS NULL
                    THEN 'dropped' ELSE 'still_exists' END) AS daily_table;
