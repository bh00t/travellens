-- Migration 011: index on reviews_raw(event_date) for /monitor date-scoped queries
--
-- WHY: _monitor_reviews and _monitor_embeddings both filter reviews_raw by
-- event_date. Without an index both queries do a Seq Scan over ~130K rows
-- (56 ms each = 112 ms total on every /monitor/data poll). The seed rows
-- (~30K rows) have NULL event_date and are excluded from both queries by the
-- date filter, so a partial index over IS NOT NULL rows only is smaller and
-- faster than a full index.
--
-- WHAT: a single partial btree index covers both queries. The monitor only
-- needs (event_date) to isolate a date range; the downstream aggregate
-- (COUNT(*) FILTER (WHERE embedding IS NOT NULL)) fetches so few heap rows
-- after the index scan that a covering index buys nothing.
--
-- CONCURRENTLY: safe on a live table (does not lock writes). Required because
-- reviews_raw is written by review_embedder and stream_consumer continuously.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reviews_raw_event_date
    ON reviews_raw (event_date)
    WHERE event_date IS NOT NULL;
