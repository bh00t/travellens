-- Migration 017 — per-review sentiment columns (B-026 Stage 1)
--
-- B-026 replaces the B-062 rating proxy with REAL per-review sentiment as the
-- fix for L-012 (sentiment-topic conflation in semantic review search). Star
-- rating is a proven-bad proxy: genuine complaints live in mixed-sentiment 3★
-- reviews that the ≤2★ / ≥4★ rating filter can never reach (29.6% of the corpus
-- is 3★).
--
-- Two append-only columns on reviews_raw, populated by
-- scripts/review_sentiment_scorer.py — CardiffNLP twitter-roberta-base-sentiment-
-- latest, pinned revision d616e2bdfcdb0ca89d9b6efc4909e1db063af290, loaded via
-- safetensors (the .bin checkpoint is blocked on the pinned torch 2.3.0 by
-- CVE-2025-32434; we do NOT upgrade torch — see L-008).
--
-- Sentiment is INDEPENDENT of the embedding: these columns are additive and do
-- NOT require re-embedding the 133K vectors. NULL sentiment_label = "unscored"
-- and is the natural backfill cursor (mirrors `embedding IS NULL`).
--
-- STAGE 1 ships the columns + one-time backfill only. The retrieval swap
-- (Stage 2: replace the rating predicate in _search_reviews with sentiment_label)
-- and the run.py 7th-proc wiring (Stage 3) are gated on this stage's verification.

ALTER TABLE reviews_raw ADD COLUMN IF NOT EXISTS sentiment_label VARCHAR(8);   -- 'positive' | 'negative' | 'neutral'; NULL = unscored
ALTER TABLE reviews_raw ADD COLUMN IF NOT EXISTS sentiment_score NUMERIC(4,3); -- model confidence 0.000–1.000 for the chosen label

-- Partial index for the Stage-2 retrieval predicate
-- (WHERE sentiment_label = 'negative' / 'positive'). Excludes unscored (NULL)
-- rows so the index stays small. Created before the backfill runs, so it matches
-- zero rows at build time and is effectively instant — no CONCURRENTLY needed.
CREATE INDEX IF NOT EXISTS idx_reviews_raw_sentiment
    ON reviews_raw (sentiment_label)
    WHERE sentiment_label IS NOT NULL;

COMMENT ON COLUMN reviews_raw.sentiment_label IS
  'B-026: 3-class CardiffNLP sentiment (positive/negative/neutral). NULL = unscored '
  '(backfill cursor). Drives the Stage-2 retrieval polarity filter, replacing the '
  'B-062 rating proxy.';
COMMENT ON COLUMN reviews_raw.sentiment_score IS
  'B-026: model confidence 0.000-1.000 for the chosen sentiment_label. Stored so a '
  'future "strongly negative" confidence gate can be added without a re-backfill.';

-- Verify
SELECT 'migration 017 applied' AS status,
       COUNT(*) FILTER (WHERE sentiment_label IS NOT NULL) AS scored_rows,
       COUNT(*) AS total_rows
FROM reviews_raw;
