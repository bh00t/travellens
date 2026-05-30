# Phase 3 — Embeddings: pgvector + Semantic Search Foundation

> **Stack:** Python 3.11 · sentence-transformers · pgvector · Postgres 16 · Docker  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Script:** `scripts/generate_embeddings.py`  
> **Status:** [ ] In progress / [x] Complete  

> **HISTORY DOCUMENT** — This records how Phase 3 was originally built. For the current embedding setup and index configuration, see [CLAUDE.md](../CLAUDE.md) · [datamodel.md](../datamodel.md) · [backlog.md](backlog.md).

---


## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Files this phase
creates / touches:

- **CREATE** `scripts/generate_embeddings.py`
- **CREATE** `scripts/semantic_playground.py`
- **CREATE (in-DB)** `idx_reviews_embedding` — IVFFlat index on `reviews_raw.embedding`, created by `generate_embeddings.py` after the batch embed completes.

## OBJECTIVE

Populate the `embedding vector(384)` column on all 30K rows in `reviews_raw` using
`sentence-transformers/all-MiniLM-L6-v2`. Build an IVFFlat index for cosine similarity.

This is the foundation for the semantic search path in Phase 4. Without embeddings,
queries like *"find reviews about noisy AC"* can only do exact keyword matching.
With embeddings, pgvector finds reviews that **mean** the same thing even if they use
different words — "room was filthy" and "hygiene was terrible" are semantically close
even though they share zero words.

---

## PREREQUISITES

- Phase 1 complete: `reviews_raw` has 30K rows, `embedding vector(384)` column exists,
  all values currently NULL
- Phase 2 complete: Docker stack running — Postgres 16 + pgvector, Kafka, MinIO
- `.venv` active with all Phase 3 dependencies installed:

```bash
pip install sentence-transformers==3.0.1 torch==2.3.0 pgvector==0.3.6 tqdm==4.66.4
```

- CUDA available — verify before running:

```bash
python -c "import torch; print('CUDA:', torch.cuda.is_available(), '|', torch.cuda.get_device_name(0))"
# Expected: CUDA: True | NVIDIA GeForce RTX 3070
```

---

## DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| `generate_embeddings.py` | `scripts/` | Script exists, runs without error |
| 30K embeddings | `reviews_raw.embedding` | Zero NULL rows remain |
| IVFFlat index | Postgres (in-DB) | `idx_reviews_embedding` exists in `pg_indexes` |

---

## ARCHITECTURE DECISIONS (ORIGINAL)

### Why `all-MiniLM-L6-v2`

384-dimensional output, 6-layer distilled model, trained on paraphrase tasks. Two reviews
that describe the same complaint in different words end up geometrically close in vector
space — that's the core property semantic search depends on. ~90MB on disk, runs fully
offline, no API cost. On RTX 3070 with batch_size=128 it processes ~1,800 reviews/sec —
the full 30K job finishes in ~17 seconds on GPU.

Larger alternatives like `all-mpnet-base-v2` (768 dims) offer marginally better recall on
subtle nuance but are 2× slower and 2× the storage. Not worth it at this scale for hotel
review search.

### Why pgvector + IVFFlat, not a dedicated vector store

At 30K vectors, a dedicated store (Pinecone, Weaviate, Qdrant) would add a second service,
a second API, and a join problem — you'd need to reconcile vector results against Postgres
rows by `hotel_id`. pgvector keeps everything in one place. With an IVFFlat index, cosine
similarity queries run under 50ms — fast enough that the bottleneck in Phase 4 will be the
LLM call, not the vector search.

Honest limitation: IVFFlat recall degrades past ~1M vectors. At 30K we are nowhere near
that ceiling.

### Why `lists = 30`, not 100

pgvector rule of thumb: `lists ≈ rows / 1000`. For 30K rows that's 30. The blueprint
specifies 100 — that's over-partitioned for this dataset and increases query time without
improving recall. `lists = 30` is the correct value here.

### Why batch_size = 128

Safe for RTX 3070 (8GB VRAM) at typical Indian hotel review lengths (80–150 chars). Stays
well within the model's 256-token hard limit. Going to 256 gives marginal throughput gain
with real OOM risk on batches that contain longer reviews.

### Why truncate at 1,000 chars, not skip

`all-MiniLM-L6-v2` silently truncates at 256 tokens (~1,000 chars) internally anyway.
Making it explicit means we can log how many reviews hit the ceiling. Skipping long reviews
leaves them with `embedding = NULL` — they become invisible to semantic search entirely.
Truncate, never skip.

### Why idempotent (`WHERE embedding IS NULL`)

The job fetches only unembedded rows. If it crashes halfway, restart picks up from where
it left off. Future reviews inserted with `embedding = NULL` are handled automatically by
the same WHERE clause — no code changes needed when new data arrives.

### Why bulk `executemany` per batch

One Postgres round-trip per 128 rows instead of 128 separate UPDATE calls. At localhost
latency the saving is small but the pattern is correct and matters when the DB is not
co-located.

### Why build the index last

IVFFlat runs k-means clustering on existing vectors at build time. Building on an empty or
partially-filled table produces bad clusters and degrades recall. Always populate all data
first, then build the index.

---

## STEPS

### Step 1 — Verify CUDA

```bash
cd C:\Users\risha\Desktop\Code\repo\travellens
.venv\Scripts\activate
python -c "import torch; print('CUDA:', torch.cuda.is_available())"
```

If `False`: check Nvidia drivers — CUDA 12.x is required. The job still runs on CPU but
takes ~25 minutes instead of ~17 seconds.

---

### Step 2 — Confirm embedding column is all NULL (pre-run check)

```bash
docker exec -it travellens-postgres psql -U travellens -d travellens
```

```sql
SELECT COUNT(*) FROM reviews_raw WHERE embedding IS NULL;
-- Expected: 30000
```

---

### Step 3 — Run the embedding job

```bash
python scripts/generate_embeddings.py
```

Expected output:

```
21:11:41 [INFO] Device: CUDA
21:11:41 [INFO] Loading model: all-MiniLM-L6-v2
21:11:45 [INFO] Rows to embed: 30000
Embedding: 100%|████████████████| 30000/30000 [00:17<00:00, 1800 reviews/s]
21:36:24 [INFO] Embedding complete. Truncated: 1500 / 30000 (5.0%)
21:36:24 [INFO] Building IVFFlat index (lists=30)...
21:36:25 [INFO] IVFFlat index built successfully.
21:36:25 [INFO] Done.
```

> **Build/seed sequence.** `generate_embeddings` runs as **Stage C2** of the canonical post-load sequence — see [`datamodel.md` → Regenerating the Dataset](../datamodel.md#regenerating-the-dataset) for when it runs relative to the base load and the B-046 expansion.

---

### Step 4 — Run acceptance tests

Connect to Postgres:

```bash
docker exec -it travellens-postgres psql -U travellens -d travellens
```

```sql
-- 1. Zero NULL embeddings remaining (must return 0)
SELECT COUNT(*) FROM reviews_raw WHERE embedding IS NULL;

-- 2. Correct dimensionality (must return 384)
SELECT array_length(embedding::float4[], 1)
FROM reviews_raw LIMIT 1;

-- 3. IVFFlat index exists (must return idx_reviews_embedding)
SELECT indexname FROM pg_indexes
WHERE tablename = 'reviews_raw'
  AND indexname = 'idx_reviews_embedding';

-- 4. Semantic search smoke test
-- First row must be the seed review itself (similarity = 1.0)
-- Remaining rows must be topically related, not random
SELECT
    r.rating,
    LEFT(r.review_text, 100) AS preview,
    ROUND((1 - (r.embedding <=> (
        SELECT embedding FROM reviews_raw LIMIT 1
    )))::numeric, 4) AS similarity
FROM reviews_raw r
ORDER BY r.embedding <=> (SELECT embedding FROM reviews_raw LIMIT 1)
LIMIT 5;
```

All 4 must pass before moving to Phase 4.

---

### Step 5 — Commit

STOP — the owner commits manually. Do not run `git add` or `git commit` from
this agent session. The owner reviews the diff and commits themselves.

---

## EXPLORE

Run these queries to understand what Phase 3 built. Use whichever tool you prefer:

**psql** (required for `<=>` cosine similarity — pgvector operators are not available in DuckDB):
```bash
docker exec -it travellens-postgres psql -U travellens -d travellens
```

**VSCode terminal:** use the psql command above, or connect via any Postgres extension.

---

### E1 — Embedding coverage

Did every row get an embedding?

```sql
SELECT
    COUNT(*)                                       AS total_reviews,
    COUNT(embedding)                               AS embedded,
    COUNT(*) - COUNT(embedding)                    AS still_null,
    ROUND(100.0 * COUNT(embedding) / COUNT(*), 2) AS pct_done
FROM reviews_raw;
```

Expected: `pct_done = 100.00`, `still_null = 0`

---

### E2 — Review length distribution

Understand how many reviews hit the 1,000-char truncation threshold.

```sql
SELECT
    MIN(LENGTH(review_text))                                    AS min_chars,
    ROUND(AVG(LENGTH(review_text)))                             AS avg_chars,
    PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY LENGTH(review_text))                          AS median_chars,
    PERCENTILE_CONT(0.95) WITHIN GROUP
        (ORDER BY LENGTH(review_text))                          AS p95_chars,
    MAX(LENGTH(review_text))                                    AS max_chars,
    COUNT(*) FILTER (WHERE LENGTH(review_text) > 1000)          AS truncated_count
FROM reviews_raw;
```

The truncation count here should match the log output from the embedding job.
If P95 is under 1,000 chars, truncation only affected the very long tail — embedding
quality for the bulk of reviews is unaffected.

---

### E3 — Sample short reviews

Very short reviews (under 50 chars) produce valid vectors but weak semantic signal.
Good to know how many exist — they won't break anything but they may show up as
near-matches for vague queries.

```sql
SELECT rating, LENGTH(review_text) AS chars, review_text
FROM reviews_raw
WHERE LENGTH(review_text) < 50
ORDER BY LENGTH(review_text)
LIMIT 10;
```

---

### E4 — Sample long reviews that were truncated

See what the model only partially saw.

```sql
SELECT
    rating,
    LENGTH(review_text)          AS total_chars,
    LEFT(review_text, 300)       AS first_300,
    RIGHT(review_text, 100)      AS last_100_lost
FROM reviews_raw
WHERE LENGTH(review_text) > 1000
ORDER BY LENGTH(review_text) DESC
LIMIT 5;
```

The `last_100_lost` column shows the tail that the model never saw. If it contains
specific complaint details (room number, staff names, dates), that context is lost.
For aggregate semantic search this is acceptable — we're finding themes, not facts.

---

### E5 — Rating distribution

```sql
SELECT
    ROUND(rating)   AS stars,
    COUNT(*)        AS reviews,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM reviews_raw
GROUP BY ROUND(rating)
ORDER BY stars;
```

Real review datasets skew toward 4–5★ (people mostly review when happy or very
unhappy). A flat distribution would indicate a data generation issue.

---

### E6 — Top hotels by review count

```sql
SELECT
    r.hotel_id,
    h.hotel_name,
    COUNT(*)                AS review_count,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM reviews_raw r
JOIN hotel_master h USING (hotel_id)
GROUP BY r.hotel_id, h.hotel_name
ORDER BY review_count DESC
LIMIT 10;
```

---

### E7 — Reviews by OTA source

```sql
SELECT
    source,
    COUNT(*)                AS reviews,
    ROUND(AVG(rating), 2)   AS avg_rating
FROM reviews_raw
GROUP BY source
ORDER BY reviews DESC;
```

---

### E8 — Does semantic search actually work?

This is the most important check. Find reviews similar to a seed review by text.
If the results are topically relevant — even using different words — embeddings are
working correctly.

Pick any review as your seed and find its neighbours:

```sql
-- Step 1: pick a seed review
SELECT review_id, rating, LEFT(review_text, 150) AS preview
FROM reviews_raw
WHERE review_text ILIKE '%dirty%'
LIMIT 1;
```

Copy the `review_id` from the result, then:

```sql
-- Step 2: find the 10 most similar reviews to that seed
-- Replace <your_review_id> with the value from Step 1
SELECT
    r.rating,
    LEFT(r.review_text, 150)                             AS preview,
    ROUND((1 - (r.embedding <=> seed.embedding))::numeric, 4) AS similarity
FROM reviews_raw r,
     (SELECT embedding FROM reviews_raw WHERE review_id = '<your_review_id>') seed
ORDER BY r.embedding <=> seed.embedding
LIMIT 10;
```

What good looks like: top results should mention similar themes (cleanliness, hygiene,
housekeeping) even if they use different words. The seed review itself should appear
first with `similarity = 1.0000`.

What a red flag looks like: top results are completely unrelated topics at high
similarity scores — suggests the embedding column has corrupt or misaligned data.

---

### E9 — Index confirmation

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'reviews_raw';
```

You should see `idx_reviews_embedding` with `USING ivfflat` and `vector_cosine_ops`.

---

## DO NOT

- Do not build the IVFFlat index before all embeddings are written
- Do not increase `BATCH_SIZE` above 128 without checking VRAM usage first
- Do not set `lists` above 50 for 30K rows — over-partitioned index is slower at query time
- Do not skip long reviews — truncate instead, otherwise they stay NULL and invisible to search
- Do not remove the `register_vector(conn)` call in the script — psycopg2 cannot
  serialise the `vector` type without it
- Do not run `generate_embeddings.py` with a partially-built IVFFlat index in place —
  drop the index first, finish embedding, then rebuild

---

## ROLLBACK

**Job crashed mid-run:** re-run as-is. Idempotency handles it — only NULL rows get
processed.

**Index corrupted or dropped:**
```sql
DROP INDEX IF EXISTS idx_reviews_embedding;
```
Then re-run `generate_embeddings.py` — it rebuilds the index at the end.

**Need to re-embed from scratch (model change):**
```sql
UPDATE reviews_raw SET embedding = NULL;
```
Then re-run the job. Do not use semantic search until re-embedding is complete —
old and new model vectors are geometrically incompatible.

---

## LESSONS LEARNED

- Actual truncation count: 1,500 / 30,000 (5%) — dataset has more long reviews than estimated
- Actual runtime: ~25 min on CPU (torch not compiled with flash attention despite CUDA=True — fix Nvidia drivers for GPU speed)
- Semantic search quality: _(fill in after running E8)_
- Any unexpected errors: flash attention UserWarning — harmless, does not affect correctness

---


## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any code
- Create both scripts in the REPO STATE FILE TREE — no extras
- Do NOT build the IVFFlat index before all embeddings are written — index must come last
- `lists = 30` is correct for 30K rows — do not use the blueprint value of 100
- `register_vector(conn)` call is mandatory in both scripts — psycopg2 cannot serialise the vector type without it
- `batch_size = 128` — do not increase above 128 without checking VRAM
- Truncate at 1,000 chars — never skip long reviews (NULL embeddings are invisible to search)
- Run all 4 acceptance tests in psql before declaring done
- Do not modify any Phase 1 or Phase 2 files
- `<=>` cosine operator requires psql — pgvector operators are not supported in DuckDB


## BUILD HISTORY / EVOLUTION

Changes to Phase 3 embeddings after the original acceptance sign-off. Earliest first.

---

### B-030b — Continuous review embedder + IVFFlat re-tune plan

New `scripts/review_embedder.py`: continuous micro-batch process that embeds booking-tied reviews written by B-030/B-030a. Uses the same `all-MiniLM-L6-v2` model and `MAX_CHARS=1000` truncation limit as the frozen `generate_embeddings.py` and `ai/semantic_search.py` — all three share one 384-d vector space. Advisory lock `pg_try_advisory_lock(7400050)` prevents duplicate instances (gold uses `7400040`). Fetches up to 500 NULL rows per pass, encodes in chunks of 128, writes back via `executemany UPDATE`, commits per batch. Sleeps `EMBED_INTERVAL_SECONDS=120` when backlog is clear.

On first "caught up" poll, logs step-by-step Part B index-rebuild instructions: run a representative query before, `DROP + CREATE INDEX ... WITH (lists = 120)` (correct for ~121K rows vs original `lists=30` built for 30K), run same query after. Rebuild is intentionally NOT automated — the operator confirms recall is unaffected before switching. `run.py` now starts the embedder as a 5th managed proc (alongside consumer, simulator, dashboard, gold_lifecycle_updater). Advisory locks prevent duplicate instances if the script is also started manually.

**Why:** The original `generate_embeddings.py` (frozen) only embedded the 30K Kaggle seed rows. B-030/B-030a added ~91K booking-tied reviews (`record_source='history'` + `'stream'`) with `embedding IS NULL`. Semantic search (`ai/semantic_search.py`) over those rows returns no results until the vectors are written. The continuous embedder clears the backlog then keeps up with the stream in near-real-time.

---

### B-026 (Stage 1) — Per-review sentiment column + backfill

New `scripts/review_sentiment_scorer.py`: a micro-batch process mirroring `review_embedder.py` that classifies each review's sentiment and stores it on `reviews_raw`, as the real fix for L-012 (sentiment-topic conflation) — replacing the B-062 rating proxy. Migration 017 adds two nullable columns (`sentiment_label VARCHAR(8)`, `sentiment_score NUMERIC(4,3)`) + a partial index. Model: CardiffNLP `twitter-roberta-base-sentiment-latest` (3-class pos/neg/neutral), pinned to safetensors revision `d616e2bd…` and loaded with `use_safetensors=True` — the main-revision `.bin` checkpoint is refused on the project's pinned torch 2.3.0 (CVE-2025-32434, would need torch ≥2.6), and torch is deliberately NOT upgraded (would risk the cu121 / pgvector / sentence-transformers stack — L-008). Advisory lock `7400070`. `--once` runs the one-time 133K backfill (~15 min on the RTX 3070 at the measured ~153 rev/s); the bare command runs a continuous loop over `sentiment_label IS NULL`.

**Sentiment is independent of the embedding** — this process never touches the `embedding` column or the IVFFlat index; the columns are purely additive (no re-embed). It is NOT yet a `run.py` proc — that wiring is B-026 Stage 3.

**Why a dedicated classifier, not the rating:** star rating is a proven-bad sentiment proxy (L-012) — genuine complaints live in mixed-sentiment 3★ reviews (29.6% of the corpus) that B-062's ≤2★ / ≥4★ rating filter can never reach. The audit's prototype confirmed the classifier recovers those 3★ complaints; Ollama-per-review was ruled out by numbers (12–37 h). The known residual — whole-review sentiment still flattens mixed reviews where a positive opener dominates — is the aspect-level limit, tracked separately and out of scope here.

**Scope:** Stage 1 only (schema + scorer + backfill). The retrieval swap (Stage 2 — `_search_reviews` switches from the rating predicate to `sentiment_label`) and run.py wiring (Stage 3) are gated on this stage's verification. See [backlog → B-026](backlog.md#b-026--sentiment-classification-for-reviews).

---

### B-026 (Stage 3) — Live scorer wired into run.py (7th managed proc) — B-026 FULLY CLOSED

`scripts/review_sentiment_scorer.py` is now run.py's **7th managed process**, launched in continuous loop mode (`python -m scripts.review_sentiment_scorer`, no `--once`) so run.py owns its full lifecycle — exactly as it owns the embedder's (B-030b). Wiring only: no change to the scorer's logic, schema, retrieval path, scoring, or batch sizes (`_ENCODE_BATCH=64`, `SENTIMENT_BATCH_SIZE=2000` unchanged). The scorer's own `pg_try_advisory_lock(7400070)` is the singleton guard; the B-045 startup-takeover lock-poll was extended from `7400030/40/50/60` to also wait on `7400070` so a fresh `run.py` cleanly reclaims the lock after killing a prior supervisor + children. The roster is now: consumer · simulator · dashboard · gold · embedder · quarantine · **scorer** (bright-cyan colour tag). With this, a stream review now flows end-to-end while live: consumer → `reviews_raw` (NULL sentiment) → scorer fills `sentiment_label`/`sentiment_score` → semantic retrieval's B-026 Stage 2 polarity filter can see it.

**Verification (2026-05-30):**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| All 7 host procs come up | consumer/simulator/dashboard/gold/embedder/quarantine/scorer | all 7 started; banner "stack is up" | PASS |
| Scorer acquires its lock | logs "Advisory lock acquired (key=7400070)" | logged at startup; model `CardiffNLP` rev `d616e2bd`, device CUDA, mode loop | PASS |
| Other 6 procs unaffected | gold 7400040 / embedder 7400050 / quarantine 7400060 held; dashboard on :5000; consumer+simulator live | all 5 advisory locks held; embedder embedded new rows, gold processed batches mid-run | PASS |
| New review scored within ~15s | NULL-sentiment stream row gets a non-NULL label fast | controlled probe row scored `negative` (0.961) inside the poll window; probe then deleted, corpus restored (stream 12,563) | PASS |
| Takeover/lock-poll | mirrors embedder behaviour | startup-takeover poll now waits on 7400070 too; verified by clean re-acquire | PASS |
| Clean shutdown (Ctrl-C) | all 7 stop, no orphan holding 7400070 | task stopped → 0 of the 5 advisory locks held, 0 orphan travellens child processes | PASS |
| `pytest tests/` | green | 76 passed, 1 xfailed (B-003a) — = baseline | PASS |

> **OPERATIONAL NOTE — run.py runtime resource profile (8 GB RTX 3070).** A read-only
> resource audit confirmed all 7 host processes + Ollama coexist without OOM, so Stage 3
> was wiring only (no batch-size or scoring change). The three resident GPU models —
> embedder MiniLM + dashboard query-embedding MiniLM + scorer RoBERTa — plus Ollama
> Qwen-7B peak at **~91 % VRAM (7.4 / 8.2 GB)** with no OOM. Because that leaves Ollama
> less than its full model size, Ollama **auto-offloads ~14 % of its layers to CPU under
> load**, so dashboard queries run **~6–20 s** — graceful degradation, never a crash;
> system RAM peaks at **~75 %**. Lever for later if query latency matters: move the
> dashboard's query-embedding MiniLM to CPU to free a GPU context for Ollama.

---

## NEXT

**Phase 4 — Text-to-SQL + Semantic Query Router**

Files to build:
- `ai/query_router.py` — classifies incoming query as SQL path or semantic path
- `ai/text_to_sql.py` — natural language → SQL via Claude API
- `ai/semantic_search.py` — embeds query → pgvector cosine search → top-K reviews

The IVFFlat index built in this phase is what `semantic_search.py` queries using the
`<=>` cosine distance operator. The E8 query above is a preview of exactly what that
function will do in production.
