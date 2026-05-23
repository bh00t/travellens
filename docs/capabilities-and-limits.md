# Capabilities and Limits

> A reference for what the AI layer **does** and how **reliable** each feature
> is. Distinct from the build-step phase docs, the open-item backlog, and the
> dev-rule CLAUDE.md. L-numbers below point to entries in
> [`docs/backlog.md`](./backlog.md) — that's where the diagnosis and fix path
> live. This file does not duplicate them.

---

## 1. Text-to-SQL (structured queries)

**What it does.** Translates a plain-English analytical question into a
single Postgres `SELECT`, executes it against the warehouse, and returns
typed rows for rendering.

**Works well on:**
- Single- and multi-dimension aggregations: `revenue by city`,
  `cancellation rate by customer segment`, `bookings by month`.
- Entity counts that come from a dimension table directly: `how many
  hotels`, `hotels per city`, `customers per state`.
- `hotel_master.opened_year` queries: `hotels opened per year` returns
  per-year counts from the dimension, not booking-row counts.
- Multi-year ranges, both forms — `revenue in 2024 and 2025` (uses
  `IN`) and `revenue from 2024 to 2025` (uses `BETWEEN`).
- Aggregations with explicit filters (`top N`, `WHERE star_category = …`,
  `WHERE city = …`).

**Reliability — high for the shapes above.** Generated SQL passes two
gates before execution: `_validate_sql` (SELECT-only safety guard) and
`_validate_columns` (every referenced column must exist in
`information_schema`). At pin time the user reviews the SQL and freezes
it on `dashboard_widgets.generated_sql`; from then on every refresh
runs that exact string.

**Limits:**
- **Bare grouped aggregations over `fact_bookings`** (e.g.
  `total revenue by city` with no LIMIT) intermittently drop the
  `WHERE NOT b.is_cancelled` filter on the local Qwen-7B model — see
  [**L-013**](./backlog.md). Two prompt rewrites have been tried; the
  failure is a small-model attention ceiling, not a prompt bug. The
  safeguard is the B-022 pin-time review: a human catches the missing
  filter once, the corrected SQL is frozen, and the read path never
  re-rolls the dice.
- **Unanswerable questions get a fabricated interpretation rather than a
  refusal.** The model doesn't say "no column exists for that" — it
  picks an adjacent concept and returns plausible-looking but wrong
  SQL. Original framing in [**L-010**](./backlog.md) (now mostly
  resolved for entity counts via migration 006 + the ENTITY COUNT
  RULE); the residual answerability gap is what pin-time review is
  for.

---

## 2. Hybrid queries (B-004)

**What it does.** Recognises a structured filter inside an
otherwise-semantic question — `rating`, `star_category`, `city` — and
pre-scopes the pgvector review search to hotels matching that filter.
"Negative reviews from hotels rated over 4" returns reviews only from
hotels with `avg_rating >= 4`, not the global review corpus.

**Works well on:**
- Filter detection via lightweight regex, no second LLM call. False
  positives blocked: `top 5 cities` doesn't trigger a `star_category=5`
  filter, `4am checkout` doesn't trigger a rating filter, `2015` doesn't
  trigger anything.
- Hybrid scoping verified end-to-end: every returned review's
  `hotel_id` belongs to the ground-truth filter set (proved via direct
  psql in [`tests/test_hybrid_queries.py`](../tests/test_hybrid_queries.py)).

**Reliability — high.** The scoping is structural (a hotel_id list
intersected with the pgvector search) and deterministic.

**Limits:**
- **Semantic search matches TOPIC, not POLARITY.** "Cleanliness
  complaints" returns reviews that mention cleanliness, positive *and*
  negative — embeddings cluster by topic. See [**L-012**](./backlog.md).
  The `reviews_raw.rating` column is **not** a reliable sentiment proxy
  either: positive 5★ reviews mention specific complaints, and
  identical review texts exist across 1–5★. Proper fix is sentiment
  scoring at embed time (a feature, not a filter).

---

## 3. Semantic review search

**What it does.** Embeds the user query with `all-MiniLM-L6-v2`,
searches `reviews_raw` via pgvector cosine distance (IVFFlat index),
then asks Ollama to extract the top 3 themes from the retrieved
reviews.

**Works well on:** topical relevance — "AC complaints", "rude staff",
"breakfast quality" surface reviews that genuinely talk about those
things. Returned reviews are de-duplicated so a single near-duplicate
cluster can't flood the top-K.

**Reliability — good** for surfacing the right topic; the theme summary
is concise and grounded in the retrieved reviews.

**Limits:**
- **Near-duplicate review clusters.** The dataset (Kaggle source —
  [**L-003**](./backlog.md)) contains reviews that share a ~190-char
  opener and differ only in a short closing sentence. Strict
  `DISTINCT ON (review_text)` treats them as distinct; the production
  code uses `DISTINCT ON (LEFT(review_text, 200))` — see
  `DEDUP_PREFIX_LEN` in [`ai/semantic_search.py`](../ai/semantic_search.py).
  Worst-case residual: a few reviews sharing their first 200 chars can
  still co-survive in the top-K. Tightening the prefix further starts
  collapsing genuinely-different reviews — empirically chosen tradeoff.
- The sentiment-vs-topic limit above (L-012) applies here too — these
  results are *topically* matched, not polarity-matched.

---

## 4. Dashboard / pin flow (B-022)

**What it does.** This is the architectural safeguard that covers the
text-to-SQL limits in §1. When the user pins a widget:

1. The LLM authors SQL once.
2. The user inspects the result and the SQL (via the **Show SQL**
   modal) before clicking **Pin to Dashboard**.
3. The SQL is frozen on `dashboard_widgets.generated_sql`; the result
   is cached as JSONB on `last_result_json`.
4. Every subsequent read serves the cache. Refreshes re-execute the
   *frozen* SQL — never re-prompt the LLM.

**Reliability — deterministic once pinned.** A widget that's been
reviewed and pinned cannot drift. The non-determinism is bounded to
the brief pin-time window.

**Limits:** the pin-time gate depends on the user actually reviewing
the SQL. The Show SQL modal exists exactly for this — it shows the
frozen SQL with the prompt as a comment header, so the user can paste
it into psql or read it inline before pinning. A pinned widget with
bad SQL is a user-review failure, not a runtime bug.

---

## 5. Pipeline monitor (`/monitor`, B-027)

**What it does.** An operational view answering "is the pipeline
processing data correctly?" — distinct from the business-analytics
Explorer. Three sections under one date+city filter: stream activity
(`agg_hourly_city_stats`), review-embedding coverage (`reviews_raw`),
and pipeline health (Airflow `/health`, MinIO quarantine counts,
stream freshness). Every external dependency is guarded — the page
renders even when Airflow or MinIO is down.

**Works well on:**
- **Processing-correctness signals** straight from what the pipeline
  landed: bookings/revenue/windows processed, embedding coverage
  (`embedding IS NOT NULL` over total), malformed + late quarantine
  object counts, and stream freshness from `MAX(window_start)`.
- **Graceful degradation.** Airflow HTTP and MinIO listing are wrapped
  with short timeouts; a down dependency shows "Unreachable" / "—",
  never a stack trace (verified — cold-load and Airflow-down
  acceptance tests).
- **Honest empty states.** A cold system shows zeros plus an
  "is the consumer running?" banner — correct, not broken. Default
  range is all stream data, not "today" (synthetic data may have
  nothing dated today).

**Reliability — high for what it measures, with three honest caveats:**
- **Cancellations are ≈-derived, not stored.** `agg_hourly_city_stats`
  holds `cancellation_rate`, not a raw count; the card reconstructs
  `rate × bookings / (1 − rate)` and labels it "≈". If the column
  is absent (schema variance), the card shows "—". The route
  introspects `information_schema` to decide, so it is correct either way.
- **No live throughput.** There is no events/sec gauge — see
  [**L-015**](./backlog.md). The consumer's in-memory counters are not
  written to a queryable table (frozen Phase-2 file), so the monitor
  infers activity from what landed, not from a live rate.
- **No check-in/check-out counts.** CHECKIN/CHECKOUT are filtered at the
  consumer's Gate 3 and never persisted — see [**L-014**](./backlog.md).
- **Sentiment is deferred.** Section 2 shows embedding *processing*
  status only; sentiment classification is pending
  [**B-026**](./backlog.md). Rating is a proven-bad polarity proxy (L-012).

**Freshness threshold caveat.** "Fresh/Aging/Stale" is the age of the
latest window vs `MONITOR_FRESH_MINUTES` (default 15) /
`MONITOR_STALE_MINUTES` (default 90). Sized for dev's 2-min windows; on
prod's 60-min windows a healthy pipeline can read "Aging" right after a
flush — raise the threshold via `.env`.

**Limits:** the monitor reads operational tables directly (not via the
AI layer) by design — it is pipeline state, not a business answer.
Tables that Phase-6 DAGs fill (`agg_daily_hotel_kpi`, sentiment, LTV)
are intentionally absent; those are the Explorer's job.

---

## Why some of these are limits, not bugs

The SQL-generating model is **Qwen2.5-Coder-7B**, run locally on an
RTX 3070 — chosen so the project costs nothing per query and no data
leaves the host. A 7B model has a hard ceiling on holding rule
discipline against attention drift; that's where L-013 and the
residual L-010 case sit. These are **capability limits**, not prompt
bugs — two rewrite attempts on L-013 confirmed that.

The path forward for that class of issue is **B-017** (model tiering,
e.g. Gemma 12B for SQL while keeping Qwen for the cheaper semantic
summary). The architecture already anticipates this: the **B-022
pin-time review** exists exactly because the model can't be fully
trusted on semantic answerability, and the **B-003 column validator**
catches the most common hallucination mode before SQL ever reaches
Postgres. Both safeguards work today, regardless of which model lives
behind them.
