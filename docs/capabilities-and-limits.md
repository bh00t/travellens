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
- **`fact_bookings` ↔ `dim_date` join paradigm is the model's hardest
  shape.** Year/month/season filters on historical bookings need
  `JOIN dim_date d ON b.date_id = d.date_id` and then `d.year` /
  `d.month` / `d.month_name`. Qwen-7B repeatedly invents
  `b.booking_date`, `d.date_key`, or `d.month_number` even after the
  B-001 retry catches one; chatty-mode escape (model wraps SQL in
  prose) is also more common on this shape. During the B-053 portfolio
  curation pass, "Revenue by month in 2025" was the one widget skipped
  out of 12 after 5 phrasings — pinning at all would have either
  frozen broken SQL or routed silently to `fact_booking_events`. See
  [**B-053**](./backlog.md) for the phrasings tried and the documented
  skip.

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
- **Topic-vs-polarity conflation — substantially fixed (B-026).** Semantic
  embeddings cluster by TOPIC, so "cleanliness complaints" used to return
  cleanliness praise *and* complaints alike ([**L-012**](./backlog.md)).
  B-026 scored every review with a dedicated sentiment model into
  `reviews_raw.sentiment_label`, and retrieval now hard-filters a
  negative-intent query to `sentiment_label = 'negative'` (positive →
  `'positive'`). This replaced the earlier B-062 star-rating proxy — which
  was unreliable (5★ reviews carry specific complaints; identical texts
  exist across 1–5★) and blind to the 3★ band where mixed-sentiment
  complaints live. **Residual ([**L-017**](./backlog.md)):** sentiment is
  scored per *whole review*, so a mixed review with a positive opener can
  still label positive and hide a complaint inside it — true aspect-level
  (per-clause) sentiment is the deeper, out-of-scope fix.

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
- Polarity filtering (B-026) now applies here: a sentiment-laden query
  ("cleanliness complaints", "what guests love") hard-filters retrieval by
  `sentiment_label`, so results are polarity-matched, not just topical. A
  query with no clear sentiment intent stays topic-only (the detector
  classifies it neutral → no filter). Whole-review labeling residual:
  [**L-017**](./backlog.md).
- **Implicit-complaint queries get no polarity filter ([L-018](./backlog.md)).**
  The query-intent detector `_detect_polarity` is keyword/lexicon-based, so a
  query that implies a complaint *without* a sentiment word ("noisy AC",
  "thin walls", "slow check-in") is classed `neutral` and runs unfiltered —
  it falls back to topic-only matching and doesn't benefit from B-026's
  `sentiment_label = 'negative'` filter. This is distinct from **L-017**: L-018
  is about *query-intent* detection (does the query express negative intent at
  all), upstream of L-017's *per-review model labeling* (whole-review vs
  aspect-level). The fix is a classifier/LLM intent pass on the query rather
  than an explicit-keyword match.

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

## 5. Pipeline monitor (`/monitor`, B-027 + B-029 + B-032)

**What it does.** An operational view answering "is the pipeline
processing data correctly, right now?" — distinct from the
business-analytics Explorer. The Chunk-4 redesign reads **two**
operational tables with different roles:

| Source | Role | Filtered? |
|---|---|---|
| `pipeline_metrics` | live "now" — events/sec, alive signal | No — header pulse, always real-time |
| `agg_hourly_city_stats` | filtered history — per-type lifecycle counts | Yes (date + city) |

Layout: a **header pulse** (events/sec + alive dot), a date+city filter
bar, then three sections — **Events** (per-type lifecycle counts +
SOON placeholders), **Quarantine** (malformed / late), **Health**
(freshness + Airflow). Every external dependency is guarded — the page
renders even when MinIO or Airflow is down.

**Works well on:**
- **Live throughput.** The header pulse derives events/sec as the delta
  between the latest two `pipeline_metrics` rows (heartbeats land every
  ~10s from the consumer). `alive` is `age < 15s` — the dot turns grey
  within one tick of the consumer stopping. Verified end-to-end against
  the producer rate (50 evt/s in, 49.6–50.2 evt/s computed at the page).
- **Per-event-type lifecycle counts.** Bookings / check-ins / check-outs
  / cancellations(≈) are real values written by the consumer to
  `agg_hourly_city_stats` (`total_checkins`, `total_checkouts`,
  `total_cancellations` columns added in migration 007). Filtered by
  the chosen date range and city.
- **In-place auto-refresh.** A small JS polls `/monitor/data` every 10s,
  carrying the current page's query string, and patches the live pulse
  and EVENTS / QUARANTINE numbers via `textContent`. Scroll position
  and filter-form focus are preserved. No external libs.
- **Honest defaults.** Filter defaults to **today (UTC)** for both
  endpoints, FIXED — the page does NOT fall back to "all data" when
  today is empty. A cold system showing zeros for today is the correct
  answer; the empty-state banner explains how to start the simulator.
- **Honest placeholders.** Three tiles in EVENTS (`Review`, `Embedded`,
  `Sentiment`) render a dashed "SOON" empty-state with the blocking
  backlog ID rather than a fabricated value — see Limits below.
- **Graceful degradation.** Airflow HTTP, MinIO listing, and the
  heartbeat read are all wrapped with short timeouts; a down dependency
  shows `—` / "Unreachable" / "waiting for heartbeat", never a stack
  trace. Verified by stopping MinIO mid-session — page still HTTP 200.

**Reliability — high for what it measures, with three honest caveats:**

- **Cancellations are ≈-derived for back-compat, not stored as the raw
  count.** Migration 007 added a `total_cancellations` column and the
  consumer now writes it, but the monitor still derives the displayed
  value algebraically from `cancellation_rate × bookings / (1 − rate)`
  so old window rows written before migration 007 still surface a
  sensible number. The card labels this "≈" to signal the NUMERIC(5,4)
  rounding error. If the rate column is absent (schema variance), the
  card shows `—`. The route introspects `information_schema` to decide,
  so it is correct either way.
- **Review / Embedded tiles are placeholders; Sentiment data has
  shipped.** The Review and Embedded tiles still render a dashed "SOON"
  empty-state with `—` because their upstream data doesn't exist yet —
  they unblock when REVIEW becomes a stream event (**B-030**). The
  **Sentiment** data dependency is now satisfied: **B-026** scored every
  review into `reviews_raw.sentiment_label` (133,543 rows; 67% positive /
  29% negative / 4% neutral), and the AI layer already consumes it to
  polarity-filter semantic search (§3). Wiring the monitor's Sentiment
  tile to surface those counts is the remaining display step. Rating was
  never used as a polarity proxy — it's a proven-bad signal (the L-012
  finding); B-026's model sentiment is the real signal. Never showing a
  fake number is the design rule; the badge plus the backlog ID makes the
  gap legible.
- **Revenue is not on this page by design.** The Chunk 4 redesign
  removed the revenue card because revenue is a business KPI, not a
  pipeline-health signal. Business answers live in the
  [Explorer](/explore).

**Resolved limitations** — both previously listed here as limits, both
fixed by the Chunk 4 redesign:

- ~~No live throughput~~ → **resolved by B-032**. The consumer writes a
  `pipeline_metrics` heartbeat every ~10s; the header pulse reads it.
  See [L-015](./backlog.md).
- ~~No check-in / check-out counts~~ → **resolved by B-032 Chunks 2 + 3**.
  CHECKIN and CHECKOUT now pass Gate 3, are aggregated per window, and
  are written to dedicated columns. The producer emits both at design
  weights (CHECKIN 0.18, CHECKOUT 0.12). See
  [L-014](./backlog.md).

**Freshness threshold caveat.** "Fresh/Aging/Stale" is the age of the
latest window vs `MONITOR_FRESH_MINUTES` (default 15) /
`MONITOR_STALE_MINUTES` (default 90). Sized for dev's 2-min windows; on
prod's 60-min windows a healthy pipeline can read "Aging" right after a
flush — raise the threshold via `.env`.

**Limits:** the monitor reads operational tables directly (not via the
AI layer) — that is the deliberate exception called out in
`render/server.py`'s module docstring. Tables that Phase-6 DAGs fill
(`agg_daily_hotel_kpi`, sentiment, LTV) are intentionally absent;
those are the Explorer's job.

---

## 6. Data Limits

Caveats rooted in how the source data is generated, not in any code
path. These are answers the system computes correctly from the data it
has — but the data itself doesn't reflect real-world distributions, so
the answers shouldn't be read as such.

- Widget "Repeat customer share by zone" (id=13) returns ~40%+ —
  structurally inflated because fact_bookings has 1M bookings sampled
  uniformly from a 100K dim_customer pool (≈10 bookings per customer
  average). Real-hospitality typical is 20-25%. Same caveat applies to
  any customer-LTV / cohort / top-customer query. Fix tracked in B-055.

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
