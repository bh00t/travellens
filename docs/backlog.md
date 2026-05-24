# TravelLens India — Project Backlog

> Living document. Items move from backlog → in progress → done as phases ship.
> Add new items at the bottom of the relevant section. Never delete — cross out if abandoned.

---

## Phase 4 Hardening — Query Engine

These must be resolved before the dashboard is reliable. A wrong query = wrong widget = misleading dashboard.

> B-001 (retry loop), B-002 (prefer fact_bookings), B-003 (column
> validation), and B-004 (hybrid filter + scoping) are DONE — see Completed.
> The system prompt was also fully rewritten this session: real column types, FK
> relationships, the two-hop fact_bookings→hotel_master→dim_location chain, date
> handling (date_id is an INTEGER key, never compare to CURRENT_DATE), and the
> city-list-is-reference-only rule. No more example-query patching.

---

### B-003a — `_validate_columns` misses bare WHERE refs on single-table queries
**Priority:** Low  
**Problem:** `_validate_columns` misses bare column references inside `WHERE`
on single-table queries (`_walk_columns` doesn't recurse into `Comparison`
nodes). False-NEGATIVE only — Postgres still catches it at execution. No
real query shape hits it (all use qualified refs in multi-table WHEREs).
Low priority polish; fix carefully to avoid introducing false-positives.  
**File:** `ai/text_to_sql.py` — extend `_walk_columns` to recurse into
`sqlparse.sql.Comparison` and other expression-bearing nodes inside `Where`.  
**Acceptance:** `_validate_columns("SELECT 1 FROM hotel_master WHERE avg_occupancy > 0.5")`
raises `ValueError` naming `avg_occupancy`; all eight queries in
`tests/test_validate_columns.py` continue to pass.

---

### B-005 — Prompt normalisation before routing
**Priority:** Low  
**Problem:** Heavy typos ("recied" instead of "received") reach Ollama as-is. The router handles them fine but Ollama may generate worse SQL.  
**Fix:** Light normalisation before routing — lowercase, strip extra spaces, optionally a simple spell-check pass using `pyspellchecker` for common hotel/data terms.  
**File:** `ai/main.py` — `normalise(query)` step before `route(query)`  
**Acceptance:** `python -m ai.main "wat r top citis by revnue"` routes and executes correctly.

---

### B-006 — Semantic search duplicate reviews
**Priority:** Low  
**Problem:** `reviews_raw` contains duplicate review texts from the Kaggle dataset. Semantic search returns multiple identical reviews at the same similarity score, wasting TOP_K slots and polluting Ollama's summary.  
**Fix:** Deduplicate `reviews_raw` on `(hotel_id, review_text)` during Phase 1 load, or apply `DISTINCT ON (review_text)` in the pgvector query in `semantic_search.py`.  
**File:** `ai/semantic_search.py` — add `DISTINCT ON` to the search query  
**Acceptance:** No two identical `review_text` values appear in the top-20 results for any query.

---

## Phase 3/4 — Review Intelligence

Cross-cutting features that touch BOTH the Phase 3 ingest pipeline and the
Phase 4 query layer. Pure prompt or query-only fixes belong in Phase 4
Hardening above; pure ingest-time data work belongs in a new Phase 3
follow-up. Items here span both phases by design.

---

### B-026 — Sentiment classification for reviews
**Priority:** Medium — resolves L-012 (sentiment-topic conflation)  
**Problem:** Semantic search matches review TOPIC not POLARITY, so "cleanliness complaints" returns cleanliness praise too (see L-012). Star rating is not a reliable sentiment proxy — confirmed via testing that complaints ("foul smell", "cobwebs") appear in mixed-sentiment 3★ reviews with positive openers.  
**Design:**
- Classify each review at INGEST (batch), alongside the existing embedding step (Phase 3) — NOT at query time. Store as a column.
- Use a DEDICATED local sentiment model (small HuggingFace classifier on the 3070), NOT the star rating (proven unreliable, L-012) and NOT the Ollama LLM (slower, overkill for a narrow task).
- Categories: sentiment = positive / negative / neutral (3-class). Keep sentiment SEPARATE from relevance — "irrelevant" (e.g. "didn't really stay" reviews) is a different axis from polarity; a separate `is_relevant` flag if needed, not crammed into the sentiment label.
- Schema: new `sentiment` column on `reviews_raw` (via migration, append-only). Backfill the existing ~30k reviews.
- Query integration (Phase 4): `detect_filters` learns sentiment words (complaints/negative/bad → `sentiment='negative'`); the hybrid path adds the filter so "complaints about X" = topic(X) AND `sentiment='negative'`.

**Locked decisions (from design discussion):**
- Output: 3-class label (`positive` / `negative` / `neutral`) PLUS a confidence score (store both — label drives filtering/display, score enables "strongly negative" later without a re-backfill). Two columns: `sentiment_label`, `sentiment_score`.
- Model: a pre-trained transformer — CardiffNLP 3-class RoBERTa (`twitter-roberta-base-sentiment-latest`), which outputs exactly pos/neutral/neg + scores and is robust on informal review text. NOT rating (L-012), NOT Ollama-LLM (slow/non-deterministic), NOT VADER (lexicon too crude for the mixed-sentiment cases).
- Prototype FIRST: before the migration + 30K backfill, run the model on a ~200-review sample (including the known-hard 3★ cobwebs cases) and eyeball accuracy vs the rating proxy. Only proceed to full build if it clearly beats rating on our data.
- Network: the HuggingFace model downloads from `huggingface.co` — NOT currently in the repo's allowlist. Add `huggingface.co` + `*.huggingface.co` before the prototype, or download weights manually.
- Caveat: trained on general/Twitter English; expect some misses on Indian-English-idiomatic or code-mixed text — still far better than rating-as-proxy. Verify on hard cases before trusting counts.

**Scope:** spans Phase 3 (ingest/embeddings) and Phase 4 (query layer) — migration + pipeline classify step + backfill + `detect_filters` extension + verification. A feature, not a quick fix.

**Known residual:** a single sentiment label still flattens mixed-sentiment reviews ("nice pool BUT cobwebs"). Big improvement over topic-only, not perfect. True solution is aspect-based sentiment (per-topic polarity) — research-grade, out of scope.

**Files:** `db/migrations/00X_review_sentiment.sql`, the ingest/embed pipeline script, `ai/main.py` (`detect_filters`), `ai/semantic_search.py`.

**Acceptance:** "cleanliness complaints" returns predominantly negative-sentiment cleanliness reviews; "best things about hotels" returns positive; the L-012 conflation no longer dominates results.

---

## Phase 5 — Dashboard

> B-007 through B-012 are all DONE — see Completed. The dashboard shipped with:
> widget renderer, pin to Postgres (dashboard_widgets table, not JSON), auto-refresh
> on page load, per-widget scheduled refresh, delete, and CSS grid layout.
>
> Built this session on top of the base dashboard:
> - Settings gear (⚙) popover per widget — floating, click-outside to close
> - Per-widget auto-refresh frequency (Off / 5 / 15 / 30 / 60 / 360 min)
> - Per-widget width (Normal / Full row) with live grid span
> - Refresh-all button in the dashboard header
> - Readability warning when a bar chart exceeds 20 bars (Option A — renders anyway,
>   warns, offers switch to table; never suggests line chart for categorical data)

---

### B-020 — Change chart type from the dashboard
**Priority:** Medium  
**Problem:** Chart type can only be set in the Explorer before pinning. Once a widget
is on the dashboard there's no way to change bar → table, etc. without deleting and
re-pinning.  
**Fix:** Add a third row to the settings popover: "Chart type" dropdown
(Bar / Line / Table / Stat card). On change, POST `widget_type` to the existing
`/api/widget/<id>/settings` endpoint (add `widget_type` to its allowed fields), then
re-render that widget in place. Option A behaviour: show all type options; if the
chosen type doesn't fit the data shape (e.g. stat card for a 16-row result), the
renderer shows a small "this data can't render as X" message instead of breaking.  
**Files:** `render/templates/dashboard.html`, `render/server.py` (allow `widget_type`
in settings endpoint), `render/widget_renderer.py` (graceful mismatch message)  
**Acceptance:** Pin a bar chart, open its settings, switch to table — widget re-renders
as a table without page reload and persists after refresh.

---

### B-021 — Drag-and-drop widget reordering
**Priority:** Medium — core DONE, polish OPEN  
**Done:** SortableJS added, grid sortable by header handle, `/api/widgets/reorder`
endpoint writes `display_order`, order persists on reload.  
**Open — look & feel polish:** the current drag ghost (dashed green border + 40% opacity)
is crude. Improve: a clean lift/shadow on the dragged card, a proper drop-placeholder
showing where it will land, smoother motion. Tune SortableJS `ghostClass`, `chosenClass`,
`dragClass` separately rather than one ghost style.  
**Files:** `render/templates/dashboard.html`  
**Acceptance:** Dragging feels smooth — clear lift on the held card, a visible gap where it
will drop, no jarring opacity flash.

---

### B-025 — Fixed-height scrollable table and review widgets
**Priority:** Medium  
**Problem:** Table widgets render every row, so a large result (e.g. 50 hotels) makes the
widget enormous and breaks the grid layout. The semantic widget's review list has the same
issue.  
**Fix:** Give the table body and the review list a fixed max-height (e.g. 320px) with
`overflow-y: auto`. For tables, keep the header row pinned (`position: sticky; top: 0`) so
column labels stay visible while the body scrolls. Apply in both the dashboard widget and
the Explorer preview.  
**Files:** `render/templates/dashboard.html`, `render/templates/explore.html`
(table + semantic render CSS)  
**Acceptance:** A 50-row table widget stays a fixed height with a scrollable body and a
pinned header; the grid layout is unaffected.

---

---

### B-022 — Decouple read path from compute (cache + frozen SQL)
**Priority:** High — fixes the tab-switch re-loading problem and the regeneration hazard  
**Problem (two related issues):**
1. Every dashboard load / tab switch re-runs every widget's prompt through Ollama. With
   Ollama taking seconds per widget, navigating back to the dashboard feels broken.
2. Re-running the *prompt* (not the SQL) on every refresh means Ollama regenerates SQL
   non-deterministically — same question, occasionally different query and result.

**Root cause:** the dashboard read path triggers LLM inference. In a correct design a user
request never triggers compute — reads hit a cache, compute is scheduled, the LLM authors
the query once.

**Architecture — three separated paths:**

```
Read path    (fast)      dashboard load → reads result cache → renders instantly
Compute path (scheduled) scheduler → runs frozen SQL on interval → writes cache
Author path  (once)      pin widget → LLM generates SQL → validate → freeze
```

The user never waits on Ollama or even on a live SQL query. The principle: a request never
triggers compute. This is the stale-while-revalidate pattern (CDNs, Vercel, React Query).

**Build now (right-sized stand-in for the production shape):**
1. Freeze SQL at pin time — generate once, validate, store in `generated_sql` column.
2. Server-side result cache — `last_result_json` (JSONB) + `last_refreshed_at` on
   `dashboard_widgets`. After any run, store result + timestamp.
3. Read path serves cache — on dashboard load, if `last_refreshed_at` is within the
   widget's refresh interval, embed the cached result directly (no Ollama). Only stale
   widgets re-run.
4. Refresh runs frozen SQL directly — never regenerates. A "regenerate SQL" option in the
   settings popover handles the rare case where fresh SQL is wanted (schema changed).

Note: freezing the SQL text does NOT freeze dates — `CURRENT_DATE` logic inside the SQL
re-evaluates each run, so "current month" widgets still auto-update correctly.

**Files:** `db/migrations/005_widget_cache.sql` (add `generated_sql`, `last_result_json`),
`ai/text_to_sql.py` (expose run-stored-SQL path separate from generate-then-run),
`render/server.py` (pin stores SQL; refresh runs frozen SQL; dashboard load serves cache),
`render/templates/dashboard.html` (render from embedded cache, only fetch stale widgets)  
**Acceptance:** Load dashboard, switch to About, switch back — widgets render instantly from
cache with no Ollama calls. Refresh a widget 5 times — identical SQL and result each time.

**Deferred to Phase 6 (production swap — documented, not built now):**
- `last_result_json` JSONB column → Redis (keyed by widget ID, TTL = refresh interval)
- On-load / manual refresh → Airflow `refresh_pinned_widgets` DAG running frozen SQL on
  schedule and writing to the cache
- Both noted in README as the production swap. The JSONB column is a deliberate scoped-down
  stand-in for Redis, not the intended production answer.

---

### B-023 — About page redesign
**Priority:** Medium  
**Problem:** Current about.html is beginner-level — basic cards, emoji icons, flat layout.
Does not match the polish of the rest of the app.  
**Fix:** Rebuild as a proper product landing page — richer visual hierarchy, real iconography
(not emoji), subtle animations/transitions, sections that match the app's design language
(green accent, mono headers, clean grid). Keep all the existing content (capabilities,
how it works, stack, data sources, disclaimer) but present it well.  
**File:** `render/templates/about.html`  
**Acceptance:** About page looks like a polished product page, not a placeholder.

---

## Phase 6 — Airflow DAGs (planned)

---

### B-013 — daily_hotel_kpi DAG
Nightly job that aggregates `fact_bookings` into `agg_daily_hotel_kpi`. Until this runs, all queries against that table return 0 rows. Blocks B-002 permanent fix.

### B-014 — reconcile_late_events DAG
Nightly job that merges `late_events/` S3 prefix back into `agg_hourly_city_stats`. Until this runs, late-arriving booking events are permanently excluded from aggregates.

### B-015 — hotel_sentiment_scores DAG
Weekly job that aggregates pgvector cosine similarity scores per hotel into a `hotel_sentiment_scores` Gold table. Enables SQL queries like "top 10 hotels by positive sentiment score" without going through the semantic path every time.

### B-016 — customer_ltv DAG
Weekly job computing customer lifetime value from `fact_bookings`. Enables loyalty and segmentation queries without full-table aggregation.

### B-024 — refresh_pinned_widgets DAG (compute path for B-022)
Scheduled job that re-runs each pinned widget's frozen SQL on its `refresh_interval_minutes`
and writes the result to the cache (Redis in production, `last_result_json` column until then).
This is the "compute path" half of B-022 — it moves widget refresh off the request entirely,
so the dashboard read path only ever reads cache. Depends on B-022's frozen-SQL storage being
in place first.

### B-033 — quarantine_daily_rollup DAG (self-healing daily summary) ⬜ PENDING [Phase 6]

**Problem:** `_monitor_quarantine()` lists ALL S3 objects under `malformed_events/` and
`late_events/` on every page load — slow (the ~8s MinIO-timeout render) and unbounded as
quarantine grows. Quarantine also can't be date-filtered today, even though the S3 keys ARE
day-partitioned (`year=/month=/day=/hour=/`). City is NOT in the quarantine key (malformed events
often can't be parsed for a city — that's why they're malformed), so city is correctly
non-filterable.

**Solution:** a daily Airflow DAG that rolls each COMPLETED day's S3 quarantine counts into a new
table:
  quarantine_daily_summary (
    summary_date    DATE PRIMARY KEY,
    malformed_count INTEGER NOT NULL DEFAULT 0,
    late_count      INTEGER NOT NULL DEFAULT 0,
    computed_at     TIMESTAMP NOT NULL DEFAULT now()
  )

**MUST be self-healing (the key requirement):** the DAG does NOT assume "yesterday." Each run it
computes  missing_days = (days present in S3 up to yesterday UTC) − (days already in
quarantine_daily_summary)  and backfills ALL of them, idempotently (INSERT ... ON CONFLICT
(summary_date) DO UPDATE). So a missed schedule, pipeline downtime, or a manual late run all
converge to a complete, correct table. The schedule is best-effort; the S3-reconciliation is the
correctness guarantee — because the objects really are in S3, a skipped run loses nothing, it just
delays the count until the next run.

**Monitor read-path change:** past days in the filter range → SUM from quarantine_daily_summary
(fast, indexed). Today (if in range) → live count (S3 today-prefix, or pipeline_metrics). Date
filter then works on quarantine; city labelled N/A on those tiles.

**Open decisions at build time:**
- Today's source: live S3 list of today's prefix (correct across consumer restarts, one cheap
  day-prefix call) vs latest pipeline_metrics row (fast but resets per consumer-run = "this run"
  not "today"). Lean S3-today.
- Write explicit zero-rows for quarantine-free completed days? Recommended YES, so "gap" vs
  "genuinely zero" is distinguishable.

**Depends on:** nothing hard; sits in the Phase-6 rollup-DAG family alongside daily_hotel_kpi /
reconcile_late_events. Same idempotent-backfill pattern is worth reusing across those DAGs.

**Why it matters (portfolio):** demonstrates idempotent gap-recovery, not just "schedule a
script" — a real DE robustness pattern.


---

## LLM Evaluation — Model Comparison (test before finalising Phase 5)

---

### B-017 — LLM comparison testing plan: Qwen2.5-Coder-7B vs Gemma 3 12B

**Priority:** Medium — do before declaring Phase 4 complete  
**Context:** Current stack uses Qwen2.5-Coder-7B for both SQL generation and review
summarisation. Gemma 3 12B is a stronger general-purpose model and may produce
better quality summaries on the semantic path. However it is larger and slower on
RTX 3070 8GB. This item defines how to compare them fairly within TravelLens scope —
not a generic LLM benchmark.

**Hypothesis:**
- SQL path: Qwen2.5-Coder-7B likely wins or ties — it is code/SQL specialized
- Semantic summarisation path: Gemma 3 12B may win — it is better at language reasoning

---

#### Test plan

**Step 1 — Pull Gemma 3 12B**
```bash
ollama pull gemma3:12b
ollama list   # confirm both models present
```

**Step 2 — SQL path evaluation (10 golden queries)**

Run each query against both models. Score each result:
- ✓ Correct SQL generated (right tables, right columns, right joins)
- ✓ Correct result returned (spot-check against psql manual query)
- ✓ No retry needed
- ✗ Wrong column / wrong table / execution error
- ✗ Retry needed

| # | Query | Qwen SQL ✓/✗ | Gemma SQL ✓/✗ | Notes |
|---|---|---|---|---|
| 1 | top 5 cities by revenue | | | |
| 2 | cancellation rate by customer segment | | | |
| 3 | average daily rate for 5-star hotels in Goa | | | |
| 4 | which hotel received maximum bookings | | | |
| 5 | monthly revenue trend for 2025 | | | |
| 6 | hotels in Rajasthan with rating above 4 | | | |
| 7 | budget hotels in Goa sorted by avg rating | | | |
| 8 | total revenue lost to cancellations by city | | | |
| 9 | customer segment with highest average booking value | | | |
| 10 | how many bookings were made on public holidays | | | |

**Step 3 — Semantic summarisation evaluation (5 golden queries)**

Run each query against both models. Score each Ollama summary:
- **Relevance** (1-5): are the 3 themes actually about what was asked?
- **Specificity** (1-5): does it mention actual issues (e.g. "AC rattling noise") vs generic categories (e.g. "noise issues")?
- **Accuracy** (1-5): do the themes match what the retrieved reviews actually say?

| # | Query | Qwen relevance | Qwen specificity | Qwen accuracy | Gemma relevance | Gemma specificity | Gemma accuracy |
|---|---|---|---|---|---|---|---|
| 1 | complaints about AC not working | | | | | | |
| 2 | what are guests saying about cleanliness in Goa | | | | | | |
| 3 | rude or unhelpful staff experiences | | | | | | |
| 4 | best things about 5-star hotels | | | | | | |
| 5 | food and breakfast quality complaints | | | | | | |

**Step 4 — Latency comparison on RTX 3070**

Run each model 3 times per path, record average:

```bash
# SQL path latency
time python -m ai.main "top 5 cities by revenue"   # run 3x per model

# Semantic path latency (includes embedding + pgvector + summarisation)
time python -m ai.main "complaints about AC not working"   # run 3x per model
```

| Path | Qwen avg latency | Gemma avg latency |
|---|---|---|
| SQL generation | | |
| Semantic summarisation | | |

**Step 5 — VRAM usage**

While each model runs a query, check VRAM in a second terminal:
```bash
nvidia-smi
```

| Model | VRAM used (approx) | OOM risk on RTX 3070 8GB |
|---|---|---|
| Qwen2.5-Coder-7B | | |
| Gemma 3 12B | | |

---

#### Decision criteria

| Scenario | Decision |
|---|---|
| Gemma wins SQL + fits in VRAM | Switch both paths to Gemma |
| Qwen wins SQL, Gemma wins summarisation, both fit in VRAM | Use Qwen for SQL, Gemma for summarisation (hybrid) |
| Gemma wins summarisation but OOM risk | Stay with Qwen for both, accept lower summary quality |
| Qwen wins or ties on both | Stay with Qwen, no change needed |

**Hybrid config if needed** — `OLLAMA_MODEL_SQL` and `OLLAMA_MODEL_SEMANTIC` as separate
`.env` variables. `text_to_sql.py` reads `OLLAMA_MODEL_SQL`, `semantic_search.py` reads
`OLLAMA_MODEL_SEMANTIC`. One-line change per file, no architectural impact.

**File to update after test:**
- `ai/text_to_sql.py` — change model env var if switching SQL model
- `ai/semantic_search.py` — change model env var if switching summarisation model
- `.env` and `.env.example` — add `OLLAMA_MODEL_SQL` / `OLLAMA_MODEL_SEMANTIC` if going hybrid
- `docs/backlog.md` — fill in the test result tables above and mark B-017 complete

---

## Developer Experience

> B-018 (CLAUDE.md) and B-019 (file-tree + Claude Code instructions in phase docs)
> are both DONE — see Completed. CLAUDE.md at repo root now also carries the hard
> rule: build all LLM-facing content from the real schema, never from memory;
> examples are a last resort, not a patch.

---

### B-028 — `run.py` dev launcher
**Priority:** Medium — built this session, NOT yet committed/verified  
**Problem:** Bringing the stack up means several manual commands (docker compose up, then
consumer, simulator, dashboard in separate terminals). Tedious every dev session.  
**What it does:** single `python run.py` at repo root → preflight-checks the Docker daemon,
runs `docker compose -f docker/docker-compose.yml --env-file .env up -d` (infra + Airflow,
whose scheduler then runs DAGs on its own — run.py does NOT schedule/track Airflow), waits
for health, then starts the 3 host Python processes (consumer, simulator, dashboard) with
tagged combined logs. Ctrl-C stops the Python processes and tears Docker down ONLY if the
script started it (least-surprise). Idempotent: `up -d` is safe if containers already run,
and it skips a duplicate dashboard if the port is already bound.  
**Flags:** `--no-sim`, `--sim-rate N`, `--no-docker`, `--down`.  
**Open before commit:**
- Verify the CONFIG block at the top against the real repo — the three start commands
  (`python -m scripts.stream_consumer`, `python -m scripts.kafka_event_producer`,
  `python -m render.server`), the simulator's rate-flag name, the dashboard port, and the
  `HEALTH_WAIT_SERVICES` service names must match the actual compose file.
**File:** `run.py` (repo root, new).  
**Acceptance:** `python run.py --no-docker` starts all 3 processes against an already-up
stack; full `python run.py` brings everything up and Ctrl-C tears it down cleanly with no
orphaned processes.  
**Update (Phase 7 session):** —chaos passthrough + stream_output UTF-8 fix added this session; CONFIG block still unverified.

---

## Phase 2 Hardening — Consumer (scoped frozen-file exception)

> `scripts/stream_consumer.py` is on the FROZEN list. This is a deliberate,
> logged exception taken to resolve L-016 (consumer never flushes under load).
> Defaults are preserved; production behaviour is unchanged unless env vars are set.
> **TODO after verification:** note this exception in CLAUDE.md's frozen-file list,
> and re-run the Phase-2 acceptance suite to confirm no regression.

### B-031 — Consumer resilience fix (resolves L-016)
**Priority:** High — fixes the pipeline's core "nothing ever flushes" failure.
**Changes (all in `scripts/stream_consumer.py`, heavily commented, defaults unchanged):**
1. **Bug A — main loop:** replaced the endless `for msg in consumer:` generator with a
   bounded `consumer.poll(timeout_ms=1000, max_records=INNER_BATCH_MAX)` batch. `poll()`
   always returns control each cycle, so the flush check and max-runtime check are
   guaranteed to run even under saturating load. Per-event processing body (all gates,
   accumulation) is byte-for-byte unchanged — verified by diff.
2. **Bug B — eviction:** added `max_poll_records`, `max_poll_interval_ms=600000`,
   `session_timeout_ms=30000`, `heartbeat_interval_ms=10000` to the `KafkaConsumer` so a
   slow chaos batch can't trigger broker eviction.
3. **Testability:** `WINDOW_SIZE_MINUTES`, `WATERMARK_GRACE_SECONDS` are now
   `os.getenv(..., <prod default>)`; added `INNER_BATCH_MAX` (default 200). Defaults
   identical to before — set `WINDOW_SIZE_MINUTES=2` / `WATERMARK_GRACE_SECONDS=30` in
   `.env` for fast dev verification only.
**Acceptance (pending — verify with own eyes):**
- **Test A (no-chaos correctness):** small-window `.env` + `--max-runtime 150` → a window
  closes mid-run, flush logs appear, `agg_hourly_city_stats` count increases, monitor Fresh.
- **Test B (chaos resilience):** same + chaos → NO "no active members" eviction, quarantine
  counts climb, consumer self-exits clean, windows still increase.
- **Test C (regression):** no env overrides → startup banner reads "Window: 60m | grace 300s".
**Caveat:** `_make_late` in the producer assumes 60m/300s when shifting events 70–180 min
back; under a 2-min test window everything late-injected lands far past the watermark
(still classified late, so Test B holds) — proportions won't mirror prod.

---

### ~~B-029 — `/monitor` auto-refresh (no manual reload)~~ — **SHIPPED**
**Status:** ✓ COMPLETE — see Completed table. Resolved by the Chunk 4 redesign:
landed straight at B-029b (the smoother JSON+JS variant, skipping B-029a's
full-page reload as unnecessary now that B-032 was landing in the same change).
`render/server.py` exposes `GET /monitor/data` returning the live + stream +
quarantine subset as JSON; `render/templates/monitor.html` polls it every 10s
and patches values via `textContent`, preserving scroll position and
filter-form focus. No env knob needed — 10s is hard-coded to match the
consumer's `FLUSH_CHECK_SECONDS`. Pairs with B-032 to make the live pulse
update in place without reload.

---

### ~~B-032 — Live pipeline metrics (real-time throughput; resolves L-015)~~ — **SHIPPED** ⭐
**Status:** ✓ COMPLETE — see Completed table. All 4 chunks delivered.
- **Chunk 1 — migration** ✓ `db/migrations/007_pipeline_live_metrics.sql` —
  `pipeline_metrics` table + 4 new count columns on `agg_hourly_city_stats`.
- **Chunk 2 — consumer** ✓ `scripts/stream_consumer.py` processes CHECKIN +
  CHECKOUT past Gate 3, writes per-type counts to the new agg columns, and
  emits a `pipeline_metrics` heartbeat every `FLUSH_CHECK_SECONDS` (~10s) in
  its own try/except.
- **Chunk 3 — producer** ✓ `scripts/kafka_event_producer.py` emits CHECKOUT
  at design weight 0.12 alongside BOOKING / CHECKIN / CANCELLATION /
  PRICE_CHANGE (weights 0.55 / 0.18 / 0.12 / 0.10 / 0.05, sum 1.0).
- **Chunk 4 — monitor UI** ✓ `/monitor` redesigned with a header live pulse
  (events/sec from heartbeat deltas, alive=age<15s, "waiting for heartbeat"
  empty state), default-today filter (FIXED — no all-data fallback), and
  EVENTS / QUARANTINE / HEALTH sections. Revenue card removed (business →
  Explorer). Three SOON placeholder tiles (Review, Embedded, Sentiment)
  render dashed "—" with blocking backlog IDs — never fake values.
- **`/monitor/data`** ✓ JSON sidecar returning `{live, stream, quarantine}`,
  same query-string contract as `/monitor`. 10s in-place polling shipped
  with this (folds in B-029).

**Verified end-to-end (Chunk 4 acceptance, 11 PASS / 0 FAIL):** events/sec
matches the 50 evt/s producer rate exactly (49.6 / 50.2 evt/s from hand-
computed heartbeat deltas); dot goes grey within one tick when the consumer
stops; filtered counts match a psql aggregate exactly (3,383 / 1,033 / 691 /
≈598 / 44 / 44); SOON tiles render `—` with the backlog IDs; revenue absent
from the page; numbers update in DOM ~every 10s with scroll preserved; page
still renders HTTP 200 with MinIO stopped.

**Retires L-015 in full and resolves L-014 for COUNTING** (CHECKIN /
CHECKOUT now counted via Chunks 2 + 3 — see L-014 entry for the residual
caveat that these counts live only in the stream aggregate, not in
`fact_bookings`).

**Design note (locked):** the hourly `agg_hourly_city_stats` stays AS-IS for
business KPIs; `pipeline_metrics` is the SEPARATE fast metrics path
alongside it. Fine windows for "now", coarse for "the trend" — exactly how
production systems run both. Do NOT shrink the production window to fake
real-time.

**Future follow-ons (not part of B-032):**
- **B-030** unblocks the Review + Embedded SOON tiles (REVIEW becomes a
  stream event).
- **B-026** unblocks the Sentiment SOON tile (scored at embed time).
- `pipeline_metrics.consumer_lag` is reserved NULL; an AdminClient hook
  would populate it but is a small follow-on, not on this item.

---

### B-030 — Reviews are not generated in the stream (feature gap, NOT a correlation bug)
**Origin:** User wanted reviews tied to real hotels/bookings rather than unrelated.
**Verified fact (settled, do not re-litigate the correlation part):** hotel-level correlation
ALREADY EXISTS — query confirmed **1,908 / 2,000 (95.4%)** of review-hotels have real bookings
in `fact_bookings`; every review joins to a real hotel via `reviews_raw.hotel_id`. The 92-hotel
gap is reviewed-but-not-booked hotels (the stream seeds from only 500 hotels) — expected, not a
bug. So "wild unrelated reviews" was a non-issue; **no correlation fix needed.**
**The ACTUAL gap:** reviews exist only as a static 30K batch load — the live stream emits no
REVIEW event. So the pipeline can't demonstrate reviews arriving/being processed live, and there
is no guest-/booking-level linkage (the stream carries no `customer_id`; `reviews_raw` has no
`customer_id`/`booking_id`).
**To add reviews as a stream event (decision, if pursued):**
- Producer emits a REVIEW event tied to a hotel (hotel-level correlation is real; guest-level
  would need customer identity added to producer→consumer→schema, currently absent everywhere).
- Consumer gains a REVIEW path → insert into `reviews_raw` (+ optional `customer_id`/`booking_id`
  columns via append-only migration).
- Streaming embedding-at-ingest (the one genuinely new capability; today embeddings are batch).
- **Real-text trade-off:** generated review text is weak for semantic search. Smart middle path:
  sample REAL Kaggle text and attach to streamed stays — correlation AND real language.
**Status:** Open decision — correlation already works (verified). Streaming-reviews is an optional
Phase-8-class feature, NOT urgent. Lower priority than B-029/B-032 (the monitor redesign).

---

## Phase 7 — Monitoring

> See `docs/phase-7-monitor.md` (executable runbook) and
> `monitor_implementation_context.md` (full design rationale + data constraints).

---



|---|---|---|---|
| L-001 | `agg_daily_hotel_kpi` empty until Phase 6 Airflow DAGs | Phase 4 | B-013 |
| L-002 | ~~Hybrid queries (SQL filter + semantic content) degrade silently~~ — RESOLVED by B-004 (Phase 5) | Phase 4 | RESOLVED |
| L-003 | Duplicate reviews in `reviews_raw` from Kaggle dataset | Phase 3 | B-006 |
| L-004 | Ollama SQL accuracy ~85-90% on real analytical queries; remaining errors are capability limits (see L-010, L-011), not prompt bugs. Prompt tuning has hit diminishing returns. | Phase 4 | B-017 (model tiering) + B-022 (pin-time verification) |
| L-005 | IVFFlat recall degrades past ~1M vectors | Phase 3 | Switch to HNSW at scale |
| L-006 | No Airflow orchestration — batch jobs run manually | Phase 4 | Phase 6 |
| L-007 | Streaming events are synthetic (Python simulator, not real PMS) | Phase 2 | Kafka Connect to real PMS APIs in production |
| L-008 | Flash attention not compiled in torch — CPU fallback for attention | Phase 3 | Update Nvidia drivers + recompile torch with CUDA 12.x |
| L-009 | Dashboard read path triggers LLM/SQL compute (no cache yet) | Phase 5 | B-022 (cache + frozen SQL), then B-024 (scheduled refresh) |
| L-010 | ~~7B model miscounts dimension entities — joins fact_bookings and counts booking rows instead of querying the dimension table directly~~ — **RESOLVED.** Migration 006 added `hotel_master.opened_year` and the system prompt gained the ENTITY COUNT RULE plus the COLUMN LOCATION block scoping `is_cancelled` to fact_bookings. Verified stable across the protected suite: "list hotels count created per year" uses `hotel_master.opened_year` (sums to ~2000); "how many hotels per city" / "how many customers per state" / "list 5 customers" all query their dimension tables directly with NO `is_cancelled` filter (Tests 1, 2, 3, 10 each pass on the reverted single-bullet prompt). The fact-aggregation cancellation portion — bare `<aggregate> by <dimension>` queries occasionally dropping `WHERE NOT b.is_cancelled` — is split out as **L-013**. | Phase 4 | RESOLVED (migration 006 + prompt updates) |
| L-011 | 7B model omits DISTINCT on plain entity-list queries — "5 customers named R" returns the same person repeated (one row per booking). Adding "unique" to the query fixes it. Capability limit, not a prompt bug; further prompt tuning regresses other query types. | Phase 4 | B-017 (bigger model) |
| L-012 | Sentiment-topic conflation in semantic review search. Embeddings match TOPIC not POLARITY — "cleanliness complaints" returns cleanliness praise and complaints alike, since both are about cleanliness. A `reviews_raw.rating` filter is NOT a reliable proxy: confirmed via testing that complaints (e.g. "foul smell", "cobwebs") appear in mixed-sentiment 3★ reviews with positive openers, and identical review texts exist across 1–5★. Proper fix requires sentiment scoring at embed time (store a sentiment score per review, filter on it) — a feature, not a filter. Current behaviour: semantic search surfaces topically-relevant reviews; summary should describe results as "reviews mentioning X" not "X complaints". Discovered during B-004 manual testing. | Phase 4 | Planned: **B-026** (sentiment-at-embed-time). Copy fix on the summary line is a smaller separate item still open. |
| L-013 | Bare grouped aggregations over `fact_bookings` (shapes like "total revenue by city", "bookings by month", "revenue by customer segment", "average nights stayed by season") intermittently drop the `WHERE NOT b.is_cancelled` filter on Qwen-7B. Stronger cues — `LIMIT`, explicit `WHERE` filters on star_category / city / etc. — usually keep the filter (Tests 6 and 7 in the verification suite). Bare grouped shapes do not. **Confirmed not promptable** at this model size: two prompt rewrites attempted — a single-bullet "applies ONLY when fact_bookings is in FROM/JOIN" version (the current text) and a two-bullet universal-rule-plus-illustrations version. Neither resolves the bare-grouped cases. The two-bullet version improved "by month" and "by segment" to 3/3 but degraded an unnamed-shape generalisation ("average nights stayed by season") and added marginal noise on the LIMIT-bearing cases. Root cause: Qwen 7B underweights universal/conditional rules in the system prompt relative to attention pull from concrete examples in the same prompt — a small-model attention budget limit, not a prompt-wording bug. Same family as L-010 and L-011 (capability ceiling, not prompt tuning). **Safeguard:** B-022 freezes generated SQL at pin time, so a missing cancellation filter is a one-time review failure at pin, not a per-refresh data integrity bug — the read path never runs unverified SQL. **Fix path:** B-017 (model tiering — Gemma 12B or similar holds rule discipline better in early testing) is the real remediation. | Phase 4 | B-017 (larger model for SQL) — B-022 pin-time review is the meanwhile safeguard |
| L-014 | ~~CHECKIN / CHECKOUT (and PRICE_CHANGE) events are silently filtered at the consumer's Gate 3 — not aggregated, not counted, not stored.~~ — **RESOLVED (B-032 Chunks 2 + 3).** `PROCESSED_EVENT_TYPES` now spans `BOOKING, CANCELLATION, CHECKIN, CHECKOUT`; the consumer counts each per window and writes `total_checkins / total_checkouts / total_cancellations` to `agg_hourly_city_stats` (migration 007 columns). The producer (Chunk 3) now actually emits both CHECKIN and CHECKOUT at design weights (0.18 / 0.12), so both columns read non-zero on every recent window — verified end-to-end. PRICE_CHANGE is still silently filtered at Gate 3 (valid event type, just not in `PROCESSED_EVENT_TYPES`) — out of scope for this item. **Note:** these counts live ONLY in the stream aggregate (`agg_hourly_city_stats`); they are NOT in `fact_bookings` or any revenue path. Anything that wants a checkin-aware booking model (e.g. tying CHECKIN/CHECKOUT events to specific bookings) still has to be designed separately. | Phase 7 | RESOLVED by B-032 Chunks 2 + 3. |
| L-015 | ~~No live "events received / sec" throughput metric. The consumer's `run_metrics` counters live in memory and print only at shutdown — not written to a queryable table mid-run.~~ — **RESOLVED in full (B-032 Chunks 1+2+4).** Consumer writes a `pipeline_metrics` heartbeat row every `FLUSH_CHECK_SECONDS` (~10s) with cumulative `events_consumed / bookings / cancellations / malformed / late / active_windows / max_event_ts` (plus reserved-NULL `consumer_lag`). The `/monitor` header pulse reads the latest two heartbeat rows and surfaces events/sec as the delta divided by interval, with `alive = age < 15s` driving a green/grey dot. Verified end-to-end against a 50 evt/s producer: computed rate 49.6–50.2 evt/s mid-run; dot goes grey within one tick when the consumer stops. | Phase 7 | RESOLVED by B-032 (all 4 chunks). |
| L-016 | Consumer never flushes under sustained load → monitor stays Stale / agg table frozen. TWO compounding root causes, both confirmed by reading `scripts/stream_consumer.py`: **(A) flush-check starvation** — the main loop was `while running: for msg in consumer:`, and the flush check + max-runtime check live OUTSIDE the inner `for`. The inner loop only exits after `consumer_timeout_ms` (1s) of SILENCE; under a continuous stream (esp. `--chaos`, where every bad event does a blocking `S3.put_object`) the topic never goes quiet for a full second, so the inner loop never ended and neither check ever ran. `--max-runtime` therefore also never fired. **(B) broker eviction** — `session_timeout_ms` / `heartbeat_interval_ms` / `max_poll_interval_ms` were UNSET (kafka-python defaults 10s / 3s / 5min); slow chaos batches between polls exceeded the interval, so the broker evicted the consumer ("no active members"), lag ballooned, and because the loop never exited cleanly the graceful-shutdown flush never fired either. NOTE: the earlier cp1252-crash hypothesis was a RED HERRING — `stream_consumer.py` already had `sys.stdout.reconfigure(encoding="utf-8")` (line 111); the consumer's own logging was never the cause. **Why the 44 existing windows exist anyway:** earlier runs were low/no-chaos (inner loop idled → flushed) and/or Ctrl-C'd (graceful force-flush). | Phase 2 / Phase 7 | **Fix SHIPPED via scoped frozen-file exception** (see "Phase 2 hardening" note below) — pending live verification (Tests A no-chaos flush / B chaos no-eviction). Until verified, B-027 Test 2's live stream-increment stays open. |

---

## Completed

| ID | Item | Completed in |
|---|---|---|
| ✓ | 14-table star schema + bulk load | Phase 1 |
| ✓ | Six-gate Kafka consumer + dual sink | Phase 2 |
| ✓ | 30K review embeddings + IVFFlat index | Phase 3 |
| ✓ | Semantic playground script | Phase 3 |
| ✓ | Keyword router (SQL vs semantic) | Phase 4 |
| ✓ | Text-to-SQL via Ollama + sqlparse guard | Phase 4 |
| ✓ | Semantic search with city scoping | Phase 4 |
| ✓ | Fix `hot`/`ac` substring trigger bug in router | Phase 4 |
| ✓ | Add `__init__.py` to `ai/` package | Phase 4 |
| ✓ | Fix `agg_daily_hotel_kpi` column hallucination in prompt | Phase 4 |
| ✓ | B-001 — Retry loop on SQL execution failure | Phase 4 |
| ✓ | B-002 — Prefer fact_bookings for booking/count queries (prompt rule) | Phase 4 |
| ✓ | B-003 — Column name validation before execution (information_schema map + sqlparse walk; conservative skip on ambiguous refs; shares B-001 retry path) — test: `tests/test_validate_columns.py` | Phase 5 |
| ✓ | B-004 — Hybrid query support (filter detection + hotel_id scoping in semantic search; no second LLM call; resolves L-002) — test: `tests/test_hybrid_queries.py` | Phase 5 |
| ✓ | Full system-prompt rewrite — real types, FK chain, date handling, city-list-reference-only | Phase 4/5 |
| ✓ | B-007 — Widget renderer (shape → widget type) | Phase 5 |
| ✓ | B-008 — Pin widget to dashboard (Postgres table, not JSON) | Phase 5 |
| ✓ | B-009 — Auto-refresh on page load | Phase 5 |
| ✓ | B-010 — Scheduled auto-refresh per widget | Phase 5 |
| ✓ | B-011 — Delete widget from dashboard | Phase 5 |
| ✓ | B-012 — Widget layout grid | Phase 5 |
| ✓ | Settings gear popover (refresh frequency + width) | Phase 5 |
| ✓ | Refresh-all button | Phase 5 |
| ✓ | Bar-chart readability warning (>20 bars, Option A) | Phase 5 |
| ✓ | B-018 — CLAUDE.md at repo root | Phase 5 |
| ✓ | B-019 — File-tree + Claude Code instructions in all phase docs | Phase 5 |
| ✓ | B-027 — Pipeline monitor dashboard (/monitor): 3 sections (stream activity / review embeddings / pipeline health), date+city filter, guarded Airflow+MinIO deps, honest empty-states | Phase 7 |
| ✓ | B-029 — `/monitor` in-place auto-refresh: `GET /monitor/data` JSON sidecar + 10s `setInterval` poll patching live pulse + EVENTS + QUARANTINE via `textContent`. Scroll position and filter focus preserved. Skipped B-029a's full-page-reload variant — shipped straight at B-029b alongside B-032 Chunk 4. | Phase 7 |
| ✓ | B-032 — Live pipeline metrics + lifecycle counts. Migration 007 (`pipeline_metrics` + 4 count cols on `agg_hourly_city_stats`); consumer heartbeat + per-type counting; producer CHECKOUT emission; `/monitor` redesigned with header live pulse (events/sec from heartbeat deltas, alive=age<15s), default-today filter, EVENTS / QUARANTINE / HEALTH sections, SOON tiles for Review / Embedded / Sentiment with blocking backlog IDs. Revenue removed (business → Explorer). Retires L-015; resolves L-014 for counting (CHECKIN/CHECKOUT now in `agg_hourly_city_stats` though not in `fact_bookings`). | Phase 7 |
