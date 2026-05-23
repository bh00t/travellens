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
| L-014 | CHECKIN / CHECKOUT (and PRICE_CHANGE) events are silently filtered at the consumer's Gate 3 — not aggregated, not counted, not stored. So the monitor (B-027) cannot show "customers checked in / out" counts; the data is never captured. Surfacing it would require instrumenting `stream_consumer.py` to count/persist these event types — a change to a FROZEN Phase-2 file. | Phase 7 | Deferred — needs consumer instrumentation (frozen file) |
| L-015 | No live "events received / sec" throughput metric. The consumer's `run_metrics` counters (events_consumed, malformed_dropped, late_dropped) live in memory and print only at shutdown — they are not written to a queryable table mid-run. The monitor (B-027) therefore infers activity from what landed (processed in `agg_hourly_city_stats` + quarantine objects in MinIO), not from a live rate. A true throughput gauge would need a metrics table the consumer writes to — a change to a FROZEN Phase-2 file. | Phase 7 | Deferred — needs a consumer-written metrics table |

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
