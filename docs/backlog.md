# TravelLens India — Project Backlog

> Living document. Items move from backlog → in progress → done as phases ship.
> Add new items at the bottom of the relevant section. Never delete — cross out if abandoned.

---

## Status at a glance

**Legend.** `B-NNN` = work item (something we plan to build or fix). `L-NNN` =
known limitation (something the system does today that we accept, work around,
or have a planned remedy for). Done items live in the [Completed](#completed)
table — that's the canonical source of done-state; the per-item sections below
carry context only.

### OPEN — work items (B-)

- B-003a — `_validate_columns` misses bare WHERE refs on single-table queries (low)
- B-005 — prompt normalisation before routing
- B-006 — semantic search duplicate reviews (resolves L-003)
- B-013 — `daily_hotel_kpi` DAG (Phase 6)
- B-014 — `reconcile_late_events` DAG (Phase 6)
- B-015 — `hotel_sentiment_scores` DAG (Phase 6)
- B-016 — `customer_ltv` DAG (Phase 6)
- B-017 — LLM comparison: Qwen2.5-Coder-7B vs Gemma 3 12B
- B-020 — change chart type from the dashboard
- B-021 — drag-and-drop reordering — polish (core done)
- B-022 — decouple read path from compute (cache + frozen SQL)
- B-023 — About page redesign
- B-024 — `refresh_pinned_widgets` DAG (Phase 6, compute path for B-022)
- B-025 — fixed-height scrollable table / review widgets
- B-026 — sentiment classification for reviews (resolves L-012)
- B-028 — `run.py` dev launcher (built; not yet committed / verified)
- B-030 — REVIEW as a stream event (feature gap; not urgent)
- B-031 — consumer resilience fix (L-016; fix shipped, pending live verification)
- B-033 — `quarantine_daily_rollup` DAG (Phase 6; self-healing daily summary)
- B-034 — stateful lifecycle simulator — step 1 shipped, superseded by B-035 + B-036/B-037 (kept for context)
- B-036 — calendar simulator Phase B (net-new synthetic + 10–25% long-stay tail)
- B-037 — calendar simulator Phase C (REVIEW emission)
- ~~B-040 — gold layer (per-booking lifecycle reconstruction + transition flags)~~ ✓ Done
- B-041 — producer rate-flag migration (run.py / README:101 `--rate` → `--sim-speed`; phase-7-monitor.md CHECKOUT "design weight 0.12" copy → calendar-replay language)

### OPEN — limitations (L-)

- L-001 — `agg_daily_hotel_kpi` empty until Phase 6 DAGs run
- L-003 — duplicate reviews in `reviews_raw` from Kaggle dataset
- L-004 — Ollama SQL accuracy ~85–90% on real analytical queries
- L-005 — IVFFlat recall degrades past ~1M vectors
- L-006 — no Airflow orchestration yet (Phase 6)
- L-007 — streaming events are synthetic, not from a real PMS
- L-008 — flash attention not compiled in torch (CPU fallback)
- L-009 — dashboard read path triggers LLM/SQL compute (B-022 in flight)
- L-011 — 7B model omits `DISTINCT` on plain entity-list queries
- L-012 — sentiment-topic conflation in semantic review search
- L-013 — bare grouped aggregations occasionally drop `WHERE NOT is_cancelled`
- L-016 — consumer never flushes under sustained load (fix shipped, pending verification)

### DONE

See the [Completed](#completed) table at the bottom of this file. Currently
holds B-001..B-004, B-007..B-012, B-018, B-019, B-027, B-029, B-032, B-035,
B-034A (calendar simulator Phase A), B-038 (bronze sink), B-039 (silver
sink), B-040 (gold lifecycle layer), B-042 (monitor quarantine date scoping),
plus the unnumbered Phase 1–5 foundation items.

### ABANDONED

None to date. (When an item is abandoned, strike through its header and add an
"Abandoned: <reason>" line — keep the body for context per the "never delete"
rule.)

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

### B-029 — `/monitor` auto-refresh (no manual reload)
**Status:** ✓ Done — see [Completed](#completed) table. Resolved by the Chunk 4 redesign:
landed straight at B-029b (the smoother JSON+JS variant, skipping B-029a's
full-page reload as unnecessary now that B-032 was landing in the same change).
`render/server.py` exposes `GET /monitor/data` returning the live + stream +
quarantine subset as JSON; `render/templates/monitor.html` polls it every 10s
and patches values via `textContent`, preserving scroll position and
filter-form focus. No env knob needed — 10s is hard-coded to match the
consumer's `FLUSH_CHECK_SECONDS`. Pairs with B-032 to make the live pulse
update in place without reload.

---

### B-032 — Live pipeline metrics (real-time throughput; resolves L-015)
**Status:** ✓ Done — see [Completed](#completed) table. All 4 chunks delivered.
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

### B-035 — Lifecycle history backfill — time-partitioned real model (step 2 of B-034)
**Status:** ✓ Done — see [Completed](#completed) table.
**Priority:** High — unblocks the calendar simulator that advances `sim_open_bookings` over time, and gives analytics a fully populated event ledger before the first live event flies.
**Origin:** B-034 step 1 left two cold-start gaps. (a) `fact_booking_events` was empty, so per-event analytics had nothing to read. (b) The stream simulator had no `sim_open_bookings` to advance.
**Revision history:**
- **First cut** sampled the oldest 250K bookings (`ORDER BY booking_ts LIMIT N`) and synthesised the open backlog with new UUIDs. Two problems: (i) history stopped at 2024-12-20 — a 17-month void between latest history and "today"; (ii) backlog bookings were invented, breaking the "every open booking is a real booking" property the simulator needs to replay realistically.
- **This revision** processes ALL of `fact_bookings` in a single sweep and time-partitions every row against `--sim-today`. No sampling, no synthesis. The "open backlog" is now the real bookings that were in-flight at the anchor; the "future" is real bookings whose `booking_ts` is on/after the anchor — exactly what the stream simulator will replay later.
**What landed:**
- **Migration `db/migrations/008_lifecycle_events.sql`** — `fact_booking_events` (silver event ledger) + `sim_open_bookings` (mutable simulator state). Unchanged from the first cut.
- **`scripts/generate_lifecycle_history.py`** — fully rewritten. CLI shrunk to `--sim-today` (default `2025-06-01`) and `--reset / --no-reset` (default reset). The old `--history-bookings` and `--open-backlog` flags are gone — there is no sampling and no synthesised backlog. Every function carries a docstring (verified: 13 / 13).
- **Routing model (one sweep over fact_bookings, ORDER BY booking_ts):**
  - `booking_ts >= sim_today` → **FUTURE** — skip; reserved for the stream simulator to replay later. Count and report range.
  - `booking_ts < sim_today` AND `checkout_date < sim_today` → **COMPLETED** — emit `BOOKING + CHECKIN + CHECKOUT` (or `BOOKING + CANCELLATION` if `is_cancelled`).
  - `booking_ts < sim_today` AND `checkin_date < sim_today <= checkout_date` → **IN_PROGRESS** — emit `BOOKING + CHECKIN` as history, insert `sim_open_bookings` state `CHECKED_IN`. Guest is mid-stay at the anchor; `is_cancelled` flag intentionally ignored here.
  - `booking_ts < sim_today` AND `checkin_date >= sim_today` → **BOOKED** — emit `BOOKING` as history, insert `sim_open_bookings` state `BOOKED`. Cancellation (if any) happens "later" — the simulator can fire it as time advances.
- **Realism guards (skip-or-fix, never silently corrupt):** `checkout > checkin` (gap ≥ 1 night); `1 ≤ nights ≤ 60`; `booking_ts ≤ checkin_date`; `1 ≤ num_guests ≤ 10`. Skipped rows counted by reason. If `nights_stayed` disagrees with `checkout-checkin`, trust the gap, recompute `revenue = nightly_rate × gap`, and count the mismatch (don't drop the row).
- **`--reset`** still deletes ONLY `source='history'`; never touches `source='stream'`. RNG seed pinned (`20260601`) so idempotent reruns reproduce identical counts.
- **Stats summary (full sweep, sim-today=2025-06-01, runtime ~120s on the dev box):**
  - 1,000,000 fact_bookings rows seen → 612,380 processed (kept), 0 skipped (real data is clean), 0 nights mismatches, 387,620 FUTURE (skipped — reserved for the stream).
  - Buckets: COMPLETED 595,192 (of which 524,935 completed, 70,257 cancelled — 11.8% cancellation rate matches `fact_bookings`); IN_PROGRESS 2,995; BOOKED 14,193.
  - Future booking_ts range: 2025-06-01 → 2026-05-16 (~11.5 months of real runway for the stream).
  - 1,735,502 events written (BOOKING 612,380 / CHECKIN 527,930 / CHECKOUT 524,935 / CANCELLATION 70,257).
  - History `event_date` range: 2023-09-03 → 2025-05-31 — **latest is 1 day before sim-today, CONTINUITY satisfied** (no void).
  - Open backlog: 17,188 rows (14,193 BOOKED + 2,995 CHECKED_IN). All real fact_bookings IDs.
  - 1,908 / 2,000 hotels (95.4%); 20,000 / 20,000 customers (100%).
  - Nights histogram (kept bookings): 1–3 nights 368,379 (60.2%); 4–7 nights 238,858 (39.0%); 8–14 nights 5,143 (0.8%); 15–30 nights 0; 31–60 nights 0. Reflects fact_bookings' own distribution (business 1–2 nights, honeymoon up to 7, near-zero long-stay tail).
  - Realism: nights min/median/mean/max 1/3.0/3.33/10; lead time 0/14/18.70/120 days; num_guests 1/2/4; nightly_rate ₹235 / ₹4,057 / ₹65,055.
- **Acceptance (TEST | EXPECTED | ACTUAL | PASS):** **15 / 15 PASS**.
  - keys resolve (customer/hotel/room) — 0/0/0 orphans; CHECKIN/CHECKOUT/CANCEL has matching BOOKING — 0 dangling.
  - revenue == nightly_rate × nights — 0 mismatches.
  - REALISM: 0 events with `checkout ≤ checkin`; 0 with `nights ∉ [1,60]`; 0 BOOKING events with `event_date > checkin_date`; 0 events with `num_guests ∉ [1,10]`.
  - CONTINUITY: gap (sim-today − latest history event_date) = **1 day** (≤ 7-day threshold).
  - BACKLOG REAL: every `sim_open_bookings.booking_id` exists in `fact_bookings` — 0 synthesised.
  - No `source='history'` event_date > sim-today — 0.
  - DOCS: 13 functions, 13 documented (100%).
  - Idempotent rerun: identical counts (1,735,502 history / 17,188 open) AND pre-seeded `source='stream'` dummy preserved.

**Files:** `db/migrations/008_lifecycle_events.sql`, `scripts/generate_lifecycle_history.py` (rewritten), `datamodel.md`, `docs/phase-2-streaming.md`, `docs/session-notes.md`, `CLAUDE.md`, this file.

**Next step (deliberately NOT in B-035):** calendar simulator. As live time advances past `sim-today`, replay the 387,620 FUTURE bookings from `fact_bookings` (sorted by `booking_ts`) as `source='stream'` BOOKING events, AND advance the real backlog in `sim_open_bookings` toward CHECKIN/CHECKOUT/CANCELLATION at their real dates. Both halves share the same `--sim-today` anchor as this script. Tracked as B-034A (Phase A) below.

---

### B-034A — Calendar simulator, Phase A (replay engine) ✓ DONE
**Origin:** the "next step" of B-035. Replaces the stateful-lifecycle producer
(B-034 step 1) with a calendar-driven REPLAY engine that walks `--sim-start`
forward one day at a time, emitting BOOKINGs from `fact_bookings.booking_ts`
order and advancing the real `sim_open_bookings` backlog through CHECKIN /
CHECKOUT / CANCELLATION at the real dates each booking carries. Deferred to
Phases B and C: net-new synthetic bookings + long-stay tail (**B-036**) and
REVIEW emission once the consumer accepts REVIEW (**B-037**).

**What landed:**
- `scripts/kafka_event_producer.py` fully rewritten as the calendar replay
  simulator. Same five wire `event_type` strings, same Kafka config
  (`acks="all"`, `linger_ms=20`, bytes-pass-through serializer, `key=city`).
  ONE new wire field: `event_date` (ISO date — the sim-day the event
  represents). `event_ts` stays wall-clock UTC NOW so the consumer's
  event-time windowing and freshness checks behave identically.
- **Sim-clock** persisted to `scripts/.sim_clock.json` (gitignored). On
  startup the producer resumes at `last_completed_day + 1`, or starts at
  `--sim-start` (default `2025-06-01`, MUST match the history generator)
  if the file is absent or `--reset-clock` is passed. `--chaos-seed` is
  ALSO persisted to the clock file so cancellation plans stay
  deterministic across restarts without re-passing the flag.
- **Hydration** on startup reads `sim_open_bookings` (every booking
  in-flight at the anchor — generator-emitted BOOKING already on the
  wire as `source='history'`), JOINs `fact_bookings` to recover
  `is_cancelled` (NOT a column on `sim_open_bookings`), and schedules
  cancellation events for the BOOKED-and-is_cancelled subset. CHECKED_IN
  rows ignore `is_cancelled` per the B-035 design — their CHECKIN was
  already emitted as history; they will check out on their real date.
- **Per sim-day D** the producer collects four query results and
  shuffles them deterministically before emitting (seed:
  `f"day|{D}|{chaos_seed}"`):
  1. CANCELLATIONS planned for D (from prior days' BOOKINGs or
     hydration). Plan is keyed off `f"{booking_id}|{chaos_seed}"` —
     ~70% `customer_cancelled` on a day in `[max(booking_ts, sim_start),
     checkin_date - 1]`, ~30% `no_show` on `checkin_date`.
  2. NEW bookings — `fact_bookings WHERE booking_ts::date = D`, emitted
     in `booking_ts` order. Inserted into `sim_open_bookings` as
     `source='stream'` state `BOOKED` UNLESS same-day cancellation
     (don't persist, emit BOOKING+CANCELLATION pair).
  3. CHECKINs — open BOOKED rows with `checkin_date = D`. UPDATE state
     to `CHECKED_IN`.
  4. CHECKOUTs — open CHECKED_IN rows with `checkout_date = D`. DELETE.
  Plus a handful of stateless PRICE_CHANGE events (default 5/day, no
  `booking_id`/`customer_id` — preserved unchanged).
- **Day boundaries are atomic commit points.** Within a day the
  producer mutates `sim_open_bookings` uncommitted; at end of day it
  flushes Kafka → commits the DB → saves the clock. SIGINT clears the
  running flag but the day-loop only checks BETWEEN days, so a
  graceful Ctrl-C finishes the in-progress day before exiting.
- **Outcome fidelity** comes from `fact_bookings.is_cancelled`, NOT
  from a random draw. An is_cancelled booking emits CANCELLATION and
  never CHECKIN/CHECKOUT; a non-cancelled booking emits BOOKING + (on
  checkin_date) CHECKIN + (on checkout_date) CHECKOUT.
- **Revenue invariant** `revenue_inr == nightly_rate_inr * nights`
  asserted on every BOOKING. When `fact_bookings.nights_stayed`
  disagrees with `checkout - checkin`, the producer trusts the gap and
  recomputes — same rule the history generator (B-035) follows.
- **Wire types unchanged → consumer unchanged.** Gate 2's required
  fields (`event_type, event_ts, city, hotel_id`) are all present;
  `event_date` is additive and ignored by the consumer.
- **REVIEW deferred to B-037.** Emitting it now would route every one
  to `unknown_event_type` quarantine — the consumer's
  `VALID_EVENT_TYPES` still excludes it (B-030).
- **Backward-compat shims.** The deprecated `--rate` and `--duration`
  flags are accepted-and-ignored with a warning so `run.py` (which
  still passes `--rate`) keeps working until its next pass.

**Acceptance (21 checks, all PASS):**
- Hydration: 17,188 rows loaded (14,193 BOOKED + 2,995 CHECKED_IN);
  1,229 cancellations planned.
- Clock resumes correctly across restarts (saved+1).
- No-chaos 4-day slice: 9,942 events emitted; consumer ingested all
  9,942 with **0 malformed / 0 late**; 44 windows flushed; per-type
  agg sums (`total_bookings 3,034 / total_checkins 2,790 /
  total_checkouts 3,397 / total_cancellations 701`) match producer
  wire counts **exactly**.
- Quarantine S3 delta = 0 on clean run.
- LINKAGE: 0 / 2,529 lifecycle booking_ids absent from `fact_bookings`;
  0 booking_ids with both CHECKOUT and CANCELLATION; 0 duplicate
  `(booking_id, event_type)` pairs.
- OUTCOME FIDELITY: 108 is_cancelled BOOKINGs in capture, 0 of them
  got CHECKIN or CHECKOUT. Cancellation reasons split into both
  `customer_cancelled` and `no_show`.
- FIELDS: 3,402 / 3,402 events carry `event_ts` AND `event_date`; 0
  events have `event_ts` outside ±10 min of wall-clock now; 0 / 5
  PRICE_CHANGE carry `booking_id`/`customer_id`; 0 / 868 BOOKING
  violate the revenue invariant; 0 / 2,529 lifecycle events missing
  `booking_id`/`customer_id`; 0 / 178 CANCELLATION missing reason.
- STATE: 4,563 new `source='stream'` rows inserted; sim_open_bookings
  shrank from 17,188 to 16,046 over 7 sim-days; deltas reconcile with
  emitted counts.
- RESTART: re-run with same `--until` after a completed slice → 0
  events, clock unchanged. Re-run with new `--until` → resumes at
  saved+1.
- CHAOS: producer chaos counts (229 malformed: 59 unknown_city + 49
  unparseable_json + 46 unparseable_event_ts + 39 missing_field + 36
  unknown_event_type, 94 late) match consumer's run summary **exactly**;
  `malformed_events/` S3 prefix gained 229 objects, `late_events/`
  gained 94. Reproducible with `--chaos-seed 42`.

**Files touched:** `scripts/kafka_event_producer.py` (rewritten),
`scripts/.sim_clock.json` (new, gitignored), `.gitignore`,
`datamodel.md`, `docs/phase-2-streaming.md`, `docs/session-notes.md`,
`CLAUDE.md`, this file.

**Next:** **B-036** wires net-new synthetic bookings + a 10–25%
long-stay tail (real `fact_bookings` has near-zero stays >7 nights).
**B-037** turns REVIEW emission on after the consumer accepts REVIEW
(closes B-030 in tandem). Neither is in B-034A's scope.

---

### B-036 — Calendar simulator, Phase B (net-new synthetic + long-stay tail)
**Priority:** Medium — depends on B-034A (done).
**Problem (and why it isn't fixed in Phase A):** Phase A REPLAYS the real
runway. That gives perfect fidelity for outcomes, dates, and customer
linkage — but it inherits `fact_bookings`' own distribution shape. Two
gaps follow:
  - **Stays >7 nights are near-zero** in `fact_bookings` (the B-035 stats
    block shows 60.2% short / 39.0% mid / 0.8% in 8–14 nights / 0% above).
    Real hospitality has a 10–25% long-stay tail (extended stays, leisure
    weeks, religious tours). The replay can't synthesise what isn't there.
  - When the runway is exhausted (~2026-05), the simulator's BOOKING
    stream stops cold. The hydrated backlog still drains, but no new
    bookings fire after that point.
**Fix sketch:** alongside the runway replay, mint a small synthetic
stream — same wire shape as Phase A's BOOKING events, but with
synthetic `booking_id`s (clearly distinguishable, e.g. `SIM-` prefix
or recorded as `source='synthetic'` on `fact_booking_events` later)
and a separate long-stay distribution (lognormal centred near 10–14
nights with a tail to 30 nights). Tunable mix ratio: target ~10–25%
of total new-booking volume. Synthetic bookings should NOT write to
`fact_bookings` (that table is the original-source historical record);
they MAY write to `sim_open_bookings` for lifecycle progression, with
`source='synthetic'`. Outcome (is_cancelled) is a draw with the same
prior as `fact_bookings` (~11.8% cancellation rate).
**Acceptance (sketch):** A 14-sim-day run produces a stay-length
histogram with the 10–25% tail visible above 7 nights; the consumer
still drops 0 events; the synthetic booking_ids never collide with
fact_bookings UUIDs; cancellation reason split holds.
**Out of scope:** REVIEW (B-037).

---

### B-037 — Calendar simulator, Phase C (REVIEW emission, resolves B-030)
**Priority:** Medium — depends on B-034A (done) AND on the consumer
accepting REVIEW (the consumer half of B-030).
**Sequencing:** consumer first. Today `VALID_EVENT_TYPES` does NOT
include `REVIEW`; emitting it from the producer routes every one to
`unknown_event_type` quarantine. The consumer change extends the
allow-list, adds a `PROCESSED_EVENT_TYPES` branch that writes
`total_reviews` on the `agg_hourly_city_stats` upsert (the column
already exists per migration 007), and ideally embeds-at-ingest.
**Producer side:** once the consumer accepts REVIEW, the producer
fires REVIEW at the right lifecycle point — POST-CHECKOUT, on the
checkout sim-day or a few days after, with sampled real Kaggle review
text per B-030's notes (real-text trade-off: generated text is weak
for semantic search; sampling real text gives hotel-level correlation
AND real language). Probabilistic: not every checkout produces a
review (a small share, ~10–20%, matches real OTA review rates).
**Acceptance (sketch):** Some bookings produce a REVIEW after
CHECKOUT; the consumer counts them in `total_reviews`; the
`/monitor` SOON tile for Review goes live; downstream
`review_embeddings` regeneration covers stream-emitted reviews when
re-run.
**Closes B-030** when both halves ship.

---

### B-038 — Bronze sink: durable raw archive of every accepted event ✓ DONE
**Origin:** the streaming pipeline had three persistent outputs — the
agg UPSERT (lossy: derived totals only), the agg Parquet archive
(lossy: aggregate rows, not events), and the two quarantine prefixes
(only events that FAILED a gate). Nothing kept an immutable copy of
the events that succeeded. Per-event analytics, replay testing, and
the planned silver layer all need that raw archive. B-038 fills the
gap.

**What landed in `scripts/stream_consumer.py`:**
- New `bronze_flush(buffer, metrics)` function. Serialises the buffer
  to JSONL bytes (one event per line, many events per file) and writes
  one S3 object per call. Wrapped in its own try/except — any failure
  logs to stderr, increments `bronze_failures`, and returns an empty
  buffer (best-effort-durable; dropping a batch is preferable to
  retrying indefinitely and leaking memory if MinIO is unreachable).
- Bronze buffer lives in `main()`; events are appended right after
  Gate 4 (the late-event guard) passes, BEFORE the window accumulator
  is touched. The buffer flushes when EITHER (a) it hits
  `BRONZE_BUFFER_CAP` events (default 500, env-overridable) OR (b)
  the existing `FLUSH_CHECK_SECONDS` periodic tick fires — whichever
  comes first. Final drain on graceful shutdown.
- S3 key:
  `raw_events/year=YYYY/month=MM/day=DD/hour=HH/HHMMSS_<uuid8>.jsonl`,
  partitioned by INGEST wall-clock time (same convention as
  `quarantine_event`'s `malformed_events/` and `late_events/`
  prefixes). NOT event-time — bronze is "what arrived," operationally
  indexed; event-time partitioning would put chaos-late events into
  past-dated folders.
- Counters: `bronze_events`, `bronze_files`, `bronze_bytes`,
  `bronze_failures` added to `run_metrics`; shutdown summary prints
  `Bronze archived: N events (F files, B KB)` and a TODO-style line
  if any flush failed.

**What did NOT change:** the agg UPSERT, the agg Parquet sink, the
heartbeat, the quarantine sinks, and the window logic are all
untouched. Bronze sits alongside them as a fourth sink, parallel to
agg.

**Acceptance (7/7 PASS — full table in this session's report):**
- A. Clean run: bronze 7,453 = accepted (BOOKING 2,177 + CHECKIN
  2,094 + CHECKOUT 2,606 + CANCELLATION 576); events_consumed 7,468 =
  bronze + 15 PRICE_CHANGE (Gate-3 silent filter).
- B. Chaos run: bronze 4,459 + malformed 228 + late 96 + 9
  PRICE_CHANGE = 4,792 emitted; 0 quarantined events appear in
  `raw_events/` (audit across 11,912 bronze events: 0 missing
  required fields, 0 PRICE_CHANGE, 0 event_ts outside ±1h of
  wall-clock now).
- C. Fidelity: every sampled BOOKING / CHECKIN / CHECKOUT /
  CANCELLATION carries the full wire payload including `event_ts`
  (wall-clock) and `event_date` (sim-day). CANCELLATION carries
  `cancellation_reason`. PRICE_CHANGE absent (filtered at Gate 3,
  never reaches bronze).
- D. Partitioning: all 32 files under
  `raw_events/year=YYYY/month=MM/day=DD/hour=HH/`.
- E. Batching: 32 files, 6,989 B → 184,530 B; cap-triggered batches
  are ~170 KB (500 events), tick-triggered batches are smaller.
  Never one-per-event, never one-giant-file.
- F. Isolation: forced via `S3_BUCKET=does-not-exist-isolation-test`.
  6 bronze flushes failed (logged "✗ Bronze sink error (N events
  dropped): NoSuchBucket"); despite that, Postgres agg upsert
  succeeded for all 44 windows, pipeline_metrics heartbeat wrote 6
  rows, the consumer consumed all 2,430 events and exited cleanly
  with the gap reported in its shutdown summary.
- G. No regression: per-type agg sums across both runs match bronze
  type counts exactly (BOOKING 3,692 / CHECKIN 3,390 / CHECKOUT
  4,019 / CANCELLATION 811); heartbeat firing every ~10s (46 rows /
  10:43); 88 windows flushed.

**Files touched:** `scripts/stream_consumer.py` (additive — no
existing sink modified). Docs same turn:
`docs/backlog.md` (this entry; B-039/B-040 reserved),
`CLAUDE.md` (consumer sinks list updated, common-mistakes row),
`datamodel.md` (medallion section — bronze raw_events/ documented),
`docs/phase-2-streaming.md` (POST-ACCEPTANCE HARDENING bullet).

**Next:** **B-039** (silver — parse + dedupe bronze JSONL into a
typed `fact_booking_events source='stream'` ledger) and **B-040**
(gold — per-booking lifecycle reconstruction + transition-flag
counts) build on bronze. Bronze is the durable input both depend
on; closing the loop on D from the Phase-A read-only assertions
("live-stream per-event ordering proof").

---

### B-039 — Silver layer: per-event ledger in fact_booking_events ✓ DONE
**Origin:** B-038 gave us a durable bronze archive but bronze is
JSON-on-S3 — wrong shape for per-event analytics. Silver writes the
SAME events to `fact_booking_events` (typed, indexed, source='stream'),
alongside the `source='history'` rows the generator (B-035) writes.
The schema is shared; the `source` column is the only distinguisher.
The original B-039 design called for an Airflow DAG that batched
bronze → silver overnight; this revision **writes silver inline on
the accept path** instead, so per-event analytics are live, not
T-minus-one-day.

**What landed in `scripts/stream_consumer.py`:**
- New `silver_flush(buffer, metrics)` and `_silver_row(event)` helpers.
  Buffer fills inline on the accept path, flushes on
  `SILVER_BUFFER_CAP` (default 500) OR the existing `FLUSH_CHECK_SECONDS`
  tick OR graceful shutdown — mirrors bronze exactly.
- Per-type field mapping in `_silver_row`:
    - BOOKING: full booking payload (booking_id, customer_id, hotel_id,
      room_type_id, checkin/checkout, nights, num_guests, rate, revenue,
      booking_source).
    - CHECKIN / CHECKOUT: base envelope + booking_id + customer_id.
    - CANCELLATION: same + `cancellation_reason`.
    - PRICE_CHANGE: hotel_id + room_type_id + old/new price.
      **booking_id and customer_id INTENTIONALLY NULL** (a price change
      is operational, not booking-scoped) — the schema allows this.
- INSERT uses `psycopg2.extras.execute_values` with **explicit
  `page_size=len(rows)`** so `cur.rowcount` reflects the entire batch.
  Default `page_size=100` silently undercounts on every batch
  > 100 rows — verified against a 500-row synthetic test (default
  rowcount=100, fixed rowcount=500). The counter bug surfaced during
  acceptance and was fixed before declaring done.
- `ON CONFLICT (event_id) DO NOTHING` — the existing PK on
  `fact_booking_events.event_id` (migration 008) provides the
  uniqueness; no migration 009 was needed.
- New `run_metrics` counters: `silver_attempted` / `silver_inserted` /
  `silver_duplicates` / `silver_flushes` / `silver_failures`.
- Shutdown summary line: `Silver inserted: N rows (F flushes, D dedup'd)`.

**Gate-order change (minimal):** to satisfy the brief's "no type
filter — all accepted types written" requirement for PRICE_CHANGE,
silver needs to fire BEFORE Gate 3's `PROCESSED_EVENT_TYPES` filter
that silently drops PRICE_CHANGE. The minimal restructure: parse
`event_ts` + Gate 4 (late guard) move above Gate 3, so the new order
is `Gate 1 → Gate 2 → parse → Gate 4 → SILVER → Gate 3 → BRONZE →
accumulator`. Bronze stays after Gate 3 (per "Do NOT touch bronze").
Quarantine semantics observably unchanged on every non-PRICE_CHANGE
event; the only side-effect is that late PRICE_CHANGE events now go
to `late_events/` instead of being silently dropped at Gate 3 — a
small fix to a previously-silent quirk, consistent with the
consumer's "nothing is silently dropped" docstring.

**Dedup posture (what silver covers and what it doesn't):**
- COVERED: Kafka redelivery (same Kafka message replayed by the
  consumer-group) — same `event_id` → ON CONFLICT short-circuits.
  Verified: re-insert 100 existing event_ids → cur.rowcount=0, table
  row count stable.
- NOT COVERED: producer crash-replay. The producer mints fresh
  `event_id = str(uuid.uuid4())` on every emit; a restart re-emitting
  a logically-identical event would mint a NEW event_id and silver
  would accept it. This is a known gap. The path forward is
  **deterministic event_id** (e.g. `uuid5(NAMESPACE, f"{booking_id}|{event_type}|{sim_day}")`)
  per-emit, which is a structural producer change tracked separately
  (not B-039's scope). Until that lands, dedup is at-least-once-per-
  emission, not exactly-once.

**Isolation:** `silver_flush` is wrapped in its own try/except.
Failures log to stderr, increment `silver_failures`, drop the batch,
and return `[]` — same posture as bronze + heartbeat. Force-failure
unit tests (`c:\tmp\silver_isolation_unit.py`) verify against two
distinct DB errors (bad password / missing table); both return `[]`,
bump the counter, and never raise.

**Acceptance (7/7 PASS):**
- A. Clean run (3-day slice + 1-day slice combined): silver inserted
  = events_consumed = accepted, 0 dedup'd, all 5 types present
  including PRICE_CHANGE (counts: BOOKING 4,543 / CHECKIN 2,228 /
  CHECKOUT 1,828 / CANCELLATION 446 / PRICE_CHANGE 30 cumulative).
- B. Chaos run (2 sim-days, 5% malformed + 2% late, seed 42):
  events_consumed 4,582 → silver inserted **4,272** (EXACT =
  4,582 − 221 malformed − 89 late); 0 quarantined rows landed in
  silver.
- C. Dedup: re-INSERT 100 existing event_ids via the same
  ON CONFLICT path → `cur.rowcount = 0`, table count delta = 0.
- D. Fidelity: one row per type sampled; field mapping correct per
  the spec, PRICE_CHANGE has NULL booking_id + NULL customer_id.
- E. Linkage: 3,845 stream BOOKING rows → 0 orphan booking_id /
  customer_id / hotel_id / room_type_id (FK JOIN to fact_bookings /
  dim_customer / hotel_master / dim_room_type).
- F. Isolation: silver_flush survives bad password + missing table
  without raising; consumer's agg + bronze + heartbeat unaffected
  (structurally identical to bronze's verified posture).
- G. No regression: clean run shows 44 windows flushed, S3 Parquet
  archive succeeds, heartbeat firing every ~10s, 0 malformed / 0
  late.

**Files touched:** `scripts/stream_consumer.py` (additive — only the
gate reorder is a structural change; no existing sink modified).
Docs same turn: `docs/backlog.md` (this entry, B-040 still reserved,
B-041 added for producer rate-flag migration), `CLAUDE.md`
(consumer-sinks description updated; the "stream-side inserts come
from stream_consumer.py" line is now IMPLEMENTED, not aspirational),
`datamodel.md` (silver section updated to describe source='stream'
rows + dedup posture).

**Next:** **B-040** (gold — per-booking lifecycle reconstruction with
`illegal_transition_flag`) reads `fact_booking_events` (both sources)
and materialises a per-booking gold table. The end-to-end ordering
proof obligation from the Phase-A read-only assertions' D becomes
`SELECT SUM(illegal_transition_flag) FROM gold` == 0.

---

### B-040 — Gold layer: per-booking lifecycle reconstruction + transition flags ✓ DONE
**Status:** Done — see [Completed](#completed) table.
**Priority:** Medium — depends on B-039 (done).

**What landed:**
- **`db/migrations/009_gold_lifecycle.sql`** — `ingest_seq BIGINT GENERATED BY DEFAULT AS IDENTITY` on `fact_booking_events` + index; `fact_booking_lifecycle` (one row per booking_id: forward-only status machine BOOKED→CHECKED_IN→COMPLETED/CANCELLED, `illegal_transition_flag`, `source_mix`, lifecycle timestamps, booking facts, outcome, event_count); `gold_watermark` (single-row cursor, seeded at 0).
- **`scripts/gold_lifecycle_updater.py`** — ~1-min micro-batch DECOUPLED from the consumer. Reads silver `WHERE ingest_seq > watermark`, groups by booking_id, sorts within each group by lifecycle order (BOOKING<CHECKIN<CHECKOUT/CANCELLATION). `illegal_transition_flag` fires on genuine business-domain inversions (CHECKOUT.event_ts < CHECKIN.event_ts; CANCELLATION after CHECKOUT) — NOT on heap-scan-order processing artifacts. Deadlock-resilient (rolls back + retries 10s). `psycopg2.extras.register_uuid()` required for UUID list param. Run via `python -m scripts.gold_lifecycle_updater`. ONLY ONE INSTANCE at a time — concurrent processes deadlock.
- **PRICE_CHANGE + REVIEW excluded from gold** (no booking_id on PRICE_CHANGE; REVIEW is hotel-level, not per-booking lifecycle).

**Acceptance (7/7 PASS):** row count 624,388 gold = 624,388 distinct silver booking_ids; illegal_flag 0; outcome completed 85.4%/cancelled 8.0%/no_show 3.4%/in_progress 3.2%; source_mix history 97.1%/stream 1.9%/mixed 0.95%; watermark==max_seq 1,763,493; 0 status inconsistencies; watermark held on re-run.

**Files:** `db/migrations/009_gold_lifecycle.sql`, `scripts/gold_lifecycle_updater.py`, `datamodel.md`, `CLAUDE.md`, this file.

---

### B-041 — Stale-references cleanup (producer rate-flag + Phase-7 doc)
**Priority:** Low — track only this turn; do NOT fix code.
**Origin:** the Phase-A producer rewrite (B-034A) repurposed
`--rate` (events/sec) as a deprecation no-op and introduced
`--sim-speed` (sim-days/sec). Several call sites and docs still
refer to the old contract:
  - `run.py` builds the simulator command with a configurable
    `rate_flag = "--rate"` and passes the `--sim-rate N` value
    straight through. The producer accepts but ignores the flag
    (it warns to stderr). The fix is to swap `rate_flag` for
    `--sim-speed` AND rescale the unit (events/sec → sim-days/sec;
    1 sim-day @ steady state ≈ 2,000 events, so `--sim-rate 50` ≈
    `--sim-speed 0.025`).
  - `README.md` line 101 quotes the old form in the `--no-sim`
    example: `python -m scripts.kafka_event_producer --rate 50
    --duration 60`. Replace with the new form
    (`--sim-speed 0.025 --until <date>` or equivalent).
  - `docs/phase-7-monitor.md` describes CHECKOUT as emitted "at
    design weight 0.12" (B-032 Chunk 3 stateless-lifecycle
    language). The calendar-replay model doesn't emit at fixed
    weights — copy should mention "advancement at real
    fact_bookings.checkout_date" instead.
**Status:** OPEN / deferred. None of these break anything today —
the producer's deprecation no-op + warning keeps `run.py` working,
and the Phase-7 doc is descriptive not load-bearing. Bundle into
the next docs pass.

---

### B-042 — Monitor: scope quarantine counts to the date filter ✓ DONE
**Origin:** the `/monitor` page's QUARANTINE section showed
bucket-wide MALFORMED / LATE counts that ignored the FROM/TO date
filter that the EVENTS section already used. The two sections
therefore disagreed by axis — EVENTS narrowed to "today", QUARANTINE
showed "everything since project start." Confusing in a UI where
both sit a few centimetres apart.

**What landed:**
- `render/server.py` — `_monitor_quarantine(date_from, date_to)`
  now takes the same date range the EVENTS section uses (validated
  YYYY-MM-DD strings from `_valid_date()` / the default-today rule),
  enumerates each day in `[date_from, date_to]`, and counts objects
  under `{prefix}/year=Y/month=M/day=D/` for each day on both the
  malformed and late prefixes. Sums across days. Inverted ranges
  (date_to < date_from) collapse to 0 by construction (no days
  iterated). When either endpoint is missing, falls back to the
  legacy bucket-wide count — kept as a safety net; the live monitor
  routes always pass dates thanks to the default-today rule.
- Both `/monitor` and `/monitor/data` route handlers pass
  `date_from, date_to` to `_monitor_quarantine`. Same date inputs as
  the EVENTS section → the two sections stay consistent on initial
  render AND on the 10s `/monitor/data` polling refresh.
- **City filter NOT applied.** Quarantine S3 keys (per
  `stream_consumer.py:quarantine_event`) carry NO city segment —
  malformed events often can't be parsed for a city, which is by
  design. The function refuses to accept a city argument; passing
  one would have no axis to filter on.
- `render/templates/monitor.html` — QUARANTINE section copy replaced:
  - The old "Counts are bucket-wide (not filtered)" caption swapped
    for the brief's prescribed copy ("Scoped to the selected date
    range. Quarantined objects aren't tagged by city, so the city
    filter doesn't narrow these — they're counted across all cities.").
  - New `badge-scope` style + a "date-scoped · all cities" chip on
    the section head, in the same visual register as the existing
    SOON badges. Tells the user the scoping rule at a glance.

**Acceptance (4/4 PASS — counts match `mc ls --recursive ... | wc -l`):**
- A. Today filter (2026-05-26, no consumer ran): malformed **0** /
  late **0** — matches `mc ls` (empty day-prefix).
- A'. Single day 2026-05-25: malformed **678** / late **279** —
  matches `mc ls`.
- B. Three-day range 2026-05-23..05-25: malformed **3,110** / late
  **1,008** = exact per-day sum (1,760+672+678 / 528+201+279).
- C. City toggle (no-city / Goa / Mumbai) with the same date range:
  quarantine **3,110 / 1,008** stable across all three, while stream
  bookings shifted **23,370 → 814 → 1,271** (as expected — same
  axis-of-filter test, opposite direction).
- D. No regression: /monitor renders HTTP 200 / 24 KB, full
  `/monitor/data` JSON payload intact (live + stream + quarantine
  all populated), heartbeat / windows / per-type counts / cities all
  read normally.

**Files touched:** `render/server.py` (additive on
`_monitor_quarantine`; 2-line callsite updates on the two routes),
`render/templates/monitor.html` (caption swap + small chip +
`.badge-scope` CSS). No DB / migration / wire-shape changes.

**Forward link:** B-033 (`quarantine_daily_rollup` DAG) is the
production-grade backfill that turns this O(days × 2) list path
into an O(1) Postgres lookup. B-042 is the right-sized stand-in
until then; the day-prefix listing is cheap enough that even a
30-day range is well under the page-render budget.

---

### B-040 — Gold layer: per-booking lifecycle reconstruction + transition flags ✓ DONE
**Status:** Done — see full entry above and [Completed](#completed) table.

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

### B-034 — Stateful booking-lifecycle producer (simulator rewrite, step 1 of N) ⬜ STEP-1 SHIPPED
**Priority:** Medium — unblocks the realistic-lifecycle-vs-stateless-mix gap that B-030 (REVIEW) and any future per-booking analytics depend on
**Background:** The previous `scripts/kafka_event_producer.py` drew every event type stateless per tick from a fixed weight vector (`BOOKING / CHECKIN / CHECKOUT / CANCELLATION / PRICE_CHANGE = 0.55 / 0.18 / 0.12 / 0.10 / 0.05`). The five wire-type strings looked right at the aggregate level, but no CHECKIN was tied to any actual BOOKING — every event was an independent random draw. So:
  - per-booking joins (CHECKIN → which BOOKING?) were impossible end-to-end,
  - cancellation_reason was uncomputable (no booking state to inspect),
  - the consumer's per-window counts were the only thing that worked, and they only worked because the consumer doesn't validate cross-event references.

**What step 1 did (this item):** rewrote the producer as a stateful open-bookings registry:
  - Loads `data/dim_customer.csv` at startup → 20,000 real `customer_id`s, sampled WITH replacement (repeat customers are realistic).
  - Replaces the per-tick type draw with: with `P_PRICE` (0.05) emit a stateless PRICE_CHANGE; else with `P_START` (0.50) START a booking (BOOKING + add to registry); else ADVANCE a random open booking one step (BOOKED→CHECKIN; BOOKED→CANCELLATION with `P_CANCEL_FROM_BOOKED`=0.12 and the reason chosen probabilistically `customer_cancelled` 0.70 / `no_show` 0.30; CHECKED_IN→CHECKOUT). Registry is capped at 5000 to bound memory; once full, ticks are forced to ADVANCE until eviction frees a slot.
  - Wire `event_type` strings UNCHANGED. Base envelope unchanged. The consumer needs no changes.
  - Field contract enriched (BOOKING adds `booking_id, customer_id, room_type_id, checkin_date, checkout_date, nights, num_guests, nightly_rate_inr, payment_mode`; CHECKIN/CHECKOUT/CANCELLATION carry `booking_id, customer_id` plus `cancellation_reason` for the latter; PRICE_CHANGE has neither booking_id nor customer_id — it's a (hotel, room_type) operational event).
  - **Invariant asserted in code:** `revenue_inr == nightly_rate_inr * nights` on every BOOKING.
  - All five CLI flags now parse — `--rate / --duration / --malformed-pct / --late-pct / --chaos-seed` — with the `CHAOS_*` env vars as fallback defaults. Chaos generators / late-shifter / dispatcher / `value_serializer` bytes-passthrough / `key=city` partition / `acks=all` / `linger_ms=20` all preserved verbatim.

**Lifecycle physics consequence — the wire-type mix shifts (this is fundamental, not a bug):**
The old stateless 55/18/12/10/5 mix CANNOT be reproduced by a full lifecycle — each BOOKING begets ~1.88 follow-up events. With the tunables above, the new steady-state mix is approximately:

| Wire type | Old stateless | New lifecycle target | Tolerance band |
|---|---:|---:|---|
| BOOKING | 0.55 | ~0.33 | ±5pp |
| CHECKIN | 0.18 | ~0.29 | ±5pp |
| CHECKOUT | 0.12 | ~0.29 | ±5pp |
| CANCELLATION | 0.10 | ~0.04 | ±5pp |
| PRICE_CHANGE | 0.05 | 0.05 | ±5pp |

The old design weights stay documented in `docs/phase-2-streaming.md` as historical context; the new lifecycle targets above are what live mix should land near on a fixed-seed run.

**Deliberately deferred to a future step (NOT in B-034):**
- **REVIEW emission is NOT implemented this step.** The consumer's Gate 2 allow-list (`VALID_EVENT_TYPES`) does NOT include REVIEW; emitting REVIEW now would quarantine every one as `unknown_event_type`. Next step: teach the consumer to accept + count REVIEW (extend `VALID_EVENT_TYPES`, route to the existing `total_reviews` column on `agg_hourly_city_stats`, optional embedding-at-ingest). Once that's verified, this producer flips REVIEW on at the right point in the lifecycle (post-CHECKOUT, with sampled real Kaggle text per B-030's notes). Both halves together resolve B-030.
- Optional later: `cancellation_reason` and `payment_mode` could land as new columns on `agg_hourly_city_stats` / a per-booking gold table, but that's its own item (no migration written this step).

**Files touched:**
- `scripts/kafka_event_producer.py` — rewritten as the lifecycle simulator (was on the post-B-032 unfrozen list)
- `data/dim_customer.csv` — NEW producer input (read at startup; not modified)
- Docs: `datamodel.md`, `docs/phase-2-streaming.md`, `docs/session-notes.md`, CLAUDE.md, this file

**Acceptance (step 1, on a fixed `--chaos-seed 42`, 180s @ 50 evt/s):**
- Parse-check passes (with `PYTHONUTF8=1` on Windows — see session-notes for the cp1252 gotcha).
- All five CLI flags parse and behave; env-var fallbacks still work.
- Every emitted `customer_id` matches `CUST-NNNNNN` and is in `data/dim_customer.csv`.
- Every CHECKIN / CHECKOUT / CANCELLATION carries a `booking_id` that traces to a prior BOOKING in the captured stream; PRICE_CHANGE carries neither `booking_id` nor `customer_id`.
- Revenue invariant holds on every BOOKING.
- Wire-type mix within ±5pp of the lifecycle targets above.
- Registry stays bounded (max ≤ 5000) over the run.
- Consumer (unchanged) still aggregates: `agg_hourly_city_stats` populates `total_bookings / total_checkins / total_checkouts / total_cancellations` on new windows.
- Quarantine sinks pick up chaos as before (`malformed_events/`, `late_events/`); no new quarantine on clean runs (the enriched fields are additive — consumer ignores unknown extras).

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
| ✓ | B-035 — Lifecycle history backfill, time-partitioned real model (step 2 of B-034). Migration 008 (`fact_booking_events` silver ledger + `sim_open_bookings` simulator state) + new `scripts/generate_lifecycle_history.py`. One sweep over all of `fact_bookings`; routes each row vs `--sim-today` (default 2025-06-01) into COMPLETED / IN_PROGRESS / BOOKED / FUTURE buckets. Backlog is real (every `sim_open_bookings.booking_id` is a real fact_bookings row); ~388K FUTURE bookings reserved for the stream simulator to replay. 15 / 15 acceptance (keys, revenue invariant, realism, continuity, BACKLOG REAL, docs, idempotency). | Phase 2 / Phase 6 |
| ✓ | B-042 — Monitor: scope quarantine MALFORMED + LATE counts to the FROM/TO date filter. `render/server.py:_monitor_quarantine(date_from, date_to)` now enumerates per-day prefixes under `{malformed,late}_events/year=Y/month=M/day=D/` for each day in the range and sums KeyCount; city stays N/A (S3 keys carry no city, by design). `/monitor` template gets the brief's date-scoped + city-agnostic copy and a small "date-scoped · all cities" chip on the section head. 4/4 PASS — counts match `mc ls --recursive | wc -l` exactly (0/0 today, 678/279 for 2026-05-25, 3,110/1,008 for the 3-day range 2026-05-23..25); city toggle leaves quarantine unchanged across no-city / Goa / Mumbai while EVENTS shifts as expected. Right-sized stand-in until B-033's quarantine_daily_rollup DAG turns this O(days) list path into an O(1) Postgres lookup. | Phase 7 |
| ✓ | B-039 — Silver sink: inline per-event ledger in `scripts/stream_consumer.py`. New `silver_flush()` writes every accepted event to `fact_booking_events` with `source='stream'` via `execute_values` + `ON CONFLICT (event_id) DO NOTHING`. Captures all 5 wire types (incl. PRICE_CHANGE with NULL booking_id/customer_id) — placed BEFORE Gate 3's accumulator filter, with parse-ts + Gate 4 (late) moved above Gate 3 to keep the late-quarantine path intact. `page_size=len(rows)` on `execute_values` so `cur.rowcount` reports the full batch (a mid-acceptance fix; default page_size=100 silently undercut new-inserts on every batch > 100 rows). Acceptance 7/7 PASS — counts reconcile (silver inserted 4,272 EXACT = events 4,582 − malformed 221 − late 89 on the chaos slice; 2,380 EXACT on the clean slice; cumulative 9,075 across the session, type-split matches producer); dedup test (re-insert 100 event_ids) → 0 inserts, table count stable; linkage (3,845 stream BOOKINGs) → 0 orphan FK targets; isolation force-test (bad pw + missing table) → silver_failures bumps, no exception. Foundation for B-040 (gold lifecycle reconstruction). | Phase 2 |
| ✓ | B-038 — Bronze sink: durable raw-event archive in `scripts/stream_consumer.py`. New `bronze_flush()` batches every accepted event (post-Gate-4) to S3 under `raw_events/year=YYYY/month=MM/day=DD/hour=HH/HHMMSS_<uuid8>.jsonl` (JSONL, ingest-time partitioned, many events per file). Flush triggers: `BRONZE_BUFFER_CAP` (default 500) OR `FLUSH_CHECK_SECONDS` tick OR graceful shutdown. Wrapped in its own try/except; failures log + increment `bronze_failures` + drop the batch — never crash the consumer, never block agg or heartbeat. Acceptance 7/7 PASS — counts reconcile (bronze 11,912 = accepted events across both test runs; per-type agg sums match bronze exactly); chaos test: 0 quarantined events in `raw_events/`; isolation test (bogus bucket): 6 bronze flushes failed, all 44 Postgres agg upserts still succeeded + 6 heartbeats wrote + consumer exited cleanly. Foundation for B-039 (silver) and B-040 (gold). | Phase 2 |
| ✓ | B-034A — Calendar simulator Phase A (replay engine). `scripts/kafka_event_producer.py` rewritten as a calendar-driven REPLAY of `fact_bookings WHERE booking_ts >= --sim-start` + advancement of the real `sim_open_bookings` backlog. Sim-clock in `scripts/.sim_clock.json`; resume = saved + 1. New wire field `event_date` (sim-day) alongside wall-clock `event_ts`. Same Kafka config / partition key / wire types as before. Cancellation timing + reasons keyed deterministically off `booking_id + chaos-seed`. 21 / 21 acceptance — 0 quarantine on clean run, exact agg-vs-producer sum match, 0 dangling booking_ids, 0 is_cancelled bookings checked in, restart re-emits 0 events, chaos reproducible. Phases B (B-036 net-new + long-stay) and C (B-037 REVIEW) explicitly deferred. | Phase 2 / Phase 6 |
| ✓ | B-040 — Gold lifecycle layer. Migration 009 (`ingest_seq` cursor column on `fact_booking_events` + `fact_booking_lifecycle` one-row-per-booking gold table + `gold_watermark` single-row cursor). `scripts/gold_lifecycle_updater.py` decoupled ~1-min micro-batch: reads silver `WHERE ingest_seq > watermark`, groups by booking_id, intra-batch lifecycle sort, forward-only status machine, business-timestamp-based `illegal_transition_flag` (NOT processing-order). 7/7 acceptance: 624,388 gold rows = 624,388 distinct silver booking_ids; illegal_flag=0 across all 606,463 history + 5,917 mixed + 12,008 stream rows; outcome completed 85.4%/cancelled 8.0%/no_show 3.4%/in_progress 3.2%; watermark==max_seq 1,763,493; 0 status inconsistencies; idempotent. | Phase 2 / Phase 6 |
