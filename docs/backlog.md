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
- ~~B-030 — REVIEW as a stream event~~ ✓ Done
- B-031 — consumer resilience fix (L-016; fix shipped, pending live verification)
- ~~B-033 — `quarantine_daily_rollup` DAG (Phase 6; self-healing daily summary)~~ ✓ Done (superseded by B-044)
- ~~B-044 — hourly quarantine rollup (run.py proc; supersedes B-033 daily DAG + migration 012)~~ ✓ Done
- B-034 — stateful lifecycle simulator — step 1 shipped, superseded by B-035 → B-034A → B-047 (kept for context)
- ~~B-036 — calendar simulator Phase B (net-new synthetic + 10–25% long-stay tail)~~ ✓ Superseded by B-047 (forward generator obviates Phase B + C entirely)
- ~~B-037 — calendar simulator Phase C (REVIEW emission)~~ ✓ Absorbed into B-030; remaining producer scope obviated by B-047
- ~~B-040 — gold layer (per-booking lifecycle reconstruction + transition flags)~~ ✓ Done
- ~~B-041 — producer rate-flag migration (run.py `--rate` no-op cleanup)~~ ✓ Closed by B-047 (deprecated flags deleted entirely; single throughput knob `--rate-multiplier`)
- B-043 — gold `illegal_transition_flag` false positives: batch-local state machine + unconditional UPSERT overwrite (~5,151 rows affected)
- B-045 — `run.py` startup takeover (newest wins): kills live supervisor + all children, polls advisory locks free, then starts fresh; foreign-`:5000` guard (built, pending owner verification + commit)
- ~~B-046 — Stage 1 dimension expansion (additive: +949 cities → 993, +18,076 hotels → 20,076, +49,904 room types → 55,446, +80,000 customers → 100,000; all fact tables untouched)~~ ✓ Done
- ~~B-047 — Stage 2a forward generator (data-aware diurnal-timed producer; migrations 014 + 015; `scripts/chaos_injector.py` extracted; single `--rate-multiplier` knob; supersedes B-034A and B-036; closes B-041)~~ ✓ Done

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
holds B-001..B-004, B-007..B-012, B-018, B-019, B-027, B-029, B-030,
B-030b (continuous embedder + monitor tiles + review/embed date-scoping +
freshness signal fix), B-032, B-033 (superseded by B-044), B-035,
B-034A (calendar simulator Phase A — *superseded by B-047*),
B-036 (Phase B *— superseded by B-047*),
B-037 (Phase C *— absorbed into B-030 + obviated by B-047*),
B-038 (bronze sink), B-039 (silver sink),
B-040 (gold lifecycle layer),
B-041 (rate-flag cleanup *— closed by B-047*),
B-042 (monitor quarantine date scoping),
B-044 (hourly quarantine rollup — supersedes B-033),
B-046 (Stage 1 dimension expansion — additive 1K cities / 20K hotels / 100K customers),
B-047 (Stage 2a forward generator — data-aware diurnal-timed producer; supersedes B-034A/B-036; closes B-041),
plus the unnumbered Phase 1–5 foundation items.

### ABANDONED

None to date. (When an item is abandoned, strike through its header and add an
"Abandoned: <reason>" line — keep the body for context per the "never delete"
rule.)

---

## Phase 1 / Foundation — Dimensional scale-out

### B-046 — Stage 1 dimension expansion (1K cities / 20K hotels / 100K customers, ADDITIVE) ✓ Done

> **Trace.** Phase(s): [Phase 1](phase-1-postgres.md#b-046--stage-1-dimension-expansion-2026-05-27) · datamodel: `dim_location`, `hotel_master`, `dim_room_type`, `dim_customer` + [Regenerating the Dataset → Stage B](../datamodel.md#stage-b--additive-expansion-b-046-run-once-after-stage-a) + [Stage 1 Dimension Expansion section](../datamodel.md#stage-1-dimension-expansion-b-046) · data: `scripts/expand_dimensions.py`, `scripts/build_cities_expansion_csv.py`, `seeds/cities_expansion.csv`.

**Goal:** scale the world from 44 cities / 2,000 hotels / 5,542 room types / 20,000 customers up to ~1K / ~20K / ~55K / 100K without touching any existing row and without inserting a single fact/booking/review row. Pure dimensional capacity expansion to unblock larger scenarios for downstream stages (Stage 2 fact backfill for the new hotels' Feb–May 2026 window will be a follow-up item; out of scope here).

**Decisions locked at planning time (from owner):**

1. `travel_purpose` vocabulary stays at the existing 9 values. No `Backwater` added — Backwater-zone customers map to `Wellness`.
2. Hotel count target 20,000 ±5% with bucket-uniform sampling. Actual delta 18,076 → 20,076 (0.38% over).
3. `opened_year` extended to 2026. Migration 006 carries no CHECK constraint — verified by `pg_get_constraintdef` over `hotel_master`; no migration needed.
4. `tourist_arrivals_annual_m` capped at 24.0. It's a hotel-demand proxy for city-popularity weights, not literal Ministry-of-Tourism footfall. Documented in `datamodel.md` under the `dim_location.csv` section.
5. `seeds/cities_expansion.csv` is the committed source of truth for the 949 net-new cities (committed path, NOT under gitignored `data/`).

**What shipped:**

- `seeds/cities_expansion.csv` — 949 curated rows (city, state, region, tourism_zone, latitude, longitude, tourist_arrivals_annual_m, peak_months, popularity_tier). State coverage = 34 (of 37 valid `india_states_zones` entries; the catalog has nothing in Chandigarh-UT or Dadra & Nagar Haveli, which were also absent from the pre-existing 44).
- `scripts/build_cities_expansion_csv.py` — deterministic CSV builder (SEED=42; catalog inline). Regenerates the CSV byte-identical. Validates state names against `STATE_REGION`, asserts no within-catalog duplicates, fails loudly on either.
- `scripts/expand_dimensions.py` — single-transaction INSERT for all four tables. Pre-flight guards: (a) every catalog state must exist in `india_states_zones`; (b) no catalog (city, state) may already exist in `dim_location` (idempotency); (c) `MAX(hotel_id)` must equal `HTL-002000`; (d) `MAX(customer_id)` must equal `CUST-020000`. Post-insert verification (still inside the transaction, before commit): fact row counts unchanged; FK orphan count = 0; every new hotel has ≥1 room type; no state drift. Any failure → ROLLBACK.

**Verification — counts (run 2026-05-27):**

| Table | Before | After | Delta |
|---|---:|---:|---:|
| `dim_location` | 44 | 993 | +949 |
| `hotel_master` | 2,000 | 20,076 | +18,076 |
| `dim_room_type` | 5,542 | 55,446 | +49,904 |
| `dim_customer` | 20,000 | 100,000 | +80,000 |
| `fact_bookings` | 1,000,000 | 1,000,000 | **0** |
| `fact_booking_events` | 2,107,759 | 2,107,759 | **0** |
| `fact_booking_lifecycle` | 762,230 | 762,230 | **0** |
| `reviews_raw` | 133,463 | 133,463 | **0** |

**Verification — integrity:**

- 0 FK orphans on hotel→location, hotel→price_tier, room_type→hotel, room_type→price_tier.
- 100% of hotels have ≥1 room type.
- 0 dim_location rows have a `state` not in `india_states_zones`.
- Zone gating intact: Houseboat 175 / 175 in Backwater, Treehouse 186 / 186 in Wildlife. Tent appears 409× in Wildlife and 266× in Hill Station (and nowhere else).
- 3 new `dim_room_type.type_name` values added: `Houseboat Suite`, `Tent`, `Treehouse` (16 distinct names total). Free-text column, no migration needed.

**Spot-check — 10 random new cities have hotels:** Auli (21), Ayodhya (148), Gangtok (70), Hampi (45), Kumarakom (60), Leh (40), Madurai (123), Pondicherry (45), Tawang (35), Tirupati (145). All non-zero, ranges plausible for the popularity_tier of each.

**Why no migration:** Every column the expansion writes is already defined in the live schema (base `db/schema.sql` + migration 006 for `opened_year`). The three new room-type values and the previously-near-empty property-types (`Houseboat`, `Treehouse`, `Tent`) are all free-text VARCHAR with no CHECK constraint. The `opened_year` upper bound is data-shape only, not schema, and migration 006 has no CHECK clause (verified).

**Idempotency:** A second run of `python -m scripts.expand_dimensions` aborts at the pre-flight (catalog cities already present, hotel max past `HTL-002000`, customer max past `CUST-020000`) with a clear stderr message and a ROLLBACK. Confirmed by inspection — no second-run was executed, but the four guard conditions are explicit and tested at the bash level (max-id mismatch raises `SystemExit` with the actual seen value).

**Out of scope (future items, not this one):** Stage 2 fact backfill for the new ~18K hotels' Feb–May 2026 window; stream-simulator gating to avoid replaying bookings that don't exist yet; lifecycle reconstruction for the new hotels. The user flagged these in the audit; they each need an independent design pass.

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

### B-033 — quarantine_daily_rollup DAG (self-healing daily summary) ✓ Done — superseded by B-044

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

### B-045 — `run.py` startup takeover (newest wins — kills prior run.py + children)

> **Trace.** Phase(s): ops/dev (no phase doc — `run.py` is documented in [CLAUDE.md → Repo layout](../CLAUDE.md#repo-layout) `run.py` line and the "Multiple run.py instances competing" / "Stale advisory lock" Common Mistakes rows) · datamodel: none · data: none.

**Priority:** Medium — built; pending owner verification + commit  
**Problem:** When `run.py` was hard-killed (power cycle, force-kill), stale orphan processes from the prior session kept holding `:5000` or the Postgres advisory locks. The next `run.py` backed off, leaving the operator with a stale dashboard. Separately, when a developer wants to restart the stack from a new terminal, `python run.py` should simply take over without requiring a manual Ctrl-C first.

**What was built (`run.py`):**
- **`_startup_cleanup()` — first action in `main()`:** runs BEFORE port 47219 is acquired. Finds the live run.py supervisor by who holds port 47219 via `_find_pid_on_port(47219)`, kills it with `taskkill /F /T` (entire process tree), then kills any remaining travellens children (consumer / simulator / dashboard / gold updater / embedder / quarantine rollup) by cmdline pattern sweep. Logs `"[system] stopping <label> (PID N)"` per process.
- **`_wait_advisory_locks_released()`** — called by `_startup_cleanup()` after killing. Polls `pg_locks WHERE locktype='advisory' AND classid=0 AND objid IN (7400030,7400040,7400050,7400060)` every 0.5s (up to 30s) until all 4 locks are free. Falls back to a 3s fixed sleep if Postgres is not reachable (e.g. `--server-only` or docker not up). Logs `"advisory locks 7400030/40/50/60 released"` when done.
- **`_acquire_run_lock()` — comes AFTER cleanup:** `socket.bind("127.0.0.1", 47219)`. With the prior supervisor dead, this binds cleanly. If two `python run.py` start at nearly the same moment, `socket.bind()` is atomic — exactly one wins; the other prints `"port 47219 still held after cleanup — another run.py may have started simultaneously; try again."` and exits 1.
- **`_pid_cmdline_map()`** — one PowerShell WMI call returns `{pid: cmdline}` for all running processes.
- **`_find_pid_on_port(port)`** — parses `netstat -ano` with needle `f":{port} "` (trailing space prevents `:50000` false-matching `:5000`).
- **`_kill_pid_windows(pid, label)`** — `taskkill /F /T` kills the full process tree; logs `"[system] stopping <label> (PID N)"`.
- **Sweep** matches command lines against `_TRAVELLENS_CMD_PATTERNS` (`scripts.stream_consumer`, `scripts.kafka_event_producer`, `scripts.gold_lifecycle_updater`, `scripts.review_embedder`, `scripts.quarantine_hourly_rollup`, `render.server`) to catch any children that survived the supervisor tree kill.
- **Foreign-process guard:** if `:5000` is held by a PID whose command line does NOT match any travellens pattern (and WMI data is available so we can be certain), `_startup_cleanup` prints `":5000 is held by PID N ... which is NOT a travellens process — stop that process manually"` and calls `sys.exit(1)`. Never blind-kills an unrelated process.
- Verifies `:5000` is free after cleanup; warns (does not stop) if still held.
- **Silent on clean start** — nothing to kill → returns immediately with no log output.
- **Removed:** the old passive `port_in_use(DASHBOARD["port"])` guard in `Launcher.start_python()` that printed "NOT starting a second dashboard". Dashboard is now always started; `_startup_cleanup()` guarantees `:5000` is free before launch.

**File:** `run.py` (repo root).  
**Acceptance — owner runs 5 steps:**
1. **Takeover (critical):** Terminal 1: `python run.py` (let all 6 procs come up). Terminal 2: `python run.py` → reports killing T1's supervisor + children, waits for locks, then starts its OWN 6 procs — all alive, none logged a lock-acquire failure or early exit. T1's procs are gone.
2. Leave a stale dashboard on `:5000` → start `run.py` → it reports killing the leftover, frees `:5000`, serves a FRESH dashboard.
3. Ctrl+C → restart `run.py` → clean, zero manual killing required.
4. Bind a non-travellens process to `:5000` (e.g. `python -c "import socket; s=socket.socket(); s.bind(('',5000)); input()"`) → start `run.py` → clear error message, exits 1, foreign process untouched.
5. Cold start (no orphans) → run.py starts silently (no cleanup noise), all 6 procs launch normally.

---

## Phase 2 Hardening — Consumer (scoped frozen-file exception)

> `scripts/stream_consumer.py` is on the FROZEN list. This is a deliberate,
> logged exception taken to resolve L-016 (consumer never flushes under load).
> Defaults are preserved; production behaviour is unchanged unless env vars are set.
> **TODO after verification:** note this exception in CLAUDE.md's frozen-file list,
> and re-run the Phase-2 acceptance suite to confirm no regression.

### B-047 — Stage 2a forward generator (data-aware diurnal-timed producer) ✓ DONE

> **Trace.** Phase(s): [Phase 2](phase-2-streaming.md#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--current-producer) · datamodel: [`sim_open_bookings` (migration 015 fire-times + `'REVIEW_PENDING'` state)](../datamodel.md#sim_open_bookings), [`sim_daily_counter` (migration 014, cap-upsert migration 016)](../datamodel.md#sim_daily_counter), [Schema Evolution → 014/015/016](../datamodel.md#schema-evolution) · data: `scripts/kafka_event_producer.py` (full rewrite), `scripts/chaos_injector.py` (new — extracted from prior producer), `db/migrations/014_sim_daily_counter.sql`, `db/migrations/015_lifecycle_fire_times.sql`, `db/migrations/016_sim_daily_counter_cap_upsert.sql`.

**Goal:** replace the calendar-replay producer (B-034A) — which has no future runway after `fact_bookings.booking_ts` exhausts ~2027-12 and cannot reproduce "events happen at realistic times of day" — with a data-aware FORWARD generator that mints net-new bookings under a diurnal IST rate curve, advances `sim_open_bookings` through CHECKIN / CHECKOUT / CANCELLATION / REVIEW at per-event-type natural fire-times, and never runs dry.

**Decisions locked at planning time (from owner):**

1. **Wall-clock event_ts, real-today event_date.** No sim-clock, no `.sim_clock.json`. Catch-up rule: overdue fire-times drain at the bucket cap with `event_ts = NOW()` — never backdated.
2. **`--rate-multiplier` is integer-only, strictly > 1; silent fallback to 1** on any invalid input (non-int, ≤ 1, negative, alphanumeric, empty, None). No argparse error, no warning. Same coerce on `RATE_MULTIPLIER` env var. Folds in B-041 (the prior `--sim-rate` / `--rate` / `--duration` / `--sim-speed` no-op flags are REMOVED entirely, not retained as accepted-and-ignored).
3. **`daily_cap = 1,000,000 × rate_multiplier`** (both scale together — otherwise high-x runs burn through cap mid-day and go silent).
4. **Drop the 95/5 BOOKING/PRICE_CHANGE coin-flip.** The per-hour picker math `prob_b(h) = w_b[h] / (w_b[h] + w_p[h])` shapes the mix naturally; the resulting ~48/52 daily aggregate is the design target.
5. **Per-tick batched commits** (~5 commits/sec at x=1) — not per-event.

**Diurnal rate curve (IST, multiplier on `BASE_RATE = 10 evt/s`):**

```
 h  ×       h  ×       h  ×       h  ×
00 0.30    06 0.55    12 1.15    18 1.85
01 0.20    07 0.75    13 1.20    19 2.05  ← peak
02 0.20    08 1.00    14 1.15    20 2.00
03 0.20    09 1.15    15 1.25    21 1.65
04 0.25    10 1.10    16 1.40    22 1.15
05 0.40    11 1.05    17 1.55    23 0.55

Σ = 24.10  →  24-h mean ≈ 1.004×
daily integral at x=1: 10 × 24.10 × 3600 ≈ 868K events  (vs 1M cap → 13% headroom)
peak rate at x=1: 20.5 evt/s @ 19 IST
peak rate at x=5: ~102 evt/s — near consumer's tested ~50 evt/s ceiling (no warn — owner wanted silent)
```

**Per-event-type IST hour distributions (sum = 1.0 each):**

| Type | Window | Weights (peak in **bold**) |
|---|---|---|
| CHECKOUT | 08–13 | 08:0.15 · **09:0.25 · 10:0.25** · 11:0.20 · 12:0.10 · 13:0.05 |
| CHECKIN | 12–21 | 12:0.05 · 13:0.05 · 14:0.08 · 15:0.12 · **16:0.15 · 17:0.15 · 18:0.15** · 19:0.12 · 20:0.08 · 21:0.05 |
| CANCELLATION | 09–21 evening-lean | 09:0.05 · 10:0.05 · 11:0.05 · 12:0.07 · 13:0.07 · 14:0.07 · 15:0.07 · 16:0.08 · 17:0.10 · **18:0.12 · 19:0.12** · 20:0.10 · 21:0.05 |
| REVIEW *(deferred from CHECKOUT/CANCELLATION emit, same calendar day)* | 20–23 | 20:0.30 · **21:0.35** · 22:0.25 · 23:0.10 |
| BOOKING *(picker weight vs PRICE_CHANGE)* | spread, peak 18-22 | 00:0.04 · 01-04:0.02-0.04 · ramp to **19:2.80** · 20:2.50 · ramp down |
| PRICE_CHANGE *(picker weight)* | follows rate curve | ≡ `DIURNAL_HOUR_MULT[h]` |

**What shipped:**

- `scripts/kafka_event_producer.py` — **full rewrite**, 1,191 lines. Token-bucket loop at `effective_rate(h_IST) × rate_multiplier`. Each tick: drains due lifecycle (priority) → fills with BOOKING/PRICE_CHANGE by hour-weighted picker. Data-aware booking pipeline: city weighted by `popularity × hotel_count × season/holiday`, hotel weighted by `total_rooms × star_category` and subject to per-hotel-per-night occupancy cap from overlapping `sim_open_bookings`, room type fitting `num_guests`, customer 75% out-of-state (vs `home_state`), lead-time 50% ≤7d / 35% 1-8wk / 15% 2-8mo, nights from empirical `fact_bookings.nights_stayed`, price `base × {weekend 1.15, holiday 1.25, monsoon 0.85, winter 1.10} × Gaussian(1.0, 0.05)`, revenue = nightly × nights (asserted invariant), `booking_source` from empirical mix (MakeMyTrip 26%, Direct 22%, OYO 15%, Booking.com 13%, Goibibo 12%, Walk-in 6%, Agoda 6%). On emit, stamps `checkin_fire_ts` + `checkout_fire_ts` from per-type distributions and (12% deterministic, keyed `Random(booking_id|cancel|daily_seed)`) `cancel_fire_ts`. On CHECKOUT or CANCELLATION emit, if the negativity-bias draw produces a review, sets `review_fire_ts` to tonight 20-23 IST (clamped ≥ now+30min) and transitions the row to `state='REVIEW_PENDING'`. Daily counter UPSERT in same per-tick commit batch. On cap-hit sleeps until IST midnight.
- `scripts/chaos_injector.py` — **new module**, 118 lines, extracted verbatim from the prior producer's chaos block. Public surface = `maybe_corrupt_or_delay(event, malformed_pct, late_pct) → (payload, mode)`. Reason vocabulary unchanged (`missing_field`, `unknown_event_type`, `unknown_city`, `unparseable_event_ts`, `unparseable_json`, `late`). **Chaos defaults: 1.2 % malformed + 0.8 % late** (locked plan rates; producer's argparse defaults to these via `CHAOS_MALFORMED_PCT="1.2"` / `CHAOS_LATE_PCT="0.8"` env-var fallbacks). `run.py --chaos` still overrides to the 5/2 stress profile; setting `CHAOS_*_PCT=0` explicitly disables.
- `db/migrations/014_sim_daily_counter.sql` — `sim_daily_counter(counter_date DATE PK, events_emitted INT, cap INT, updated_at TIMESTAMPTZ)`. Per-day TOTAL-EVENTS guardrail.
- `db/migrations/015_lifecycle_fire_times.sql` — adds 4 nullable TIMESTAMPTZ cols on `sim_open_bookings` (`checkin_fire_ts`, `checkout_fire_ts`, `cancel_fire_ts`, `review_fire_ts`) + 4 partial indexes (one per fire-time × applicable state); extends the state CHECK constraint to include `'REVIEW_PENDING'`.
- `run.py` — `--sim-rate` flag and `rate_flag = "--rate"` simulator wiring **deleted entirely**. New `--rate-multiplier N` passes through to the simulator. Two stale example lines (`--rate 50 --duration 60`) replaced with `--rate-multiplier 3`.
- `CLAUDE.md` — Phase 2 status line, current-phase summary, hard-rules under-active-hardening line, repo-layout block for `kafka_event_producer.py` + new `chaos_injector.py`, repo-layout migrations directory listing (014 + 015), 4 rows in Common Mistakes (sim_open_bookings extended, calendar-replay row struck out, forward producer row, `sim_daily_counter` row, fire-time-cols row, event_ts/event_date row updated).
- `datamodel.md` — Schema Evolution adds rows 014 + 015; `sim_open_bookings` section grows the 4 fire-time cols + `'REVIEW_PENDING'` state; new `sim_daily_counter` section; sim-clock section marked retired.
- `docs/phase-2-streaming.md` — header backlog-ref list adds B-047; B-034A section noted as superseded; new BUILD HISTORY entry at the end describes the rewrite.

**`--rate-multiplier` coerce-or-default-to-1 verification (22/22 PASS):**

| Input | Result |
|---|---|
| omitted (default) | 1 |
| `2`, `5`, `100`, `99999999` | 2, 5, 100, 99999999 |
| `2.5`, `abc`, ``, `0`, `1`, `-3`, `-2.5`, `0x10`, `None` | 1 (silent) |
| ` 4 ` (leading/trailing space) | 4 (`int(' 4 ')` is fine) |

Env var `RATE_MULTIPLIER` follows the same rule; CLI `--rate-multiplier` takes precedence when explicitly passed.

**Smoke-test verification (consumer + producer at x=1, ~65s wall-clock):**

| Signal | Expected | Actual |
|---|---|---|
| Events consumed | matches silver delta | **298** |
| Late dropped | 0 | **0** |
| Malformed dropped | 0 | **0** |
| Silver inserted | 5 silver-eligible types | **298 rows, 10 flushes, 0 dedup** |
| Bronze archived (excludes PRICE_CHANGE per Gate 3) | 38 | **38 events, 10 files** |
| Reviews inserted | 0 (no CHECKOUT fired yet, lead times 0-7d) | **0** |
| Revenue invariant `revenue = nightly × nights` | 0 broken | **0** broken across 38 BOOKINGs |
| `sim_daily_counter` for IST today | row created + counted | **393 / 1,000,000** |
| Fire-times stamped on `sim_open_bookings` | yes | **all 48 BOOKED rows stamped; 4 with `cancel_fire_ts` (8.3%, P_CANCEL=12% within noise)** |
| CHECKIN fire-hour histogram (IST) | window 12-21, peak 16-18 | **all in 12-21, peak hour 17 (n=14)** |
| CHECKOUT fire-hour histogram (IST) | window 8-13, peak 9-10 | **all in 8-13, peak hour 10 (n=21), hour 9 (n=18)** |
| BOOKING vs PRICE_CHANGE mix at IST ~00-01 | per picker math ~10-13% BOOKING | **38 BOOKING / 260 PC = 12.7%** ✓ matches `w_b/(w_b+w_p) = 0.04/0.34 ≈ 0.118` |
| Sample BOOKINGs use real hotels + cities | hotel_master + dim_location FK | **HTL-014369/Kumarakom, HTL-001693/Mumbai, HTL-018032/Palakkad** |

**Why this supersedes B-034A / B-036 / B-037:**

- **B-034A** (calendar replay) had no future runway after `fact_bookings.booking_ts` exhausts ~2026-05; replay is fundamentally backward-looking. B-047's forward generator has no such horizon — it mints new bookings indefinitely.
- **B-036** (Phase B: net-new synthetic + long-stay tail) — B-047 ALREADY mints net-new bookings, drawing nights from the empirical distribution (1-2 ≈60%, 3-5 ≈30%, 6+ ≈10%). Long-stay tail can be tuned via the `NIGHTS_MIX` constant without a new backlog item.
- **B-037** (Phase C: REVIEW emission) — already absorbed into B-030 on the consumer side; B-047 emits REVIEW on the producer side via deferred `review_fire_ts` scheduled on CHECKOUT/CANCELLATION.

**B-041 closure:** the prior `--sim-rate` / `--rate` / `--duration` / `--sim-speed` no-op deprecation cleanup is done by B-047 — flags are deleted from both `run.py` and the producer, not retained as accepted-and-ignored. Single throughput knob across the system: `--rate-multiplier`.

**Out of scope (future):** Stage 2b — long-stay tail tuning if the empirical mix proves insufficient (no current evidence it does); Stage 2c — hot-reload of dim cache mid-run (cache currently loaded once at producer startup; refresh requires restart, which is fine at single-user scale).

---

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

### B-034A — Calendar simulator, Phase A (replay engine) ✓ DONE — ★ SUPERSEDED BY B-047
**Status update (2026-05-28):** Retired by [B-047 (Stage 2a forward generator)](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done). The calendar-replay model is no longer the producer — `scripts/.sim_clock.json` is deleted on first launch of the forward generator and never re-created. `sim_open_bookings.source='stream'` rows still come from the forward generator; rows with `source='history'` (from B-035) remain valid simulator-state seed. This entry is kept for historical context.

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

### B-036 — Calendar simulator, Phase B (net-new synthetic + long-stay tail) ✓ SUPERSEDED BY B-047
**Status (2026-05-28):** Obviated by [B-047 (Stage 2a forward generator)](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done). The forward generator mints net-new bookings as its core loop (no longer "alongside the replay" — there is no replay). Nights are drawn from an empirical `NIGHTS_MIX` constant in `scripts/kafka_event_producer.py`; tuning the long-stay tail is a one-line constant change, not a new backlog item. Synthetic-vs-historical `booking_id` distinction is moot — every B-047-emitted BOOKING is data-aware-synthetic by construction.

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

### B-037 — Calendar simulator, Phase C (REVIEW emission, resolves B-030) ✓ Absorbed into B-030 + obviated by B-047
**Status (2026-05-28):** Consumer-side REVIEW was already absorbed into B-030. Producer-side REVIEW emission is now done by [B-047](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done): on CHECKOUT or CANCELLATION emit, if `make_review_event_dict` returns a payload, the row's `review_fire_ts` is set to tonight 20-23 IST (clamped ≥ now+30min) and `state → 'REVIEW_PENDING'`; REVIEW fires when wall-clock reaches the stamp.

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

**Acceptance (7/7 PASS):** Clean run: bronze 7,453 = accepted (BOOKING+CHECKIN+CHECKOUT+CANCELLATION); events_consumed 7,468 = bronze + 15 PRICE_CHANGE filtered at Gate 3 — 0 PRICE_CHANGE in raw_events/. Chaos run: 11,912 bronze events across 2 runs, 0 PRICE_CHANGE / malformed / late in raw_events/. Partitioning: ingest-time year/month/day/hour correct. Isolation: 6 bronze failures (bad bucket) → agg upsert + heartbeat unaffected, consumer exited cleanly.

**Files touched:** `scripts/stream_consumer.py` (additive — no
existing sink modified). Docs same turn:
`docs/backlog.md` (this entry; B-039/B-040 reserved),
`CLAUDE.md` (consumer sinks list updated, common-mistakes row),
`datamodel.md` (medallion section — bronze raw_events/ documented),
`docs/phase-2-streaming.md` (Build History / Evolution section).

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

**Acceptance (7/7 PASS):** Clean run: silver inserted = events_consumed = accepted (0 dedup'd, all 5 types including PRICE_CHANGE). Chaos run: 4,272 inserted EXACT = 4,582 consumed − 221 malformed − 89 late; 0 quarantined rows in silver. Dedup: re-insert 100 existing event_ids → rowcount=0, count stable. Linkage: 3,845 BOOKING rows → 0 orphan FKs. Isolation: silver_flush survives bad pw + missing table; agg/bronze/heartbeat unaffected.

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

### B-041 — Stale-references cleanup (producer rate-flag + Phase-7 doc) ✓ Closed by B-047
**Status (2026-05-28):** Closed by [B-047 (Stage 2a forward generator)](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done). Resolution path: the deprecated flags (`--sim-rate` / `--rate` / `--duration` / `--sim-speed` / `--sim-start` / `--until` / `--reset-clock`) are **deleted entirely** from both `run.py` and `scripts/kafka_event_producer.py` — not kept as accepted-and-ignored shims. Single throughput knob across the system is now `--rate-multiplier` (int>1, silent fallback to 1). The Phase-7 doc's "design weight 0.12" CHECKOUT copy is now obsolete in a stronger way — there are no per-tick fixed weights in the forward generator at all; CHECKOUT fires when its stamped `checkout_fire_ts` reaches NOW, drawn from the CHECKOUT IST hour distribution at BOOKING emit time. Update to `docs/phase-7-monitor.md` is folded into the B-047 doc-sync turn.

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

**Superseded by:** B-033 (hybrid Postgres+S3 daily rollup) → then B-044 (pure-Postgres hourly rollup,
`quarantine_hourly_summary`). B-042's S3 day-prefix listing is no longer on the request path.

---

### B-043 — Gold `illegal_transition_flag` false positives (batch-local state machine)
**Priority:** Medium — `illegal_transition_flag` is currently unreliable; do NOT use it for data
quality assertions until this lands.  
**Observed count:** 5,151 rows with `illegal_transition_flag = TRUE` as of the discovery run
(all `last_updated` timestamps between 15:05–18:47 UTC on the discovery day; zero new flags added
after 19:00 UTC, confirming these are a standing pre-existing cohort, not a regression from any
recent change).

**Root cause (two cooperating bugs in `scripts/gold_lifecycle_updater.py`):**

1. **Batch-local state machine** — `gold_lifecycle_updater` reads `fact_booking_events WHERE
   ingest_seq > watermark` for the current batch only. It initialises a fresh in-memory state
   for each `booking_id` in that batch — it never reads the existing row in
   `fact_booking_lifecycle` to discover what `checkin_ts` was already committed in a prior
   batch. Consequence: if a BOOKING + CHECKOUT arrive in the current batch but the history
   CHECKIN (with a lower `ingest_seq`, committed in a prior gold pass) is already in the gold
   table, the machine sees CHECKOUT with no CHECKIN in its local window and sets
   `illegal_transition_flag = True` — a false positive. The data is correct; the machine's
   view is incomplete.

2. **Unconditional UPSERT overwrite** (line ~333 in `gold_lifecycle_updater.py`) — the UPSERT
   uses `COALESCE` for `checkin_ts` (preserves the prior committed value) but writes
   `illegal_transition_flag = EXCLUDED.illegal_transition_flag` unconditionally. So a
   batch-local `True` overwrites the already-correct `False` that the first-pass committed, and
   the false positive is permanently stamped on the row.

**Why cross-batch CHECKOUTs are common:** History events (source='history') have lower
`ingest_seq` values; stream events have higher ones. When the gold micro-batch runs for the
first time after a consumer session, it processes history CHECKIN (low seq, prior watermark
batch) before stream CHECKOUT (high seq, current batch). If both don't fall in the same gold
pass, the cross-batch case arises naturally for every booking whose history CHECKIN was committed
in an earlier gold pass and whose stream CHECKOUT arrives later.

**Fix (do NOT implement yet — this is the planned approach):**
1. In `gold_lifecycle_updater.py`: before evaluating a CHECKOUT event for a booking_id, query
   `fact_booking_lifecycle` for its existing `checkin_ts`. If `checkin_ts IS NOT NULL` in the
   gold table, treat it as "CHECKIN already seen" — do not set `illegal_transition_flag = True`
   for the CHECKOUT.
2. Change the UPSERT to `illegal_transition_flag = CASE WHEN EXCLUDED.illegal_transition_flag
   THEN TRUE ELSE fact_booking_lifecycle.illegal_transition_flag END` (i.e., only upgrade to
   True, never downgrade a stored True to False, and never overwrite a stored False with a
   batch-local False either). A True can only be cleared by the one-time corrective pass below.
3. One-time corrective pass: re-evaluate all 5,151 flagged rows against the full
   `fact_booking_events` history (not just the last batch) and set `illegal_transition_flag =
   FALSE` wherever no genuine business-timestamp inversion exists.

**Acceptance (sketch):**
- After fix + corrective pass: `SELECT COUNT(*) FROM fact_booking_lifecycle WHERE
  illegal_transition_flag = TRUE` returns 0 (or a very small count of genuine inversions).
- Idempotent re-run of the corrective pass changes 0 rows on the second run.
- New stream CHECKOUTs for bookings whose CHECKIN is already in gold do NOT produce new false
  positives.
- No regression: watermark still advances; forward-only status machine still holds; all other
  gold acceptance checks still pass.

**Files:** `scripts/gold_lifecycle_updater.py` only. No migration needed (flag column already
exists; the corrective pass is a plain UPDATE, not a schema change).

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
**Status:** ✓ Resolved — see [Completed](#completed) table.

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

## Known Limitations (L-)

| ID | Description | Introduced | Resolution |
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
| ✓ | B-030 — REVIEW as a booking-tied stream event (absorbs B-037). Migration 010 (`reviews_raw` extended with 7 new columns: `booking_id`, `customer_id`, `review_stage`, `review_channel`, `event_ts`, `event_date`, `record_source NOT NULL DEFAULT 'seed'`; 2 new indexes). Shared generation library `scripts/review_generator.py` (rating anchored on hotel avg_rating + stage adj + Gaussian noise; negativity bias shape _P_REVIEW_BASE={1:0.75,2:0.65,3:0.40,4:0.25,5:0.20} scaled by REVIEW_PROPENSITY_SCALE=0.47 → effective p={1:0.35,2:0.31,3:0.19,4:0.12,5:0.09}; overall rate ~15%; OTA channel constraint; India reason bank + 40/35/25 blend with Kaggle seed texts; deterministic uuid5 review_id). `scripts/generate_review_backfill.py` backfills 90,980 history reviews from 612,380 bookings (14.9%). `scripts/stream_consumer.py` gains REVIEW branch (intercepts after Gate 4, before silver buffer — routes to bronze + reviews_raw, skips silver/agg/gold), `review_flush()` sink, and review metrics in run summary. `scripts/kafka_event_producer.py` emits REVIEW events at CHECKOUT (lifecycle_status=COMPLETED) and CANCELLATION (lifecycle_status=CANCELLED/same-day). `scripts/review_stats.py` — read-only diagnostic: counts by source/stage, overall rate, hotel_id consistency, no_show check, avg rating by star_category gradient. B-030a: REVIEW_PROPENSITY_SCALE tuned to 0.47 for ~15% rate (down from original 31.6%). 6/6 acceptance: A (90,980 history rows, all fields populated) B (0 orphan booking_ids) C (0 wrong OTA channels) D (negativity gradient intact) E (sentiment gradient 0★2.89→5★3.94 monotone) F (30K seed rows unchanged). Diagnostic write-nothing verified (two runs → identical counts). **event_ts fix (B-030a):** `make_review_event_dict` is a pure function — it returns no `event_ts`. Each of the 3 producer emission sites now stamps `_rv["event_ts"] = _now_iso_utc()` immediately after the non-None check; without this, Gate 2's unconditional `event_ts` check quarantined every REVIEW as `missing_field`. Verified: 666 `record_source='stream'` rows, 0 duplicates, 0 `malformed` growth. | Phase 2 / Phase 6 |
| ✓ | B-030b — Continuous review embedder + monitor tile activation + run.py full-stack wiring. New `scripts/review_embedder.py`: continuous micro-batch, `pg_try_advisory_lock(7400050)`, loads `all-MiniLM-L6-v2` once, loops every 120s (`EMBED_INTERVAL_SECONDS`), fetches up to 500 (`EMBED_BATCH_SIZE`) NULL rows per pass, encodes + writes, commits per batch. On first "backlog clear" event logs detailed Part B index-rebuild instructions (lists=120 for ~121K rows). `render/server.py`: `_monitor_embeddings` denominator fixed to `COUNT(*) FILTER (WHERE review_text IS NOT NULL)` so NULL-text rows don't cap coverage below 100%; new `_monitor_reviews(conn)` returns total + seed/history/stream split; `/monitor/data` now includes `reviews` and `embed` keys (previously excluded as "static"). `render/templates/monitor.html`: Review + Embedded SOON tiles replaced with live cards (IDs `ev-reviews`, `ev-reviews-note`, `ev-embedded`, `ev-embedded-note`, `ev-embedded-bar`); coverage progress bar added to Embedded tile; JS poller extended with `updateReviews()` + `updateEmbed()` called on each 10s tick. Sentiment tile stays SOON (B-026). `run.py` wired with 5 managed procs: consumer + simulator + dashboard + gold_lifecycle_updater + review_embedder (both with colour tags; started in all modes except `--server-only`; advisory locks prevent duplicate instances). | Phase 3 / Phase 7 |
| ✓ | B-030b follow-up — Review/Embedded date-scoping + Freshness signal fix. `_monitor_reviews(conn, date_from, date_to)` and `_monitor_embeddings(conn, date_from, date_to)` now filter by `reviews_raw.event_date`; seed reviews (`record_source='seed'`, `NULL event_date`) excluded (static corpus). Return shape change: `reviews` now carries `in_range/history/stream/date_scoped` (not `total/seed/history/stream`). Both `/monitor` and `/monitor/data` routes pass the active date range. Effect: today → only today's reviews, climbs with stream; yesterday → fixed count at ~100% coverage. `badge-scope` chip added to both tile labels. `updateReviews()` JS updated to `in_range`. Stream Freshness tile decoupled: `_monitor_freshness` is now informational context (window age + pill) only; consumer-alive verdict moved to `<span id="freshness-consumer-note">` driven by `live.alive` (heartbeat) on both server render and each 10s tick (new `updateFreshness(data.live)` in the poller). Eliminates the contradiction where "Stale / is the consumer running?" appeared alongside a live events/sec. | Phase 7 |
| ✓ | B-033 — `quarantine_daily_rollup` DAG + hybrid monitor read-path. Migration 012 (`quarantine_daily_summary DATE PK + malformed_count + late_count + computed_at`). New `airflow/dags/quarantine_daily_rollup.py`: self-healing daily DAG (`schedule_interval='0 3 * * *'`, `catchup=False`); per run computes `missing_days = (calendar range from earliest S3 day to yesterday) − (existing summary rows)` and UPSERT-backfills all gaps; explicit 0/0 rows for quarantine-free days; idempotent. `render/server.py`: old `_monitor_quarantine(date_from, date_to)` renamed `_monitor_quarantine_s3_scan` (graceful fallback); new `_monitor_quarantine(conn, date_from, date_to)` hybrid: past days → Postgres `COALESCE(SUM(malformed_count+late_count), 0)` over `quarantine_daily_summary` (O(1) indexed range); today (if in range) → single S3 day-prefix `list_objects_v2` call; migration 012 absent → warning + fallback to S3 scan. Both `/monitor` and `/monitor/data` route handlers: quarantine call moved inside the existing `try` block (reuses open connection). Eliminates the per-request O(days × 2) S3 listing that scaled unboundedly with quarantine growth. **Superseded by B-044** — `airflow/dags/quarantine_daily_rollup.py` retired; `quarantine_daily_summary` dropped by migration 013. | Phase 6 / Phase 7 |
| ✓ | B-044 — Hourly quarantine rollup (supersedes B-033 daily DAG + migration 012). **Trace.** Phase(s): [Phase 6](phase-6-airflow.md#b-044--hourly-quarantine-rollup-supersedes-b-033-dag) + [Phase 7](phase-7-monitor.md#b-044--pure-postgres-quarantine-read-path-hourly-rollup-proc) · datamodel: [Schema Evolution migration 013](../datamodel.md#schema-evolution) (`quarantine_hourly_summary`) · data: none. Migration 013: drops `quarantine_daily_summary`, creates `quarantine_hourly_summary (summary_date DATE, summary_hour SMALLINT, malformed_count INT, late_count INT, is_final BOOL, computed_at TIMESTAMPTZ, PK(summary_date, summary_hour))`. New `scripts/quarantine_hourly_rollup.py` (run.py 6th proc, `pg_try_advisory_lock(7400060)`): 5-min loop; watermark derived from `MAX(is_final=TRUE)` row (no separate cursor table); each cycle walks watermark+1 → current_hour, re-counts S3 objects via `list_objects_v2 KeyCount` (RAM-safe, no key materialisation); `GRACE_MINUTES=10` — hour finalised only once `now >= hour_end + 10min`; explicit 0/0/FINAL rows for empty completed hours; eager first cycle on startup; O(8) API calls to discover earliest hour for backfill. `airflow/dags/quarantine_daily_rollup.py` deleted. `render/server.py`: ALL S3 code removed from quarantine read-path (`boto3`, `botocore.Config`, S3 constants, both old quarantine functions); new `_monitor_quarantine(conn, date_from, date_to)` is pure Postgres — single `COALESCE(SUM(...))` over `quarantine_hourly_summary`. `render/templates/monitor.html`: tile notes updated to "≈ refreshed every 5 min"; error text → "Hourly rollup unavailable." Verification: 159 hours written on initial backfill (158 final, 1 open); hour-level spot check exact match (2026-05-25 h=16: 229 mal / 94 late); 3-day total = 3110/1008 (matches B-042 reference); 7-day range response sub-1ms (pure Postgres); singleton lock correct; grep confirms 0 S3 references in server.py quarantine path. | Phase 6 / Phase 7 |
| ✓ | B-046 — Stage 1 dimension expansion (additive, no migration). `seeds/cities_expansion.csv` (949 curated rows, 34 states) + `scripts/build_cities_expansion_csv.py` (deterministic builder, SEED=42) + `scripts/expand_dimensions.py` (single-transaction idempotent insert). Run 2026-05-27 added 949 cities → 993, 18,076 hotels → 20,076 (within ±5% of 20,000), 49,904 room types → 55,446, 80,000 customers → 100,000. Fact tables (`fact_bookings` 1M, `fact_booking_events` 2.11M, `fact_booking_lifecycle` 762K, `reviews_raw` 133K) unchanged. Zone gating verified — Houseboat 175/175 in Backwater, Treehouse 186/186 in Wildlife, Tent 409 Wildlife + 266 Hill Station. 0 FK orphans, 0 hotels without room types, 0 state drift. 3 new `dim_room_type.type_name` values (`Houseboat Suite`, `Tent`, `Treehouse`). `opened_year` extended to 2026 — no migration (migration 006 carries no CHECK constraint). Travel-purpose vocabulary unchanged (Backwater customers → Wellness per decision). Pre-flight aborts a second run by detecting catalog collision in `dim_location` and ID guards on hotel_master / dim_customer max IDs. | Phase 1 |
| ✓ | B-047 — Stage 2a forward generator. **Trace.** Phase(s): [Phase 2](phase-2-streaming.md#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--current-producer) · datamodel: [`sim_open_bookings` (migration 015 fire-times + `'REVIEW_PENDING'` state)](../datamodel.md#sim_open_bookings), [`sim_daily_counter` (migration 014)](../datamodel.md#sim_daily_counter), [Schema Evolution → 014/015](../datamodel.md#schema-evolution) · data: `scripts/kafka_event_producer.py` (full rewrite, 1,191 lines), `scripts/chaos_injector.py` (new — extracted), migrations 014 + 015. Replaces the calendar-replay producer (B-034A): no sim-clock, `.sim_clock.json` deleted on first launch. Wall-clock `event_ts = datetime.now(UTC)`; `event_date = today in IST`. Token bucket paced at `effective_rate(h_IST) = BASE_RATE(10) × DIURNAL_HOUR_MULT[h] × rate_multiplier` (Σ=24.10 → mean ≈ 1.004×, peak 2.05× @ 19 IST; daily integral ≈ 868K at x=1 vs 1M cap → 13% headroom). Each tick (~200ms) drains lifecycle-priority then fills with BOOKING vs PRICE_CHANGE by per-hour weight `prob_b = w_b[h]/(w_b[h]+w_p[h])` — no 95/5 coin flip; daily aggregate shapes itself ≈ 48/52. Data-aware new-BOOKINGs: city by `popularity × hotel_count × season/holiday` from `dim_date`; hotel by `total_rooms × star` under per-hotel-per-night occupancy cap against overlapping `sim_open_bookings`; room type fitting `num_guests`; customer 75% out-of-state; lead-time 50% ≤7d / 35% 1-8wk / 15% 2-8mo; nights from empirical `fact_bookings.nights_stayed` mix; price = base × {weekend/holiday/season} × N(1.0,0.05); revenue = nightly × nights (invariant). Each BOOKING stamps `checkin_fire_ts` + `checkout_fire_ts` (and optional `cancel_fire_ts` at 12% deterministic) drawn from per-event-type IST hour distributions (CHECKOUT 8-13 peak 9-10, CHECKIN 12-21 peak 16-18, CANCELLATION 9-21 evening-lean). On CHECKOUT or CANCELLATION emit, if the negativity-bias draw yields a review, `state → 'REVIEW_PENDING'` + `review_fire_ts` set to tonight 20-23 IST (clamped ≥ now+30min); REVIEW fires when wall-clock reaches the stamp; row deleted on REVIEW emit. Catch-up rule: overdue fire-times at startup drain at the bucket cap with `event_ts = NOW()` — never backdated. `sim_daily_counter.cap = 1_000_000 × rate_multiplier`, **last-write-wins across same-day sessions** (migration 016 follow-on — every producer-startup UPSERT overwrites cap with `EXCLUDED.cap`; `events_emitted` is not overwritten and keeps accumulating). On cap-hit the producer sleeps until IST midnight. All per-tick DB mutations committed in ONE batch. Single throughput knob `--rate-multiplier` (int>1; any invalid value silently → 1; same coerce on `RATE_MULTIPLIER` env). Folds in B-041 (prior `--sim-rate` / `--rate` / `--duration` / `--sim-speed` no-op flags REMOVED entirely). Chaos extracted to `scripts/chaos_injector.py` (byte-identical behaviour, shared module). Single-instance: `pg_try_advisory_lock(7400030)`. Smoke-test verification: 298 events consumed / 0 malformed / 0 late / 100 silver inserted / 20 reviews / 0 revenue invariant breaks; CHECKIN fire-hour histogram all in 12-21 IST peak 17 (n=14); CHECKOUT all in 8-13 IST peak 10 (n=21); BOOKING vs PRICE_CHANGE mix at IST 00-01 = 12.7% (matches `w_b/(w_b+w_p) = 0.04/0.34 ≈ 0.118`); cancel rate 13.1% (target 12%). `--rate-multiplier` coerce verified 22/22 cases (CLI + env var). | Phase 2 |
