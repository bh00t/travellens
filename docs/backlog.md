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
- ~~B-026 — sentiment classification for reviews (substantially resolves L-012; aspect-level residual logged as L-017) — Stage 1 (schema + scorer + 133K backfill) + Stage 2 (retrieval swap to `sentiment_label`) + Stage 3 (run.py wiring of the live scorer as 7th proc) all shipped~~ ✓ FULLY CLOSED
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
- ~~B-048 — Explore tab: fact_booking_events live-data routing + cancellation-rate stacking fix~~ ✓ Done
- ~~B-049 — producer `SEASON_MULT` vocab fix (real `dim_date.season` keys + `_compute_price` Peak/Monsoon + unknown-value warning; follow-up to B-047)~~ ✓ Done
- ~~B-050 — README doc-consolidation: Documentation map + `run.py` reference (one entry-point, scattered nav links consolidated)~~ ✓ Done
- ~~B-051 — IVFFlat resize for the 133K-row corpus: rebuild index lists=30 → lists=120 + wire `SET ivfflat.probes = 11` into `ai/semantic_search.py` + dynamic `_ivfflat_lists()` helper in `scripts/generate_embeddings.py` (logged frozen-file exception in CLAUDE.md); recall preserved, partial remedy for the small end of L-005~~ ✓ Done
- ~~B-052 — README portfolio polish: animated dual-theme SVG hero (`docs/assets/pipeline-flow-{light,dark}.svg`) replaces the prior ASCII diagram; engineering-highlights intro tightened; `reviews_raw` count corrected to ~133K; redundant DuckDB aside dropped (already covered by the blueprint blockquote)~~ ✓ Done
- ~~B-053 — Dashboard curation: 11 portfolio widgets pinned across simple / grouped / complex / live / hybrid tiers; prior 13 exploratory widgets cleared first. 1 of 12 curated widgets skipped (monthly revenue 2025 — date-paradigm bleed)~~ ✓ Done
- ~~B-054 — Blueprint AI-sell: two namespaced (`.tl-*`) animations + business-first lead paragraphs for §07 Text-to-SQL and §08 Semantic Search; §02 metric strip updates (Schema tables 15 → 22 to match live DB · Real reviews 30K+ → 133K · added 6th tile "~20 Peak evt/sec @ 19 IST"); "Life before vs. after TravelLens" comparison block removed from §01 (header + 2-column ✗/✓ grid both gone); §05 schema heading + body left at "15 Tables" (curated star-schema view of the original 14 + dashboard_widgets), and README's "14 tables" mention flagged for a separate follow-up~~ ✓ Done
- B-055 — Realistic customer distribution in bookings: fact_bookings samples customer_id uniformly from a 100K dim_customer pool → ≈10 bookings/customer avg → every customer is structurally a "repeat" (widget id=13 reads ~40%+ vs real-hospitality 20-25%). Regenerate with weighted long-tail sampler; same distribution wired into the live producer.
- B-056 — Router: operation detection + hybrid-aggregation path: `ai/query_router.py` classifies on topic only (no operation primitive), so review-analytics queries route to semantic which has no aggregation ("top 5 hotels with most cleanliness complaints" returns 5★ "high cleanliness standards" reviews). Add pattern-based operation detection + new `ai/hybrid_aggregator.py`; `query_router.py` logged frozen-file exception.
- ~~B-059 — `_validate_columns` false-positive on SELECT-list alias reused in ORDER BY (single-table query): an alias like `COUNT(*) AS customer_count … ORDER BY customer_count` is rejected as a hallucinated bare column → valid model SQL becomes a both-attempts error. Surfaced by the B-058 eval harness (`customers_per_state` runs 1 & 3). B-003a-adjacent (false-POSITIVE). Fix in `ai/text_to_sql._validate_columns` — treat SELECT-list output aliases as known identifiers when validating ORDER BY on single-table queries~~ ✓ Done
- ~~B-060 — post-generation cancellation-filter lint + corrective retry (mitigates L-013): `run()`'s retry fires only on an ERROR — clean-but-wrong SQL ships unchallenged. Adds `lint_cancellation_filter_missing(sql, user_query)` (mirrors `run_eval`'s detection) — on a clean first execute, if a `fact_bookings` aggregate dropped `WHERE NOT is_cancelled` (not a rate, no cancellation intent, no `is_cancelled` anywhere — B-048 guard), drive ONE corrective retry naming the general schema rule. Falls back to the original on retry-error/still-firing (never degrades). Lint-only this cut; L-011/DISTINCT lint deferred~~ ✓ Done
- ~~B-061 — surface the B-060 cancellation-filter lint outcome as a pin-time warning in Explore: the lint records `result["lint_cancellation_filter"]` but nothing shows it. `fired_uncorrected` (lint fired, auto-fix didn't take → answer suspect) now shows a clear, non-blocking caution in the Explore preview BEFORE pinning (complements B-022 pin review); `fired_corrected` shows a subtle info note. WARN, never block — pin stays enabled. Template-only (flag already flows through `/api/query`); no schema change~~ ✓ Done
- ~~B-058 — Text-to-SQL accuracy eval harness (`ai/eval/`): on-demand measurement tool (NOT pytest) that runs a fixed question fixture through `ai.main.answer()`, grades by EXECUTION MATCH vs an owner-verified reference query (N runs/question, default 3), and flags L-011 (missing DISTINCT) + L-013 (missing cancellation filter) independently of exec-match. Cap-aware + probe-aware honest denominator. Fixture is TEST DATA — never fed back into the prompt~~ ✓ Done
- ~~B-062 — rating-based polarity filter for semantic search (meanwhile mitigation for L-012): `_detect_polarity` reads general negative/positive/neutral keyword rules off the query; negative → hard-filter retrieval to ≤2★, positive → ≥4★, neutral → unchanged, with a relax-and-note fallback below `MIN_POLARITY_RESULTS`. Flips the dominant polarity ("cleanliness complaints" now returns ≤2★ complaints, not 4-5★ praise). Rating is a coarse proxy — the deeper fix is B-026. `ai/semantic_search.py` + a one-line `filters`-merge in `ai/main.py`; no schema change~~ ✓ Done
- B-063 — `.claude/settings.json` allowlist hardening: narrow/remove destructive wildcards so they prompt (`docker volume *` → `ls` + `inspect` only; drop `docker rm *` / `docker cp *` / `pip install *` / broad `Stop-Process` + `taskkill`), declutter ~80 stale one-offs (specific PIDs, `c:\tmp\…` log paths, dated `monitor/data?from=…` URLs, dead B-047 `--rate`/`--duration` runs) down to the durable grouped subset the file's own `_comment` describes. Config-only; one-offs belong in the gitignored `.claude/settings.local.json` (built — pending owner review + commit).
- B-064 — accurate topic/aspect ranking (top-N hotels by complaint type) — PARKED, approach TBD: aspect-based sentiment tagging (each review → (topic, aspect_sentiment) PAIRS) ranked with deterministic SQL; new `review_aspects` table (PROPOSED, not built) + `aspect_rank` routing label. Blocked on tagging-engine choice (local qwen2.5:7b-instruct ~85% vs DeepInfra gemma-3-12B vs generator-hybrid). Supersedes the semantic path for complaint-ranking queries; related L-018, B-056.

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
B-048 (Explore tab: fact_booking_events live-data routing + cancellation-rate stacking fix),
B-049 (producer `SEASON_MULT` vocab fix — real `dim_date.season` keys + unknown-value warning; follow-up to B-047),
B-050 (README doc-consolidation — Documentation map + `run.py` reference; scattered nav-links consolidated, all 9 flags covered),
B-051 (IVFFlat resize for the 133K-row corpus — lists=30 → lists=120 + session `ivfflat.probes = 11` wired into `ai/semantic_search.py` + dynamic `_ivfflat_lists()` helper in `scripts/generate_embeddings.py` under logged frozen-file exception; partial remedy for the small end of L-005),
B-052 (README portfolio polish — animated dual-theme SVG hero replaces ASCII; engineering-highlights intro tightened; reviews_raw count corrected to 133K; redundant DuckDB aside dropped),
B-053 (dashboard curation — 11 portfolio widgets pinned across simple / grouped / complex / live / hybrid tiers; prior 13 exploratory widgets cleared first; 1 of 12 widgets skipped on date-paradigm bleed),
B-054 (blueprint AI-sell — two namespaced animations + business-first lead paragraphs for §07/§08; metric strip reconciled to live DB; "Life before vs. after TravelLens" comparison block removed from §01; README's "14 tables" mention flagged for follow-up),
B-058 (Text-to-SQL accuracy eval harness `ai/eval/` — on-demand execution-match measurement tool, N runs/question, L-011/L-013 flags independent of exec-match, cap/probe-aware denominator; fixture is test data, never prompt content),
B-059 (`_validate_columns` false-positive on SELECT alias reused in ORDER BY),
B-060 (post-generation cancellation-filter lint + corrective retry — meanwhile mitigation for L-013),
B-061 (surface the B-060 lint outcome as a non-blocking pin-time warning in Explore),
B-062 (rating-based polarity filter for semantic search — meanwhile mitigation for L-012),
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

### B-048 — Explore tab: fact_booking_events live-data routing + cancellation-rate stacking fix ✓ DONE

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) · datamodel: [`fact_booking_events` schema](../datamodel.md#fact_booking_events) (no schema change — documents the table for Explore-tab routing) · data: none.

**Priority:** High — two narrow Explore-tab defects, both rooted in the text-to-SQL system prompt.

**Problem 1 — Explore has no awareness of live data.** Every "today / now / live / streaming / last hour" question was funnelled through `fact_bookings`, the frozen 1M-row historical snapshot whose latest `booking_ts` is in May 2026. Live-stream events landing on `fact_booking_events` (B-039 silver + B-047 forward producer) were invisible to the Explorer — same dataset the `/monitor` page reads from, never reachable from a natural-language question.

**Problem 2 — "Cancellation rate" always returned 0.00%.** The system prompt's default rule "exclude cancelled bookings: `WHERE NOT b.is_cancelled`" stacked with the cancellation-rate formula `100.0 * SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END) / COUNT(*)`. The `WHERE` filtered out the very rows the numerator was counting → numerator always 0 → rate always 0.00. The "unless the question is specifically about cancellations" caveat in the rule wasn't strong enough; the LLM applied both anyway.

**What shipped (all in `ai/prompts/text_to_sql_system.txt` — general rules, no per-query examples):**

1. **`fact_booking_events` added to SCHEMA section** — all 26 columns enumerated with per-event-type population notes (PRICE_CHANGE has NULL booking_id/customer_id; revenue/rate populated on BOOKING only; REVIEW carries rating/review_channel/review_text; CANCELLATION carries reason). Spans both `source='history'` (B-035 backfill) and `source='stream'` (B-039 silver). Explicit note: NO `is_cancelled` flag — cancellations are EVENTS (`event_type='CANCELLATION'`).
2. **New "HISTORICAL vs LIVE" routing section** between JOIN MAP and COLUMN LOCATION. Two-rule order: (1) explicit live keywords (`today / now / current / live / streaming / so far today / right now / last <N> hour(s) / last <N> minute(s) / this hour / since midnight`) → `fact_booking_events` with `source='stream'`; (2) everything else → `fact_bookings`. "this month / this year / in 2025 / monthly / by year" stay routed to `fact_bookings` via dim_date. "recent" left ambiguous (defaults to historical). Worked-snippet for "today" + "last hour" included — kept tight (no per-question patches).
3. **Cancellation-rate stacking fixed.** Default exclude-cancelled rule rewritten to explicitly NOT apply when the query computes a rate/ratio/percentage/share that depends on counting BOTH cancelled and non-cancelled rows in the denominator. Formula changed from `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` to `COUNT(*) FILTER (WHERE b.is_cancelled)` — same math, cleaner idiom, named explicitly so it's harder for the LLM to silently substitute back. Explicit warning that stacking the default filter on a rate formula zeroes the answer.
4. **Cosmetic:** `opened_year` comment range updated `1975-2023` → `1975-2026` (B-046 widened the column).

**Files:** `ai/prompts/text_to_sql_system.txt`. No code change. `ai/text_to_sql.py` and `ai/query_router.py` untouched.

**Initial test results (12 queries / 11 PASS / 1 fail diagnosed below):** the first build of B-048 left 5e ("revenue this month by zone") failing with the model hallucinating `b.booking_date_key`, `b.booking_date`, `b.checkin_date=d.date_id` across three retries. Initially mis-classified as a pre-existing L-004 flake; the owner asked for a clean before/after diagnostic.

**Diagnostic (no git, hand-revert via backup file):** 5e BEFORE the B-048 prompt edit ran **5/5 PASS** (real per-region figures, correct `JOIN dim_date d ON b.date_id = d.date_id` every time). 5e AFTER the initial B-048 edit ran **1/5 PASS** (4 runs hallucinated the dim_date join column with three different wrong names). Conclusion: not L-004 — the B-048 edit caused it. Root cause: introducing `fact_booking_events` with its prominent `event_date` column gave the model a second "date column on a fact table" pattern; it bled the event-ledger pattern onto historical fact_bookings queries, inventing `b.event_date` / `b.booking_date` / `b.booking_date_key` instead of using the canonical `date_id → dim_date` join.

**Follow-up fix — DATE PARADIGMS rule (verified against real schema via information_schema before applying):**

Replaced the DATE HANDLING block in `ai/prompts/text_to_sql_system.txt` with a two-paradigm rule, picked by FACT TABLE:
- **fact_bookings paradigm:** canonical `JOIN dim_date d ON b.date_id = d.date_id`; enumerates the real `dim_date` column list (`date_id, full_date, day_of_month, day_of_week, day_num, week_of_year, month, month_name, quarter, year, is_weekend, is_holiday, is_high_demand_holiday, season`) so the model knows the real names rather than inventing `date_key` / `month_number`; enumerates `fact_bookings`' own date columns (`date_id, checkin_date, checkout_date, booking_ts, event_ts` — `booking_ts` and `event_ts` were the columns the LLM kept hinting at via Postgres errors). Closes with "For year/month/season filters on fact_bookings, always use the date_id → dim_date join above."
- **fact_booking_events paradigm:** `event_date` and `event_ts` live on the row — NO dim_date join. Direct filters: `WHERE event_date = CURRENT_DATE` for today; `WHERE event_ts >= NOW() - INTERVAL '<N> hour'` for last-N-hour.

Iteration notes (every variation pre-flight-tested, several rejected):
- ✗ Adding `booking_ts` / `event_ts` to the SCHEMA listing helped the model recognise those names but invited a NEW failure — it tried `JOIN dim_date d ON b.booking_ts = d.date_key`, which broke H1. Reverted.
- ✗ Long preamble paragraphs and negative-example lists (`do NOT invent booking_date / event_date`) made the model lose strict-SQL discipline and emit prose+markdown+SQL — caught by the SELECT-only guard but failing the query. Reverted in favour of positive-only phrasing.
- ✓ Final form: short positive rule with real column enumerations. The model still reaches for `b.event_date` on the FIRST attempt ~80% of the time on 5e, but the rule plus the B-001 retry loop now self-correct that into a working dim_date join most of the time.

**Final test results (after DATE PARADIGMS — 5e ×5 plus full battery, retried sequentially):**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| 5e ×5 "revenue this month by zone" | fact_bookings + dim_date via date_id | Run 1 PASS (retry from b.event_date) · Run 2 PASS (retry) · Run 3 PASS (retry) · **Run 4 FAIL** (intermittent chatty mode — model emitted prose+SQL, rejected by SELECT-only guard, NOT a column hallucination) · Run 5 PASS (retry). Passing runs return real per-region revenue (North ₹6.89cr, West ₹2.00cr, South ₹1.91cr, East ₹0.25cr). | **4 / 5 PASS** (was 1/5 before fix, 5/5 pre-B-048) |
| 5a "top 5 cities by revenue" | fact_bookings, 5 cities | Goa ₹235.37cr, Varanasi, Alleppey, Delhi, Jaipur (default exclude-cancelled applied this run) | PASS |
| 5b "hotels in Kumarakom" | hotel_master direct | 1 row (COUNT-form interpretation) — model interpretation drift Ollama-side, query executes correctly | PASS |
| 5c "average nightly rate in Goa" | fact_bookings + default `NOT is_cancelled` | ₹7,714.92 | PASS |
| L1 "how many bookings today" | fact_booking_events + event_date + source='stream' | **821** (matches DB) | PASS |
| L2 "bookings in the last hour" | fact_booking_events + event_ts | 0 (producer idle) | PASS |
| L3 "live bookings so far today" | fact_booking_events + source='stream' | **821** | PASS |
| H1 "total bookings in 2025" | fact_bookings (NOT fact_booking_events) | 415,067 via retry — close to DB ground truth 414,755 (small drift from is_active / scope interpretation) | PASS |
| H2 "revenue by city all time" | fact_bookings chain | 44 cities returned | PASS |
| C1 "cancellation rate for 5-star hotels" | FILTER-form, no is_cancelled WHERE | **12.69%** | PASS |
| C2 "overall cancellation rate" | FILTER-form | **11.81%** (matches DB) | PASS |
| C3 "cancellation rate in Goa" | FILTER + city scope only | **11.83%** (matches DB) | PASS |

**Net result:** 5e moved from 1/5 (regression introduced by initial B-048) → 4/5 (after DATE PARADIGMS rule). All live-routing and cancellation-rate fixes from the initial B-048 still pass with identical results. Residual 1/5 fail on 5e is an intermittent chatty-mode flake (model emits prose+SQL despite "Output ONLY a SELECT" instruction) — falls through the SELECT-only guard cleanly, not a column hallucination. The B-001 retry loop is now load-bearing for 5e; the rule pushed first-attempt and retry into the same successful pattern most of the time.

**Out of scope:** restoring 5/5 on 5e would require either (a) removing `fact_booking_events` from the SCHEMA entirely — abandoning B-048's live-data goal — or (b) a model swap (B-017 territory: Qwen2.5-Coder-7B's bleed between similarly-named patterns is the underlying limit).

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

> **Trace.** Phase(s): [Phase 3](phase-3-embeddings.md) (ingest/embeddings) + [Phase 4](phase-4-ai-layer.md) (AI layer) · datamodel: `reviews_raw` (+ `sentiment_label`, `sentiment_score`; [migration 017](../datamodel.md#migration-017--per-review-sentiment-columns-b-026-stage-1)) · data: `scripts/review_sentiment_scorer.py` (backfill + continuous ingest), [Regenerating the Dataset → Stage C3](../datamodel.md#stage-c--post-load-seeds-run-after-stage-a-safe-before-or-after-stage-b).

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

**Staging (built + verified in safe steps — each later stage gated on the prior's verification):**

- **Stage 1 — schema + backfill + scoring infra (✅ SHIPPED, retrieval UNCHANGED).** `db/migrations/017_review_sentiment.sql` (2 nullable cols `sentiment_label VARCHAR(8)` + `sentiment_score NUMERIC(4,3)` + partial index `idx_reviews_raw_sentiment`); `scripts/review_sentiment_scorer.py` (advisory lock 7400070; `--once` backfill + continuous loop; CardiffNLP `twitter-roberta-base-sentiment-latest` pinned to safetensors revision `d616e2bd…`, `use_safetensors=True` — the `.bin` checkpoint is blocked on torch 2.3.0 by CVE-2025-32434, torch NOT upgraded per L-008). One-time 133K backfill run; label spot-check gate passed (see verification table). `_search_reviews` / `_detect_polarity` UNTOUCHED — B-062 stays live this stage. NOT wired into run.py yet.
  - Audit/feasibility decisions: sentiment is INDEPENDENT of the embedding (additive columns, **no re-embed of the 133K vectors**); measured GPU throughput ~153 rev/s → ~15 min backfill (vs. Ollama's 12–37 h, ruled out by numbers); separate process (not co-located in `review_embedder.py`) for VRAM headroom + one-process-per-concern.
- **Stage 2 — retrieval swap (✅ SHIPPED 2026-05-30).** In `_search_reviews`, **REPLACED** the B-062 rating predicate (`r.rating <= 2.0` / `>= 4.0`) with `r.sentiment_label = %s` ('negative' / 'positive'); the `rating_max`/`rating_min` params became a single `sentiment_label` param. NOT combined with rating (AND-ing `rating<=2` back in would re-exclude the 3★ complaints this item exists to recover). `_detect_polarity` reused unchanged; the relax-and-note fallback below `MIN_POLARITY_RESULTS` preserved (drops the sentiment predicate, sets `polarity_relaxed=True`). `filters.polarity_threshold` now reads `sentiment='negative'` instead of `<=2`. Label-only v1 — `sentiment_score` stored but NOT yet gated (a later confidence lever). Additive: the returned review dict gains a `sentiment_label` key (existing consumers read keys by name, unaffected). Rows with `sentiment_label` NULL (new stream reviews not yet scored — Stage 3) are correctly excluded from polarity results. **Files:** `ai/semantic_search.py` + adapted `tests/test_polarity_filter.py` (rating-predicate assertions → sentiment-predicate assertions; `_detect_polarity` tests unchanged). SQL path / `text_to_sql.py` / system prompt UNTOUCHED. Marks L-012 substantially resolved; aspect-level residual logged as **L-017**.
- **Stage 3 — run.py wiring (✅ SHIPPED 2026-05-30 — B-026 FULLY CLOSED).** `review_sentiment_scorer.py` added as run.py's **7th managed proc**, launched bare (continuous loop mode — run.py owns its lifecycle exactly as it owns the embedder's, B-030b); the B-045 startup-takeover lock-poll extended from `7400030/40/50/60` to also wait on `7400070`. **Wiring only** — no change to the scorer's logic, schema, retrieval path, scoring, or batch sizes (`_ENCODE_BATCH=64`, `SENTIMENT_BATCH_SIZE=2000` unchanged); a prior read-only resource audit confirmed all 7 procs + Ollama fit (~91% VRAM peak, no OOM; ~75% RAM) so no batch/scoring change was needed. **Files:** `run.py` (SCORER spec + `start_python()` registration + colour tag + `_TRAVELLENS_CMD_PATTERNS`/`_TRAVELLENS_PROC_LABELS` + lock-poll/docstrings extended to 7400070). VRAM finding captured as an OPERATIONAL NOTE in [phase-3-embeddings.md](phase-3-embeddings.md#b-026-stage-3--live-scorer-wired-into-runpy-7th-managed-proc--b-026-fully-closed) alongside the Stage 3 build-history entry. Verification 7/7 PASS (all 7 procs up; scorer logs "Advisory lock acquired (key=7400070)"; other 6 unaffected; controlled NULL-sentiment probe scored `negative` 0.961 within ~15s then deleted, corpus restored; clean Ctrl-C → 0 locks held, 0 orphan children; `pytest` 76 passed/1 xfailed = baseline). Full table in [phase-3-embeddings.md → B-026 (Stage 3)](phase-3-embeddings.md#b-026-stage-3--live-scorer-wired-into-runpy-7th-managed-proc--b-026-fully-closed).

**Stage 1 verification (2026-05-30):**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Migration 017 applied | 2 cols + partial index | `sentiment_label`, `sentiment_score`, `idx_reviews_raw_sentiment` created | PASS |
| Backfill coverage | 133,543 scored / 0 unscored | 133,543 / 0 (5,209s ≈ 87 min on shared GPU) | PASS |
| Embedding untouched | 133,543 embedded, index intact | unchanged (additive columns only) | PASS |
| `pytest tests/ -q` | green | 76 passed, 1 xfailed (B-003a) — = baseline | PASS |
| Retrieval / SQL path untouched | `_search_reviews` / `_detect_polarity` / `text_to_sql.py` unchanged | unchanged this stage | PASS |
| **Label spot-check GATE** | model beats rating proxy; labels trustworthy | **~93% net-sentiment agreement on 100 stratified; PASSED** (below) | PASS |

**Label distribution (all 133,543):** positive 89,800 (67%) · negative 38,834 (29%) · neutral 4,909 (4%). Star×label crosstab is monotonic and correct: 1★ 91% neg · 2★ 94% neg · **3★ 38% neg / 53% pos / 9% neutral** · 4★ 96% pos · 5★ 98% pos.

**Spot-check (100 stratified, 20/star, hand-adjudicated on NET sentiment of the text, not the star):** ~93/100 agree. 1★ 19/20 · 2★ 20/20 · 4★ 20/20 · 5★ 19/20; the disagreements are **all** concentrated in mixed-sentiment 3★ reviews and off-topic noise — i.e. the documented aspect-level residual (a positive opener flattening a later complaint), NOT systematic error. **Decisive B-026-vs-B-062 test (18 hard 3★ complaint-text reviews — B-062 gives 3★ NO filter, so it renders all 18 invisible to a negative query):** B-026 recovers **11/18 genuine complaints as `negative`**; the 7 it labels positive are the positive-opener mixed cases (the known residual). The 3★ band is exactly where the rating proxy is blind, and B-026 correctly splits it. **Gate passed — labels are trustworthy for the Stage-2 retrieval swap.**

**Stage 2 verification (2026-05-30):**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Predicate swap | `_search_reviews` emits `r.sentiment_label = %s`, NO `r.rating <=/>= %s` | confirmed via fake-cursor SQL capture | PASS |
| `_detect_polarity` unchanged | negative/positive/neutral intent + terms identical | all 10 detector tests pass unchanged | PASS |
| Relax fallback intact | <`MIN_POLARITY_RESULTS` drops predicate, sets `polarity_relaxed=True` | unchanged seam, drops sentiment_label | PASS |
| A/B negative "cleanliness complaints" | recovers 3★ negatives the ≤2★ filter excluded | **19** genuine 3★ NEGATIVE reviews now surfaced; 20/20 labelled negative | PASS |
| A/B negative "rude staff" | same | **8** genuine 3★ NEGATIVE reviews recovered; 20/20 negative | PASS |
| Positive control "what guests love" | high-sentiment | 20/20 `positive`, rating 3.0–5.0 | PASS |
| Neutral control "what are guests saying about cleanliness" | unfiltered, spans labels | no `filters` key; labels span positive+negative | PASS |
| `pytest tests/ -v` | green | 76 passed, 1 xfailed (B-003a) — = baseline | PASS |
| SQL path / `text_to_sql.py` untouched | unchanged | only `semantic_search.py` + `test_polarity_filter.py` in diff | PASS |

Note: "noisy AC" is classified **neutral** by `_detect_polarity` (no sentiment keyword), so in the live path it stays unfiltered — the A/B forced a negative predicate only to demonstrate the mechanism. The owner-runnable negative SEMANTIC query that returns reviews is **"cleanliness complaints"** (NOT a "top 5 hotel names" ranking query — that aggregation gap is separate and unsolved).

> **Status:** ✅ **FULLY CLOSED (2026-05-30)** — Stage 1 (schema + scorer + 133K backfill) + Stage 2 (retrieval swap to `sentiment_label`) + Stage 3 (run.py wiring of the live scorer as the 7th managed proc) all shipped + verified. Each stage was committed before the next stage's session opened (converge state).

> **Meanwhile mitigation SUPERSEDED:** [B-062](#b-062--rating-based-polarity-filter-for-semantic-search-mitigates-l-012--done)'s rating proxy is now replaced in retrieval by B-026 Stage 2's `sentiment_label` predicate. B-062 remains in the codebase as the query-intent detector (`_detect_polarity`, reused unchanged) — only its SQL predicate was swapped. The aspect-level residual is tracked as **L-017**.

---

### B-062 — rating-based polarity filter for semantic search (mitigates L-012) · DONE · retrieval predicate SUPERSEDED by B-026 Stage 2

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) (AI layer) · datamodel: none · data: none.

> **Superseded (2026-05-30):** B-026 Stage 2 replaced this item's `r.rating <= 2 / >= 4` retrieval predicate with a `sentiment_label` predicate. The query-intent detector built here (`_detect_polarity`) is RETAINED and reused unchanged — only the SQL bound it drives was swapped from star rating to model sentiment. See [B-026](#b-026--sentiment-classification-for-reviews) and [L-012](#known-limitations-l-).

**Problem:** [L-012](#completed) — semantic search matches a query's TOPIC but ignores its SENTIMENT.
"cleanliness complaints" embeds close to *both* praise and complaints about cleanliness, and since
high-rated topical matches vastly outnumber low-rated ones (live corpus: 19,661 ≥4★ cleanliness
reviews vs 2,928 ≤2★), the top-K comes back as PRAISE — the opposite of the user's intent. Extends
[B-004](#completed)'s hybrid-filter pattern (the seam where structured filters slot into the
pgvector retrieval) with a sentiment dimension. The proper fix is [B-026](#b-026--sentiment-classification-for-reviews)
(sentiment-at-embed-time); this is the rating-based **meanwhile mitigation**.

**Data gate (passed before building):** rating distribution across the 133,543-review corpus —
1★ 4,730 · 2★ 19,037 · 3★ 39,520 · 4★ 39,810 · 5★ 30,446. ≤2★ = 23,767 (17.8%), with 2,928 / 5,927 /
2,608 low-rated reviews on cleanliness / staff-service / noise respectively. The corpus is NOT
overwhelmingly positive — a polarity filter has ample low-rated reviews to surface.

**What shipped — `ai/semantic_search.py` + a one-line merge in `ai/main.py`:**

- `_detect_polarity(query) -> (polarity, matched_terms)` — GENERAL keyword rules (NOT per-query
  patterns). Counts DISTINCT terms from a negative lexicon (`complaint(s)`, `bad`, `worst`, `dirty`,
  `rude`, `terrible`, …) and a positive lexicon (`best`, `love(d)`, `excellent`, `recommend`, …);
  larger side wins, tie / none → **neutral**. Word-boundary regex so "love" ∉ "glove", "best" ∉
  "bestseller". "clean"/"cleanliness" deliberately excluded (topic word, not sentiment).
- `_search_reviews` gains `rating_max` / `rating_min` params → `r.rating <= %s` / `>= %s` appended to
  the SAME inner WHERE as the city + hotel_ids filters (one execution plan). Review-grain
  `reviews_raw.rating`; independent of and composes with B-004's hotel-grain `hotel_master.avg_rating`.
- `run()`: negative → `rating_max=2.0`, positive → `rating_min=4.0`, neutral → no bound.
  **HARD filter** (not a soft re-rank — a bias would lose to the 19.6K-vs-2.9K volume and not flip
  the polarity). **Relax-and-note fallback:** if the hard filter returns `< MIN_POLARITY_RESULTS` (5),
  re-run without the bound and set `polarity_relaxed=True` — handles thin city-scoped scopes honestly.
- Transparency: the applied polarity is recorded in the result's `filters` field
  (`polarity`, `polarity_terms`, `polarity_threshold`, `polarity_relaxed`); neutral adds no key
  (result shape unchanged). `ai/main.py:244` flipped `result["filters"] = filters` →
  `{**result.get("filters", {}), **filters}` so B-004's keys and B-062's polarity keys coexist
  (disjoint key-sets — neither transparency is lost).

**Hard boundary honoured:** semantic path ONLY — `text_to_sql.py` / the SQL path untouched. No schema
change, no migration (reuses the existing rating). Detector is general keyword rules, no per-query
patches.

**Known caveat (accepted):** the detector fires negative on a single negative word (`neg=1, pos=0`),
so a query with an incidental "bad"/"best" over-filters. The relax fallback bounds the downside and
precision-over-recall is the stated stance. Live testing showed no over-firing on the verification
queries (neutral stayed neutral). Rating is also a coarse proxy — real complaints in mixed-sentiment
3★ reviews are missed by design; B-026 is the deeper fix.

**Tests (`tests/test_polarity_filter.py`, +16):** detector classification (negative / positive /
neutral, ties, word-boundary, dedup/lowercase, "clean" not a polarity term); predicate construction
via a fake cursor (≤2★ / ≥4★ / no-bound SQL; composition with city + hotel_ids); and a slow live-DB
proof (Ollama stubbed) asserting returned ratings obey the polarity. Two B-004 tests
(`test_pure_semantic_no_hybrid_keys`, `test_city_only_semantic_no_hybrid_keys`) updated: their queries
("rude staff", "complaints in Goa") carry sentiment words, so `filters` now legitimately holds polarity
keys — the assertions were narrowed to the true hybrid markers (`hotel_id_count` + the B-004
rating/star/city keys), which remain absent.

**Acceptance / quality-gate** (the dev cannot take browser screenshots — the owner does; the dev
verifies the retrieval deterministically + reports repro queries):

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Negative query "…cleanliness complaints" | returned reviews ≤2★ | 20/20 ≤2★ (one 1★, rest 2★) | PASS |
| Image 3's "…cleanness complaints" | negative, ≤2★ ("complaints" fires it) | 20/20 = 2★ | PASS |
| Positive query "…love most about beach resorts" | returned reviews ≥4★ | 20/20 ≥4★ (4-5★ mix) | PASS |
| Neutral "what are guests saying about cleanliness" | unchanged, no polarity filter | spans 3-5★, no `filters` key | PASS |
| Detector classification (16 unit cases) | correct polarity + terms | all pass | PASS |
| `r.rating` predicate per bound (fake cursor) | ≤2 / ≥4 / none | all pass | PASS |
| `pytest tests/ -v` | green | 76 passed, 1 xfailed | PASS |
| SQL path / `text_to_sql.py` | untouched | unchanged | PASS |

**Owner screenshot repro queries:**
- Negative: **"top 5 hotels with cleanness complaints"** (Image 3's exact wording) — now surfaces ≤2★ complaints.
- Positive: **"what do guests love most about beach resorts"** — ≥4★ praise.
- Neutral: **"what are guests saying about cleanliness"** — unchanged, full star range.

**Out of scope (named):** sentiment-at-embed-time (B-026, the proper fix); surfacing the polarity
badge in the Explore/dashboard UI (template work, separate item); aspect-based per-topic polarity.

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

**Follow-up:** [B-049](#b-049--producer-season_mult-vocab-fix-real-dim_dateseason-keys--unknown-value-warning--done) fixed a vocab-mismatch defect in this rewrite — the producer's `SEASON_MULT` and `_compute_price` season branch were keyed to assumed names (`Winter`/`Spring`/`Autumn`) that don't exist in the real `dim_date.season` vocabulary (`Peak`/`Shoulder`/`Summer`/`Monsoon`), so the season signal silently fell through to 1.0 on most days.

---

### B-049 — Producer `SEASON_MULT` vocab fix (real `dim_date.season` keys + unknown-value warning) ✓ DONE

> **Trace.** Phase(s): [Phase 2](phase-2-streaming.md) · datamodel: [`dim_date.season` vocabulary](../datamodel.md#dim_datecsv) (no schema change — documents the real enum values consumed by the producer) · data: `scripts/kafka_event_producer.py` (`SEASON_MULT` dict + new `_season_mult()` helper + `_compute_price` season branch).

**Priority:** High for realism · low for stability — narrow follow-up to [B-047](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done). No schema change, no wire-contract change, no downstream consumer/gold/monitor impact.

**Problem.** `SEASON_MULT` in `scripts/kafka_event_producer.py` was keyed `{"Summer", "Winter", "Monsoon", "Spring", "Autumn"}`. The real `SELECT DISTINCT season FROM dim_date` vocabulary is `{"Peak", "Shoulder", "Summer", "Monsoon"}`. Only `Summer` and `Monsoon` matched; **`Peak` (1,059 of 2,557 days — Jan/Feb/Oct/Nov/Dec, the entire Indian winter tourism peak) and `Shoulder` (217 days — March) silently fell through `dict.get(..., 1.0)` to a neutral multiplier**. The producer's city-selection weighting therefore had NO winter-peak signal — the largest, highest-demand season was the most heavily neutralized. Same defect in `_compute_price`'s inline `elif season == "Winter": mult *= 1.10` — never fires against the real `Peak` value, so the winter-peak pricing uplift was completely missing.

**Root cause.** Vocab assumed from generic seasonal-naming conventions, not verified against the actual `dim_date.season` column. Closely related to (and avoided by) the existing CLAUDE.md hard rule "Build all LLM-facing content from the real schema, never from memory" — extended here to producer enum keys in general.

**What shipped (single file, `scripts/kafka_event_producer.py`):**

1. **`SEASON_MULT` re-keyed to the real vocabulary** — every value `SELECT DISTINCT season FROM dim_date` returns is now an explicit key (0 fall-through, 0 unused). Multipliers chosen to match Indian tourism semantics:
    - `Peak` → **1.30** (Jan/Feb/Oct/Nov/Dec — winter peak, highest demand)
    - `Shoulder` → **1.00** (March — transitional)
    - `Summer` → **1.00** (Apr/May/Jun — mixed: plains low, hill-stations high; kept neutral)
    - `Monsoon` → **0.75** (Jul/Aug/Sep — lowest demand across most of India)
2. **New `_season_mult(season)` helper** replaces the bare `SEASON_MULT.get(..., 1.0)` call. Behaviour: returns the mapped multiplier; on an unrecognised value, returns 1.0 AND emits a **one-time WARNING to stderr** naming the unknown value + the expected set. Uses a module-level `_UNKNOWN_SEASON_WARNED` set so future vocab drift is caught loudly without log spam.
3. **`_compute_price` season branch updated** — `elif season == "Winter": mult *= 1.10` → `elif season == "Peak": mult *= 1.10`. Kept noise-free (no per-tick warning); `_season_mult()` above is the canonical warner.

**Date-source finding (surfaced, not changed).** Both `season_mult` and `_compute_price` are keyed on `today_ist` (the BOOKING/emit date), not on `checkin_d` (the STAY date). Real hotel demand tracks the TRAVEL season, not the booking-decision moment — so a December booking for a July monsoon stay should ideally weight by Monsoon, not Peak. **Left as-is in this fix** — changing it would shift the empirical mix and needs an owner decision (likely a separate B-number, possibly bundled with Stage 2b tuning). Flagged here for visibility.

**Verification — TEST | EXPECTED | ACTUAL | PASS/FAIL:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| `SELECT DISTINCT season FROM dim_date` vs `SEASON_MULT.keys()` | every real value has an explicit multiplier, no unused keys | real = {Monsoon, Peak, Shoulder, Summer}; keys = {Monsoon, Peak, Shoulder, Summer}; fall-through = []; unused = [] | PASS |
| `_season_mult("Peak" / "Shoulder" / "Summer" / "Monsoon")` | 1.30 / 1.00 / 1.00 / 0.75 | 1.30 / 1.00 / 1.00 / 0.75 | PASS |
| Unknown value (`"Spring"`) → 1.0 + one-line WARNING to stderr; second call silent | one warning, default 1.0, dedup on repeat | warning emitted on first call only; `_UNKNOWN_SEASON_WARNED` = {Spring, Autumn} after exercising both | PASS |
| Producer 45s standalone smoke test (no chaos override, today = Summer) | clean run; no WARNING/ERROR/Traceback; counter advances at curve rate | 889 events emitted (10,575 → 11,464 on `sim_daily_counter`); ~20 evt/s observed at IST 19-20 peak; 0 WARNING / 0 ERROR / 0 Traceback in stdout | PASS |
| Revenue invariant `revenue = nightly × nights` (asserted at INSERT time — would crash producer if broken) | 0 assertion failures across all new BOOKINGs | 0 (producer ran clean to manual termination after 45s) | PASS |
| All 5 event types reachable on the wire | BOOKING / PRICE_CHANGE / CHECKIN / CANCELLATION emit in window; CHECKOUT/REVIEW conditional on fire-time | 394 BOOKING + 325 PRICE_CHANGE + 53 CHECKIN + 46 CANCELLATION observed in 45s (CHECKOUT/REVIEW fire-times not due during window — expected) | PASS |
| Downstream call sites of `SEASON_MULT` / `HIGH_DEMAND_HOLIDAY_MULT` outside the producer | none | `grep -r SEASON_MULT` returns only `scripts/kafka_event_producer.py`; `datamodel.md` match is the unrelated `off_season_multiplier` string in `scripts/generate_datasets.py` | PASS |

**Files:** `scripts/kafka_event_producer.py`. No migration, no consumer change, no wire-contract change, no monitor or gold change.

**Out of scope:** (a) switching the season lookup to `checkin_d` (stay-date) instead of `today_ist` (booking-date) — see Date-source finding above; (b) per-city `peak_months` matching (already noted as future tuning hook on `weighted_city_pick`'s docstring under B-047); (c) re-tuning the price `Peak` mult above the current 1.10 — owner can adjust without a new B-number, alongside Stage 2b tuning.

---

### B-050 — README doc-consolidation: Documentation map + `run.py` reference ✓ DONE

> **Trace.** Phase(s): ops/dev · datamodel: none · data: none.

**Priority:** Medium — README was the only doc without a clear navigation contract. Inline `db/schema.sql` and `datamodel.md` links were scattered across the Highlights and Data Model sections; the existing `run.py` snippet listed 4 of the 9 flags and didn't mention the B-045 takeover or the B-047 `--rate-multiplier` semantics. Newcomers had no single page that mapped "where do I find X."

**Problem.** Two thin issues bundled into one consolidation pass per owner instruction (one doc-consolidation item, not two):
1. **No documentation map.** Scattered inline links (`db/schema.sql` × 2, `datamodel.md`, `capabilities-and-limits.md`, `phase-2-streaming.md`) gave a reader the docs piecemeal in the order the README's marketing prose chose to mention them — never as a navigable index.
2. **`run.py` reference was stale + thin.** Showed 4 of 9 flags (`--no-sim`, `--server-only`, `--window`, and an implicit default), missed `--rate-multiplier` / `--chaos` / `--malformed-pct` / `--late-pct` / `--chaos-seed` / `--no-docker` / `--down`. Did not mention the B-045 takeover (newest-wins startup, foreign-:5000 guard, 47219 singleton port), did not name the six host processes, did not say `--sim-rate` was deleted by B-047. Session-notes line 57 carried an open TODO for "README line 101 quotes the deprecated `--rate 50 --duration 60`" — still unresolved at the start of this turn.

**What shipped (single file, `README.md`):**

1. **New `## Documentation map` section** inserted between Data model and Quickstart. Single table, one line per doc, link + "what's in it / when to read it." Covers: `CLAUDE.md` (repo index), `datamodel.md` (data-model source of truth + canonical regenerate sequence), `db/schema.sql` (frozen base + reminder to read migrations alongside), all 7 phase docs (`phase-1-postgres` → `phase-7-monitor`, framed as history documents per the conventions hierarchy), `docs/backlog.md` (B-/L- status), `docs/capabilities-and-limits.md` (per-feature reliability), and the external blueprint (original design + repo-wins-where-they-conflict). Links + one-liners ONLY — no duplication of doc content; each doc remains the single home of its own detail.
2. **`### Running the stack — \`run.py\` reference`** (renamed from "Running the stack") expanded to:
    - 7-line **Common shapes** code block covering the most-used invocations.
    - **Flags** table — all 9 flags with one-line each, source-verified against `run.py --help` and the `add_argument` block.
    - **Behaviour notes** — singleton port 47219 + child advisory-lock pairs (producer 7400030 · gold 7400040 · embedder 7400050 · quarantine-rollup 7400060), foreign-port `:5000` guard, wall-clock-only producer model (B-047), daily-cap last-write-wins (migration 016).
    - Closing cross-reference to `docs/phase-2-streaming.md` for wire-format and consumer gates.
3. **Scattered nav-links consolidated.** `db/schema.sql` inline reference at the "14-table star schema" highlight stripped (now only in the map + Quickstart's `psql <` redirect, which is a shell command not a nav link). `datamodel.md` inline reference in the Data model section stripped (replaced with "see the Documentation map below"). Other inline references kept (`docs/phase-2-streaming.md` at the producer-commands sentence, `ai/text_to_sql.py` in the AI highlight) — those are contextual code citations, not navigation, and serve a different reading purpose.

**Files:** `README.md` (the only edit). `docs/session-notes.md` line 57 ("README.md line 101 quotes `--rate 50 --duration 60` as a manual example") pruned — the rewritten section no longer carries that snippet; the in-flight TODO is closed by this item.

**Verification — TEST | EXPECTED | ACTUAL | PASS/FAIL:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Flag list in README matches `run.py --help` exactly | all 9 flags covered with accurate one-line semantics | `--rate-multiplier` / `--chaos` / `--malformed-pct` / `--late-pct` / `--chaos-seed` / `--no-sim` / `--server-only` / `--no-docker` / `--window` / `--down` — all present, semantics match | PASS |
| No stale flag mentions (B-041 deletions) | no `--sim-rate` / `--rate` / `--duration` / `--sim-speed` in any code block or table | grep clean | PASS |
| Docs-map covers every actively-maintained doc | CLAUDE.md, datamodel.md, schema.sql, all 7 phase docs, backlog.md, capabilities-and-limits.md, blueprint | 7 rows present, all covered | PASS |
| No content duplication (links + one-liners only) | each doc's substance stays in that doc | one-liner descriptions only; no schema rows, no flag tables duplicated, no backlog content restated | PASS |
| Anchor / relative-link integrity | all map links resolve | `CLAUDE.md`, `datamodel.md`, `db/schema.sql`, `db/migrations/`, `docs/phase-*.md` (×7), `docs/backlog.md`, `docs/capabilities-and-limits.md` — all paths exist (verified by `ls`); external blueprint URL unchanged | PASS |
| `run.py` behaviour-notes claims are accurate | takeover + foreign-:5000 guard + advisory-lock pairs + wall-clock producer + last-write-wins cap | all four claims cross-checked against [B-045](#b-045--runpy-startup-takeover-newest-wins--kills-prior-runpy--children) (takeover + guard + port 47219), [B-047 / migration 016](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done) (wall-clock + last-write-wins), and CLAUDE.md repo-layout for the 6-process list | PASS |
| Session-notes line 57 TODO closed | line removed, dangling reference resolved | pruned (kept the rest of the 2026-05-25 entry intact — only the `--rate 50 --duration 60` bullet removed) | PASS |

**Out of scope:** further README tightening (Tech stack table → could be a docs link; Design decisions section → could move to the blueprint and be reduced to a stub) — both unprompted and beyond the consolidation brief; future B-numbers if owner wants them.

---

### B-051 — IVFFlat resize for the 133K-row corpus (lists 30 → 120 + probes wired + dynamic-lists frozen-file exception) ✓ DONE

> **Trace.** Phase(s): [Phase 3](phase-3-embeddings.md) · datamodel: [`review_embeddings` index sizing + probes rule of thumb](../datamodel.md#review_embeddings) (no schema change — documents the live index + per-connection GUC) · data: `scripts/generate_embeddings.py` (`_ivfflat_lists()` helper added under logged frozen-file exception).

**Priority:** High for retrieval quality at the new scale (133K reviews vs. the original 30K) — was a soft no-op for correctness but a measurable degradation of recall headroom past ~100K vectors if left at `lists=30`. Partial remedy for the small end of [L-005](#open--limitations-l-) (IVFFlat past ~1M vectors stays open — HNSW is the planned switch).

**Problem.** Two coupled gaps were sitting against the post-B-030a / B-046 grown corpus:

1. **Index undersized.** `idx_reviews_embedding` was built at `lists=30` — correct for the 30K Kaggle seed but 4× too few partitions for the 133K-row live corpus (rule of thumb: `lists ≈ rows / 1000` up to ~1M). Per-partition vector counts had grown ~4×, so each query's partition-local scan was doing more work without any change to the IO budget. Recall headroom for scale-out (further reviews, future cities) was thinning silently.
2. **Probes never set.** `ai/semantic_search.py` never called `SET ivfflat.probes` — production ran at the pgvector default `probes=1`. At the original 30 partitions, scanning 1 of 30 was acceptable. **Critical risk for this turn:** raising `lists` to 120 WITHOUT raising `probes` would have scanned 1 of 120 (~3× fewer cells of the index) and silently degraded recall vs. the old setup. The two parameters move together.

**What shipped:**

1. **Index rebuilt** (manual, single `DROP / CREATE`, no migration — IVFFlat indexes are not schema):
   ```sql
   DROP INDEX IF EXISTS idx_reviews_embedding;
   CREATE INDEX idx_reviews_embedding ON reviews_raw
   USING ivfflat (embedding vector_cosine_ops) WITH (lists = 120);
   ```
2. **`ai/semantic_search.py`** — `SET ivfflat.probes = 11` (≈ `sqrt(120)`) added once per connection inside `run()`, right after `register_vector(conn)`. Session-scoped GUC, executed before any vector query. Documented in-file with the rule-of-thumb + why it's per-connection.
3. **`datamodel.md`** — `review_embeddings` section updated: live `lists=120` + `probes=11`, sizing rule of thumb (`lists ≈ rows/1000`, `probes ≈ sqrt(lists)`), explicit warning that the two parameters must move together. Stage-C2 row notes the dynamic-lists helper below.
4. **`scripts/generate_embeddings.py`** — **logged frozen-file exception, taken second turn.** `IVFFLAT_LISTS = 30` constant removed; replaced with `_ivfflat_lists(embedded_rowcount)` helper invoked from `build_ivfflat_index()` (reads `COUNT(*) WHERE embedding IS NOT NULL` at build time). Formula: `max(rowcount // 1000, 30)` up to 1M vectors; `int(sqrt(rowcount))` past 1M (graceful fallback ahead of the L-005 HNSW switch). Logged in [CLAUDE.md frozen-file list](../CLAUDE.md) as a one-line exception. No effect on the running system or the live index — only changes what a FUTURE full re-embed builds.

**Frozen-file exception taken (option (a)).** `scripts/generate_embeddings.py` previously hardcoded `IVFFLAT_LISTS = 30` and is on the CLAUDE.md frozen list. The constant is now replaced with a `_ivfflat_lists(embedded_rowcount)` helper computed at index-build time: `lists = max(rowcount // 1000, 30)` up to 1M vectors, `int(sqrt(rowcount))` past 1M (graceful fallback ahead of the L-005 HNSW switch). `build_ivfflat_index()` reads `COUNT(*) WHERE embedding IS NOT NULL` from `reviews_raw` and feeds it to the helper. Logged in [CLAUDE.md frozen-file list](../CLAUDE.md) as a one-line exception (file · reason · B-051 · 2026-05-28).

Verified at the live corpus (formula spot-check, no full re-embed run): rows=30K→lists=30, rows=121K→121, rows=133,503→133 (within rule-of-thumb noise of the 120 shipped manually), rows=1M→1000, rows=4M→2000.

The `ai/semantic_search.py` `SET ivfflat.probes = 11` is sized for the current 120-partition index. If a future re-embed produces an index whose `lists` differ materially (e.g. corpus jumps to ~250K → lists=250 → probes target ≈ 16), update the SET line at the same time as the re-embed runs.

**Verification — TEST | EXPECTED | ACTUAL | PASS/FAIL:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Pre-flight: no active embedder, no in-flight semantic query | safe to `DROP INDEX` without interrupting a live process | `pg_stat_activity` 0 rows; no `generate_embeddings`/`review_embedder` process | PASS |
| Pre-flight: production probes setting | identify the actual GUC value at risk | `ai/semantic_search.py` never SETs `ivfflat.probes` → pgvector default `probes=1` (verified by code grep + `SHOW` failing in a fresh session) | PASS (load-bearing finding) |
| BEFORE baseline — 3 queries × top-5 similarity at lists=30, probes=1 | recorded as ground truth | AC=0.7241/0.7221/0.7154/0.7146/0.7139; rude staff=0.8061/0.7997/0.7956/0.7945/0.7944; dirty bathroom=0.6936/0.6756/0.6736/0.6718/0.6711 | PASS |
| Index rebuild | new index reports `WITH (lists='120')` | `pg_indexes.indexdef` confirms `WITH (lists='120')` | PASS |
| AFTER comparison — same 3 queries × top-5 at lists=120, probes=11 | similarity scores same or better; no regression | top-1 IDENTICAL (0.7241 / 0.8061 / 0.6936); ranks 2–5 same similarity values; some `hotel_id` attributions shuffle among byte-identical `review_text` (DISTINCT ON tie order, expected) | PASS (recall preserved) |
| Hybrid verification — `"complaints about dirty rooms in Goa"` at 993-city + 133K-review scale | `_detect_city → 'goa'`; all 20 returned reviews scope to Goa via `dim_location` | city detected `'goa'`; 20/20 reviews resolve to Goa city (zero bleed-through from other 992 cities); 384-d MiniLM + city-scoped JOIN healthy | PASS |
| `SET ivfflat.probes = 11` is per-connection, not global | new connection without the SET sees default (no leak) | confirmed — GUC is session-scoped; the line lives inside `run()`, runs every call | PASS |
| `scripts/generate_embeddings.py` AST parse + module import | clean — no syntax / import errors | `ast.parse` OK, `py_compile.compile` OK, `importlib` exec_module OK | PASS |
| `_ivfflat_lists()` formula spot-check at representative corpus sizes | floor of 30 for tiny corpora; ≈ rows/1000 in the 1K–1M band; sqrt(rows) past 1M | rows=0/1/1K/29,999/30K → 30; 100K → 100; 121K → 121; 133,503 (live) → 133; 999,999 → 999; 1M → 1000; 1.5M → 1224; 4M → 2000 | PASS |

**Files:** `ai/semantic_search.py` (added `SET ivfflat.probes = 11` block, ~8 lines incl. comment); `datamodel.md` (Stage C2 row + `review_embeddings` section updated with live `lists=120` + `probes=11` + rule-of-thumb); `scripts/generate_embeddings.py` (logged frozen-file exception — `IVFFLAT_LISTS = 30` constant replaced with `_ivfflat_lists(embedded_rowcount)` helper invoked from `build_ivfflat_index()`); `CLAUDE.md` (frozen-file list — exception line logged with B-051 + date); `docs/backlog.md` (this entry). No migration; the IVFFlat index is rebuilt by hand, not by SQL migration.

**Out of scope:** (a) switching to HNSW — owner-tracked under [L-005](#open--limitations-l-) for the >1M-vector regime; (b) `_log_index_notice` rebuild instructions in `scripts/review_embedder.py` — the file still advises `lists=120` for the post-B-030b backlog clear, which matches this fix, so no edit needed; if a future corpus growth changes the target, update there + here together; (c) re-running the index rebuild against the new dynamic-helper formula — the manual `lists=120` already in place is within rule-of-thumb noise of the helper's 133 at the current corpus; not worth re-DROP/CREATE today.

---

### B-052 — README portfolio polish: animated dual-theme SVG hero + intro tightening ✓ DONE

> **Trace.** Phase(s): ops/dev · datamodel: none · data: `docs/assets/pipeline-flow-light.svg`, `docs/assets/pipeline-flow-dark.svg` (new doc assets — animated SVG hero rendered via README `<picture>` element; not data-generation scripts).

**Priority:** Medium — README is the portfolio front door. The prior ASCII-art diagram rendered as a flat code block on GitHub (no animation, no theme-awareness, not visually competitive with the project's actual polish), the engineering-highlights paragraph carried a stale framing sentence, the data-model section quoted a `reviews_raw` row count from the pre-B-030 backfill era (~30K) that drifted ~4× from the live corpus (~133K), and the DuckDB pivot was called out twice (once in the blueprint-vs-repo blockquote on line 17, once in the body) — duplication the docs-conventions hierarchy explicitly discourages.

**What shipped (all in `README.md` + 2 new SVG assets):**

1. **Animated dual-theme SVG hero.** ASCII pipeline diagram replaced with a `<picture>` element that auto-switches by `prefers-color-scheme`:
    ```html
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="./docs/assets/pipeline-flow-dark.svg">
      <img alt="Pipeline data flow" src="./docs/assets/pipeline-flow-light.svg">
    </picture>
    ```
    Both SVGs carry `viewBox="0 0 800 240"` (responsive scaling on narrow viewports), `role="img"` + `aria-label="TravelLens pipeline: booking events stream through Kafka and a Python consumer into a Postgres + S3 dual sink, with an Ollama AI layer serving the Flask dashboard."` (screen-reader accessibility), 3 SMIL `animate*` elements each (animated flow-dots between pipeline stages — render on GitHub web view, which strips `<script>` but preserves declarative SMIL).
2. **Engineering-highlights intro tightened.** The lead sentence under `## Engineering highlights` was pruned of its preamble and now points directly at `docs/capabilities-and-limits.md` for the honest per-feature reliability statement. No content lost — the per-feature reliability claims still live in `capabilities-and-limits.md` (single source per the conventions hierarchy); the intro is now a one-line redirect, not a restatement.
3. **`reviews_raw` count corrected to ~133K.** The Data model paragraph's `(~30K rows)` was stale — it referenced the original Kaggle seed, not the post-B-030 (+ 90,980 history reviews) + stream-emitted corpus that the live DB has carried since 2026-05-25. Updated to `(~133K rows)` to match `SELECT COUNT(*) FROM reviews_raw` and the figure documented in [`datamodel.md`](../datamodel.md) and CLAUDE.md ([B-046 verification table](#b-046--stage-1-dimension-expansion-1k-cities--20k-hotels--100k-customers-additive--done)).
4. **DuckDB aside dropped from the body.** The blueprint-vs-repo blockquote on README line 17 already names DuckDB as one of the three pivots ("Flink streaming, DuckDB exploration, Claude API … dropped DuckDB"); the Documentation-map row for the blueprint on line 72 names it again. A third in-body callout was redundant and removed; the two remaining mentions are the right ones (one in the pivot-context blockquote near the top, one in the docs-map row that links to the blueprint).

**Files:** `README.md` (only edit), `docs/assets/pipeline-flow-light.svg` (new, 4,692 bytes), `docs/assets/pipeline-flow-dark.svg` (new, 4,538 bytes), `docs/backlog.md` (this entry). No code, no script, no migration, no template change. Both SVGs are static assets — no build step, no preprocessor, no dependency added.

**Verification — TEST | EXPECTED | ACTUAL | PASS/FAIL:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Both SVG assets present at the paths the README references | `./docs/assets/pipeline-flow-light.svg` + `./docs/assets/pipeline-flow-dark.svg` exist | both files present (4,692 bytes light + 4,538 bytes dark) | PASS |
| Both SVGs carry animation elements | ≥1 `<animate>` / `<animateMotion>` / `<animateTransform>` per file | 3 animate elements in each | PASS |
| Both SVGs use `viewBox` (responsive scaling on narrow viewports) | `viewBox="0 0 800 240"` present | both `viewBox` declared, 1 match each | PASS |
| Accessibility — `role="img"` + descriptive `aria-label` on both | screen readers read a meaningful description | `role="img"` + ~50-word aria-label naming Kafka / consumer / dual sink / Ollama / Flask on both | PASS |
| README `<picture>` element well-formed (fallback works in browsers without `<picture>` support) | `<source>` for dark + `<img src=…light.svg>` fallback | `<picture>` block at README:27-30 with both `<source>` and `<img>` — `<img>` fallback resolves on legacy browsers | PASS |
| README references `reviews_raw` row count consistently with live DB | `~133K` in the Data model section | line 56: `reviews_raw table holding the embedded text corpus (~133K rows)` — matches `SELECT COUNT(*)` and datamodel.md | PASS |
| DuckDB mentions in README ≤ 2 (blockquote + docs-map row) | exactly 2 mentions, both contextual | `grep DuckDB README.md` returns 2 hits: line 17 (blueprint-pivot blockquote) + line 72 (docs-map blueprint row) | PASS |
| No new files outside `README.md` + `docs/assets/` touched | docs-only change | git status shows `README.md` modified + `docs/assets/pipeline-flow-{light,dark}.svg` untracked + `docs/backlog.md` modified (this entry); no code / scripts / SQL / templates / configs altered | PASS |

**Light/dark theme eyeball-check is owner-side.** Can't render GitHub's prefers-color-scheme theme-switch from the local working tree — the `<picture>` element + theme-swap logic only takes effect once GitHub serves the README under its rendered web view (and toggles based on the viewer's theme setting). Owner should: (a) push the branch, (b) load the README on github.com under the Light theme + confirm the light SVG renders with dots animating, (c) toggle to Dark theme + confirm the dark SVG renders with dots animating, (d) resize the viewport / open on mobile + confirm both SVGs scale cleanly via their `viewBox`.

**Out of scope:** (a) further README compression (Tech stack table → docs link · Design decisions section → blueprint link) — owner-tracked, would be a separate B-number per [B-050 § Out of scope](#b-050--readme-doc-consolidation-documentation-map--runpy-reference--done); (b) animated GIF dashboard preview to replace `dashboard.png` — separate visual-polish item, not part of this consolidation pass; (c) embedding the SVGs inline in the README (data URI) — would bloat the README diff and prevent independent asset versioning; the file-based `<picture>` element is the right shape.

---

### B-053 — Dashboard curation: 11 portfolio widgets pinned across 5 query-shape tiers ✓ DONE

> **Trace.** Phase(s): [Phase 5](phase-5-dashboard.md) · datamodel: `dashboard_widgets` rows (state-only; no schema change) · data: existing `fact_bookings` · `fact_booking_events` · `hotel_master` · `dim_location` · `dim_customer` · `reviews_raw`.

**Priority:** Medium — the dashboard is the second portfolio surface (after the README). Prior to this item it held 13 exploratory widgets pinned during prior sessions, several with typos and rough phrasing ("how many booking record we reviced current month day wise?") that diluted the at-a-glance impression. This curation replaces them with a deliberately-shaped set covering simple aggregations, filtered/grouped queries, complex multi-table joins, live-data routing (B-048), and hybrid semantic+city scoping (B-051).

**Decisions locked at session start (from owner):**

1. Clear all 13 existing widgets before pinning — confirmed via `AskUserQuestion`. Reason: existing prompts were one-off explorations, not deliberate state; mixing curated and exploratory widgets would defeat the portfolio purpose.
2. Pin via the Explore tab path (preview SQL → eyeball → pin → set refresh interval) — exercises the real user-facing flow that a portfolio viewer would.
3. One NL retry per widget; if still wrong, skip and document the phrasings tried. Don't accumulate retry budget into a stubborn-LLM loop.

**Curated widget list — 12 specified, 11 shipped:**

| # | Title (final) | NL prompt that worked | Route | Type | Refresh | Status |
|---:|---|---|---|---|---:|---|
| 1 | Top 10 cities by revenue | `Top 10 cities by revenue` | SQL | bar_chart | 60 min | ✓ pinned (id=1) |
| 2 | Bookings by tourism zone | `total bookings per tourism zone` | SQL | bar_chart | 60 min | ✓ pinned (id=2) — 3 retries (model first interpreted "by zone" as hotel inventory) |
| 3 | Booking sources mix | `Booking sources mix` | SQL | bar_chart | 60 min | ✓ pinned (id=3) |
| 4 | Average nightly rate by star category | `avg nightly rate by star category` | SQL | bar_chart | 60 min | ✓ pinned (id=4) — 1 retry (first attempt rolled chatty) |
| 5 | Cancellation rate by city (top 15) | `Cancellation rate by city, top 15` | SQL | bar_chart | 60 min | ✓ pinned (id=5) — B-048 formula `COUNT(*) FILTER (WHERE is_cancelled) / NULLIF(COUNT(*),0)` exact |
| 6 | Top 10 hotels by revenue in Goa | `top 10 hotel names by revenue in Goa` | SQL | bar_chart | 60 min | ✓ pinned (id=6) — 1 retry (first attempt returned 3-col `hotel_id + name + rev` that would have broken bar_chart) |
| 7 | Revenue by month in 2025 | — | SQL | line_chart | — | **SKIPPED** — date-paradigm bleed; 5 phrasings tried, all variants either invented `b.booking_date` / `d.date_key` / `d.month_number` (B-001 retry caught some but model re-hallucinated on retry) or rolled chatty. Pin once succeeded but froze broken `date_key` SQL → deleted. Known L-013-adjacent ceiling on the dim_date join paradigm. |
| 8 | Live: bookings today | `Live: bookings today` | SQL (live) | stat_card | 5 min | ✓ pinned (id=8) — B-048 live routing exact: `fact_booking_events WHERE event_type='BOOKING' AND event_date=CURRENT_DATE AND source='stream'` |
| 9 | Revenue this month by tourism zone | `sum revenue this month grouped by tourism_zone` | SQL | bar_chart | 60 min | ✓ pinned (id=9) — 2 retries (first used `region` not `tourism_zone`; second invented `b.event_date` on fact_bookings) |
| 10 | Top complaints across reviews | `Top complaints in reviews` | semantic | semantic | manual | ✓ pinned (id=10) |
| 11 | Cleanliness complaints in Goa | `Top complaints about cleanliness in Goa` | semantic + city | semantic | manual | ✓ pinned (id=11) — B-051 hybrid scoping confirmed (city=goa filter applied; 20/20 reviews resolved to Goa) |
| 12 | Repeat customer share by zone | `percentage of bookings from repeat customers by tourism_zone` | SQL | bar_chart | 360 min | ✓ pinned (id=13) — 4 attempts; first pin rolled chatty (cached error state, deleted id=12); second pin rolled clean. Frozen SQL uses `dim_customer.is_repeat_customer` flag; metric is "% of unique customers in zone who are repeat" rather than literally "% of bookings from repeat customers" (semantic nuance documented). |

**Refresh-interval mapping (owner spec → allowed values 0/5/15/30/60/360):**
- `hourly` → 60
- `daily` → 360 (closest available; true 1440 not in the `dashboard_widgets_widget_type_check`-adjacent settings whitelist)
- `60s` → 5 (closest available)
- `manual` → 0

**Process per widget:**

1. `POST /api/query` with the NL prompt; eyeball the returned SQL for correct table (`fact_bookings` vs `fact_booking_events`), correct joins, correct filters.
2. If wrong: refine NL phrasing once and re-preview.
3. If still wrong: skip and document the phrasings tried (see widget 7 row).
4. If right: `POST /api/pin` with `widget_type` + `title`.
5. `POST /api/widget/<id>/settings` to set the refresh interval (`/api/pin` auto-detects but the auto-rules — `hourly`/`live`/`streaming` keyword → 60 min, semantic → 1440 → coerced to 0, else 0 — don't match all owner-specified intervals).

**Verification — TEST | EXPECTED | ACTUAL | PASS/FAIL:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Pre-flight: dashboard_widgets cleared before curation | `SELECT COUNT(*)` returns 0 immediately after `TRUNCATE … RESTART IDENTITY` | 0 | PASS |
| Final pinned-widget count | 10-12 widgets pinned per the curated list | 11 (1 of 12 skipped per the documented retry rule) | PASS |
| All pinned rows have cached results (no error state) | `last_result_json::text` does NOT contain `"type": "error"` for any row | 11/11 rows OK (1 prior chatty-pin attempt deleted, not retained) | PASS |
| `/dashboard` renders all pinned widgets | HTTP 200 + every `widget_id` appears in the HTML | 200, 11/11 `data-widget-id` attributes present | PASS |
| Frozen-SQL refresh path runs cleanly | `POST /api/refresh/<id>` returns no error for spot-checked SQL widgets (1, 5, 6, 8, 9, 13) | 6/6 returned `error: None`; widget 8 ran frozen SQL (cache stale at 5-min interval), others served from cache | PASS |
| B-048 live-data routing exercised | Widget 8's frozen SQL targets `fact_booking_events` with `source='stream'` + `event_date=CURRENT_DATE` | confirmed via DB query of `generated_sql` column | PASS |
| B-051 hybrid scoping exercised | Widget 11 returns reviews filtered to Goa | preview returned 20/20 Goa reviews + summary themed on cleanliness | PASS |
| B-022 cache-then-frozen-SQL contract intact | New widgets have `generated_sql NOT NULL` (SQL) or `NULL` (semantic) | 9/11 have SQL frozen; 2 semantic widgets have NULL as designed | PASS |

**Files:** `docs/backlog.md` (this entry + 2 cross-reference touch-ups), `docs/capabilities-and-limits.md` (one-line note linking to this entry). No code, no script, no migration, no template change. Postgres `dashboard_widgets` state changed (11 new rows after a `TRUNCATE`) — DB state is not git-versioned, so this is the change.

**Out of scope:** (a) closing the date-paradigm bleed for widget 7 — owner-tracked under [L-013](#open--limitations-l-) and the broader "Qwen-7B `fact_bookings` ↔ `dim_date` join" L-class limitation; not a curation problem. (b) Adding a "donut" widget type — `dashboard_widgets_widget_type_check` whitelists `bar_chart / line_chart / stat_card / table / semantic` only; the owner-spec'd donut shapes (widgets 2 + 3) are pinned as bar_chart. (c) Reordering the 11 pinned widgets via `display_order` — left at default 0 (preserves pin order on the grid); owner can drag-and-drop on the live dashboard. (d) Custom refresh-interval whitelisting beyond `{0,5,15,30,60,360}` — would be a separate `dashboard_widgets_widget_type_check`-adjacent edit; the closest-available mapping documented above is sufficient for portfolio purposes.

**Addendum (2026-05-30):** five further widgets pinned in a follow-up curation pass to add chart-type variety (`line_chart` and `table` were unused; `stat_card` was under-used at 1). Final state 16 widgets: bar_chart × 8, stat_card × 4 (was 1), line_chart × 1 (new), table × 1 (new), semantic × 2. New widgets: id=14 daily revenue trend last 90 days (line_chart, 360 min — direct-insert after 2 NL retries hit the same date-paradigm bleed as widget 7), id=15 total revenue this month so far (stat_card, 60 min — direct-insert; NL routed to live stream today-only instead of historical month-to-date), id=16 active hotels this week (stat_card, 60 min — NL pinned, model routed to `fact_booking_events` with `source='stream'` which is a valid live interpretation of "last 7 days"), id=17 cancellations this week (stat_card, 60 min — NL pinned cleanly to `fact_booking_events WHERE event_type='CANCELLATION'`), id=18 top 20 hotels by revenue this month (table, 60 min — direct-insert after 2 NL retries invented `b.event_date` and `h.name`). Same B-053 retry-rule pattern (1-2 NL retries → direct-insert with curated SQL) — date-paradigm bleed is the dominant residual failure mode, same root cause as widget 7. Customer-distribution caveat on id=13 now documented in `capabilities-and-limits.md` §6 and tracked under B-055.

---

### B-054 — Blueprint AI-sell: two animations, reframed §07/§08, "Life before vs. after" block removed, metric-strip reconciliation ✓ DONE

> **Trace.** Phase(s): ops/dev (docs · `index.html` blueprint) · datamodel: none · data: none.

**Priority:** Medium — the blueprint is the deep-dive portfolio surface a recruiter / hospitality buyer lands on after the README. Prior to this item it opened §07 (Text-to-SQL) and §08 (Semantic Search) with technical preambles that read engineer-to-engineer, with no concrete demonstration of how the AI helps a hotel team make a decision; §01 carried a "Life before vs. after TravelLens" comparison block whose framing duplicated the value-prop work done by the new animations and ran stale against the live corpus (its semantic-search row still quoted `30K reviews`); the Executive Summary's metric strip quoted a "15 schema tables" count that was true at the §05 curated-star-schema view but stale against the live DB (22 tables since the post-base migrations 003–014), and a "30K+ Real reviews" tile that was stale against the post-B-030 corpus (~133K). No throughput tile existed for the live producer.

> **Scope drift note.** This entry was first written assuming three animations + a new "Questions Hotels Ask" section between §02 and §03. The owner subsequently dropped the third animation placement; the "Questions Hotels Ask" `<section id="questions">` and the `.tl-anim3` / `tl-a3-expand` CSS + reduced-motion line were removed in the same session. Final shipped scope is two animations (§07 + §08) and the §01 "Life before vs. after" removal.

**What shipped (all in `index.html` — single file; no code, no script, no migration touched):**

1. **Two namespaced CSS animations + JS cyclers.** A new `<style>` block before the main `</style>` (just above `<head>` close) introduces `.tl-anim1` (Text-to-SQL personas) and `.tl-anim2` (semantic spotlight) along with their child classes (`.tl-ex`, `.tl-layer-*`, `.tl-sql-line`, `.tl-bar*`, `.tl-hbar-*`, `.tl-search-*`, `.tl-review-card`, etc.) and 8 keyframes (`@keyframes tl-blink`, `tl-line-in`, `tl-bar-in`, `tl-hbar-in`, `tl-type-q`, `tl-card-match`, `tl-card-noma`, `tl-note-in`). All prefixes are `tl-` to namespace cleanly against the existing blueprint CSS — the existing blueprint defines its own `@keyframes blink` (border-pulse on a different selector) and a separate batch of `tl-pin-jump` / `tl-shadow` / `tl-wave*` for the hero, none of which collide. The shared syntax-highlighting classes `.kw` / `.str` / `.fn` / `.num` / `.cmt` (defined once at `index.html:70`) are reused inside the SQL panels — same colour palette, harmonious with the existing "Generated SQL" code blocks; no duplicate definition. Two JS cyclers (added before `</body>`) are each scoped to `document.querySelector('.tl-anim1')` / `.tl-anim2` so they only bind to the new elements; `prefers-reduced-motion: reduce` short-circuits each cycler to render the first example statically and the reduced-motion `@media` block freezes all CSS animations (so layouts stay visible, just no motion).
2. **§01 "Life before vs. after TravelLens" comparison block removed.** Located between the "Why not just build a Power BI dashboard?" callout and the "Questions TravelLens answers in natural language" sample-query strip. Both the mono uppercase header `<div>` (with its toggle/refresh SVG icon) and the 2-column comparison grid below it (4 ✗ / ✓ row pairs covering revenue-manager occupancy, GM review reading, tourism-board reports, and analyst dashboard fragility) were removed. The surrounding flow — "Why not Power BI" → "Questions TravelLens answers in natural language" — reads cleanly without the block.
3. **§07 (Text-to-SQL Engine) reframed business-first, body trimmed ~30%.** The opening preamble was replaced with a 45-word lead about a revenue manager who types a question and gets a chart; animation 1 (`.tl-anim1`) follows immediately, cycling three personas (Revenue / Ops / Marketing) × four phases (q → sql → chart → 1s pause) with three different SQL shapes (top-N · filter+ratio · group+percent) and three different chart shapes (vertical bars · horizontal bars with %s · horizontal bars with %s). The 80/20 SQL-vs-Semantic split table was removed (the animation does this job visually). The "Try this — sample queries" UI mockup with six query cards was removed (the animation now shows three of those same query shapes inline with their generated SQL). The two original "Generated SQL" example blocks were trimmed to one (kept the `ref_price_tiers` budget-resolution example as the more interesting one). The system-prompt callout was condensed from a 5-bullet enumeration to one paragraph naming the four prompt-input categories (live schema · India context · column-location rules · SELECT-only) and the post-generation `sqlparse` + retry pipeline, with a one-line link to `docs/capabilities-and-limits.md` for honest accuracy and a short closer about `python -m ai.main "..."`.
4. **§08 (Review Intelligence) reframed business-first, body trimmed ~30%.** Opening preamble was replaced with a 42-word lead about an ops head who types `"dirty room"` and gets reviews mentioning mildew / stains / grime — meaning, not string matches. Animation 2 (`.tl-anim2`) cycles three search themes (`rude staff` / `dirty room` / `amazing view`) with synonym-callouts on matching cards. The existing "Why not just full-text search?" comparison table was kept (the prompt explicitly retained it). The four-step pipeline strip (Detect → Embed → Filter+Search → Summarise) was removed (animation visualises the embed + match conceptually; routing keyword detection is now mentioned in §07's closing line). A single paragraph now carries the technical content: `all-MiniLM-L6-v2` 384-dim vector + pgvector cosine via `<=>` + IVFFlat with the B-051 rule of thumb (`lists ≈ rows ÷ 1000`, `probes ≈ √lists` → `lists = 120`, `probes = 11` at the current ~133K corpus). The "Real data — not simulated" callout was condensed to a single sentence and updated to call out the 30K Kaggle seed + ~91K B-030 history backfill = ~133K live corpus.
5. **Metric strip + supporting prose reconciled against the live DB.** "Schema tables" tile 15 → 22 (verified `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'` returns 22; same value derived from `db/schema.sql` 14 + migrations 003 / 007 / 008 (×2) / 009 (×2) / 013 / 014 minus the dropped `quarantine_daily_summary`). "Real reviews" tile 30K+ → 133K. New 6th tile added — `~20 Peak evt/sec @ 19 IST` — derived from CLAUDE.md's B-047 entry (`BASE_RATE(10) × DIURNAL_HOUR_MULT[19=2.05] × rate_multiplier(1) ≈ 20.5 evt/s`). The 5-column grid already used `repeat(auto-fit, minmax(130px, 1fr))`, so the 6-tile row wraps cleanly with no grid edit. Matching prose updates: the "What it is" card's `15-table star schema warehouse` → `22-table warehouse on a star-schema core`; `30K+ review JSONs` → `~133K review JSONs`; the terminal block's `▶ s3-reviews 30,000 reviews indexed` → `133K reviews indexed`.

**Files:** `index.html` (only file edited — the prior session inserted the namespaced CSS / §02 strip + prose updates / two reframes / cyclers; this session removed the §01 "Life before vs. after" block, the "Questions Hotels Ask" section HTML, the `.tl-anim3` CSS rules, the `tl-a3-expand` keyframe, and the anim3 reduced-motion line); `docs/backlog.md` (this entry + status-strip line + DONE-list one-liner + Completed table row — 4 cross-reference touch-ups, all updated in-place to reflect the dropped scope). No code, no script, no migration, no template change.

**Verification — TEST | EXPECTED | ACTUAL | PASS/FAIL:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Live table count matches blueprint metric strip | Postgres `information_schema.tables` count == strip "Schema tables" tile | DB returns 22 (one query); strip tile reads `22` | PASS |
| Reviews_raw count reconciled to live corpus | tile "Real reviews" reads `133K` | strip tile reads `133K`; matching "what it is" prose reads `~133K review JSONs`; terminal block reads `133K reviews indexed` | PASS |
| 6th throughput tile present | tile reads `~20 Peak evt/sec @ 19 IST`, matches B-047 formula | strip carries 6 tiles; 6th has value `~20` + sublabel `PEAK EVT/SEC @ 19 IST`; computed `10 × 2.05 × 1 ≈ 20.5` matches CLAUDE.md B-047 | PASS |
| Grid auto-fits 6 tiles | no CSS grid edit needed | strip CSS unchanged — `repeat(auto-fit,minmax(130px,1fr))` accommodates 6 tiles natively | PASS |
| §01 "Life before vs. after" block gone | grep `index.html` for that string returns 0 matches; surrounding flow clean | 0 matches; "Why not Power BI" callout flows directly into "Questions TravelLens answers in natural language" strip | PASS |
| §07 / §08 trim target ~30% | each section ~30% shorter than pre-edit | §07: 80/20 table removed (~12 lines) · "Try this" UI mockup removed (~60 lines) · 2 SQL examples → 1 (~12 lines saved) · system-prompt callout 1 paragraph (~6 lines saved); §08: 4-step strip removed (~6 lines) · 4-paragraph technical body → 1 paragraph · callout 4 lines → 1; both sections net ~30–40% shorter even after animation added | PASS |
| Animation namespacing — no class/keyframe collision | only `blink` would have collided; renamed to `tl-blink` | grep shows existing blueprint defines `@keyframes blink` for border-pulse (line 81) and `@keyframes tl-pin-jump/shadow/wave/bird*/glow` for hero (lines 253-259); B-054 adds `tl-blink` + 7 other `tl-*` keyframes — zero name collisions | PASS |
| JS cyclers correctly scoped | only `.tl-anim1` and `.tl-anim2` activated | both `(function(){var root = document.querySelector('.tl-anim*')...})()` IIFEs bail with `if (!root) return;` if the element is missing; each cycler binds to exactly one element | PASS |
| `prefers-reduced-motion` honored | animations stop, content still legible | reduced-motion `@media` block freezes all `tl-*` animations + JS cyclers short-circuit to render first example statically + first example's `data-phase` is set to `chart` for anim1 so the bar chart stays visible | PASS |
| Anim3 leftovers fully removed | grep for `tl-anim3`, `tl-a3-expand`, `Questions Hotels Ask`, `id="questions"` returns 0 matches | 0 matches across all four patterns; no orphaned CSS rules, no orphaned HTML, no orphaned keyframe, no orphaned reduced-motion line | PASS |
| `index.html` parses as valid HTML | no unbalanced tags / no broken anchors | nav-item list (12 entries 01-12) unchanged; section-num sequence 01-12 preserved (no nav item ever pointed to `#questions` — anim3 placement used a "For Hotels" tag chip instead, so removal needed no nav edit) | PASS |

**Out of scope (deferred):** (a) **README `14 tables` mention** — README is touched in B-052; updating it to 22 (or to a brief `14 base + N migration` framing) belongs to a separate B-number to keep blueprint and README changes auditable as independent passes. (b) **§05 schema heading and body** — the heading still reads "Data Schema — 15 Tables" and its tabbed explorer documents exactly 15 tables (3 fact · 5 dim · 4 ref · 3 operational); these are the curated star-schema core, and extending the explorer to cover the 7 additional migration-added tables (`fact_booking_events` / `sim_open_bookings` / `fact_booking_lifecycle` / `gold_watermark` / `quarantine_hourly_summary` / `sim_daily_counter` / `pipeline_metrics` + `dashboard_widgets`) is a separate, larger edit — out of scope for this AI-sell pass. The mismatch between the §02 metric strip (22) and §05 heading (15) is a known internal inconsistency to be resolved when §05 is extended. (c) **Animation 1 cap on shown bars** — the revenue example renders 10 bars; on viewports < 480px the bar-labels (vertical rotated text) may compress. The reduced-motion @media block keeps the chart visible but does not re-flow the bar count; acceptable for the portfolio viewer's likely viewport (desktop/tablet). (d) **Animation pacing accessibility** — anim1's full cycle is 9.6s per persona × 3 = ~29s; anim2's is 8.5s × 3 = ~26s. No "pause / play" control was added (matches the rest of the blueprint, which has multiple auto-playing animations without controls); a future accessibility pass could add a single global control if needed. (e) **Animation 3 placement (Questions Hotels Ask gallery)** — owner-removed mid-session; not re-tracked. If a future session wants a between-§02-and-§03 break, it would land as a new B-number with a different rationale, not a revival of this scope.

2026-05-30 follow-up: §08 .tl-anim2 container fixed to height 420px desktop / 760px mobile with position:absolute children (resolves layout pulse during inter-example pause). "Why a single HTML file — not Streamlit or Tableau?" subsection removed per owner.

---

### B-055 — Realistic customer distribution in bookings  ·  OPEN

> **Trace.** Phase(s): [Phase 1](phase-1-postgres.md) (data-gen) · datamodel: `fact_bookings` + `fact_booking_events` + possibly `dim_customer` · data-gen: `scripts/generate_datasets.py` + `generate_stage2.py` + `scripts/kafka_event_producer.py` (customer-sampling function).

**Problem:** `fact_bookings` samples `customer_id` uniformly from a 100K `dim_customer` pool against 1M bookings → ~10 bookings/customer avg → every customer is structurally a "repeat". Surfaces most visibly in the "repeat-customer-by-zone" widget (B-053 id=13) showing 40%+ when real-hospitality typical is 20-25%. Also affects any LTV / cohort / top-customer / loyalty-segment query.

**Fix plan:**

- Generate `fact_bookings` (and `fact_booking_events`) with a weighted long-tail customer sampler. Target distribution (real-hospitality match):
  - 70% of customers → 1 booking
  - 20% → 2-3 bookings
  - 8% → 4-7 bookings
  - 2% → 8+ bookings (loyalty)
- Decide at implementation: grow `dim_customer` to ~300-500K (more one-timers) OR keep 100K with many low-count customers. Pick based on join-perf benchmark.
- Apply the same distribution to the live producer (`scripts/kafka_event_producer.py`) so historical and stream stay consistent.
- After regeneration, recompute the customer-touching widgets pinned in B-053 — expect repeat-by-zone to land ~25%.

**Out of scope:**

- Customer attributes (age / gender / etc.) — separate item if ever needed.
- Review-to-customer linkage (reviews aren't linked to customers in this schema).
- Widgets unaffected by customer behavior (revenue / occupancy / cancellation / source mix / semantic review search).

---

### B-056 — Router: operation detection + hybrid-aggregation path  ·  OPEN

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) (AI layer) · datamodel: none · data: none.

**Problem:** `ai/query_router.py` classifies on TOPIC only (keyword match on review / complaint / staff / cleanliness / etc.). It can't distinguish review-RETRIEVAL ("what do guests say about cleanliness") from review-ANALYTICS ("top 5 hotels with most cleanliness complaints"). Both route to the semantic path, which has no aggregation primitive — it returns individual reviews, not COUNT-by-hotel.

Surfaces as: "top 5 hotels with most cleanliness complaints" returns 5★ "high standards of cleanliness" reviews (semantic match on topic, no polarity / no aggregation). Adding more semantic-trigger keywords does NOT fix this — keyword routing has no way to express "aggregate over the matches."

**Fix plan:**

1. Add operation detection to `ai/query_router.py`: structural pattern match on "top N" / "most" / "count" / "by X" / "rate" / "share" / "average" / etc. Returns `'analytics'` or `'retrieval'`. Pattern-based, NOT keyword-based — generalizes across topic vocabularies without per-domain additions.
2. Extend the classifier output from `{sql, semantic}` to `{sql, semantic, hybrid_aggregation}`:
   - topic=reviews + operation=analytics → `hybrid_aggregation`
   - topic=reviews + operation=retrieval → `semantic` (unchanged)
   - topic=bookings (any operation) → `sql` (unchanged)
3. New path `ai/hybrid_aggregator.py`:
   - Run `semantic_search` on query → get candidate `review_id`s (top 200, say).
   - Build SQL: `SELECT hotel_field, COUNT(*) FROM reviews_raw JOIN hotel_master USING(hotel_id) WHERE review_id = ANY(:candidate_ids) GROUP BY hotel_field ORDER BY count DESC LIMIT N`.
   - Return aggregated rows in the `{labels, values}` shape so bar_chart / table widgets render directly.
4. Frozen-file exception: `ai/query_router.py` is on the frozen list. Logged exception with rationale (keyword-only routing structurally incomplete).
5. Tests: extend `tests/test_hybrid_queries.py` with operation-detection cases and end-to-end hybrid-aggregation cases.

Once shipped, the blocked widget "Top 5 hotels by cleanliness complaint count" can be pinned and will return a real ranked hotel list — no phrasing tricks.

**Files:** `ai/query_router.py` (frozen-file exception) + `ai/main.py` + new `ai/hybrid_aggregator.py` + `tests/test_hybrid_queries.py`.

**Out of scope:**

- LLM-based routing (a separate alternative, more complex; keep on the table as B-057 if pattern-based detection proves insufficient).
- B-026 sentiment-classification per review (orthogonal — would improve PRECISION of the semantic match but doesn't solve the aggregation gap).
- Per-domain query expansion (food / amenity / location verticals).

---

### B-064 — Accurate topic/aspect ranking (top-N hotels by complaint type)  ·  PARKED (approach TBD)

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) — query-routing / text-to-SQL · datamodel: new `review_aspects` table (PROPOSED, not built) · data-gen: `scripts/review_generator.py` / `scripts/kafka_event_producer.py` (only if the generator-hybrid engine is chosen).

Accurate topic/aspect ranking (top-N hotels by complaint type) — PARKED, approach TBD.

**PROBLEM:** Queries like "top 5 hotels with most cleanliness complaints" route to the semantic path and return themes + sample reviews (wrong shape). They can't GROUP BY hotel + COUNT + rank by a specific (topic, sentiment). Observed repeatedly in the dashboard.

**CHOSEN DIRECTION (locked):** Aspect-based sentiment — tag each review with (topic, aspect_sentiment) PAIRS (multi-label; per-aspect sentiment so "lift fine, room dirty" doesn't pollute counts), then rank with deterministic SQL — NOT the LLM-SQL path.

**DESIGN ALREADY SETTLED (ready to build once an engine is chosen):**

- Taxonomy: cleanliness, staff_service, location, room_comfort, value_price, food_breakfast, facilities, noise, maintenance, booking_checkin, + other. Boundary rules: dirty/stained/smelly→cleanliness; AC/hot-water/kettle/plumbing/power→maintenance; cancellation/refund/checkout-charges→booking_checkin; lack-of-facility→facilities:negative; wifi/pool/parking/lift presence→facilities.
- Routing seams: (a) third label "aspect_rank" in query_router.route(), detected BEFORE the semantic keyword match wins; (b) dispatch branch in main.py to a new ai/aspect_ranking.py emitting deterministic ranking SQL (filter topic+sentiment, GROUP BY hotel, COUNT, ORDER, LIMIT).
- Proposed schema (migration when built): review_aspects(review_id uuid FK, topic varchar(20) CHECK in taxonomy, aspect_sentiment varchar(8), hotel_id varchar(20) FK [denormalized for ranking join], confidence numeric(4,3), model_version varchar(40), tagged_at timestamptz), PK(review_id, topic), partial index on (topic, aspect_sentiment, hotel_id) WHERE aspect_sentiment <> 'neutral'. Neutrals stored but dropped before counting. model_version enables non-destructive re-tagging.

**WHAT'S BLOCKING (why parked):** no tagging-engine / processing-load story is clearly good enough yet. Engines evaluated (read-only experiments; frozen v4 prompt + strict-JSON validator; temp 0; ~50-review samples; owner-eyeballed):

- gemma3:12b — ~90% (best accuracy) but ~10GB overflows 8GB VRAM, ~9.3s/review, not batchable, ~14-day local backfill.
- qwen2.5:7b-instruct — ~85% (borderline), fits GPU (~4.9GB), batchable (~3-day local backfill); a deterministic normalizer (drop neutrals / force kettle→maintenance / block facilities:negative+positive-adjective) pushes effective >85%. Local sweet-spot.
- qwen2.5-coder:7b (the resident SQL model) — ~76%; rules out reusing the resident model.
- gemma3:4b ~67%, llama3.2:3b ~76% — below bar.
- Cloud: DeepInfra hosts gemma-3-12B at ~$0.04/M → backfill ~$7 in hours, stream pennies, zero local VRAM. Security reviewed safe (outbound API only, public data, key in .env).
- Generator-hybrid: review_generator.py composes ~35% of reviews from a known REASON_BANK (taggable for free, ground-truth) but ~40% is organic Kaggle text + ~25% blended (both need the LLM) → cuts LLM load ~35%, not elimination; adds generator-coupling complexity (must annotate ~56 snippets with topics + keep them in sync).

**OPEN DECISION before implementing:** which engine/architecture (local qwen-instruct vs DeepInfra gemma-12B vs generator-hybrid + LLM), and whether ~85–90% tag accuracy is acceptable for ranking. Implementation stages once decided: review_aspects migration + tagger (sample-gated) → backfill → routing fix + ranking SQL handler → live stream tagging.

**RELATED:** L-018 (keyword polarity detector misses implicit complaints) — this feature would supersede the semantic path for complaint-ranking queries.

---

### B-058 — Text-to-SQL accuracy eval harness (`ai/eval/`) ✓ DONE

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) (AI layer) · datamodel: none · data: none.

**What:** An on-demand accuracy **measurement** tool for the Text-to-SQL path — explicitly **NOT** a
pytest regression suite and **NOT** part of `pytest tests/`. It runs a fixed fixture of
natural-language questions through `ai.main.answer()` against the live DB + Ollama, scores each by
**EXECUTION MATCH** against an owner-verified reference query, and flags the two known
confident-wrong failure modes (L-011 missing `DISTINCT`, L-013 missing cancellation filter). Run:
`python -m ai.eval.run_eval` (`--runs N` / `--only sql` / `--id <question_id>`).

**Why execution match, not SQL-string match:** many correct SQLs exist per question and the dataset
is regenerable, so comparing SQL text or hardcoding expected row values both break. The harness runs
the model's SQL AND the reference SQL against live data and compares **normalized result sets**, so
the metric survives a dataset regenerate.

**Hard boundary:** the fixture (`ai/eval/eval_questions.py`) is **TEST DATA, not prompt content**. It
is never read by, written into, or referenced from `ai/prompts/text_to_sql_system.txt`. The harness
never feeds examples back into the model — it is not a few-shot patch and must not become one.

**Design highlights (locked):**

- **N runs/question (default 3)** — Ollama is non-deterministic and L-013 is intermittent; one run
  passes/fails by luck. Per-question pass-rate + aggregate.
- **Flags recorded INDEPENDENTLY of execution match** — a result can match by coincidence while the
  SQL is structurally wrong. Proven live: `avg_nights_by_segment` had `cancellation_filter_missing×2`
  yet one filterless run coincidentally matched the reference — exec-match alone would have hidden it.
- **`is_rate_query` defined narrowly** — `FILTER (WHERE ... is_cancelled)`, `100.0 *`, `/ NULLIF(COUNT`,
  or a rate/ratio/share/percent alias. A `/ 1e7` crore unit-conversion is NOT a rate, so the
  cancellation-filter flag is not suppressed on the exact L-013 revenue cases it must catch.
- **Half-up rounding** to 2 dp (`Decimal(str(v)).quantize(..., ROUND_HALF_UP)`) to match Postgres
  `ROUND()`; positional column comparison (names ignored, count enforced); multiset by default,
  ordered list when the fixture sets `ordered: true`.
- **Headline metric counts errors as failures** — `execution accuracy = passed / ALL runs` (a
  both-attempts error or a misroute is a non-pass, not a free exclusion). A secondary
  `among scored runs` line and a per-subtype error breakdown
  (`validator_rejection` / `model_sql_error` / `ollama_unreachable` / `other`) are reported alongside,
  so a B-059 validator false-positive is visibly distinct from a genuine model failure.
- **Honest denominator** — exec-match is excluded from the *among-scored* accuracy % (and reported
  separately) when it is meaningless rather than failed: INDETERMINATE (result set hit `MAX_ROWS=100`
  → truncated subset) or PROBE (a `scored: False` entry whose answer is inherently under-determined,
  e.g. "5 of 1,418 valid rows" — exists for its L-011 flag, not for exec-match).
- **Skip cleanly** — if Postgres or Ollama is unreachable, prints a clear SKIP and exits 2; never
  reports a fake 100%. Exit codes: 0 ran · 2 clean skip · other nonzero crashed.

**Files (all additive):** `ai/eval/__init__.py`, `ai/eval/eval_questions.py` (11-question fixture),
`ai/eval/run_eval.py` (runner), `ai/eval/results/` (gitignored). Plus `.gitignore`
(`ai/eval/results/`), `CLAUDE.md` (repo-layout tree), `docs/phase-4-ai-layer.md` (BUILD HISTORY
entry). No frozen file touched; no schema change (datamodel.md untouched). Reference SQLs lifted
from owner-verified origins (`tests/test_validate_columns.py`, `docs/phase-4-ai-layer.md` E1 +
acceptance, prompt OUTPUT RULES).

**Re-run after owner-requested fixes (11 questions × 3 runs = 33, default `--runs 3`)** — headline
now counts errors/misroutes as failures; fixture reworked (`top10_cities_by_hotel_count` replaces the
permanently cap-excluded `how many hotels per city`; `list 5 customers named R` drops the "unique"
that made `distinct_missing` vacuous); errors tagged by subtype:

```
TEST                               PATH  RUNS PASS/TOTAL  EXEC-MATCH  FLAGS
top5_cities_by_revenue             sql      3        0/3        0.0%  cancellation_filter_missingx3
cancellation_rate_by_segment       sql      3        2/3       66.7%  -
adr_5star_goa                      sql      3        0/3        0.0%  cancellation_filter_missingx1, err:otherx1
revenue_by_month_2025              sql      3        0/3         n/a  err:model_sql_errorx1, err:otherx1, err:validator_rejectionx1
top10_cities_by_hotel_count        sql      3        3/3      100.0%  -
customers_per_state                sql      3        1/3      100.0%  err:validator_rejectionx2
hotels_opened_per_year             sql      3        3/3      100.0%  -
list_5_customers_named_r           sql      3        0/3       probe  err:validator_rejectionx2
bookings_by_segment                sql      3        0/3        0.0%  -
revenue_by_star_category           sql      3        1/3       33.3%  cancellation_filter_missingx2
avg_nights_by_segment              sql      3        1/3       33.3%  cancellation_filter_missingx2
```

| Metric | Value |
|---|---|
| execution accuracy (headline — errors & misroutes count as fails) | **33.3%** (11/33 of ALL runs) |
| among scored runs only | 45.8% (11/24; excludes cap=0, probe=1, error=8, misroute=0) |
| `cancellation_filter_missing` (L-013) fired | 8 |
| `distinct_missing` (L-011) fired | 0 (the model still emitted `DISTINCT` on its one successful `list_5` run; its other 2 runs errored before a flag could be computed) |
| error rate | 24.2% (8/33) — `validator_rejection` 5 · `model_sql_error` 1 · `other` 2 |
| misroute rate | 0.0% (0/33) |
| indeterminate (cap) / probe runs | 0 / 1 (excluded from both accuracy denominators except the headline total) |

Per-question reading: `top10_cities_by_hotel_count` / `hotels_opened_per_year` → 3/3 ·
`top5_cities_by_revenue` → 0/3 (filter dropped all 3 runs — flagged ×3) · `adr_5star_goa` → 0/3
(filter dropped + the kept-filter run emits `ROUND(...,2)` vs the prompt-mandated ADR `,0`, line 315 —
a genuine format miss, reference left at `,0` per design) · `revenue_by_month_2025` → 0/3 all errored
(date-paradigm bleed → `booking_date`/`date_key`/`l.month`; the expected low signal) ·
`customers_per_state` → 1/3 with `validator_rejection×2` (the B-059 false-positive). The 5
`validator_rejection` errors are visibly separated from the 1 genuine `model_sql_error`.

**Acceptance / quality-gate:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Harness runs end-to-end against live DB + Ollama | prints table + aggregate, writes results file, exit 0 | exit 0; `ai/eval/results/eval_*.txt` written | PASS |
| Headline counts errors as failures | accuracy = passed / ALL runs | 11/33 = 33.3% headline; 45.8% among-scored shown separately | PASS |
| Error subtype tagging | validator-rejection vs model-SQL vs Ollama separated | breakdown: `validator_rejection` 5 · `model_sql_error` 1 · `other` 2 | PASS |
| Skip-clean when infra down (failure mode) | clear SKIP, exit 2, no fake 100% | `check_postgres`/`check_ollama` short-circuit to exit 2 | PASS (design-verified) |
| Flag independent of exec-match | flag can fire on a coincidentally-matching run | `avg_nights_by_segment` flag×2 with a 1/3 match; `top5` flag×3 at 0/3 | PASS |
| `is_rate_query` doesn't swallow `/1e7` | crore conversion still flagged when filter missing | `top5` (×3) / `revenue_by_star_category` (×2) crore cases flagged | PASS |
| `cancellation_rate_by_segment` NOT flagged | rate query exempt from the filter flag | 2/3, no flag (rate query correctly exempt) | PASS |
| Bounded rework gradable | `top10_cities_by_hotel_count` scores, no cap | 3/3, no `n/a (cap)` | PASS |
| Probe excluded | under-determined L-011 question not scored | `list_5_customers_named_r` → probe, excluded | PASS |
| Fixture never reaches the prompt (hard boundary) | no code path injects the fixture into the prompt | `ai/prompts/` has zero references to the fixture; `ai/eval/` never opens/imports the prompt file (only docstring prose names it to document the boundary) | PASS |
| AST parse-check both modules | parse OK | `ast.parse` OK on `run_eval.py` + `eval_questions.py` | PASS |

**Out of scope:** semantic-relevance grading (needs a labeled set — documented follow-on); a lint
module to consume these flags (this harness ships first and will measure it later). Forward-compat:
when B-056 lands its `hybrid_aggregation` path, do NOT add review-analytics questions to THIS
fixture — they belong to a different path; the current `path != 'sql' → misroute` rule is correct
as-is.

**Bonus observation (out of scope — opened as [B-059](#b-059--_validate_columns-false-positive-on-select-alias-reused-in-order-by--done), since resolved):**
the harness surfaced a `_validate_columns` (B-003) false-positive — a `SELECT` alias referenced in
`ORDER BY` on a single-table query (e.g. `... AS customer_count ... ORDER BY customer_count`) is
rejected as a hallucinated bare column, turning otherwise-valid model SQL into a both-attempts error
(seen on `customers_per_state` runs 1 & 3). Fix is its own session, not B-058.

---

### B-059 — `_validate_columns` false-positive on SELECT alias reused in ORDER BY · DONE

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) (AI layer) · datamodel: none · data: none.

**Symptom:** `ai/text_to_sql._validate_columns` rejects valid model SQL when a `SELECT`-list output
alias is referenced in the `ORDER BY` of a single-table query. Example:

```sql
SELECT home_state, COUNT(*) AS customer_count
FROM dim_customer
GROUP BY home_state
ORDER BY customer_count DESC;   -- customer_count is a SELECT alias, NOT a column
```

The validator's single-table bare-reference branch treats `customer_count` (the `ORDER BY` term) as a
bare column on `dim_customer`, finds no such column, and raises
`ValueError("Column 'customer_count' does not exist on table 'dim_customer' …")`. Because the same
hallucination-looking SQL is produced again on the B-001 retry, the query becomes a **both-attempts
error** — even though Postgres would have run it fine (SQL permits an output alias in `ORDER BY`).

**Repro:** the B-058 baseline eval run — `customers_per_state` runs 1 & 3 both errored this way
(`... COUNT(*) AS customer_count ... ORDER BY customer_count` and the `num_customers` variant). The
eval harness tags these as `err:validator_rejection`, separating them from genuine model failures.

**Family:** B-003a-adjacent, but the opposite polarity — B-003a is a known false-NEGATIVE (a bare
`WHERE` ref slips past); this is a false-POSITIVE (a valid `ORDER BY` alias is wrongly rejected).
A false-positive is the worse class: it silently breaks queries that would otherwise have run, which
is exactly the property `_validate_columns` was built to avoid.

**Fix shipped (this session — `ai/text_to_sql.py`, B-059):**

1. New helper `_collect_select_aliases(stmt)` collects the top-level `SELECT`-list output aliases
   (sqlparse `Identifier.get_alias()` on each projection identifier, lowercased), scoped to the
   projection only (tokens between the leading `SELECT` and the first `FROM`/`JOIN`) — consistent
   with the validator's existing conservative top-level-only posture (CTEs/subqueries skipped).
2. In `_validate_columns`, the single-table bare-reference loop now `continue`s past any bare ref
   whose name is in that alias set. Qualified refs (`alias.col`) are unaffected — an output alias is
   never qualified. The alias `customer_count` in `ORDER BY` / `GROUP BY` / `HAVING` is therefore
   recognised as a valid output reference, not a hallucinated column.
3. **Provably no new false-negative:** a bare ref matching a `SELECT` alias *is* a valid reference to
   that alias, never a hallucination; a genuinely hallucinated `ORDER BY` column that is NOT an alias
   still falls through and is rejected.

**Tests added — `tests/test_validate_columns.py`** (CLAUDE.md rule: undocumented tests get skipped):
- `test_b059_single_table_alias_in_order_by_not_rejected` — the exact repro (alias in `ORDER BY` on
  one table) must NOT raise.
- `test_b059_single_table_hallucinated_order_by_still_rejected` — guard: a genuine hallucination
  (`SELECT city FROM dim_location ORDER BY made_up_col`) on a single-table query STILL raises.
- Full suite: 19 passed, B-003a stays `xfail` (this fix does not touch the WHERE/Comparison gap).

**Blast radius:** `_validate_columns` runs on the live generate-then-run path (`run()`), NOT on the
frozen-SQL refresh path (`run_stored_sql` uses `_validate_sql` only) — so only the Explore live path
is affected; pinned/refresh widgets are untouched.

**Re-run verification (`python -m ai.eval.run_eval`) — the B-059 case is gone end-to-end:**
- **`customers_per_state` (the B-059 question):** B-058 baseline `1/3` pass with
  `err:validator_rejection×2` (the alias false-positive) → AFTER `3/3` pass, **0 validator_rejection**.
  This is the direct end-to-end proof — the exact query that became a both-attempts error now runs.
- **Controlled A/B** on the same question (5 runs, fix temporarily disabled vs enabled, all else
  identical): `validator_rejection` **1 → 0**.
- **Full-fixture aggregate is a coincidental 5 → 5 — read it carefully, do NOT report it as "no
  change".** The composition flipped completely: the 2 baseline rejections were the
  `customers_per_state` alias false-positive (now eliminated); all 5 AFTER-run rejections are
  **genuine B-003 catches** — hallucinated columns the validator *should* reject: qualified refs
  `b.booking_date` / `c.segment` / `l.month` / `dcs.customer_segment` (×4 of the 5) plus a bare
  `customer_name` that genuinely does not exist on `dim_customer` (`SELECT DISTINCT customer_name
  FROM dim_customer …`, not an alias). None is the B-059 pattern. Their run-to-run count tracks
  model non-determinism — the date-paradigm-bleed question `revenue_by_month_2025` (a known
  L-013-adjacent hard case) alone contributed 2 this run. The aggregate count is therefore not the
  right B-059 metric; the per-question `customers_per_state` result above is. AFTER headline
  execution accuracy 42.4% (14/33), among-scored 58.3% (14/24).

**Out of scope:** the broader B-003a false-negative (bare `WHERE` ref on single-table — already
tracked, `xfail` in the suite); any change to the B-001 retry loop.

---

### B-060 — post-generation cancellation-filter lint + corrective retry (mitigates L-013) · DONE

> **Trace.** Phase(s): [Phase 4](phase-4-ai-layer.md) (AI layer) · datamodel: none · data: none.

**Problem:** `run()`'s retry (B-001/B-003) fires **only on an exception** — it does nothing for SQL
that executes cleanly but is wrong. The B-058 eval baseline proved the most frequent such case: a
bare grouped aggregate over `fact_bookings` that silently drops `WHERE NOT is_cancelled`
(`cancellation_filter_missing` fired repeatedly) — valid SQL, wrong numbers, no error. This is
**L-013**, and the read-path safeguard (B-022 pin-time freeze) doesn't help the live Explore path,
which runs unverified model SQL on every question.

**What shipped (`ai/text_to_sql.py` only — no prompt-file edit, no per-query few-shot):**

- `lint_cancellation_filter_missing(sql, user_query) -> bool` — a deterministic post-execution lint
  that **mirrors `ai/eval/run_eval.py`'s `flag_cancellation_filter_missing`** so the harness and the
  runtime lint agree on what "missing filter" means. Pure regex (same as `run_eval`). Fires only when
  ALL hold: (1) `fact_bookings` in FROM/JOIN; (2) NOT a rate/ratio query (`_lint_is_rate_query`,
  verbatim from `run_eval` — `FILTER(WHERE…is_cancelled)`, `100.0 *`, `/ NULLIF(COUNT`, or a
  rate/ratio/share/percent/pct alias; a `/1e7` crore conversion is a unit scale, NOT a rate);
  (3) `is_cancelled` appears NOWHERE in the SQL; (4) the user's question is NOT about cancellations
  (no `cancel`).
- **Condition 3 is a bare-presence check (B-048 guard — owner must-fix on top of STEP A).** The
  corrective retry injects an exclusion predicate; if the SQL already references `is_cancelled` in a
  form the rate check misses — e.g. `SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END)` with no
  rate-style alias and no "cancel" in the question — firing would **stack** a second predicate and
  zero the count out (the B-048 bug). A bare presence check is a strict safety superset: every
  genuine L-013 case has NO `is_cancelled` at all (the filter was dropped entirely), so no real fire
  is lost; any query already touching the flag is left alone. Accepted trade-off: a query that wrote
  the filter in the WRONG direction is also left alone — rarer, different error, out of scope.
- `run()` hook — runs the lint **only on a CLEAN first execute** (the L-013 surface). On fire: ONE
  corrective retry via `_call_ollama(user_query, lint_retry_sql=sql)` (new mode — an alias-agnostic
  prompt naming the *general* schema rule, never a per-question example), then re-run
  `_validate_sql` + `_validate_columns` + `_execute`. Adopt the retry only if it is clean AND the
  lint no longer fires; otherwise **fall back to the original successful result — never degrade a
  working query**. Outcome recorded on `result["lint_cancellation_filter"]` =
  `"fired_corrected" | "fired_uncorrected"` (key absent when the lint doesn't fire).
- **Bounded:** at most one error-retry OR one lint-retry, never stacked. The lint never runs on the
  error-retry branch. `run_stored_sql` (frozen-SQL refresh) is **untouched** — the lint lives only in
  the live `run()` path.

**Scope (this cut):** L-013 / cancellation-filter only. The L-011/`DISTINCT` lint is deferred —
the eval has not shown `distinct_missing` firing and "should this be DISTINCT?" is false-positive-
prone; it's a fast-follow once the harness's `distinct_missing` flag shows it's real and frequent.

**Unit tests (`tests/test_lint_cancellation_filter.py` — direct-call, NO Ollama, NO DB):** 3 fire
cases (bare grouped COUNT; the `/1e7` crore trap; single-join AVG), 4 don't-fire cases (rate query;
already-has-filter; cancellation-intent question; non-`fact_bookings`), and the **B-048-guard test**
`test_no_fire_case_when_is_cancelled_no_rate_alias` (`SUM(CASE WHEN b.is_cancelled …)` with no rate
alias / no "cancel" → must NOT fire). 8/8 pass.

**Quality gate:**

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Lint unit tests (fire/don't-fire/B-048-guard) | 8/8 pass | 8 passed in 0.34s | PASS |
| Full `pytest tests/` unaffected | all green, B-003a still xfail | 60 passed, 1 xfailed | PASS |
| `ast.parse` on edited module | parse OK | OK | PASS |
| `run_stored_sql` untouched (blast radius) | frozen path has no lint | lint only in `run()`; `run_stored_sql` unchanged | PASS |
| Bounded retry | ≤1 error-retry OR ≤1 lint-retry | lint runs only on clean first execute; error branch has no lint | PASS |
| Never degrades a working query | original kept unless retry clean-and-corrected | fallback on retry-error/still-firing | PASS (code-verified) |
| Eval `cancellation_filter_missing` (direction) | documented baseline 7–8 → lower | **3** this run | PASS (corroboration; see caveat) |

**Eval re-run (`python -m ai.eval.run_eval`, single clean run, `eval_20260530T061139Z.txt`):**

```
TEST                               PATH  RUNS PASS/TOTAL  EXEC-MATCH  FLAGS
top5_cities_by_revenue             sql      3        2/3       66.7%  cancellation_filter_missingx1
cancellation_rate_by_segment       sql      3        2/3       66.7%  -
adr_5star_goa                      sql      3        0/3        0.0%  -
revenue_by_month_2025              sql      3        0/3         n/a  err:otherx1, err:validator_rejectionx2
top10_cities_by_hotel_count        sql      3        3/3      100.0%  -
customers_per_state                sql      3        3/3      100.0%  -
hotels_opened_per_year             sql      3        3/3      100.0%  -
list_5_customers_named_r           sql      3        0/3       probe  err:validator_rejectionx1
bookings_by_segment                sql      3        0/3        0.0%  cancellation_filter_missingx1
revenue_by_star_category           sql      3        3/3      100.0%  -
avg_nights_by_segment              sql      3        0/3        0.0%  cancellation_filter_missingx1, err:validator_rejectionx2
```

| Metric | Documented baseline (B-058 re-run) | This run |
|---|---|---|
| `cancellation_filter_missing` (L-013) fired | 8 (brief cites 7) | **3** |
| headline execution accuracy | 33.3% (11/33) | 48.5% (16/33) |
| among scored runs | 45.8% (11/24) | 64.0% (16/25) |
| `distinct_missing` (L-011) | 0 | 0 |
| error rate | 24.2% (8/33) | 18.2% (6/33) — `validator_rejection` 5 · `other` 1 |

**Honest reading (the aggregate is NOISY — the 8 unit tests are the proof, the harness is
corroboration):**

- The L-013 flag count dropped **8 → 3**, consistent with the lint working: the eval flags the
  FINAL (post-lint-retry) SQL, so a query the corrective retry fixes no longer carries the flag.
  Direction is right, but a single Ollama run is non-deterministic — I do NOT claim the full drop is
  attributable to B-060 alone, and the residual 3 are real (cases where the retry errored or
  re-dropped the filter → `fired_uncorrected` → fell back to the original, which still lacks it; a
  bounded-one-retry mitigation, not a cure).
- Several previously-failing questions improved exec-match this run (`top5_cities_by_revenue`
  0/3→2/3; `revenue_by_star_category` 1/3→3/3) — improvement is real but partly run-to-run variance,
  not solely B-060.
- **B-060 introduced no new error class.** The 5 `validator_rejection` + 1 `other` are pre-existing
  model failures on the hard questions: `revenue_by_month_2025` date-paradigm bleed (×2) and
  `avg_nights_by_segment` first-attempt column hallucinations (`home_segment`) that go through the
  existing B-001/B-003 **error-retry** path (the lint never runs when the first execute fails). The
  B-060 lint-retry catches its own exceptions and falls back to the original successful result, so it
  cannot surface as a harness error — confirmed by the logic and consistent with the error subtypes
  here all matching known pre-B-060 failure modes.

**Files:** `ai/text_to_sql.py` (lint fns + `_call_ollama` lint mode + `run()` hook + docstring),
`tests/test_lint_cancellation_filter.py` (new), `docs/backlog.md` (this entry + L-013 update),
`docs/phase-4-ai-layer.md` (BUILD HISTORY), `CLAUDE.md` (hardening line). `datamodel.md` untouched;
`ai/prompts/text_to_sql_system.txt` untouched (general rule, not a few-shot); no frozen file touched.

---

### B-061 — surface the B-060 cancellation-filter lint outcome as a pin-time warning in Explore · DONE

> **Trace.** Phase(s): [Phase 5](phase-5-dashboard.md) (dashboard) · datamodel: none · data: none.

**Problem:** [B-060](#b-060--post-generation-cancellation-filter-lint--corrective-retry-mitigates-l-013--done)'s
lint flags a `fact_bookings` query that's likely missing the cancellation filter and records the
outcome on `result["lint_cancellation_filter"]` (`"fired_corrected"` | `"fired_uncorrected"`, absent
when it doesn't fire) — but nothing showed it to the user. A `fired_uncorrected` query (lint fired,
the one corrective retry didn't take → the answer is suspect) could be pinned with no signal that the
numbers may be wrong. This complements [B-022](#completed) (pin-time SQL freeze): B-022 makes the SQL
reviewable at pin time; B-061 flags WHEN that review matters most.

**What shipped — template-only (`render/templates/explore.html`):**

- **No `server.py` change needed.** `/api/query` already returns the full `result` dict verbatim
  (`"result": result`), so `lint_cancellation_filter` already reaches the client as
  `data.result.lint_cancellation_filter`. `answer()` returns `text_to_sql.run()` unchanged for the
  SQL path, so the flag flows through untouched. The whole change is in the Explore preview render.
- `buildLintBanner()` (new JS) reads the global `currentResult` and returns banner markup:
  - `"fired_uncorrected"` → a clear amber caution (`#fffbeb` / `#fde68a` / `#92400e`, matching the
    existing readability-warning palette): *"This query may be missing the cancellation filter and
    couldn't be auto-corrected. It may be counting cancelled bookings — review the SQL before
    pinning."*
  - `"fired_corrected"` → a subtle blue info note (`#eff6ff` / `#bfdbfe` / `#1e40af`):
    *"Auto-corrected a missing cancellation filter — review the SQL."*
  - absent → empty string → **preview behaves exactly as before**.
  - Each banner carries a collapsed `<details>` "Show SQL" (from `currentResult.sql`, HTML-escaped
    via a new `escapeHtml` helper) so *"review the SQL"* is actionable — the Explore preview has no
    other SQL view.
- `renderPreview()` injects `${lintBanner}` between the preview header and the existing readability
  `${warningBanner}` — the correctness caution leads the chart-type hint. Because the banner is
  rebuilt from `currentResult` on every `renderPreview` call, it **persists across chart-type
  switches** (`switchType` / `dismissWarning` / `overrideType` all re-render) — correct, since the
  lint is about the SQL/data, not the widget type.
- **WARN, never block.** The banner emits no controls that touch the pin flow; `#pinBtn` stays
  enabled. The human decides.

**Out of scope (named):** persisting the flag onto the pinned widget / dashboard (needs a
`dashboard_widgets` schema column — a separate item). B-061 touches only the Explore preview path.
The lint logic, `run_stored_sql`, and `ai/prompts/text_to_sql_system.txt` are untouched.

**Acceptance / quality-gate** (the dev cannot take browser screenshots — the owner does; the dev
verifies the render logic deterministically + reports a repro query):

| TEST | EXPECTED | ACTUAL | PASS/FAIL |
|---|---|---|---|
| Lint flag reaches the Explore client end-to-end | `data.result.lint_cancellation_filter` present on a fired query | live `/api/query`: `fired_corrected` (filter dropped → retry added it) + `fired_uncorrected` (both dropped) + `None` (filter present) all observed | PASS |
| `buildLintBanner` emits the warning ONLY when it should (deterministic, real fns in node) | uncorrected→amber+ShowSQL+pin-untouched; corrected→blue info; absent/undefined/unknown→`''` | 14/14 node assertions pass | PASS |
| WARN never blocks | banner emits no pin-button / disable markup | asserted: output has no `pinBtn` / `disabled` | PASS |
| `escapeHtml` keeps SQL from breaking markup | `< > &` escaped | `a<b>&c` → `a&lt;b&gt;&amp;c` | PASS |
| `pytest tests/ -v` unaffected (template-only) | all green, B-003a xfail | 60 passed, 1 xfailed | PASS |
| No schema change; `run_stored_sql` + lint logic untouched; no `.py` changed | template-only diff | `git status`: only `explore.html` (+ docs) modified | PASS |

**Repro for the owner's screenshots** (run from the Explore page with the stack + Ollama up):
- **`fired_uncorrected` (amber warning — the must-have):** `total revenue by tourism zone`. Observed
  this session — final SQL `SELECT l.tourism_zone, ROUND(SUM(b.revenue_inr)/1e7,2) … GROUP BY
  l.tourism_zone` with **no `is_cancelled` filter** (genuinely suspect). **Probabilistic** — it is a
  two-failure event (both the first SQL AND the corrective retry must drop the filter), so it landed
  ~1 in 2 tries; other tries roll `fired_corrected` or `None`. Re-run a couple of times if the first
  attempt shows the blue note or no banner.
- **No-warning (normal):** `top 5 cities by revenue` — the model reliably includes `WHERE NOT
  b.is_cancelled` first try → flag absent → no banner.
- **`fired_corrected` (blue info — optional):** `total bookings by customer segment` — landed
  `fired_corrected` on every attempt this session (the retry reliably adds the filter).

**Files:** `render/templates/explore.html` (banner JS + injection), `docs/backlog.md` (this entry),
`docs/phase-5-dashboard.md` (BUILD HISTORY), `CLAUDE.md` (render hardening line). `datamodel.md`
untouched; no schema/migration; no `server.py` change; no frozen file touched.

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

### B-063 — `.claude/settings.json` permission-allowlist hardening (destructive ops prompt; one-offs live in local)

> **Trace.** Phase(s): ops/dev (no phase doc — Claude Code config; the committed allowlist lives in [`.claude/settings.json`](../.claude/settings.json), per-developer/one-off rules in the gitignored `.claude/settings.local.json`) · datamodel: none · data: none.

**Priority:** Medium — built; pending owner review + commit (config-only, isolated to `.claude/`).
**Problem:** The committed `allow` list had grown to ~127 entries by distillation from `settings.local.json`. Two issues: (1) over-broad **destructive** wildcards traded a safety prompt for a silent irreversible action — most dangerously `Bash(docker volume *)`, which permits `docker volume rm/prune` and can wipe the Postgres data volume (993 cities / 20,076 hotels / 1M bookings / 2M+ lifecycle events / 133K reviews — unrecoverable); also `docker rm *`, `docker cp *`, `pip install *` (arbitrary code), and broad/argument-less `Stop-Process` / `taskkill` (kill any process). (2) Accumulated single-use clutter — specific PIDs, `c:\tmp\…` log-capture paths, dated `monitor/data?from=2026-05-…` curls, and dead B-047 `--rate`/`--duration`/`--chaos-seed` producer runs — none of which are durable rules.

**What changed (`.claude/settings.json`):**
- **Narrowed** `Bash(docker volume *)` → `Bash(docker volume ls)` + `Bash(docker volume inspect *)` (read-only only; `rm`/`prune` now prompt).
- **Removed** (now prompt): `Bash(docker rm *)`, `Bash(docker cp *)`, `Bash(pip install *)`, `PowerShell(pip install *)`, and every `Stop-Process` / `taskkill` form (specific-PID and argument-less alike, incl. `PowerShell(Stop-Process -Force)`).
- **Decluttered** ~80 stale one-offs down to the durable grouped subset the file's own `_comment` enumerates (docker/exec · venv+module python · `pip list` · health probes · pytest · read-only fs/process inspection). `docker stop *` kept (restartable, non-destructive).
- **Kept** all benign work permissions: `python *` / `python3 *` / venv python, `pytest *`, `pip list`, the four service health-probe curls, and read-only inspection (`Get-ChildItem`, `tasklist`, `netstat`, `Get-NetTCPConnection` on the project ports 5000/47219, `Get-WmiObject Win32_Process`, `Read(…)`).
- **`_comment`** updated to list the deliberately-not-allowlisted destructive ops and to state that one-offs belong in `.claude/settings.local.json`.
- The stale one-offs were **removed, not moved** — being exact-match PID/path/date strings, they would never match a future command, so relocating them to local adds no value. `settings.local.json` was left untouched; confirmed gitignored.

**Convention reinforced:** any allow rule that can silently destroy data, kill arbitrary processes, or install arbitrary code must require a prompt; the committed `settings.json` holds only durable general rules, and dated/PID-/path-specific commands live in the gitignored `.claude/settings.local.json`.

**File:** `.claude/settings.json` (committed). Result: 47 durable `allow` entries (from ~127); valid JSON; zero destructive wildcards.

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
| L-012 | ~~Sentiment-topic conflation in semantic review search. Embeddings match TOPIC not POLARITY — "cleanliness complaints" returns cleanliness praise and complaints alike, since both are about cleanliness.~~ — **SUBSTANTIALLY RESOLVED by B-026 (whole-review sentiment).** Stage 1 scored every review with CardiffNLP into `reviews_raw.sentiment_label` (migration 017); Stage 2 swapped semantic retrieval from the B-062 star-rating proxy to a hard `sentiment_label = 'negative'/'positive'` predicate (`_search_reviews`), so a negative query now filters on model sentiment, not stars. The proxy's known hole — genuine complaints buried in mixed-sentiment 3★ reviews — is closed: deterministic A/B over the live corpus shows "cleanliness complaints" surfaces 19 and "rude staff" 8 three-star NEGATIVE reviews that B-062's ≤2★ filter rendered invisible. Original B-004-era finding (complaints like "foul smell"/"cobwebs" in 3★ positive-opener reviews; identical texts across 1–5★) is what the rating proxy could not handle and the label now does. **Residual: aspect-level sentiment — see L-017** (mixed reviews whose net/opening sentiment reads positive still label positive, so a complaint inside them is excluded from negative results). Copy fix on the summary line ("reviews mentioning X" vs "X complaints") remains a smaller separate item. | Phase 4 | **Substantially resolved by B-026 Stage 1+2** (whole-review sentiment label + retrieval swap), superseding the B-062 rating-proxy mitigation. Aspect-level residual tracked as **L-017**. |
| L-017 | Aspect-level sentiment is not modelled — B-026 stores ONE `sentiment_label` per whole review, so a mixed review whose net/opening sentiment reads positive ("Staff were incredibly helpful… but the kettle was broken and reception was rude") is labelled `positive` as a unit. The genuine complaint inside it is therefore excluded from a negative-intent semantic search, even though B-026 substantially fixed the dominant L-012 conflation. Quantified during the B-026 Stage 2 gate: of 18 hard 3★ complaint-text reviews, B-026 recovers 11 as `negative`; the 7 it labels positive are exactly these positive-opener mixed cases. Whole-review sentiment is the right altitude for the topic/polarity flip L-012 was about; per-aspect (clause-level) sentiment is a deeper, separate model — out of scope for B-026. | Phase 4 | Aspect-based sentiment analysis (clause/aspect-level classifier, or an LLM aspect-extraction pass) — future item; whole-review label (B-026) is the current ceiling. A confidence gate on the stored `sentiment_score` (kept by B-026 for exactly this) is the cheap next lever before full ABSA. |
| L-013 | Bare grouped aggregations over `fact_bookings` (shapes like "total revenue by city", "bookings by month", "revenue by customer segment", "average nights stayed by season") intermittently drop the `WHERE NOT b.is_cancelled` filter on Qwen-7B. Stronger cues — `LIMIT`, explicit `WHERE` filters on star_category / city / etc. — usually keep the filter (Tests 6 and 7 in the verification suite). Bare grouped shapes do not. **Confirmed not promptable** at this model size: two prompt rewrites attempted — a single-bullet "applies ONLY when fact_bookings is in FROM/JOIN" version (the current text) and a two-bullet universal-rule-plus-illustrations version. Neither resolves the bare-grouped cases. The two-bullet version improved "by month" and "by segment" to 3/3 but degraded an unnamed-shape generalisation ("average nights stayed by season") and added marginal noise on the LIMIT-bearing cases. Root cause: Qwen 7B underweights universal/conditional rules in the system prompt relative to attention pull from concrete examples in the same prompt — a small-model attention budget limit, not a prompt-wording bug. Same family as L-010 and L-011 (capability ceiling, not prompt tuning). **Safeguards:** (1) **B-060** post-generation lint — on a clean first execute, `run()` detects a bare `fact_bookings` aggregate that dropped `WHERE NOT is_cancelled` (not a rate, no cancellation intent, no `is_cancelled` anywhere) and drives ONE corrective retry naming the general schema rule; falls back to the original on retry-error/still-firing so it never degrades a working query. Catches the live-Explore case the pin-time review can't (mitigation, not a cure — bounded to one retry, leaves the underlying model ceiling). (2) **B-022** freezes generated SQL at pin time, so a missing cancellation filter is a one-time review failure at pin, not a per-refresh data integrity bug — the read path never runs unverified SQL. **Fix path:** B-017 (model tiering — Gemma 12B or similar holds rule discipline better in early testing) is the real remediation. | Phase 4 | B-017 (larger model for SQL) — B-060 post-gen lint + B-022 pin-time review are the meanwhile mitigations |
| L-014 | ~~CHECKIN / CHECKOUT (and PRICE_CHANGE) events are silently filtered at the consumer's Gate 3 — not aggregated, not counted, not stored.~~ — **RESOLVED (B-032 Chunks 2 + 3).** `PROCESSED_EVENT_TYPES` now spans `BOOKING, CANCELLATION, CHECKIN, CHECKOUT`; the consumer counts each per window and writes `total_checkins / total_checkouts / total_cancellations` to `agg_hourly_city_stats` (migration 007 columns). The producer (Chunk 3) now actually emits both CHECKIN and CHECKOUT at design weights (0.18 / 0.12), so both columns read non-zero on every recent window — verified end-to-end. PRICE_CHANGE is still silently filtered at Gate 3 (valid event type, just not in `PROCESSED_EVENT_TYPES`) — out of scope for this item. **Note:** these counts live ONLY in the stream aggregate (`agg_hourly_city_stats`); they are NOT in `fact_bookings` or any revenue path. Anything that wants a checkin-aware booking model (e.g. tying CHECKIN/CHECKOUT events to specific bookings) still has to be designed separately. | Phase 7 | RESOLVED by B-032 Chunks 2 + 3. |
| L-015 | ~~No live "events received / sec" throughput metric. The consumer's `run_metrics` counters live in memory and print only at shutdown — not written to a queryable table mid-run.~~ — **RESOLVED in full (B-032 Chunks 1+2+4).** Consumer writes a `pipeline_metrics` heartbeat row every `FLUSH_CHECK_SECONDS` (~10s) with cumulative `events_consumed / bookings / cancellations / malformed / late / active_windows / max_event_ts` (plus reserved-NULL `consumer_lag`). The `/monitor` header pulse reads the latest two heartbeat rows and surfaces events/sec as the delta divided by interval, with `alive = age < 15s` driving a green/grey dot. Verified end-to-end against a 50 evt/s producer: computed rate 49.6–50.2 evt/s mid-run; dot goes grey within one tick when the consumer stops. | Phase 7 | RESOLVED by B-032 (all 4 chunks). |
| L-018 | Implicit-complaint queries get no polarity filter — the query-intent detector `_detect_polarity` (B-062, reused by B-026 Stage 2) is **keyword/lexicon-based**, so a query that implies a complaint *without* an explicit sentiment word ("noisy AC", "thin walls", "slow check-in", "small rooms") is classed `neutral` and the semantic search runs unfiltered (topic-only). Such queries therefore don't yet benefit from B-026's `sentiment_label = 'negative'` retrieval filter — they fall back to the pre-B-026 topic-match behaviour. Distinct from **L-017**, which is about per-review *model labeling* (whole-review vs aspect-level sentiment); L-018 is upstream of that, about *query-intent* detection (does the query express negative intent at all). Confirmed during the B-026 Stage 2 gate: "noisy AC" was classified neutral → unfiltered (the A/B forced a negative predicate only to demonstrate the mechanism). | Phase 4 | Replace/augment the keyword detector with a small sentiment/intent classifier on the QUERY (the same class of model B-026 already runs on reviews), or an LLM intent pass — future item; the explicit-keyword detector is the current ceiling. |
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
| ✓ | B-026 — Sentiment classification for reviews (FULLY CLOSED across 3 stages; substantially resolves L-012, aspect-level residual L-017, implicit-complaint residual L-018). **Stage 1:** migration 017 (`sentiment_label VARCHAR(8)` + `sentiment_score NUMERIC(4,3)` + partial index) + `scripts/review_sentiment_scorer.py` (CardiffNLP `twitter-roberta-base-sentiment-latest`, safetensors rev `d616e2bd`, lock 7400070, `--once` backfill + loop) + 133,543-row backfill (67% pos / 29% neg / 4% neutral; star×label monotonic; ~93% net-sentiment agreement on a 100-row stratified spot-check; decisive 3★ test recovers 11/18 buried complaints). **Stage 2:** `_search_reviews` retrieval predicate swapped from the B-062 star-rating bound (`r.rating <=2/>=4`) to `r.sentiment_label = 'negative'/'positive'`; `_detect_polarity` reused unchanged; relax-and-note fallback preserved; A/B surfaced 19 ("cleanliness complaints") + 8 ("rude staff") buried 3★ negatives. **Stage 3:** `review_sentiment_scorer.py` wired as run.py's **7th managed proc** (loop mode), B-045 startup-takeover lock-poll extended to 7400070 — wiring only, no logic/schema/scoring/batch change (resource audit confirmed all 7 procs + Ollama fit, ~91% VRAM peak, no OOM). Stage 3 verification 7/7 PASS (all 7 procs up; scorer acquires 7400070; other 6 unaffected; NULL-sentiment probe scored within ~15s; clean shutdown → 0 locks/0 orphans; pytest baseline). | Phase 3 / Phase 4 |
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
| ✓ | B-050 — README doc-consolidation: Documentation map + `run.py` reference. **Trace.** Phase(s): ops/dev · datamodel: none · data: none. New `## Documentation map` section between Data model and Quickstart — single table mapping every actively-maintained doc (CLAUDE.md repo index · datamodel.md data-model truth · db/schema.sql frozen base + reminder to read migrations · 7 phase docs framed as history per the conventions hierarchy · backlog.md B-/L- status · capabilities-and-limits.md per-feature reliability · external blueprint) to a one-line "what's in it / when to read it." Links + one-liners only — zero content duplication. Scattered nav-links to `db/schema.sql` and `datamodel.md` consolidated into the map (one home per detail). `### Running the stack` renamed `### Running the stack — \`run.py\` reference` and expanded from 4 examples to: 7-line Common shapes block + 9-flag table (source-verified against `run.py --help` and the `add_argument` block — all of `--rate-multiplier` / `--chaos` / `--malformed-pct` / `--late-pct` / `--chaos-seed` / `--no-sim` / `--server-only` / `--no-docker` / `--window` / `--down` covered with accurate semantics; deprecated flag-names absent) + Behaviour notes (B-045 takeover + foreign-:5000 guard + 47219 singleton port + advisory-lock pairs 7400030/40/50/60 + B-047 wall-clock producer + migration-016 last-write-wins cap). 7/7 verification PASS. Session-notes 2026-05-25 entry line 57 ("README.md line 101 quotes `--rate 50 --duration 60`") pruned — closed by this item. | Phase ops/dev |
| ✓ | B-052 — README portfolio polish: animated dual-theme SVG hero + intro tightening. **Trace.** Phase(s): ops/dev · datamodel: none · data: `docs/assets/pipeline-flow-light.svg`, `docs/assets/pipeline-flow-dark.svg` (new doc assets). ASCII pipeline diagram replaced by a `<picture>` element that auto-switches by `prefers-color-scheme`: dark mode → `pipeline-flow-dark.svg`, light fallback → `pipeline-flow-light.svg`. Both SVGs carry `viewBox="0 0 800 240"` (responsive on narrow viewports), `role="img"` + descriptive ~50-word aria-label (Kafka / consumer / dual sink / Ollama / Flask), and 3 SMIL `animate*` elements each (flow-dots animate on GitHub web view — declarative SMIL is preserved by GitHub's sanitizer; `<script>` would not be). Engineering-highlights intro tightened to a one-line redirect to `docs/capabilities-and-limits.md` (the single source for per-feature reliability per the conventions hierarchy — no content lost). `reviews_raw` row count corrected `(~30K rows)` → `(~133K rows)` to match the live corpus (post-B-030 + 90,980 history reviews; cf. B-046 verification table). Redundant in-body DuckDB callout removed — the blueprint-vs-repo pivot blockquote (README:17) and the docs-map blueprint row (README:72) already cover it; third mention was duplication the docs-conventions hierarchy discourages. 8/8 verification PASS (both SVG assets present at the paths the README references; both carry ≥1 animation element; both use `viewBox`; both have `role="img"` + aria-label; `<picture>` element well-formed with `<img>` fallback for legacy browsers; README `reviews_raw` count matches live DB; DuckDB mentions ≤ 2 contextual hits; no code/script/SQL/template/config touched — only `README.md` + 2 new SVG assets + this backlog entry). Light/dark theme eyeball-check is owner-side (requires push — GitHub's `prefers-color-scheme` swap only takes effect under the rendered web view). | Phase ops/dev |
| ✓ | B-053 — Dashboard curation: 11 portfolio widgets pinned across 5 query-shape tiers. **Trace.** Phase(s): [Phase 5](phase-5-dashboard.md) · datamodel: `dashboard_widgets` rows (state-only; no schema change) · data: existing facts / dims / reviews (no new data). Prior 13 exploratory widgets cleared first (owner-confirmed via question). Curated set covers Tier 1 simple aggregations (top cities by revenue · bookings by tourism zone · booking sources mix), Tier 2 filtered/grouped (avg nightly rate by star category · cancellation rate by city top 15 — B-048 formula exact · top 10 hotels in Goa), Tier 3 complex/live/hybrid (live: bookings today — B-048 live routing on `fact_booking_events` with `source='stream'` + `event_date=CURRENT_DATE` · revenue this month by tourism zone · top complaints across reviews · cleanliness complaints in Goa — B-051 hybrid scoping 20/20 reviews on Goa · repeat customer share by zone via `dim_customer.is_repeat_customer`). 1 of 12 widgets skipped — "Revenue by month in 2025" (date-paradigm bleed: 5 phrasings tried, model invented `b.booking_date` / `d.date_key` / `d.month_number` even after B-001 retry; pin-once succeeded but froze broken `date_key` SQL → deleted; tracked as L-013-adjacent). 8/8 verification PASS: dashboard_widgets cleared pre-pin (0 rows) · final count 11 (10-12 target met) · 0 cached-error rows · `/dashboard` returns 200 with 11/11 `data-widget-id` present · `POST /api/refresh/<id>` clean on 6/6 spot-checked SQL widgets · widget 8 frozen SQL targets B-048 live path · widget 11 returns Goa-scoped reviews · B-022 pin-time freeze contract intact (9/11 SQL widgets carry `generated_sql`, 2 semantic widgets NULL by design). Refresh-interval mapping: hourly → 60, daily → 360, 60s → 5, manual → 0 (closest values from the `{0,5,15,30,60,360}` whitelist). Files: `docs/backlog.md` (this entry) + `docs/capabilities-and-limits.md` (one-line note linking here). No code / scripts / templates / migrations touched. | Phase 5 |
| ✓ | B-049 — Producer `SEASON_MULT` vocab fix. **Trace.** Phase(s): [Phase 2](phase-2-streaming.md) · datamodel: [`dim_date.season` vocabulary](../datamodel.md#dim_datecsv) (no schema change — documents real enum values consumed by the producer) · data: `scripts/kafka_event_producer.py` (`SEASON_MULT` dict + new `_season_mult()` helper + `_compute_price` season branch). Narrow follow-up to [B-047](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done). The producer's `SEASON_MULT` was keyed `{Summer, Winter, Monsoon, Spring, Autumn}`; real `SELECT DISTINCT season FROM dim_date` returns `{Peak, Shoulder, Summer, Monsoon}`. Only `Summer` and `Monsoon` matched — **`Peak` (1,059 of 2,557 days, Jan/Feb/Oct/Nov/Dec — entire Indian winter peak) and `Shoulder` (March) silently fell through to neutral 1.0**, removing the season signal on most days. Same defect in `_compute_price`'s `elif season == "Winter": mult *= 1.10` — never fires against real `Peak` value. Re-keyed `SEASON_MULT` to the real vocabulary (Peak 1.30 · Shoulder 1.00 · Summer 1.00 · Monsoon 0.75); added `_season_mult(season)` helper that warns once-per-unknown-value to stderr and returns 1.0 default so future vocab drift is caught loudly; updated `_compute_price` to use `season == "Peak"`. Verification: real ∩ keys = full match (0 fall-through, 0 unused); `_season_mult` returns correct values for all 4 real keys; unknown values emit one-line WARNING + dedup on repeat (`_UNKNOWN_SEASON_WARNED` set); producer 45s smoke-test ran clean (889 events emitted at ~20 evt/s peak, 0 WARNING / 0 ERROR / 0 Traceback / 0 revenue-invariant breaks; all reachable event types BOOKING + PRICE_CHANGE + CHECKIN + CANCELLATION visible). Date-source finding surfaced (not changed): season is keyed on `today_ist` (booking date) not `checkin_d` (stay date) — flagged for separate owner decision. No migration, no wire-contract change, no consumer/gold/monitor impact. | Phase 2 |
| ✓ | B-054 — Blueprint AI-sell: two animations, reframed §07/§08, §01 "Life before vs. after" block removed, metric-strip reconciliation. **Trace.** Phase(s): ops/dev · datamodel: none · data: none. Single file edited (`index.html`). Two namespaced animations (`.tl-anim1` Text-to-SQL personas · `.tl-anim2` semantic spotlight) with `tl-`-prefixed classes + 8 `tl-`-prefixed keyframes — zero collisions with the blueprint's existing `@keyframes blink` (border-pulse, line 81) or `tl-pin-jump/shadow/wave/bird*/glow` (hero); shared syntax classes `.kw / .str / .fn / .num / .cmt` (line 70) reused inside the SQL panels. Two JS cyclers (anim1 4-phase persona rotation · anim2 7.5s+1s theme rotation) scoped via `document.querySelector('.tl-anim*')` so they bind only to the new elements; `prefers-reduced-motion` short-circuits both to render first example statically + `@media` block freezes all `tl-*` animations. §01 "Life before vs. after TravelLens" block removed entirely (mono uppercase header + 2-column ✗/✓ comparison grid both gone — value-prop work now carried by the new animations + the existing "Why not Power BI" callout). §07 reframed business-first — preamble replaced with revenue-manager lead → animation 1 (Rev/Ops/Mkt × top-N/filter+ratio/group+percent); 80/20 SQL-vs-Semantic table removed, "Try this" 6-card UI mockup removed (animation supersedes both), 2 SQL examples → 1 (kept `ref_price_tiers` budget-resolution as the more interesting), system-prompt callout 5-bullet enum → one paragraph; closes with `python -m ai.main` + `capabilities-and-limits.md` link. §08 reframed similarly — ops-head "dirty room" lead → animation 2 (rude staff / dirty room / amazing view × 3 matches + 3 non-matches each with synonym callouts); "Why not full-text search" comparison KEPT; 4-step pipeline strip removed (animation covers embed+match conceptually); 4-paragraph technical body → one paragraph naming MiniLM 384-d + pgvector `<=>` + IVFFlat B-051 rule of thumb (`lists ≈ rows/1000`, `probes ≈ √lists` → 120/11 at the current ~133K corpus); "Real data" callout 4 lines → 1. §02 metric strip: tile "Schema tables" 15 → 22 (verified via `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'` returning 22; matches schema.sql 14 base + migrations 003/007/008(×2)/009(×2)/013/014 minus dropped quarantine_daily_summary); tile "Real reviews" 30K+ → 133K; new 6th tile `~20 Peak evt/sec @ 19 IST` (B-047 formula 10×2.05×1 ≈ 20.5); supporting prose in "what it is" card (`15-table star schema` → `22-table warehouse on a star-schema core`, `30K+ review JSONs` → `~133K`) and terminal block (`30,000 reviews indexed` → `133K reviews indexed`) updated for internal consistency. 11/11 verification PASS. Mid-session scope change: third animation (Questions Hotels Ask gallery) initially built then dropped on owner direction — `.tl-anim3` CSS, `tl-a3-expand` keyframe, anim3 reduced-motion line, and the `<section id="questions">` block all removed in the same session; final shipped scope is 2 animations, not 3. §05 schema heading and body left at "15 Tables" (curated star-schema view of 14 base + dashboard_widgets — explicit out-of-scope) and README's "14 tables" mention flagged for a separate follow-up — captured in B-054 "Out of scope." | Phase ops/dev |
| ✓ | B-047 — Stage 2a forward generator. **Trace.** Phase(s): [Phase 2](phase-2-streaming.md#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--current-producer) · datamodel: [`sim_open_bookings` (migration 015 fire-times + `'REVIEW_PENDING'` state)](../datamodel.md#sim_open_bookings), [`sim_daily_counter` (migration 014)](../datamodel.md#sim_daily_counter), [Schema Evolution → 014/015](../datamodel.md#schema-evolution) · data: `scripts/kafka_event_producer.py` (full rewrite, 1,191 lines), `scripts/chaos_injector.py` (new — extracted), migrations 014 + 015. Replaces the calendar-replay producer (B-034A): no sim-clock, `.sim_clock.json` deleted on first launch. Wall-clock `event_ts = datetime.now(UTC)`; `event_date = today in IST`. Token bucket paced at `effective_rate(h_IST) = BASE_RATE(10) × DIURNAL_HOUR_MULT[h] × rate_multiplier` (Σ=24.10 → mean ≈ 1.004×, peak 2.05× @ 19 IST; daily integral ≈ 868K at x=1 vs 1M cap → 13% headroom). Each tick (~200ms) drains lifecycle-priority then fills with BOOKING vs PRICE_CHANGE by per-hour weight `prob_b = w_b[h]/(w_b[h]+w_p[h])` — no 95/5 coin flip; daily aggregate shapes itself ≈ 48/52. Data-aware new-BOOKINGs: city by `popularity × hotel_count × season/holiday` from `dim_date`; hotel by `total_rooms × star` under per-hotel-per-night occupancy cap against overlapping `sim_open_bookings`; room type fitting `num_guests`; customer 75% out-of-state; lead-time 50% ≤7d / 35% 1-8wk / 15% 2-8mo; nights from empirical `fact_bookings.nights_stayed` mix; price = base × {weekend/holiday/season} × N(1.0,0.05); revenue = nightly × nights (invariant). Each BOOKING stamps `checkin_fire_ts` + `checkout_fire_ts` (and optional `cancel_fire_ts` at 12% deterministic) drawn from per-event-type IST hour distributions (CHECKOUT 8-13 peak 9-10, CHECKIN 12-21 peak 16-18, CANCELLATION 9-21 evening-lean). On CHECKOUT or CANCELLATION emit, if the negativity-bias draw yields a review, `state → 'REVIEW_PENDING'` + `review_fire_ts` set to tonight 20-23 IST (clamped ≥ now+30min); REVIEW fires when wall-clock reaches the stamp; row deleted on REVIEW emit. Catch-up rule: overdue fire-times at startup drain at the bucket cap with `event_ts = NOW()` — never backdated. `sim_daily_counter.cap = 1_000_000 × rate_multiplier`, **last-write-wins across same-day sessions** (migration 016 follow-on — every producer-startup UPSERT overwrites cap with `EXCLUDED.cap`; `events_emitted` is not overwritten and keeps accumulating). On cap-hit the producer sleeps until IST midnight. All per-tick DB mutations committed in ONE batch. Single throughput knob `--rate-multiplier` (int>1; any invalid value silently → 1; same coerce on `RATE_MULTIPLIER` env). Folds in B-041 (prior `--sim-rate` / `--rate` / `--duration` / `--sim-speed` no-op flags REMOVED entirely). Chaos extracted to `scripts/chaos_injector.py` (byte-identical behaviour, shared module). Single-instance: `pg_try_advisory_lock(7400030)`. Smoke-test verification: 298 events consumed / 0 malformed / 0 late / 100 silver inserted / 20 reviews / 0 revenue invariant breaks; CHECKIN fire-hour histogram all in 12-21 IST peak 17 (n=14); CHECKOUT all in 8-13 IST peak 10 (n=21); BOOKING vs PRICE_CHANGE mix at IST 00-01 = 12.7% (matches `w_b/(w_b+w_p) = 0.04/0.34 ≈ 0.118`); cancel rate 13.1% (target 12%). `--rate-multiplier` coerce verified 22/22 cases (CLI + env var). | Phase 2 |
