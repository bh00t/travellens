# TravelLens India — Claude Code Instructions

> This file is read automatically by Claude Code at the start of every session.
> Customise the paths, username, and preferences below to match your environment.

---

## Contents

- [Project](#project)
- [Stack](#stack)
- [Phase status](#phase-status)
- [Hard rules](#hard-rules----never-break-these)
- [Repo layout](#repo-layout)
- [Documentation conventions](#documentation-conventions)
- [Common mistakes](#common-mistakes----always-avoid-these)
- [Key design decisions](#key-design-decisions-locked----do-not-re-litigate)
- [Docker stack](#docker-stack)
- [Quality gate](#quality-gate----every-change)
- [If something breaks](#if-something-breaks)

---

## Project

Real-time hotel and tourism intelligence platform for the Indian hospitality market.
Learning project — building data engineering skills by shipping real code, not studying theory.

**Repo:** `C:\Users\risha\Desktop\Code\repo\travellens`  
**OS:** Windows 11 · **GPU:** RTX 3070 8GB · **RAM:** 31GB  
**Python:** 3.11 (always use `.venv`) · **Run modules as:** `python -m <module>`

---

## Stack

| Layer | Technology |
|---|---|
| Database | Postgres 16 + pgvector |
| Streaming | Apache Kafka + MinIO (S3) |
| AI — SQL | Ollama · Qwen2.5-Coder-7B (local) |
| AI — Embeddings | sentence-transformers · all-MiniLM-L6-v2 |
| Dashboard | Flask · Jinja2 · Chart.js |
| Orchestration | Airflow (Phase 6 — in progress, own container, LocalExecutor) |
| Dev tools | Docker · psql |

---

## Phase status

| Phase | Description | Status |
|---|---|---|
| 0 | Environment setup | ✓ Complete |
| 1 | Postgres schema + data load | ✓ Complete — **B-046 expansion shipped 2026-05-27**: additive 949 cities → 993, 18,076 hotels → 20,076, 49,904 room types → 55,446, 80,000 customers → 100,000. Fact tables UNCHANGED. No migration (every column already existed). Source of truth: `seeds/cities_expansion.csv` + `scripts/expand_dimensions.py`. |
| 2 | Kafka streaming + dual sink | ✓ Complete — **B-047 forward generator shipped 2026-05-28** (Stage 2a): producer replaced from calendar-replay → data-aware FORWARD with diurnal IST rate curve (`scripts/kafka_event_producer.py`, migrations 014 + 015, `scripts/chaos_injector.py`, single `--rate-multiplier` knob folding in B-041). |
| 3 | Embeddings + pgvector | ✓ Complete |
| 4 | AI layer (Text-to-SQL + semantic) | ✓ Complete |
| 5 | Flask dashboard | ✓ Complete |
| 6 | Airflow DAGs | ⬜ In progress — infra/containers up; B-033 `quarantine_daily_rollup` DAG retired (superseded by B-044 run.py proc); B-024/013/014/015/016 not built |
| 7 | Pipeline monitor (/monitor) | ✓ Complete — B-027 base + B-029 in-place auto-refresh + B-032 live throughput redesign all shipped (live pulse, default-today filter, lifecycle counts, SOON placeholders, `/monitor/data` JSON sidecar); STREAM FRESHNESS tile removed (owner decision — live pulse + windows-flushed cover liveness; singleton guard prevents the stall it caught); B-044 quarantine read-path is pure Postgres SUM from `quarantine_hourly_summary` (run.py 6th proc, 5-min loop, lock 7400060; no S3 on request path) |

Current phase: **6** — Phase 7 shipped (monitor redesign done end-to-end). Read `docs/phase-7-monitor.md` for the live-pulse + auto-refresh design. B-044 hourly quarantine rollup shipped (migration 013 + `scripts/quarantine_hourly_rollup.py` run.py proc; supersedes B-033 Airflow DAG + migration 012). Phase 6 remaining DAGs (B-024/013/014/015/016) still open; infra is up. **B-047 (Stage 2a) forward generator shipped 2026-05-28**: `scripts/kafka_event_producer.py` rewritten — calendar replay deleted, diurnal IST rate curve (Σ=24.10 → mean ≈ 1×, peak 2.05× @ 19 IST), data-aware net-new bookings under per-hotel-per-night occupancy cap, per-event-type fire-time stamps (CHECKOUT 8-13, CHECKIN 12-21, CANCELLATION 9-21 evening-lean, REVIEW 20-23 deferred). Migrations 014 (`sim_daily_counter`) + 015 (fire-time cols on `sim_open_bookings` + new state `'REVIEW_PENDING'`). Chaos extracted to `scripts/chaos_injector.py`. Single throughput knob `--rate-multiplier` (int>1; invalid silently → 1). Folds in B-041 (the prior `run.py --sim-rate` no-op deprecation is closed by B-047 — flag deleted from `run.py`). Calendar simulator Phase A (B-034A) and Phase B/C (B-036/B-037) all superseded. **B-040 (gold lifecycle layer) shipped:** migration 009 + `scripts/gold_lifecycle_updater.py` — 624,388 lifecycle rows, 0 illegal flags, watermark-based incremental updates, decoupled from the consumer.

> **Phases are not strictly sequential.** Phase 7 (monitor) shipped ahead of
> Phase 6 (Airflow, in progress) because the monitor unblocked stream visibility
> without needing the batch DAGs first. A spec's `PREREQUISITES` chain may
> therefore reference a later-numbered phase — trust each spec's own
> prerequisites block over the phase-number ordering.

> Phase 4 and 5 are acceptance-complete but under ongoing hardening via backlog
> items (B-003, B-004, B-006, B-022). "Complete" means the phase shipped — it does
> NOT mean its files are frozen. See the hard rules below for what is and isn't editable.

---

## Hard rules — never break these

- **Dev server is single-process, no reloader.** `render/server.py` runs with
  `debug=False, use_reloader=False`. The Werkzeug reloader on Windows spawns
  child workers that hold the port socket; after a file change the old worker
  survives and serves stale code silently. To pick up any code change, fully
  restart `run.py` (or kill the Flask process and re-run it directly). Never
  re-enable `debug=True` or remove `use_reloader=False`.
- **Build all LLM-facing content from the real schema, never from memory.** Before
  writing or editing any system prompt, SQL-generation context, column-validation map,
  or anything an LLM must understand about the data model, read the actual schema first:
  `docker exec travellens-postgres psql -U travellens -d travellens -c "\d <table>"`
  or query `information_schema.columns`. Include exact column names, data types, and
  FK relationships. Approximating the schema from memory causes type-mismatch errors
  (e.g. comparing a VARCHAR hotel_id to a UUID location_id) that no amount of example
  queries will fix. The fix is always correct schema knowledge, not more examples.
  This applies to runtime code too: `_validate_columns` / `_load_schema` in
  `text_to_sql.py` build their known-columns map from `information_schema` at import —
  never hardcode a table→column dict.
- **Examples in a prompt are a last resort, not a patch.** If an LLM generates wrong
  SQL, the first question is "does the prompt contain the schema knowledge it needed?"
  — fix that, do not bolt on a one-off example for each failing query.
- **Frozen files — never modify these (acceptance locked, no open backlog item):**
  `db/schema.sql`, `scripts/load_to_postgres.py`, `scripts/validate_load.py`,
  `scripts/generate_embeddings.py`, `scripts/init_s3_buckets.py`,
  `ai/query_router.py`.
  - **Logged frozen-file exceptions** (one-line each — file · reason · B-number · date):
    - `scripts/generate_embeddings.py` — IVFFlat `lists` made dynamic so a future re-embed sizes correctly for the grown corpus · B-051 · 2026-05-28.
- **Under active hardening — edit ONLY per a specific backlog item:**
  `ai/text_to_sql.py` (B-022 run_stored_sql, B-003 + B-059 _validate_columns,
  B-060 lint_cancellation_filter_missing + run() lint-retry hook, B-004 next),
  `ai/prompts/text_to_sql_system.txt` (B-003 schema context, B-022 SQL rules; edit whenever schema
  knowledge or SQL generation rules change — keep in sync with `text_to_sql.py`),
  `ai/main.py` (B-004, B-005; B-062 one-line `filters`-merge), `ai/semantic_search.py` (B-006 dedup, B-004, B-062 rating-based polarity filter `_detect_polarity` + rating bounds — mitigates L-012),
  `render/server.py` and `render/templates/dashboard.html` (B-022 cache, Show SQL,
  rename),
  `render/templates/explore.html` (B-061 — Explore preview surfaces the B-060
  cancellation-filter lint outcome as a non-blocking pin-time warning; the
  `lint_cancellation_filter` flag already flows through `/api/query`'s `result`,
  so B-061 is template-only — no `server.py` change),
  `scripts/stream_consumer.py` (B-031 resilience, B-032 heartbeat + per-type counts,
  B-038 bronze sink, B-039 silver sink, B-030 REVIEW accept done; B-031 next),
  `scripts/gold_lifecycle_updater.py` (B-040 done — gold lifecycle reconstruction),
  `scripts/review_embedder.py` (B-030b done — continuous micro-batch embedder; lock 7400050),
  `scripts/kafka_event_producer.py` (B-032 CHECKOUT emission, B-034 stateful
  lifecycle simulator, B-034A calendar replay simulator, B-030 REVIEW emission,
  **B-047 forward-generator rewrite done — calendar replay REMOVED, diurnal IST
  rate curve, data-aware net-new bookings, per-event-type fire-time stamps**;
  no open backlog item, but scope future edits to a named B-number),
  `scripts/chaos_injector.py` (B-047 extracted shared chaos generators; behaviour
  identical to the prior in-producer block — wire-contract change requires a new
  B-number).
  When editing these, scope the change to the named backlog item — do not
  refactor adjacent code.
- **Always create `__init__.py`** in every new Python package folder — without it,
  Python cannot find the module (`ai/`, `render/` both need one).
- **Cities are in `dim_location.city`** — never `hotel_master.city` (column does not exist).
- **Run modules correctly:** `python -m ai.main "question"` not `python ai/main.py`.
- **Run acceptance tests before declaring done** — do not skip any.
- **Fix the code if a test fails** — never modify the acceptance test to make it pass.
- **Never commit `.env`** — it is gitignored, keep it that way. Confirm with
  `git check-ignore .env` before any commit.
- **Never modify `data/`** — source files are read-only.
- **Migrations are append-only** — never edit any applied migration. The frozen
  set is whatever already exists in `db/migrations/`; treat every file in that
  directory as immutable and add a new numbered migration for any schema change.
- **`schema.sql` is the base; later columns live in migrations.** The frozen
  `db/schema.sql` captures the original 14-table star schema. Every later
  structural change — new columns, new tables, new indexes — ships as a numbered
  migration in `db/migrations/`; that directory is the current source of truth
  for the migration set. The actual current schema is `schema.sql` PLUS every
  file in `db/migrations/` — both together are the source of truth. Read both
  before writing anything LLM-facing that depends on a column existing; do not
  rely on an enumerated list here, because this bullet drifts the moment a new
  migration lands.

---

## Repo layout

> **Build/seed sequence:** for the single ordered sequence (Stage A base rebuild · Stage B B-046 additive expansion · Stage C post-load seeds for lifecycle history and embeddings), see [`datamodel.md` → "Regenerating the Dataset"](datamodel.md#regenerating-the-dataset). Never re-run `load_to_postgres` after Stage B or C — it TRUNCATEs everything.

```
travellens/
├── CLAUDE.md                        ← this file
├── README.md                        ← portfolio front door (links to the blueprint)
├── datamodel.md                     ← per-table schemas + migration history (003 → current)
├── index.html                       ← original technical blueprint (deep-dive, served via Pages)
├── run.py                           ← dev launcher (B-028 + B-030b + B-044 + B-045): up the stack + 6 host procs (consumer, simulator, dashboard, gold_lifecycle_updater, review_embedder, quarantine_hourly_rollup); --chaos; TAKES OVER on startup (B-045 newest-wins: _startup_cleanup() kills live run.py supervisor via port 47219 + all children by cmdline match; polls pg_locks until advisory locks 7400030/40/50/60 free; foreign-:5000 guard exits instead of blind-killing unrelated processes); then binds TCP 47219 as singleton (simultaneous double-start race → one exits "try again")
├── .claude/                         ← shared Claude Code config (settings.json, allowed tools, etc.)
├── docs/
│   ├── phase-0-setup.md             ← Phase 0 spec
│   ├── phase-1-postgres.md          ← Phase 1 spec
│   ├── phase-2-streaming.md         ← Phase 2 spec
│   ├── phase-3-embeddings.md        ← Phase 3 spec
│   ├── phase-4-ai-layer.md          ← Phase 4 spec
│   ├── phase-5-dashboard.md         ← Phase 5 spec
│   ├── phase-6-airflow.md           ← Phase 6 spec (current)
│   ├── backlog.md                   ← known issues + future work (L-numbers diagnosed here)
│   ├── capabilities-and-limits.md   ← what the AI layer does + reliability per feature (behavior reference)
│   ├── session-notes.md             ← short-lived handoff context between sessions
│   └── claude-code-prompts.md       ← prompts to start each phase
│   └── assets/
│       └── dashboard.png            ← dashboard screenshot used in README
├── ai/
│   ├── __init__.py                  ← required
│   ├── main.py                      ← entry point: answer(query) → result dict
│   ├── query_router.py              ← keyword classifier: sql vs semantic
│   ├── text_to_sql.py               ← Ollama → SQL → validate → Postgres
│   │                                  (+ run_stored_sql for frozen-SQL refresh,
│   │                                   + _load_schema / _validate_columns)
│   ├── semantic_search.py           ← MiniLM embed → pgvector → Ollama summary
│   │                                  (+ DISTINCT ON dedup)
│   ├── prompts/
│   │   └── text_to_sql_system.txt   ← schema DDL + India context (authoritative)
│   └── eval/                        ← B-058: on-demand Text-to-SQL accuracy eval harness
│       │                              (MEASUREMENT tool — NOT pytest, NOT part of `pytest tests/`).
│       │                              Runs fixture questions through ai.main.answer(), grades by
│       │                              EXECUTION MATCH vs an owner-verified reference query, flags
│       │                              L-011 (missing DISTINCT) + L-013 (missing cancellation filter).
│       │                              Fixture is TEST DATA — never fed back into the prompt.
│       ├── __init__.py              ← required
│       ├── eval_questions.py        ← fixture: QUESTIONS = [{question -> reference_sql}, ...]
│       ├── run_eval.py              ← runner: `python -m ai.eval.run_eval` (--runs N / --only sql / --id X)
│       └── results/                 ← written eval logs (gitignored)
├── render/
│   ├── __init__.py                  ← required
│   ├── server.py                    ← Flask app: pages + API (pin freezes SQL,
│   │                                  refresh serves cache or runs frozen SQL;
│   │                                  + /monitor + /monitor/data routes, Phase 7
│   │                                  B-027 base + B-029/B-032 live-pulse redesign)
│   ├── widget_renderer.py           ← result shape → Chart.js config
│   └── templates/
│       ├── base.html                ← shared nav + layout
│       ├── dashboard.html           ← pinned widgets grid (+ Show SQL modal, rename)
│       ├── explore.html             ← chat interface + widget preview
│       ├── monitor.html             ← Phase 7 monitor (B-027 + B-029 + B-032 Chunk 4):
│       │                              header live pulse, default-today filter,
│       │                              EVENTS/QUARANTINE/HEALTH sections, SOON
│       │                              placeholders, 10s in-place auto-refresh;
│       │                              HEALTH = Airflow scheduler only (STREAM
│       │                              FRESHNESS tile removed — owner decision)
│       └── about.html               ← product page
├── scripts/
│   ├── generate_embeddings.py       ← Phase 3: batch embed reviews_raw
│   ├── semantic_playground.py       ← Phase 3: interactive semantic search test
│   ├── load_to_postgres.py          ← Phase 1: bulk loader
│   ├── validate_load.py             ← Phase 1: 20-check validator
│   ├── stream_consumer.py           ← Phase 2 + B-032 Ch.2 + B-030: five parallel sinks off the accept path:
│   │                                  (1) Postgres agg UPSERT to agg_hourly_city_stats per window flush;
│   │                                  (2) S3 agg Parquet to processed/agg/hourly_city_stats/...;
│   │                                  (3) B-038 bronze: raw JSONL to s3://.../raw_events/year=/month=/day=/hour=/,
│   │                                      batched (BRONZE_BUFFER_CAP=500 OR FLUSH_CHECK_SECONDS tick),
│   │                                      ingest-time partitioning, post-Gate-3 (excludes PRICE_CHANGE);
│   │                                  (4) B-039 silver: typed INSERT into fact_booking_events with source='stream'
│   │                                      via execute_values + ON CONFLICT (event_id) DO NOTHING, page_size=len
│   │                                      (so cur.rowcount is honest), pre-Gate-3 so ALL 5 event types land
│   │                                      including PRICE_CHANGE (booking_id/customer_id NULL on those).
│   │                                  (5) B-030 REVIEW: intercepts AFTER Gate 4, BEFORE silver buffer append;
│   │                                      routes to bronze (raw archive) + reviews_raw (record_source='stream')
│   │                                      via review_flush(); ON CONFLICT (review_id) DO NOTHING; then `continue`
│   │                                      — REVIEW never touches silver/agg/gold.
│   │                                  Plus pipeline_metrics heartbeat every ~10s. Each sink wrapped in its own
│   │                                  try/except so any one failure logs + drops the batch + bumps a counter,
│   │                                  never crashes the consumer. Gate order: 1 (JSON) → 2 (schema) →
│   │                                  parse-ts → 4 (late) → REVIEW → SILVER → 3 (type filter) → BRONZE → accumulator.
│   ├── kafka_event_producer.py      ← Phase 2 + B-047 (Stage 2a): DATA-AWARE FORWARD GENERATOR. Replaces the
│   │                                  prior calendar-replay simulator entirely (no sim-clock; .sim_clock.json
│   │                                  is deleted on first launch and never re-created). Wall-clock `event_ts`
│   │                                  = `datetime.now(UTC)`; `event_date` = today in IST. Token bucket paced
│   │                                  at `effective_rate(h_IST) = BASE_RATE(10) × DIURNAL_HOUR_MULT[h] ×
│   │                                  rate_multiplier` (Σ=24.10 → mean ≈1.004×, peak 2.05× @ 19 IST; daily
│   │                                  integral ≈ 868K at x=1 vs 1M cap → 13% headroom). Each tick (~200ms)
│   │                                  drains lifecycle-priority then fills with BOOKING vs PRICE_CHANGE by
│   │                                  per-hour weight `w_b/(w_b+w_p)` — no 95/5 coin flip; daily mix shapes
│   │                                  itself ≈ 48/52 BOOKING/PC at x=1. New BOOKINGs sample real city ×
│   │                                  season → hotel under per-night occupancy cap → room type fitting
│   │                                  num_guests → customer (75% out-of-state) → lead-time mix (50% ≤7d /
│   │                                  35% 1-8wk / 15% 2-8mo) → nights from empirical dist → price = base ×
│   │                                  weekend/holiday/season Gaussian noise → revenue = nightly × nights
│   │                                  (asserted invariant). Each new BOOKING stamps `checkin_fire_ts`,
│   │                                  `checkout_fire_ts`, and (12% deterministic) `cancel_fire_ts` from
│   │                                  per-event-type IST hour distributions (CHECKOUT 8-13 peak 9-10, CHECKIN
│   │                                  12-21 peak 16-18, CANCELLATION 9-21 evening-lean). On CHECKOUT/CANCEL
│   │                                  emit, REVIEW is DEFERRED to tonight 20-23 IST via `review_fire_ts`;
│   │                                  row state → `'REVIEW_PENDING'` until REVIEW fires, then deleted.
│   │                                  Catch-up rule: overdue fire-times at startup drain at the bucket cap
│   │                                  with `event_ts = NOW()` — never backdated. `sim_daily_counter`
│   │                                  (migration 014) is the per-day TOTAL-EVENTS guardrail; on cap-hit the
│   │                                  producer sleeps until IST midnight. All per-tick DB mutations
│   │                                  committed in ONE batch (~5/sec at x=1, not per-event). Single throughput
│   │                                  knob: `--rate-multiplier` (int>1; any invalid value — non-int, ≤1,
│   │                                  negative, alpha — silently falls back to 1; same coerce rule for
│   │                                  `RATE_MULTIPLIER` env var). Folds in B-041 (the prior `--sim-rate` /
│   │                                  `--rate` / `--duration` / `--sim-speed` no-op flags REMOVED entirely
│   │                                  from `run.py` + producer; not retained as accepted-and-ignored).
│   │                                  Chaos extracted to `scripts/chaos_injector.py` (shared with future
│   │                                  producers; behaviour byte-identical to the prior in-producer block).
│   │                                  Single-instance: pg_try_advisory_lock(7400030); second instance exits 1.
│   ├── chaos_injector.py            ← B-047: SHARED CHAOS GENERATORS — extracted verbatim from the prior
│   │                                  kafka_event_producer.py. Reason vocabulary preserved (`missing_field`,
│   │                                  `unknown_event_type`, `unknown_city`, `unparseable_event_ts`,
│   │                                  `unparseable_json`, `late`); changing it is a wire-contract change
│   │                                  that requires a new B-number. Imported by the forward generator;
│   │                                  ready for re-use by any future producer.
│   ├── generate_lifecycle_history.py ← B-035: TIME-PARTITIONED BACKFILL — one sweep over ALL fact_bookings.
│   │                                  Routes each row vs --sim-today (default 2025-06-01) into 4 buckets:
│   │                                  COMPLETED (BOOKING+CI+CO or BOOKING+CANCEL), IN_PROGRESS (BOOKING+CI →
│   │                                  sim_open CHECKED_IN), BOOKED (BOOKING → sim_open BOOKED), or FUTURE
│   │                                  (SKIP — booking_ts >= sim-today; ~388K bookings reserved for the
│   │                                  stream simulator to replay). sim_open_bookings backlog is REAL (every
│   │                                  row is a real fact_bookings booking). Idempotent --reset deletes ONLY
│   │                                  source='history'. Run via `python -m scripts.generate_lifecycle_history`.
│   ├── gold_lifecycle_updater.py    ← B-040: GOLD LAYER MICRO-BATCH — continuous ~1-min process DECOUPLED from
│   │                                  the consumer. Reads fact_booking_events WHERE ingest_seq > gold_watermark,
│   │                                  groups by booking_id, sorts intra-batch by lifecycle order (BOOKING<CHECKIN<
│   │                                  CHECKOUT/CANCELLATION), applies forward-only status machine (never regresses),
│   │                                  flags illegal BUSINESS-TIMESTAMP inversions (NOT processing-order artifacts),
│   │                                  upserts to fact_booking_lifecycle, advances gold_watermark. Excludes
│   │                                  PRICE_CHANGE + REVIEW (no booking_id / hotel-level not per-booking).
│   │                                  Atomic commit per batch. Deadlock-resilient (rollback + retry 10s).
│   │                                  Run ONE instance only: `python -m scripts.gold_lifecycle_updater`.
│   ├── review_generator.py          ← B-030: SHARED REVIEW GENERATION UTILITIES — pure functions (no I/O),
│   │                                  deterministically seeded. Used by generate_review_backfill.py (history)
│   │                                  and kafka_event_producer.py (stream). Implements: generate_rating,
│   │                                  should_review, pick_stage, pick_channel, pick_event_date, pick_text,
│   │                                  make_review_event_dict. india-aware reason bank + Kaggle seed text blend.
│   │                                  REVIEW_PROPENSITY_SCALE=0.47 (B-030a) → ~15% overall review rate.
│   │                                  Tune this single constant to change the rate; shape is _P_REVIEW_BASE.
│   ├── generate_review_backfill.py  ← B-030: HISTORY REVIEW BACKFILL — sweeps fact_bookings WHERE booking_ts
│   │                                  < sim-today (default 2025-06-01), calls make_review_event_dict per
│   │                                  booking, INSERTs with record_source='history'. 90,980 reviews / 612,380
│   │                                  bookings (14.9%). Idempotent via uuid5 review_id + ON CONFLICT DO NOTHING.
│   │                                  --reset deletes WHERE record_source='history' before re-run.
│   │                                  Run: `python -m scripts.generate_review_backfill`.
│   ├── review_stats.py              ← B-030a: READ-ONLY REVIEW DIAGNOSTICS — prints counts by source/stage,
│   │                                  overall review rate, hotel_id consistency (0 mismatches expected),
│   │                                  no_show stage count (0 expected), avg rating by star_category gradient.
│   │                                  No writes. Run: `python -m scripts.review_stats`.
│   ├── review_embedder.py           ← B-030b: CONTINUOUS REVIEW EMBEDDER — micro-batch process, loads
│   │                                  all-MiniLM-L6-v2 once (same 384-d model as generate_embeddings.py and
│   │                                  semantic_search.py — vectors share one space). SELECTs up to
│   │                                  EMBED_BATCH_SIZE=2000 NULL rows per pass, encodes in chunks of 128,
│   │                                  writes back via executemany UPDATE, commits per batch. Sleeps
│   │                                  EMBED_INTERVAL_SECONDS=15 when backlog is clear. On first "caught up"
│   │                                  poll logs Part B instructions: run representative query BEFORE, DROP+
│   │                                  CREATE index with lists=120 (for ~121K rows), run same query AFTER.
│   │                                  Single-instance: pg_try_advisory_lock(7400050); second instance exits 1.
│   │                                  Run ONE instance only: `python -m scripts.review_embedder`.
│   ├── review_sentiment_scorer.py   ← B-026 (Stage 1): PER-REVIEW SENTIMENT SCORER — micro-batch process
│   │                                  mirroring review_embedder.py. Classifies reviews_raw WHERE
│   │                                  sentiment_label IS NULL with CardiffNLP twitter-roberta-base-sentiment-
│   │                                  latest (3-class pos/neg/neutral), pinned to safetensors revision
│   │                                  d616e2bd…, loaded via use_safetensors=True (the .bin checkpoint is
│   │                                  blocked on torch 2.3.0 by CVE-2025-32434; torch is NOT upgraded — L-008).
│   │                                  Writes sentiment_label + sentiment_score (migration 017); INDEPENDENT of
│   │                                  the embedding (never touches the 384-d vectors or the IVFFlat index).
│   │                                  `--once` = one-time 133K backfill then exit; bare = continuous loop,
│   │                                  sleep when caught up. Single-instance: pg_try_advisory_lock(7400070);
│   │                                  second instance exits 1. NOT yet a run.py proc (that is Stage 3). Run:
│   │                                  `python -m scripts.review_sentiment_scorer --once`.
│   ├── quarantine_hourly_rollup.py  ← B-044: HOURLY QUARANTINE ROLLUP — 5-min loop run.py proc (6th).
│   │                                  Each cycle: watermark = MAX(is_final=TRUE row) in quarantine_hourly_summary;
│   │                                  from watermark+1 to current hour: re-counts S3 objects via
│   │                                  list_objects_v2 KeyCount (RAM-safe, never materialises key list);
│   │                                  GRACE_MINUTES=10 — an hour is sealed (is_final=TRUE) only once
│   │                                  now >= end_of_hour + 10min; open hours re-counted each cycle.
│   │                                  Explicit 0/0/TRUE rows for empty completed hours so watermark
│   │                                  advances contiguously. First run on empty table: walks year=/month=/
│   │                                  day=/hour= virtual dirs (O(8) API calls) to find earliest quarantine
│   │                                  hour and backfills forward. Eager first cycle on startup.
│   │                                  Supersedes B-033 Airflow DAG + quarantine_daily_summary (migration 012).
│   │                                  Single-instance: pg_try_advisory_lock(7400060); second instance exits 1.
│   │                                  Run: `python -m scripts.quarantine_hourly_rollup`.
│   ├── build_cities_expansion_csv.py ← B-046: deterministic builder (SEED=42) — emits seeds/cities_expansion.csv from inline curated catalog (949 cities, 34 states, 7 zones).
│   ├── expand_dimensions.py         ← B-046: STAGE 1 DIMENSION EXPANSION — additive insert (no migration).
│   │                                  Reads seeds/cities_expansion.csv; INSERTs ~949 dim_location + ~18K
│   │                                  hotel_master + ~50K dim_room_type + 80K dim_customer rows in a single
│   │                                  transaction. Pre-flight aborts if any catalog (city, state) is already in
│   │                                  dim_location, or if hotel_master/dim_customer max-id is past expansion start.
│   │                                  NEVER run load_to_postgres.py against this DB — it TRUNCATEs everything.
│   │                                  Run ONCE: `python -m scripts.expand_dimensions`.
│   └── init_s3_buckets.py           ← Phase 2: MinIO bucket bootstrap
├── ai/                              (see above)
├── db/
│   ├── schema.sql                   ← 14-table star schema (frozen)
│   └── migrations/
│       ├── 003_dashboard_widgets.sql      ← Phase 5: dashboard state table
│       ├── 004_widget_settings.sql        ← Phase 5: width column
│       ├── 005_widget_cache.sql           ← B-022: generated_sql + last_result_json
│       ├── 006_hotel_opened_year.sql      ← hotel_master.opened_year (entity-count queries)
│       ├── 007_pipeline_live_metrics.sql  ← B-032: pipeline_metrics + 4 new agg_hourly_city_stats count cols
│       ├── 008_lifecycle_events.sql       ← B-035: fact_booking_events (silver event ledger) + sim_open_bookings (simulator state)
│       ├── 009_gold_lifecycle.sql         ← B-040: ingest_seq cursor on fact_booking_events + fact_booking_lifecycle gold table + gold_watermark cursor
│       ├── 010_review_stream.sql          ← B-030: 7 new columns on reviews_raw (booking_id, customer_id, review_stage, review_channel, event_ts, event_date, record_source)
│       ├── 011_reviews_event_date_index.sql ← B-030b: index on reviews_raw.event_date
│       ├── 012_quarantine_daily_summary.sql ← B-033 [RETIRED — dropped by 013]: quarantine_daily_summary; superseded by hourly table
│       ├── 013_quarantine_hourly_summary.sql ← B-044: drops quarantine_daily_summary; creates quarantine_hourly_summary (summary_date DATE + summary_hour SMALLINT PK, malformed_count, late_count, is_final BOOL, computed_at)
│       ├── 014_sim_daily_counter.sql        ← B-047: sim_daily_counter (counter_date PK, events_emitted, cap, updated_at); cap = 1_000_000 × rate_multiplier; counter_date is in IST
│       ├── 015_lifecycle_fire_times.sql     ← B-047: adds 4 TIMESTAMPTZ cols on sim_open_bookings (checkin_fire_ts, checkout_fire_ts, cancel_fire_ts, review_fire_ts), 4 partial indexes, and extends state CHECK to include 'REVIEW_PENDING'
│       ├── 016_sim_daily_counter_cap_upsert.sql ← B-047 follow-on: COMMENT-only ledger entry; producer's startup UPSERT flipped from ON CONFLICT DO NOTHING (sticky cap) → DO UPDATE SET cap=EXCLUDED.cap (last-write-wins across same-day sessions). events_emitted unchanged.
│       └── 017_review_sentiment.sql      ← B-026 (Stage 1): 2 nullable cols on reviews_raw (sentiment_label VARCHAR(8), sentiment_score NUMERIC(4,3)) + partial index idx_reviews_raw_sentiment; populated by scripts/review_sentiment_scorer.py. Additive — no re-embed.
├── docker/
│   ├── postgres.Dockerfile          ← Postgres 16 + pgvector
│   └── docker-compose.yml           ← postgres + kafka + zookeeper + minio
│                                      (+ airflow + airflow-postgres once Phase 6 infra lands)
├── airflow/
│   ├── dags/                        ← Phase 6 remaining DAGs (B-024, B-013, B-014, B-015, B-016); quarantine_daily_rollup.py RETIRED (B-044)
│   └── plugins/
├── seeds/                           ← committed reference data (B-046).
│   └── cities_expansion.csv         ← 949 curated cities (city, state, region, tourism_zone, lat, lng,
│                                      tourist_arrivals_annual_m, peak_months, popularity_tier).
│                                      Source of truth for the additive Stage 1 expansion. Regenerable from
│                                      scripts/build_cities_expansion_csv.py.
├── data/                            ← gitignored — 12 CSVs + 1 JSON seed
└── tests/
```

---

## Documentation conventions

### Single source of truth hierarchy

One place per detail — cross-reference, never restate. Restating current state in a second file
creates two sources that drift apart.

| Document | Owns |
|---|---|
| `CLAUDE.md` | Authoritative current state: phase status, hard rules, repo layout, common mistakes, design decisions, quality gate. If it's a rule or a current fact, it lives here. |
| `datamodel.md` | Schema source of truth: per-table columns, types, FKs, migration history. Every LLM-facing prompt that touches the data model must be built from here or from `information_schema`, never from memory. |
| `backlog.md` | Work items: B-/L- IDs, open items, in-progress items, completed table. Status and history of every planned or shipped change. |
| `docs/phase-*.md` | Build history and narrative: how each phase was originally built and how it evolved. These are HISTORY documents, not current specs. |
| `README.md` | Portfolio entry point: what the project is, where to start, links out. |
| `docs/claude-code-prompts.md` | Phase starters: prompt templates for beginning each phase with Claude Code. |
| `docs/capabilities-and-limits.md` | AI-layer behaviour + per-feature reliability reference: what each query path does, accuracy levels, known caveats (L-numbers), and what is explicitly out of scope. Update this when a backlog item changes observable AI behaviour. |

> `docs/session-notes.md` is ephemeral handoff context between sessions — gitignored, not part of
> the canonical hierarchy, and intentionally discarded once its context is absorbed into code or
> a backlog item.

### Phase docs are build history, not current spec

Phase docs record how a phase was built and how it evolved — they are not kept current with the
live system. Every phase doc carries a **HISTORY DOCUMENT** banner immediately after the status
line, naming where current truth lives:

```
> **HISTORY DOCUMENT** — This records how Phase N was originally built and how it evolved.
> For current behaviour, see [CLAUDE.md](../CLAUDE.md) · [datamodel.md](../datamodel.md) · [backlog.md](backlog.md).
```

The banner must name the specific SSoT destinations (CLAUDE.md / datamodel.md / backlog.md) — not
just say "this is history."

**Inside the phase doc:**

- The original build runbook is kept intact and labeled `## ARCHITECTURE DECISIONS (ORIGINAL)` for
  any decisions section.
- Steps that were superseded by later backlog items are marked `[SUPERSEDED]` with an inline note
  pointing to the Build History entry that replaced them.
- All post-acceptance evolution (new features, redesigns, hardening) goes in a
  `## BUILD HISTORY / EVOLUTION` section at the bottom of the doc (before `## NEXT`), in
  chronological order, earliest first. Each entry cites its B-number and summarizes what changed
  and why — it does NOT restate current behaviour inline (that would re-duplicate and drift).
- Current behaviour is NEVER restated in the phase doc body — the body stays as the original build
  record; the BUILD HISTORY entry names the change; CLAUDE.md / datamodel.md carry the live state.
- A forward-looking spec (phase not yet shipped, or phase in-progress with no completed evolution)
  uses a banner that says "planned build steps" and carries **no BUILD HISTORY section** until at
  least one component has shipped and been superseded.

### Cross-doc traceability — trace tags on backlog items

A reader landing on any node — issue, phase, datamodel, sim data — must be able to reach every other node in one hop. Every backlog item (B-XXX or L-XXX) carries a single-line trace tag at the top of its full body section, in this exact shape:

```
> **Trace.** Phase(s): <phase-N or "ops/dev"> · datamodel: <tables / sections / "none"> · data: <gen/seed scripts, seed CSVs, or "none">
```

**Reciprocity rule** — every doc the trace tag names must back-reference the B-XXX ID:
- A phase doc named in `Phase(s):` has the item ID in its build-history (or main flow) entry.
- A datamodel section named in `datamodel:` mentions the item ID inline.
- A script/seed file named in `data:` is referenced in `CLAUDE.md` repo layout or `datamodel.md` "Regenerating the Dataset" with the item ID.

Tags are **cross-references, not restatements** — they tell readers where to look, never duplicate the content. The single source of truth for each detail still lives in its owning doc (per the hierarchy table above).

### Editing docs

- **Preserve substance** — reorder and reframe, never amputate. Decisions, lessons, acceptance
  numbers, backlog refs, and evolution history are deliberate kept assets.
- **Sync the same turn** — when a section is renamed or moved, fix every cross-reference to it in
  the same edit session. Do not leave dangling links.
- **Verify anchors resolve** — after any section rename or doc restructure, grep all docs for links
  with `#` anchors and confirm every target heading still exists. Report 0 dangling before declaring
  done.
- **Docs only** — doc edits never touch code, scripts, SQL, configs, or code comments. When a doc
  edit session is complete, surface for owner review; the owner commits.

---

## Common mistakes — always avoid these

Full column schemas for all tables below are in `datamodel.md`.

| Mistake | Correct approach |
|---|---|
| Forgetting `__init__.py` in new package | Create it (empty) immediately when making a new package folder |
| Using `hotel_master.city` in SQL | Always `dim_location.city` via JOIN on `location_id` |
| Querying `agg_daily_hotel_kpi` for booking counts | Use `fact_bookings` — KPI table is empty until B-013 DAG runs |
| Steering Explorer queries to `agg_hourly_city_stats` | Stream rollup is populated but is for `/monitor` only — NOT Explorer. Business booking/revenue/cancellation analytics → `fact_bookings`. |
| Routing Explore tab between historical and live fact tables (B-048) | Standing convention enforced by `ai/prompts/text_to_sql_system.txt` "HISTORICAL vs LIVE" section. Two-rule order: (1) questions naming `today / now / current / live / streaming / so far today / right now / last <N> hour(s) / last <N> minute(s) / this hour / since midnight` → `fact_booking_events` with `source='stream'`; (2) everything else (including `this month` / `this year` / `in 2025` / `monthly` / `by year`) → `fact_bookings` via dim_date. `recent` alone stays historical (ambiguous). When editing the prompt, preserve the two-rule order; do not bolt on per-question examples. **Paired with DATE PARADIGMS rule (also B-048):** the "DATE HANDLING" section enforces two non-interchangeable date paradigms — `fact_bookings` uses `JOIN dim_date d ON b.date_id = d.date_id` for year/month/season/quarter/weekend/holiday filters (canonical surrogate-key path; `dim_date` has no `date_key` / `month_number` / bare `day` columns), while `fact_booking_events` uses `event_date` / `event_ts` directly on the row with NO `dim_date` join. The two paradigms bleed across tables on Qwen2.5-Coder-7B — model frequently invents `b.event_date` / `b.booking_date` on `fact_bookings`; the rule plus the B-001 retry loop catches most cases (5e ~4/5 pass rate; residual is an intermittent chatty-mode flake, not a column hallucination). Do NOT add `booking_ts` / `event_ts` to the `fact_bookings` SCHEMA listing — verified to make the model attempt `JOIN dim_date ON b.booking_ts = d.date_id` and break historical year-filter queries (H1). |
| Cancellation-rate formula stacks with default exclude-cancelled filter (B-048) | The default rule `WHERE NOT b.is_cancelled` MUST NOT apply when the query computes a rate/ratio/percentage/share that needs both cancelled and non-cancelled rows in the denominator. Formula is `ROUND(100.0 * COUNT(*) FILTER (WHERE b.is_cancelled) / NULLIF(COUNT(*),0), 2)` — apply NO `WHERE` filter on `is_cancelled`. Stacking returns 0.00% always (numerator filtered out). Same rule applies to cancellation %, % cancelled, share of bookings cancelled, ratio of cancellations. |
| Short words in `SEMANTIC_TRIGGERS` | Minimum ~5 chars or multi-word phrases — `"hot"` matches inside `"hotels"` |
| Running `python ai/main.py` | Always `python -m ai.main` from repo root |
| Building IVFFlat index before all embeddings written | Always embed first, index last |
| Replacing docker-compose.yml | Add new services to existing file — never replace |
| Querying Postgres from `render/server.py` for data | All data through `ai.main.answer()` — only widget state in server.py |
| Hardcoding schema / column lists | Build from `information_schema` at runtime (see `_load_schema` / `_validate_columns` in `text_to_sql.py`) |
| Editing an existing migration | Migrations are append-only — add a new numbered file |
| `agg_daily_hotel_kpi` column names | Real columns: `total_bookings`, `total_revenue_inr`, `avg_nightly_rate_inr`, `cancellation_rate`, `avg_rating` — no `occupancy_rate`, no `revpar_inr` |
| `chain_name` treated as always present | `chain_name` is NULL for ~60% (independents) — exclude NULL when ranking chains |
| Counting entities via `fact_bookings` rows | "How many hotels", "hotels per city", "hotels opened per year" → query `hotel_master` directly. `fact_bookings` is for booking ROWS, not entity counts. |
| `hotel_master.opened_year` | `opened_year SMALLINT` added in migration 006. Range **1975–2026** (original 2,000 hotels were 1975–2023, correlated with `star_category`; B-046 expansion uses 1975–2026 freely — migration 006 carries no CHECK constraint). Use directly for "hotels opened per year" — never derive opening year from `fact_bookings` dates. |
| Post-B-046 row counts | `dim_location` 993 · `hotel_master` 20,076 · `dim_room_type` 55,446 · `dim_customer` 100,000 · `fact_bookings` 1,000,000 (unchanged). New ID ranges: hotels HTL-002001…HTL-020076, customers CUST-020001…CUST-100000. ALL fact tables UNCHANGED by the expansion. |
| New `dim_room_type.type_name` values (B-046) | `Houseboat Suite` (Backwater hotels), `Tent` (Wildlife + Hill Station), `Treehouse` (Wildlife) — free text, no migration. Total 16 distinct names (was 13). |
| New `hotel_master.property_type` values populated (B-046) | `Houseboat` (205), `Treehouse` (186), `Tent` (688) — gated by `tourism_zone` in `scripts/expand_dimensions.py`. Free text, no CHECK constraint. |
| `dim_location.tourist_arrivals_annual_m` interpretation | **Hotel-demand proxy, NOT literal Ministry-of-Tourism footfall.** Capped at 24.0 by the expansion script — pilgrimage mega-sites (Tirupati, Sabarimala) get 50-80m real visitors/year but that doesn't translate to bookable hotel demand. Used as a city-popularity weight for the booking generator. See `datamodel.md` for the formula. |
| Running `load_to_postgres.py` after B-046 | **Don't.** It TRUNCATEs every table (it's frozen and was designed for the initial bulk load). Doing so wipes the 993 cities / 20,076 hotels / 55,446 room types / 100,000 customers / 1M bookings / 2M+ lifecycle events / 762K gold rows / 133K reviews. Use `scripts/expand_dimensions.py` for additive growth; never load_to_postgres for any operation on the live DB. |
| `is_cancelled` scope | `is_cancelled` lives ONLY on `fact_bookings` — not on dimensions or `reviews_raw`. Exclude cancelled bookings by default (`WHERE NOT b.is_cancelled`) unless the question is specifically about cancellations. |
| `agg_hourly_city_stats` column drift | Post-migration 007 columns: `city`, `window_start`, `window_end`, `total_bookings`, `total_revenue_inr`, `avg_occupancy_rate`, `cancellation_rate`, `ingestion_ts`, `total_checkins`, `total_checkouts`, `total_cancellations`, `total_reviews`. `total_reviews` stays NULL — REVIEW events are routed to reviews_raw directly and do NOT feed the city-level agg accumulator. |
| `pipeline_metrics` table | Append-only heartbeat every ~10s (migration 007). Counters are **cumulative-since-start** — derive events/sec as a delta between adjacent rows. Drop deltas where newer < older (consumer restart reset). `consumer_lag` is reserved/NULL. |
| `fact_booking_events` / `sim_open_bookings` (migration 008, extended by 015) | Silver ledger spans history AND stream; `source` ('history'\|'stream') is the only separator. BOOKING invariant: `revenue_inr == nightly_rate_inr * nights`. `sim_open_bookings` is mutable simulator state. Under B-047 it carries 4 fire-time cols (`checkin_fire_ts`, `checkout_fire_ts`, `cancel_fire_ts`, `review_fire_ts`) + a new state `'REVIEW_PENDING'` between CHECKOUT/CANCELLATION emit and REVIEW emit; rows are deleted on REVIEW emit (or sooner if the negativity-bias draw declines the review). |
| `--sim-today` anchor + FUTURE bucket | Default anchor **`2025-06-01`**. ~388K bookings with `booking_ts >= sim-today` are FUTURE — skip in history generator, reserved for stream replay. Do not write history events for them. |
| Backfilling lifecycle events with arbitrary scripts | Use `scripts/generate_lifecycle_history.py`. Never INSERT directly into `fact_booking_events` — script enforces FK validity, revenue invariant, source='history' tag, and BOOKING-for-every-followup rule. Consumer is the only stream-side writer (`source='stream'`). |
| ~~Calendar replay producer (`scripts/.sim_clock.json`)~~ — superseded by B-047 | The forward generator (B-047) has NO sim-clock; `.sim_clock.json` is deleted on first launch and never re-created. Don't try to resurrect it; the producer no longer reads it. |
| Producer enum/vocab keys assumed from naming convention (B-049) | Producer dicts keyed on a DB column's enum vocabulary (e.g. `SEASON_MULT` on `dim_date.season`, `BOOKING_SOURCE_MIX` on `fact_bookings.booking_source`, channel mixes on `reviews_raw.review_channel`) MUST match the real values returned by `SELECT DISTINCT <col>` — verify before writing or editing them. **`dict.get(key, default)` patterns silently neutralize unknown keys**: in B-049 the producer's `SEASON_MULT` was keyed `{Summer, Winter, Monsoon, Spring, Autumn}` while the real `dim_date.season` is `{Peak, Shoulder, Summer, Monsoon}` — `Peak` (1,059 days, the entire winter peak season) and `Shoulder` fell through to the 1.0 default for months before anyone noticed. Same trap caught `_compute_price`'s `elif season == "Winter"`. The producer now resolves season through `_season_mult()` which warns once-per-unknown-value to stderr; future drift surfaces loudly. Same principle as the existing hard rule "Build all LLM-facing content from the real schema, never from memory" — apply to producer enum keys too. |
| Forward producer (`scripts/kafka_event_producer.py`, B-047) | Loop is wall-clock-only, paced by a 24-vector IST `DIURNAL_HOUR_MULT` × `rate_multiplier`. Tick = 200 ms; per-tick mutations to `sim_open_bookings` + `sim_daily_counter` are committed in ONE batch. Single throughput knob `--rate-multiplier` (integer >1; any invalid value silently → 1; same coerce on `RATE_MULTIPLIER` env). Run: `python -m scripts.kafka_event_producer` or `python -m scripts.kafka_event_producer --rate-multiplier 3`. Single-instance: `pg_try_advisory_lock(7400030)`. |
| `sim_daily_counter` (migration 014 + 016 cap-upsert, B-047) | Per-day TOTAL-EVENTS guardrail. `counter_date` is in IST. On cap-hit (`events_emitted >= cap`) the producer sleeps until IST midnight ("goes silent"). `cap = 1_000_000 × rate_multiplier`; **LAST-WRITE-WINS across same-day sessions** (migration 016 — every producer-startup UPSERT does `ON CONFLICT DO UPDATE SET cap = EXCLUDED.cap`). `events_emitted` is NOT in the SET clause and continues to accumulate across all sessions in the IST day. So `python run.py --rate-multiplier 3` after a bare `python run.py` jumps today's cap from 1M to 3M immediately. |
| Per-event-type fire-time stamps (migration 015, B-047) | When a BOOKING is emitted, its `checkin_fire_ts`, `checkout_fire_ts`, and optionally `cancel_fire_ts` are sampled in IST from per-type hour distributions (CHECKOUT 8-13 peak 9-10, CHECKIN 12-21 peak 16-18, CANCELLATION 9-21 evening-lean). `review_fire_ts` is NULL until CHECKOUT or CANCELLATION emits, then sampled in tonight's 20-23 IST window with state→`'REVIEW_PENDING'`. Catch-up: if a fire_ts is already past at restart, the event fires at the next bucket slot with `event_ts = NOW()` — original stamped time NEVER written to the wire. |
| Every emitted event carries TWO timestamps | `event_ts` = wall-clock UTC (consumer windowing/SLOs); `event_date` = today in IST (business analytics). Under B-047 event_date is always *today* — no more sim-day. Gate 2 ignores unknown fields — `event_date` passes harmlessly. |
| Bronze archive — PRICE_CHANGE not bronzed | Accepted events (post-Gate-4) → `s3://.../raw_events/` (INGEST-time partitioned JSONL). PRICE_CHANGE filtered at Gate 3 before bronze. Malformed/late → their own S3 paths. Bronze is best-effort — failure logs and continues. |
| `fact_booking_lifecycle` / `gold_watermark` (migration 009) | Gold layer — one row per `booking_id`, forward-only machine: BOOKED→CHECKED_IN→COMPLETED/CANCELLED. `illegal_transition_flag` fires on business-timestamp inversions (CHECKOUT before CHECKIN, CANCELLATION after CHECKOUT) — NOT processing-order artifacts. Advisory lock (`pg_try_advisory_lock(7400040)`) prevents duplicate instances; second instance exits code 1. Kill stuck instance: `Get-WmiObject Win32_Process \| Where-Object { $_.CommandLine -like '*gold_lifecycle_updater*' } \| ForEach-Object { Stop-Process -Id $_.ProcessId -Force }`. Full schema: `datamodel.md`. **KNOWN FALSE POSITIVES (B-043): `illegal_transition_flag` currently has ~5,151 false positives.** Root cause: the gold state machine is batch-local (never reads existing `checkin_ts` from `fact_booking_lifecycle`), so a stream CHECKOUT arriving in a later gold batch than its history CHECKIN is flagged illegal even though the data is correct. The UPSERT then unconditionally overwrites the prior correct `FALSE` with the batch-local `TRUE` (line ~333 in `gold_lifecycle_updater.py`). **Do NOT trust `illegal_transition_flag = TRUE` as a data quality signal until B-043 lands.** |
| `reviews_raw` new columns (migration 010 / B-030) | 7 new columns added: `booking_id` (UUID), `customer_id` (VARCHAR(12)), `review_stage` (VARCHAR(20)), `review_channel` (VARCHAR(100)), `event_ts` (TIMESTAMPTZ), `event_date` (DATE), `record_source` (VARCHAR(10) NOT NULL DEFAULT 'seed'). Original 30 K Kaggle rows have `record_source='seed'`, new columns NULL. History backfill wrote 90,980 rows with `record_source='history'` (14.9% of 612,380 eligible bookings — B-030a tuned REVIEW_PROPENSITY_SCALE=0.47). Stream reviews land with `record_source='stream'`. REVIEW never enters `fact_booking_events` or the agg accumulator. |
| REVIEW routing in consumer | REVIEW is intercepted AFTER Gate 4 (late-guard) and BEFORE the silver buffer append. It is routed to: (a) bronze raw archive, (b) `reviews_raw` via `review_flush()`. After routing, `continue` skips silver/Gate-3/bronze-agg/accumulator. REVIEW validation in `validate_event` requires `hotel_id, review_id, booking_id, customer_id, review_stage, review_channel, rating` — NO city field (REVIEW has no city). |
| `quarantine_hourly_summary` (migration 013 / B-044) | One row per UTC hour, PK `(summary_date, summary_hour)`. `is_final=TRUE` once `now >= hour_end + 10-min grace`; open/grace hours stay `FALSE` and are re-counted each 5-min cycle. Derived watermark: `MAX(is_final=TRUE)` row — no separate cursor table. Monitor read: `SELECT summary_date, SUM(malformed_count), SUM(late_count) FROM quarantine_hourly_summary WHERE summary_date BETWEEN %s AND %s GROUP BY summary_date` — O(1), no S3 on request path. Populated by `scripts/quarantine_hourly_rollup.py` (run.py 6th proc, lock 7400060). `quarantine_daily_summary` (migration 012, B-033) dropped by this migration. |
| Stale advisory lock blocks new process start | When a Python process is killed abruptly its Postgres session can linger, holding the lock. All four locks (producer 7400030, gold 7400040, embedder 7400050, quarantine_rollup 7400060) can get stuck this way. **`run.py` takeover (B-045) handles this automatically on every restart:** kills the prior run.py supervisor + all travellens children, then polls `pg_locks` (0.5s interval, up to 30s) until all 4 locks are free — falls back to a 3s grace if Postgres is unreachable. No manual unlock needed on a normal restart. If a lock is still stuck after a `run.py` restart (rare — DB session outlived the process kill), release manually: `docker exec travellens-postgres psql -U travellens -d travellens -c "SELECT pg_terminate_backend(sa.pid) FROM pg_stat_activity sa JOIN pg_locks l ON sa.pid = l.pid WHERE l.locktype='advisory' AND l.granted=true;"` |
| Multiple run.py instances competing | `run.py` TAKES OVER on startup (B-045, newest wins): **`_startup_cleanup()` is the very first action** — kills the live run.py supervisor (port 47219 holder, `taskkill /F /T` tree kill) + all travellens children (consumer / simulator / dashboard / gold updater / embedder / quarantine rollup), then polls `pg_locks` until advisory locks 7400030/40/50/60 are free. After that, `_acquire_run_lock()` binds port 47219. If two `python run.py` start at nearly the same moment, `socket.bind()` is atomic — exactly one wins; the other prints `"port 47219 still held after cleanup — another run.py may have started simultaneously; try again."` and exits 1. **Foreign-process guard:** if `:5000` is held by a non-travellens PID (identified from WMI), `run.py` prints a clear error and exits 1 — it never blind-kills unrelated processes. Producer also holds `pg_try_advisory_lock(7400030)` as an independent guard. |
| Consumer replaying old Kafka messages after restart | Symptom: `pipeline_metrics.max_event_ts` is in the past while `events_consumed` climbs fast. Cause: the consumer group has committed offsets near offset 0. Fix: kill all Python processes, reset offsets — `docker exec travellens-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group travellens-python-consumer --reset-offsets --to-latest --topic booking-events --execute` — then restart `run.py`. |

---

## Key design decisions (locked — do not re-litigate)

- **Ollama not Claude API** — Qwen2.5-Coder-7B runs locally, no API cost, no data leaving machine
- **Keyword router not LLM router** — instant, deterministic, zero API cost for classification
- **pgvector not a dedicated vector store** — everything in one DB, no join problem
- **Flask not FastAPI** — server-rendered Jinja2 templates with Chart.js, no React frontend
- **Widget state in Postgres** — `dashboard_widgets` table, not a JSON file
- **Pure Python consumer not PyFlink** — PyFlink unstable on Windows + Python 3.11
- **`lists=30` for IVFFlat** — correct for 30K rows (rows/1000), blueprint value of 100 is wrong. B-030b backlog is now 100% cleared (133,463 rows total); rebuild IVFFlat index with `lists=134` — see `_log_index_notice` in `scripts/review_embedder.py` for step-by-step instructions (run query before rebuild, rebuild, run same query after).
- **Read path never triggers compute (B-022)** — LLM authors SQL once at pin time; SQL is
  frozen on the widget row; refresh runs frozen SQL; dashboard load serves the JSONB cache.
  Stale-while-revalidate, same shape a CDN uses.
- **Airflow runs in its OWN container with its OWN metadata Postgres** — isolates its
  `sqlalchemy<2.0` requirement from the rest of the stack; keeps orchestration metadata
  out of warehouse data. LocalExecutor only — no Celery/Redis/Flower at single-user scale.

---

## Docker stack

```bash
# Start everything
cd docker
docker compose --env-file ../.env up -d

# Check health
docker compose --env-file ../.env ps

# Connect to Postgres
docker exec -it travellens-postgres psql -U travellens -d travellens

# MinIO UI
http://localhost:9001  # minioadmin / minioadmin

# Ollama
http://localhost:11434

# Airflow UI (once Phase 6 infra is up)
http://localhost:8080
```

---

## Quality gate — every change

After acceptance tests pass, run a verification pass before declaring done:

1. **Failure-mode test** — test the change's primary failure mode directly
   (validators → false rejects; caches → staleness; DAGs → empty/null/large input;
   UI → adjacent widget/layout breakage).
2. **Blast radius** — enumerate every existing path that now depends on the change,
   and confirm each still works (e.g. `_validate_columns` runs on the pin path AND
   every frozen-SQL refresh — verify both).
3. **Regression check** — run 3–5 representative existing operations that touch the
   changed code and confirm they are unaffected.
4. **Report** a table: TEST | EXPECTED | ACTUAL | PASS/FAIL. No FAIL left outstanding.
5. If the change adds or changes tests, reference them in the relevant 
   phase doc and backlog item — tests that aren't documented get forgotten 
   and skipped on re-runs.

"Acceptance passed" is not "verified."

---

## If something breaks

1. Check `docs/backlog.md` — known limitations and resolution paths are listed there
2. Check `docs/session-notes.md` — recent build context not yet in code or migrations
3. Check which phase/backlog item introduced the file — scope fixes to that item
4. Use the `ROLLBACK` section of the relevant phase MD to reset cleanly
5. Commit after every accepted item — `git reset --hard` is your safety net
