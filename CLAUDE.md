# TravelLens India — Claude Code Instructions

> This file is read automatically by Claude Code at the start of every session.
> Customise the paths, username, and preferences below to match your environment.

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
| 1 | Postgres schema + data load | ✓ Complete |
| 2 | Kafka streaming + dual sink | ✓ Complete |
| 3 | Embeddings + pgvector | ✓ Complete |
| 4 | AI layer (Text-to-SQL + semantic) | ✓ Complete |
| 5 | Flask dashboard | ✓ Complete |
| 6 | Airflow DAGs | ⬜ In progress — infra/containers up, 5 DAGs not built |
| 7 | Pipeline monitor (/monitor) | ✓ Complete — B-027 base + B-029 in-place auto-refresh + B-032 live throughput redesign all shipped (live pulse, default-today filter, lifecycle counts, SOON placeholders, `/monitor/data` JSON sidecar) |

Current phase: **6** — Phase 7 shipped (monitor redesign done end-to-end). Read `docs/phase-7-monitor.md` for the live-pulse + auto-refresh design. Phase 6 DAGs (B-024/013/014/015/016) still open; infra is up. Calendar simulator Phase A (B-034A) landed ahead of Phase 6: producer now REPLAYS `fact_bookings` on a sim-clock day by day. Phases B (B-036 net-new + long-stay tail) and C (B-037 REVIEW emission) deferred. **B-040 (gold lifecycle layer) shipped:** migration 009 + `scripts/gold_lifecycle_updater.py` — 624,388 lifecycle rows, 0 illegal flags, watermark-based incremental updates, decoupled from the consumer.

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
- **Under active hardening — edit ONLY per a specific backlog item:**
  `ai/text_to_sql.py` (B-022 run_stored_sql, B-003 _validate_columns, B-004 next),
  `ai/main.py` (B-004, B-005), `ai/semantic_search.py` (B-006 dedup, B-004),
  `render/server.py` and `render/templates/dashboard.html` (B-022 cache, Show SQL,
  rename),
  `scripts/stream_consumer.py` (B-031 resilience, B-032 heartbeat + per-type counts,
  B-038 bronze sink, B-039 silver sink; B-037 next for REVIEW accept),
  `scripts/gold_lifecycle_updater.py` (B-040 done — gold lifecycle reconstruction),
  `scripts/kafka_event_producer.py` (B-032 CHECKOUT emission, B-034 stateful
  lifecycle simulator, B-034A calendar replay simulator; B-036 / B-037 next).
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

```
travellens/
├── CLAUDE.md                        ← this file
├── README.md                        ← portfolio front door (links to the blueprint)
├── datamodel.md                     ← per-table schemas + migration history (003 → current)
├── index.html                       ← original technical blueprint (deep-dive, served via Pages)
├── run.py                           ← dev launcher (B-028): up the stack + 3 host procs; --chaos
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
│   └── prompts/
│       └── text_to_sql_system.txt   ← schema DDL + India context (authoritative)
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
│       │                              placeholders, 10s in-place auto-refresh
│       └── about.html               ← product page
├── scripts/
│   ├── generate_embeddings.py       ← Phase 3: batch embed reviews_raw
│   ├── semantic_playground.py       ← Phase 3: interactive semantic search test
│   ├── load_to_postgres.py          ← Phase 1: bulk loader
│   ├── validate_load.py             ← Phase 1: 20-check validator
│   ├── stream_consumer.py           ← Phase 2 + B-032 Ch.2: four parallel sinks off the accept path:
│   │                                  (1) Postgres agg UPSERT to agg_hourly_city_stats per window flush;
│   │                                  (2) S3 agg Parquet to processed/agg/hourly_city_stats/...;
│   │                                  (3) B-038 bronze: raw JSONL to s3://.../raw_events/year=/month=/day=/hour=/,
│   │                                      batched (BRONZE_BUFFER_CAP=500 OR FLUSH_CHECK_SECONDS tick),
│   │                                      ingest-time partitioning, post-Gate-3 (excludes PRICE_CHANGE);
│   │                                  (4) B-039 silver: typed INSERT into fact_booking_events with source='stream'
│   │                                      via execute_values + ON CONFLICT (event_id) DO NOTHING, page_size=len
│   │                                      (so cur.rowcount is honest), pre-Gate-3 so ALL 5 event types land
│   │                                      including PRICE_CHANGE (booking_id/customer_id NULL on those).
│   │                                  Plus pipeline_metrics heartbeat every ~10s. Each sink wrapped in its own
│   │                                  try/except so any one failure logs + drops the batch + bumps a counter,
│   │                                  never crashes the consumer. Gate order: 1 (JSON) → 2 (schema) →
│   │                                  parse-ts → 4 (late) → SILVER → 3 (type filter) → BRONZE → accumulator.
│   ├── kafka_event_producer.py      ← Phase 2 + B-034A: CALENDAR REPLAY SIMULATOR — walks a sim-clock day by day,
│   │                                  REPLAYS `fact_bookings WHERE booking_ts >= --sim-start` as BOOKING events,
│   │                                  and advances the real `sim_open_bookings` backlog through
│   │                                  CHECKIN/CHECKOUT/CANCELLATION at the real dates. Sim-clock persisted at
│   │                                  scripts/.sim_clock.json (gitignored); resume = saved+1. Wire types and
│   │                                  Kafka config UNCHANGED. New ADDITIVE wire field `event_date` (sim-day)
│   │                                  alongside wall-clock `event_ts`. Outcome (is_cancelled) comes from
│   │                                  fact_bookings, NOT randomised. Cancellation timing + reason are
│   │                                  deterministic from booking_id + chaos-seed. `--rate` / `--duration` are
│   │                                  deprecated no-ops for run.py compatibility. REVIEW deferred to B-037
│   │                                  (Phase C); net-new synthetic + long-stay tail deferred to B-036 (Phase B).
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
│       └── 009_gold_lifecycle.sql         ← B-040: ingest_seq cursor on fact_booking_events + fact_booking_lifecycle gold table + gold_watermark cursor
├── docker/
│   ├── postgres.Dockerfile          ← Postgres 16 + pgvector
│   └── docker-compose.yml           ← postgres + kafka + zookeeper + minio
│                                      (+ airflow + airflow-postgres once Phase 6 infra lands)
├── airflow/
│   ├── dags/                        ← Phase 6 DAGs (B-024, B-013, B-014, B-015, B-016)
│   └── plugins/
├── data/                            ← gitignored — 12 CSVs + 1 JSON seed
└── tests/
```

---

## Common mistakes — always avoid these

| Mistake | Correct approach |
|---|---|
| Forgetting `__init__.py` in new package | Create it (empty) immediately when making a new package folder |
| Using `hotel_master.city` in SQL | Always `dim_location.city` via JOIN on `location_id` |
| Querying `agg_daily_hotel_kpi` for booking counts | Use `fact_bookings` — KPI table is empty until B-013 DAG runs |
| Steering the SQL prompt to `agg_hourly_city_stats` for business answers | The stream rollup is now POPULATED (was previously claimed "empty" in the prompt — corrected during the B-032 Chunk 4 doc pass). It is the source for the `/monitor` dashboard, NOT for Explorer queries. `text_to_sql_system.txt` now names `agg_daily_hotel_kpi`, `agg_hourly_city_stats`, AND `pipeline_metrics` together as "operational/streaming aggregate tables, not the booking analytics source" — all business booking/revenue/cancellation analytics route to `fact_bookings`. |
| Short words in `SEMANTIC_TRIGGERS` | Minimum ~5 chars or multi-word phrases — `"hot"` matches inside `"hotels"` |
| Running `python ai/main.py` | Always `python -m ai.main` from repo root |
| Building IVFFlat index before all embeddings written | Always embed first, index last |
| Replacing docker-compose.yml | Add new services to existing file — never replace |
| Querying Postgres from `render/server.py` for data | All data through `ai.main.answer()` — only widget state in server.py |
| Hardcoding schema / column lists | Build from `information_schema` at runtime (see `_load_schema` / `_validate_columns` in `text_to_sql.py`) |
| Editing an existing migration | Migrations are append-only — add a new numbered file |
| `agg_daily_hotel_kpi` column names | Real columns: `total_bookings`, `total_revenue_inr`, `avg_nightly_rate_inr`, `cancellation_rate`, `avg_rating` — no `occupancy_rate`, no `revpar_inr` |
| `chain_name` treated as always present | `chain_name` is NULL for ~60% (independents) — exclude NULL when ranking chains |
| Counting entities via `fact_bookings` rows | "How many hotels", "hotels per city", "hotels opened per year" → query `hotel_master` directly (optionally joined to other dimensions). `fact_bookings` is for booking ROWS, not entity counts. The system prompt's ENTITY COUNT RULE codifies this. |
| `hotel_master.opened_year` columns | `opened_year SMALLINT` added in migration 006 (synthetic, range 1975–2023, correlated with `star_category`, capped at the hotel's earliest booking year). Use this directly for "hotels opened/created per year" — never derive opening year from `fact_bookings` dates. |
| `is_cancelled` scope | `is_cancelled` lives ONLY on `fact_bookings` — it does NOT exist on `hotel_master`, `dim_customer`, `dim_location`, `dim_date`, `dim_room_type`, or `reviews_raw`. A query whose FROM/JOIN doesn't include `fact_bookings` MUST NOT reference it (counting hotels, listing customers, enumerating cities — none take an `is_cancelled` filter). For fact_bookings queries, exclude cancelled bookings by default (`WHERE NOT b.is_cancelled`) unless the question is specifically about cancellations. |
| `agg_hourly_city_stats` column drift | Real columns post-migration 007: `city`, `window_start`, `window_end`, `total_bookings`, `total_revenue_inr`, `avg_occupancy_rate`, `cancellation_rate`, `ingestion_ts`, `total_checkins`, `total_checkouts`, `total_cancellations`, `total_reviews`. Population (B-032 Chunks 2 + 3 — end-to-end): `total_checkins`, `total_checkouts`, and `total_cancellations` are all written by the consumer for every flushed window and read non-zero on recent windows because the producer now emits all three event types at design weights (CHECKIN 0.18, CHECKOUT 0.12, CANCELLATION 0.10). `total_reviews` stays NULL — REVIEW is not a stream event today (B-030). |
| `pipeline_metrics` table | Append-only heartbeat written by `scripts/stream_consumer.py` every `FLUSH_CHECK_SECONDS` (~10s) — live since B-032 Chunk 2. Columns: `metric_ts` (PK), `events_consumed`, `bookings`, `cancellations`, `malformed`, `late` (BIGINT, NOT NULL default 0), `active_windows` (INT, NOT NULL default 0), `max_event_ts` (TIMESTAMP, NULL), `consumer_lag` (BIGINT, NULL — reserved, not computed yet). Counters are cumulative-since-start — derive events/sec as a delta between adjacent rows, not as a stored column. Drop deltas where the newer value < older value (consumer restart reset). Added in migration 007 to resolve L-015. |
| `fact_booking_events` / `sim_open_bookings` (migration 008, B-035) | One silver event ledger spans history AND stream; `source` ('history'\|'stream') is the only separator. Schema: `event_id`/`event_type`/`booking_id`/`customer_id`/`hotel_id`/`city`/`room_type_id`/`event_ts`/`event_date` + per-type nullable cols (`cancellation_reason`, `rating`/`review_channel`/`review_text` reserved for REVIEW, `old/new_price_inr` reserved for PRICE_CHANGE). **Invariant on BOOKING rows:** `revenue_inr == nightly_rate_inr * nights` (asserted in generator; generator trusts the `checkout - checkin` gap and recomputes revenue when stored `nights_stayed` disagrees). `sim_open_bookings` is mutable simulator state — one row per booking awaiting CHECKIN (`state='BOOKED'`) or CHECKOUT (`state='CHECKED_IN'`); rows are deleted on CHECKOUT/CANCELLATION. **Backlog is REAL** — every `sim_open_bookings.booking_id` exists in `fact_bookings`; no synthesised IDs. Populated by `python -m scripts.generate_lifecycle_history` (idempotent `--reset` deletes ONLY source='history', never source='stream'). |
| `--sim-today` anchor + FUTURE bucket | Default anchor is **`2025-06-01`** (configurable via `--sim-today YYYY-MM-DD`). Real `fact_bookings` runs to ~2026-05, so ~388K bookings with `booking_ts >= sim-today` sit AFTER the anchor — these are the **FUTURE** bucket: the generator skips them and they are **reserved for the stream simulator to replay** in `booking_ts` order as `source='stream'` BOOKING events. Do not write history events for them; do not insert them into `sim_open_bookings`. The generator's stats block reports `bucket_future` + its `booking_ts` range so the runway is visible. |
| Backfilling lifecycle events with arbitrary scripts | Use `scripts/generate_lifecycle_history.py`. Never INSERT directly into `fact_booking_events` from ad-hoc SQL — the script enforces FK validity, the revenue invariant, the source='history' tag, and the matching BOOKING-for-every-followup rule. Stream-side inserts (`source='stream'`) come from `scripts/stream_consumer.py`'s silver sink (B-039 — inline on the accept path, every accepted event of every type, ON CONFLICT (event_id) DO NOTHING for Kafka-redelivery dedup). The producer (B-034A calendar replay simulator) writes to Kafka, not to `fact_booking_events` directly — the consumer is the only stream-side writer to that table. |
| Calendar replay producer (`scripts/.sim_clock.json`) | The producer persists `{last_completed_day, chaos_seed}` to `scripts/.sim_clock.json` at the end of every sim-day; on restart it resumes at `saved+1`. Running with `--reset-clock` deletes the file. **Do not edit the file by hand to skip days** — the simulator owns the FUTURE bucket; skipping days drops real `fact_bookings` rows from the stream. The file is gitignored (local machine state). If `--chaos-seed` is changed between runs, hydrated cancellation plans whose date is now in the past get clamped forward and the producer prints a warning — pass the same seed on every restart, or accept the small re-shuffle. |
| Every emitted event carries TWO timestamps | `event_ts` = wall-clock UTC NOW (keeps consumer windowing/freshness unchanged); `event_date` = the SIM-DAY the event represents (NEW additive field). Use `event_date` for business-day analytics (CHECKIN counts per business day), use `event_ts` for operational SLOs (events-per-second, watermark grace). The consumer's Gate 2 doesn't require `event_date` — unknown fields pass through harmlessly. |
| Bronze archive (`raw_events/`) is the SOURCE OF TRUTH for raw stream events | `scripts/stream_consumer.py` writes every accepted event (post-Gate-4) to `s3://travellens-data/raw_events/year=/month=/day=/hour=/HHMMSS_<uuid8>.jsonl` (JSONL, many events per file, INGEST-time partitioning). PRICE_CHANGE is NOT bronzed — it's silently filtered at Gate 3 before bronze. Malformed and late events are NOT bronzed either — they only land in `malformed_events/` and `late_events/` respectively. Bronze is append-only — never dedup or rewrite a file; raw redeliveries are archived as-is and dedup happens later at silver (B-039). Bronze writes are best-effort: a bronze failure logs and continues, never crashes the consumer or blocks agg/heartbeat. |
| `fact_booking_lifecycle` / `gold_watermark` (migration 009, B-040) | Gold layer — one row per `booking_id`. Updated by `scripts/gold_lifecycle_updater.py` (decoupled ~1-min micro-batch; run ONE instance only). Forward-only status machine: BOOKED→CHECKED_IN→COMPLETED/CANCELLED. `illegal_transition_flag` fires on BUSINESS-TIMESTAMP inversions: (1) `checkin.event_ts < booking.event_ts`; (2) `checkout.event_ts < checkin.event_ts`; (3) CHECKOUT with NO preceding CHECKIN when `_seeded=True` (booking was seen — guarded so cross-batch artifacts where CHECKOUT ingest_seq < BOOKING ingest_seq do NOT falsely flag); (4) CANCELLATION after CHECKOUT. NOT fired for processing-order artifacts from heap scan order or bulk INSERT ordering. Detector verified by `tests/test_gold_lifecycle_flag.py` (6 negative tests — 3 must-flag, 3 must-not-flag; run via `python -m pytest tests/test_gold_lifecycle_flag.py -v`). `source_mix` tracks provenance ('history'/'stream'/'mixed'). PRICE_CHANGE and REVIEW excluded (no per-booking lifecycle). `gold_watermark` persists the last committed `ingest_seq` so the updater resumes after restart without reprocessing. Run via `python -m scripts.gold_lifecycle_updater`. |
| Running multiple gold_lifecycle_updater instances | **Structurally prevented by Postgres advisory lock** (`pg_try_advisory_lock(7400040)`) acquired at startup. A second instance logs `"Another gold_lifecycle_updater instance holds the advisory lock"` and exits immediately with code 1 — deadlock is impossible. If you need to restart: the lock releases automatically when the process exits. If a process is stuck, kill it: `Get-WmiObject Win32_Process \| Where-Object { $_.CommandLine -like '*gold_lifecycle_updater*' } \| ForEach-Object { Stop-Process -Id $_.ProcessId -Force }`. |

---

## Key design decisions (locked — do not re-litigate)

- **Ollama not Claude API** — Qwen2.5-Coder-7B runs locally, no API cost, no data leaving machine
- **Keyword router not LLM router** — instant, deterministic, zero API cost for classification
- **pgvector not a dedicated vector store** — everything in one DB, no join problem
- **Flask not FastAPI** — server-rendered Jinja2 templates with Chart.js, no React frontend
- **Widget state in Postgres** — `dashboard_widgets` table, not a JSON file
- **Pure Python consumer not PyFlink** — PyFlink unstable on Windows + Python 3.11
- **`lists=30` for IVFFlat** — correct for 30K rows (rows/1000), blueprint value of 100 is wrong
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
