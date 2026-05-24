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
| Dev tools | Docker · DuckDB · psql |

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

Current phase: **6** — Phase 7 shipped (monitor redesign done end-to-end). Read `docs/phase-7-monitor.md` for the live-pulse + auto-refresh design. Phase 6 DAGs (B-024/013/014/015/016) still open; infra is up.

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
  `scripts/stream_consumer.py`, `scripts/kafka_event_producer.py`,
  `scripts/generate_embeddings.py`, `scripts/init_s3_buckets.py`,
  `ai/query_router.py`.
- **Under active hardening — edit ONLY per a specific backlog item:**
  `ai/text_to_sql.py` (B-022 run_stored_sql, B-003 _validate_columns, B-004 next),
  `ai/main.py` (B-004, B-005), `ai/semantic_search.py` (B-006 dedup, B-004),
  `render/server.py` and `render/templates/dashboard.html` (B-022 cache, Show SQL,
  rename). When editing these, scope the change to the named backlog item — do not
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
├── run.py                           ← dev launcher (B-028): up the stack + 3 host procs; --chaos
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
│   ├── stream_consumer.py           ← Phase 2 + B-032 Ch.2: dual sink + per-type counts + pipeline_metrics heartbeat
│   ├── kafka_event_producer.py      ← Phase 2 + B-032 Ch.3: emits 5 event types (BOOKING/CHECKIN/CHECKOUT/CANCELLATION/PRICE_CHANGE; weights 0.55/0.18/0.12/0.10/0.05)
│   └── init_s3_buckets.py           ← Phase 2: MinIO bucket bootstrap
├── ai/                              (see above)
├── db/
│   ├── schema.sql                   ← 14-table star schema (frozen)
│   └── migrations/
│       ├── 003_dashboard_widgets.sql      ← Phase 5: dashboard state table
│       ├── 004_widget_settings.sql        ← Phase 5: width column
│       ├── 005_widget_cache.sql           ← B-022: generated_sql + last_result_json
│       ├── 006_hotel_opened_year.sql      ← hotel_master.opened_year (entity-count queries)
│       └── 007_pipeline_live_metrics.sql  ← B-032: pipeline_metrics + 4 new agg_hourly_city_stats count cols
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
