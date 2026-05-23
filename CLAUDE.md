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
| Orchestration | Airflow (Phase 6 — not started) |
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
| 5 | Flask dashboard | ⬜ In progress |
| 6 | Airflow DAGs | ⬜ Not started |

Current phase: **5** → read `docs/phase-5-dashboard.md` before doing anything.

---

## Hard rules — never break these

- **Build all LLM-facing content from the real schema, never from memory.** Before
  writing or editing any system prompt, SQL-generation context, or anything an LLM
  must understand about the data model, read the actual schema first:
  `docker exec travellens-postgres psql -U travellens -d travellens -c "\d <table>"`
  or query `information_schema.columns`. Include exact column names, data types, and
  FK relationships. Approximating the schema from memory causes type-mismatch errors
  (e.g. comparing a VARCHAR hotel_id to a UUID location_id) that no amount of example
  queries will fix. The fix is always correct schema knowledge, not more examples.
- **Examples in a prompt are a last resort, not a patch.** If an LLM generates wrong
  SQL, the first question is "does the prompt contain the schema knowledge it needed?"
  — fix that, do not bolt on a one-off example for each failing query.
- **Never modify files from a completed phase** — `schema.sql`, `load_to_postgres.py`,
  `validate_load.py`, `stream_consumer.py`, `kafka_event_producer.py`,
  `generate_embeddings.py`, `ai/query_router.py`, `ai/text_to_sql.py`,
  `ai/semantic_search.py`, `ai/main.py`
- **Always create `__init__.py`** in every new Python package folder — without it,
  Python cannot find the module (`ai/`, `render/` both need one)
- **Cities are in `dim_location.city`** — never `hotel_master.city` (column does not exist)
- **Run modules correctly:** `python -m ai.main "question"` not `python ai/main.py`
- **Run acceptance tests before declaring a phase done** — do not skip any
- **Fix the code if a test fails** — never modify the acceptance test to make it pass
- **Never commit `.env`** — it is gitignored, keep it that way
- **Never modify `data/`** — source files are read-only

---

## Repo layout

```
travellens/
├── CLAUDE.md                        ← this file
├── docs/
│   ├── phase-0-setup.md             ← Phase 0 spec
│   ├── phase-1-postgres.md          ← Phase 1 spec
│   ├── phase-2-streaming.md         ← Phase 2 spec
│   ├── phase-3-embeddings.md        ← Phase 3 spec
│   ├── phase-4-ai-layer.md          ← Phase 4 spec
│   ├── phase-5-dashboard.md         ← Phase 5 spec (current)
│   ├── backlog.md                   ← known issues + future work
│   └── claude-code-prompts.md       ← prompts to start each phase
├── ai/
│   ├── __init__.py                  ← required
│   ├── main.py                      ← entry point: answer(query) → result dict
│   ├── query_router.py              ← keyword classifier: sql vs semantic
│   ├── text_to_sql.py               ← Ollama → SQL → Postgres
│   ├── semantic_search.py           ← MiniLM embed → pgvector → Ollama summary
│   └── prompts/
│       └── text_to_sql_system.txt   ← schema DDL + India context + few-shot examples
├── render/
│   ├── __init__.py                  ← required
│   ├── server.py                    ← Flask app: /dashboard /explore /about + 4 API routes
│   ├── widget_renderer.py           ← result shape → Chart.js config
│   └── templates/
│       ├── base.html                ← shared nav + layout
│       ├── dashboard.html           ← pinned widgets grid
│       ├── explore.html             ← chat interface + widget preview
│       └── about.html               ← product page
├── scripts/
│   ├── generate_embeddings.py       ← Phase 3: batch embed reviews_raw
│   ├── semantic_playground.py       ← Phase 3: interactive semantic search test
│   ├── load_to_postgres.py          ← Phase 1: bulk loader
│   ├── validate_load.py             ← Phase 1: 20-check validator
│   ├── stream_consumer.py           ← Phase 2: Kafka consumer + dual sink
│   ├── kafka_event_producer.py      ← Phase 2: synthetic event producer
│   └── init_s3_buckets.py           ← Phase 2: MinIO bucket bootstrap
├── db/
│   ├── schema.sql                   ← 14-table star schema
│   └── migrations/
│       └── 003_dashboard_widgets.sql ← Phase 5: dashboard state table
├── docker/
│   ├── postgres.Dockerfile          ← Postgres 16 + pgvector
│   └── docker-compose.yml           ← Postgres + Kafka + Zookeeper + MinIO
├── data/                            ← gitignored — 12 CSVs + 1 JSON seed
├── airflow/dags/                    ← Phase 6 (not started)
└── tests/
```

---

## Common mistakes — always avoid these

| Mistake | Correct approach |
|---|---|
| Forgetting `__init__.py` in new package | Create it (empty) immediately when making a new package folder |
| Using `hotel_master.city` in SQL | Always `dim_location.city` via JOIN on `location_id` |
| Querying `agg_daily_hotel_kpi` for booking counts | Use `fact_bookings` — KPI table is empty until Phase 6 Airflow |
| Short words in `SEMANTIC_TRIGGERS` | Minimum ~5 chars or multi-word phrases — `"hot"` matches inside `"hotels"` |
| Running `python ai/main.py` | Always `python -m ai.main` from repo root |
| Building IVFFlat index before all embeddings written | Always embed first, index last |
| Replacing docker-compose.yml | Add new services to existing file — never replace |
| Querying Postgres from `render/server.py` for data | All data through `ai.main.answer()` — only widget state in server.py |
| `agg_daily_hotel_kpi` column names | Real columns: `total_bookings`, `total_revenue_inr`, `avg_nightly_rate_inr`, `cancellation_rate`, `avg_rating` — no `occupancy_rate`, no `revpar_inr` |

---

## Key design decisions (locked — do not re-litigate)

- **Ollama not Claude API** — Qwen2.5-Coder-7B runs locally, no API cost, no data leaving machine
- **Keyword router not LLM router** — instant, deterministic, zero API cost for classification
- **pgvector not a dedicated vector store** — everything in one DB, no join problem
- **Flask not FastAPI** — server-rendered Jinja2 templates with Chart.js, no React frontend
- **Widget state in Postgres** — `dashboard_widgets` table, not a JSON file
- **Pure Python consumer not PyFlink** — PyFlink unstable on Windows + Python 3.11
- **`lists=30` for IVFFlat** — correct for 30K rows (rows/1000), blueprint value of 100 is wrong

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
```

---

## If something breaks

1. Check `docs/backlog.md` — known limitations and resolution paths are listed there
2. Check which phase introduced the file — never fix by modifying a completed phase's files
3. Use the `ROLLBACK` section of the relevant phase MD to reset cleanly
4. Commit after every `PHASE N ACCEPTED` — `git reset --hard` is your safety net
