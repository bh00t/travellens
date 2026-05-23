# TravelLens India

![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-16-336791?logo=postgresql&logoColor=white)
![pgvector](https://img.shields.io/badge/pgvector-enabled-336791)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-streaming-231F20?logo=apachekafka&logoColor=white)
![Ollama](https://img.shields.io/badge/LLM-Ollama%20Qwen2.5-000000?logo=ollama&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-dashboard-000000?logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

A real-time hotel and tourism intelligence platform for the Indian hospitality market.
TravelLens turns plain-English questions into charts, tables, and review insights —
no SQL required. A local LLM generates the queries, pgvector powers semantic review
search, and a Flask dashboard lets you pin any answer as a live widget.

Everything runs locally: the database, the streaming pipeline, the embeddings, and the
language model. No cloud, no external APIs.

> A learning and portfolio project exploring an end-to-end data engineering stack —
> streaming ingestion, a star-schema warehouse, vector search, and an LLM query layer.
> Booking data is synthetic; review data is real (Kaggle).

---

## Features

- **Ask in plain English** — "top 5 cities by revenue this year" becomes SQL, runs against
  the warehouse, and comes back as a chart. A local LLM writes the query; a validation and
  self-correction loop keeps it safe and runnable.

- **Semantic review search** — find reviews by *meaning*, not keywords. "complaints about
  dirty rooms" surfaces the relevant reviews even when they never use those words, via
  sentence-transformer embeddings and pgvector cosine similarity.

- **Pinnable dashboard** — pin any result as a widget. Drag to reorder, set per-widget
  auto-refresh, and toggle full-width. Build the exact view you want instead of fixed reports.

- **Auto visualisation** — every result picks its own best form: a stat card for a single
  number, a bar or line chart for trends, a table for detail, or a review list for semantic
  search.

- **Real-time streaming** — booking events flow through Kafka into a 13-table star-schema
  warehouse, with a parallel sink to S3-compatible object storage (MinIO) as Parquet.

- **Local-first AI** — both SQL generation and review summarisation run on a local Ollama
  model. Private, offline, and free to run.

---

## Tech stack

| Layer | Technology |
|---|---|
| Warehouse | Postgres 16 with a star schema |
| Vector search | pgvector · sentence-transformers (all-MiniLM-L6-v2, 384-dim) |
| Streaming | Apache Kafka → Postgres + MinIO (S3) Parquet |
| Language model | Ollama · Qwen2.5-Coder-7B (local inference) |
| Dashboard | Flask · Jinja2 · Chart.js |
| Infrastructure | Docker · MinIO · DuckDB |

---

## How it works

```
                    ┌─► Postgres  (fact_bookings + dimensions)
  Kafka events ─────┤
                    └─► MinIO/S3  (Parquet)

  Reviews ──► sentence-transformers ──► pgvector (384-dim)

  Your question ──► router ─┬─► Ollama → SQL → Postgres ──► chart / table
                            └─► embed → pgvector → Ollama summary ──► reviews
```

A keyword router decides whether a question is best answered by SQL (structured: revenue,
counts, rankings) or by semantic search (review sentiment and themes), then sends it down
the matching path.

---

## Running

```bash
docker compose -f docker/docker-compose.yml up -d   # start Postgres, Kafka, MinIO
.venv\Scripts\activate                              # activate the virtualenv (Windows)
python -m render.server                             # launch the dashboard
```

Then open **http://localhost:5000**.

### Try a query

```bash
python -m ai.main "total revenue by city"
python -m ai.main "complaints about dirty rooms"
```

---

## Project layout

```
ai/          text-to-SQL, semantic search, query router, prompts
render/       Flask app, widget renderer, dashboard templates
db/           schema + migrations
scripts/      data generation, embeddings, Kafka producer/consumer
docker/       compose stack (Postgres, Kafka, MinIO)
docs/         design docs, data model, build notes
```
