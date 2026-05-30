# TravelLens India — Data Model

**Version:** 1.1
**Generated:** 2026-05-16 (base); 2026-05-27 (Stage 1 expansion — B-046)
**Coverage:** Jan 2024 → May 2026 (28 months)
**Total rows:** ~1.34M across 13 files + the additive Stage 1 expansion. Live Postgres totals: 993 cities · 20,076 hotels · 55,446 room types · 100,000 customers · 1,000,000 fact_bookings (unchanged).

---

## Table of Contents

1. [Overview](#overview)
2. [The Source Data Categories](#the-source-data-categories)
3. [Update Patterns — Who Changes What, How Often](#update-patterns--who-changes-what-how-often)
4. [Processing Layers — Bronze, Silver, Gold](#processing-layers--bronze-silver-gold)
5. [Entity Relationship Map](#entity-relationship-map)
6. [Reference Data](#reference-data)
   - [ref_price_tiers.csv](#ref_price_tierscsv)
   - [ref_state_centroids.csv](#ref_state_centroidscsv)
   - [india_states_zones.csv](#india_states_zonescsv)
   - [public_holidays.csv](#public_holidayscsv)
7. [Dimensions](#dimensions)
   - [dim_date.csv](#dim_datecsv)
   - [dim_location.csv](#dim_locationcsv)
   - [dim_customer.csv](#dim_customercsv)
   - [hotel_master.csv](#hotel_mastercsv-dim_hotel)
   - [dim_room_type.csv](#dim_room_typecsv)
8. [Facts](#facts)
   - [fact_bookings.csv](#fact_bookingscsv-)
   - [fact_price_events.csv](#fact_price_eventscsv)
   - [reviews_raw.csv](#reviews_rawcsv-)
9. [Gold Layer — Derived Analytics](#gold-layer--derived-analytics)
   - [agg_hourly_city_stats](#agg_hourly_city_stats)
   - [pipeline_metrics](#pipeline_metrics)
   - [agg_daily_hotel_kpi](#agg_daily_hotel_kpi)
   - [agg_monthly_zone_summary](#agg_monthly_zone_summary)
   - [customer_lifetime_value](#customer_lifetime_value)
   - [review_embeddings](#review_embeddings)
   - [hotel_sentiment_scores](#hotel_sentiment_scores)
10. [Streaming Configuration](#streaming-configuration)
    - [booking_events_seed.json](#booking_events_seedjson)
11. [Data Quality Guarantees](#data-quality-guarantees)
12. [Loading Order for Postgres](#loading-order-for-postgres)
13. [Schema Evolution](#schema-evolution)

---

## Overview

This folder contains the complete data layer for the platform — 13 datasets across reference tables, dimensions, facts, and one streaming configuration file. The model follows a **star schema** with **Lambda architecture** patterns (historical batch data + live streaming aggregates).

The datasets here support three query patterns:
- **Structured analytics** via SQL — *"Top 5 cities by revenue Q3 2024"*
- **Semantic search** via vector embeddings — *"What do guests complain about in Mumbai 3-star hotels?"*
- **Real-time KPIs** via streaming consumer — *"Goa cluster occupancy right now"*

### How to use this folder

- **Read this `datamodel.md`** for schemas, relationships, sample rows, and design decisions
- **`schema.sql`** — DDL statements to create all tables in Postgres
- **CSV files** — the actual data (~248 MB, ~1.16M rows total)
- **Need to regenerate or scale?** See [Regenerating the Dataset](#regenerating-the-dataset) below — single ordered build/seed sequence covering base rebuild, B-046 additive expansion, and post-load seeds.

---

## Regenerating the Dataset

The single canonical order for building this database from nothing — or extending it. Every script is deterministic (`SEED=42`). Each row below lists what the script reads, what it writes, and its idempotency posture, so you can reason about safety before running.

### ⚠ Safety — read first

`load_to_postgres.py` **TRUNCATEs every table with CASCADE** before loading CSVs (see [scripts/load_to_postgres.py:43-79](scripts/load_to_postgres.py#L43-L79)). It is the base rebuild only. Running it after Stage B or Stage C wipes:

- The B-046 expansion (993 cities → back to 44; 20,076 hotels → back to 2,000; 55,446 room types → back to 5,542; 100,000 customers → back to 20,000).
- `fact_booking_events` history (~ 2.1M rows) and `sim_open_bookings` simulator state.
- Every `reviews_raw.embedding` populated by Phase 3.
- Any live stream-emitted data (`source='stream'` rows in `fact_booking_events`, stream reviews in `reviews_raw`).

**Stages B and C run ONCE, after Stage A, on top of the loaded base.** Never re-run Stage A after Stage B or C unless you intend a full wipe.

### Stage A — From-scratch base rebuild (mandatory first)

Run in order. Stage 2 reads Stage 1 outputs; Stage 3 overwrites Stage 2's `fact_bookings.csv`; the loader needs all CSVs present; the validator queries the loaded Postgres.

| # | Step | Script | Runtime | Reads | Writes | Idempotency |
|---|---|---|---|---|---|---|
| A1 | Foundation | `scripts/generate_datasets.py` | ~30s | nothing | `hotel_master.csv` (2K), `dim_location.csv` (44), `dim_room_type.csv` (~5.5K), `reviews_raw.csv` (30K seed), `booking_events_seed.json` (500), `public_holidays.csv` (147), `india_states_zones.csv` (37) | Overwrites CSVs in `data/`. No DB touch. |
| A2 | Facts | `scripts/generate_stage2.py` | ~90s | Stage 1 CSVs | `fact_bookings.csv` (1M, Jan 2024–May 2026), `fact_price_events.csv` (~86K), `dim_date.csv` (2,557), `dim_customer.csv` (20K), `ref_price_tiers.csv` (4); regenerates `reviews_raw.csv` with power-law + seasonality fixes | Overwrites CSVs in `data/`. No DB touch. |
| A3 | Correlation | `scripts/generate_stage3.py` | ~60s | Stage 1+2 CSVs | Overwrites `fact_bookings.csv` with segment-driven behaviour (Business→weekday/premium, Honeymoon→luxury/5–7 nights, etc.); adds `ref_state_centroids.csv` (38) | Overwrites CSVs in `data/`. No DB touch. |
| A4 | Load | `python -m scripts.load_to_postgres` | ~3 min | All Stage 1/2/3 CSVs from `data/` | **TRUNCATE every table CASCADE**, then `COPY` each CSV into its table, re-enable FKs, build indexes ([scripts/load_to_postgres.py:43-79](scripts/load_to_postgres.py#L43-L79)). | DESTRUCTIVE. Always wipes whatever was loaded before. Frozen file — do not edit. |
| A5 | Validate | `python -m scripts.validate_load` | ~5s | The loaded Postgres | Read-only — runs 20 checks ([scripts/validate_load.py](scripts/validate_load.py)). Exits 0 if all pass. | Read-only. **Passes only against the base load** — it asserts EXACT counts (`hotel_master=2000`, `dim_customer=20000`, `dim_room_type=5542`, `reviews_raw=30000`, embeddings IS NULL on every row). After Stage B or C those asserts fail; that's expected and not a problem with the data. |

### Stage B — Additive expansion (B-046, run ONCE after Stage A)

Grows the world to ~1,000 cities / ~20K hotels / ~55K room types / 100K customers without touching any base row and without going through `load_to_postgres`.

| # | Step | Script | Runtime | Reads | Writes | Idempotency |
|---|---|---|---|---|---|---|
| B1 | Stage 1 expansion | `python -m scripts.expand_dimensions` | ~10s | `seeds/cities_expansion.csv` (committed) + the loaded Postgres for pre-flight guards | INSERTs **949 dim_location + 18,076 hotel_master + 49,904 dim_room_type + 80,000 dim_customer** rows in a single transaction. Direct INSERT, NOT via `load_to_postgres`. No migration. | Aborts on re-run. Four pre-flight guards: state-name validation against `india_states_zones`; no catalog `(city, state)` may already exist in `dim_location`; `MAX(hotel_id) == 'HTL-002000'`; `MAX(customer_id) == 'CUST-020000'`. Any guard failure → clear stderr + ROLLBACK + non-zero exit. |

Source of truth for the catalog: [`seeds/cities_expansion.csv`](seeds/cities_expansion.csv). Regenerable byte-identical from [`scripts/build_cities_expansion_csv.py`](scripts/build_cities_expansion_csv.py).

### Stage C — Post-load seeds (run after Stage A; safe before or after Stage B)

These two are independent — both read facts/reviews and write derived data on top. Order between them does not matter; run either before or after Stage B.

| # | Step | Script | Runtime | Reads | Writes | Idempotency |
|---|---|---|---|---|---|---|
| C1 | Lifecycle history backfill | `python -m scripts.generate_lifecycle_history` | ~3–5 min | `fact_bookings` | `fact_booking_events` (~ 2.1M rows tagged `source='history'`: BOOKING + CHECKIN + CHECKOUT or BOOKING + CANCELLATION) and `sim_open_bookings` (~17K rows of in-flight bookings at the `--sim-today` anchor, default 2025-06-01). The FUTURE bucket (`booking_ts >= sim-today`) is SKIPPED — reserved for the stream simulator. | `--reset` (default) runs `DELETE FROM fact_booking_events WHERE source='history'; TRUNCATE sim_open_bookings;` before writing. `source='stream'` rows are never touched. RNG seed pinned → identical counts on rerun. Use `--no-reset` to append without wiping. |
| C2 | Review embeddings backfill | `python -m scripts.generate_embeddings` | ~5–8 min on RTX 3070 | `reviews_raw WHERE embedding IS NULL` | Writes `embedding` (384-d `vector` from `all-MiniLM-L6-v2`) to each NULL row, then `DROP / CREATE` the IVFFlat index. `lists` is now computed dynamically by `_ivfflat_lists(embedded_rowcount)` at build time (B-051 logged frozen-file exception): `max(rowcount // 1000, 30)` up to 1M vectors; `int(sqrt(rowcount))` past 1M. So a re-embed against the live ~133K corpus rebuilds at `lists≈133` — within rule-of-thumb noise of the manually-shipped `lists=120`. After any full re-embed, the query side stays correct because `ai/semantic_search.py` already issues `SET ivfflat.probes = 11` per connection (pair it manually with the build lists if a future corpus changes the index size materially). | Re-entrant — operates only on `embedding IS NULL`. Restart-safe (no row mid-batch is half-written). The continuous embedder (`scripts/review_embedder.py`) does NOT touch the index. |
| C3 | Review sentiment backfill (B-026) | `python -m scripts.review_sentiment_scorer --once` | ~15 min on RTX 3070 | `reviews_raw WHERE sentiment_label IS NULL` | Writes `sentiment_label` + `sentiment_score` (CardiffNLP 3-class) to each unscored row. Requires migration 017 applied (the columns must exist). Independent of C2 — does NOT touch `embedding` or the IVFFlat index; order vs. C2 does not matter. | Re-entrant — operates only on `sentiment_label IS NULL`. Commits per batch; restart-safe. Single-instance (advisory lock 7400070). |

After C1 + C2 (+ C3) the database has: the loaded base (Stage A) plus optional expansion (Stage B) plus history events for the simulator to read plus searchable embeddings plus per-review sentiment.

### Tuning knobs (for re-shaping the dataset itself)

Each generator script has a CONFIG block at the top. `SEED=42` everywhere — keep aligned across stages or referential integrity breaks.

| Knob | Stage | Effect |
|---|---|---|
| `NUM_HOTELS` | A1 | Cascades to bookings and reviews |
| `NUM_REVIEWS` | A1 | Review volume |
| `NUM_BOOKINGS` | A2 | Scale `fact_bookings` |
| `NUM_CUSTOMERS` | A2 | Unique customer count |
| `HISTORY_START` / `HISTORY_END` | A2 + A3 | Booking date range |
| `SEED` | All | RNG seed for different dataset shape |
| `--sim-today` | C1 | Anchor that splits history vs. future bucket (default 2025-06-01) |
| `--reset` / `--no-reset` | C1 | Whether to wipe `source='history'` events before writing |

### Quick reference — full sequence

```bash
# Stage A — base rebuild (DESTRUCTIVE on A4)
python scripts/generate_datasets.py    # A1  ~30s   → CSVs
python scripts/generate_stage2.py      # A2  ~90s   → CSVs
python scripts/generate_stage3.py      # A3  ~60s   → CSVs
python -m scripts.load_to_postgres     # A4  ~3min  → Postgres (TRUNCATE + COPY)
python -m scripts.validate_load        # A5  ~5s    → 20 checks on loaded Postgres

# Stage B — additive expansion (idempotent, aborts on re-run)
python -m scripts.expand_dimensions    # B1  ~10s   → 99K new dim rows via INSERT

# Stage C — post-load seeds (independent, any order)
python -m scripts.generate_lifecycle_history   # C1  ~3-5min  → ~2.1M history events + ~17K sim_open rows
python -m scripts.generate_embeddings           # C2  ~5-8min  → 384-d vectors + IVFFlat index
python -m scripts.review_sentiment_scorer --once  # C3  ~15min  → sentiment_label/score (needs migration 017)
```

**Re-running A4 after B or C wipes the expansion + history events + embeddings.** That is the only ordering rule that matters.

---

## The Source Data Categories

The platform consumes three logically distinct source types, plus an internal Stream config. Knowing which is which determines how each gets ingested.

```
┌────────────────────────┬──────────────────────────────────────────┬──────────────────┐
│ Source Category        │ What it is                               │ Update Frequency │
├────────────────────────┼──────────────────────────────────────────┼──────────────────┤
│ Reference              │ Vocabulary lookups (tiers, holidays,     │ Annually         │
│                        │  states, centroids) — hand-edited        │                  │
├────────────────────────┼──────────────────────────────────────────┼──────────────────┤
│ Master (Dimensions)    │ Entity descriptions (hotels, customers,  │ Slowly-changing  │
│                        │  locations, dates, room types)           │                  │
├────────────────────────┼──────────────────────────────────────────┼──────────────────┤
│ Historical (Facts)     │ Event records (bookings, prices,         │ Append-only      │
│                        │  reviews)                                │                  │
├────────────────────────┼──────────────────────────────────────────┼──────────────────┤
│ Stream                 │ Live JSON events flowing through Kafka   │ Continuous       │
│                        │  (configured by booking_events_seed.json)│                  │
└────────────────────────┴──────────────────────────────────────────┴──────────────────┘
```

| Category | Files | Total Rows | Ingestion path |
|---|---|---:|---|
| **Reference** | 4 files | 226 | Direct load → Postgres (no ETL needed) |
| **Master (Dimensions)** | 5 files | ~30K | Bronze → Silver ETL → Postgres |
| **Historical (Facts)** | 3 files | ~1.12M | Bronze → Silver ETL → Postgres |
| **Stream config** | 1 file | 500 | Read by Python simulator → Kafka |

**Why Master and Historical are separated:** Both go through the same Bronze→Silver→Postgres ETL path, but they behave very differently once loaded. **Master tables change slowly** (a hotel's chain affiliation might change once a year) — production systems use SCD Type 2 to track history. **Historical tables are append-only** (every new booking is a new row) — no row is ever updated, only added. This distinction affects how queries treat them and how data quality issues are handled.

---

## Update Patterns — Who Changes What, How Often

Not all data changes the same way. Some files are meant to be edited by hand. Others are append-only pipeline outputs. Knowing which is which prevents you from accidentally overwriting machine-generated data — or trying to run a pipeline against a table that's really a config file.

### Pattern 1: User-editable reference tables

**Edit these CSVs directly when business rules change.** Then reload into Postgres.

| File | When to edit | Typical change |
|---|---|---|
| `ref_price_tiers.csv` | Inflation, market repositioning | Bump Budget ceiling from ₹1,999 → ₹2,499 |
| `public_holidays.csv` | Each new year | Add Diwali 2027 (Nov 7), Holi 2027 (Mar 22), etc. |
| `india_states_zones.csv` | New UT created, Ministry of Tourism publishes updated arrivals | Update `tourist_arrivals_m` annually |
| `ref_state_centroids.csv` | Essentially never | Geographic centroids don't shift |

**Note:** When `public_holidays.csv` changes, the `is_holiday` and `is_high_demand_holiday` flags in `dim_date.csv` become stale. Either regenerate `dim_date.csv` via Stage 2, or update the flags directly with SQL.

### Pattern 2: Slowly-changing dimensions

All dimensions are currently **SCD Type 1** (latest value only, no history tracking in this build).

| File | What changes | Edit notes |
|---|---|---|
| `hotel_master.csv` | New hotels, star re-categorization, chain changes | Overwrite current row (Type 1) |
| `dim_room_type.csv` | Hotels add/remove room types | Append-only — old `room_type_id` references in `fact_bookings` must keep working |
| `dim_customer.csv` | Loyalty tier upgrades, home state changes | Overwrite current row; `fact_bookings` carries the booking-time values inline |
| `dim_location.csv` | New cities added | Append-only, never edit existing rows |
| `dim_date.csv` | Extend by another year | Append rows only — never edit existing rows |

### Pattern 3: Append-only facts

**Never edit these by hand.** They record what happened. In production, rows arrive from the pipeline.

| File | Source in production | Why never edit |
|---|---|---|
| `fact_bookings.csv` | Kafka events → stream consumer → table | Editing breaks audit trail. Cancellations are NEW rows with `is_cancelled=TRUE`, not updates. |
| `fact_price_events.csv` | Kafka `PRICE_CHANGE` events → stream consumer → table | Each price change is its own row by design |
| `reviews_raw.csv` | OTA API scraper (Phase 3) → append on ingest | Reviews are timestamped facts |

If you need to fix a bad row, the production pattern is to insert a corrective row, not update the original.

### Pattern 4: Stream configuration

| File | Update frequency | Notes |
|---|---|---|
| `booking_events_seed.json` | Whenever you tune the Kafka simulator | Pure config — not data. Edit to change event rates, OTA mix, seasonality multipliers. |

### Quick reference: edit-safety by file

```
✓ Safe to edit by hand:    ref_price_tiers, public_holidays, india_states_zones,
                            ref_state_centroids, booking_events_seed.json

⚠ Edit with care (SCD):    hotel_master, dim_room_type, dim_customer,
                            dim_location, dim_date

✗ Never edit by hand:      fact_bookings, fact_price_events, reviews_raw
```

### Note: data model vs storage

This document defines the **structure** of data — schemas, columns, relationships. It deliberately does NOT specify storage locations (which S3 bucket, file format, partitioning). Those are deployment concerns documented separately (`storage.md` / `pipeline.md`).

The same `hotel_master` schema is valid whether it sits in `~/data/hotel_master.csv`, `s3://travellens-data/raw/bronze/hotel_master.csv`, or `s3://travellens-data/raw/silver/hotel_master.parquet`. The data model travels with the data, regardless of where the data lives.

---

## Processing Layers — Bronze, Silver, Gold

Data does not stay in one shape. It moves through processing stages, each serving a different consumer. This is the **medallion architecture** (Bronze → Silver → Gold), a standard pattern in modern data platforms.

```
┌───────────────────────────────────────────────────────────────────────┐
│                                                                       │
│  BRONZE             SILVER                GOLD                        │
│  ──────             ──────                ────                        │
│  Raw, untouched     Cleaned, typed,       Pre-aggregated,             │
│  Source of truth    warehouse-ready       analysis-ready              │
│                                                                       │
│  Example:           Example:              Example:                    │
│  hotel_master.csv → hotel_master.parquet → agg_daily_hotel_kpi        │
│  (amenities as     (amenities as          (revenue, occupancy_pct,   │
│   JSON string)      ARRAY<VARCHAR>)         cancellation_rate per     │
│                                             hotel per day)            │
└───────────────────────────────────────────────────────────────────────┘
```

### What each layer is for

**Bronze** — exactly what the generator scripts (or in production, the source system) produced. CSV format. Schema as documented in the Reference/Dimensions/Facts sections below. Never modified. If everything downstream breaks, this is what you reload from.

**Silver** — Bronze after data engineering cleanup. Type enforcement, JSON parsing, null normalization, deduplication. Stored as Parquet (columnar, fast scans). This is what queries actually hit when performance matters.

**Gold** — Pre-computed analytical outputs. SQL aggregations resolved, business logic applied. Powers dashboards and frequently-asked queries without recomputing on every request. Both batch (daily, from Airflow) and streaming (sub-hourly, from the stream consumer) flavors exist.

### Which tables exist at which layers

| Table | Bronze | Silver | Gold | Notes |
|---|---|---|---|---|
| `ref_*` (4 reference tables) | ✓ | — | — | **Skip Silver — direct load to Postgres.** No transformation needed; CSVs already clean. |
| `dim_date` | ✓ | ✓ | — | Master/dim — slowly changing |
| `dim_location` | ✓ | ✓ | — | Master/dim |
| `dim_customer` | ✓ | ✓ | — | Master/dim — loyalty_tier normalized |
| `hotel_master` | ✓ | ✓ | — | Master/dim — amenities parsed to array |
| `dim_room_type` | ✓ | ✓ | — | Master/dim |
| `fact_bookings` | ✓ | ✓ | — | Historical/fact — append-only |
| `fact_price_events` | ✓ | ✓ | — | Historical/fact — append-only |
| `reviews_raw` | ✓ | ✓ | — | Historical/fact — append-only |
| `review_embeddings` | — | — | ✓ | Generated in Phase 3, stored as `vector(384)` in pgvector |
| `agg_hourly_city_stats` | — | — | ✓ | Stream consumer output (continuous) |
| `agg_daily_hotel_kpi` | — | — | ✓ | Airflow daily batch |
| `agg_monthly_zone_summary` | — | — | ✓ | Airflow monthly batch |
| `customer_lifetime_value` | — | — | ✓ | Airflow weekly batch |
| `hotel_sentiment_scores` | — | — | ✓ | Derived from review_embeddings |

**Reference path is structurally simpler.** Reference tables (`ref_*`) skip the Silver/ETL stage entirely — they go directly from Bronze CSV into Postgres via a simple `COPY`. No type casting beyond defaults, no JSON parsing, no validation pass. Master and Historical tables, by contrast, all go through Silver because they have type/format quirks that need cleanup (booleans as strings, JSON-in-CSV, etc.).

### What changes from Bronze to Silver

Most tables don't change shape — they only change format (CSV → Parquet) and type enforcement (string → INT, string → BOOLEAN). For four tables, the **schema changes meaningfully** at Silver. Those changes are documented inline in each table's section below as "Silver variant."

The four tables with schema changes at Silver:
- `hotel_master` — `amenities_json` (TEXT) becomes `amenities` (ARRAY)
- `dim_customer` — `loyalty_tier` blank values become explicit `'None'`; `home_state` standardized
- `reviews_raw` — `review_text` normalized (lowercased copy added as `review_text_normalized`)
- `fact_bookings` — date validations enforced; constraint columns added

### When to build each layer

| Phase | Bronze | Silver | Gold |
|---|---|---|---|
| Phase 1 (now) | ✓ exists | — skip | — skip |
| Phase 2 (streaming) | ✓ | — skip | ✓ `agg_hourly_city_stats` only |
| Phase 3 (embeddings) | ✓ | ✓ (for reviews only) | ✓ `review_embeddings` |
| Phase 4 (Text-to-SQL) | ✓ | ✓ (all tables) | ✓ + batch agg tables |
| Phase 5 (dashboards) | ✓ | ✓ | ✓ all tables |

Don't build layers before you feel the pain of not having them. Bronze + Postgres is fine for Phase 1.

### Streaming-side bronze: `raw_events/`

For the **streaming path** (events flowing through Kafka, not the
historical batch load above), bronze takes a different shape. The
consumer (`scripts/stream_consumer.py`) writes a durable raw archive
of every accepted event to:

```
s3://travellens-data/raw_events/year=YYYY/month=MM/day=DD/hour=HH/HHMMSS_<uuid8>.jsonl
```

Key properties:

| Property | Value |
|---|---|
| Format | JSONL — one event per line, many events per file. The full wire payload (base envelope + per-type fields incl. `event_date` and `event_ts`) is written verbatim. |
| Partitioning | INGEST wall-clock time (when the consumer writes the file), NOT event_time. Mirrors the existing quarantine prefixes — operators ask "what arrived in the last hour" not "what business-day did the event represent." |
| Batching | Many events per file. Buffer flushes on size cap (`BRONZE_BUFFER_CAP`, default 500) OR `FLUSH_CHECK_SECONDS` periodic tick (~10s), whichever first. Final drain on graceful shutdown. |
| What's in it | Every event that passed all four consumer gates (JSON parse, schema, type filter, late guard). PRICE_CHANGE is NOT in bronze (silent Gate-3 filter). Malformed and late events are NOT in bronze (they're in `malformed_events/` and `late_events/`). |
| Append-only | Files are immutable once written. Raw redeliveries are archived as-is — dedup happens at silver, not here. |
| Durability posture | Best-effort. A flush failure logs to stderr, increments a counter, and drops the batch; it never crashes the consumer or blocks agg/heartbeat. The shutdown summary reports any gaps. |

Why bronze on the stream when the agg sink already exists: the
`agg_hourly_city_stats` UPSERT is **lossy by construction** — it
emits derived totals per (city, window), not events; per-event
analytics (per-booking lifecycle reconstruction, replay testing,
ad-hoc forensic queries) can't be answered from it. Bronze
captures the events themselves so silver (typed event ledger) and
gold (lifecycle reconstruction with transition-flag QA) can be
built on top without any "if only we'd kept the raw payload."

Where bronze sits in the streaming pipeline:

```
Kafka topic ──► consumer
                 ├─► malformed_events/  (Gates 1+2, JSON or schema fail)
                 ├─► late_events/       (Gate 4, watermark fail)
                 ├─► raw_events/        (ACCEPT path — bronze, every accepted event)
                 ├─► agg_hourly_city_stats  (Postgres + Parquet, aggregated)
                 └─► pipeline_metrics       (Postgres, heartbeat)
```

Bronze is parallel to agg, not upstream of it — both fire off the
same accept path. A bronze failure cannot affect agg, and a
Postgres / agg-Parquet failure cannot affect bronze.

Tracked under B-038. Silver (per-event ledger in
`fact_booking_events source='stream'`) shipped as B-039 (see below);
gold (per-booking lifecycle reconstruction with
`illegal_transition_flag`) is B-040.

### Streaming-side silver: `fact_booking_events source='stream'`

The consumer (`scripts/stream_consumer.py`) also writes silver inline
on the accept path — every accepted event is INSERTed into
`fact_booking_events` with `source='stream'`, in the SAME table the
history backfill (B-035) writes to with `source='history'`. The
`source` column is the only distinguisher; downstream analytics
treat both alike. (B-035 design note: "HISTORY and STREAM both write
here; a `source` column ('history' | 'stream') is the only
separator.")

| Property | Value |
|---|---|
| Table | `fact_booking_events` (migration 008 schema — no change). `event_id` UUID PK provides the uniqueness for ON CONFLICT. |
| Trigger | Buffer fills inline on the accept path (post-Gate-4, pre-Gate-3 — so PRICE_CHANGE is INCLUDED). Flushes on `SILVER_BUFFER_CAP` (default 500) OR `FLUSH_CHECK_SECONDS` tick OR graceful shutdown. |
| Type coverage | All 5 wire types: BOOKING / CHECKIN / CHECKOUT / CANCELLATION / PRICE_CHANGE. PRICE_CHANGE rows carry `booking_id=NULL, customer_id=NULL` by design (operational event, not booking-scoped). |
| Dedup | `INSERT ... ON CONFLICT (event_id) DO NOTHING` via `execute_values(page_size=len(rows))`. Covers Kafka redelivery cleanly (same event_id → ignored). Does NOT cover producer crash-replay (producer mints a fresh `event_id = uuid4()` per emit, so a re-emitted logically-identical event would land as a new row); deterministic event_id (e.g. `uuid5(NAMESPACE, f"{booking_id}|{event_type}|{sim_day}")`) is the long-term path to exactly-once, tracked as a future enhancement. |
| Isolation | `silver_flush` wrapped in its own try/except. Failure → log + bump `silver_failures` + drop the batch + continue. Never crashes the consumer, never blocks agg/bronze/heartbeat. |
| Linkage | Stream BOOKING rows reference real `fact_bookings.booking_id`, `dim_customer.customer_id`, `hotel_master.hotel_id`, `dim_room_type.room_type_id` — by construction (the producer replays real `fact_bookings` rows). PRICE_CHANGE `room_type_id` is producer-synthesised (fresh UUID), not a real FK — there are no FK constraints on `fact_booking_events`, so this doesn't fail at write time, but downstream gold/analytics that join on it should LEFT JOIN. |

Where silver sits in the streaming pipeline:

```
Kafka topic ──► consumer
                 ├─► malformed_events/         (Gates 1+2)
                 ├─► late_events/              (Gate 4)
                 ├─► raw_events/               (BRONZE — post-Gate-3, JSONL on S3)
                 ├─► fact_booking_events       (SILVER — pre-Gate-3, source='stream')
                 ├─► agg_hourly_city_stats     (Postgres + Parquet, aggregated)
                 └─► pipeline_metrics          (Postgres, heartbeat)
```

Why silver is **pre-Gate-3** while bronze is **post-Gate-3**: bronze
predates B-039 and was placed where the brief at the time told it to
sit (post-all-gates); the brief for B-039 explicitly requires
PRICE_CHANGE rows in silver ("this row MUST be written, no type
filter"). To get PRICE_CHANGE into silver without touching bronze,
parse-ts + Gate 4 (late) were moved above Gate 3, and silver fires
between Gate 4 and Gate 3. Same accept criteria for both (valid +
non-late); the difference is that bronze excludes PRICE_CHANGE while
silver includes it. Late PRICE_CHANGE events now go to `late_events/`
instead of being silently dropped at Gate 3 — a small fix to a
previously-silent quirk, consistent with the consumer's "nothing is
silently dropped" docstring.

---

## Entity Relationship Map

```
                          ┌─────────────────────┐
                          │  ref_price_tiers    │
                          │  (4 tiers)          │
                          └──────────┬──────────┘
                                     │
                                     │ price_tier_id
                                     │
┌──────────────────┐       ┌─────────▼──────────┐      ┌──────────────────┐
│  dim_location    │◄──────┤   hotel_master     ├─────►│  dim_room_type   │
│  (993 cities)    │ FK    │   (20,076 hotels)  │      │  (55,446 rooms)  │
└────────┬─────────┘       └────────┬───────────┘      └────────┬─────────┘
         │                          │                            │
         │                          │                            │
         │                  ┌───────▼────────────────────────────▼──────┐
         │                  │                                            │
         └─────────────────►│         fact_bookings (1,000,000)         │
                            │                                            │
         ┌─────────────────►│  FK: hotel_id, customer_id, room_type_id, │
         │                  │      location_id, date_id                  │
         │                  └────────────┬───────────────────────────────┘
         │                               │
┌────────┴─────────┐           ┌─────────▼──────────┐     ┌──────────────────┐
│  dim_customer    │           │    dim_date        │     │ fact_price_events│
│ (100,000 cust.)  │           │  (2,557 days)      │     │   (86,650)       │
└──────────────────┘           └─────────┬──────────┘     └──────────────────┘
                                         │
                                ┌────────▼──────────┐
                                │ public_holidays   │ (feeds is_holiday flag)
                                └───────────────────┘

                            ┌────────────────────┐
                            │   reviews_raw      │
                            │   (30,000)         │
                            │   FK: hotel_id     │
                            └────────────────────┘
```

---

## Reference Data

Static lookup tables. Load once into Postgres via simple `COPY` — no ETL needed. Live in `s3://travellens-data/reference/`.

---

### `ref_price_tiers.csv`

**Purpose:** Defines what "Budget", "Mid", "Premium", and "Luxury" mean in INR.
**Rows:** 4
**Used by:** Text-to-SQL prompt — resolves user phrases like "budget hotel" to actual price filters.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `tier_id` | VARCHAR(10) | **PK.** BUDGET / MID / PREMIUM / LUXURY |
| `tier_name` | VARCHAR(20) | Human-readable label |
| `min_price_inr` | INT | Lower bound (inclusive) |
| `max_price_inr` | INT | Upper bound (inclusive) |
| `description` | TEXT | Context for Text-to-SQL prompt |

#### Sample Data

```
tier_id   tier_name   min_price_inr   max_price_inr   description
BUDGET    Budget      349             1999            Affordable stays under 2000 INR/night — OYO, budget independent hotels
MID       Mid         2000            4999            Mid-range 3-star properties — branded chains and quality independents
PREMIUM   Premium     5000            9999            Upscale 4-star hotels — Lemon Tree Premier, ITC, Marriott
LUXURY    Luxury      10000           35000           5-star and heritage luxury — Taj, Oberoi, Leela
```

#### Why it exists

A user query like *"show me budget hotels in Goa"* needs the LLM to know what "budget" means. Hardcoding `< 2000` in the prompt is brittle. With this table, the generated SQL is:

```sql
SELECT h.* FROM hotel_master h
JOIN ref_price_tiers pt ON h.price_tier_id = pt.tier_id
WHERE pt.tier_name = 'Budget' AND ...
```

---

### `ref_state_centroids.csv`

**Purpose:** Geographic centroid (lat/lng) for every Indian state and UT.
**Rows:** 38 (28 states + 9 UTs + 1 "Other")
**Used by:** Geo-visualizations — customer origin-to-destination flow maps.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `state_name` | VARCHAR(50) | **PK.** Joins to `dim_customer.home_state` |
| `latitude` | DECIMAL(6,2) | Geographic center, degrees North |
| `longitude` | DECIMAL(6,2) | Geographic center, degrees East |

#### Sample Data

```
state_name           latitude   longitude
Andhra Pradesh       15.91      79.74
Arunachal Pradesh    28.22      94.73
Assam                26.20      92.94
Bihar                25.10      85.31
Goa                  15.30      74.12
Maharashtra          19.75      75.71
Rajasthan            27.02      74.22
Delhi NCR            28.61      77.21
```

#### Why it exists

`dim_location.csv` has lat/lng for **destination cities** (where hotels are). But `dim_customer.home_state` only has state names. To draw "Maharashtra customers travelling to Goa" flow lines on a map, you need state-level lat/lng — that's what this provides.

---

### `india_states_zones.csv`

**Purpose:** State-level tourism reference data with macro tourism zones and arrival statistics.
**Rows:** 37 (28 states + 9 UTs)

#### Schema

| Column | Type | Notes |
|---|---|---|
| `state_code` | VARCHAR(2) | **PK.** Standard 2-letter codes (RJ, MH, KA) |
| `state_name` | VARCHAR(50) | Full state name |
| `zone` | VARCHAR(20) | Heritage / Beach / Hill Station / Pilgrimage / Wildlife / Metro / Mixed |
| `tourist_arrivals_m` | DECIMAL(6,2) | Annual visitors in millions (Ministry of Tourism) |
| `peak_months` | TEXT | Pipe-separated months e.g. `Oct\|Nov\|Dec\|Jan\|Feb` |
| `primary_attractions` | TEXT | Pipe-separated landmark names |

#### Sample Data

```
state_code   state_name    zone        tourist_arrivals_m   peak_months                primary_attractions
RJ           Rajasthan     Heritage    54.3                 Oct|Nov|Dec|Jan|Feb        Jaipur|Udaipur|Jaisalmer|Hawa Mahal|Amber Fort
MH           Maharashtra   Mixed       120.5                Oct|Nov|Dec|Jan|Feb        Gateway of India|Ajanta Caves|Lonavala|Shirdi
DL           Delhi NCR     Metro       29.8                 Oct|Nov|Dec|Jan|Feb        Red Fort|Qutub Minar|India Gate|Lotus Temple
KL           Kerala        Backwater   18.4                 Sep|Oct|Nov|Dec|Jan        Alleppey Backwaters|Munnar|Kovalam|Kochi
GA           Goa           Beach       8.5                  Nov|Dec|Jan|Feb            Calangute Beach|Anjuna|Old Goa Churches|Dudhsagar
```

#### Important distinction

`tourist_arrivals_m` here is **macro-level** (all visitors to a state in a year). Don't confuse with `fact_bookings` which is operational data — total bookings on the TravelLens platform alone.

---

### `public_holidays.csv`

**Purpose:** Real Indian holiday calendar for 2020–2026, with demand impact ratings.
**Rows:** 147
**Used by:** Populates `dim_date.is_holiday` and `dim_date.is_high_demand_holiday` flags.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `holiday_date` | DATE | Actual date in YYYY-MM-DD |
| `holiday_name` | VARCHAR(50) | e.g. Diwali, Holi, Eid ul-Fitr, Christmas |
| `holiday_type` | VARCHAR(20) | National / Religious |
| `states_applicable` | TEXT | `ALL` for national, or pipe-separated codes like `TN\|KL` |
| `demand_impact` | VARCHAR(10) | HIGH / MEDIUM / LOW — how it affects hotel demand |

#### Sample Data

```
holiday_date   holiday_name        holiday_type   states_applicable   demand_impact
2024-11-01     Diwali              Religious      ALL                 HIGH
2024-12-25     Christmas           Religious      ALL                 HIGH
2024-01-26     Republic Day        National       ALL                 MEDIUM
2024-01-15     Pongal              Religious      TN                  MEDIUM
2024-03-25     Holi                Religious      ALL                 HIGH
```

#### Why dates are hand-curated, not computed

Indian religious holidays move with the lunar calendar:
- Diwali: Nov 14 (2020), Nov 4 (2021), Oct 24 (2022), Nov 12 (2023), Nov 1 (2024), Oct 21 (2025), Nov 8 (2026)
- No clean formula. All 7 years × 16 movable holidays are hand-curated for accuracy.

`demand_impact` matters operationally: prices in `fact_price_events.csv` surge 50% within a day of HIGH-impact holidays. Without this column, the price surge pattern would be random.

---

## Dimensions

The "WHO/WHAT/WHERE/WHEN" tables. Every fact row references these.

---

### `dim_date.csv`

**Purpose:** Date spine for the entire project timeline.
**Rows:** 2,557 (one per day from 2020-01-01 to 2026-12-31)
**Used by:** `fact_bookings.date_id` references this. Makes date arithmetic declarative.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `date_id` | INT | **PK.** Surrogate key 1 through 2557 |
| `full_date` | DATE | The actual date |
| `day_of_month` | INT | 1–31 |
| `day_of_week` | VARCHAR(3) | Mon, Tue, Wed, Thu, Fri, Sat, Sun |
| `day_num` | INT | 1=Mon, 7=Sun |
| `week_of_year` | INT | ISO week number |
| `month` | INT | 1–12 |
| `month_name` | VARCHAR(20) | January, February, etc. |
| `quarter` | VARCHAR(2) | Q1, Q2, Q3, Q4 |
| `year` | INT | 2020–2026 |
| `is_weekend` | BOOLEAN | TRUE for Sat/Sun |
| `is_holiday` | BOOLEAN | TRUE if any holiday falls on this date |
| `is_high_demand_holiday` | BOOLEAN | TRUE only for HIGH-impact holidays |
| `season` | VARCHAR(10) | Peak (Oct-Feb) / Shoulder (Mar) / Summer (Apr-Jun) / Monsoon (Jul-Sep) |

#### Sample Data

```
date_id   full_date    day_of_week   month_name   quarter   is_weekend   is_holiday   is_high_demand_holiday   season
1         2020-01-01   Wed           January      Q1        FALSE        TRUE         TRUE                     Peak
2         2020-01-02   Thu           January      Q1        FALSE        FALSE        FALSE                    Peak
1767      2024-11-01   Fri           November     Q4        FALSE        TRUE         TRUE                     Peak  (Diwali)
1772      2024-11-06   Wed           November     Q4        FALSE        FALSE        FALSE                    Peak
```

#### Why this exists at all

Without `dim_date`, every query needs date arithmetic:
```sql
WHERE EXTRACT(MONTH FROM checkin_date) = 12
  AND EXTRACT(YEAR FROM checkin_date) = 2024
  AND EXTRACT(DOW FROM checkin_date) IN (0,6);  -- weekends, December 2024
```

With `dim_date`, the same query becomes declarative:
```sql
JOIN dim_date d ON b.date_id = d.date_id
WHERE d.month_name = 'December' AND d.year = 2024 AND d.is_weekend = TRUE;
```

The LLM generates the second pattern reliably. The first pattern requires the model to remember SQL date functions correctly across edge cases; the dimensional join is unambiguous and fast.

---

### `dim_location.csv`

**Purpose:** Geographic dimension — one row per city.
**Rows:** 993 cities across 35 states/UTs (44 base + 949 added in B-046 Stage 1 expansion). Existing 44 rows are untouched; the expansion is additive only.
**Used by:** `hotel_master.location_id`, `fact_bookings.location_id`

#### Schema

| Column | Type | Notes |
|---|---|---|
| `location_id` | UUID | **PK** |
| `city` | VARCHAR(50) | e.g. Jaipur, Goa, Manali |
| `state` | VARCHAR(50) | Standardized state name |
| `region` | VARCHAR(20) | North/South/East/West India |
| `tourism_zone` | VARCHAR(20) | Heritage/Beach/Hill Station/Pilgrimage/Wildlife/Metro/Backwater |
| `latitude` | DECIMAL(9,6) | Real coordinates |
| `longitude` | DECIMAL(9,6) | Real coordinates |
| `tourist_arrivals_annual_m` | DECIMAL(5,2) | City-level visitor count in millions |
| `peak_months` | TEXT | Pipe-separated months |

#### Sample Data

```
location_id   city      state        region        tourism_zone   latitude   longitude   tourist_arrivals_annual_m   peak_months
fcc0cf29-...  Jaipur    Rajasthan    North India   Heritage       26.9124    75.7873     4.2                         Oct|Nov|Dec|Jan|Feb
2fffc36c-...  Jodhpur   Rajasthan    North India   Heritage       26.2389    73.0243     2.1                         Oct|Nov|Dec|Jan|Feb
{goa_uuid}    Goa       Goa          West India    Beach          15.2993    74.1240     8.5                         Nov|Dec|Jan|Feb
{mumbai_uuid} Mumbai    Maharashtra  West India    Metro          19.0760    72.8777     12.5                        Jan|Feb|Mar|Oct|Nov|Dec
```

#### Key field

**`tourism_zone`** is the most queried column. *"Compare beach destinations to hill stations"* becomes `GROUP BY tourism_zone`. Without this, you'd hardcode city lists everywhere.

#### `tourist_arrivals_annual_m` — interpretation

**It is a hotel-demand proxy, NOT literal Ministry-of-Tourism footfall.** Real pilgrimage footfall in India (Tirupati ~50-80m/year, Sabarimala ~40m/year) is orders of magnitude larger than the hotel-bookable demand those destinations generate. The expansion script caps the value at **24.0** for the largest sites so the implied booking volume stays in a reasonable proportion to the rest of the catalog when the figure later feeds city-popularity weights in the booking generator.

Derived in the B-046 expansion as: `arrivals_m = POP_TIER_BASE[popularity_tier] × ZONE_FACTOR[tourism_zone] × jitter(0.85, 1.15)`, capped at 24.0. See `scripts/build_cities_expansion_csv.py` for the constants.

#### Source of truth: `seeds/cities_expansion.csv`

The 949 net-new cities are sourced from a curated CSV committed at `seeds/cities_expansion.csv` (an additional `popularity_tier` column there drives hotel-count bucketing — `mega`/`major`/`mid`/`small`/`obscure`). The CSV is regenerable from `scripts/build_cities_expansion_csv.py` (deterministic, SEED=42). Insertion into Postgres is handled by `scripts/expand_dimensions.py` (idempotent, single-transaction, FK-safe).

---

### `dim_customer.csv`

**Purpose:** Customer master with segmentation and demographics.
**Rows:** 100,000 (20,000 base + 80,000 added in B-046 Stage 1 expansion, IDs `CUST-020001`–`CUST-100000`). Travel-purpose vocabulary unchanged — the 9 canonical values are kept; no `Backwater` value was added (Backwater-zone customers map to `Wellness`).
**Used by:** `fact_bookings.customer_id`

#### Schema

| Column | Type | Notes |
|---|---|---|
| `customer_id` | VARCHAR(12) | **PK.** Format: `CUST-NNNNNN` |
| `first_name` | VARCHAR(30) | Real Indian names from 140-name pool |
| `last_name` | VARCHAR(30) | From 110-name pool |
| `age` | INT | 18–75, normal distribution centered at 35 |
| `gender` | CHAR(1) | M (52%) / F (48%) |
| `home_state` | VARCHAR(50) | Weighted by population (MH/KA/Delhi NCR top) |
| `customer_segment` | VARCHAR(20) | Leisure_FIT/Family/Business/Group_Tour/Honeymoon/Backpacker |
| `travel_purpose` | VARCHAR(20) | Heritage/Beach/Hill_Station/Religious/Business/Wildlife/Wellness/Adventure/Shopping |
| `loyalty_tier` | VARCHAR(10) | None (65%) / Silver (20%) / Gold (10%) / Platinum (5%) |
| `is_repeat_customer` | BOOLEAN | TRUE for ~35% |

#### Sample Data

```
customer_id    first_name   last_name   age   gender   home_state      customer_segment   travel_purpose   loyalty_tier   is_repeat_customer
CUST-000001    Vikram       Aggarwal    40    M        Karnataka       Business           Business         Silver         FALSE
CUST-000002    Shaurya      Patel       33    M        Karnataka       Leisure_FIT        Hill_Station     None           TRUE
CUST-000005    Usha         Kohli       32    M        Gujarat         Family             Heritage         Gold           TRUE
CUST-013881    Priya        Sharma      28    F        Maharashtra     Honeymoon          Beach            None           FALSE
```

#### Segment definitions

| Segment | Share | Typical behavior |
|---|---|---|
| Leisure_FIT | 45% | Free Independent Traveller — varied tier choice, 3-night avg stay |
| Family | 25% | Cost-conscious mid/budget, 4-night avg, low cancellation |
| Business | 15% | Premium tier preference, 1-2 nights, high cancellation (22%) |
| Group_Tour | 8% | Budget/Mid, 3-4 nights, near-zero cancellation |
| Honeymoon | 4% | Luxury preference, 5+ nights, lowest cancellation (4%) |
| Backpacker | 3% | Budget only, 1-3 nights, OYO-heavy |

#### Silver variant — schema changes

When Bronze CSV is processed to Silver Parquet:

| Column | Bronze (CSV) | Silver (Parquet) | Reason |
|---|---|---|---|
| `loyalty_tier` | empty string for non-members | explicit `'None'` | Avoid NULL confusion in queries |
| `home_state` | mixed casing possible | standardized to canonical names matching `ref_state_centroids.state_name` | Reliable joins for geo-flow analysis |
| `is_repeat_customer` | `"TRUE"`/`"FALSE"` strings | proper BOOLEAN | Type safety |
| `age` | INT | INT (validated 18–75) | Reject malformed rows |

All other columns unchanged in shape, only types enforced.

---

### `hotel_master.csv` (dim_hotel)

**Purpose:** Hotel master table — the most-queried dimension.
**Rows:** 20,076 hotels (2,000 base + 18,076 added in B-046 Stage 1 expansion, IDs `HTL-002001`–`HTL-020076`). Existing 2,000 rows untouched. New hotels have `review_count=0` until reviews accumulate; ALL fact tables (`fact_bookings`, `fact_booking_events`, `fact_booking_lifecycle`, `reviews_raw`) are unchanged by this expansion.
**Used by:** `dim_room_type.hotel_id`, `fact_bookings.hotel_id`, `fact_price_events.hotel_id`, `reviews_raw.hotel_id`

#### Schema

| Column | Type | Notes |
|---|---|---|
| `hotel_id` | VARCHAR(10) | **PK.** Format: `HTL-NNNNNN` |
| `hotel_name` | VARCHAR(100) | Mix of branded (OYO, Taj), heritage (Haveli, Palace), independent |
| `chain_name` | VARCHAR(50) | NULL for ~60% independents. Otherwise: OYO/Taj/ITC/Marriott/etc. |
| `property_type` | VARCHAR(30) | Hotel/Resort/Lodge/Houseboat/Palace/Villa (14 options) |
| `star_category` | INT | 0–5. Distribution: 35%/12%/10%/20%/15%/8% |
| `total_rooms` | INT | Budget: 10–40, Mid: 30–80, Premium: 60–150, Luxury: 80–300 |
| `location_id` | UUID | **FK** → `dim_location` |
| `avg_rating` | DECIMAL(3,2) | 3.2–4.9 — correlated with star_category |
| `amenities_json` | TEXT | **JSON string in CSV cell.** e.g. `["WiFi","AC","Swimming Pool"]` |
| `review_count` | INT | Drives review distribution. Budget: 5-200, Luxury: 500-7000 |
| `price_tier_id` | VARCHAR(10) | **FK** → `ref_price_tiers` |
| `base_price_inr` | INT | Nightly rate. Must fit within tier band. |
| `is_active` | BOOLEAN | ~95% TRUE. Inactive hotels generate no new bookings. |
| `opened_year` | SMALLINT | Year the hotel opened to guests (1975–2026). **Added via migration 006, not in base schema.** Synthetic but constrained — see *opened_year (migration 006)* below. Range was 1975–2023 until B-046 extended the upper bound to 2026 (no schema change — migration 006 carries no CHECK constraint; only data shape changed). |

#### Sample Data

```
hotel_id     hotel_name                 chain_name   property_type   star   rooms   avg_rating   price_tier   base_price_inr   is_active
HTL-000001   Karol Bagh Guest House     (NULL)       Hotel           0      13      3.61         BUDGET       912              TRUE
HTL-000002   Pink Inn                   (NULL)       Lodge           0      24      3.55         BUDGET       1679             TRUE
HTL-000003   Pichola Haveli             (NULL)       Hotel           4      118     4.32         PREMIUM      9945             TRUE
HTL-001700   Taj View Haveli            ITC Hotels   Hotel           5      168     4.39         LUXURY       34515            TRUE
HTL-001316   Hotel Aerocity             OYO          Hotel           3      59      4.07         MID          3521             TRUE
```

#### Two gotchas when loading

**1. `amenities_json` is a JSON string inside a CSV cell.**

To query "hotels with a swimming pool":
```sql
-- Bronze (CSV loaded as TEXT)
WHERE amenities_json LIKE '%Swimming Pool%'

-- Silver (loaded as JSONB or text[] in Postgres)
WHERE amenities @> '["Swimming Pool"]'::jsonb     -- JSONB containment
-- OR if loaded as text[]:
WHERE 'Swimming Pool' = ANY(amenities)
```

**2. `chain_name` empty means independent, not unknown.** Pandas reads these as NaN. When loading to Postgres, treat empty as `NULL` explicitly.

#### `opened_year` (migration 006)

Year the hotel opened to guests, range 1975–2026 (was 1975–2023 in the
original 2,000 hotels; B-046 expansion uses 1975–2026 freely — migration
006 carries no CHECK constraint, so no schema change was needed). **Added
via migration `db/migrations/006_hotel_opened_year.sql`, not in the base
schema** — the base `schema.sql` is frozen and every later structural
change ships as a numbered migration. Synthetic but constrained:

- Correlated with `star_category`: 5-star skews older (1975–2010), tighter
  ranges step forward through 4/3/2/1, uncategorised/budget skews newest
  (2005–2023).
- OYO override: `chain_name ILIKE 'oyo%'` clamps to 2010–2023 regardless
  of star (OYO founded 2013).
- HARD CONSTRAINT: `opened_year <= MIN(dim_date.year)` over the hotel's
  `fact_bookings` rows — a hotel cannot be booked before it opened.
  Verified invariant: 0 violations across all 2000 rows.

Populated by `scripts/populate_opened_year.py` with a fixed RNG seed so
re-runs reproduce. Distribution by decade after load: 1970s 22 · 1980s
108 · 1990s 281 · 2000s 583 · 2010s 748 · 2020s 258.

**Why it exists.** The 7B SQL model was answering "hotels created per
year" by counting `fact_bookings` rows (the L-010 dimension-vs-fact
confusion). A dedicated `opened_year` column on `hotel_master` makes the
correct query trivially expressible (`SELECT opened_year, COUNT(*) FROM
hotel_master GROUP BY opened_year`) and removes the failure mode at its
root. See the ENTITY COUNT rule in `ai/prompts/text_to_sql_system.txt`.

#### Silver variant — schema changes

When Bronze CSV is processed to Silver Parquet:

| Column | Bronze (CSV) | Silver (Parquet) | Reason |
|---|---|---|---|
| `amenities_json` | TEXT (JSON string `'["WiFi","AC"]'`) | renamed to `amenities`, type `JSONB` (or `TEXT[]`) | Native containment queries; indexable |
| `chain_name` | empty string for independents | explicit NULL | Distinguishes "no chain" from "unknown" |
| `is_active` | `"TRUE"`/`"FALSE"` strings | proper BOOLEAN | Type safety |
| `avg_rating` | DECIMAL(3,2) | DECIMAL(3,2) (validated 0.00–5.00) | Reject out-of-range rows |

**Query difference after promotion to Silver:**

```sql
-- Bronze (CSV — TEXT column)
SELECT hotel_name FROM hotel_master_bronze
WHERE amenities_json LIKE '%Swimming Pool%';

-- Silver (Postgres — JSONB column)
SELECT hotel_name FROM hotel_master
WHERE amenities @> '["Swimming Pool"]'::jsonb;
```

---

### `dim_room_type.csv`

**Purpose:** Room types per hotel (each hotel has 2–4).
**Rows:** 55,446 (5,542 base + 49,904 added in B-046 Stage 1 expansion). Three new `type_name` values were introduced by the expansion (free text — no migration needed): `Houseboat Suite` (Backwater hotels), `Tent` (Wildlife + Hill Station), `Treehouse` (Wildlife). Total distinct names: 16 (was 13).
**Used by:** `fact_bookings.room_type_id`, `fact_price_events.room_type_id`

#### Schema

| Column | Type | Notes |
|---|---|---|
| `room_type_id` | UUID | **PK** |
| `hotel_id` | VARCHAR(10) | **FK** → `hotel_master` |
| `type_name` | VARCHAR(50) | Standard Non AC/Standard AC/Deluxe/Suite/Sea View/Heritage Room/etc. |
| `capacity` | INT | 1–4 persons |
| `has_ac` | BOOLEAN | Non-AC common in hill station budget hotels |
| `has_breakfast` | BOOLEAN | TRUE for ~30% |
| `price_tier` | VARCHAR(10) | Matches parent hotel's tier |
| `base_price_inr` | INT | Per-night rate. Varies within tier band by room type. |

#### Sample Data

```
room_type_id    hotel_id     type_name           capacity   has_ac   has_breakfast   price_tier   base_price_inr
c7cc4a48-...    HTL-000001   Standard Non AC     4          FALSE    TRUE            BUDGET       842
6bbc9856-...    HTL-000001   Standard AC         1          TRUE     FALSE           BUDGET       358
{uuid}          HTL-000003   Deluxe              2          TRUE     TRUE            PREMIUM      8400
{uuid}          HTL-000003   Premium Suite       2          TRUE     TRUE            PREMIUM      15300
{uuid}          HTL-001700   Luxury Suite        2          TRUE     TRUE            LUXURY       45200
{uuid}          {goa_hotel}  Sea View            2          TRUE     TRUE            MID          4200
{uuid}          {manali}     Valley View         2          FALSE    FALSE           BUDGET       1800
```

#### Zone-aware room types

The generator created context-appropriate room types:
- **Beach hotels** → Sea View, Pool View
- **Hill stations** → Valley View, Cottage Room, Tent (B-046)
- **Heritage cities** → Heritage Room
- **Pilgrimage cities** → standard mix (no special types)
- **Backwater hotels** → Houseboat Suite (B-046)
- **Wildlife hotels** → Tent, Treehouse (B-046)

You won't find Sea View at a Jaipur hotel, Valley View at Mumbai, Houseboat Suite outside Backwater, or Treehouse outside Wildlife.

---

## Facts

The append-only "what happened" tables. Live in `s3://travellens-data/raw/` partitioned by year/month.

---

### `fact_bookings.csv` ⭐

**Purpose:** The central fact table. Every booking is one row.
**Rows:** 1,000,000
**Date range:** 2024-01-01 → 2026-05-16 (28 months)
**Total revenue (non-cancelled):** ₹2,097 crore
**Used by:** Every analytical query in the platform.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `booking_id` | UUID | **PK** |
| `hotel_id` | VARCHAR(10) | **FK** → `hotel_master` |
| `customer_id` | VARCHAR(12) | **FK** → `dim_customer` |
| `room_type_id` | UUID | **FK** → `dim_room_type` |
| `location_id` | UUID | **FK** → `dim_location` |
| `date_id` | INT | **FK** → `dim_date` (check-in date) |
| `checkin_date` | DATE | Actual check-in date |
| `checkout_date` | DATE | checkin + nights_stayed |
| `nights_stayed` | INT | Stored, not derived |
| `num_guests` | INT | 1–4 |
| `nightly_rate_inr` | INT | What was paid per night (after seasonality + surge) |
| `revenue_inr` | INT | `nightly_rate × nights_stayed`. Stored to avoid recomputing. |
| `booking_source` | VARCHAR(20) | MakeMyTrip/OYO/Booking.com/Goibibo/Direct/Walk-in/Agoda |
| `is_cancelled` | BOOLEAN | ~11.8% overall. Cancelled bookings still have a row. |
| `booking_ts` | TIMESTAMP | When booking was made (0–120 days before checkin) |
| `event_ts` | TIMESTAMP | Same as booking_ts. Used by the stream consumer for event-time watermarks in the streaming path. |

#### Sample Data

```
booking_id    hotel_id     customer_id   checkin_date   checkout_date   nights   guests   nightly_rate   revenue   source         is_cancelled
ccf846ec-...  HTL-001123   CUST-007508   2024-12-15     2024-12-16      1        1        4776           4776      MakeMyTrip     FALSE
f3d89fd1-...  HTL-001152   CUST-019021   2025-11-20     2025-11-22      2        1        2145           4290      Booking.com    FALSE
f0e82432-...  HTL-001683   CUST-014640   2024-11-30     2024-12-04      4        1        5674           22696     Goibibo        FALSE
```

#### Behavioral patterns baked in

This is what makes the data interview-worthy:

**Customer segment drives room tier:**
- Backpackers → 75% Budget, 1% Luxury
- Business → 65% Premium+Luxury combined
- Honeymoon → 75% Premium+Luxury

**Customer segment drives stay length:**
- Business: 1.92 avg nights (shortest)
- Honeymoon: 5.46 avg nights (longest)
- Family: 4.0 avg nights

**Customer segment drives cancellation:**
- Business: 22.2% (meetings shift)
- Honeymoon: 4.0% (most committed)

**Travel purpose drives destination zone:**
- Religious → 75% Pilgrimage zone
- Heritage → 65% Heritage zone
- Beach → 70% Beach zone

**Seasonality drives bookings:**
- Goa: 3x baseline Nov-Feb, 0.25x in monsoon
- Hill stations: 2.5x in May-Jun and Oct
- Heritage cities: 2.6x Oct-Feb, 0.4x in summer

**Holiday spikes:**
- Bookings around Diwali, Christmas, New Year: 2.2x normal levels

#### Silver variant — schema changes

When Bronze CSV is processed to Silver Parquet:

| Column | Bronze (CSV) | Silver (Parquet) | Reason |
|---|---|---|---|
| `is_cancelled` | `"TRUE"`/`"FALSE"` strings | proper BOOLEAN | Type safety |
| `checkin_date`, `checkout_date` | TEXT (`'2024-12-15'`) | proper DATE | Native date arithmetic |
| `booking_ts`, `event_ts` | TEXT | proper TIMESTAMP | Native timestamp filtering |
| (new) `is_valid_dates` | — | BOOLEAN computed: `checkout_date > checkin_date` | Quality flag for downstream filtering |
| (new) `derived_nights` | — | INT computed: `DATEDIFF(day, checkin_date, checkout_date)` | Sanity check vs `nights_stayed` |

**Validation rows rejected at Silver:** any row where `revenue_inr ≠ nightly_rate × nights_stayed` (tolerance ±1) or `checkout ≤ checkin`. In Bronze these stay; in Silver they're moved to a `fact_bookings_rejected/` quarantine path.

---

### `fact_price_events.csv`

**Purpose:** Price change history per (hotel, room_type).
**Rows:** 86,650 (~20–35 changes per hotel-room over 28 months)
**Used by:** "Show me price spikes in Goa around Diwali" — queries about pricing behavior.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `event_id` | UUID | **PK** |
| `hotel_id` | VARCHAR(10) | **FK** → `hotel_master` |
| `room_type_id` | UUID | **FK** → `dim_room_type` |
| `event_ts` | TIMESTAMP | When the price changed |
| `old_price_inr` | INT | Price before change |
| `new_price_inr` | INT | Price after change |
| `delta_inr` | INT | `new - old`. Can be negative. |
| `pct_change` | DECIMAL(5,2) | Percentage change |
| `change_reason` | VARCHAR(30) | SEASONAL (40%) / DEMAND_SURGE (25%) / COMPETITOR_MATCH (15%) / MANUAL_OVERRIDE (10%) / EVENT_NEARBY (10%) |
| `flink_window_id` | VARCHAR(30) | Synthetic window label (e.g. `win_20240318_22`). Named `flink_window_id` for legacy reasons — the streaming layer now uses a Python consumer with the same windowing semantics. Column kept for backward compatibility with the source CSV. |

#### Sample Data

```
event_id      hotel_id     event_ts              old_price   new_price   delta   pct_change   change_reason     flink_window_id
1d9f6dce-...  HTL-000001   2024-03-18 22:31:00   842         894         52      6.18         SEASONAL          win_20240318_22
428a5342-...  HTL-000001   2025-06-09 17:50:00   894         836         -58     -6.49        SEASONAL          win_20250609_17
4f4b317c-...  HTL-000001   2025-12-15 22:10:00   836         911         75      8.97         SEASONAL          win_20251215_22
{uuid}        HTL-001700   2024-10-30 16:00:00   28000       42000       14000   50.00        DEMAND_SURGE      win_20241030_16  (Diwali)
```

#### Why both old AND new price are stored

Saves a self-join. Query: *"average price change in December"*:
```sql
SELECT AVG(new_price_inr - old_price_inr) FROM fact_price_events
WHERE EXTRACT(MONTH FROM event_ts) = 12;
```

Without storing `old_price_inr`, you'd need a `LAG()` window function — much slower at scale.

---

### `reviews_raw.csv` ⭐

**Purpose:** Guest reviews — the semantic search corpus.
**Rows:** 30,000
**Used by:** Vector embedding pipeline (Phase 3) → cosine similarity search.

#### Schema

| Column | Type | Notes |
|---|---|---|
| `review_id` | UUID | **PK** |
| `hotel_id` | VARCHAR(10) | **FK** → `hotel_master` |
| `reviewer_name` | VARCHAR(60) | `First Last` from 140×110 name pool |
| `review_text` | TEXT | **The critical column.** 42–237 words. Real complaint/praise themes. |
| `rating` | INT | 1–5 stars. Distribution: 6/4/15/30/44% |
| `review_date` | DATE | Biased to 0–60 days after that city's peak season |
| `source` | VARCHAR(20) | MakeMyTrip/OYO/Booking.com/Goibibo/TripAdvisor/Google |
| `travel_type` | VARCHAR(20) | Family/Couple/Solo/Business/Friends/Pilgrimage |

#### Sample Data

**5-star Couple review (real positive themes):**
```
review_id:     711796cb-27a0-4826-8082-20fd9a4130ac
hotel_id:      HTL-001265
reviewer_name: Rahul Khan
rating:        5
review_date:   2025-09-28
source:        Goibibo
travel_type:   Couple
review_text:   "AC worked perfectly even in peak summer heat, cooled the room fast.
                10 minutes from the railway station, very convenient. Hot water was
                inconsistent, had to wait 20 minutes some mornings. A memorable stay,
                thank you team. The location was decent for the most part and we
                managed to get around without too much difficulty."
```

**2-star negative review (theme: cleanliness + staff):**
```
review_id:     {uuid}
hotel_id:      HTL-000437
reviewer_name: Tanya Sethi
rating:        2
review_date:   2024-10-21
source:        MakeMyTrip
travel_type:   Family
review_text:   "Bathroom was not clean, there were stains on the tiles. Rude
                behaviour at the front desk, very disappointing. The carpet looked
                like it hadn't been vacuumed in weeks. Will not be returning.
                Management needs to seriously look into these issues."
```

**Irrelevant off-topic review (the 5% noise):**
```
review_id:     {uuid}
hotel_id:      HTL-000891
reviewer_name: Komal Banerjee
rating:        3
review_date:   2024-08-15
source:        Google
travel_type:   Business
review_text:   "The weather was terrible. Heavy rain for three days straight. Could
                not step out at all. Watched Netflix in the room the whole time.
                Not the hotel's fault but the trip was a waste. Anyway, sharing
                this so others know what to expect."
```

#### Three quality properties

**1. Amenity-gated themes:**
Hotels without WiFi in `amenities_json` will NEVER have reviews mentioning WiFi. No pool complaints for hotels without pools. Your semantic search will surface real signal.

**2. 5% irrelevant noise (1,500 reviews):**
About taxi drivers, lost luggage, weather, personal events. Tests whether your cosine similarity correctly de-prioritizes them on queries like *"cleanliness complaints in Mumbai"*.

**3. 5% verbose reviews (1,500 reviews, 150–237 words):**
Long-form essays. Tests embedding model on longer documents.

#### Distribution facts

- Rating: ~6% 1★, ~4% 2★, ~15% 3★, ~30% 4★, ~44% 5★
- Top 10% of hotels capture 77% of reviews (power law)
- Every hotel has at least 1 review (no orphans)
- All reviews ≥42 words

#### Migration 010 — booking-tied stream columns (B-030)

Added by `db/migrations/010_review_stream.sql`. All new columns are nullable
except `record_source` (NOT NULL DEFAULT `'seed'`). Existing 30 K Kaggle rows
have `record_source = 'seed'` with the new columns NULL.

| Column | Type | Notes |
|---|---|---|
| `booking_id` | UUID | FK semantics → `fact_bookings` (no hard FK for ingest speed). NULL for seed rows. |
| `customer_id` | VARCHAR(12) | FK semantics → `dim_customer`. NULL for seed rows. |
| `review_stage` | VARCHAR(20) | Stage at which review was written: `booked`, `checked_in`, `checked_out`, `cancelled`. |
| `review_channel` | VARCHAR(100) | Where the review was posted. OTA channel MUST equal `booking_source` when used; direct channels (Email/SMS/WhatsApp/Phone·Call/Reception) are free. |
| `event_ts` | TIMESTAMPTZ | Wall-clock UTC when the event was emitted (consumer windowing). NULL for seed rows. |
| `event_date` | DATE | Sim-day or calendar day the review represents. NULL for seed rows. |
| `record_source` | VARCHAR(10) NOT NULL | `'seed'` (30 K Kaggle corpus) · `'history'` (backfill) · `'stream'` (live consumer). |

New indexes: `idx_reviews_raw_booking_id` (partial, WHERE booking_id IS NOT NULL) and `idx_reviews_raw_record_source`.

**Counts as of B-030a backfill:** 30,000 seed + 90,980 history = 120,980 total. History reviews: 14.9% of 612,380 pre-sim-today bookings (REVIEW_PROPENSITY_SCALE=0.47 in `scripts/review_generator.py`; to retune the rate adjust that constant and re-run `generate_review_backfill --reset`).

#### Migration 017 — per-review sentiment columns (B-026 Stage 1)

Added by `db/migrations/017_review_sentiment.sql`. Two nullable columns that hold
the REAL sentiment of each review, computed by a dedicated classifier — replacing
the B-062 rating proxy as the fix for L-012 (sentiment-topic conflation). The star
`rating` is a proven-bad proxy: genuine complaints live in mixed-sentiment 3★
reviews (29.6% of the corpus) that the ≤2★ / ≥4★ rating filter can never reach.

| Column | Type | Notes |
|---|---|---|
| `sentiment_label` | VARCHAR(8) | `positive` / `negative` / `neutral`. **NULL = unscored** — the natural backfill cursor (mirrors `embedding IS NULL`). |
| `sentiment_score` | NUMERIC(4,3) | Model confidence 0.000–1.000 for the chosen label. Stored so a future "strongly negative" confidence gate can be added without a re-backfill. |

New index: `idx_reviews_raw_sentiment` (partial, WHERE sentiment_label IS NOT NULL) —
supports the Stage-2 retrieval predicate `WHERE sentiment_label = 'negative'/'positive'`.

**Model:** CardiffNLP `twitter-roberta-base-sentiment-latest` (3-class RoBERTa),
pinned to safetensors revision `d616e2bdfcdb0ca89d9b6efc4909e1db063af290` and loaded
with `use_safetensors=True` — the main-revision `.bin` checkpoint is blocked on the
project's torch 2.3.0 by CVE-2025-32434, and torch is deliberately NOT upgraded
(L-008). Populated by `scripts/review_sentiment_scorer.py` (advisory lock 7400070):
`--once` runs the one-time 133K backfill, the bare command runs a continuous
micro-batch loop. **Sentiment is independent of the embedding** — these columns are
additive; the 133K vectors and the IVFFlat index are untouched.

**Scope (B-026):** Stage 1 ships the columns + backfill. The retrieval swap that
makes this user-visible (Stage 2 — replace the rating predicate in
`_search_reviews` with `sentiment_label`) and the run.py wiring (Stage 3) are
separate, each gated on the prior stage's verification.

#### Silver variant — schema changes

When Bronze CSV is processed to Silver Parquet:

| Column | Bronze (CSV) | Silver (Parquet) | Reason |
|---|---|---|---|
| `review_text` | TEXT (mixed case, formatting) | TEXT (unchanged — preserved as written) | Embeddings consume the original |
| (new) `review_text_normalized` | — | TEXT (lowercased, extra whitespace collapsed) | Optional input for some keyword-search baselines |
| `review_date` | TEXT | proper DATE | Native filtering |
| `rating` | INT | INT (validated 1–5) | Reject malformed |
| (new) `word_count` | — | INT | Pre-computed for filtering "short" vs "verbose" reviews |
| (new) `is_off_topic` | — | BOOLEAN | Flagged via simple heuristic (no hotel-related keywords) — useful for tuning semantic search |

The `review_embeddings` table (Gold layer) consumes the original `review_text`, not the normalized version. Embeddings are case- and structure-aware.

---

## Gold Layer — Derived Analytics

Pre-aggregated tables that power dashboards and frequently-asked queries. None of these exist as CSVs — they are produced by the streaming consumer (Phase 2) or Airflow batch jobs (Phase 4) and materialized into Postgres + Parquet.

**Documenting them now** even though they're not all populated yet, because the schema is the contract between pipeline producers and dashboard consumers. Build them in Phase 2–5 per the implementation plan.

---

### `agg_hourly_city_stats`

**Purpose:** Live city-level KPIs, updated continuously by the streaming consumer.
**Produced by:** `scripts/stream_consumer.py` (Phase 2)
**Update cadence:** One row per (city, window_start) per tumbling window — 1 hour in production, 2 minutes during dev
**Lookback:** All windows since pipeline started — never truncated

#### Schema (actual, as deployed)

| Column | Type | Notes |
|---|---|---|
| `city` | VARCHAR(100) | **PK part 1.** From `dim_location.city` |
| `window_start` | TIMESTAMP | **PK part 2.** Start of the tumbling window |
| `window_end` | TIMESTAMP | End of the window. Derived as `window_start + WINDOW_SIZE_MINUTES` |
| `total_bookings` | INTEGER | Count of BOOKING events in this window for this city |
| `total_revenue_inr` | NUMERIC(15,2) | Sum of `revenue_inr` from BOOKING events |
| `avg_occupancy_rate` | NUMERIC(5,2) | Occupancy estimate as a percentage (0.00–100.00). Approximated as `min(bookings / 50.0 × 100, 100.0)` since true occupancy needs room-inventory join |
| `cancellation_rate` | NUMERIC(5,4) | `cancellations / (bookings + cancellations)` as a fraction in `[0, 1]` |
| `ingestion_ts` | TIMESTAMP | When this row was written. `DEFAULT CURRENT_TIMESTAMP`, NOT NULL |
| `total_checkins` | INTEGER | Count of CHECKIN events in this window for this city. **Added via migration 007.** NULL on rows written before the consumer is taught to populate it (L-014 fix). |
| `total_checkouts` | INTEGER | Count of CHECKOUT events in this window. **Migration 007.** Same caveat as `total_checkins`. |
| `total_cancellations` | INTEGER | Count of explicit CANCELLATION events. **Migration 007.** Distinct from `cancellation_rate` (which is the booking-to-cancellation ratio); this is a raw count. |
| `total_reviews` | INTEGER | Count of REVIEW events landed in this window. **Migration 007.** Today reviews are batch-loaded only — column stays NULL until a REVIEW event type ships in the producer/consumer (see B-030). |

> **Schema notes:** The column is named `avg_occupancy_rate` for historical reasons — it actually holds a percentage value (0–100), not a rate (0–1). The other metric `cancellation_rate` is a true rate. `cancellation_rate` and `ingestion_ts` shipped in Phase 2 but were undocumented here until migration 007 — the migration records them with `IF NOT EXISTS` so a single migration file is a complete record of the columns the live pipeline depends on. Migration 007 also adds the four `total_checkins`/`total_checkouts`/`total_cancellations`/`total_reviews` count columns; the consumer write-path that populates them is a separate change (the storage is in place, the producer/consumer wiring is the next chunk).

#### Sample Data (real, from a 5-minute dev run)

```
city     window_start         window_end           bookings   revenue_inr     occupancy_rate   cancellation_rate
Goa      2026-05-17 16:26     2026-05-17 16:28           87       187234.00            87.00            0.1023
Mumbai   2026-05-17 16:26     2026-05-17 16:28          124       298891.00           100.00            0.0833
Jaipur   2026-05-17 16:26     2026-05-17 16:28           45        81450.00            90.00            0.0625
Manali   2026-05-17 16:28     2026-05-17 16:30           38        59320.00            76.00            0.1351
```

#### Use cases

- *"Goa occupancy right now"* — most recent row for city='Goa'
- *"Hour-over-hour booking trend Mumbai today"* — time series over `window_start`
- *"Cities with cancellation spikes in last 7 days"* — `cancellation_rate > 0.20`

#### Stream consumer behavior (Phase 2)

The producer of this table is `scripts/stream_consumer.py`. Worth noting in the data model because the consumer's failure handling determines which events do and don't appear in this aggregate:

- **Valid events** → accumulate in memory, flush at window close. One row per `(city, window_start)`, UPSERT semantics.
- **Malformed events** (failed schema validation — missing field, unknown event_type, unknown city, unparseable timestamp, unparseable JSON) → routed to operational quarantine, NOT included in this aggregate. The data model does not see them.
- **Late events** (arrived after their window closed) → routed to operational quarantine, NOT included in this aggregate at flush time. A future reconciliation job (Phase 6) will additively merge late events into the affected `(city, window_start)` rows.

The `ingestion_ts` column reflects when the row was last UPSERTed. A row whose `ingestion_ts` is significantly later than its `window_end` indicates a re-flush — either from a dev-mode short window reopening, or eventually from the Phase 6 late-event reconciler.

---

### `pipeline_metrics`

**Purpose:** Append-only consumer heartbeat. One row written every ~5–10s by `scripts/stream_consumer.py` capturing the current snapshot of its in-memory `run_metrics` counters. Powers the monitor's LIVE THROUGHPUT view (events/sec, lag, consumer-alive).
**Produced by:** `scripts/stream_consumer.py` (B-032 / migration 007; consumer write-path is the next chunk after the migration)
**Update cadence:** ~5–10s per row while the consumer is running; gaps imply the consumer is down
**Lookback:** Append-only, never truncated; meant to be trimmed by retention later (not yet implemented)

#### Schema (actual, as deployed in migration 007)

| Column | Type | Notes |
|---|---|---|
| `metric_ts` | TIMESTAMP | **PK.** Heartbeat timestamp the consumer wrote the row at |
| `events_consumed` | BIGINT | Cumulative consumed-since-start. NOT NULL, default 0 |
| `bookings` | BIGINT | Cumulative BOOKING events seen. NOT NULL, default 0 |
| `cancellations` | BIGINT | Cumulative CANCELLATION events seen. NOT NULL, default 0 |
| `malformed` | BIGINT | Cumulative Gate 1/2 drops (failed schema validation, parse errors, unknown event types, unknown cities). NOT NULL, default 0 |
| `late` | BIGINT | Cumulative late-watermark drops (events arriving after their window closed). NOT NULL, default 0 |
| `active_windows` | INTEGER | Open windows held in memory at heartbeat time. NOT NULL, default 0 |
| `max_event_ts` | TIMESTAMP | Latest event timestamp seen so far. Nullable (no events yet → NULL) |
| `consumer_lag` | BIGINT | Kafka consumer lag at heartbeat time. Nullable — may be unavailable from kafka-python |

#### Why this exists (resolves L-015)

The consumer's `run_metrics` counters used to live only in memory and print only at shutdown — nothing queryable mid-run. So the monitor could only infer activity from what landed in `agg_hourly_city_stats` (which only moves when an hourly window flushes) and from object counts in the MinIO quarantine prefixes. That made the monitor a batch-rollup viewer, not a real-time monitor. `pipeline_metrics` is the live metrics path that runs alongside the hourly aggregate — fine windows for "now", coarse windows for "the trend" — exactly how production systems run both.

#### Use cases

- *"Events per second right now"* — `(LATEST.events_consumed - PREVIOUS.events_consumed) / interval_seconds`
- *"Is the consumer alive?"* — heartbeat age = `NOW() - MAX(metric_ts)`; large = down
- *"Current Kafka lag"* — latest `consumer_lag`
- *"Late-event rate trend"* — `late` delta over a sliding window
- *"Open windows held in memory"* — latest `active_windows`, alerts if it grows without bound

#### Read pattern (monitor)

```sql
-- last two heartbeats, for an events/sec delta
SELECT *
FROM pipeline_metrics
ORDER BY metric_ts DESC
LIMIT 2;
```

Counters are cumulative-since-consumer-start, so the read path derives rates as deltas between adjacent rows rather than storing a `rate` column. Restarts reset counters; rate calculations should drop deltas where the newer cumulative value is lower than the older one (process restart).

---

### `agg_daily_hotel_kpi`

**Purpose:** Per-hotel daily performance summary.
**Produced by:** Airflow DAG `daily_hotel_kpi_dag.py` (Phase 4 of blueprint)
**Update cadence:** Daily at 02:00 IST for previous day's data
**Lookback:** Recomputable from `fact_bookings` for any historical date

#### Schema

| Column | Type | Notes |
|---|---|---|
| `hotel_id` | VARCHAR(10) | **PK part 1.** From `hotel_master` |
| `date_id` | INT | **PK part 2.** From `dim_date` |
| `bookings_count` | INT | Confirmed bookings (excludes cancellations) |
| `cancellations_count` | INT | Cancellations made on this date |
| `total_revenue_inr` | BIGINT | Sum of revenue for confirmed bookings |
| `avg_nightly_rate_inr` | INT | Mean rate paid |
| `total_room_nights_sold` | INT | Sum of `nights_stayed` for confirmed bookings |
| `occupancy_pct` | DECIMAL(5,2) | `(room_nights_sold / (total_rooms × 1)) × 100` — assumes daily slice |
| `unique_customers` | INT | Distinct `customer_id` count |
| `top_source` | VARCHAR(20) | Most-used `booking_source` that day |

#### Sample query usage

```sql
-- "Which Goa hotels had the highest December occupancy?"
SELECT h.hotel_name, AVG(k.occupancy_pct) AS avg_occ
FROM agg_daily_hotel_kpi k
JOIN hotel_master h USING(hotel_id)
JOIN dim_location l USING(location_id)
JOIN dim_date d USING(date_id)
WHERE l.city = 'Goa' AND d.month = 12 AND d.year = 2024
GROUP BY h.hotel_name
ORDER BY avg_occ DESC LIMIT 10;
```

---

### `agg_monthly_zone_summary`

> **Planned — not yet deployed.** Will be created by a Phase 6 Airflow DAG (`monthly_zone_summary_dag.py`, B-015 or B-016).

**Purpose:** Monthly roll-up of booking volume, revenue, and cancellation rate by `dim_location.tourism_zone`.
**Planned cadence:** Monthly, day 1 for previous month.
**Use cases:** Seasonality charts; zone-vs-zone revenue comparisons; year-over-year growth.

---

### `customer_lifetime_value`

> **Planned — not yet deployed.** Will be created by a Phase 6 Airflow DAG (`clv_weekly_dag.py`, B-016).

**Purpose:** Per-customer aggregate: lifetime revenue, nights, cancellation rate, CLV tier (PLATINUM/GOLD/SILVER/BRONZE).
**Planned cadence:** Weekly, full customer base.
**Use cases:** Segment-level value analysis; loyalty targeting; cancellation-prone segment identification.

---

### `review_embeddings`

**Purpose:** Vector representation of every review's text, for semantic search.
**Produced by:** One-time embedding job in Phase 3 (re-runs when new reviews ingested)
**Storage:** Postgres column using **pgvector** extension
**Update cadence:** Generated once per review at ingest time; immutable afterwards

#### Schema

| Column | Type | Notes |
|---|---|---|
| `review_id` | UUID | **PK / FK** → `reviews_raw` |
| `embedding` | `vector(384)` | 384-dim float vector from `sentence-transformers/all-MiniLM-L6-v2` |
| `model_version` | VARCHAR(50) | e.g. `'all-MiniLM-L6-v2'` — for audit when models change |
| `embedded_at` | TIMESTAMP | When this row was generated |

**Storage in this build:** Column `embedding vector(384)` added to `reviews_raw` via Phase 3 migration (option 2). The standalone-table schema above is the production-flexible form. The actual IVFFlat index now uses `lists=120` paired with a session-scoped `SET ivfflat.probes = 11` in `ai/semantic_search.py` (B-051; sized for the 133K-row post-B-030a / B-046 corpus). The original `lists=30` was correct for the 30K seed corpus only.

#### Index (actual, on `reviews_raw`)

```sql
-- Build-time (one of: the manual rebuild shipped under B-051 turn 1,
-- or a future re-run of scripts/generate_embeddings.py — its
-- _ivfflat_lists() helper sizes this automatically for the corpus):
CREATE INDEX ON reviews_raw
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 120);   -- live value; helper computes 133 against the live ~133K-row corpus

-- Query-time (per-connection GUC inside ai/semantic_search.run()):
SET ivfflat.probes = 11;
```

**Rule of thumb for resizing the index.** `lists ≈ rows / 1000` up to ~1M vectors (HNSW above that — tracked as L-005; `_ivfflat_lists()` falls back to `int(sqrt(rows))` past 1M as a graceful interim); `probes ≈ sqrt(lists)`. Raising `lists` without raising `probes` silently degrades recall — `lists=120` with the pgvector default `probes=1` would scan only ~1/120 of partitions per query. The two parameters move together: `_ivfflat_lists()` in `scripts/generate_embeddings.py` handles the build-side size automatically; the `SET ivfflat.probes` line in `ai/semantic_search.py` must be paired manually if a future re-embed produces an index whose lists differ materially from the current 120 (e.g. lists jumps to 250 → probes target ≈ 16).

#### Use cases

- *"Find reviews similar to this one"* — exact cosine lookup
- *"Cluster reviews by theme"* — k-means on embeddings
- Powers `hotel_sentiment_scores` aggregation downstream

---

### `hotel_sentiment_scores`

> **Planned — not yet deployed.** Depends on `review_embeddings` being populated (Phase 3 ✓) and a sentiment clustering DAG (Phase 6+).

**Purpose:** Per-hotel theme-specific sentiment scores (cleanliness, staff, location, food, value) derived from pgvector embeddings.
**Planned cadence:** Weekly, recomputed against full review corpus.
**Use cases:** Disconnect analysis (high rating but low cleanliness score); ranking hotels by specific sentiment dimensions; trend monitoring.

---

## Lifecycle Events + Simulator State

Two tables that bridge the historical batch world (`fact_bookings`) and the
live stream world (`scripts/kafka_event_producer.py`). Both ship in
migration **`db/migrations/008_lifecycle_events.sql`** and are populated by
`scripts/generate_lifecycle_history.py`.

The pivot: `fact_bookings` records *one row per booking with start and end
dates*. The stream world thinks in *events* — one wire message per state
transition (BOOKING, CHECKIN, CHECKOUT, CANCELLATION). To run live and
historical analytics off the same table, the historical bookings are
**exploded** into the events they imply, and the live stream writes its
events into the same table. A `source` column distinguishes them.

### The `sim-today` anchor

Every generator and simulator command takes `--sim-today` (default
**`2025-06-01`**, picked so ~11–12 months of real `fact_bookings` rows
remain ahead of it as the stream's future runway). It is the project's
calendar "now."

The generator processes **every row of `fact_bookings` exactly once**
and routes it into one of four buckets based on `booking_ts`,
`checkin_date`, `checkout_date` vs the anchor:

```
                        booking_ts < sim-today              booking_ts >= sim-today
                ┌──────────────────────────────────┐  ┌──────────────────────────┐
checkout <  S   │ COMPLETED                        │  │   (impossible — guard:   │
                │   history: BOOKING + CHECKIN +   │  │    booking_ts must be    │
                │   CHECKOUT  (or BOOKING +        │  │    on/before check-in)   │
                │   CANCELLATION if is_cancelled)  │  │                          │
                ├──────────────────────────────────┤  ├──────────────────────────┤
checkin < S     │ IN_PROGRESS  (guest mid-stay)    │  │                          │
≤ checkout      │   history: BOOKING + CHECKIN     │  │                          │
                │   sim_open_bookings: CHECKED_IN  │  │                          │
                ├──────────────────────────────────┤  ├──────────────────────────┤
checkin >= S    │ BOOKED  (awaiting check-in)      │  │ FUTURE — SKIPPED.        │
                │   history: BOOKING               │  │   Reserved for the       │
                │   sim_open_bookings: BOOKED      │  │   stream simulator       │
                │                                  │  │   to replay later.       │
                └──────────────────────────────────┘  └──────────────────────────┘
```

**The crucial property: the open backlog is real.** Every row in
`sim_open_bookings` is a booking that actually exists in `fact_bookings`
— the simulator advances real customers staying at real hotels with
real reservations. Nothing is invented.

**FUTURE bookings are reserved for the stream.** Roughly 388K of the
1M `fact_bookings` rows have `booking_ts >= sim-today (2025-06-01)`
running out to 2026-05-16. They are deliberately NOT written as
history events — they are exactly what the stream simulator (next
backlog item, B-035 "next step") will replay, sorted by `booking_ts`,
as `source='stream'` BOOKING events.

The stream simulator picks rows out of `sim_open_bookings` to advance
CHECKIN → CHECKED_IN → CHECKOUT (or BOOKED → CANCELLATION) at their
real dates, and emits the FUTURE bookings as `source='stream'`.

### The `source` concept

`fact_booking_events.source` is the only thing that distinguishes a
backfilled event from a live one:

| `source`  | Written by                                      | When            |
|-----------|-------------------------------------------------|-----------------|
| `history` | `scripts/generate_lifecycle_history.py`         | One-shot backfill |
| `stream`  | `scripts/stream_consumer.py` (via Kafka)        | Continuous, post-anchor |

Analytics queries usually ignore `source` (a checkout is a checkout).
Operational queries (monitor, lag tracking) filter on it.

---

### `fact_booking_events`

**Purpose:** Append-only silver ledger — one row per lifecycle event.
**Produced by:** `scripts/generate_lifecycle_history.py` (history) +
`scripts/stream_consumer.py` (stream).
**Update cadence:** Bulk-insert at backfill time; continuous from the
stream consumer thereafter.
**Explore-tab routing (B-048):** the AI text-to-SQL layer routes live questions
(`today / now / live / streaming / so far today / last <N> hour(s)`) to this
table with `source='stream'`; all historical/analytical questions route to
`fact_bookings` instead. See `ai/prompts/text_to_sql_system.txt` "HISTORICAL vs
LIVE" section for the standing convention.

#### Schema (as deployed by migration 008)

| Column | Type | Notes |
|---|---|---|
| `event_id` | UUID | **PK.** One per event row, regardless of booking. |
| `event_type` | VARCHAR(20) | `BOOKING` / `CHECKIN` / `CHECKOUT` / `CANCELLATION` (today). Reserved: `PRICE_CHANGE`, `REVIEW`. CHECK constraint enforces allowed set. |
| `booking_id` | UUID | The booking this event belongs to. Same `booking_id` is repeated across BOOKING + CHECKIN + CHECKOUT (or BOOKING + CANCELLATION). |
| `customer_id` | VARCHAR(20) | **FK semantics → `dim_customer`** (no FK constraint declared for ingest speed). |
| `hotel_id` | VARCHAR(20) | **FK semantics → `hotel_master`**. |
| `city` | VARCHAR(50) | Resolved via `dim_location` at write time. Denormalised so the stream-partition key never needs a runtime join. |
| `room_type_id` | UUID | **FK semantics → `dim_room_type`**. |
| `event_ts` | TIMESTAMPTZ | When the event happened in event-time. For history: real `booking_ts` for BOOKING; afternoon of `checkin_date` for CHECKIN; morning of `checkout_date` for CHECKOUT; uniform between booking and check-in for CANCELLATION. |
| `event_date` | DATE | `event_ts::date`. Indexed; cheap date-range filter. |
| `checkin_date`, `checkout_date`, `nights`, `num_guests` | — | Same values as the underlying booking. Repeated on every event row for that booking so per-event queries don't need a self-join. |
| `nightly_rate_inr`, `revenue_inr` | NUMERIC(10,2) | **Invariant on BOOKING rows:** `revenue_inr == nightly_rate_inr × nights`. Asserted in the generator and verified by acceptance. |
| `booking_source` | VARCHAR(50) | OTA / channel. |
| `payment_mode` | VARCHAR(20) | `UPI` / `CREDIT_CARD` / `DEBIT_CARD` / `NET_BANKING` / `WALLET`. Synthesised in history (fact_bookings doesn't carry it). |
| `cancellation_reason` | VARCHAR(40) | Set ONLY on CANCELLATION rows: `customer_cancelled` (~70%) or `no_show` (~30%). |
| `rating`, `review_channel`, `review_text` | — | Reserved for REVIEW (B-030). Always NULL today. |
| `old_price_inr`, `new_price_inr` | NUMERIC(10,2) | Reserved for PRICE_CHANGE. Always NULL today. |
| `source` | TEXT | `'history'` or `'stream'`. CHECK constraint enforced. |
| `ingested_at` | TIMESTAMPTZ | When the row hit the table. Default `now()`. |

Indexes: `(booking_id)`, `(event_date)`, `(hotel_id)`, `(source)`.

#### Why type-specific columns are nullable

A single events table is the simplest schema that supports every event
type; nullable columns hold per-type payload. The alternative (one table
per event type) would force a `UNION ALL` on every analytical query —
slower to write, slower to teach, slower to query.

---

### `sim_open_bookings`

**Purpose:** Mutable simulator state — the "open" bookings the stream
producer can advance.
**Produced by:** `scripts/generate_lifecycle_history.py` (initial seed) +
`scripts/kafka_event_producer.py` (live mutations as it advances rows).
**Update cadence:** Continuous while the producer runs; rows are
deleted as bookings CHECKOUT or CANCEL.

#### Schema (as deployed by migration 008)

| Column | Type | Notes |
|---|---|---|
| `booking_id` | UUID | **PK.** Matches the BOOKING event already written to `fact_booking_events`. |
| `customer_id` | VARCHAR(20) | Real customer from `dim_customer`. |
| `hotel_id` | VARCHAR(20) | Real hotel from `hotel_master`. |
| `city` | VARCHAR(50) | Denormalised from `dim_location.city`. |
| `room_type_id` | UUID | Real room from `dim_room_type` — chosen from the **hotel's own** room types (a Goa hotel's "Sea View" cannot end up on a Jaipur booking). |
| `checkin_date`, `checkout_date`, `nights`, `num_guests` | — | Schedule of the open booking. **Invariant:** `checkout_date > sim-today` for every row, so the producer always has something to advance toward. |
| `nightly_rate_inr`, `payment_mode`, `booking_source` | — | Same shape as the BOOKING event. |
| `state` | TEXT | `'BOOKED'` (awaiting CHECKIN), `'CHECKED_IN'` (awaiting CHECKOUT), or `'REVIEW_PENDING'` (awaiting deferred REVIEW). CHECK constraint enforced. **REVIEW_PENDING added by migration 015 (B-047).** |
| `booked_event_ts` | TIMESTAMPTZ | When the BOOKING event for this row was emitted — handy for "how long has this been open." |
| `source` | TEXT | Default `'history'`. The forward generator (B-047) writes `'stream'` for net-new bookings it mints. |
| `checkin_fire_ts` | TIMESTAMPTZ NULL | **(migration 015, B-047)** Wall-clock time at which the CHECKIN event should fire. Sampled in IST from `CHECKIN_HOUR_DIST` (12-21 IST, peak 16-18) at BOOKING emit time. NULL on legacy `source='history'` rows. |
| `checkout_fire_ts` | TIMESTAMPTZ NULL | **(migration 015, B-047)** Same for CHECKOUT, sampled from `CHECKOUT_HOUR_DIST` (08-13 IST, peak 09-10) on `checkout_date`. |
| `cancel_fire_ts` | TIMESTAMPTZ NULL | **(migration 015, B-047)** NULL unless the deterministic cancel-decision `Random(booking_id\|cancel\|daily_seed).random() < P_CANCEL` (=0.12) selects this booking; if so, sampled from `CANCELLATION_HOUR_DIST` (09-21 IST, evening-lean) on a date in `[booking_ts, checkin_date]`. |
| `review_fire_ts` | TIMESTAMPTZ NULL | **(migration 015, B-047)** NULL until CHECKOUT or CANCELLATION fires; then, if `make_review_event_dict` produces a payload, set to a time tonight 20-23 IST (`REVIEW_HOUR_DIST`, clamped ≥ anchor+30min) and state advances to `'REVIEW_PENDING'`. Row is deleted on REVIEW emit. |

Indexes (post-015):
- `(booking_id)` PK
- `(checkin_date)`, `(checkout_date)`, `(state)` — pre-existing
- `(checkin_fire_ts) WHERE state='BOOKED' AND checkin_fire_ts IS NOT NULL` — partial, B-047
- `(checkout_fire_ts) WHERE state='CHECKED_IN' AND checkout_fire_ts IS NOT NULL` — partial, B-047
- `(cancel_fire_ts) WHERE state='BOOKED' AND cancel_fire_ts IS NOT NULL` — partial, B-047
- `(review_fire_ts) WHERE review_fire_ts IS NOT NULL` — partial, B-047

**Catch-up semantics.** When the producer restarts and a row's fire_ts is already in the past, the event fires as soon as the diurnal-rate bucket has capacity, with `event_ts = NOW()` (current wall-clock UTC) and `event_date = today in IST`. The original stamped time is NEVER written to the wire — it lives only in `sim_open_bookings` so the producer knows the row is overdue.

#### Why a separate mutable table

`fact_booking_events` is an **immutable** ledger (the audit trail).
`sim_open_bookings` is **mutable** simulator state (rows deleted on
CHECKOUT/CANCELLATION/REVIEW). Mixing them would lose the audit trail.
Same pattern as event-sourcing + a materialised projection.

---

### `sim_daily_counter`

**Purpose:** Per-day TOTAL-EVENTS guardrail for the forward generator (B-047). Lets multiple on/off producer sessions in the same IST day collectively respect a single cap.
**Produced by:** `scripts/kafka_event_producer.py` (B-047 forward generator) — `INSERT … ON CONFLICT DO NOTHING` for today's row at startup; per-tick UPDATE increment.
**Update cadence:** Continuous while the producer runs (one UPDATE per tick batch).
**Added by:** Migration 014 (B-047).

#### Schema (as deployed by migration 014)

| Column | Type | Notes |
|---|---|---|
| `counter_date` | DATE | **PK.** Date in **IST**, not UTC — the rate curve and daily semantics are anchored to the Indian business day. |
| `events_emitted` | INTEGER NOT NULL DEFAULT 0 | Cumulative TOTAL events emitted today across all sessions (BOOKING + CHECKIN + CHECKOUT + CANCELLATION + REVIEW + PRICE_CHANGE). Includes events lost downstream to chaos/quarantine — it counts what the producer *emitted*, not what the consumer accepted. |
| `cap` | INTEGER NOT NULL | `1_000_000 × rate_multiplier`. **LAST-WRITE-WINS across same-day sessions** (B-047 + migration 016): every producer-startup UPSERT overwrites cap with the new value, so a follow-up `python run.py --rate-multiplier 3` over a bare `python run.py` (=1) jumps today's cap from 1M to 3M immediately. `events_emitted` is **NOT** overwritten — it keeps accumulating across all sessions in the day. |
| `updated_at` | TIMESTAMPTZ NOT NULL DEFAULT NOW() | Stamped on every UPDATE. |

Index: PK on `(counter_date)`.

#### Why a separate single-row-per-day table

The cap could have been encoded as a single row in some "simulator state" table, but a per-day row keeps the audit trail honest — you can see exactly how many events landed each IST day, and tune the cap historically by row without rewriting code. The producer's startup UPSERT writes (and overwrites) today's `cap` from the current `rate_multiplier`, so a mid-day `--rate-multiplier N` change takes effect immediately on the next session (both up and down — B-047 + migration 016). Day rollover: at the next tick whose IST date differs from the cached one, the producer inserts a new row at the current `rate_multiplier`.

**Cap-hit behaviour.** When `events_emitted >= cap`, the producer sleeps until IST midnight and skips its tick loop entirely (no BOOKING, no PRICE_CHANGE, no lifecycle drain). On the IST midnight wake-up the new day's row is inserted at the current `rate_multiplier` and emission resumes. Overdue lifecycle rows then drain through the catch-up rule with `event_ts = NOW()`.

---

### How history and stream stay aligned

`sim_open_bookings` at startup mirrors the real shape of in-flight
bookings at the anchor: at `--sim-today 2025-06-01` the seed is ~17K
rows (~14K BOOKED awaiting CHECKIN; ~3K CHECKED_IN awaiting CHECKOUT)
— the actual mid-flight backlog that fact_bookings implies. The
producer can advance the CHECKED_IN slice toward CHECKOUT immediately
without waiting for new bookings to accumulate. This is the cold-start
solve that B-034 left open.

The continuity property the generator guarantees: **latest `source='history'`
`event_date` is at most 7 days before `sim-today`** (acceptance threshold).
At the default anchor, the latest history event lands on 2025-05-31 —
1 day before sim-today — so the join from "what just happened" to
"what's happening now" has no visible void on the monitor or in
analytical queries.

---

### The sim-clock (`scripts/.sim_clock.json`) and `event_date` — RETIRED by B-047

> **Status (2026-05-28).** The sim-clock model described below is **retired**. The forward generator (B-047, current producer) has NO sim-clock and reads no clock file — `.sim_clock.json` is deleted on first launch and never re-created. `event_date` on the wire is now simply `today in IST` at emit time. The historical description below is preserved for context; see [`sim_daily_counter`](#sim_daily_counter) for the replacement state, [Schema Evolution → 014/015](#schema-evolution) for the migrations, and [B-047](docs/backlog.md#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--done) for the rewrite rationale.

The calendar-replay producer (`scripts/kafka_event_producer.py`,
B-034A) walks a sim-clock one day at a time. Two pieces of state make
that work:

**The clock file.** A small JSON file at `scripts/.sim_clock.json`
(gitignored — local machine state, rebuilt on first run):

```json
{
  "last_completed_day": "2025-06-04",
  "chaos_seed": 42
}
```

On startup, the producer reads `last_completed_day` and resumes at
the next day. If the file is absent (or `--reset-clock` is passed) it
starts at `--sim-start` (default `2025-06-01`, MUST match the
generator's anchor). `chaos_seed` is persisted alongside so
cancellation plans — which are keyed off `f"{booking_id}|{chaos_seed}"`
— stay deterministic across restarts without the user re-passing the
flag.

The clock is written at the END of each sim-day, AFTER Kafka flush +
DB commit. Day boundaries are the atomic commit points; a graceful
Ctrl-C finishes the in-progress day before exiting.

**`event_date` on the wire.** Every event the producer emits now
carries TWO timestamps:

| Field | Meaning | Why |
|---|---|---|
| `event_ts` | Wall-clock UTC NOW at emit time | Keeps the consumer's event-time windowing, watermarks, and freshness checks unchanged — windows close in seconds, the `/monitor` heartbeat stays "alive." |
| `event_date` (NEW, additive) | The sim-day the event represents (ISO date) | Carries the *business* date for downstream silver consumers. The consumer's Gate 2 requires only `(event_type, event_ts, city, hotel_id)`; unknown fields pass through harmlessly. |

The split matters because Phase A replays bookings whose
`checkin_date` / `checkout_date` are in 2025–2026 but `event_ts` is
"now." Without `event_date`, downstream consumers that care about the
business day a CHECKIN happened on would have to derive it from the
booking — fragile and expensive. With `event_date`, it's one column.

**Mutating `sim_open_bookings` live.** The producer treats
`sim_open_bookings` as live working state: it INSERTs newly-emitted
BOOKINGs (with `source='stream'`) and DELETEs rows on CHECKOUT or
CANCELLATION; for CHECKIN it UPDATEs the row to `state='CHECKED_IN'`.
The generator's `source='history'` rows and the producer's
`source='stream'` rows live in the same table — the `source` column
on each row is the only distinguisher, and downstream queries treat
an open booking the same regardless of source.

The reverse continuity property the producer guarantees: while the
runway lasts (`booking_ts` in `fact_bookings` reaches 2026-05-16),
every sim-day produces some new BOOKINGs — about 1,000 per day on
average — so the stream never goes quiet.

---

## Streaming Configuration

This file is **not loaded into the DWH**. It configures the Kafka simulator.

### Streaming Implementation Note

The original blueprint specified **PyFlink** for the stream-processing layer (Kafka → keyed event-time windows → dual sink to Postgres + S3 Parquet). The actual implementation uses a **pure Python Kafka consumer** (`scripts/stream_consumer.py`, ~500 lines including failure handling) with the same architectural pattern.

**What changed:**

- PyFlink 1.18's DataStream Python API hit serialization issues on Windows during the build (`apache-beam` coder errors, JNI gateway crashes during graceful shutdown). Three iterations of type-system patches did not resolve them.
- For the project's workload (50 events/sec aggregated into hourly windows), the full Flink runtime is over-provisioned. The Python consumer handles this rate comfortably and is much simpler to reason about and debug.

**What stayed the same:**

- Kafka source, keyed-by-city aggregation, event-time windowing with watermark grace, dual sink (Postgres upsert + S3 Parquet archive)
- Output schema (`agg_hourly_city_stats`) is unchanged
- Window semantics: tumbling, configurable size, watermark-driven flush

**What got added during Phase 2 build (beyond original blueprint):**

- **Six-gate failure handling** — JSON parse, missing fields, unknown event_type, unknown city, unparseable timestamp, and late-arrival. Each gate has a dedicated reason code and routes to operational quarantine in S3 (`malformed_events/` for validation failures, `late_events/` for window-closed arrivals). Per-reason counters surface non-zero malformed totals in the consumer's shutdown summary as explicit team triage signals.
- **Late-event guard** — explicit check before in-memory state mutation, preventing the `defaultdict`-resurrection bug where late events would silently corrupt already-flushed aggregates.
- **Three shutdown paths** — SIGINT (interactive), SIGTERM (process supervisor), and `--max-runtime` (CLI-driven bounded run, avoids cross-process SIGINT issues on Windows).
- **Chaos injection in the producer** — `--malformed-pct`, `--late-pct`, `--chaos-seed` CLI flags for stress-testing the consumer's failure paths with reproducible random corruption.

**Trade-offs accepted:**

- At-least-once delivery instead of exactly-once. Acceptable because Postgres `ON CONFLICT DO UPDATE` and deterministic S3 keys make sinks idempotent.
- Single-process, not horizontally scalable. Fine for 50 events/sec; would revisit at 5,000+/sec.
- No automatic checkpointing or recovery. On restart, in-flight windows are lost; already-flushed windows remain durable in Postgres/S3.
- Late-event reconciliation is deferred to Phase 6 (Airflow). Until then, late events sit in quarantine; the live aggregate is eventually-consistent only for events that arrived on time.

**Production deployment** would use AWS Managed Flink with the Table API (SQL-based aggregation, native side-output for late events) plus Amazon MSK for managed Kafka, RDS Postgres + S3 sinks. The Python wrapper issues we hit locally are not present in AWS's curated Linux Flink runtime. The failure-handling architecture stays the same conceptually — Flink's `allowed_lateness + side-output` is exactly the late-event quarantine pattern, just in-process.

---

### `booking_events_seed.json`

**Purpose:** Per-hotel behavior profile for the Kafka event simulator.
**Records:** 500
**Used by:** Python simulator reads this, then generates JSON events into the Kafka `booking-events` topic at realistic rates.

#### Schema (per record)

```json
{
  "hotel_id":                 "HTL-NNNNNN",
  "city":                     "Goa",
  "tier":                     "LUXURY",
  "base_daily_bookings":      41,            // baseline event rate
  "peak_multiplier":          2.8,           // scale during peak months
  "off_season_multiplier":    0.25,          // scale during off-season
  "peak_months":              ["November", "December", "January", "February"],
  "cancellation_rate_base":   0.14,
  "avg_stay_nights":          2.9,
  "booking_sources":          {              // OTA mix probabilities (sum = 1.0)
    "Direct":      0.40,
    "Booking.com": 0.25,
    "MakeMyTrip":  0.20,
    "Agoda":       0.15
  },
  "room_types":               ["Pool View", "Luxury Suite", "Executive"],
  "price_range_inr":          {"min": 10000, "max": 35000}
}
```

#### How the simulator uses it

For each hotel, every "tick" (e.g. every second):
1. Calculate current rate = `base_daily_bookings × (peak_mult if peak_month else off_season_mult)`
2. Generate Poisson-distributed events at that rate
3. For each event: draw an event type from the weighted mix below, then for BOOKING/PRICE_CHANGE fill the type-specific extra fields
4. Publish JSON to Kafka `booking-events` topic

#### Producer model — data-aware forward generator (B-047, current)

> The B-034 stateful in-memory registry producer was superseded by the B-034A calendar replay simulator, which was in turn **superseded by the B-047 forward generator** (this entry). See `docs/phase-2-streaming.md` BUILD HISTORY for the full pivot rationale.

`scripts/kafka_event_producer.py` (B-047) is a **data-aware FORWARD generator** paced by a 24-value IST diurnal rate curve. Each tick (~200 ms) drains the soonest-due lifecycle event from `sim_open_bookings` (CHECKIN / CHECKOUT / CANCELLATION / REVIEW — priority over net-new), then fills the remaining bucket capacity with BOOKING vs PRICE_CHANGE by per-hour weight `prob_b(h) = w_b[h] / (w_b[h] + w_p[h])` — no fixed 95/5 coin flip; the daily aggregate (~48/52 BOOKING/PC at x=1) emerges from the curves. New-BOOKINGs are minted from the live dim tables under a per-hotel-per-night occupancy cap (city × season → hotel weighted by `total_rooms × star` → room type fitting `num_guests` → customer 75% out-of-state → lead-time + nights + price); each row's `checkin_fire_ts` / `checkout_fire_ts` / (optional, 12%) `cancel_fire_ts` are stamped at emit time from per-event-type IST hour distributions. REVIEW emission is deferred: on CHECKOUT or CANCELLATION emit, if the negativity-bias draw produces a review, `review_fire_ts` is set to tonight 20-23 IST (clamped ≥ now+30min) and `state → 'REVIEW_PENDING'` until REVIEW fires. **No sim-clock; `.sim_clock.json` is deleted on first launch and never re-created.** `event_ts` is always wall-clock UTC NOW; `event_date` is today in IST. Catch-up rule: an overdue fire_ts (producer was off) fires at the bucket cap with `event_ts = NOW()`, never backdated. Per-tick batched commits to `sim_open_bookings` + `sim_daily_counter` (~5 commits/sec at x=1, not per-event). On `events_emitted >= cap` (= `1_000_000 × rate_multiplier`) the producer sleeps until IST midnight. Single throughput knob: `--rate-multiplier` (integer > 1; any invalid value silently → 1).

#### Event types emitted

| Event type | Extra fields beyond base envelope | Notes |
|---|---|---|
| `BOOKING` | `booking_id`, `customer_id`, `room_type_id`, `checkin_date`, `checkout_date`, `nights`, `num_guests`, `nightly_rate_inr`, `revenue_inr`, `booking_source`, `event_date` | **INVARIANT:** `revenue_inr == nightly_rate_inr * nights`. `event_date` = today in IST under B-047 (was sim-day under B-034A). |
| `CHECKIN` | `booking_id`, `customer_id`, `event_date` | Traceable to its originating BOOKING. Fires when wall-clock reaches the row's stamped `checkin_fire_ts`. |
| `CHECKOUT` | `booking_id`, `customer_id`, `event_date` | Same `booking_id` as matching CHECKIN. Fires when wall-clock reaches `checkout_fire_ts`. |
| `CANCELLATION` | `booking_id`, `customer_id`, `cancellation_reason` (`customer_cancelled` or `no_show`), `event_date` | Fires when wall-clock reaches `cancel_fire_ts` (12% of bookings; deterministic from `Random(booking_id\|cancel\|daily_seed)`). |
| `PRICE_CHANGE` | `room_type_id`, `old_price_inr`, `new_price_inr` (NO `booking_id`) | Stateless operational event. Passes Gate 2 but silently filtered at Gate 3 (not bronzed). Drawn from all ~20K active hotels (B-047) — no longer from `data/booking_events_seed.json`. |
| `REVIEW` | `review_id` (uuid5 deterministic — `uuid5(REVIEW_NS, booking_id)`), `booking_id`, `customer_id`, `review_stage` (`booked` / `checked_in` / `checked_out` / `cancelled`), `rating` (1–5), `review_text`, `review_channel`, `event_date` | Emitted DEFERRED under B-047 — on CHECKOUT or CANCELLATION the row's `review_fire_ts` is stamped in tonight 20-23 IST and `state → 'REVIEW_PENDING'`; REVIEW fires when wall-clock reaches the stamp; row deleted. No `city` field. `event_ts` stamped by the producer at each emission site. Routed to `reviews_raw` only; never enters `fact_booking_events`, agg, or gold. |

**Base envelope** (BOOKING / CHECKIN / CHECKOUT / CANCELLATION / PRICE_CHANGE): `event_id` (uuid4), `event_type`, `hotel_id`, `city`, `event_ts` (ISO 8601 UTC wall-clock), `event_date` (sim-day). REVIEW omits `city` — `hotel_id` is the routing anchor.

#### Producer & consumer CLI

```bash
# Forward generator (B-047, current). Single throughput knob:
python -m scripts.kafka_event_producer                        # default x=1
python -m scripts.kafka_event_producer --rate-multiplier 3    # scale rate + cap together
python -m scripts.kafka_event_producer --malformed-pct 5 --late-pct 2 --chaos-seed 42

# Consumer
python -m scripts.stream_consumer \
    --max-runtime 420            # seconds, 0 = forever (default)
```

`--rate-multiplier` is **integer-only, strictly > 1**; any invalid value (non-int, ≤ 1, negative, alpha, empty, None) silently falls back to `1`. Same coerce rule on the `RATE_MULTIPLIER` env var. Chaos flags on the producer side: `--malformed-pct N --late-pct N --chaos-seed N` (env-var fallback `CHAOS_MALFORMED_PCT` / `CHAOS_LATE_PCT` / `CHAOS_SEED`).

---

## Data Quality Guarantees

All datasets pass these gates (validated by the generator scripts):

| Gate | Status |
|---|---|
| Zero orphan FKs across all fact-to-dimension joins | ✓ |
| Zero rows violate `ref_price_tiers` price bands | ✓ |
| Zero hotels without at least 1 review | ✓ |
| All reviews ≥ 42 words | ✓ |
| Star distribution within ±2% of spec (35/12/10/20/15/8%) | ✓ |
| Rating distribution within ±3% of spec (8/5/12/30/45%) | ✓ |
| Goa peaks Dec, drops in monsoon (verified by `month`-grouped counts) | ✓ |
| Customer segments drive realistic tier preferences | ✓ |

---

## Loading Order for Postgres

Foreign keys require this load order. Use `\copy` in `psql` (client-side path) or `COPY FROM` with server-side paths:

```sql
-- 1. Reference tables (no FKs) — direct load, no ETL
\copy ref_price_tiers      FROM 'ref_price_tiers.csv'     CSV HEADER;
\copy ref_state_centroids  FROM 'ref_state_centroids.csv' CSV HEADER;
\copy india_states_zones   FROM 'india_states_zones.csv'  CSV HEADER;
\copy public_holidays      FROM 'public_holidays.csv'     CSV HEADER;

-- 2. Independent dimensions (master data)
\copy dim_date             FROM 'dim_date.csv'            CSV HEADER;  -- uses public_holidays for flags
\copy dim_location         FROM 'dim_location.csv'        CSV HEADER;
\copy dim_customer         FROM 'dim_customer.csv'        CSV HEADER;

-- 3. Dimensions with FKs
\copy hotel_master         FROM 'hotel_master.csv'        CSV HEADER;  -- FK: dim_location, ref_price_tiers
\copy dim_room_type        FROM 'dim_room_type.csv'       CSV HEADER;  -- FK: hotel_master

-- 4. Facts (depend on everything above) — historical data
\copy fact_bookings        FROM 'fact_bookings.csv'       CSV HEADER;  -- FK: hotel_master, dim_customer, dim_room_type, dim_location, dim_date
\copy fact_price_events    FROM 'fact_price_events.csv'   CSV HEADER;  -- FK: hotel_master, dim_room_type
\copy reviews_raw          FROM 'reviews_raw.csv'         CSV HEADER;  -- FK: hotel_master

-- booking_events_seed.json → consumed by Kafka simulator, NOT loaded into DWH
-- review_embeddings → populated later in Phase 3 (sentence-transformers + pgvector)
```

**Loading tip:** disable FK constraints during bulk load, then re-enable after:
```sql
SET session_replication_role = 'replica';   -- disable FK checks
-- ... run all \copy commands ...
SET session_replication_role = 'origin';    -- re-enable
```

#### FK validation queries to run after load

```sql
-- Should all return 0
SELECT COUNT(*) FROM fact_bookings b LEFT JOIN hotel_master h USING(hotel_id) WHERE h.hotel_id IS NULL;
SELECT COUNT(*) FROM fact_bookings b LEFT JOIN dim_customer c USING(customer_id) WHERE c.customer_id IS NULL;
SELECT COUNT(*) FROM fact_bookings b LEFT JOIN dim_date d USING(date_id) WHERE d.date_id IS NULL;
SELECT COUNT(*) FROM dim_room_type r LEFT JOIN hotel_master h USING(hotel_id) WHERE h.hotel_id IS NULL;
SELECT COUNT(*) FROM reviews_raw r LEFT JOIN hotel_master h USING(hotel_id) WHERE h.hotel_id IS NULL;
```

#### pgvector setup (Phase 3, when embeddings get added)

```sql
-- One-time, after database creation
CREATE EXTENSION IF NOT EXISTS vector;

-- Add embedding column to reviews_raw (or create separate review_embeddings table)
ALTER TABLE reviews_raw ADD COLUMN embedding vector(384);

-- Index for fast cosine similarity (after embeddings are populated)
CREATE INDEX ON reviews_raw USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
```

#### Sanity check queries

```sql
-- Should return ~1M, dates ~Jan 2024 to May 2026, ~₹2097 cr, ~11.8%
SELECT COUNT(*) AS bookings,
       MIN(checkin_date), MAX(checkin_date),
       SUM(revenue_inr)/1e7 AS revenue_cr,
       AVG(CASE WHEN is_cancelled THEN 1 ELSE 0 END) * 100 AS cancel_pct
FROM fact_bookings;

-- Should show 6 segments with different tier preferences
SELECT customer_segment, price_tier, COUNT(*) AS n
FROM fact_bookings b
JOIN dim_customer c USING(customer_id)
JOIN dim_room_type r USING(room_type_id)
GROUP BY 1, 2 ORDER BY 1, 2;
```

---

## Quick Reference Card

| Need to query... | Best table (after Phase 4) | Fallback (Phase 1) |
|---|---|---|
| Revenue by city, this hour | `agg_hourly_city_stats` | `fact_bookings` + `dim_location` |
| Hotel daily performance | `agg_daily_hotel_kpi` | `fact_bookings` + `hotel_master` + `dim_date` |
| Zone seasonality (monthly) | `agg_monthly_zone_summary` | `fact_bookings` + `dim_location` + `dim_date` |
| Customer LTV / top spenders | `customer_lifetime_value` | `fact_bookings` + `dim_customer` |
| Hotel review sentiment | `hotel_sentiment_scores` | `reviews_raw` (with embeddings) |
| Revenue by season | (any aggregate) + `dim_date` | `fact_bookings` + `dim_date` |
| Room type preferences | `fact_bookings` | (no Gold table — query Silver directly) |
| Price surge patterns | `fact_price_events` | (no Gold table — query Silver directly) |
| Guest complaints (text) | `reviews_raw` (semantic search) | (Gold has aggregates only) |
| Live occupancy "right now" | `agg_hourly_city_stats` | (Phase 1 has no real-time path) |
| Geo flow maps | `fact_bookings` + `dim_customer` → `ref_state_centroids` + `dim_location` | (same) |

**Rule of thumb:** if a Gold table exists for your query, use it. It's 10-100x faster than aggregating raw facts on every request. The fallback column shows what you'd join in Phase 1 before Gold tables exist.

---

## Schema Evolution

Numbering starts at **003** because 001 and 002 were folded into the
initial `db/schema.sql` (the frozen 14-table star schema). From 003
onward, every structural change ships as a numbered, append-only file
in `db/migrations/` — that directory plus `db/schema.sql` together are
the source of truth.

| #   | What it changed (plain language)                                                                       | Phase / feature                  | Backlog ID |
|-----|--------------------------------------------------------------------------------------------------------|----------------------------------|------------|
| 003 | `dashboard_widgets` table — pinned widgets + prompts + refresh schedule                                | Phase 5 — Dashboard              | (B-008)    |
| 004 | `dashboard_widgets.width` column — Normal / Full layout                                                | Phase 5 — Dashboard polish       | (B-012)    |
| 005 | `dashboard_widgets.generated_sql` + `last_result_json` — frozen SQL + JSONB result cache               | Phase 5 — Dashboard read-path    | B-022      |
| 006 | `hotel_master.opened_year` column — fixes L-010 entity-count confusion                                 | Phase 4 — SQL prompt fix         | (L-010)    |
| 007 | `pipeline_metrics` heartbeat table + 4 new count columns on `agg_hourly_city_stats` (checkins/checkouts/cancellations/reviews) | Phase 7 / Phase 2 hardening      | B-032      |
| 008 | Lifecycle event tables (`fact_booking_events` + `sim_open_bookings`) for the streaming redesign        | Phase 2 — Streaming lifecycle    | B-035      |
| 009 | Gold lifecycle layer: `ingest_seq` cursor column on `fact_booking_events` + `fact_booking_lifecycle` gold table (one row per booking_id, forward-only status machine, `illegal_transition_flag`, `source_mix`) + `gold_watermark` single-row cursor table | Phase 2 — Gold layer | B-040      |
| 010 | 7 new nullable columns on `reviews_raw` for booking-tied stream reviews (`booking_id UUID`, `customer_id VARCHAR(12)`, `review_stage VARCHAR(20)`, `review_channel VARCHAR(100)`, `event_ts TIMESTAMPTZ`, `event_date DATE`) + `record_source VARCHAR(10) NOT NULL DEFAULT 'seed'`; 2 new indexes (`idx_reviews_raw_booking_id` partial, `idx_reviews_raw_record_source`) | Phase 2 — REVIEW stream | B-030      |
| 011 | `idx_reviews_raw_event_date` index on `reviews_raw.event_date` — supports the date-range scoping in `_monitor_reviews` and `_monitor_embeddings` | Phase 7 — Monitor date scoping   | B-030b     |
| 012 | `quarantine_daily_summary` table (`summary_date DATE PK`, `malformed_count INT`, `late_count INT`, `computed_at TIMESTAMPTZ`) — **RETIRED: dropped by migration 013** | Phase 6 / Phase 7 — Quarantine   | B-033 *(superseded by B-044)* |
| 013 | Drops `quarantine_daily_summary`; creates `quarantine_hourly_summary` (`summary_date DATE`, `summary_hour SMALLINT`, `malformed_count INT`, `late_count INT`, `is_final BOOL`, `computed_at TIMESTAMPTZ`, PK `(summary_date, summary_hour)`) | Phase 6 / Phase 7 — Quarantine   | B-044      |
| 014 | `sim_daily_counter` table (`counter_date DATE PK`, `events_emitted INTEGER NOT NULL DEFAULT 0`, `cap INTEGER NOT NULL`, `updated_at TIMESTAMPTZ`). Per-day TOTAL-EVENTS guardrail for the forward generator; `counter_date` is in IST; `cap = 1_000_000 × rate_multiplier` at row creation (not recomputed by later sessions of the same day). On cap-hit the producer sleeps until IST midnight. | Phase 2 — Forward generator      | B-047      |
| 015 | 4 nullable `TIMESTAMPTZ` cols on `sim_open_bookings` (`checkin_fire_ts`, `checkout_fire_ts`, `cancel_fire_ts`, `review_fire_ts`); 4 partial indexes narrowed by `state` and IS NOT NULL; **state CHECK constraint extended** to allow `'REVIEW_PENDING'` (was `IN ('BOOKED','CHECKED_IN')`, now `IN ('BOOKED','CHECKED_IN','REVIEW_PENDING')`). Fire-time stamps are sampled in IST from per-event-type hour distributions at BOOKING emit time and persisted on the row so a restart picks up exactly where it left off. Catch-up rule: overdue stamps fire at `event_ts = NOW()`, never backdated. | Phase 2 — Forward generator      | B-047      |
| 016 | COMMENT-only refresh on `sim_daily_counter.cap` documenting the producer's cap-upsert flip from `ON CONFLICT DO NOTHING` (sticky cap) → `ON CONFLICT DO UPDATE SET cap = EXCLUDED.cap` (last-write-wins across same-day sessions). Behavioural change lives in `scripts/kafka_event_producer.py:ensure_daily_counter_row`; this migration is the ledger entry. `events_emitted` is intentionally not in the SET clause and continues to accumulate. | Phase 2 — Forward generator      | B-047 (follow-on) |
| 017 | 2 new nullable columns on `reviews_raw` for per-review sentiment (`sentiment_label VARCHAR(8)` — positive/negative/neutral, NULL = unscored; `sentiment_score NUMERIC(4,3)` — model confidence) + partial index `idx_reviews_raw_sentiment` (WHERE sentiment_label IS NOT NULL). Populated by `scripts/review_sentiment_scorer.py` (CardiffNLP, pinned safetensors revision). Independent of `embedding` — additive, no re-embed. | Phase 3 — Review sentiment       | B-026 (Stage 1) |

**Note — B-046 added no migration.** Stage 1 dimension expansion (949 cities · 18,076 hotels · 49,904 room types · 80,000 customers) uses existing columns only. The `type_name` field on `dim_room_type` and the `property_type` field on `hotel_master` are both free-text VARCHAR with no CHECK constraint, so the three new room-type values (`Houseboat Suite`, `Tent`, `Treehouse`) and the previously-unused property-types (`Houseboat`, `Treehouse`) inserted cleanly. `opened_year` (migration 006) carries no CHECK constraint either, so extending the data range to 2026 needed no schema change.

---

## Stage 1 Dimension Expansion (B-046)

> **Trace.** Issue: [backlog.md → B-046](docs/backlog.md#b-046--stage-1-dimension-expansion-1k-cities--20k-hotels--100k-customers-additive--done) · Phase(s): [phase-1-postgres.md → B-046 build history](docs/phase-1-postgres.md#b-046--stage-1-dimension-expansion-2026-05-27) · data: `scripts/expand_dimensions.py`, `scripts/build_cities_expansion_csv.py`, `seeds/cities_expansion.csv`. See also [Regenerating the Dataset → Stage B](#stage-b--additive-expansion-b-046-run-once-after-stage-a) for the run order.

Run date: **2026-05-27**. ADDITIVE only — no existing row was modified.

| Table | Before | After | Delta |
|---|---:|---:|---:|
| `dim_location` | 44 | 993 | +949 |
| `hotel_master` | 2,000 | 20,076 | +18,076 |
| `dim_room_type` | 5,542 | 55,446 | +49,904 |
| `dim_customer` | 20,000 | 100,000 | +80,000 |
| `fact_bookings` | 1,000,000 | 1,000,000 | 0 |
| `fact_booking_events` | 2,107,759 | 2,107,759 | 0 |
| `fact_booking_lifecycle` | 762,230 | 762,230 | 0 |
| `reviews_raw` | 133,463 | 133,463 | 0 |

**State coverage:** the 44 base cities spanned 16 states; the expansion adds presence across the full 34 inhabited states + UTs that have any catalog entry. Total distinct states in `dim_location` after expansion: 35. No new state values introduced — every catalog row's `state` matches an existing `india_states_zones.state_name`.

**Zone targets vs actuals (incl. existing 44):**

| Zone | Final | Target |
|---|---:|---:|
| Metro | 181 | ~180 |
| Pilgrimage | 181 | ~180 |
| Hill Station | 170 | ~180 |
| Wildlife | 169 | ~170 |
| Heritage | 159 | ~160 |
| Beach | 85 | ~80 |
| Backwater | 48 | ~50 |

**Hotels-per-city rule** — bucket-uniform sampling by `popularity_tier`:

| popularity_tier | Hotels-per-city range |
|---|---:|
| mega | 120–150 |
| major | 40–80 |
| mid | 20–40 |
| small | 8–15 |
| obscure | 3–8 |

**Property-type distribution after expansion (top 10):** Hotel 7,701 · Guest House 2,252 · Resort 2,038 · Lodge 1,924 · Homestay 1,679 · Apartment 824 · BnB 725 · Tent 688 · Hostel 537 · Service Apartment 418. New non-zero property-types relative to the 2,000-hotel baseline: `Tent` (688), `Treehouse` (186), `Houseboat` (205) — present in pre-expansion `PROPERTY_TYPES` list but rare or absent in actual rows; now meaningfully populated.

**Zone-property gating verified:**

| Check | Hotels |
|---|---:|
| Houseboat in Backwater | 175 |
| Treehouse in Wildlife | 186 |
| Tent in Wildlife | 409 |
| Tent in Hill Station | 266 |

No cross-zone leakage — the expansion script gates `Houseboat`/`Treehouse` strictly by zone.

**Idempotency.** `scripts/expand_dimensions.py` aborts if (a) any catalog city already exists in `dim_location` by `(city, state)`, (b) `MAX(hotel_id)` is past `HTL-002000`, or (c) `MAX(customer_id)` is past `CUST-020000`. A second run is a hard no-op (exits non-zero with a clear message).

**Files committed:**

| Path | Purpose |
|---|---|
| `seeds/cities_expansion.csv` | 949 curated city rows (city, state, region, tourism_zone, lat, lng, tourist_arrivals_annual_m, peak_months, popularity_tier) |
| `scripts/build_cities_expansion_csv.py` | Deterministic CSV builder (SEED=42; catalog inline). Regenerates `seeds/cities_expansion.csv` byte-identical on every run. |
| `scripts/expand_dimensions.py` | Reads the CSV, generates hotels/room-types/customers, INSERTs all four tables in a single transaction with pre/post integrity checks. |

---

## Gold Lifecycle Layer (migration 009, B-040)

The gold layer materialises one row per `booking_id` from the silver event ledger (`fact_booking_events`). A separate ~1-min micro-batch process (`scripts/gold_lifecycle_updater.py`) — decoupled from the consumer so gold latency never back-pressures the silver accept path — reads silver events in `ingest_seq` order, applies a forward-only status machine, and upserts to the gold table.

### `fact_booking_lifecycle` — one row per booking

| Column | Type | Notes |
|---|---|---|
| `booking_id` | UUID PK | One row per booking |
| `customer_id` | VARCHAR(20) | From the BOOKING event |
| `hotel_id` | VARCHAR(20) | From the BOOKING event |
| `room_type_id` | UUID | From the BOOKING event |
| `city` | VARCHAR(50) | From the BOOKING event |
| `booking_ts` | TIMESTAMPTZ | event_ts of the BOOKING event |
| `booking_date` | DATE | event_date (sim-day) of BOOKING |
| `checkin_ts` | TIMESTAMPTZ | event_ts of the CHECKIN event |
| `checkin_date` | DATE | event_date of CHECKIN |
| `checkout_ts` | TIMESTAMPTZ | event_ts of the CHECKOUT event |
| `checkout_date` | DATE | event_date of CHECKOUT |
| `cancellation_ts` | TIMESTAMPTZ | event_ts of the CANCELLATION event |
| `cancellation_date` | DATE | event_date of CANCELLATION |
| `nights` | SMALLINT | From BOOKING |
| `num_guests` | SMALLINT | From BOOKING |
| `nightly_rate_inr` | NUMERIC(10,2) | From BOOKING |
| `revenue_inr` | NUMERIC(10,2) | From BOOKING |
| `booking_source` | VARCHAR(50) | From BOOKING |
| `payment_mode` | VARCHAR(20) | From BOOKING |
| `cancellation_reason` | VARCHAR(40) | From CANCELLATION |
| `current_status` | VARCHAR(20) NOT NULL | Forward-only: BOOKED → CHECKED_IN → COMPLETED / CANCELLED |
| `outcome` | VARCHAR(15) | in_progress / completed / cancelled / no_show |
| `illegal_transition_flag` | BOOLEAN NOT NULL | TRUE when a genuine business-domain timestamp inversion is detected (e.g. checkout.event_ts < checkin.event_ts). NOT triggered by processing-order artifacts from heap scan or bulk INSERT ordering. |
| `source_mix` | VARCHAR(10) | history / stream / mixed — which source(s) contributed events |
| `event_count` | INT NOT NULL | Total silver events applied to this row |
| `first_event_ts` / `last_event_ts` | TIMESTAMPTZ | Timestamp range of all applied events |
| `last_updated` | TIMESTAMPTZ NOT NULL | Timestamp of last gold upsert |

**Status machine (forward-only — never regresses):**
- `BOOKED` (0) → `CHECKED_IN` (1) → `COMPLETED` (2) on CHECKOUT
- `BOOKED` (0) → `CANCELLED` (2) on CANCELLATION
- `CHECKED_IN` (1) → `CANCELLED` (2) on CANCELLATION

**Excluded from gold:** `PRICE_CHANGE` (no booking_id) and `REVIEW` (hotel-level, not per-booking lifecycle).

**`illegal_transition_flag` semantics:** fires on BUSINESS-DOMAIN inversions detected from `event_ts` (business timestamp in history data, wall-clock in stream data):
- CHECKOUT: fires if `checkout.event_ts < checkin.event_ts`, OR if a CANCELLATION was already recorded (mutually exclusive outcomes)
- CHECKIN: fires if `checkin.event_ts < booking.event_ts`
- CANCELLATION: fires if a CHECKOUT was already recorded

Processing-order artifacts (e.g. CHECKOUT getting a lower `ingest_seq` than BOOKING because the history generator bulk-inserted event types in separate passes) do NOT set this flag.

### `gold_watermark` — single-row cursor

| Column | Type | Notes |
|---|---|---|
| `id` | INT PK | Always 1 (single-row constraint) |
| `last_ingest_seq` | BIGINT NOT NULL | Last `ingest_seq` committed to gold; 0 on fresh table |
| `updated_at` | TIMESTAMPTZ NOT NULL | Timestamp of last watermark advance |

The updater reads `WHERE ingest_seq > last_ingest_seq`, processes a batch, upserts gold, then advances the watermark — all in one atomic commit. On restart it resumes from the committed watermark.

### `ingest_seq` cursor on `fact_booking_events`

Added by migration 009: `BIGINT GENERATED BY DEFAULT AS IDENTITY`. Existing rows were auto-filled by PostgreSQL in heap scan order (not `ingested_at` order — the intended backfill UPDATE was a no-op because PostgreSQL already auto-assigned values on `ADD COLUMN`). New stream inserts auto-increment from the sequence. An index on `ingest_seq` makes the `WHERE ingest_seq > watermark ORDER BY ingest_seq LIMIT batch` query efficient.

### Gold updater operational notes

- **One instance at a time** — enforced by Postgres advisory lock `pg_try_advisory_lock(7400040)` at startup; a second instance exits code 1 immediately. See kill command in CLAUDE.md Common Mistakes.
- **Configurable via env:** `GOLD_INTERVAL_SECONDS` (default 60), `GOLD_BATCH_SIZE` (default 5000).
- **Deadlock resilience:** on `DeadlockDetected`, the updater rolls back the current batch, logs the error, sleeps 10s, and retries from the committed watermark.
- **Run via:** `python -m scripts.gold_lifecycle_updater`

---

**End of data model documentation.**
