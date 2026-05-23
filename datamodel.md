# TravelLens India — Data Model

**Version:** 1.0
**Generated:** 2026-05-16
**Coverage:** Jan 2024 → May 2026 (28 months)
**Total rows:** ~1.16M across 13 files (~248 MB on disk)

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
   - [agg_daily_hotel_kpi](#agg_daily_hotel_kpi)
   - [agg_monthly_zone_summary](#agg_monthly_zone_summary)
   - [customer_lifetime_value](#customer_lifetime_value)
   - [review_embeddings](#review_embeddings)
   - [hotel_sentiment_scores](#hotel_sentiment_scores)
10. [Streaming Configuration](#streaming-configuration)
    - [booking_events_seed.json](#booking_events_seedjson)
11. [Data Quality Guarantees](#data-quality-guarantees)
12. [Loading Order for Postgres](#loading-order-for-postgres)

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
- **Need to regenerate or scale?** Run the three generator scripts in order:

  ```bash
  python generate_datasets.py    # Stage 1 — base schema (~30s)
  python generate_stage2.py      # Stage 2 — facts, customers, dates (~90s)
  python generate_stage3.py      # Stage 3 — customer behavior correlation (~60s)
  ```

  All scripts are deterministic (`SEED = 42`). Edit constants at the top of each to scale rows or change date ranges.

---

## Regenerating the Dataset

All CSVs in this folder are produced by three deterministic generator scripts in `scripts/`. Re-running them produces byte-identical output (seed=42) — so you can safely regenerate at any time to scale rows, extend the date range, or recover from accidental edits.

### Stage 1 — Foundation (`scripts/generate_datasets.py`)

Builds the entities that everything else references. No dependencies on other stages.

| Output | Rows | Description |
|---|---:|---|
| `hotel_master.csv` | 2,000 | Hotels with name, city, chain, star, total_rooms, amenities (JSON), price_tier_id |
| `dim_location.csv` | 44 | City → state → region → tourism zone mapping with lat/lng |
| `dim_room_type.csv` | ~5,500 | Room types per hotel, with capacity and category |
| `reviews_raw.csv` | 30,000 | Reviews seed (Stage 2 regenerates with quality fixes) |
| `booking_events_seed.json` | 500 | Per-hotel behavior profile for the Kafka simulator |
| `public_holidays.csv` | ~120 | Indian public holidays 2024–2027 |
| `india_states_zones.csv` | 37 | State-level tourism zones and arrival statistics |

Runtime: ~30s. Outputs land in `data/` (configurable via `OUT_DIR` at the top of the script).

### Stage 2 — Facts and dependent dims (`scripts/generate_stage2.py`)

Reads Stage 1 outputs and adds the high-volume fact tables plus the calendar dim.

| Output | Rows | Description |
|---|---:|---|
| `fact_bookings.csv` | 1,000,000 | Bookings Jan 2024 → May 2026, with seasonality and OTA mix |
| `fact_price_events.csv` | ~86,000 | Price change history per hotel + room_type |
| `dim_date.csv` | 2,557 | Date spine 2020–2026 with season, holiday flags, fiscal quarter |
| `dim_customer.csv` | 20,000 | Customers with segments (Business/Leisure/Backpacker/Honeymoon/Family/Religious) |
| `ref_price_tiers.csv` | 4 | Budget/Mid/Premium/Luxury INR bands |

Also regenerates `reviews_raw.csv` with quality fixes: power-law hotel distribution (so a few hotels have many reviews and most have few), seasonal date weighting, amenity ↔ review-content correlation, and verbose-reviewer behavior.

Runtime: ~90s. **Must run AFTER Stage 1.**

### Stage 3 — Customer behavior correlation (`scripts/generate_stage3.py`)

Re-regenerates `fact_bookings.csv` so customer segments actually drive realistic behavior:

- Business customers → mid/premium rooms, 1-2 nights, weekday-skewed, higher cancellation
- Honeymoon customers → luxury rooms, 5-7 nights, beach/heritage destinations, low cancellation
- Backpackers → budget rooms, 2-3 nights, hill stations
- Religious customers → mid rooms, Varanasi/Rishikesh/Pushkar
- Family/Leisure follow their own distributions

Also produces `ref_state_centroids.csv` (38 rows) for state-level lat/lng used in geo-flow visualizations.

Runtime: ~60s. **Must run AFTER Stage 2** (overwrites Stage 2's `fact_bookings.csv`).

### Why three stages and not one script

Each stage is independently re-runnable and the dependencies are linear. If you decide later to tune customer behavior without rebuilding hotels and dates, you re-run Stage 3 alone — Stage 1 and 2 outputs stay untouched. If you change the date range, Stage 1 stays valid but Stage 2 and 3 need a re-run. This decomposition was added incrementally during data design as quality issues were discovered; the staged structure preserves the option to fix individual concerns without rebuilding the whole dataset.

### Tuning knobs

Each script has a CONFIG block at the top. Common changes:

| Knob | Where | Effect |
|---|---|---|
| `NUM_HOTELS` | Stage 1 | More/fewer hotels (cascades to bookings, reviews) |
| `NUM_REVIEWS` | Stage 1 | More/fewer reviews |
| `NUM_BOOKINGS` | Stage 2 | Scale fact_bookings up or down |
| `NUM_CUSTOMERS` | Stage 2 | More/fewer unique customers |
| `HISTORY_START` / `HISTORY_END` | Stage 2 + 3 | Extend or shrink the booking date range |
| `SEED` | All three | Change RNG seed for a different dataset shape |

`SEED = 42` everywhere for reproducibility. Keep them aligned across stages or referential integrity breaks.

### Full regeneration workflow

```bash
cd C:\Users\risha\Desktop\Code\repo\travellens
.venv\Scripts\activate

# Optional: clear existing data
del data\*.csv data\*.json

# Run all three in order
python scripts/generate_datasets.py    # ~30s — Stage 1: foundation
python scripts/generate_stage2.py      # ~90s — Stage 2: facts + dims
python scripts/generate_stage3.py      # ~60s — Stage 3: customer correlation

# Validate referential integrity
python scripts/validate_load.py        # Catches FK violations before Postgres load

# Load into Postgres
python scripts/load_to_postgres.py     # See "Loading Order for Postgres" below
```

Total time: ~3 minutes for the full 1M-booking dataset.

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

### Pattern 2: Slowly-changing dimensions (SCD)

**These describe entities that change in the real world.** Production systems use SCD Type 1 (overwrite) or Type 2 (versioned rows) depending on whether historical state matters.

| File | What changes | SCD strategy for production |
|---|---|---|
| `hotel_master.csv` | New hotels, star re-categorization, ownership/chain changes | Type 1 for `chain_name`, `total_rooms`. Type 2 if you need historical context for old bookings. |
| `dim_room_type.csv` | Hotels add/remove room types | Type 1 — old `room_type_id` references in `fact_bookings` keep working |
| `dim_customer.csv` | Loyalty tier upgrades, home state changes | Type 2 recommended — "what tier was this customer WHEN they booked?" matters for analytics |
| `dim_location.csv` | New cities added to platform coverage | Append-only, no edits to existing rows |
| `dim_date.csv` | Extend by another year | Append rows. Never edit existing. |

**For this dataset:** All dimensions are currently SCD Type 1 (latest value only, no history tracking). If you need Type 2 later, you'd add `effective_from` / `effective_to` / `is_current` columns.

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
│  (44 cities)     │ FK    │   (2,000 hotels)   │      │  (5,542 rooms)   │
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
│  (20,000 cust.)  │           │  (2,557 days)      │     │   (86,650)       │
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
**Rows:** 44 cities across 17 states
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

---

### `dim_customer.csv`

**Purpose:** Customer master with segmentation and demographics.
**Rows:** 20,000
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
**Rows:** 2,000 hotels
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
**Rows:** 5,542
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
- **Hill stations** → Valley View, Cottage Room
- **Heritage cities** → Heritage Room
- **Pilgrimage cities** → standard mix (no special types)

You won't find Sea View at a Jaipur hotel or Valley View at Mumbai.

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
| `ingestion_ts` | TIMESTAMP | When this row was written. `DEFAULT CURRENT_TIMESTAMP` |

> **Schema notes:** The column is named `avg_occupancy_rate` for historical reasons — it actually holds a percentage value (0–100), not a rate (0–1). The other metric `cancellation_rate` is a true rate. Earlier design drafts proposed extra columns (`total_checkins`, `total_cancellations`, `avg_nightly_rate_inr`, `late_event_count`) — these were dropped in implementation because they were either trivially derivable (checkins ≈ bookings on a small lag) or not yet needed by any dashboard. They can be added via `ALTER TABLE` if a future query needs them.

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

**Purpose:** Macro view of tourism zone performance by month.
**Produced by:** Airflow DAG `monthly_zone_summary_dag.py`
**Update cadence:** Monthly on day 1 for previous month
**Lookback:** Full project history

#### Schema

| Column | Type | Notes |
|---|---|---|
| `year` | INT | **PK part 1** |
| `month` | INT | **PK part 2** |
| `tourism_zone` | VARCHAR(20) | **PK part 3.** From `dim_location.tourism_zone` |
| `total_bookings` | INT | Confirmed bookings in this zone-month |
| `total_revenue_inr` | BIGINT | Aggregated revenue |
| `avg_booking_value_inr` | INT | Mean revenue per booking |
| `avg_nights_per_booking` | DECIMAL(4,2) | Mean stay duration |
| `cancellation_rate_pct` | DECIMAL(5,2) | Zone-level cancellation rate |
| `top_city` | VARCHAR(50) | City contributing most revenue |
| `top_source` | VARCHAR(20) | Top booking channel |

#### Use cases

- Seasonality charts on the dashboard
- *"Compare Heritage vs Hill Station revenue in Q4"*
- Year-over-year growth visualizations

---

### `customer_lifetime_value`

**Purpose:** Per-customer aggregate metrics for segment analysis.
**Produced by:** Airflow DAG `clv_weekly_dag.py`
**Update cadence:** Weekly on Sunday for full customer base
**Lookback:** Computed from inception to current date

#### Schema

| Column | Type | Notes |
|---|---|---|
| `customer_id` | VARCHAR(12) | **PK** |
| `first_booking_date` | DATE | Earliest confirmed booking |
| `last_booking_date` | DATE | Most recent confirmed booking |
| `total_bookings` | INT | Lifetime confirmed bookings |
| `total_cancellations` | INT | Lifetime cancellations |
| `total_revenue_inr` | BIGINT | Lifetime revenue contribution |
| `avg_booking_value_inr` | INT | Mean revenue per booking |
| `total_nights_stayed` | INT | Lifetime nights |
| `favorite_zone` | VARCHAR(20) | Most-visited tourism zone |
| `favorite_city` | VARCHAR(50) | Most-visited city |
| `top_booking_source` | VARCHAR(20) | Preferred OTA |
| `cancellation_rate_pct` | DECIMAL(5,2) | Cancellation rate for this customer |
| `tenure_days` | INT | Days between first and last booking |
| `clv_tier` | VARCHAR(10) | Derived: PLATINUM / GOLD / SILVER / BRONZE (binned by revenue) |

#### Use cases

- *"Which customer segments are highest-value?"*
- *"Identify Backpackers who actually upgrade to Premium over time"*
- *"Cancellation-prone Business customers — should we change deposit policy?"*

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

**Two valid storage options:**
1. **Separate table** as above — clean separation; flexible if you swap models
2. **Column on `reviews_raw`** — simpler; one less join

For TravelLens, option 2 (column added to `reviews_raw` via Phase 3 migration) is fine. The standalone-table form shown here is the production-flexible version.

#### Index

```sql
CREATE INDEX ON review_embeddings
USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
```

#### Use cases

- *"Find reviews similar to this one"* — exact cosine lookup
- *"Cluster reviews by theme"* — k-means on embeddings
- Powers `hotel_sentiment_scores` aggregation downstream

---

### `hotel_sentiment_scores`

**Purpose:** Aggregated review sentiment per hotel, derived from embeddings.
**Produced by:** Airflow DAG `sentiment_weekly_dag.py` (Phase 3 of blueprint)
**Update cadence:** Weekly, recomputed against full review corpus
**Lookback:** Full review history

#### Schema

| Column | Type | Notes |
|---|---|---|
| `hotel_id` | VARCHAR(10) | **PK** |
| `total_reviews` | INT | Count used in aggregation |
| `avg_sentiment_score` | DECIMAL(4,3) | -1.000 (very negative) to 1.000 (very positive) |
| `cleanliness_score` | DECIMAL(4,3) | Theme-specific sentiment (from clustered embeddings) |
| `staff_score` | DECIMAL(4,3) | Theme-specific |
| `location_score` | DECIMAL(4,3) | Theme-specific |
| `food_score` | DECIMAL(4,3) | Theme-specific (NULL if hotel has no restaurant) |
| `value_score` | DECIMAL(4,3) | Theme-specific |
| `pct_off_topic_reviews` | DECIMAL(5,2) | Share of reviews flagged irrelevant |
| `computed_at` | TIMESTAMP | When this row was last refreshed |

#### Use cases

- *"Hotels with high cleanliness complaints but high ratings overall"* (find disconnect)
- *"Rank Goa beach hotels by food sentiment"*
- *"Surface hotels with declining sentiment over time"* (compare snapshots)

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
3. For each event: pick room type, pick OTA source by weights, generate price within range
4. Publish JSON to Kafka `booking-events` topic

#### Producer & consumer CLI

The producer (`scripts/kafka_event_producer.py`) and consumer (`scripts/stream_consumer.py`) both take CLI flags:

```bash
# Producer
python scripts/kafka_event_producer.py \
    --rate 50                    # events per second
    --duration 300               # seconds, 0 = forever
    --malformed-pct 1            # % corrupt events (real-world: 0.5-1)
    --late-pct 2                 # % delayed events (real-world: 1-3)
    --chaos-seed 42              # reproducible chaos

# Consumer
python scripts/stream_consumer.py \
    --max-runtime 420            # seconds, 0 = forever (default)
```

Default behavior with no flags: producer runs forever at 50 evt/s with no chaos; consumer runs forever. CLI flags shape each run. Env-var fallback exists for orchestrator use (Docker, CI): `CHAOS_MALFORMED_PCT`, `CHAOS_LATE_PCT`, `CHAOS_SEED`.

For realistic real-world chaos rates (mimicking production data quality), use `--malformed-pct 1 --late-pct 2`. For aggressive correctness testing, use `--malformed-pct 5 --late-pct 2 --chaos-seed 42`.

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

**End of data model documentation.**
