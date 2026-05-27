# Phase 1 — Postgres Foundation

> **Stack:** Python 3.11 · Postgres 16 + pgvector · Docker  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Scripts:** `scripts/load_to_postgres.py` · `scripts/validate_load.py`  
> **Status:** [ ] In progress / [x] Complete  

> **HISTORY DOCUMENT** — This records how Phase 1 was originally built. For the current schema source of truth, see [CLAUDE.md](../CLAUDE.md) · [datamodel.md](../datamodel.md) · [backlog.md](backlog.md).

---


## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Files this phase
creates / touches:

- **CREATE** `docker/postgres.Dockerfile`
- **CREATE** `docker/docker-compose.yml` (Postgres only — Phase 2 extends it)
- **CREATE** `db/schema.sql` (14 tables, extension, no inline indexes)
- **CREATE** `scripts/load_to_postgres.py`
- **CREATE** `scripts/validate_load.py`

## OBJECTIVE

Stand up a Postgres 16 database in Docker with the pgvector extension, create the
14-table TravelLens schema, and bulk-load all 13 source CSVs from `data/` into the
right tables with foreign-key integrity verified.

---

## PREREQUISITES

- [ ] Phase 0 accepted (`PHASE 0 ACCEPTED` printed, all 8 checks pass)
- [ ] `docker --version` returns Docker 24.x or newer
- [ ] `docker info` shows the daemon is running
- [ ] Python 3.11+ with `.venv` active: `python -c "import psycopg2"` succeeds
- [ ] `data/` directory contains all 13 source files:
  - `ref_price_tiers.csv`, `ref_state_centroids.csv`, `india_states_zones.csv`, `public_holidays.csv`
  - `dim_date.csv`, `dim_location.csv`, `dim_customer.csv`, `hotel_master.csv`, `dim_room_type.csv`
  - `fact_bookings.csv` (1M rows), `fact_price_events.csv` (~86K), `reviews_raw.csv` (30K)
  - `booking_events_seed.json` (used in Phase 2, not Phase 1)
- [ ] `.env` exists at repo root with: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`,
  `POSTGRES_USER`, `POSTGRES_PASSWORD`, `DATA_DIR`

If any prerequisite fails, stop. Do not improvise.

---

## DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| `postgres.Dockerfile` | `docker/` | Builds Postgres 16 + pgvector |
| `docker-compose.yml` | `docker/` | Postgres-only (Phase 2 extends it) |
| `schema.sql` | `db/` | 14 tables + extension created |
| `load_to_postgres.py` | `scripts/` | All 13 CSVs loaded, FK integrity verified |
| `validate_load.py` | `scripts/` | 20/20 checks pass |

Do not create files outside this list. Do not modify `data/` or `.env`.

---

## ARCHITECTURE DECISIONS (ORIGINAL)

### Why Postgres + pgvector, not DuckDB

DuckDB has a single-writer lock — concurrent Flink streaming writes, Airflow batch jobs,
and live queries cause contention. Postgres is multi-writer. pgvector adds native
`vector(384)` column support and cosine similarity operators in the same DB — no second
store to manage, no join problem between fact tables and embeddings.

### Why disable FKs during bulk load

Postgres checks every FK on every row inserted. At 1M fact rows that's 3–5× slower.
Pattern: define FKs in schema (safety net for future data), disable with
`SET session_replication_role = replica` during bulk load, re-enable after. If re-enable
fails, an orphan exists — rollback catches it loudly.

### Why `COPY FROM STDIN`, not INSERT

`COPY` is Postgres's bulk-load path — bypasses row-by-row overhead. At 1M rows the
difference is ~60 seconds vs ~15 minutes.

### Why defer index creation to post-load

Postgres maintains indexes on every INSERT. Deferring creation until after all rows are
loaded gives ~5× faster loading. Indexes are created once at the end of
`load_to_postgres.py`.

---

## STEPS

### Step 1 — Write `docker/postgres.Dockerfile`

```dockerfile
FROM postgres:16

RUN apt-get update && \
    apt-get install -y postgresql-16-pgvector && \
    rm -rf /var/lib/apt/lists/*

ENV POSTGRES_INITDB_ARGS="--encoding=UTF-8 --locale=C.UTF-8"
```

---

### Step 2 — Write `docker/docker-compose.yml`

```yaml
version: '3.9'

services:
  postgres:
    build:
      context: .
      dockerfile: postgres.Dockerfile
    container_name: travellens-postgres
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ../data:/data:ro
      - ../db:/db:ro
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "${POSTGRES_USER}", "-d", "${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

---

### Step 3 — Start the container

```bash
cd docker
docker compose --env-file ../.env up -d --build
sleep 10
docker compose --env-file ../.env ps
```

Expected: `travellens-postgres` shows status `Up (healthy)`. If `unhealthy` or
`Restarting`, run `docker compose logs postgres` and stop — do not continue.

---

### Step 4 — Enable pgvector

```bash
docker exec travellens-postgres psql -U travellens -d travellens \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

Verify:
```bash
docker exec travellens-postgres psql -U travellens -d travellens -c "\dx vector"
```

Must show the `vector` extension. If missing, the Dockerfile build failed — rebuild
with `--force-recreate`.

---

### Step 5 — Write `db/schema.sql`

Table creation order matters for FK constraints — follow exactly:

1. Extensions: `CREATE EXTENSION IF NOT EXISTS vector;`
2. Reference (no FKs): `ref_price_tiers`, `ref_state_centroids`, `india_states_zones`, `public_holidays`
3. Dimensions: `dim_date`, `dim_location`, `dim_customer`, `hotel_master` (FK → `dim_location`), `dim_room_type` (FK → `hotel_master`, `ref_price_tiers`)
4. Facts: `fact_bookings`, `fact_price_events`, `reviews_raw` (with `embedding vector(384)`)
5. Aggregates: `agg_hourly_city_stats`, `agg_daily_hotel_kpi`
6. **Do NOT create indexes inside schema.sql** — they go in `load_to_postgres.py` post-load

Critical column specifics:
- `reviews_raw.embedding` is `vector(384)` — exactly this type
- `hotel_master.amenities` is `JSONB`
- `fact_bookings.is_cancelled` is `BOOLEAN NOT NULL DEFAULT FALSE`
- `agg_hourly_city_stats` PRIMARY KEY `(city, window_start)` — use `avg_occupancy_rate` (not `_pct`). Full schema: `datamodel.md` (migration 007 added 4 count columns post-Phase-1).

Apply the schema:
```bash
docker exec travellens-postgres psql -U travellens -d travellens -f /db/schema.sql
```

Verify 14 tables exist:
```bash
docker exec travellens-postgres psql -U travellens -d travellens -c "\dt" | grep -c "^ public"
# Expected: 14
```

---

### Step 6 — Write `scripts/load_to_postgres.py`

The loader must:
- Read config from `.env` via `python-dotenv`
- Be idempotent: truncate all tables with `CASCADE` and `RESTART IDENTITY` before loading
- Disable FK checks with `SET session_replication_role = replica;` before loading
- Load CSVs in this exact order (reference → dim → fact):
  1. `ref_price_tiers.csv` → `ref_price_tiers`
  2. `ref_state_centroids.csv` → `ref_state_centroids`
  3. `india_states_zones.csv` → `india_states_zones`
  4. `public_holidays.csv` → `public_holidays`
  5. `dim_date.csv` → `dim_date`
  6. `dim_location.csv` → `dim_location`
  7. `dim_customer.csv` → `dim_customer`
  8. `hotel_master.csv` → `hotel_master`
  9. `dim_room_type.csv` → `dim_room_type`
  10. `fact_bookings.csv` → `fact_bookings`
  11. `fact_price_events.csv` → `fact_price_events`
  12. `reviews_raw.csv` → `reviews_raw`
- Use `cursor.copy_expert("COPY {} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')", file)` — NOT row-by-row INSERTs
- Re-enable FK checks with `SET session_replication_role = DEFAULT;` — if this fails, an orphan exists; rollback
- After load, create indexes on:
  - `fact_bookings (hotel_id, date_id)`, `(date_id)`, `(customer_id)`
  - `fact_price_events (hotel_id, event_ts)`
  - `reviews_raw (hotel_id)`
  - `dim_room_type (hotel_id)`
  - `hotel_master (location_id)`
  - `agg_hourly_city_stats (city, window_start DESC)`
- Use a single transaction wrapping all steps; rollback on any exception

---

### Step 7 — Run the loader

```bash
python scripts/load_to_postgres.py
```

Expected final output: `✓ COMMIT — Phase 1 load complete`
Expected runtime: 60–120 seconds. Over 5 minutes means indexes were not deferred — stop
and investigate.

---

### Step 8 — Write `scripts/validate_load.py`

20 checks using a `Check` dataclass with `name`, `sql`, `pass_if` (lambda), `detail`.
Print ✓ / ✗ per check. Exit 0 if all pass, exit 1 otherwise.

| # | Check | Pass condition |
|---|---|---|
| 1 | `ref_price_tiers` count | `== 4` |
| 2 | `public_holidays` count | `== 147` |
| 3 | `dim_date` count | `== 2557` |
| 4 | `dim_customer` count | `== 20000` |
| 5 | `hotel_master` count | `== 2000` |
| 6 | `dim_room_type` count | `== 5542` |
| 7 | `fact_bookings` count | `== 1000000` |
| 8 | `fact_price_events` count | `== 86650` |
| 9 | `reviews_raw` count | `== 30000` |
| 10 | Orphan bookings → hotels | `== 0` |
| 11 | Orphan bookings → customers | `== 0` |
| 12 | Orphan bookings → dim_date | `== 0` |
| 13 | Orphan reviews → hotels | `== 0` |
| 14 | Orphan room_types → hotels | `== 0` |
| 15 | No negative revenue (non-cancelled) | `== 0` |
| 16 | Cancellation rate in range | `8 <= v <= 15` |
| 17 | Ratings in valid range | `== 0` |
| 18 | No embeddings yet (Phase 3 work) | `== 0` |
| 19 | All hotels linked to location | `== 0` |
| 20 | Total revenue in plausible range (crore) | `1800 <= v <= 2500` |

---

### Step 9 — Run validation

```bash
python scripts/validate_load.py
```

Expected: `PASSED: 20 / 20` and exit code 0. If any check fails, do not proceed to
Phase 2 — report which check failed and stop.

---

## ACCEPTANCE TESTS

Run all 7 in sequence. Every command must succeed.

```bash
# 1. Container is healthy
docker compose --env-file .env -f docker/docker-compose.yml ps | grep "travellens-postgres" | grep -q "healthy"

# 2. Schema has 14 tables
test $(docker exec travellens-postgres psql -U travellens -d travellens -tAc \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'") -eq 14

# 3. pgvector extension installed
docker exec travellens-postgres psql -U travellens -d travellens -tAc \
  "SELECT 1 FROM pg_extension WHERE extname='vector'" | grep -q "^1$"

# 4. fact_bookings has exactly 1M rows
test $(docker exec travellens-postgres psql -U travellens -d travellens -tAc \
  "SELECT COUNT(*) FROM fact_bookings") -eq 1000000

# 5. reviews_raw has exactly 30K rows with NULL embeddings
test $(docker exec travellens-postgres psql -U travellens -d travellens -tAc \
  "SELECT COUNT(*) FROM reviews_raw") -eq 30000
test $(docker exec travellens-postgres psql -U travellens -d travellens -tAc \
  "SELECT COUNT(*) FROM reviews_raw WHERE embedding IS NOT NULL") -eq 0

# 6. Zero FK orphans
test $(docker exec travellens-postgres psql -U travellens -d travellens -tAc \
  "SELECT COUNT(*) FROM fact_bookings b LEFT JOIN hotel_master h ON b.hotel_id=h.hotel_id WHERE h.hotel_id IS NULL") -eq 0

# 7. Validator passes all 20 checks
python scripts/validate_load.py
```

After all 7 succeed, output `PHASE 1 ACCEPTED` and stop. Do not proceed to Phase 2.

---

## EXPLORE

Run these queries after the load completes to understand what's in the database.

**psql:**
```bash
docker exec -it travellens-postgres psql -U travellens -d travellens
```

**VSCode terminal:** use the psql command above, or connect via any Postgres extension.

---

### E1 — Table row counts at a glance

Confirm everything loaded correctly:

```sql
SELECT schemaname, tablename,
       n_live_tup AS approx_row_count
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
```

---

### E2 — Top 10 cities by total revenue

```sql
SELECT
    l.city,
    COUNT(*)                              AS bookings,
    ROUND(SUM(b.revenue_inr) / 1e7, 1)  AS revenue_cr,
    ROUND(AVG(b.revenue_inr), 0)         AS avg_booking_inr
FROM fact_bookings b
JOIN hotel_master h  ON b.hotel_id  = h.hotel_id
JOIN dim_location l  ON h.location_id = l.location_id
WHERE NOT b.is_cancelled
GROUP BY l.city
ORDER BY revenue_cr DESC
LIMIT 10;
```

---

### E3 — Cancellation rate by customer segment

```sql
SELECT
    c.customer_segment,
    COUNT(*)                                                     AS total_bookings,
    ROUND(100.0 * SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END)
          / COUNT(*), 2)                                         AS cancel_pct
FROM fact_bookings b
JOIN dim_customer c ON b.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY cancel_pct DESC;
```

---

### E4 — Seasonal booking pattern (top 5 cities)

```sql
SELECT
    l.city,
    d.month_name,
    COUNT(*) AS bookings
FROM fact_bookings b
JOIN hotel_master h ON b.hotel_id = h.hotel_id
JOIN dim_location l ON h.location_id = l.location_id
JOIN dim_date d     ON b.date_id = d.date_id
WHERE l.city IN ('Goa', 'Mumbai', 'Manali', 'Jaipur', 'Udaipur')
  AND NOT b.is_cancelled
GROUP BY l.city, d.month_name, d.month
ORDER BY l.city, d.month;
```

---

### E5 — Price tier distribution

```sql
SELECT
    pt.tier_name,
    COUNT(rt.room_type_id)  AS room_types,
    ROUND(AVG(rt.base_price_inr), 0) AS avg_base_price
FROM dim_room_type rt
JOIN ref_price_tiers pt ON rt.price_tier_id = pt.tier_id
GROUP BY pt.tier_name
ORDER BY avg_base_price;
```

---

### E6 — Sample reviews (no embeddings yet — that's Phase 3)

```sql
SELECT
    review_id,
    hotel_id,
    rating,
    source,
    LEFT(review_text, 100) AS preview,
    embedding IS NOT NULL  AS has_embedding
FROM reviews_raw
ORDER BY RANDOM()
LIMIT 10;
```

All rows should show `has_embedding = false` after Phase 1. Embeddings are populated in
Phase 3.

---

### E7 — Hotels with the most price change events

```sql
SELECT
    h.hotel_name,
    l.city,
    COUNT(*)                AS price_changes,
    ROUND(AVG(ABS(pe.new_price_inr - pe.old_price_inr)), 0) AS avg_change_inr
FROM fact_price_events pe
JOIN hotel_master h ON pe.hotel_id = h.hotel_id
JOIN dim_location l ON h.location_id = l.location_id
GROUP BY h.hotel_name, l.city
ORDER BY price_changes DESC
LIMIT 10;
```

---

### E8 — Revenue sanity check

```sql
SELECT
    COUNT(*)                                AS total_bookings,
    MIN(checkin_date)                       AS earliest_checkin,
    MAX(checkin_date)                       AS latest_checkin,
    ROUND(SUM(revenue_inr) / 1e7, 0)      AS total_revenue_cr,
    ROUND(AVG(CASE WHEN is_cancelled THEN 1.0 ELSE 0.0 END) * 100, 1) AS cancel_pct
FROM fact_bookings;
-- Expected: ~1M bookings, ~₹2097 cr revenue, ~11.8% cancellation rate
```

---

## DO NOT

- Do not use `INSERT INTO ... VALUES` for bulk loading — `COPY FROM STDIN` only
- Do not create indexes inside `schema.sql` — they go in `load_to_postgres.py` post-load
- Do not generate embeddings — `reviews_raw.embedding` stays NULL after Phase 1 (Phase 3 work)
- Do not skip the FK re-enable step — silently disabling FKs forever defeats the safety net
- Do not modify any file in `data/`
- Do not commit `.env` to git
- Do not run on a production Postgres instance — `TRUNCATE ... CASCADE` will wipe data

---

## ROLLBACK

```bash
cd docker
docker compose --env-file ../.env down -v   # -v removes the data volume
rm -rf ../db/schema.sql ../scripts/load_to_postgres.py ../scripts/validate_load.py
rm -rf ../docker/postgres.Dockerfile ../docker/docker-compose.yml
```

To restart: bring the container back up and re-run Steps 3 onward.

---

## LESSONS LEARNED

- Actual load time:
- Any FK orphan errors during re-enable:
- Any validation check failures:

---


## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any code
- Create every file in the REPO STATE FILE TREE — no extras, no missing files
- Do NOT create indexes inside `schema.sql` — indexes go in `load_to_postgres.py` post-load
- Do NOT use `INSERT INTO ... VALUES` for bulk loading — `COPY FROM STDIN` only
- Do NOT generate embeddings — `reviews_raw.embedding` stays NULL after Phase 1
- Table creation order in `schema.sql` must follow: reference → dimensions → facts → aggregates
- `agg_hourly_city_stats` column name is `avg_occupancy_rate` (not `avg_occupancy_pct`)
- `agg_daily_hotel_kpi` columns: `total_bookings`, `total_revenue_inr`, `avg_nightly_rate_inr`, `cancellation_rate`, `avg_rating` — no `occupancy_rate`, no `revpar_inr`
- Run all 7 acceptance tests — report `PHASE 1 ACCEPTED` only after all pass
- Do not auto-proceed to Phase 2
- Never modify `data/` or `.env`


## BUILD HISTORY / EVOLUTION

### B-046 — Stage 1 dimension expansion (2026-05-27)

Additive scale-out of the four dimension tables. No fact data was touched; no migration was added.

**Delta:** `dim_location` 44 → 993 (+949) · `hotel_master` 2,000 → 20,076 (+18,076) · `dim_room_type` 5,542 → 55,446 (+49,904) · `dim_customer` 20,000 → 100,000 (+80,000). `fact_bookings`, `fact_booking_events`, `fact_booking_lifecycle`, `reviews_raw` all unchanged.

**Why a new path (not regen).** The Phase 1 loader `scripts/load_to_postgres.py` `TRUNCATE`s every table on every run ([scripts/load_to_postgres.py:43-79](scripts/load_to_postgres.py#L43-L79)) — that's correct behaviour for a one-shot bulk load but destructive for any in-place evolution. Re-running the Stage 1/2/3 generators would also overwrite the CSVs in `data/` rather than appending. The expansion needed a separate code path that (a) doesn't touch existing rows, (b) doesn't depend on the frozen loader, (c) is idempotent.

**What shipped:**

- `seeds/cities_expansion.csv` — committed catalog of 949 curated cities (city, state, region, tourism_zone, latitude, longitude, tourist_arrivals_annual_m, peak_months, popularity_tier).
- `scripts/build_cities_expansion_csv.py` — deterministic CSV builder (SEED=42) with inline catalog. Validates state names against `india_states_zones` and rejects within-catalog (city, state) duplicates.
- `scripts/expand_dimensions.py` — single-transaction INSERT for all four tables. Pre-flight idempotency guards: (a) no catalog (city, state) may already exist in `dim_location`; (b) `MAX(hotel_id) == 'HTL-002000'`; (c) `MAX(customer_id) == 'CUST-020000'`. Post-insert integrity guards inside the same transaction: fact-row counts unchanged, FK orphan count = 0, every new hotel has ≥1 room type, no state drift. Any failure → ROLLBACK.

**Decisions made before the build (locked at planning):**

| # | Decision |
|---|---|
| 1 | `travel_purpose` vocabulary stays at 9 values — Backwater-zone customers map to `Wellness`. No vocabulary drift. |
| 2 | Hotel target 20,000 ±5%, bucket-uniform sampling. Actual = 20,076 (0.38% over). |
| 3 | `opened_year` extended to 2026. Migration 006 carries no CHECK constraint — verified, no new migration needed. |
| 4 | `tourist_arrivals_annual_m` capped at 24.0. It's a hotel-demand proxy for city-popularity weights, not Ministry-of-Tourism footfall. Documented in `datamodel.md`. |
| 5 | `cities_expansion.csv` lives in a committed `seeds/` directory, not gitignored `data/`. |

**New room-type values (free text, no migration):** `Houseboat Suite` (Backwater), `Tent` (Wildlife + Hill Station), `Treehouse` (Wildlife). Total distinct `dim_room_type.type_name` values = 16 (was 13).

**Verification — independent queries after commit:**

| Check | Result |
|---|---|
| Houseboat in Backwater hotels | 175 / 175 (no leakage) |
| Treehouse in Wildlife hotels | 186 / 186 (no leakage) |
| Tent in Wildlife / Hill Station | 409 / 266 (zone-gated) |
| Distinct states in `dim_location` | 35 of 37 valid (Chandigarh-UT and Dadra & Nagar Haveli remain unrepresented — same as pre-expansion) |
| FK orphans (hotel→loc, hotel→price_tier, room→hotel, room→price_tier) | 0 / 0 / 0 / 0 |
| Hotels without ≥1 room type | 0 |
| `opened_year` range | 1975 → 2026 (1,084 hotels with opened_year ≥ 2024) |
| Spot-check: 10 random new cities have hotels | Auli 21, Ayodhya 148, Gangtok 70, Hampi 45, Kumarakom 60, Leh 40, Madurai 123, Pondicherry 45, Tawang 35, Tirupati 145 — all non-zero, ranges plausible per popularity tier |

**Idempotency:** A second `python -m scripts.expand_dimensions` aborts at the pre-flight (catalog cities already present, hotel max past HTL-002000, customer max past CUST-020000) with a clear stderr message and a ROLLBACK. Total runtime for the successful run: 7.9s end-to-end (generate in Python + four bulk INSERTs + integrity checks).

**Out of scope (deliberate — separate items):** Stage 2 fact backfill for the new ~18K hotels' Feb–May 2026 window; stream-simulator gating so the producer doesn't replay bookings that don't exist for the new hotels; lifecycle reconstruction. The audit flagged these as needing independent design (sim-clock anchor 2025-06-01 makes Feb–May 2026 stream-replay territory).

---

## NEXT

Phase 2 — `docs/phase-2-streaming.md`
