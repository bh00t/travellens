# Phase 6 — Airflow Orchestration

> **Stack:** Apache Airflow 2.9.2 · Postgres 16 (metadata, separate instance) · Docker · LocalExecutor  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **DAGs:** `refresh_pinned_widgets` · `daily_hotel_kpi` · `reconcile_late_events` · `hotel_sentiment_scores` · `customer_ltv`  
> **Status:** [x] In progress / [ ] Complete  

---


## REPO STATE AFTER THIS PHASE

```
travellens/
├── docker/
│   └── docker-compose.yml            ← MODIFY (add airflow + airflow-postgres services)
├── airflow/
│   ├── dags/
│   │   ├── refresh_pinned_widgets.py ← CREATE (B-024)
│   │   ├── daily_hotel_kpi.py        ← CREATE (B-013)
│   │   ├── reconcile_late_events.py  ← CREATE (B-014)
│   │   ├── hotel_sentiment_scores.py ← CREATE (B-015)
│   │   └── customer_ltv.py           ← CREATE (B-016)
│   ├── plugins/                      ← LEAVE EMPTY (bind-mount target only)
│   └── requirements-airflow.txt      ← CREATE (provider manifest, container-only)
├── db/
│   └── migrations/
│       ├── 006_hotel_sentiment_scores.sql ← CREATE (target table for B-015)
│       └── 007_customer_ltv.sql           ← CREATE (target table for B-016)
├── docs/
│   └── phase-6-airflow.md            ← this file
├── requirements.txt                  ← LEAVE ALONE (sqlalchemy stays at 2.0.x)
└── .env                              ← LEAVE ALONE (add Airflow vars manually)
```

## OBJECTIVE

Stand up Airflow as an isolated orchestration layer and ship the five
scheduled DAGs that move batch compute off manual execution:
`refresh_pinned_widgets` (B-024), `daily_hotel_kpi` (B-013),
`reconcile_late_events` (B-014), `hotel_sentiment_scores` (B-015),
`customer_ltv` (B-016).

After this phase: the warehouse fills its empty aggregate tables on a
schedule; late streaming events stop being silently excluded; the dashboard
read path serves a Redis-shaped cache that a scheduler writes to (no
on-request compute); and two new Gold tables (sentiment + LTV) replace
queries that previously required either a full table scan or a
semantic-path round-trip.

---

## PREREQUISITES

- [ ] Phase 1 accepted — `fact_bookings` has 1M rows, `dim_location` populated, `agg_daily_hotel_kpi` exists but is empty (Limitation L-001)
- [ ] Phase 2 accepted — `agg_hourly_city_stats` populated, MinIO has `late_events/` quarantine prefix
- [ ] Phase 3 accepted — `reviews_raw.embedding` populated, IVFFlat index exists
- [ ] Phase 4 accepted — `ai.main.answer()` works end-to-end against the warehouse
- [ ] Phase 5 accepted — dashboard ships with B-022 in place: `dashboard_widgets.generated_sql` and `last_result_json` columns exist. B-024 has nothing to execute without `generated_sql`.
- [ ] `docker ps` shows `travellens-postgres`, `travellens-kafka`, `travellens-minio` healthy
- [ ] Port 8080 free on the host (Airflow webserver)
- [ ] `cryptography` available locally for one-off key generation:

```bash
pip install cryptography
```

If any prerequisite fails, stop. Do not improvise. B-024 is the most
sensitive — without Phase 5's frozen SQL, the DAG has nothing to run.

---

## ARCHITECTURE DECISIONS (LOCKED)

These design choices are settled. Do not re-litigate during STEPS.

### Airflow runs in its own container, on its own Python

Airflow 2.9.x requires `SQLAlchemy < 2.0`. The rest of the project pins
`sqlalchemy==2.0.30` because everything else — psycopg2, the Flask
dashboard, the embedding generator — works fine on 2.x and uses the newer
query API. A combined Airflow image runs in its own environment, sees
its own SQLAlchemy 1.4.x, and never collides with the project's pin. Do
NOT downgrade `sqlalchemy` in `requirements.txt` to make Airflow
installable locally — the conflict is the point.

### Airflow metadata DB is separate from the warehouse

**Decision.** Airflow uses its own metadata Postgres — `airflow-postgres`,
a dedicated Postgres 16 instance with its own volume and credentials,
internal-only (no host port). DAG runs, task instances, XCom, encrypted
Connection passwords, and the audit log live there. `fact_bookings`,
`dim_location`, `agg_*`, `reviews_raw`, and `dashboard_widgets` stay in
the warehouse Postgres.

**Why, in order of weight:**

1. **The hinge — runtime isolation already forces a container split.**
   Airflow 2.9.x requires `SQLAlchemy < 2.0`; the project pins 2.0.x. The
   Airflow container therefore exists no matter what. Given that, a
   separate metadata Postgres is nearly free (~150 MB) — and sharing the
   warehouse DB instance would *re-couple at the data layer what was just
   decoupled at the runtime layer*. Isolated runtime + shared state is an
   inconsistent half-decision, worse than either pure option (fully
   shared, or fully separate).

2. **Blast radius.** Airflow writes metadata continuously — scheduler
   heartbeats, task instance transitions, DAG parse results, audit log
   rows. The 10-min `refresh_pinned_widgets` schedule makes the write
   rate heavier still. A separate instance means Airflow's load, locks,
   disk-fill, or a bad metadata-schema migration cannot take the
   warehouse down. Orchestration bookkeeping never threatens business
   data.

3. **Independent lifecycle.** Airflow metadata gets reset far more often
   than the warehouse — re-initing during DAG development, clearing
   stuck task instances, blowing away test runs. With a separate
   container, an Airflow reset is `docker compose down -v airflow-postgres`
   and back up — no fear of glancing the warehouse volume. With shared
   state, that same operation would either be impossible without
   touching warehouse rows, or would require schema-level surgery to
   keep the warehouse safe.

4. **Production fidelity.** Real deployments never co-locate Airflow
   metadata with the analytical warehouse. Mirroring the production
   shape locally — separate orchestration store, separate connection
   string — is the entire point of a learning/portfolio project. A
   shared-DB shortcut would build a habit that doesn't survive the
   first managed-Airflow deployment.

**Alternative considered (and rejected): a separate schema inside
travellens-postgres.** Technically works, saves the ~150 MB and the
extra container. Rejected because it re-introduces every coupling
above: same instance = same locks, same disk, same backup window, same
"can I reset Airflow without touching the warehouse?" question. The
container is already isolated for SQLAlchemy reasons; doing the
schema-only shortcut would mean isolated runtime + shared instance —
exactly the inconsistent half-decision item 1 calls out. Rejected for
consistency with the rest of the architecture.

### LocalExecutor with a combined image, not Celery

A single container runs scheduler + webserver + triggerer via
`airflow standalone`. No Celery workers, no Redis broker, no Flower.
The portfolio workload is a handful of DAGs over a 1M-row warehouse;
LocalExecutor is correctly sized. `airflow standalone` is documented as
dev/test only — accurate for this scope. The production migration is a
custom Dockerfile + split scheduler/webserver/triggerer services; the
canonical package list already lives in `airflow/requirements-airflow.txt`.

### DAGs reach the world through Airflow Connections, not hardcoded creds

`travellens_warehouse` (Postgres) and `travellens_s3` (MinIO via the
Amazon provider's `endpoint_url` override) come from `AIRFLOW_CONN_*`
env vars that Airflow auto-imports as Connection objects at startup.
DAGs use `PostgresHook` and `S3Hook` and never see raw URIs. Repointing
at a real Postgres or real AWS later is two env var edits — no DAG
changes. Rotation is the same: change the env var, restart the
scheduler.

### Frozen SQL is the contract — DAGs do not regenerate

`refresh_pinned_widgets` runs `dashboard_widgets.generated_sql`
verbatim. It never calls Ollama, never re-prompts, never falls back to
`answer()`. If the frozen SQL breaks, the DAG records the failure and
moves on; recovery is the dashboard's user-triggered Regenerate
action, not an automatic retry. Same SQL on every refresh is the whole
reason B-022 froze it.

### Provider versions are pinned

`apache-airflow-providers-postgres==5.10.0` and
`apache-airflow-providers-amazon==8.20.0`. Newer is not better here —
provider Connection schemas occasionally break across major versions.

---

## STEPS

### Step 1 — Stand up Airflow infrastructure

Extend `docker/docker-compose.yml` with two new services without
touching the existing four. Mirror Phase 2's pattern of additive
service blocks; do not replace the file.

```yaml
  airflow-postgres:
    image: postgres:16
    container_name: travellens-airflow-postgres
    environment:
      POSTGRES_DB:       ${AIRFLOW_POSTGRES_DB:-airflow}
      POSTGRES_USER:     ${AIRFLOW_POSTGRES_USER:-airflow}
      POSTGRES_PASSWORD: ${AIRFLOW_POSTGRES_PASSWORD:-airflow}
    volumes:
      - airflow_postgres_data:/var/lib/postgresql/data
    # No ports: — internal-only.

  airflow:
    image: apache/airflow:2.9.2
    depends_on:
      airflow-postgres: { condition: service_healthy }
    environment:
      AIRFLOW__CORE__EXECUTOR:                 LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES:            "False"
      AIRFLOW__CORE__FERNET_KEY:               ${AIRFLOW_FERNET_KEY}
      AIRFLOW__WEBSERVER__SECRET_KEY:          ${AIRFLOW_WEBSERVER_SECRET_KEY}
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:     postgresql+psycopg2://...@airflow-postgres:5432/airflow
      AIRFLOW_CONN_TRAVELLENS_WAREHOUSE:       ${AIRFLOW_CONN_TRAVELLENS_WAREHOUSE}
      AIRFLOW_CONN_TRAVELLENS_S3:              ${AIRFLOW_CONN_TRAVELLENS_S3}
      _PIP_ADDITIONAL_REQUIREMENTS: >-
        apache-airflow-providers-postgres==5.10.0
        apache-airflow-providers-amazon==8.20.0
    ports:
      - "8080:8080"
    volumes:
      - ../airflow/dags:/opt/airflow/dags
      - ../airflow/plugins:/opt/airflow/plugins
    command: standalone
```

Add `airflow_postgres_data:` to the existing `volumes:` block. Create
`airflow/requirements-airflow.txt` as the canonical provider manifest,
separate from the project's `requirements.txt`.

Extend `.env` with five new variables — `AIRFLOW_POSTGRES_PASSWORD`,
`AIRFLOW_FERNET_KEY`, `AIRFLOW_WEBSERVER_SECRET_KEY`,
`AIRFLOW_CONN_TRAVELLENS_WAREHOUSE`, `AIRFLOW_CONN_TRAVELLENS_S3`.
`.env.example` documents the format; the Fernet key and webserver
secret must be generated locally and never committed.

Validate before bringing anything up:

```bash
cd docker
docker compose --env-file ../.env config -q
```

Silent output = valid. Then start the new services only:

```bash
docker compose --env-file ../.env up -d airflow-postgres airflow
```

First start is 3–5 minutes (image pull + provider install + db migrate).

---

### Step 2 — Initialise the metadata DB, admin user, and Connections

`airflow standalone` runs `db migrate` against `airflow-postgres` and
creates an admin user automatically. Retrieve the generated password
once:

```bash
docker exec travellens-airflow cat /opt/airflow/standalone_admin_password.txt
```

Username is `admin`. Log in at `http://localhost:8080` and confirm:

- `Admin → Connections` lists `travellens_warehouse` and `travellens_s3`
  (auto-imported from `AIRFLOW_CONN_*` env vars).
- `DAGs` page is empty (no examples — `LOAD_EXAMPLES=False`).

Test each Connection from inside the container before writing any DAG
that uses it:

```bash
docker exec travellens-airflow airflow connections test travellens_warehouse
docker exec travellens-airflow airflow connections test travellens_s3
```

Both must return success. The most common failure is `localhost` in
the URI instead of the Docker service name (`postgres` / `minio`) —
Airflow runs inside the compose network and must address peers by
service name.

---

### Step 3 — `refresh_pinned_widgets` DAG (B-024)

**First DAG by design.** Smallest scope, depends only on B-022 (already
in place), and its output (a populated `last_result_json`) is already
consumed by the dashboard's read path. Building this first validates
the warehouse Connection, the scheduler, and the cache-write contract
before tackling heavier aggregations.

#### Configuration — every value has a reason

| Field             | Value          | Why this exactly |
|-------------------|----------------|------------------|
| `schedule_interval` | `*/10 * * * *` | Polls due widgets every 10 minutes. 10 min over-samples the Phase 2 streaming window (60 min) by 6×, so any widget that depends on `agg_hourly_city_stats` always sees the latest closed window within a fraction of its size. 10 min also sits safely above Airflow's scheduler overhead floor (the scheduler heartbeat + DAG parse pass costs more than 1 min in non-trivial setups), so a tighter cadence would just churn the metadata DB without finer-grained user-visible freshness. |
| `max_active_runs` | `1`            | Two overlapping runs would race on the same `last_result_json` writes — last-write-wins, but the loser still consumed warehouse cycles. Capping to 1 makes the run profile single-threaded and bounds the queue to exactly one pending run regardless of how long a run takes. |
| `catchup`         | `False`        | A 10-min schedule has 144 slots per day. Without `catchup=False`, deploying the DAG with a `start_date` of e.g. 2026-01-01 materialises one DagRun per missed slot since that date — thousands of runs queued before the first ever fires. The compute is also pointless: cache freshness is "what's stale RIGHT NOW", not "what was stale at 03:40 last Tuesday". |
| `dagrun_timeout`  | `timedelta(minutes=8)` | Hard upper bound on a single run so a stuck run cannot block the next slot. Set below the schedule interval (8 < 10) so a killed run still leaves a buffer minute for the scheduler to mark it failed and start the next one on time. |
| `execution_timeout` (task-level) | `timedelta(minutes=6)` | A single runaway widget query (broken frozen SQL hitting a table lock, network stall on the warehouse connection) cannot hold the entire run hostage. Set below the DAG timeout so the task fails before the dagrun does — gives Airflow a clean failure to attribute. |
| `start_date`      | A fixed past date in code, e.g. `datetime(2026, 5, 1)` | Dynamic `start_date` (e.g. `days_ago(1)`) is a classic Airflow footgun: the scheduler computes which runs are due by comparing wall-clock now to `start_date`, and a moving `start_date` interacts badly with `catchup` and run-pinning. Fix it once, never change it. |
| `default_args.retries` | `0` for the single task in this DAG | Re-running stale widgets is what the next slot already does. Burning retries inside the same run just delays the next due-check. |

#### Logic

The DAG does NOT refresh every widget every run. It runs a **due-check**
against the warehouse:

```sql
SELECT widget_id, generated_sql, refresh_interval_minutes, widget_type, title
FROM dashboard_widgets
WHERE generated_sql IS NOT NULL
  AND refresh_interval_minutes > 0
  AND (
    last_refreshed_at IS NULL
    OR NOW() - last_refreshed_at >= refresh_interval_minutes * INTERVAL '1 minute'
  );
```

For each due widget:

1. Execute `generated_sql` against the warehouse via `PostgresHook`. Cap
   at 100 rows to match the runtime path.
2. Build the widget config (same shape as `render.widget_renderer.build_widget_config`).
3. UPDATE `dashboard_widgets` SET `last_result_json = <config>`,
   `last_refreshed_at = NOW()` for that widget.
4. On per-widget error: log the error, do NOT touch
   `last_result_json` (the previous cache is better than no cache),
   continue to the next widget.

The **per-widget `refresh_interval_minutes`** is the freshness contract.
The **10-minute DAG cadence** is the polling resolution. These are
different knobs: a widget configured for 60-minute refresh is checked
every 10 min but only re-executed when ≥60 min have elapsed since its
last refresh.

#### Operational guarantees worth stating explicitly

- **Bounded pending queue.** With `max_active_runs=1` and `catchup=False`,
  the pending queue is structurally capped at one waiting run, regardless
  of how slow any single run is or how long Airflow was offline. The
  scheduler will not stack 200 queued runs after a maintenance window.

- **Late-run self-correction.** Because the DAG decides what to do by
  querying `last_refreshed_at` at execution time, the exact firing time
  is irrelevant. A delayed run fires at 03:17 instead of 03:10 and
  simply refreshes whatever is stale at 03:17 — including the widget
  that would have been refreshed at 03:10. The contract is on
  `last_refreshed_at`, never on the DagRun's scheduled time. This is
  what makes the DAG safe to leave running indefinitely; no manual
  backfill is ever required.

- **Effective minimum refresh = 10 min.** A widget set to
  `refresh_interval_minutes < 10` cannot benefit — the DAG only runs
  every 10 min. The dashboard UI should drop the **5** and **15** values
  from the refresh-interval dropdown and stock multiples of 10 instead:
  `Off / 10 / 30 / 60 / 360 / 1440`. This is a **follow-on dashboard
  change**, not part of this DAG; track it as a B-024 sub-item in the
  backlog.

#### Acceptance check

```sql
-- 1. Every widget with refresh_interval_minutes > 0 has staleness < its interval + one tick
SELECT widget_id,
       refresh_interval_minutes,
       last_refreshed_at,
       EXTRACT(EPOCH FROM (NOW() - last_refreshed_at))/60 AS minutes_stale
FROM dashboard_widgets
WHERE refresh_interval_minutes > 0
ORDER BY widget_id;
-- For every row: minutes_stale < refresh_interval_minutes + 10
```

```bash
# 2. Dashboard read path returns from_cache:true on the call after a scheduled run
curl -s -X POST http://localhost:5000/api/refresh/<widget_id> | grep '"from_cache":true'
```

If both pass, the read path no longer triggers compute — B-022 + B-024
are end-to-end proven.

---

### Step 4 — `daily_hotel_kpi` DAG (B-013)

Unblocks `agg_daily_hotel_kpi`, which has been empty since Phase 1
(Limitation L-001). Until this DAG runs, every dashboard widget that
aggregates from the KPI table returns 0 rows.

| Field             | Value |
|-------------------|-------|
| `schedule_interval` | `0 2 * * *` (daily at 02:00 — off-peak) |
| `catchup`         | `True` for the first deploy (backfill from earliest `fact_bookings.event_ts`); flip to `False` after the backfill completes |
| `max_active_runs` | `1` |
| Source tables     | `fact_bookings`, `hotel_master`, `reviews_raw`, `dim_date` |
| Target            | `agg_daily_hotel_kpi` (UPSERT on `(hotel_id, date_id)`) |
| Connection        | `travellens_warehouse` |

**Computation** — for each `(hotel_id, date_id)` in the previous day's
`fact_bookings`:

- `total_bookings` — `COUNT(*) WHERE NOT is_cancelled`
- `total_revenue_inr` — `SUM(revenue_inr) WHERE NOT is_cancelled`
- `avg_nightly_rate_inr` — `AVG(nightly_rate_inr)`
- `cancellation_rate` — `COUNT(*) FILTER (WHERE is_cancelled) / COUNT(*)`
- `avg_rating` — `AVG(rating)` from `reviews_raw` joined on `hotel_id`
  for that date

The column list is **LOCKED** to the existing schema — no
`occupancy_rate`, no `revpar_inr`; both have been hallucinated in the
past, neither exists.

**Acceptance check:**

```sql
-- Row count > 0 after first run
SELECT COUNT(*), COUNT(DISTINCT hotel_id) FROM agg_daily_hotel_kpi;

-- Sanity: aggregate totals match fact_bookings spot-check, to the rupee
SELECT SUM(total_revenue_inr) FROM agg_daily_hotel_kpi WHERE date_id = <yesterday>;
SELECT SUM(revenue_inr)        FROM fact_bookings       WHERE date_id = <yesterday> AND NOT is_cancelled;
```

---

### Step 5 — `reconcile_late_events` DAG (B-014)

Merges Phase 2's `late_events/` quarantine back into
`agg_hourly_city_stats`. Until this runs, late-arriving Kafka events
are permanently excluded from aggregates.

| Field             | Value |
|-------------------|-------|
| `schedule_interval` | `0 3 * * *` (daily at 03:00 — runs after `daily_hotel_kpi` to avoid lock contention) |
| `catchup`         | `False` — every run processes all unread `late_events/` files |
| `max_active_runs` | `1` |
| Source            | `s3://travellens-data/late_events/` Parquet via `travellens_s3` |
| Target            | `agg_hourly_city_stats` (UPSERT on `(city, window_start)`); processed files moved to `late_events/processed/` |
| Connections       | `travellens_warehouse`, `travellens_s3` |

**Logic:**

1. List `late_events/` Parquet files via `S3Hook`; skip the `processed/`
   sub-prefix.
2. For each file, read with `pyarrow`, re-group by
   `(city, hour_bucket(event_ts))`, compute the same aggregates the
   Phase 2 consumer produces.
3. UPSERT into `agg_hourly_city_stats` with
   `ON CONFLICT (city, window_start) DO UPDATE` — existing window
   counters are summed with the late contribution.
4. Move processed file to `late_events/processed/<original_key>` so the
   next run does not double-count. Copy then delete (atomic per file).

**Acceptance check:**

```sql
SELECT city, window_start, total_bookings, total_revenue_inr, updated_at
FROM agg_hourly_city_stats
WHERE city = '<known_late_city>' AND window_start = '<known_late_window>';
-- updated_at must be more recent than Phase 2's original write
-- total_bookings must equal Phase 2 value + late contribution
```

```bash
# Processed files moved out of the active prefix
docker exec travellens-minio mc ls local/travellens-data/late_events/ | grep -v processed/
# Should print nothing (or only the processed/ entry)
```

---

### Step 6 — Weekly Gold tables: `hotel_sentiment_scores` (B-015) and `customer_ltv` (B-016)

Two weekly DAGs that build Gold-layer aggregates other tooling can
query without paying the underlying compute cost on each request.
Both write to new tables created by migrations `006` and `007`.

#### B-015 — `hotel_sentiment_scores`

| Field             | Value |
|-------------------|-------|
| `schedule_interval` | `0 4 * * 0` (Sundays at 04:00) |
| `catchup`         | `False` — recomputed in full each week |
| `max_active_runs` | `1` |
| Source tables     | `reviews_raw` (with `embedding`), `hotel_master` |
| Target            | `hotel_sentiment_scores` (new Gold table, migration 006) |
| Connection        | `travellens_warehouse` |

**Computation** — per `hotel_id`:

- `review_count` — `COUNT(*)`
- `avg_rating` — `AVG(rating)`
- `positive_share` — fraction with rating ≥ 4
- `negative_share` — fraction with rating ≤ 2
- `sentiment_score` — composite: `avg_rating + positive_share - negative_share`
  (range ≈ `[-1, 6]`, higher is better)

Truncate-and-load. Table is small (~1K hotels) and re-derived weekly;
no incremental complexity needed.

**Acceptance check:**

```sql
-- One row per hotel with at least one review
SELECT COUNT(*) FROM hotel_sentiment_scores;

-- Top 10 — answers in <50 ms, no semantic round-trip
SELECT hotel_id, sentiment_score, review_count
FROM hotel_sentiment_scores
ORDER BY sentiment_score DESC LIMIT 10;
```

#### B-016 — `customer_ltv`

| Field             | Value |
|-------------------|-------|
| `schedule_interval` | `0 5 * * 0` (Sundays at 05:00 — after B-015) |
| `catchup`         | `False` — recomputed in full each week |
| `max_active_runs` | `1` |
| Source tables     | `fact_bookings`, `dim_customer` |
| Target            | `customer_ltv` (new Gold table, migration 007) |
| Connection        | `travellens_warehouse` |

**Computation** — per `customer_id`:

- `lifetime_bookings` — `COUNT(*) WHERE NOT is_cancelled`
- `lifetime_revenue_inr` — `SUM(revenue_inr) WHERE NOT is_cancelled`
- `first_booking_date` / `last_booking_date` — MIN / MAX of `event_ts`
- `tenure_days` — `last_booking_date - first_booking_date`
- `ltv_tier` — bucket: Bronze (<₹50K), Silver (₹50K–₹2L),
  Gold (₹2L–₹10L), Platinum (≥₹10L)

Truncate-and-load.

**Acceptance check:**

```sql
-- Total revenue across tiers matches fact_bookings to the rupee
SELECT SUM(lifetime_revenue_inr) FROM customer_ltv;
SELECT SUM(revenue_inr)          FROM fact_bookings WHERE NOT is_cancelled;
-- Equal.

-- Tier distribution is monotonic in avg revenue
SELECT ltv_tier, COUNT(*), ROUND(AVG(lifetime_revenue_inr)) AS avg_rev
FROM customer_ltv
GROUP BY ltv_tier
ORDER BY MIN(lifetime_revenue_inr);
-- Platinum > Gold > Silver > Bronze
```

---

## ACCEPTANCE TESTS

Run after Step 6. Every command must succeed.

```bash
# 1. All six containers healthy (4 original + airflow + airflow-postgres)
docker compose --env-file .env -f docker/docker-compose.yml ps

# 2. Webserver responds
curl -fs http://localhost:8080/health

# 3. Both Connections test green from inside the container
docker exec travellens-airflow airflow connections test travellens_warehouse
docker exec travellens-airflow airflow connections test travellens_s3

# 4. All five DAGs registered, none in import-error state
docker exec travellens-airflow airflow dags list

# 5. Per-DAG: at least one successful run in the last 24 h
docker exec travellens-airflow airflow dags list-runs -d refresh_pinned_widgets --state success | head -3
docker exec travellens-airflow airflow dags list-runs -d daily_hotel_kpi        --state success | head -3
docker exec travellens-airflow airflow dags list-runs -d reconcile_late_events  --state success | head -3

# 6. Warehouse aggregates populated
docker exec travellens-postgres psql -U travellens -d travellens -c "
  SELECT 'agg_daily_hotel_kpi'    AS table, COUNT(*) FROM agg_daily_hotel_kpi
  UNION ALL SELECT 'hotel_sentiment_scores', COUNT(*) FROM hotel_sentiment_scores
  UNION ALL SELECT 'customer_ltv',           COUNT(*) FROM customer_ltv;"

# 7. End-to-end cache proof — refresh a pinned widget; second call returns from_cache:true
curl -s -X POST http://localhost:5000/api/refresh/<widget_id> | grep '"from_cache":true'

# 8. B-024 due-check actually short-circuits — widget refreshed within the last minute
#    should NOT be refreshed again on the next DAG tick (last_refreshed_at unchanged)
```

Test 7 is the proof of B-022 + B-024 together: scheduler writes the
cache, dashboard reads it, no Ollama or warehouse SQL fires on the
user's request path.

---

## DO NOT

- Do not pin `sqlalchemy < 2.0` in the project's `requirements.txt` — Airflow lives in its own container; the project's pin stays at 2.0.x
- Do not point Airflow's metadata DB at the warehouse Postgres — orchestration state and business data stay on separate instances
- Do not hardcode warehouse or S3 credentials inside any DAG file — use Conn IDs `travellens_warehouse` and `travellens_s3`
- Do not call `ai.main.answer()` or Ollama from any DAG — the AUTHOR path is user-triggered only; DAGs are the COMPUTE path
- Do not make `refresh_pinned_widgets` regenerate broken SQL — recovery is the dashboard's Regenerate button, never automatic
- Do not write to `last_result_json` on a per-widget error path — the previous cache is better than no cache; only successful runs touch it
- Do not use a dynamic `start_date` (`days_ago(...)`) — fix it once to a past date, never change it
- Do not raise `max_active_runs` above 1 for `refresh_pinned_widgets` — overlapping runs will race on the same row writes
- Do not expose `airflow-postgres` on the host (no `ports:` mapping) — it stores encrypted Connection passwords
- Do not commit the generated `AIRFLOW_FERNET_KEY` or `AIRFLOW_WEBSERVER_SECRET_KEY` — `.env` is gitignored; keep it that way
- Do not modify Phase 1–5 deliverables to make a DAG easier to write — adapt the DAG instead
- Do not switch to `CeleryExecutor` to "be more production-like" — at single-user scale it adds Redis + workers for zero throughput gain

---

## ROLLBACK

```bash
# Stop new services — leaves warehouse, Kafka, MinIO untouched
cd docker
docker compose --env-file ../.env stop airflow airflow-postgres
docker compose --env-file ../.env rm -f airflow airflow-postgres

# Optionally delete Airflow metadata DB volume (loses DAG history)
docker volume rm docker_airflow_postgres_data

# Remove DAG files
rm airflow/dags/refresh_pinned_widgets.py
rm airflow/dags/daily_hotel_kpi.py
rm airflow/dags/reconcile_late_events.py
rm airflow/dags/hotel_sentiment_scores.py
rm airflow/dags/customer_ltv.py

# Revert docker-compose.yml to the Phase 5 state (4 services) — manual edit:
# delete the airflow + airflow-postgres service blocks and the
# airflow_postgres_data volume entry.

# Drop the Gold tables Phase 6 migrations created
docker exec travellens-postgres psql -U travellens -d travellens -c "
  DROP TABLE IF EXISTS hotel_sentiment_scores;
  DROP TABLE IF EXISTS customer_ltv;"

# Truncate aggregates the DAGs populated (do NOT drop — Phase 1 created them)
docker exec travellens-postgres psql -U travellens -d travellens -c "
  TRUNCATE TABLE agg_daily_hotel_kpi;"
```

To restart: re-run Steps 1–6 from the top.

---

## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any DAG code
- Read `docs/phase-2-streaming.md` for the S3 key pattern and quarantine layout — `reconcile_late_events` must match that contract exactly
- Read `db/schema.sql` for the canonical column lists of `agg_daily_hotel_kpi`, `agg_hourly_city_stats`, `fact_bookings`, and `dashboard_widgets` BEFORE writing any SQL — never infer column names from memory
- Use Airflow Connections `travellens_warehouse` and `travellens_s3` — never hardcode URIs in DAG code
- Build B-024 FIRST — smallest scope, output already consumed by the dashboard, fastest validation loop
- Each DAG file lives at `airflow/dags/<dag_name>.py` and contains exactly one DAG with `dag_id` matching the filename
- For B-024 specifically: `schedule_interval='*/10 * * * *'`, `max_active_runs=1`, `catchup=False`, `dagrun_timeout=timedelta(minutes=8)`, task `execution_timeout=timedelta(minutes=6)`, `start_date` is a fixed past `datetime(...)` constant — NEVER `days_ago()` or any function that re-evaluates
- B-024 logic is a DUE-CHECK: `SELECT ... WHERE NOW() - last_refreshed_at >= refresh_interval_minutes * INTERVAL '1 minute'`. Only refresh widgets that come back from that query.
- B-024 must NEVER call Ollama, never regenerate SQL, never overwrite `last_result_json` on a per-widget failure
- Provider versions LOCKED at `apache-airflow-providers-postgres==5.10.0` and `apache-airflow-providers-amazon==8.20.0` — pinned in `airflow/requirements-airflow.txt`
- `daily_hotel_kpi` column list is LOCKED: `total_bookings`, `total_revenue_inr`, `avg_nightly_rate_inr`, `cancellation_rate`, `avg_rating` — no `occupancy_rate`, no `revpar_inr`
- Cities source of truth is `dim_location.city` — never `hotel_master.city` (does not exist)
- Generate `AIRFLOW_FERNET_KEY` and `AIRFLOW_WEBSERVER_SECRET_KEY` ONCE per environment, store in `.env` (not `.env.example`) — losing the Fernet key breaks every stored Connection password
- Test each Connection (`airflow connections test ...`) before writing the DAG that uses it
- After B-024 ships, file a follow-on backlog item to drop the `5` and `15` options from the dashboard's refresh-interval dropdown — they are below the DAG's resolution and silently no-op
- Run the ACCEPTANCE TESTS section in full before marking Phase 6 complete — including Test 7, the end-to-end cache proof
- Do not modify `requirements.txt`, `schema.sql`, or any Phase 1–5 deliverable
- Do not commit `.env`, generated keys, or `airflow/logs/`


## NEXT

Phase 6 is the last build phase. After it ships:

- Move B-013, B-014, B-015, B-016, B-024 from `docs/backlog.md` Phase 6 section to the Completed table
- File the dashboard follow-on: drop `5`/`15` from the refresh-interval dropdown (below B-024's 10-min resolution)
- Begin the production hardening list from the technical blueprint: custom Airflow Dockerfile in place of `_PIP_ADDITIONAL_REQUIREMENTS`, `last_result_json` JSONB → Redis with TTL, pure-Python streaming consumer → Kafka Connect against a real PMS, Postgres + pgvector → Snowflake + dedicated vector store
