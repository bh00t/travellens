# Phase 2 — Streaming Pipeline

> **Stack:** Python 3.11 · Kafka · MinIO (S3) · Postgres 16 · Docker  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Scripts:** `scripts/kafka_event_producer.py` · `scripts/stream_consumer.py`  
> **Status:** [ ] In progress / [x] Complete  

> **HISTORY DOCUMENT** — This records how Phase 2 was originally built and how it
> evolved through subsequent backlog items. For the current behaviour of
> `stream_consumer.py` and `kafka_event_producer.py`, see
> [CLAUDE.md](../CLAUDE.md) (repo layout, hard rules, gate order) ·
> [datamodel.md](../datamodel.md) (schema, bronze/silver/gold tables) ·
> [backlog.md](backlog.md) (B-031, B-032, B-034, B-034A, B-035, B-038, B-039, B-040, B-047).

---


## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Files this phase
creates / touches:

- **MODIFY** `docker/docker-compose.yml` (add Kafka, Zookeeper, MinIO services)
- **CREATE** `scripts/init_s3_buckets.py`
- **CREATE** `scripts/kafka_event_producer.py`
- **CREATE** `scripts/stream_consumer.py`
- **MODIFY** `.env` — add Kafka + S3 vars manually
- Post-acceptance changes touched these scripts under B-031 / B-032 / B-034 — see [Build History / Evolution](#build-history--evolution) below.

---

## OBJECTIVE

Build a pure-Python Kafka consumer that reads booking events, validates them through a
six-gate validator, aggregates them by city in tumbling event-time windows, and dual-sinks
each window to Postgres (`agg_hourly_city_stats`) and S3 Parquet
(`processed/agg/hourly_city_stats/`). Quarantine malformed events to
`s3://travellens-data/malformed_events/` and late events to
`s3://travellens-data/late_events/` so nothing is silently dropped.

A producer script generates synthetic events from the Phase 1 hotel pool, with
configurable chaos injection (`--malformed-pct`, `--late-pct`) for reproducible failure
testing.

> **Note:** "six-gate" and "dual-sink" describe the original Phase 2 design. The consumer
> grew to four sinks (B-038 bronze, B-039 silver added post-acceptance) and the gate order
> was revised (B-039). Current gate sequence: [CLAUDE.md](../CLAUDE.md) hard rules
> (`stream_consumer.py` row). The original six-gate design is documented in Architecture
> Decisions and Steps below and is preserved as build history.

---

## PREREQUISITES

- [ ] Phase 1 accepted (`PHASE 1 ACCEPTED` printed, 20/20 validation pass)
- [ ] `python scripts/validate_load.py` still passes
- [ ] `docker ps` shows `travellens-postgres` healthy
- [ ] `data/booking_events_seed.json` exists (Phase 1 generator output)
- [ ] `.env` contains: `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`,
  `S3_BUCKET=travellens-data`, `S3_PREFIX_PROCESSED=processed/`,
  `AWS_ENDPOINT_URL=http://localhost:9000`, `AWS_ACCESS_KEY_ID=minioadmin`,
  `AWS_SECRET_ACCESS_KEY=minioadmin`
- [ ] Required Python packages installed:

```bash
pip install kafka-python boto3 pyarrow psycopg2-binary python-dotenv
```

If any prerequisite fails, stop. Do not improvise.

---

## DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| Extended `docker-compose.yml` | `docker/` | Kafka + Zookeeper + MinIO added |
| `init_s3_buckets.py` | `scripts/` | `travellens-data` bucket + 4 prefixes exist |
| `kafka_event_producer.py` | `scripts/` | Generates events with chaos injection |
| `stream_consumer.py` | `scripts/` | Validates, aggregates, dual-sinks |

Do not create files outside this list. Do not modify Phase 1 deliverables.

---

## ARCHITECTURE DECISIONS (ORIGINAL)

These design choices are settled. Do not re-litigate during STEPS.

### Pure Python consumer, not PyFlink

PyFlink 1.18 is unstable on Windows + Python 3.11 — JNI gateway crashes during graceful
shutdown. At 50 evt/sec on a single node the Flink runtime is over-provisioned. Trade-off
accepted: at-least-once delivery (idempotent via Postgres `ON CONFLICT` and deterministic
S3 keys); no automatic checkpointing.

### Event-time windowing, not processing-time

Windows bucket by `event_ts` (the booking's actual occurrence), not wall-clock arrival.
Watermark = `max_event_ts - WATERMARK_GRACE_SECONDS`. Windows whose end is before the
watermark are closed and flushed. This correctly handles out-of-order events and network
delay from rural hotel properties.

### Six-gate validation with two quarantine sinks *(original design)*

Five malformed reasons (`unparseable_json`, `missing_field`, `unknown_event_type`,
`unknown_city`, `unparseable_event_ts`) route to `malformed_events/`. Late events route to
`late_events/`. Each gate is explicit. Nothing is silently dropped.

> **Gate order evolved.** The original spec lists six gates in a specific sequence (see
> Step 4). After B-039 the SILVER sink was inserted between Gate 4 and Gate 3 and the
> gate numbering changed. The authoritative current gate order is in
> [CLAUDE.md](../CLAUDE.md) under the `stream_consumer.py` repo-layout row. The original
> six-gate sequence is documented in Step 4 and is preserved as build history.

### `dim_location` is the source of truth for cities

Not `hotel_master`. Cities live in the dim table; hotels reference via `location_id` FK.
Any SQL that pulls cities must use `SELECT DISTINCT city FROM dim_location`.

### Three shutdown paths

SIGINT (interactive), SIGTERM (process supervisor), `--max-runtime` CLI arg (bounded run).
On Windows, always prefer `--max-runtime` — cross-process SIGINT is unreliable.

### Production constants — not dev shortcuts

`WINDOW_SIZE_MINUTES = 60`, `WATERMARK_GRACE_SECONDS = 300`, `FLUSH_CHECK_SECONDS = 10`.
These are locked. Do not change them to smaller values for testing.

---

## STEPS

### Step 1 — Extend `docker/docker-compose.yml` with Kafka, Zookeeper, MinIO

Add three services to the existing Postgres-only compose file:

```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: travellens-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: travellens-kafka
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  minio:
    image: minio/minio:latest
    container_name: travellens-minio
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
```

Add `minio_data:` to the existing `volumes:` section. Then bring up:

```bash
cd docker
docker compose --env-file ../.env up -d
sleep 10
docker compose --env-file ../.env ps
```

Expected: all four containers show `Up`. MinIO web console at `http://localhost:9001`
(credentials `minioadmin` / `minioadmin`).

---

### Step 2 — Write `scripts/init_s3_buckets.py`

Creates the `travellens-data` bucket and four prefixes (`raw/`, `reviews/`, `reference/`,
`processed/`). Idempotent — safe to re-run.

```bash
python scripts/init_s3_buckets.py
```

Verify:

```bash
docker exec travellens-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec travellens-minio mc ls local/travellens-data/
```

Must list four prefixes.

---

### Step 3 — Write `scripts/kafka_event_producer.py` *(original stateless design)*

> **[SUPERSEDED]** — The stateless producer described here was replaced by the B-034A
> calendar replay simulator. The current producer CLI and behaviour are documented in
> [Build History — B-034A](#b-034a--calendar-simulator-phase-a-replay-engine--current-producer)
> and in [CLAUDE.md](../CLAUDE.md) (`kafka_event_producer.py` repo-layout row).
> This step is preserved as the original build record.

Synthetic event generator. Reads `data/booking_events_seed.json`, generates events at a
configurable rate with chaos injection on demand.

**Required CLI flags:**

| Flag | Default | Purpose |
|---|---|---|
| `--rate` | 50 | Events per second |
| `--duration` | 0 | Stop after N seconds (0 = forever) |
| `--malformed-pct` | 0 | % of events to corrupt |
| `--late-pct` | 0 | % of events to delay past watermark |
| `--chaos-seed` | None | Seed for reproducible chaos |

**Required chaos generators (5 — must match consumer reason vocabulary):**

- `_corrupt_missing_field` — drops one of `event_type`, `event_ts`, `city`, `hotel_id`
- `_corrupt_unknown_event_type` — replaces `event_type` with off-vocabulary string
- `_corrupt_unknown_city` — replaces `city` with typo / wrong-language / fictional
- `_corrupt_unparseable_ts` — replaces `event_ts` with non-ISO string
- `_corrupt_unparseable_json` — returns raw broken bytes instead of dict

**Required serializer:**

```python
value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8")
```

Required so unparseable-JSON chaos (bytes) passes through unchanged.

**Required:** print per-reason breakdown at shutdown so it can be compared against the
consumer's summary.

---

### Step 4 — Write `scripts/stream_consumer.py` *(original dual-sink design)*

> **Note:** This step documents the original dual-sink, six-gate consumer. The consumer
> grew to four sinks (B-038 bronze, B-039 silver) and the gate order was revised (B-039).
> Current consumer behaviour: [CLAUDE.md](../CLAUDE.md) hard rules and
> [Build History](#build-history--evolution) below.

Pure-Python consumer with six-gate validation, late-event guard, in-memory windowed state,
and dual sink.

**Required CLI flag:**

| Flag | Default | Purpose |
|---|---|---|
| `--max-runtime` | 0 | Self-exit after N seconds (0 = forever) |

**Required validation gates (original sequence — in order):**

1. JSON parseable → else `unparseable_json`
2. Required fields present (`event_type`, `event_ts`, `hotel_id`, `city`, `revenue_inr`) → else `missing_field`
3. `event_type` in known vocabulary → else `unknown_event_type`
4. `city` in `dim_location` city set → else `unknown_city`
5. `event_ts` parseable as ISO datetime → else `unparseable_event_ts`
6. `event_ts` > watermark → else late event (separate quarantine)

**Required Postgres sink:** UPSERT on `(city, window_start)` via `ON CONFLICT DO UPDATE`. Idempotent.

**Required S3 key pattern:**
`processed/agg/hourly_city_stats/year=YYYY/month=MM/day=DD/hour=HH_{city}.parquet`

**Required run summary on shutdown:** `events_consumed`, `windows_flushed`,
`late_dropped`, `malformed_dropped` with `malformed_by_reason` breakdown.

---

### Step 5 — Parse-check both scripts

```bash
python -c "import ast; ast.parse(open('scripts/stream_consumer.py').read()); print('consumer OK')"
python -c "import ast; ast.parse(open('scripts/kafka_event_producer.py').read()); print('producer OK')"
```

Both must print `OK` before proceeding.

---

### Step 6 — Run the pipeline

Open two terminals:

**Terminal A — consumer (leave running):**

```bash
cd C:\Users\risha\Desktop\Code\repo\travellens
.venv\Scripts\activate
python scripts/stream_consumer.py
```

**Terminal B — producer (runs 5 min then exits):**

```bash
cd C:\Users\risha\Desktop\Code\repo\travellens
.venv\Scripts\activate
python scripts/kafka_event_producer.py --rate 50 --duration 300
```

After the producer finishes (~5 min), wait ~6 minutes for the consumer's watermark + grace
period to elapse, then Ctrl-C the consumer.

You should see at least one log line pair like:

```
✓ Postgres upsert: Goa@2026-...
✓ S3 archive: s3://travellens-data/processed/agg/hourly_city_stats/...
```

---

## ACCEPTANCE TESTS (ORIGINAL)

Run after Step 6. Every command must succeed.

```bash
# 1. All four containers healthy
docker compose --env-file .env -f docker/docker-compose.yml ps

# 2. travellens-data bucket exists with 4 prefixes
docker exec travellens-minio mc ls local/travellens-data/

# 3. agg_hourly_city_stats has rows (expected >= 44)
docker exec travellens-postgres psql -U travellens -d travellens \
  -c "SELECT COUNT(*) FROM agg_hourly_city_stats;"

# 4. S3 has Parquet files under processed/agg/ (expected >= 44)
docker exec travellens-minio mc ls --recursive \
  local/travellens-data/processed/agg/hourly_city_stats/

# 5. Both scripts parse cleanly
python -c "import ast; ast.parse(open('scripts/stream_consumer.py').read()); print('consumer OK')"
python -c "import ast; ast.parse(open('scripts/kafka_event_producer.py').read()); print('producer OK')"

# 6. Consumer constants match production values
findstr /N "WINDOW_SIZE_MINUTES = 60" scripts/stream_consumer.py
findstr /N "WATERMARK_GRACE_SECONDS = 300" scripts/stream_consumer.py

# 7. Consumer uses dim_location (not hotel_master) for cities
findstr /N "DISTINCT city FROM dim_location" scripts/stream_consumer.py
```

**Bonus — chaos validation (run once before declaring phase complete):**

```bash
# Reset state
docker exec travellens-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group travellens-python-consumer \
  --reset-offsets --to-latest --topic booking-events --execute

docker exec travellens-postgres psql -U travellens -d travellens \
  -c "TRUNCATE TABLE agg_hourly_city_stats;"

# Run with chaos
python scripts/stream_consumer.py --max-runtime 420 > consumer_chaos.log 2>&1 &
python scripts/kafka_event_producer.py --rate 50 --duration 300 \
  --malformed-pct 5 --late-pct 2 --chaos-seed 42 > producer_chaos.log 2>&1

# Verify quarantine sinks received events — both must return > 0 files
docker exec travellens-minio mc ls --recursive local/travellens-data/malformed_events/
docker exec travellens-minio mc ls --recursive local/travellens-data/late_events/
```

---

## EXPLORE

Run these queries after the pipeline has processed at least one run.

**psql:**
```bash
docker exec -it travellens-postgres psql -U travellens -d travellens
```

**MinIO UI:** `http://localhost:9001` → username `minioadmin` / password `minioadmin`

---

### E1 — How many windows were flushed?

```sql
SELECT
    COUNT(*)                AS total_windows,
    COUNT(DISTINCT city)    AS cities_seen,
    MIN(window_start)       AS earliest_window,
    MAX(window_start)       AS latest_window
FROM agg_hourly_city_stats;
```

Expected: at least 44 windows (one per city per hour for a 5-min producer run bucketed
into the current hour).

---

### E2 — Revenue and bookings by city

```sql
SELECT
    city,
    SUM(total_bookings)                     AS total_bookings,
    ROUND(SUM(total_revenue_inr) / 1e7, 2) AS revenue_cr,
    ROUND(AVG(avg_occupancy_pct), 2)        AS avg_occupancy_pct,
    ROUND(AVG(cancellation_rate) * 100, 2)  AS avg_cancel_pct
FROM agg_hourly_city_stats
GROUP BY city
ORDER BY revenue_cr DESC;
```

---

### E3 — Window activity over time

Verify event-time bucketing is correct — all windows should fall into the right hours.

```sql
SELECT
    DATE_TRUNC('hour', window_start)    AS hour_bucket,
    COUNT(DISTINCT city)                AS cities_active,
    SUM(total_bookings)                 AS bookings_in_hour
FROM agg_hourly_city_stats
GROUP BY DATE_TRUNC('hour', window_start)
ORDER BY hour_bucket;
```

---

### E4 — Spot check a single city

```sql
SELECT *
FROM agg_hourly_city_stats
WHERE city = 'Goa'
ORDER BY window_start DESC
LIMIT 5;
```

---

### E5 — S3 Parquet file count and structure

How many files landed:

```bash
docker exec travellens-minio mc ls --recursive \
  local/travellens-data/processed/agg/hourly_city_stats/ | wc -l
```

Browse Hive partition structure:

```bash
docker exec travellens-minio mc ls \
  local/travellens-data/processed/agg/hourly_city_stats/
```

Expected: `year=YYYY/month=MM/day=DD/` subdirectories.

---

### E6 — Inspect a Parquet file

Copy a file locally and read it to verify schema:

```bash
docker exec travellens-minio mc cp \
  "local/travellens-data/processed/agg/hourly_city_stats/year=2026/month=05/day=21/hour=21_Goa.parquet" \
  /tmp/sample.parquet
```

```python
import pyarrow.parquet as pq
tbl = pq.read_table('/tmp/sample.parquet')
print(tbl.schema)
print(tbl.to_pandas())
```

---

### E7 — Quarantine sinks (if chaos was run)

```bash
# Malformed events
docker exec travellens-minio mc ls --recursive \
  local/travellens-data/malformed_events/

# Late events
docker exec travellens-minio mc ls --recursive \
  local/travellens-data/late_events/
```

Inspect one quarantine record:

```bash
docker exec travellens-minio mc cat \
  local/travellens-data/malformed_events/<filename>
```

Each file is JSON with `raw_value`, `reason`, `kafka_offset`, `kafka_partition`,
`consumer_ts`.

---

### E8 — Consumer group offset status

Check whether the consumer is caught up:

```bash
docker exec travellens-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group travellens-python-consumer \
  --describe
```

`LAG = 0` means all produced events have been consumed. Non-zero lag is normal during an
active run — should reach 0 after the producer stops and the watermark elapses.

---

## DO NOT

- Do not try to send cross-process SIGINT on Windows — use `--max-runtime` instead
- Do not change `WINDOW_SIZE_MINUTES` or `WATERMARK_GRACE_SECONDS` from production values (60 / 300)
- Do not use `SELECT DISTINCT city FROM hotel_master` — cities live in `dim_location`
- Do not access `state[key]` before the late-event guard fires — `defaultdict` will
  resurrect flushed windows and the next UPSERT overwrites the correct aggregate
- Do not include empty string `""` in chaos lists for `event_ts` or `event_type` — they
  trigger `missing_field` instead of the intended reason
- Do not commit `.bak`, `.log`, or test artifact files to git
- Do not modify Phase 1 files (`schema.sql`, `load_to_postgres.py`, `validate_load.py`)
- Do not modify `data/` or `.env`

---

## ROLLBACK

```bash
# Stop new services — keeps Postgres and Phase 1 data intact
docker compose --env-file ../.env stop kafka zookeeper minio
docker compose --env-file ../.env rm -f kafka zookeeper minio

# Optionally delete MinIO data volume
docker volume rm docker_minio_data

# Remove Phase 2 scripts
rm scripts/stream_consumer.py scripts/kafka_event_producer.py scripts/init_s3_buckets.py

# Revert docker-compose.yml to Phase 1 (Postgres-only) — manual edit

# Truncate the aggregate table
docker exec travellens-postgres psql -U travellens -d travellens \
  -c "TRUNCATE TABLE agg_hourly_city_stats;"
```

To restart: re-run Steps 1–6 from the top.

---

## LESSONS LEARNED

1. **Kafka consumer-group offsets are sticky.** `auto_offset_reset="latest"` only applies
   on first connect. Reset explicitly between test runs — the consumer group must have no
   active members when you reset.

2. **Windows cross-process SIGINT is fragile.** `kill -INT` from Git Bash is a hard kill,
   not SIGINT. Reliable answer: `--max-runtime` (consumer self-terminates).

3. **`defaultdict` resurrects flushed windows.** Accessing `state[key]` for an
   already-flushed window silently creates zero state, and the next UPSERT overwrites the
   correct aggregate. The late-event guard (Gate 6) must fire BEFORE `state[key]` is
   accessed.

4. **Empty strings are ambiguous corruption.** `""` in `event_ts` or `event_type` triggers
   `missing_field` (because `not event.get(f)` is True) rather than the type-specific
   reason. Removed from those chaos generators.

5. **S3 key omits minute.** In dev mode with 2-min windows, three windows in the same hour
   overwrite each other in S3. Production 60-min windows don't have this collision.

6. **Schema introspection is mandatory.** `hotel_master.city` doesn't exist — cities are
   in `dim_location`. Writing SQL from memory caused a startup crash. Always read
   [`../datamodel.md`](../datamodel.md) (at repo root) before writing any SQL against this schema.

7. **Auto-compact loses context mid-task.** Capture test results to log files on disk so
   they survive Claude Code conversation compaction.

---

## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any code
- Create every file in the REPO STATE FILE TREE — modify docker-compose.yml, create 3 new scripts
- MODIFY docker-compose.yml — do not replace it, add the 3 new services to the existing file
- Cities source of truth is `dim_location` — never `hotel_master` in any SQL
- Production constants are LOCKED: `WINDOW_SIZE_MINUTES = 60`, `WATERMARK_GRACE_SECONDS = 300`, `FLUSH_CHECK_SECONDS = 10` — do not change for testing
- On Windows use `--max-runtime` flag for consumer shutdown — never cross-process SIGINT
- Parse-check both scripts before running: `python -c "import ast; ast.parse(open('scripts/stream_consumer.py').read())"`
- Run acceptance tests AND chaos test before declaring done
- Do not modify Phase 1 files: `schema.sql`, `load_to_postgres.py`, `validate_load.py`
- Do not modify `data/` or `.env`

---

## BUILD HISTORY / EVOLUTION

> Changes in chronological order, each under the backlog item that authorized it.
> Superseded steps are labeled **[SUPERSEDED]**. For the current state of any file,
> see [CLAUDE.md](../CLAUDE.md) · [datamodel.md](../datamodel.md) · [backlog.md](backlog.md).

Phase 2 shipped acceptance-complete. The changes below were taken under explicit backlog
items after `scripts/stream_consumer.py` was un-frozen on scoped exceptions. These items
are why `scripts/stream_consumer.py` and `scripts/kafka_event_producer.py` are no longer
in the "frozen — never modify" list. Any further consumer or producer changes still need a
named backlog item.

---

### B-031 — Consumer poll loop + broker-eviction resilience

Resolves L-016. Main loop swapped from the endless `for msg in consumer:` generator to a
bounded `consumer.poll(timeout_ms=1000, max_records=INNER_BATCH_MAX)` so the flush check
and max-runtime check are guaranteed to run each cycle even under saturating load.

Broker eviction defences added (`max_poll_interval_ms=600000`, `session_timeout_ms=30000`,
`heartbeat_interval_ms=10000`). `WINDOW_SIZE_MINUTES` and `WATERMARK_GRACE_SECONDS` are
now env-overridable (defaults UNCHANGED at 60 / 300) so a small-window dev verification
doesn't require editing the source. Production behaviour is byte-for-byte unchanged when
no env vars are set.

---

### B-032 Chunk 2 — Five event types + per-type counts + pipeline_metrics heartbeat

Resolves L-014 (counting half) and L-015.

- **`VALID_EVENT_TYPES` now five:** `BOOKING, CANCELLATION, CHECKIN, CHECKOUT,
  PRICE_CHANGE`. `PROCESSED_EVENT_TYPES` is now **four** — CHECKIN and CHECKOUT join
  BOOKING and CANCELLATION past the Gate 3 silent filter. PRICE_CHANGE is still the only
  valid-but-dropped type.
- **Per-type counts on `agg_hourly_city_stats`:** the consumer now writes
  `total_checkins`, `total_checkouts`, `total_cancellations` (migration 007 columns)
  alongside the original `total_bookings` and the derived `cancellation_rate` (the ratio
  is unchanged — the new `total_cancellations` column is the raw count, kept separate so
  both signals are queryable). `total_reviews` stays NULL — REVIEW is not a stream event
  today (B-030).
- **`pipeline_metrics` heartbeat:** every flush-check tick (`FLUSH_CHECK_SECONDS`,
  default 10s) the consumer writes ONE row to the `pipeline_metrics` table (cumulative
  `events_consumed`, `bookings`, `cancellations`, `malformed`, `late`, plus
  `active_windows = len(state)`, `max_event_ts`, `consumer_lag` reserved NULL). The
  INSERT is wrapped in its OWN try/except — a heartbeat failure logs to stderr and is
  swallowed; it can never crash the loop. Events/sec is derived in the read path as the
  delta between consecutive heartbeat rows, not stored.

---

### B-032 Chunk 3 — CHECKOUT emission (stateless producer) **[SUPERSEDED]**

Added CHECKOUT emission to the stateless producer with weights
`[0.55, 0.18, 0.12, 0.10, 0.05]` for BOOKING/CHECKIN/CHECKOUT/CANCELLATION/PRICE_CHANGE.
Verified: `total_checkouts > 0` on every recent window; 0 malformed.

**[SUPERSEDED]** — These stateless weights were superseded by B-034A (calendar replay
simulator). The producer no longer uses per-tick fixed weights. See B-034A below.

---

### B-034 — Stateful booking-lifecycle simulator (intermediate step) **[SUPERSEDED]**

**[SUPERSEDED]** — Intermediate step that replaced the stateless per-tick draw with an
in-memory open-bookings registry (each tick: start a booking OR advance an open one through
CHECKIN/CHECKOUT/CANCELLATION). Wire event_type strings UNCHANGED; consumer required no
changes.

Superseded by B-034A (calendar replay simulator below), which replaced the synthetic
in-memory registry with deterministic replay of real `fact_bookings`.

---

### B-035 — Lifecycle history backfill (time-partitioned real model)

New migration `db/migrations/008_lifecycle_events.sql` and new script
`scripts/generate_lifecycle_history.py`. Two tables ship:

- **`fact_booking_events`** — silver append-only ledger, one row per lifecycle event
  (BOOKING / CHECKIN / CHECKOUT / CANCELLATION today; PRICE_CHANGE / REVIEW reserved).
  HISTORY and STREAM both write here; a `source` column ('history' | 'stream') is the
  only separator. Indexes on `booking_id`, `event_date`, `hotel_id`, `source`.
- **`sim_open_bookings`** — mutable simulator state. One row per booking awaiting CHECKIN
  (`state='BOOKED'`) or CHECKOUT (`state='CHECKED_IN'`); the producer mutates it on each
  advance.

The generator processes **all of `fact_bookings` in a single sweep** and time-partitions
every row against the `--sim-today` anchor. Each row routes into one of four buckets:

- `booking_ts >= sim-today` → **FUTURE** — skipped; reserved for the stream simulator to
  replay later.
- `booking_ts < sim-today` AND `checkout_date < sim-today` → **COMPLETED** — emit
  `BOOKING + CHECKIN + CHECKOUT` (or `BOOKING + CANCELLATION` if `is_cancelled`).
- `booking_ts < sim-today` AND `checkin_date < sim-today <= checkout_date` →
  **IN_PROGRESS** — emit `BOOKING + CHECKIN` as history, insert `sim_open_bookings`
  state `CHECKED_IN`.
- `booking_ts < sim-today` AND `checkin_date >= sim-today` → **BOOKED** — emit `BOOKING`
  as history, insert `sim_open_bookings` state `BOOKED`.

**The backlog is real.** Every row in `sim_open_bookings` is an actual `fact_bookings`
booking — there is no synthesis, no minted UUIDs, no "seeded fresh open bookings." The
BOOKED / CHECKED_IN split is **derived** from the real shape of in-flight bookings at the
anchor (≈82.5 / 17.5 on the default run), not a fixed ratio.

Default run (`--sim-today 2025-06-01`, ~120s on dev box): **1,735,502 history events**
across **612,380 kept bookings** (1,000,000 seen; 387,620 FUTURE skipped for the stream;
0 dropped by realism guards); 17,188 open backlog rows (14,193 BOOKED + 2,995 CHECKED_IN,
all real fact_bookings IDs); 95.4% hotel coverage, 100% customer coverage; latest history
`event_date` 2025-05-31 — 1 day before sim-today, so there is no continuity void between
history and the simulator's start line. Acceptance **15 / 15 PASS** — including BACKLOG
REAL (0 synthesised IDs), CONTINUITY (gap ≤ 7-day threshold), DOCS (13 / 13 functions
documented), and idempotency (rerun with `--reset` gives identical counts AND preserves
any planted `source='stream'` row).

REVIEW + PRICE_CHANGE history are deliberately NOT generated — REVIEW is the B-030
feature gap; PRICE_CHANGE already lives in `fact_price_events`. Full details in
`docs/backlog.md` (B-035) and the "Lifecycle Events + Simulator State" section in
`datamodel.md`.

> **Build/seed sequence.** `generate_lifecycle_history` runs as **Stage C1** of the canonical post-load sequence — see [`datamodel.md` → Regenerating the Dataset](../datamodel.md#regenerating-the-dataset) for when it runs relative to the base load and the B-046 expansion.

---

### B-034A — Calendar simulator (Phase A: replay engine) — *[SUPERSEDED by B-047]*

> **[SUPERSEDED]** — The calendar-replay model described here was the producer between B-034A landing and B-047 landing. Replaced by [B-047 — Stage 2a forward generator](#b-047--stage-2a-forward-generator-data-aware-diurnal-timed-producer--current-producer) below. The fact-replay model has no future runway once `fact_bookings.booking_ts` exhausts, and cannot model "events happen at realistic times of day." `.sim_clock.json` is now deleted on first launch and never re-created.

The producer is no longer a stateless or stateful per-tick lifecycle simulator. It is a
**calendar-driven REPLAY** of `fact_bookings`. A sim-clock (persisted at
`scripts/.sim_clock.json`, gitignored) advances one logical day at a time. Each sim-day D
the producer:

- drains cancellations scheduled for D (planned the first time each is_cancelled booking
  was seen; the plan is deterministic from `f"{booking_id}|{chaos_seed}"`),
- emits BOOKING events for every `fact_bookings WHERE booking_ts::date = D` (in
  `booking_ts` order), inserting them into `sim_open_bookings` with `source='stream'`,
- emits CHECKIN events for every open BOOKED row with `checkin_date = D`,
- emits CHECKOUT events for every open CHECKED_IN row with `checkout_date = D`,
- emits a small number of stateless PRICE_CHANGE events (no `booking_id`).

Day boundaries are atomic commit points: end of day → Kafka flush → DB commit → write
`.sim_clock.json`. A graceful Ctrl-C lands between days. Outcome (is_cancelled) comes from
`fact_bookings`, NOT a random draw — replay preserves real outcomes.

The five wire `event_type` strings, the Kafka config (`acks="all"`, `linger_ms=20`,
bytes-passthrough `value_serializer`, `key_serializer`), and partition `key=city` are all
UNCHANGED. ONE new ADDITIVE wire field — `event_date` (ISO date — the sim-day) — sits
alongside the wall-clock `event_ts` so the consumer's event-time windowing keeps behaving
the same; the consumer's Gate 2 doesn't require `event_date` and ignores it harmlessly.

**Current CLI:** `--sim-start` (default `2025-06-01`, MUST match the generator),
`--until <sim-date>`, `--sim-speed` (sim-days per real-second, default `0.05` → ~100
evt/s at steady state), `--reset-clock`, `--num-prices-per-day`, plus the unchanged chaos
trio (`--malformed-pct`, `--late-pct`, `--chaos-seed`). `--rate` and `--duration` are
accepted-and-ignored deprecation no-ops.

**Acceptance results (verified end-to-end):**

- No-chaos slice (4 sim-days, sim-speed 0.05, fixed seed): 9,942 events emitted; consumer
  ingested all 9,942 with **0 malformed / 0 late**; 44 windows flushed; per-type agg sums
  match producer wire counts EXACTLY (BOOKING 3,034 / CHECKIN 2,790 / CHECKOUT 3,397 /
  CANCELLATION 701).
- LINKAGE: 0 / 2,529 lifecycle booking_ids absent from `fact_bookings`; 0 bookings have
  both CHECKOUT and CANCELLATION; 0 duplicate `(booking_id, event_type)` pairs.
- OUTCOME FIDELITY: 108 is_cancelled BOOKINGs in capture, 0 of them got CHECKIN or
  CHECKOUT; cancellation reasons split into both `customer_cancelled` and `no_show`.
- FIELDS: 3,402 / 3,402 events carry both `event_ts` AND `event_date`; 0 events have
  `event_ts` outside ±10 min of wall-clock now; 0 / 5 PRICE_CHANGE carry `booking_id`/
  `customer_id`; 0 / 868 BOOKING violate the revenue invariant.
- STATE: `sim_open_bookings` grew by 4,563 stream rows over 7 sim-days while shrinking by
  checkouts/cancellations; final counts reconcile with the emitted event counts exactly.
- RESTART: re-running with the same `--until` after a completed slice emits 0 events; the
  clock resumes at saved+1 from `scripts/.sim_clock.json`.
- CHAOS (`--malformed-pct 5 --late-pct 2 --chaos-seed 42`): producer 229 malformed + 94
  late → consumer's run summary reports the same 229 + 94 with identical per-reason
  splits; `malformed_events/` and `late_events/` S3 prefixes gained exactly those object
  counts.

**REVIEW emission is STILL deferred** — the consumer's `VALID_EVENT_TYPES` does not
include REVIEW. The next two follow-ons are tracked as B-036 (Phase B — net-new synthetic
+ 10–25% long-stay tail) and B-037 (Phase C — REVIEW emission, closes B-030).
*(2026-05-28 follow-up: B-030 absorbed B-037 on the consumer side; B-036 and the residual
producer-side REVIEW work were both obviated by B-047 below.)*

---

### B-038 — Consumer bronze sink (durable raw-event archive)

`scripts/stream_consumer.py` now writes every accepted event to a fourth sink alongside
the agg Postgres UPSERT, the agg S3 Parquet, and the two quarantine prefixes. The fourth
sink — **bronze** — appends each post-Gate-4 event's raw JSON payload to an in-memory
buffer and flushes it as one JSONL file per batch to:

```
s3://travellens-data/raw_events/year=YYYY/month=MM/day=DD/hour=HH/HHMMSS_<uuid8>.jsonl
```

Many events per file (one event per line), INGEST-time partitioning (matches the existing
quarantine convention), `BRONZE_BUFFER_CAP` cap (default 500) OR `FLUSH_CHECK_SECONDS`
periodic tick triggers a flush — whichever first. Final drain on graceful shutdown.

**Why bronze:** the agg UPSERT is lossy (derived totals only); the Parquet archive is
lossy (aggregate rows, not events); the quarantine prefixes only capture failed events.
Bronze fills the gap by archiving the events that SUCCEEDED. Silver (B-039 — parse +
dedupe → typed `fact_booking_events source='stream'`) and gold (B-040 — per-booking
lifecycle reconstruction with `illegal_transition_flag` for the end-to-end ordering proof)
are the downstream payoff.

**What's NOT in bronze:** PRICE_CHANGE (silently filtered at the consumer's Gate 3 before
bronze fires), malformed events (Gates 1+2 → `malformed_events/`), late events (Gate 4 →
`late_events/`). Append-only — raw redeliveries land in bronze as-is; dedup is silver's
job, not bronze's.

**Isolation:** the bronze write is wrapped in its own try/except. A flush failure logs to
stderr, increments `bronze_failures`, drops the batch (best-effort posture: retrying
indefinitely under MinIO outage would leak memory), and continues. The agg UPSERT, the
Parquet sink, the quarantine sinks, the heartbeat, and the window flush all live outside
this try/except and are unaffected.

**Verified end-to-end (7/7 PASS):**

- Clean 3-day slice: bronze 7,453 = accepted (BOOKING 2,177 + CHECKIN 2,094 + CHECKOUT
  2,606 + CANCELLATION 576); events consumed 7,468 = bronze + 15 PRICE_CHANGE filtered
  at Gate 3.
- Chaos 2-day slice (5% malformed + 2% late, seed 42): bronze 4,459 + malformed 228 +
  late 96 + 9 PRICE_CHANGE filtered + 1 PRICE_CHANGE corrupted into malformed = 4,792
  emitted.
- Audit across 11,912 bronze events: 0 missing event_date, 0 missing event_ts, 0
  event_ts outside ±1h of wall-clock, 0 PRICE_CHANGE, 0 missing-required-fields
  signatures.
- Per-type agg sums match bronze type counts exactly (BOOKING 3,692 / CHECKIN 3,390 /
  CHECKOUT 4,019 / CANCELLATION 811).
- 32 JSONL files across the two runs, sizes 6,989 B → 184,530 B (median 174,743 B);
  cap-triggered batches are ~170 KB, tick-triggered batches are smaller. Never
  one-per-event, never one-giant-file.
- Isolation test (`S3_BUCKET=does-not-exist-isolation-test`): 6 bronze flushes failed
  (logged), 0 events durable to bronze, consumer survived — Postgres agg upsert succeeded
  for all 44 windows, pipeline_metrics heartbeat wrote 6 rows, consumer consumed all 2,430
  events and exited cleanly with the bronze gap reported in the shutdown summary.

---

### B-030 / B-030a — REVIEW as a booking-tied stream event

REVIEW became a first-class stream event. The producer emits it at CHECKOUT and
CANCELLATION lifecycle events; the consumer intercepts it after Gate 4 and routes it
to `reviews_raw` via `review_flush()`, bypassing silver/agg/gold entirely.
`scripts/review_generator.py` (pure — no I/O) handles generation; history was seeded
by `scripts/generate_review_backfill.py` (90,980 reviews at ~15% rate).
`scripts/review_stats.py` is the read-only diagnostic. Migration 010 extended
`reviews_raw` with 7 new columns. For current wire format, Gate-2 field set, and
routing rules see [CLAUDE.md](../CLAUDE.md) · [datamodel.md](../datamodel.md) ·
[backlog.md](backlog.md) (B-030 completed entry).

---

### B-047 — Stage 2a forward generator (data-aware diurnal-timed producer) — CURRENT PRODUCER

The producer is **no longer a calendar replay of `fact_bookings`**. It is a **data-aware FORWARD generator** paced by a 24-value IST diurnal rate curve. The calendar-replay model (B-034A above) is retired: `.sim_clock.json` is deleted on first launch and never re-created, `event_date` on the wire is `today in IST` (not a sim-day), and the deprecated CLI flags (`--sim-rate`, `--rate`, `--duration`, `--sim-speed`, `--sim-start`, `--until`, `--reset-clock`) are removed entirely (closes B-041 — the single throughput knob across the system is now `--rate-multiplier`).

**Loop shape.** Token bucket at `effective_rate(h_IST) = BASE_RATE(10) × DIURNAL_HOUR_MULT[h] × rate_multiplier`. Each tick (~200 ms):

1. Pull the soonest-due lifecycle row from `sim_open_bookings` (CHECKIN / CHECKOUT / CANCELLATION / REVIEW). Lifecycle wins priority.
2. Else, pick BOOKING vs PRICE_CHANGE by per-hour weight `prob_b = w_b[h] / (w_b[h] + w_p[h])` — no 95/5 coin flip. Daily aggregate shapes itself ≈ 48/52 BOOKING/PC at x=1.
3. On `events_emitted >= cap` (= 1,000,000 × rate_multiplier), sleep until IST midnight.

**New-BOOKING pipeline (data-aware).** Each new BOOKING samples real entities under realistic constraints: city by `popularity × hotel_count × season/holiday from dim_date`; hotel by `total_rooms × star`, subject to per-hotel-per-night occupancy cap from overlapping `sim_open_bookings`; room type fitting `num_guests`; customer 75 % out-of-state vs `home_state`; lead time 50 % ≤7 d / 35 % 1–8 wk / 15 % 2–8 mo; nights from the empirical `fact_bookings.nights_stayed` mix; price = base × {weekend 1.15, holiday 1.25, monsoon 0.85, winter 1.10} × Gaussian(1.0, 0.05); revenue = nightly × nights (asserted invariant); `booking_source` from the empirical fact_bookings mix (MakeMyTrip 26 %, Direct 22 %, OYO 15 %, Booking.com 13 %, Goibibo 12 %, Walk-in 6 %, Agoda 6 %).

**Lifecycle fire-times.** Each new BOOKING stamps `checkin_fire_ts` and `checkout_fire_ts` from per-event-type IST hour distributions (CHECKOUT 8-13 peak 9-10, CHECKIN 12-21 peak 16-18), and — for the 12 % of bookings the deterministic `Random(booking_id|cancel|daily_seed)` selects — `cancel_fire_ts` from CANCELLATION distribution (9-21 evening-lean) on a date in `[booking_ts, checkin_date]`. Persisted on the row (migration 015) so restart is idempotent.

**Deferred REVIEW.** On CHECKOUT or CANCELLATION emit, if `make_review_event_dict` produces a payload, the row's `review_fire_ts` is sampled from REVIEW_HOUR_DIST (20-23 IST same day, clamped ≥ now+30 min) and state advances to `'REVIEW_PENDING'`. REVIEW fires when wall-clock reaches the stamp; row deleted on REVIEW emit. (The state `'REVIEW_PENDING'` is a new value, added to the `sim_open_bookings_state_chk` CHECK constraint by migration 015.)

**Catch-up rule.** If the producer was off and a `*_fire_ts` is in the past at startup, the event fires as soon as the bucket has capacity, with `event_ts = NOW()` and `event_date = today in IST`. The original stamped time is NEVER written to the wire. This produces realistic "late but present" arrivals — a CHECKIN that should have fired at yesterday 14:00 IST instead fires today at 10:30 IST under the new bucket cap.

**Migrations.** `014_sim_daily_counter.sql` adds the per-IST-day TOTAL-EVENTS guardrail. `015_lifecycle_fire_times.sql` adds the four nullable `*_fire_ts` columns on `sim_open_bookings`, four matching partial indexes, and the relaxed state CHECK constraint.

**Chaos.** Extracted verbatim to `scripts/chaos_injector.py` (new shared module, 118 lines, byte-identical behaviour) so future producers can import the same generators. The five malformed corruptors, the `_make_late` shifter, and the `maybe_corrupt_or_delay` dispatcher are all unchanged.

**Per-tick batched commits.** All `sim_open_bookings` inserts/updates/deletes plus the `sim_daily_counter` increment land in ONE commit per ~200 ms tick (~5 commits/sec at x=1, not per-event). At rate_multiplier = 5 with ~100 events/sec, the producer issues ~5 commits/sec rather than ~100.

**Smoke-test verification (consumer + producer at x=1, ~65 s wall-clock):** events consumed 298 / late dropped 0 / malformed dropped 0; silver inserted 298 rows in 10 flushes with 0 dedup; revenue invariant 0 broken across 38 BOOKINGs; CHECKIN fire-hour histogram all in 12-21 IST with peak hour 17 (n=14); CHECKOUT all in 8-13 IST peak hour 10 (n=21); BOOKING vs PRICE_CHANGE mix at IST 00-01 = 12.7 % matching the picker math `w_b/(w_b+w_p) = 0.04/0.34 ≈ 0.118`; cancel rate 13.1 % (target 12 %). `--rate-multiplier` coerce verified 22/22 cases across CLI + env var (silent fallback to 1 on non-int, ≤ 1, negative, alpha, empty, None, hex; valid ints ≥ 2 pass through).

**Closes:** B-041 (rate-flag cleanup — flags deleted entirely, not retained as accepted-and-ignored). **Supersedes:** B-034A (calendar replay), B-036 (Phase B net-new + long-stay tail — the forward generator mints net-new bookings as its core loop; long-stay tail is a one-line `NIGHTS_MIX` tune). **Folds with B-030:** producer-side REVIEW emission previously deferred to B-037 is now done via deferred `review_fire_ts` scheduling.

---

## NEXT

Phase 3 — `docs/phase-3-embeddings.md`
