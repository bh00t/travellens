# Phase 2 — Streaming Pipeline

> **Stack:** Python 3.11 · Kafka · MinIO (S3) · Postgres 16 · Docker  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Scripts:** `scripts/kafka_event_producer.py` · `scripts/stream_consumer.py`  
> **Status:** [ ] In progress / [x] Complete  

---


## REPO STATE AFTER THIS PHASE

```
travellens/
├── docker/
│   └── docker-compose.yml       ← MODIFY (add Kafka, Zookeeper, MinIO services)
├── scripts/
│   ├── init_s3_buckets.py       ← CREATE
│   ├── kafka_event_producer.py  ← CREATE
│   └── stream_consumer.py       ← CREATE
├── db/
│   └── schema.sql               ← LEAVE ALONE
├── data/                        ← LEAVE ALONE
├── docs/
│   └── phase-2-streaming.md     ← LEAVE ALONE
└── .env                         ← LEAVE ALONE (add Kafka + S3 vars manually)
```

---

## POST-ACCEPTANCE HARDENING (logged after Phase 2 shipped)

Phase 2 is acceptance-complete. The two changes below were taken under
explicit backlog items (B-031, B-032) after `scripts/stream_consumer.py`
was un-frozen on a scoped exception. Steps / ARCHITECTURE DECISIONS / DO
NOT below still describe the consumer's **original** shape; the bullets
here describe what is actually running now. The backlog is the
authoritative status — see `docs/backlog.md` (B-031, B-032, L-014, L-015,
L-016).

- **B-031 (resolves L-016):** main loop swapped from the endless
  `for msg in consumer:` generator to a bounded
  `consumer.poll(timeout_ms=1000, max_records=INNER_BATCH_MAX)` so the
  flush check and max-runtime check are guaranteed to run each cycle
  even under saturating load. Broker eviction defences added
  (`max_poll_interval_ms=600000`, `session_timeout_ms=30000`,
  `heartbeat_interval_ms=10000`). `WINDOW_SIZE_MINUTES` and
  `WATERMARK_GRACE_SECONDS` are now env-overridable (defaults UNCHANGED
  at 60 / 300) so a small-window dev verification doesn't require
  editing the source. Production behaviour is byte-for-byte unchanged
  when no env vars are set.

- **B-032 Chunk 2 (resolves L-014 counting half and resolves L-015):**
    - **`VALID_EVENT_TYPES` now five:** `BOOKING, CANCELLATION,
      CHECKIN, CHECKOUT, PRICE_CHANGE`. `PROCESSED_EVENT_TYPES` is now
      **four** — CHECKIN and CHECKOUT join BOOKING and CANCELLATION
      past the Gate 3 silent filter. PRICE_CHANGE is still the only
      valid-but-dropped type.
    - **Per-type counts on `agg_hourly_city_stats`:** the consumer now
      writes `total_checkins`, `total_checkouts`, `total_cancellations`
      (migration 007 columns) alongside the original `total_bookings`
      and the derived `cancellation_rate` (the ratio is unchanged — the
      new `total_cancellations` column is the raw count, kept separate
      so both signals are queryable). `total_reviews` stays NULL —
      REVIEW is not a stream event today (B-030).
    - **`pipeline_metrics` heartbeat:** every flush-check tick
      (`FLUSH_CHECK_SECONDS`, default 10s) the consumer writes ONE row
      to the `pipeline_metrics` table (cumulative `events_consumed`,
      `bookings`, `cancellations`, `malformed`, `late`, plus
      `active_windows = len(state)`, `max_event_ts`, `consumer_lag`
      reserved NULL). The INSERT is wrapped in its OWN try/except — a
      heartbeat failure logs to stderr and is swallowed; it can never
      crash the loop. Events/sec is derived in the read path as the
      delta between consecutive heartbeat rows, not stored.

- **B-032 Chunk 3 (producer — completes the CHECKOUT lifecycle):**
  `scripts/kafka_event_producer.py` now emits `CHECKOUT` events. The
  `EVENT_TYPES` array is `["BOOKING", "CHECKIN", "CHECKOUT",
  "CANCELLATION", "PRICE_CHANGE"]` with weights `[0.55, 0.18, 0.12,
  0.10, 0.05]` (sums to exactly 1.0). `make_event` carries only the
  base envelope (`event_id, event_type, hotel_id, city, event_ts`)
  for CHECKOUT — same shape as CHECKIN/CANCELLATION, no extra
  type-specific fields. The full counted lifecycle is now
  `BOOKING → CHECKIN → CHECKOUT → CANCELLATION`; `PRICE_CHANGE` is
  still emitted but still silently dropped at consumer Gate 3.
  Verified with a 120s @ 50 evt/s run: `total_checkouts > 0` on every
  recent window; consumer reports `Malformed dropped: 0` (CHECKOUT is
  accepted, not quarantined as unknown_event_type — proves the Chunk 2
  allow-list ordering was right); aggregate mix across the run matches
  the design weights within ~1pp.

These items are why `scripts/stream_consumer.py` and
`scripts/kafka_event_producer.py` are no longer in the "frozen — never
modify" list. Any further consumer or producer changes still need a
named backlog item.

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

## ARCHITECTURE DECISIONS (LOCKED)

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

### Six-gate validation with two quarantine sinks

Five malformed reasons (`unparseable_json`, `missing_field`, `unknown_event_type`,
`unknown_city`, `unparseable_event_ts`) route to `malformed_events/`. Late events route to
`late_events/`. Each gate is explicit. Nothing is silently dropped.

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

### Step 3 — Write `scripts/kafka_event_producer.py`

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

### Step 4 — Write `scripts/stream_consumer.py`

Pure-Python consumer with six-gate validation, late-event guard, in-memory windowed state,
and dual sink.

**Required CLI flag:**

| Flag | Default | Purpose |
|---|---|---|
| `--max-runtime` | 0 | Self-exit after N seconds (0 = forever) |

**Required validation gates (in order):**

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

## ACCEPTANCE TESTS

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

Run these queries after the pipeline has processed at least one run. Use whichever tool
you prefer:

**DuckDB UI (`duckdb -ui`):**

Run this once in the first cell. Keep the cell context set to `memory` (top-right dropdown):

```sql
INSTALL postgres;
LOAD postgres;
ATTACH 'host=localhost port=5432 dbname=travellens user=travellens password=yourpassword'
    AS travellens_postgres (TYPE postgres);
```

Then for every subsequent query cell — add `USE travellens_postgres.public;` as the
**first line of that cell**, then write your query below it:

```sql
USE travellens_postgres.public;
SELECT ... FROM agg_hourly_city_stats;
```

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


## NEXT

Phase 3 — `docs/phase-3-embeddings.md`
