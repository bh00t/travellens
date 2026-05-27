# Phase 7 — Pipeline Monitor Dashboard

> **Stack:** Python 3.11 · Flask · Jinja2 · Postgres 16 · boto3 (MinIO) · Airflow REST
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11
> **Files:** `render/server.py` (+ `/monitor` + `/monitor/data` + 5 `_monitor_*` helpers) · `render/templates/monitor.html`
> **Status:** [x] Complete — original B-027 build PLUS the redesign delivered by
> B-029 (in-place auto-refresh) and B-032 (live throughput + per-type lifecycle
> counts). All B-027 acceptance tests pass; redesign verified end-to-end in
> Chunk 4 (live events/sec matches producer rate exactly; in-page numbers
> update in place every ~10s; page renders even with MinIO stopped).

> For per-feature reliability and known caveats, see
> [`capabilities-and-limits.md`](./capabilities-and-limits.md). For the
> migration that unlocked the live-throughput path, see
> [`../db/migrations/007_pipeline_live_metrics.sql`](../db/migrations/007_pipeline_live_metrics.sql).

> **HISTORY DOCUMENT** — This records how Phase 7 was originally built and how it evolved. For current behaviour of `/monitor`, see [CLAUDE.md](../CLAUDE.md) · [backlog.md](backlog.md).

---

## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Files this phase
creates / touches:

- **MODIFY** `render/server.py` (add `/monitor` + `/monitor/data` routes + 5 helpers)
- **CREATE** `render/templates/monitor.html` (Chunk 4 layout)
- **REQUIRED** `db/migrations/007_pipeline_live_metrics.sql` (creates `pipeline_metrics` + agg count cols — applied before the monitor reads anything)
- **DEPENDS ON** `scripts/stream_consumer.py` writing the heartbeat the monitor reads (the consumer was touched under B-031 / B-032 — see [Phase 2 Build History / Evolution](phase-2-streaming.md#build-history--evolution))

---

## OBJECTIVE

Add a `/monitor` page that answers **"is the solution processing data correctly,
right now?"** — distinct from the business-analytics Explorer. The page must:

- show **live throughput** (events/sec) updating in place every ~10s without a
  page reload;
- show **per-event-type lifecycle counts** (BOOKING / CHECKIN / CHECKOUT /
  CANCELLATION) for a filterable date+city window;
- show **quarantine + freshness + Airflow** so an operator can tell at a glance
  whether the pipeline is healthy;
- render even when MinIO, Airflow, or the heartbeat table is unavailable —
  every external dependency is guarded.

End-to-end: `python -m render.server` → `http://localhost:5000/monitor` →
header pulse shows events/sec live while the consumer runs; numbers refresh in
place every ~10s; the dot pill goes grey when the consumer stops.

---

## PREREQUISITES

- [x] Phase 5 accepted — the Flask dashboard runs (`python -m render.server`).
- [x] Phase 2 artifacts exist — `agg_hourly_city_stats` populated; MinIO bucket
  `travellens-data` with prefixes `malformed_events/` and `late_events/`.
- [x] Phase 3 done — `reviews_raw.embedding` column exists.
- [x] **Migration 007 applied** — creates `pipeline_metrics` and adds
  `total_checkins / total_checkouts / total_cancellations / total_reviews` to
  `agg_hourly_city_stats`. Without it, the live pulse never has data to read.
- [x] **Consumer chunk 2 shipped** — `scripts/stream_consumer.py` writes a
  `pipeline_metrics` heartbeat every `FLUSH_CHECK_SECONDS` (~10s) and populates
  the per-type count columns. Without it, the heartbeat never lands and
  per-type tiles stay zero.
- [x] **Producer emits CHECKOUT** — `scripts/kafka_event_producer.py` emits CHECKOUT events
  (B-032 Chunk 3 / B-034A calendar replay). Without it, `total_checkouts` reads 0 forever.
- [x] `.env` contains the S3/MinIO vars (`AWS_ENDPOINT_URL`, etc.) and Airflow
  is reachable at `http://localhost:8080` when its container is up.

If any prerequisite fails, stop. Do not improvise.

---

## ARCHITECTURE DECISIONS (ORIGINAL)

### Two data sources, two roles

The monitor reads **two operational tables, with different semantics**:

| Table | Role | Filtered by date+city? | Refresh cadence |
|---|---|---|---|
| `pipeline_metrics` | **live "now"** — events/sec, alive signal | No — always real-time | Heartbeat ~10s |
| `agg_hourly_city_stats` | **filtered history** — per-type counts for the chosen range | Yes | On window flush |

The live pulse (header) ignores the filter on purpose: it's a real-time vital
sign, not a historical view. The EVENTS section honours the filter because
those are window-aggregate counts that only make sense within a chosen range.

### Default range is TODAY (UTC), FIXED — no fall-back to all-data

When `from` / `to` are absent from the query string, the route sets BOTH to
today (UTC). This is deliberate and locked: a cold system showing zeros for
today is the correct answer, not a "broken filter" failure. The empty-state
banner inside the EVENTS section explains how to start the simulator. The
user can widen the range manually via the filter bar.

(This replaces the original B-027 default of "all stream data", which was
intended to avoid empty-looking pages on synthetic data but stopped making
sense once the live pulse existed and the page polled itself.)

### In-place auto-refresh, not full-page reload

A small JS at the bottom of `monitor.html` polls `/monitor/data` every 10s
(carrying the current page's query string verbatim) and patches the live
pulse, EVENTS counts, and QUARANTINE counts via `textContent`. Scroll
position and filter-form focus are preserved. No external libs.

Health (freshness / Airflow) and the cities list are intentionally NOT polled
— they change rarely and have an obvious "Apply" path via the filter bar.

### Filters are date + city only

These two dimensions exist in BOTH the stream aggregate
(`agg_hourly_city_stats`) and the warehouse. Segment / star / source exist
only in the warehouse — including them created asymmetry, so they were
dropped. Ad-hoc dimension slicing lives in the Explorer.

### Pipeline-processing metrics only — no business content

Revenue, ratings, sentiment trends, top cities — none of these belong on the
monitor. Business answers are the Explorer's job. The Chunk 4 redesign
explicitly **removed the revenue card** that the original B-027 build showed,
because revenue is a business KPI, not a pipeline-health signal.

### SOON placeholders, not fake values

The monitor reserves three tiles — **review**, **embedded**, **sentiment** —
that render an empty-state with a dashed border, a SOON badge, and the
blocking backlog ID. These activate when the upstream data lands:

| Tile | Unblocked by |
|---|---|
| Review (live REVIEW events in stream) | **B-030** |
| Embedded (of reviews in stream, filtered) | **B-030** |
| Sentiment (scored at embed time) | **B-026** |

Never render a fake number to fill the space. The dashed-border treatment is
the design language for "we know this should be here, here's why it isn't".

### Freshness doubles as the consumer-alive signal (and the live pulse adds the second source) [SUPERSEDED]

> [SUPERSEDED] — The STREAM FRESHNESS tile was removed from the HEALTH section (owner decision). See "Owner decision — STREAM FRESHNESS tile removed" in Build History.

The original B-027 design used `MAX(window_start)` as the only "is the
consumer running?" signal. That signal was in the HEALTH section as a
"Data freshness" tile, but the **live pulse** is the faster, second-by-second
answer: a heartbeat is "alive" when its age is < 15s (slightly more than one
`FLUSH_CHECK_SECONDS` tick of slack). When the consumer stops, the dot goes
grey within a tick.

### Guarded dependencies

Every external call — DB query, MinIO list, Airflow `/health` — is wrapped in
try/except with short timeouts. Failures degrade to `—` / "Unreachable" /
"waiting for heartbeat". The page renders even with MinIO stopped (verified).

---

## ROUTES

### `GET /monitor`

Server-rendered HTML page. Query params: `from`, `to` (YYYY-MM-DD), `city`
(optional). When neither date is present, both default to today UTC.

Context passed to the template:

| Key | Source | Filtered? |
|---|---|---|
| `cities` | `_monitor_cities(conn)` — `dim_location.city` | No |
| `live` | `_monitor_live(conn)` — latest 2 rows of `pipeline_metrics` | No |
| `stream` | `_monitor_stream_activity(conn, from, to, city)` | **Yes** |
| `embed` | `_monitor_embeddings(conn)` — `reviews_raw` | No |
| `freshness` [SUPERSEDED] | ~~`_monitor_freshness(conn)` — `MAX(window_start)`~~ — see Build History: "STREAM FRESHNESS tile removed" | — |
| `quarantine` | [SUPERSEDED — B-044] ~~`_monitor_quarantine(from, to)` — MinIO `list_objects_v2` per day-prefix~~ → pure-Postgres `SUM` from `quarantine_hourly_summary`; see Build History: B-044 | **Yes (date only)** |
| `airflow` | `_monitor_airflow()` — HTTP `/health` | No |
| `sel_from`, `sel_to`, `sel_city` | echoed back for the filter form | — |

### `GET /monitor/data`

JSON sidecar for the in-place refresh. Same query-string contract as
`/monitor` (same default-today behaviour). Returns the subset of monitor
state that changes second-to-second:

```json
{
  "live":       { "events_per_sec": 49.6, "alive": true,
                  "latest_ts": "...", "age_secs": 9.3, "has_data": true },
  "stream":     { "bookings": 3383, "checkins": 1033, "checkouts": 691,
                  "cancellations": 598, "windows": 44, "cities": 44 },
  "quarantine": { "reachable": true, "date_scoped": true,
                  "date_from": "2026-05-25", "date_to": "2026-05-25",
                  "malformed": 678, "late": 279 }
}
```

Excluded on purpose: `cities` (schema-rare), `embed` (embedding-job-rare),
`freshness` / `airflow` (server-rendered, re-fetched on full reload).

### Quarantine scoping (B-042) [SUPERSEDED — see Build History: B-044]

> **Current state:** quarantine counts are read exclusively from `quarantine_hourly_summary`
> (pure Postgres, no S3 on the request path). The S3-listing approach below is historical —
> it was the original B-042 implementation, superseded first by B-033 and then definitively
> by B-044. See Build History entries for both.

`_monitor_quarantine(date_from, date_to)` is **date-scoped, city-agnostic**.

- **Date** — for each day in `[date_from, date_to]` the function lists
  `{prefix}/year=YYYY/month=MM/day=DD/` on both `malformed_events/` and
  `late_events/` and sums `KeyCount`. The keys are partitioned by the
  consumer's INGEST wall-clock time (`datetime.now(timezone.utc)` at
  `quarantine_event` write time — see
  `scripts/stream_consumer.py:_monitor_quarantine` design notes), the
  same axis the EVENTS section uses against `agg_hourly_city_stats`'s
  `window_start`. So the two sections stay consistent.
- **City** — the keys carry NO city segment. Malformed events often
  can't be parsed for a city (that's why they're malformed), so
  partitioning by city would silently lose triage data. The function
  refuses to take a city argument; the UI shows a `date-scoped · all
  cities` chip + the explicit "city filter doesn't narrow these"
  caption so the operator isn't confused when they pick a city and
  the quarantine numbers don't move.
- **Fallback** — if either date endpoint is missing (only possible
  via direct calls in tests; the live monitor routes always pass
  dates thanks to the default-today rule), the function falls back
  to the legacy bucket-wide count.
- **Cost** — O(days × 2) `list_objects_v2` calls per render. Each
  day-prefix call is tiny (MinIO indexes per prefix), so a 30-day
  range is 60 cheap calls — well under the page-render budget.
  [Replaced by B-033 (hybrid Postgres+S3) then B-044 (pure Postgres) — see Build History.]

### Live throughput math

```
events_per_sec = (latest.events_consumed - prev.events_consumed)
               / max((latest.metric_ts - prev.metric_ts).total_seconds(), 1)
```

- `max(..., 1)` protects against a zero / negative interval (two heartbeats
  in the same second after clock skew).
- A negative delta — consumer restart reset the counter — is clamped to 0
  rather than reporting a misleading negative rate.
- With one heartbeat only, rate is reported as 0 but `has_data: true` so the
  pulse shows "live · Xs ago" instead of "waiting for heartbeat".

---

## LAYOUT (monitor.html)

```
┌─────────────────────────────────────────────────────────────────────┐
│ Pipeline Monitor                                  ╔══════════════╗  │
│ <one-line sub-caption>                            ║ EVENTS/SEC   ║  │
│                                                   ║   49.6  ●live ║  │
│                                                   ╚══════════════╝  │
├─────────────────────────────────────────────────────────────────────┤
│ [From: today] [To: today] [City: All cities ▾]  [Apply] [Clear]     │
├─────────────────────────────────────────────────────────────────────┤
│ EVENTS                                                              │
│ ┌──────────┐┌──────────┐┌──────────┐┌──────────┐                    │
│ │ Bookings ││ Check-ins││ Check-out││ Cancel(≈)│                    │
│ │  3,383   ││  1,033   ││   691    ││  ≈598    │                    │
│ └──────────┘└──────────┘└──────────┘└──────────┘                    │
│ ┌╌╌╌╌╌╌╌╌╌╌┐┌╌╌╌╌╌╌╌╌╌╌┐┌╌╌╌╌╌╌╌╌╌╌┐┌──────────┐                    │
│ │ Review🔲 ││ Embed 🔲 ││ Sentim 🔲││ Windows  │                    │
│ │    —     ││    —     ││    —     ││    44    │                    │
│ │ B-030    ││ B-030    ││ B-026    ││ 44 cities│                    │
│ └╌╌╌╌╌╌╌╌╌╌┘└╌╌╌╌╌╌╌╌╌╌┘└╌╌╌╌╌╌╌╌╌╌┘└──────────┘                    │
├─────────────────────────────────────────────────────────────────────┤
│ QUARANTINE                                                          │
│ ┌──────────┐┌──────────┐                                            │
│ │ Malformed││  Late    │                                            │
│ │  3,616   ││  1,088   │                                            │
│ └──────────┘└──────────┘                                            │
├─────────────────────────────────────────────────────────────────────┤
│ HEALTH                                                              │
│ ┌──────────┐┌──────────┐                                            │
│ │ Freshness││ Airflow  │                                            │
│ │ [Fresh ✓]││ [Healthy]│                                            │
│ └──────────┘└──────────┘                                            │
└─────────────────────────────────────────────────────────────────────┘
```

Reuses the existing neo-brutalist design language from `base.html`:
`.metric`, `.metric-grid`, `.pill`, `.filter-bar`, `--green / --amber / --red`
tokens, 2px black border + 4px black box-shadow. SOON tiles are the same
`.metric` card with a dashed border, no shadow, and a `badge-soon` chip.

---

## ACCEPTANCE TESTS

Original B-027 acceptance (cold / warm / chaos / filters / dependency-down)
all PASS. Chunk 4 redesign verification table (run with your own eyes):

| TEST | EXPECTED | METHOD |
|---|---|---|
| Cold load | HTTP 200, `from`/`to` form values = today UTC, pulse shows "waiting for heartbeat" | open `/monitor` with no heartbeats in DB |
| Heartbeat → live | `alive=true`, dot pill green, events/sec ≈ producer rate | start consumer + producer, hit `/monitor/data` mid-run |
| Consumer stops → grey | `alive=false`, dot grey, "stale · Xs ago" | stop consumer, re-hit `/monitor/data` |
| Math match | events/sec equals `(Δ events_consumed) / (Δ secs)` between adjacent rows | hand-compute from `SELECT … FROM pipeline_metrics ORDER BY metric_ts DESC LIMIT 5` |
| Filtered counts match psql | EVENTS card values = `SUM(…) WHERE window_start::date = today` | compare `/monitor/data` JSON to a psql aggregate |
| In-place polling | numbers change in DOM every ~10s without scroll loss | DevTools → Network → `/monitor/data` ticks |
| Revenue removed | no `₹`, no `total_revenue_inr`, no revenue card | search the rendered HTML |
| SOON placeholders honest | review/embedded/sentiment cards render `—` with backlog IDs | inspect the EVENTS section |
| MinIO down → still renders | HTTP 200, `qu-*` cards show `—`, no traceback | `docker stop travellens-minio`, hit `/monitor` |

Output "PHASE 7 ACCEPTED" only after every row passes. Do not auto-proceed.

---

## ROLLBACK

If the redesign needs to be reverted (keep B-027's original three-section
monitor — remove the `/monitor/data` route, the `_monitor_live` helper, the
live-pulse header markup, the SOON tiles, and the JS poller; restore the
revenue card), the owner can run:

```bash
git diff main -- render/server.py render/templates/monitor.html | git apply -R
```

Do not run this from an agent session — it is an owner-driven recovery step.

The Phase 2 / migration changes that the redesign depends on
(`pipeline_metrics` table, per-type count columns on `agg_hourly_city_stats`,
consumer heartbeat) are append-only and SHOULD NOT be rolled back — they're
correct on their own and have other consumers (future Airflow DAGs, ad-hoc
queries).

---

## DEFERRED (backlog, not this phase)

- **B-026 sentiment** → unblocks the "Sentiment" SOON tile in EVENTS.
- **B-030 reviews-in-stream** → unblocks the "Review" and "Embedded" SOON
  tiles (REVIEW becomes a stream event; embedding-at-ingest follows).
- **Consumer lag from Kafka** → `pipeline_metrics.consumer_lag` is reserved
  NULL; wiring it to AdminClient is a small follow-on but not on this phase.

---

## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any code.
- Confirm the real schema FIRST:
  `docker exec travellens-postgres psql -U travellens -d travellens -c "\d pipeline_metrics"`
  and `"\d agg_hourly_city_stats"`. The route uses `_column_exists` to handle
  schema drift gracefully — the real schema wins.
- MODIFY `render/server.py` — add the `/monitor` and `/monitor/data` routes
  plus the five `_monitor_*` helpers; do not rewrite existing routes.
- Render `monitor.html` using the existing design tokens from `base.html` —
  do NOT import a different style.
- Guard every external dependency (Airflow, MinIO, `pipeline_metrics`) with
  a short timeout — the page MUST render even when a dependency is down.
- The live pulse READS `pipeline_metrics` directly. Do NOT proxy this through
  the AI layer — that is the deliberate exception called out in
  `render/server.py`'s module docstring.
- Pipeline-processing metrics ONLY — no business content. If you find
  yourself adding revenue / ratings / sentiment trends, you are on the wrong
  page; it belongs in the Explorer.
- Default `from` and `to` to today UTC when absent — do NOT fall back to "all
  data".
- Do not run any git commands — the user commits after acceptance.

---

## BUILD HISTORY / EVOLUTION

Changes to the Phase 7 monitor after the original B-027 acceptance sign-off. Earliest first.

---

### B-027 — Base monitor build

Original `/monitor` route in `render/server.py` + `render/templates/monitor.html`. Three fixed sections (EVENTS, QUARANTINE, HEALTH). Data served from `agg_hourly_city_stats` and MinIO bucket counts. No live pulse, no auto-refresh — static load on each page visit.

---

### B-029 — In-place auto-refresh

Added the `/monitor/data` JSON sidecar route. JavaScript poller hits `/monitor/data` every ~10s and updates page numbers in place without a full reload. Added the live-pulse header: green dot + events/sec derived from adjacent `pipeline_metrics` rows. Dot goes grey when the consumer stops (no heartbeat in the last 30s).

**Why:** A static-load monitor cannot answer "is the pipeline running right now?" A 10s in-place refresh gives near-live visibility without a full page cycle.

---

### B-032 Chunk 4 — Live-throughput redesign

Full redesign of `monitor.html`: default-today filter, per-event-type lifecycle counts (BOOKING / CHECKIN / CHECKOUT / CANCELLATION), SOON placeholder tiles (Sentiment, Review, Embedded — honest about what is not built yet), revenue removed from pipeline metrics (business content belongs in Explorer). Live events/sec matches producer rate exactly; page renders even with MinIO stopped.

**Files:** `render/server.py` (five `_monitor_*` helpers, guard on every external dependency), `render/templates/monitor.html` (Chunk 4 layout). Migration 007 is the schema anchor.

---

### B-030b — Review + Embedded tiles lit; sidecar extended

Replaced the two SOON placeholders (Review, Embedded) with live metric cards:

- **Review tile** (`id="ev-reviews"`, `id="ev-reviews-note"`): total review count with seed/history/stream breakdown. Sourced from new `_monitor_reviews(conn)` helper in `render/server.py` — single `COUNT(*) FILTER` query over `reviews_raw.record_source`.
- **Embedded tile** (`id="ev-embedded"`, `id="ev-embedded-note"`, `id="ev-embedded-bar"`): embedding coverage percentage with a CSS progress bar. Denominator fixed to `COUNT(*) FILTER (WHERE review_text IS NOT NULL)` (embeddable rows only, not total) so coverage can actually reach 100%. Sourced from fixed `_monitor_embeddings(conn)`. Color: `ok` ≥ 99%, `warn` > 0%, `idle` = 0%.
- **Sentiment tile** stays SOON (blocked by B-026).
- `/monitor/data` JSON sidecar now includes `reviews` and `embed` keys. JS poller extended with `updateReviews()` + `updateEmbed()` functions called on each 10s tick.

**Why:** B-030/B-030a wrote ~91K booking-tied reviews; the tiles had been SOON placeholders since B-032 Chunk 4. B-030b ships the embedder that fills `embedding`, so the coverage metric is now meaningful in real time.

---

### B-030b follow-up — Review/Embedded date-scoping + Freshness signal fix

Two correctness fixes applied after initial B-030b landing:

**1. Review + Embedded tiles date-scoped.**
Both helpers (`_monitor_reviews`, `_monitor_embeddings`) now accept `date_from` / `date_to` and filter by `reviews_raw.event_date`. Seed reviews (`record_source='seed'`) have `NULL event_date` — they are excluded (static Kaggle corpus, not date-stamped events); only history + stream reviews participate. Both the `/monitor` route and the `/monitor/data` sidecar pass the active date range to both helpers. Effect: picking today shows only today's reviews/coverage (climbs as `review_embedder` embeds stream arrivals); picking yesterday shows a fixed number at ~100% coverage. `badge-scope` chip ("date-scoped · all cities") added to both tile labels. JS `updateReviews()` updated to use `in_range` (not `total`); note drops the `seed` line. `/monitor/data` response shape change: `reviews` object now has `in_range / history / stream / date_scoped` (no longer `total / seed / history / stream`).

**2. Stream Freshness consumer-alive verdict moved to the heartbeat.**
Previously the freshness tile derived "is the consumer running?" from `MAX(window_start)` age — causing a contradiction where the header showed "live · Xs ago" with events/sec > 0 while the tile simultaneously said "Stale — is the consumer running?". Fix: `_monitor_freshness` is now informational context only (window age / pill). A separate `<span id="freshness-consumer-note">` is driven by `live.alive` (the heartbeat signal) both on server render and on each 10s JS tick (`updateFreshness(data.live)` added to the poller). The pill (Fresh / Aging / Stale) still reflects window age — it remains a useful diagnostic about when the last window closed, distinct from whether the consumer process is up.

---

### Owner decision — STREAM FRESHNESS tile removed from HEALTH

Removed the "Data freshness" metric card from the HEALTH section.

**Why:** The live pulse (header, `pipeline_metrics` heartbeat) provides second-by-second consumer-alive signal. The Windows flushed tile (EVENTS section) shows window throughput. The `run.py` singleton guard (TCP port 47219 mutex) prevents the consumer-stall failure mode the freshness tile was designed to catch. With those two signals and the guard in place, the freshness card is redundant.

**Changes:**
- `render/templates/monitor.html`: Data freshness tile removed from HEALTH `metric-grid`; `updateFreshness()` JS function removed; HEALTH section now shows Airflow scheduler only.
- `render/server.py`: `_monitor_freshness()` helper deleted; `MONITOR_FRESH_MINUTES` / `MONITOR_STALE_MINUTES` constants deleted; `freshness` variable removed from `monitor()` and `monitor_data()` routes; `"freshness"` key removed from `/monitor/data` JSON response.

---

### B-033 — Quarantine read-path: O(1) Postgres SUM replaces whole-bucket S3 scan

Replaced the multi-day S3 listing in `_monitor_quarantine()` with a hybrid read path backed by the new `quarantine_daily_summary` table (migration 012, populated by the `quarantine_daily_rollup` Airflow DAG).

**Why:** The previous implementation called `list_objects_v2` for every day in the filter range on every `/monitor` page load and every `/monitor/data` poll. With a 30-day range that was 60 paginated S3 calls per request — slow and unbounded as quarantine grows. The new path does a single indexed `SUM` over past days, reducing the per-request cost to O(1) regardless of range width.

**Changes — `render/server.py`:**
- Renamed old `_monitor_quarantine(date_from, date_to)` → `_monitor_quarantine_s3_scan(date_from, date_to)` (preserved as graceful-degradation fallback).
- Added `_monitor_quarantine(conn, date_from, date_to)` — hybrid read path:
  - Checks `information_schema` for `quarantine_daily_summary`; falls back to `_monitor_quarantine_s3_scan` with a warning log if migration 012 is absent.
  - Past days `[d0 .. min(d1, yesterday)]` → `COALESCE(SUM(...), 0)` from `quarantine_daily_summary` (indexed on PK `summary_date`).
  - Today (only if today ∈ range) → single `list_objects_v2` call for today's day-prefix on each of `malformed_events/` and `late_events/`.
  - No whole-bucket scan anywhere on the request path.
- Both `monitor()` and `monitor_data()` route handlers: moved `quarantine = _monitor_quarantine(conn, ...)` inside the `try` block (connection reused; `conn.close()` in the existing `finally` covers it). Old out-of-block calls removed.

**New files:** `db/migrations/012_quarantine_daily_summary.sql`, `airflow/dags/quarantine_daily_rollup.py`. See phase-6-airflow.md BUILD HISTORY for the DAG narrative.

---

### B-044 — Pure-Postgres quarantine read-path (hourly rollup proc)

Supersedes B-033's hybrid read-path (daily DAG + S3-today fallback). The monitor now reads quarantine counts exclusively from `quarantine_hourly_summary` — no S3 listing on the request path at all.

**Why:** The B-033 hybrid still called `list_objects_v2` for today's counts on every poll. With ~200K late-event objects landing in a single day the per-request list call was slow and growing. The hourly rollup proc (B-044, `scripts/quarantine_hourly_rollup.py`) runs in the background every 5 minutes and keeps the Postgres table current; the monitor just reads it.

**Changes — `render/server.py`:**
- Removed `import boto3`, `from botocore.config import Config`.
- Removed `MONITOR_S3_BUCKET`, `MONITOR_PREFIX_MALFORMED`, `MONITOR_PREFIX_LATE` constants.
- Removed `_monitor_quarantine_s3_scan()` (the B-033 S3 scan fallback) and the B-033 hybrid `_monitor_quarantine(conn, ...)`.
- New `_monitor_quarantine(conn, date_from, date_to)` — pure Postgres only:
  ```sql
  SELECT COALESCE(SUM(malformed_count), 0), COALESCE(SUM(late_count), 0)
  FROM quarantine_hourly_summary WHERE summary_date BETWEEN %s AND %s
  ```
  O(1) regardless of date range width or quarantine volume. Returns `{reachable, date_scoped, date_from, date_to, malformed, late}` — JSON shape unchanged; `updateQuarantine()` JS unchanged.

**Changes — `render/templates/monitor.html`:**
- Malformed tile note: "Objects under `malformed_events/`. ≈ refreshed every 5 min."
- Late tile note: "Objects under `late_events/`. ≈ refreshed every 5 min."
- Error state: "Hourly rollup unavailable." (was "MinIO unreachable.")

**New files:** `db/migrations/013_quarantine_hourly_summary.sql`, `scripts/quarantine_hourly_rollup.py`. See phase-6-airflow.md BUILD HISTORY for the proc design.

---

## NEXT

Phase 6 — `docs/phase-6-airflow.md`. Airflow infrastructure is up, but the
five DAGs (B-024, B-013, B-014, B-015, B-016) are not built — that is the
next build work. Order: migrations first, then **B-024**
`refresh_pinned_widgets` (smallest scope, output already consumed by the
dashboard, fastest validation loop), then B-013 / B-014 / B-015 / B-016.
