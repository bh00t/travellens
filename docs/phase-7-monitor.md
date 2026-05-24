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

---

## REPO STATE AFTER THIS PHASE

```
travellens/
├── render/
│   ├── server.py                ← MODIFY (/monitor + /monitor/data routes + 5 helpers)
│   └── templates/
│       ├── base.html            ← LEAVE ALONE
│       ├── dashboard.html       ← LEAVE ALONE
│       └── monitor.html         ← CREATE (Chunk 4 layout)
├── db/migrations/
│   └── 007_pipeline_live_metrics.sql  ← REQUIRED (creates pipeline_metrics + agg count cols)
├── scripts/
│   └── stream_consumer.py       ← writes the heartbeat the monitor reads
│                                  (touched under B-031 / B-032, see Phase 2 post-acceptance note)
├── docs/
│   └── phase-7-monitor.md       ← LEAVE ALONE (this file)
└── .env                         ← LEAVE ALONE (already has S3/MinIO + Airflow vars)
```

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
- [x] **Producer chunk 3 shipped** — `scripts/kafka_event_producer.py` emits
  CHECKOUT at design weight 0.12 (along with the other four types). Without
  it, `total_checkouts` reads 0 forever.
- [x] `.env` contains the S3/MinIO vars (`AWS_ENDPOINT_URL`, etc.) and Airflow
  is reachable at `http://localhost:8080` when its container is up.

If any prerequisite fails, stop. Do not improvise.

---

## ARCHITECTURE DECISIONS (LOCKED)

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

### Freshness doubles as the consumer-alive signal (and the live pulse adds the second source)

The original B-027 design used `MAX(window_start)` as the only "is the
consumer running?" signal. That signal is still here (HEALTH section), but
the **live pulse** is the faster, second-by-second answer: a heartbeat is
"alive" when its age is < 15s (slightly more than one `FLUSH_CHECK_SECONDS`
tick of slack). When the consumer stops, the dot goes grey within a tick.

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
| `freshness` | `_monitor_freshness(conn)` — `MAX(window_start)` | No |
| `quarantine` | `_monitor_quarantine()` — MinIO `list_objects_v2` | No |
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
  "quarantine": { "reachable": true, "malformed": 3616, "late": 1088 }
}
```

Excluded on purpose: `cities` (schema-rare), `embed` (embedding-job-rare),
`freshness` / `airflow` (server-rendered, re-fetched on full reload).

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

```bash
# Revert the redesign — keep B-027's original three-section monitor.
# Remove the /monitor/data route, the _monitor_live helper, the live-pulse
# header markup, the SOON tiles, and the JS poller. Restore the revenue card.
git diff main -- render/server.py render/templates/monitor.html | git apply -R
```

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

## NEXT

Phase 6 — `docs/phase-6-airflow.md`. Airflow infrastructure is up, but the
five DAGs (B-024, B-013, B-014, B-015, B-016) are not built — that is the
next build work. Order: migrations first, then **B-024**
`refresh_pinned_widgets` (smallest scope, output already consumed by the
dashboard, fastest validation loop), then B-013 / B-014 / B-015 / B-016.
