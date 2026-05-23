# Phase 7 — Pipeline Monitor Dashboard

> **Stack:** Python 3.11 · Flask · Jinja2 · Chart.js · Postgres 16 · boto3 (MinIO) · Airflow REST  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Files:** `render/server.py` (+route) · `render/templates/monitor.html` (new)  
> **Status:** [x] In progress / [x] Complete  *(acceptance: 1a/1b/1d/1e/2/4/5 hard PASS,
3 PASS via chaos deltas, 1c surrogate PASS on a non-empty dev DB, 2 verified by warm-load
+ code inspection — incrementing is the same query re-run)*

> For the full design rationale and data constraints behind this phase, see
> [`../monitor_implementation_context.md`](../monitor_implementation_context.md).
> For per-feature reliability, see [`capabilities-and-limits.md`](./capabilities-and-limits.md).

---

## REPO STATE AFTER THIS PHASE

```
travellens/
├── render/
│   ├── server.py                ← MODIFY (add /monitor route + helpers; do not rewrite existing routes)
│   └── templates/
│       ├── base.html            ← LEAVE ALONE (extend it)
│       ├── dashboard.html       ← LEAVE ALONE
│       └── monitor.html         ← CREATE
├── docs/
│   └── phase-7-monitor.md       ← LEAVE ALONE (this file)
└── .env                         ← LEAVE ALONE (already has S3/MinIO + Airflow vars)
```

Do not modify any Phase 2 file (`scripts/stream_consumer.py`, `kafka_event_producer.py`) —
they are frozen. This phase only reads what they already produce.

## OBJECTIVE

Add a `/monitor` route serving an operational dashboard that answers "is the solution
processing data correctly?" — stream activity, review-embedding coverage, and pipeline
health (Airflow, quarantine, freshness). Business analytics stay in the Explorer; this is
purely a pipeline monitor. The page must render even when monitored dependencies are down.

End-to-end: `python -m render.server` → open `http://localhost:5000/monitor` → see live
pipeline state, filterable by date + city.

---

## PREREQUISITES

- [ ] Phase 5 accepted — the Flask dashboard runs (`python -m render.server`, pages load)
- [ ] Phase 2 artifacts exist — `agg_hourly_city_stats` table present; MinIO bucket
  `travellens-data` with prefixes `malformed_events/` and `late_events/`
- [ ] Phase 3 done — `reviews_raw.embedding` column exists
- [ ] `.env` contains the S3/MinIO vars (`AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, bucket `travellens-data`) and Airflow is reachable at
  `http://localhost:8080` when its container is up
- [ ] Confirm real column names before coding:
  `docker exec travellens-postgres psql -U travellens -d travellens -c "\d agg_hourly_city_stats"`
  and `"\d reviews_raw"`. The SQL below GUESSES names (bookings, cancellations, revenue,
  window_start, city, embedding) — the real schema wins.

If any prerequisite fails, stop. Do not improvise.

---

## DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| `/monitor` route + helpers | `render/server.py` | Route renders all 3 sections, guarded against dependency-down |
| `monitor.html` | `render/templates/` | Extends base; filter bar + 3 sections; honest empty-states |
| "Monitor" nav link | base/nav | Appears alongside existing nav links |

Do not add business-analytics panels (revenue trends, top cities, rating, sentiment).

---

## ARCHITECTURE DECISIONS (LOCKED)

Do not re-litigate during STEPS.

### Filters are date + city only
Those two dimensions exist in BOTH the stream (`agg_hourly_city_stats`, keyed by city +
window) and the warehouse. Segment/star/source exist only in the warehouse — including them
created asymmetry, so they were dropped. Ad-hoc dimension slicing lives in the Explorer.

### Three sections, one filter bar
1. Stream activity — `agg_hourly_city_stats`, date+city filtered.
2. Review embeddings — `reviews_raw`, NOT filtered (reviews are static, no time/city).
3. Pipeline health — current state (Airflow, quarantine, freshness), NOT filtered.

### Pipeline-processing metrics only
Every metric reflects whether data is being processed correctly. No business content
(ratings, sentiment, revenue trends) — that is the Explorer's job.

### Sentiment is deferred to B-026
No sentiment data exists. Rating is a proven-bad proxy (L-012). Section 2 shows embedding
PROCESSING status only, with a caption noting sentiment is pending B-026.

### Freshness doubles as the consumer-alive signal
There is no clean way for Flask to know a host process is alive. Instead: latest
`window_start` recent ⇒ consumer running. No separate "consumer status" probe.

### Default range is all-available stream data, not "today"
Synthetic data may have nothing dated today; a "today" default would load empty and look
broken. Default to all stream data; the user narrows with the filter.

### Guarded dependencies
Airflow HTTP and MinIO listing are wrapped in try/except with short timeouts. The page
renders even if Airflow or MinIO is down (show "Unreachable" / "—"). Never crash.

---

## STEPS

### Step 1 — Read the real schema and base template
Run the `\d` commands from PREREQUISITES. Read `render/server.py` (DB helper, route
pattern, env loading) and the base template the dashboard extends. Match these patterns.

### Step 2 — Add the `/monitor` route to `render/server.py`
- `GET /monitor`, optional params `from`, `to`, `city` (empty = all).
- Reuse the existing DB-connection helper.
- Gather: stream-activity row, embeddings row, freshness timestamp, quarantine counts
  (MinIO), Airflow health (HTTP). Each external call guarded.
- Render `monitor.html`. (SQL and helper snippets in
  `../monitor_implementation_context.md` §6.)

### Step 3 — Create `render/templates/monitor.html`
- Extend the same base as `dashboard.html`; reuse its stat-card markup and Chart.js include.
- Filter bar (GET form): From, To, City `<select>` (populated from `dim_location`), Apply.
- Three sections in order: Stream activity (4 cards), Review embeddings (4 cards),
  Pipeline health (4 cards). Per-section captions stating what is/isn't filtered.
- Empty-states per §5 of the context doc.

### Step 4 — Add the nav link
Add "Monitor" to the existing nav so `/monitor` is reachable from the other pages.

### Step 5 — Smoke test cold, then warm (via run.py)
Bring the page up with nothing running (cold), then with `python run.py` (warm). See
ACCEPTANCE TESTS.

Do not run any git commands — the user commits after acceptance.

---

## ACCEPTANCE TESTS

Run with your own eyes; report ✓/✗ for each.

1. **Cold load** — nothing running. `/monitor` renders, no crash. Stream cards = 0 with
   "is the consumer running?" note. Airflow card shows "Unreachable" (amber), not a stack
   trace.
2. **Warm load** — `python run.py` up. Reopen `/monitor`: stream activity non-zero and
   climbing on refresh; freshness "Fresh" (green); Airflow "Healthy"; embeddings show real
   counts (e.g. 30000 / 30000 / 100%).
3. **Chaos** — run the simulator with `--malformed-pct` / `--late-pct` (or `run.py --chaos`).
   Quarantine cards become non-zero; refresh confirms.
4. **Filters** — set a date range + a city, Apply: stream activity narrows. Set a range
   with no stream data → stream = 0 with the note. Embeddings + health unchanged by filters
   (correct).
5. **Dependency-down** — stop only the Airflow container; reload `/monitor`. Page still
   renders; Airflow card shows "Unreachable". (Proves the guards work.)

Output "PHASE 7 ACCEPTED" only after all 5 pass. Do not auto-proceed.

---

## ROLLBACK

```bash
# Remove the new template and revert the route additions in server.py.
rm -f render/templates/monitor.html
# In render/server.py: delete the /monitor route, its helper functions, and the nav link.
# No DB or schema changes were made — nothing else to undo.
```

---

## DEFERRED (backlog, not this phase)
- **B-026 sentiment** → adds a sentiment panel here once `reviews_raw.sentiment_label` +
  `sentiment_score` exist (3-class + score, CardiffNLP RoBERTa, embed-time + 30K backfill,
  prototype-first, needs `huggingface.co` allowlisted).
- **CHECKIN/CHECKOUT counts** → needs consumer instrumentation (frozen Phase-2 file).
- **Live events/sec throughput** → needs a metrics table the consumer writes to.
