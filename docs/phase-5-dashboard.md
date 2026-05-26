# Phase 5 — Dashboard: Flask Server + Grafana-style UI

> **Stack:** Python 3.11 · Flask · Jinja2 · Chart.js · Postgres 16 · Docker  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Entry point:** `python -m render.server` → `http://localhost:5000`  
> **Status:** [x] Complete  

> **HISTORY DOCUMENT** — This records how Phase 5 was originally built and how it evolved. For current behaviour of the Flask dashboard, see [CLAUDE.md](../CLAUDE.md) · [datamodel.md](../datamodel.md) · [backlog.md](backlog.md).

---

## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Files this phase
creates / touches:

- **CREATE** `render/__init__.py` (empty)
- **CREATE** `render/server.py`
- **CREATE** `render/widget_renderer.py`
- **CREATE** `render/templates/base.html` (shared nav + layout)
- **CREATE** `render/templates/dashboard.html`
- **CREATE** `render/templates/explore.html`
- **CREATE** `render/templates/about.html`
- **CREATE** `db/migrations/003_dashboard_widgets.sql`
- **MODIFY** `requirements.txt` (add `flask==3.0.3`)
- Phase-5 polish later added migrations 004 (widget settings) and 005 (B-022 frozen SQL + cache) — see [`datamodel.md`](../datamodel.md) Schema Evolution.

Build a three-page Flask web application that wraps the Phase 4 AI layer into a
Grafana-style intelligence dashboard.

- **`/dashboard`** — pinned widgets grid, auto-refresh, delete per widget
- **`/explore`** — chat interface, widget preview, pin to dashboard
- **`/about`** — product page: what TravelLens is, capabilities, stack, data sources

Widget state (pinned prompts, types, refresh intervals) persists in a Postgres table.
The Phase 4 `answer()` function is the only data source — no direct DB queries from
the render layer.

---

## PREREQUISITES

- [ ] Phase 4 complete and accepted — all SQL and semantic path tests pass
- [ ] `python -m ai.main "top 5 cities by revenue"` returns rows with no error
- [ ] Flask installed: `pip install flask==3.0.3`
- [ ] Docker stack running: Postgres, Kafka, MinIO, Ollama

---

## DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| `server.py` | `render/` | Flask app starts, all 3 routes respond |
| `dashboard.html` | `render/templates/` | Pinned widgets render with real data |
| `explore.html` | `render/templates/` | Prompt → widget preview → pin works |
| `about.html` | `render/templates/` | Product page renders correctly |
| `widget_renderer.py` | `render/` | Detects result shape, returns Chart.js config |
| `dashboard_widgets` table | Postgres | Schema created, CRUD operations work |

---

## ARCHITECTURE DECISIONS (ORIGINAL)

### Flask, not FastAPI

Flask is simpler for a single-developer portfolio project with server-rendered HTML.
FastAPI would make sense if Phase 5 were a pure API consumed by a React frontend.
Since we're using Jinja2 templates with Chart.js, Flask is the right tool.

### Widget state in Postgres, not JSON file

Everything in one DB — fits the project philosophy. A JSON file would work but
breaks if two browser tabs are open simultaneously, and doesn't survive a file
system reset. Postgres gives us ACID guarantees and the table is trivial to add.

### Phase 4 `answer()` is the only data source

`render/` never imports `psycopg2` directly or writes SQL. It calls `ai.main.answer()`
and renders whatever comes back. This keeps the layers clean — the render layer
is purely presentation, the AI layer owns all data access.

### Widget type auto-detection

The renderer inspects the result dict shape and picks the right Chart.js widget:

| Result shape | Widget type |
|---|---|
| `path=semantic` | Theme summary card + review list |
| 1 column, 1 row, numeric | Stat card |
| 2 columns: label + number, ≤ 15 rows | Bar chart |
| 2 columns: date/time + number | Line chart |
| Any other shape | Table |

User can override the auto-detected type from the Explorer page before pinning.

### Auto-refresh driven by `refresh_interval_minutes`

Each pinned widget stores a `refresh_interval_minutes` value:
- SQL widgets: 0 (refresh on page load only)
- Streaming widgets (queries mentioning `agg_hourly_city_stats`): 60
- Semantic widgets: 0 (reviews don't change frequently)

JavaScript `setInterval` on the dashboard page drives per-widget refresh.

---

## DATABASE SCHEMA

Run this migration before starting the server:

```sql
CREATE TABLE IF NOT EXISTS dashboard_widgets (
    widget_id          SERIAL PRIMARY KEY,
    prompt             TEXT NOT NULL,
    widget_type        VARCHAR(20) NOT NULL
                       CHECK (widget_type IN ('bar_chart','line_chart','stat_card','table','semantic')),
    title              VARCHAR(200),
    refresh_interval_minutes INTEGER NOT NULL DEFAULT 0,
    pinned_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_refreshed_at  TIMESTAMP,
    display_order      INTEGER NOT NULL DEFAULT 0
);

-- Index for ordered dashboard display
CREATE INDEX IF NOT EXISTS idx_dashboard_widgets_order
    ON dashboard_widgets (display_order ASC, pinned_at DESC);
```

Add this to `db/migrations/003_dashboard_widgets.sql`.

---

## STEPS

### Step 1 — Run the migration

```bash
docker exec travellens-postgres psql -U travellens -d travellens \
  -c "CREATE TABLE IF NOT EXISTS dashboard_widgets (
    widget_id SERIAL PRIMARY KEY,
    prompt TEXT NOT NULL,
    widget_type VARCHAR(20) NOT NULL CHECK (widget_type IN ('bar_chart','line_chart','stat_card','table','semantic')),
    title VARCHAR(200),
    refresh_interval_minutes INTEGER NOT NULL DEFAULT 0,
    pinned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_refreshed_at TIMESTAMP,
    display_order INTEGER NOT NULL DEFAULT 0
  );"
```

Verify:
```bash
docker exec travellens-postgres psql -U travellens -d travellens -c "\d dashboard_widgets"
```

---

### Step 2 — Write `render/widget_renderer.py`

Detects result shape from `answer()` and returns a widget config dict.

_(see source file provided separately)_

---

### Step 3 — Write `render/server.py`

_(see source file provided separately)_

---

### Step 4 — Write `render/templates/base.html`

Source file: `render/templates/base.html` (provided separately)

This is the shared Jinja2 layout extended by all three pages. It must contain:
- `<!DOCTYPE html>` with viewport meta and Chart.js CDN import from jsDelivr
- CSS design tokens in `:root` — `--green: #00B140`, `--black`, `--white`, `--gray-50/100/200/500/900`, `--font-mono`, `--font-sans`
- Sticky top nav (black background) with brand name in green monospace on the left, three nav links on the right (Dashboard · Explorer · About), active state in green
- Global button styles: `.btn-primary` (green), `.btn-ghost` (outlined), `.btn-danger` (red outlined)
- Toast notification system — fixed bottom-right, `.toast.show` class triggers visibility, `showToast(msg, type)` JS function
- CSS spinner animation for loading states
- `.page` wrapper with `max-width: 1280px`, centered, `padding: 2rem`
- Jinja2 blocks: `title`, `extra_styles`, `content`, `extra_scripts`

Read comments in the source file — they explain each section.

---

### Step 5 — Write `render/templates/dashboard.html`

Source file: `render/templates/dashboard.html` (provided separately)

Extends `base.html`. Sets `active_page = 'dashboard'`.

The page has two states:

**Empty state** (no widgets pinned):
- Centred message: "No widgets pinned yet"
- Subtext: "Go to Explorer to create your first widget."
- Green "Open Explorer" button linking to `/explore`

**Populated state** (widgets exist):
- Header row: "Dashboard" title + widget count + "New Widget" button linking to `/explore`
- CSS grid of widget cards — auto-fill, min 480px per card, 1.25rem gap
- Each widget card has:
  - Header: title (monospace), "Last refreshed: X" timestamp, refresh (↻) and delete (×) buttons
  - Body: initially shows spinner — JavaScript fills it via `/api/refresh/<id>`
  - `data-widget-id` and `data-refresh-interval` attributes on the card element

JavaScript (inline `<script>` in `extra_scripts` block):
- `renderWidget(bodyEl, config)` — handles all 5 widget types:
  - `stat_card` → large green number + label below
  - `semantic` → Ollama summary text + top 5 reviews list (rating★ + 120 chars)
  - `table` → HTML table with monospace headers
  - `bar_chart` / `line_chart` → Chart.js canvas, green color scheme, no legend
  - `error` → red error message box
- `refreshWidget(widgetId)` — POST to `/api/refresh/<id>`, shows spinner, calls `renderWidget`
- `deleteWidget(widgetId)` — confirm dialog → DELETE `/api/widget/<id>` → remove card from DOM → toast
- `DOMContentLoaded` listener → calls `refreshWidget` for every card → sets `setInterval` for cards with `refresh_interval_minutes > 0`

---

### Step 6 — Write `render/templates/explore.html`

Source file: `render/templates/explore.html` (provided separately)

Extends `base.html`. Sets `active_page = 'explore'`.

**Layout:** Two-column grid — 300px sidebar on left, main content on right.

**Prompt bar** (above the two-column grid, full width):
- Text input with placeholder "e.g. top 5 cities by revenue, complaints about AC in Goa..."
- "Run →" button — disabled during fetch, shows "..." while loading
- Enter key triggers run
- Autofocus on load

**Sidebar — Recent queries:**
- Card with monospace "RECENT QUERIES" header
- List of last 10 queries (session only — JS array, not persisted)
- Click any history item to re-run it
- Empty state: "No queries yet"

**Main — Preview area:**
- Placeholder state: centred icon + "Run a query to preview the widget" + example prompt
- Loading state: spinner + "Running query..."
- Result state — preview card with:
  - Header: truncated prompt as title + widget type override `<select>` (Auto / Bar chart / Line chart / Table / Stat card)
  - Body: rendered widget (same rendering logic as dashboard, but inline — no `/api/refresh` call)
  - Pin bar at bottom: title input (pre-filled with prompt) + "📌 Pin to Dashboard" button

JavaScript:
- `runQuery()` → POST `/api/query` → store result → call `renderPreview(config, prompt)` → add to history
- `renderPreview(config, prompt)` → builds the preview card HTML → calls `renderWidgetBody()`
- `renderWidgetBody(bodyEl, config)` → same 5 widget types as dashboard (destroy old Chart.js instance first)
- `overrideType()` → re-POST `/api/query` with `widget_type` override → re-render body
- `pinWidget()` → POST `/api/pin` with prompt + widget_type + title → `showToast('✓ Pinned to dashboard')`
- `addToHistory(prompt)` → prepend to array, deduplicate, slice to 10 → re-render sidebar list
- `rerunHistory(prompt)` → set input value → call `runQuery()`

---

### Step 7 — Write `render/templates/about.html`

Source file: `render/templates/about.html` (provided separately)

Extends `base.html`. Sets `active_page = 'about'`.

Seven sections in order:

**Hero** (full-width, white background, centred):
- Green monospace version badge: "v1.0 — Local Build"
- H1: "TravelLens India" with "India" in green
- Subtitle paragraph: what the platform does, max-width 560px

**What it does** — 4 capability cards in a responsive grid:
- 💬 Natural Language Queries — ask in plain English, Ollama generates SQL
- 🔍 Semantic Review Search — meaning-based, not keyword-based, powered by pgvector
- ⚡ Live Streaming Data — Kafka + hourly aggregates, dashboards show current data
- 📌 Pinnable Dashboard — pin any result, auto-refresh, no fixed reports

**How it works** — 4-step horizontal flow (flex row, dividers between steps):
- Step 1: You ask → Step 2: Router classifies → Step 3: AI fetches → Step 4: Widget renders

**Tech stack** — pill badges in two styles:
- Green highlighted pills: Postgres 16, pgvector, Apache Kafka, Ollama · Qwen2.5-Coder-7B, sentence-transformers · all-MiniLM-L6-v2
- Grey pills: Python 3.11, Flask, Chart.js, MinIO, Docker

**Data sources** — 2-column grid:
- Hotel Reviews: 30K real reviews from MakeMyTrip/OYO (Kaggle), embedded into pgvector
- Booking Events: 1M simulated records seeded from real hotel IDs, live Kafka streaming

**Build info** — single row: Version · Built by Rishabh · GitHub link ↗ · Dashboard link · Explorer link

**Disclaimer** — amber/yellow warning box:
"TravelLens is a learning and portfolio project. Booking data is synthetic. Review data is real but used for educational purposes only. This application runs locally and is not intended for production use."

---

### Step 7b — Add `__init__.py` to render package

```bash
New-Item render\__init__.py -ItemType File
```

---

### Step 8 — Run the server

```bash
python -m render.server
```

Open: `http://localhost:5000`

Expected: Dashboard page loads (empty — no widgets pinned yet). Navigate to Explorer, type a prompt, verify widget preview appears.

---

### Step 9 — End-to-end test

```
1. Go to http://localhost:5000/explore
2. Type: "top 5 cities by revenue" → widget preview shows bar chart
3. Click Pin to Dashboard → success toast appears
4. Go to http://localhost:5000/dashboard → bar chart widget visible with live data
5. Click refresh button on widget → data reloads
6. Type: "complaints about AC not working" → semantic card preview appears
7. Pin it → goes to dashboard
8. Click × on a widget → widget disappears
9. Reload page → deleted widget stays gone, remaining widgets reload data
10. Go to http://localhost:5000/about → product page renders correctly
```

---

## ACCEPTANCE TESTS

```bash
# 1. Server starts without error
python -m render.server &
sleep 3
curl -s http://localhost:5000/dashboard | grep -q "TravelLens" && echo "dashboard OK"
curl -s http://localhost:5000/explore   | grep -q "TravelLens" && echo "explore OK"
curl -s http://localhost:5000/about     | grep -q "TravelLens" && echo "about OK"

# 2. API query returns widget config
curl -s -X POST http://localhost:5000/api/query \
  -H "Content-Type: application/json" \
  -d '{"prompt": "top 5 cities by revenue"}' \
  | python -c "import sys,json; d=json.load(sys.stdin); assert d['widget_config']['type']=='bar_chart', d; print('query API OK')"

# 3. Pin a widget
curl -s -X POST http://localhost:5000/api/pin \
  -H "Content-Type: application/json" \
  -d '{"prompt": "top 5 cities by revenue", "widget_type": "bar_chart", "title": "Revenue by City"}' \
  | python -c "import sys,json; d=json.load(sys.stdin); assert 'widget_id' in d, d; print('pin API OK — widget_id:', d['widget_id'])"

# 4. Refresh returns data
# Replace <widget_id> with the id from step 3
curl -s -X POST http://localhost:5000/api/refresh/<widget_id> \
  | python -c "import sys,json; d=json.load(sys.stdin); assert d['widget_config']['type']!='error', d; print('refresh API OK')"

# 5. Delete widget
curl -s -X DELETE http://localhost:5000/api/widget/<widget_id> \
  | python -c "import sys,json; d=json.load(sys.stdin); assert 'deleted' in d['message'].lower() or 'deleted' in str(d), d; print('delete API OK')"

# 6. dashboard_widgets table confirms persistence
docker exec travellens-postgres psql -U travellens -d travellens \
  -c "SELECT widget_id, prompt, widget_type FROM dashboard_widgets;"
```

---

## EXPLORE

After Phase 5 is running:

**psql — check widget state:**
```sql
SELECT widget_id, prompt, widget_type, pinned_at, last_refreshed_at
FROM dashboard_widgets
ORDER BY pinned_at DESC;
```

**Test different widget types:**
- Stat card: `"total number of hotels in database"`
- Bar chart: `"top 10 cities by total bookings"`
- Line chart: `"monthly revenue trend for 2025"`
- Table: `"show me hotels in Goa with their star rating and total rooms"`
- Semantic: `"what are guests complaining about most"`

**Test auto-refresh:** Pin a streaming widget with a query mentioning "hourly". Leave the dashboard open — it should auto-refresh every 60 minutes.

---

## DO NOT

- Do not query Postgres directly from `server.py` for data — only for widget state. All data comes through `ai.main.answer()`
- Do not render widgets server-side on page load — load empty shells and fill with JavaScript via `/api/refresh`. This keeps the initial page load fast.
- Do not store Chart.js rendered output in the DB — store only the prompt and widget type. Always re-render from fresh data.
- Do not run Flask in production mode (`debug=True` is fine for local dev)
- Do not add authentication — this is a local dev tool

---

## ROLLBACK

```bash
# Stop the server (Ctrl+C)
# Drop the widgets table
docker exec travellens-postgres psql -U travellens -d travellens \
  -c "DROP TABLE IF EXISTS dashboard_widgets;"
# Remove render files
rm render/server.py render/widget_renderer.py render/__init__.py
rm render/templates/dashboard.html render/templates/explore.html render/templates/about.html
```

Phase 4 and all data are unaffected.

---

## LESSONS LEARNED

- Widget type auto-detection: accuracy depends on query phrasing; `widget_renderer.py` uses shape-based heuristics (single scalar → stat card, multiple rows + 2 columns → bar chart, etc.).
- Read path never triggers compute (B-022 — pin freezes SQL; refresh runs frozen SQL; dashboard load serves JSONB cache). See backlog B-022 for the full cache architecture.
- Ollama latency on first query ~2–3s (model warm-up); subsequent queries ~1s on RTX 3070.

---

## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any code
- Create every file in the REPO STATE FILE TREE above — no extras, no missing files
- `render/__init__.py` must be created (empty file) — Python won't find the module without it
- `render/templates/base.html` must be created first — all other templates extend it
- Copy all code blocks in Steps 2, 3, 4, 5, 6, 7 exactly — do not rewrite or simplify
- Run the database migration (Step 1) before starting the server
- Run ALL acceptance tests before declaring Phase 5 done — do not skip any
- If an acceptance test fails, fix the code — do not modify the test
- Never modify any file in `ai/`, `scripts/`, `db/schema.sql`, `docker/`
- Never import psycopg2 in server.py for data queries — only for dashboard_widgets table CRUD
- Replace `yourusername` in about.html GitHub link with the actual GitHub username
- The server must be run as `python -m render.server` not `python render/server.py`

---

## BUILD HISTORY / EVOLUTION

Changes to the Phase 5 dashboard after the original acceptance sign-off. Earliest first.

---

### Migration 004 — Widget width setting

`db/migrations/004_widget_settings.sql` added a `width` column to `dashboard_widgets`. See [`datamodel.md`](../datamodel.md) Schema Evolution for the DDL.

---

### B-022 — Frozen SQL cache (read path never triggers compute)

`db/migrations/005_widget_cache.sql` added `generated_sql` and `last_result_json` to `dashboard_widgets`. `render/server.py` updated: pin now stores the Ollama-generated SQL frozen on the widget row; refresh runs that frozen SQL directly without a new LLM call; dashboard page load serves the `last_result_json` JSONB cache immediately.

**Why:** LLM calls on every refresh added latency and non-determinism. Frozen SQL makes refresh fast, deterministic, and LLM-cost-free — same stale-while-revalidate pattern a CDN uses.

**Files modified:** `render/server.py`, `render/templates/dashboard.html` (Show SQL modal + widget rename). See [`backlog.md`](backlog.md) B-022 for the full design.

---

## NEXT

Phase 6 — Airflow DAGs

Files to build:
- `airflow/dags/daily_hotel_kpi.py` — nightly `fact_bookings` → `agg_daily_hotel_kpi`
- `airflow/dags/reconcile_late_events.py` — merge `late_events/` S3 back into aggregates
- `airflow/dags/hotel_sentiment_scores.py` — weekly pgvector sentiment aggregation
- `airflow/dags/customer_ltv.py` — weekly customer lifetime value computation
