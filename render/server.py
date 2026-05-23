"""
server.py — TravelLens Flask Application
=========================================
Part of: TravelLens Phase 5 — Dashboard
File:    render/server.py

What this file does:
    Three-page Flask app wrapping the Phase 4 AI layer into a Grafana-style dashboard.

    Page routes:
      GET  /dashboard  — pinned widgets grid
      GET  /explore    — chat interface + widget preview
      GET  /about      — product page

    API routes (called by JavaScript — return JSON):
      POST   /api/query                    — run a prompt, return widget config
      POST   /api/pin                      — save widget to dashboard_widgets table
      DELETE /api/widget/<id>              — remove a pinned widget
      POST   /api/widget/<id>/settings     — patch refresh interval / width
      POST   /api/widgets/reorder          — persist drag-and-drop order
      POST   /api/refresh/<id>             — read the widget cache, or run frozen SQL

Why separate API routes from page routes:
    Page routes return full HTML. API routes return JSON.
    The Explorer uses /api/query to preview without pinning.
    The Dashboard uses /api/refresh for per-widget live data.
    JavaScript can update one widget without reloading the whole page.

Why render/ never queries Postgres for data directly:
    All data comes through ai.main.answer(). The render layer is purely
    presentation — it only touches Postgres for widget state (dashboard_widgets
    table CRUD). This keeps the layers clean and testable independently.

Run with:
    python -m render.server
    → http://localhost:5000
"""

import os
import json
import logging
import psycopg2
from decimal import Decimal
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

from ai.main import answer
from ai.text_to_sql import run_stored_sql
from render.widget_renderer import build_widget_config, detect_widget_type

load_dotenv()

# ── Flask app setup ────────────────────────────────────────────────────────────
# template_folder points to render/templates/ — Jinja2 finds base.html etc. there
app = Flask(__name__, template_folder="templates")
log = logging.getLogger(__name__)

# ── DB config — used only for dashboard_widgets CRUD ──────────────────────────
# All data queries go through ai.main.answer(), not directly here.
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    """Open a new Postgres connection. Caller is responsible for closing it."""
    return psycopg2.connect(**DB_CONFIG)


def _serialise_result(obj):
    """
    Recursively convert Decimal/datetime/date values to JSON-friendly types.

    Why this exists (B-022):
        The widget config returned by build_widget_config() can contain
        Decimal (Postgres NUMERIC columns) and datetime values inside its
        'data' dict. We store this config in dashboard_widgets.last_result_json
        as JSONB and also embed it inline on the dashboard page. Both code
        paths need plain JSON-serialisable Python primitives.

        json.dumps with a custom default= can convert these but doesn't
        recurse — Decimal values nested inside lists of tuples slip through.
        This helper walks the structure once and converts every node.
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialise_result(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialise_result(v) for v in obj]
    return obj


def _is_cache_stale(last_refreshed_at, last_result_json, interval_minutes) -> bool:
    """
    Decide whether a widget's cache must be refreshed before render.

    Stale when:
      - There is no cached result yet (last_result_json is NULL), OR
      - There is no last_refreshed_at timestamp, OR
      - interval_minutes > 0 AND now - last_refreshed_at > interval

    Widgets with interval_minutes == 0 (manual refresh only) are NOT stale
    just because time has passed — that's the whole point of opting out of
    auto-refresh. The user-triggered refresh button still re-runs the
    frozen SQL on demand.
    """
    if last_result_json is None or last_refreshed_at is None:
        return True
    if interval_minutes and interval_minutes > 0:
        age = datetime.now() - last_refreshed_at
        return age > timedelta(minutes=interval_minutes)
    return False


def get_pinned_widgets() -> list[dict]:
    """
    Fetch all pinned widgets ordered by display_order then pinned_at.
    Returns a list of dicts ready to pass to the dashboard template.

    B-022: each dict now also carries the cached widget config and a
    'is_stale' flag so the dashboard read path can render instantly from
    cache without calling /api/refresh (which would re-run SQL or — for
    semantic widgets — re-prompt Ollama).
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # generated_sql is selected so the dashboard's "Show SQL" toggle
            # can render the frozen query inline without any extra HTTP call.
            # NULL for semantic widgets (they have no SQL to freeze).
            cur.execute("""
                SELECT widget_id, prompt, widget_type, title,
                       refresh_interval_minutes, pinned_at, last_refreshed_at,
                       width, last_result_json, generated_sql
                FROM dashboard_widgets
                ORDER BY display_order ASC, pinned_at DESC
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    widgets = []
    for r in rows:
        (widget_id, prompt, widget_type, title, interval,
         pinned_at, last_refreshed_at, width, cached_config, generated_sql) = r

        is_stale = _is_cache_stale(last_refreshed_at, cached_config, interval)

        widgets.append({
            "widget_id":   widget_id,
            "prompt":      prompt,
            "widget_type": widget_type,
            "title":       title or prompt[:55],
            "refresh_interval_minutes": interval,
            "pinned_at":   pinned_at.strftime("%d %b %Y %H:%M") if pinned_at else "",
            "last_refreshed_at": (
                last_refreshed_at.strftime("%d %b %Y %H:%M")
                if last_refreshed_at else "Never"
            ),
            "width":         width,
            # JSONB → already a dict from psycopg2; None if never cached
            "cached_config": cached_config,
            "is_stale":      is_stale,
            # Read-only display: the frozen SQL string for the "Show SQL"
            # toggle. Empty string when the widget has no SQL (semantic
            # widgets, or a SQL widget that's never been refreshed under
            # B-022). has_sql is the boolean the template uses to decide
            # whether to render the toggle at all.
            "sql":     generated_sql or "",
            "has_sql": bool(generated_sql),
        })
    return widgets


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/")
@app.route("/dashboard")
def dashboard():
    """
    Dashboard page — loads all pinned widgets as empty shells.
    JavaScript fills each shell with live data via /api/refresh/<id> on page load.
    Widgets with refresh_interval_minutes > 0 also auto-refresh on a timer.
    """
    widgets = get_pinned_widgets()
    return render_template("dashboard.html", widgets=widgets)


@app.route("/explore")
def explore():
    """
    Explorer page — chat interface for creating new widgets.
    User types a prompt → /api/query returns a preview → user pins via /api/pin.
    """
    return render_template("explore.html")


@app.route("/about")
def about():
    """About page — product description, capabilities, stack, data sources."""
    return render_template("about.html")


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/query", methods=["POST"])
def api_query():
    """
    Run a prompt through the Phase 4 AI layer and return a widget config.
    Called by the Explorer page to preview a widget before pinning.

    Request body:  {"prompt": "top 5 cities by revenue", "widget_type": null}
    Response:      {"widget_config": {...}, "result": {...}, "detected_type": "bar_chart"}

    widget_type in the request is optional — if provided it overrides auto-detection.
    This is what the Explorer's type dropdown sends when the user changes the type.
    """
    data        = request.get_json()
    prompt      = (data.get("prompt") or "").strip()
    widget_type = data.get("widget_type")  # None = auto-detect

    if not prompt:
        return jsonify({"error": "prompt is required"}), 400

    log.info("API query: %s", prompt)
    result = answer(prompt)
    config = build_widget_config(result, widget_type=widget_type)

    return jsonify({
        "widget_config": config,
        "result":        result,
        "detected_type": config["type"]
    })


@app.route("/api/pin", methods=["POST"])
def api_pin():
    """
    Save a widget to the dashboard_widgets table and freeze its SQL + result.

    B-022 — author path (runs once, at pin time):
        1. Run the prompt through the full AI layer (Ollama → SQL → execute,
           or semantic → embed → pgvector → summarise).
        2. Freeze the generated SQL into dashboard_widgets.generated_sql.
           Every future refresh runs THIS exact SQL — never re-prompts Ollama.
           This eliminates the non-determinism where the same prompt produced
           subtly different SQL (and different results) between refreshes.
        3. Cache the rendered widget config into last_result_json so the
           dashboard read path can render instantly without any data query.

    Semantic widgets have no SQL to freeze — generated_sql stays NULL and
    refresh falls back to a full answer() call. The result cache still helps
    them: a freshly-cached semantic widget renders instantly on dashboard load.

    Request body:
    {
        "prompt":      "top 5 cities by revenue",
        "widget_type": "bar_chart",
        "title":       "Revenue by City"   (optional — defaults to prompt)
    }

    Refresh interval auto-detection (three tiers):
        - "hourly" / "streaming" / "live" prompts        → 60 min
          (matches the Kafka window — fresh data within an hour)
        - semantic widgets (review-corpus queries)       → 1440 min (24 h)
          (Reviews are a slow-moving corpus — they don't change minute-to-minute,
           and re-running a semantic widget on every page load re-embeds the
           query and re-prompts Ollama. A 24 h TTL keeps results stable across
           a working day without paying that latency on every navigation.)
        - everything else (regular SQL widgets)          → 0
          (No auto-refresh; the frozen SQL re-runs each time the user clicks
           the refresh button. interval=0 means the cache stays fresh forever
           in the read-path sense — pages always render from cache.)
    """
    data        = request.get_json()
    prompt      = (data.get("prompt") or "").strip()
    widget_type = data.get("widget_type", "table")
    title       = data.get("title") or prompt[:60]

    if not prompt:
        return jsonify({"error": "prompt is required"}), 400

    # ── Author path: run once, freeze SQL, cache config ───────────────────
    # We must run answer() BEFORE choosing the refresh interval, because
    # the rule for semantic widgets (1440 min TTL) depends on result["path"].
    # Detecting "semantic" from the prompt alone would re-implement the
    # router's logic and drift out of sync — ask the AI layer instead.
    log.info("Pinning widget — running prompt once to freeze SQL: %s", prompt)
    result = answer(prompt)
    config = build_widget_config(result, widget_type=widget_type, title=title)

    # Decide refresh interval AFTER answer() so we know which path was taken.
    # Order matters: the streaming hint wins over the semantic default —
    # a hypothetical "live review summary" should still refresh hourly.
    if any(w in prompt.lower() for w in ("hourly", "streaming", "live")):
        refresh_interval = 60      # streaming — refresh hourly
    elif result.get("path") == "semantic":
        refresh_interval = 1440    # semantic — reviews don't change, cache 24h
    else:
        refresh_interval = 0       # SQL — re-execute on every page load

    # Only SQL widgets have generated_sql to freeze; semantic widgets stay NULL.
    generated_sql = result.get("sql") if result.get("path") == "sql" else None
    cached_json   = json.dumps(_serialise_result(config))
    now           = datetime.now()

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dashboard_widgets
                    (prompt, widget_type, title, refresh_interval_minutes,
                     pinned_at, last_refreshed_at,
                     generated_sql, last_result_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING widget_id
            """, (prompt, widget_type, title, refresh_interval,
                  now, now, generated_sql, cached_json))
            widget_id = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    log.info(
        "Pinned widget %d (frozen_sql=%s): %s",
        widget_id, "yes" if generated_sql else "no (semantic)", prompt
    )
    return jsonify({
        "widget_id": widget_id,
        "message":   "Pinned to dashboard",
        "frozen":    bool(generated_sql),
    })


@app.route("/api/widget/<int:widget_id>", methods=["DELETE"])
def api_delete_widget(widget_id: int):
    """
    Delete a pinned widget by ID.
    Called by the Dashboard page when user clicks the × button on a widget.
    The widget card is removed from the DOM by JavaScript after a successful response.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM dashboard_widgets WHERE widget_id = %s",
                (widget_id,)
            )
            deleted = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    if deleted == 0:
        return jsonify({"error": "Widget not found"}), 404

    log.info("Deleted widget %d", widget_id)
    return jsonify({"message": "Widget deleted"})


@app.route("/api/widget/<int:widget_id>/settings", methods=["POST"])
def api_widget_settings(widget_id: int):
    """
    Update a widget's settings: refresh interval, width, and/or title.
    Called by the settings popup (gear icon) on the dashboard.

    Request body (any subset):
    {
        "refresh_interval_minutes": 15,    (optional — 0, 5, 15, 30, 60, 360)
        "width": "full",                   (optional — 'normal' or 'full')
        "title": "New title"               (optional — non-empty, ≤200 chars)
    }

    Only the fields present in the request are updated. This keeps the popup
    flexible — it can update one setting without touching the others. The
    rename input in the popover sends just {"title": "..."}, the dropdowns
    each send their own single field, all through this one endpoint.
    """
    data = request.get_json() or {}

    # Build the SET clause dynamically from whatever fields were sent
    updates = []
    params  = []

    if "refresh_interval_minutes" in data:
        interval = int(data["refresh_interval_minutes"])
        if interval not in (0, 5, 15, 30, 60, 360):
            return jsonify({"error": "invalid refresh interval"}), 400
        updates.append("refresh_interval_minutes = %s")
        params.append(interval)

    if "width" in data:
        width = data["width"]
        if width not in ("normal", "full"):
            return jsonify({"error": "invalid width"}), 400
        updates.append("width = %s")
        params.append(width)

    if "title" in data:
        # Trim incidental whitespace so " foo " and "foo" don't both end up
        # in the table, and so a string of only spaces is treated as empty.
        title = (data["title"] or "").strip()
        if not title:
            # Reject empty rename outright — an unnamed widget is confusing
            # in the dashboard listing and has no recoverable meaning.
            return jsonify({"error": "title cannot be empty"}), 400
        # Schema guard: dashboard_widgets.title is VARCHAR(200). Reject
        # client-side instead of letting Postgres raise a length error.
        if len(title) > 200:
            return jsonify({"error": "title too long"}), 400
        updates.append("title = %s")
        params.append(title)

    if not updates:
        return jsonify({"error": "no valid settings provided"}), 400

    params.append(widget_id)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE dashboard_widgets SET {', '.join(updates)} WHERE widget_id = %s",
                params
            )
            updated = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    if updated == 0:
        return jsonify({"error": "Widget not found"}), 404

    log.info("Updated settings for widget %d: %s", widget_id, data)
    return jsonify({"message": "Settings updated"})


@app.route("/api/widgets/reorder", methods=["POST"])
def api_widgets_reorder():
    """
    Save a new widget display order after a drag-and-drop reorder.
    Called by the dashboard when the user drops a widget in a new position.

    Request body:
    {
        "order": [5, 2, 8, 1]    # widget IDs in their new display order
    }

    Sets display_order = position index for each widget. The dashboard query
    orders by display_order ASC, so the new order persists on reload.
    """
    data  = request.get_json() or {}
    order = data.get("order", [])

    if not isinstance(order, list) or not order:
        return jsonify({"error": "order must be a non-empty list of widget IDs"}), 400

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Each widget's display_order = its index in the new order list
            for position, widget_id in enumerate(order):
                cur.execute(
                    "UPDATE dashboard_widgets SET display_order = %s WHERE widget_id = %s",
                    (position, int(widget_id))
                )
        conn.commit()
    finally:
        conn.close()

    log.info("Reordered widgets: %s", order)
    return jsonify({"message": "Order saved"})


@app.route("/api/refresh/<int:widget_id>", methods=["POST"])
def api_refresh_widget(widget_id: int):
    """
    Re-run a pinned widget on the cache-aware compute path (B-022).

    Three branches, in order:
      1. CACHE HIT — interval > 0 and cache is within its refresh window:
         return the stored widget config from last_result_json with
         from_cache: true. No SQL, no Ollama, sub-millisecond.
      2. FROZEN SQL — widget has generated_sql (SQL widgets): run that exact
         SQL through run_stored_sql(). No Ollama call. Typically <200ms.
      3. FULL RECOMPUTE — fallback for semantic widgets (no SQL to freeze)
         or SQL widgets that pre-date B-022 and have no generated_sql yet.
         Goes through the full answer() pipeline; the result is also used
         to backfill generated_sql so the next refresh hits branch 2.

    Why a cache check on /api/refresh and not just on dashboard load:
        The dashboard page reads from the embedded cache on load (no API
        call needed). But scheduled setInterval ticks and manual refresh
        clicks all come through here. Honouring the refresh interval on
        the server keeps cache semantics centralised — the JS doesn't
        need to know whether the interval has elapsed.

    On success: updates last_refreshed_at + last_result_json (cache) and
    backfills generated_sql for pre-B-022 widgets.

    Response shape:
        {"widget_config": {...}, "from_cache": true|false}
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prompt, widget_type, title, generated_sql,
                       last_refreshed_at, refresh_interval_minutes,
                       last_result_json
                FROM dashboard_widgets
                WHERE widget_id = %s
            """, (widget_id,))
            row = cur.fetchone()

        if not row:
            return jsonify({"error": "Widget not found"}), 404

        (prompt, widget_type, title, generated_sql,
         last_refreshed_at, interval, cached_config) = row

        # ── Branch 1: cache hit ───────────────────────────────────────────
        # The widget has a cached config and the cache is still within its
        # refresh window. Serve it straight back — no compute.
        if not _is_cache_stale(last_refreshed_at, cached_config, interval):
            log.info("Widget %d served from cache (interval=%s)", widget_id, interval)
            return jsonify({
                "widget_config": cached_config,
                "from_cache":    True,
            })

        # ── Branches 2 & 3: cache miss — recompute ────────────────────────
        if generated_sql:
            # Frozen SQL path — no Ollama, just psycopg2.
            log.info("Widget %d running frozen SQL", widget_id)
            result = run_stored_sql(generated_sql, user_query=prompt)
        else:
            # Semantic widget OR a pre-B-022 widget with no frozen SQL.
            # Fall back to the full pipeline.
            log.info("Widget %d full recompute (no frozen SQL)", widget_id)
            result = answer(prompt)

        config = build_widget_config(result, widget_type=widget_type, title=title)

        # Backfill generated_sql if this was the first refresh after B-022
        # for an older SQL widget that didn't have its SQL frozen at pin time.
        new_sql_to_store = (
            result.get("sql")
            if (not generated_sql and result.get("path") == "sql")
            else None
        )

        cached_json = json.dumps(_serialise_result(config))
        now         = datetime.now()

        with conn.cursor() as cur:
            if new_sql_to_store:
                cur.execute("""
                    UPDATE dashboard_widgets
                       SET last_refreshed_at = %s,
                           last_result_json  = %s::jsonb,
                           generated_sql     = %s
                     WHERE widget_id = %s
                """, (now, cached_json, new_sql_to_store, widget_id))
            else:
                cur.execute("""
                    UPDATE dashboard_widgets
                       SET last_refreshed_at = %s,
                           last_result_json  = %s::jsonb
                     WHERE widget_id = %s
                """, (now, cached_json, widget_id))
        conn.commit()

    finally:
        conn.close()

    return jsonify({
        "widget_config": config,
        "from_cache":    False,
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )
    # debug=True enables auto-reload on file changes — fine for local dev
    # Never use debug=True in production
    app.run(host="0.0.0.0", port=5000, debug=True)
