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
      POST   /api/query          — run a prompt, return widget config
      POST   /api/pin            — save widget to dashboard_widgets table
      DELETE /api/widget/<id>    — remove a pinned widget
      POST   /api/refresh/<id>   — re-run a pinned widget's prompt, return fresh data

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
import logging
import psycopg2
from datetime import datetime
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

from ai.main import answer
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


def get_pinned_widgets() -> list[dict]:
    """
    Fetch all pinned widgets ordered by display_order then pinned_at.
    Returns a list of dicts ready to pass to the dashboard template.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT widget_id, prompt, widget_type, title,
                       refresh_interval_minutes, pinned_at, last_refreshed_at, width
                FROM dashboard_widgets
                ORDER BY display_order ASC, pinned_at DESC
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "widget_id":   r[0],
            "prompt":      r[1],
            "widget_type": r[2],
            "title":       r[3] or r[1][:55],
            "refresh_interval_minutes": r[4],
            "pinned_at":   r[5].strftime("%d %b %Y %H:%M") if r[5] else "",
            "last_refreshed_at": r[6].strftime("%d %b %Y %H:%M") if r[6] else "Never",
            "width":       r[7],
        }
        for r in rows
    ]


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
    Save a widget to the dashboard_widgets table.
    Called by the Explorer page when user clicks 'Pin to Dashboard'.

    Request body:
    {
        "prompt":      "top 5 cities by revenue",
        "widget_type": "bar_chart",
        "title":       "Revenue by City"   (optional — defaults to prompt)
    }

    Refresh interval auto-detection:
        Queries mentioning "hourly" or "streaming" get 60-minute auto-refresh
        to match the Kafka window size. Everything else refreshes on page load only.
    """
    data        = request.get_json()
    prompt      = (data.get("prompt") or "").strip()
    widget_type = data.get("widget_type", "table")
    title       = data.get("title") or prompt[:60]

    if not prompt:
        return jsonify({"error": "prompt is required"}), 400

    # Streaming widgets refresh every 60 min to match the Kafka window
    refresh_interval = 60 if any(
        w in prompt.lower() for w in ("hourly", "streaming", "live")
    ) else 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dashboard_widgets
                    (prompt, widget_type, title, refresh_interval_minutes, pinned_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING widget_id
            """, (prompt, widget_type, title, refresh_interval, datetime.now()))
            widget_id = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    log.info("Pinned widget %d: %s", widget_id, prompt)
    return jsonify({"widget_id": widget_id, "message": "Pinned to dashboard"})


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
    Update a widget's settings: refresh interval and/or width.
    Called by the settings popup (gear icon) on the dashboard.

    Request body (any subset):
    {
        "refresh_interval_minutes": 15,    (optional — 0, 5, 15, 30, 60, 360)
        "width": "full"                    (optional — 'normal' or 'full')
    }

    Only the fields present in the request are updated. This keeps the popup
    flexible — it can update one setting without touching the others.
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
    Re-run a pinned widget's prompt and return fresh widget config.

    This is what makes the dashboard live — every widget re-fetches its data
    by re-running the original prompt through the full Phase 4 AI layer.
    The dashboard page calls this on page load for every widget, and then
    again on the setInterval timer for streaming widgets.

    On success: updates last_refreshed_at in dashboard_widgets.
    On error: returns error in the widget_config so the dashboard can show
              a friendly error message inside the widget card.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT prompt, widget_type, title FROM dashboard_widgets WHERE widget_id = %s",
                (widget_id,)
            )
            row = cur.fetchone()

        if not row:
            conn.close()
            return jsonify({"error": "Widget not found"}), 404

        prompt, widget_type, title = row

        # Re-run through Phase 4 AI layer — always fresh data
        result = answer(prompt)
        config = build_widget_config(result, widget_type=widget_type, title=title)

        # Update last_refreshed_at timestamp
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dashboard_widgets SET last_refreshed_at = %s WHERE widget_id = %s",
                (datetime.now(), widget_id)
            )
        conn.commit()

    finally:
        conn.close()

    return jsonify({"widget_config": config})


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
