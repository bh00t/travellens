"""
widget_renderer.py — Result Shape Detection + Chart.js Config Builder
======================================================================
Part of: TravelLens Phase 5 — Dashboard
File:    render/widget_renderer.py

What this file does:
    Takes the result dict from ai.main.answer() and returns a widget config
    dict that the Jinja2 templates use to render Chart.js visualisations.

    Auto-detection rules (in priority order):
      1. path=semantic       → semantic summary card
      2. 1 col, 1 row, num   → stat card (single big number)
      3. 2 cols, date+num    → line chart (time series)
      4. 2 cols, label+num, ≤20 rows → bar chart
      5. 2 cols, label+num, >20 rows → bar chart WITH a readability warning
      6. everything else     → table

    User can override the auto-detected type from the Explorer dropdown.

Readability warning (Option A behaviour):
    When a bar chart would have more than READABLE_BAR_LIMIT (20) bars, we still
    render the bar chart the user asked for, but we attach a "warning" field
    explaining that it may be cramped and suggesting a table instead.

    The suggestion is ALWAYS "table" for categorical data — NEVER "line chart".
    A line chart implies a sequence/trend, which is wrong for categories like
    states, cities, or customer segments. Suggesting a line chart for
    categorical data would be inaccurate, so we never do it. Line charts are
    only ever used when the x-axis is genuinely time (dates/months).
"""

from datetime import date, datetime
from decimal import Decimal

# Past this many bars, a bar chart gets cramped and labels overlap.
# Your data has 16 cities / 16 states, so 20 covers all geographic groupings
# while still flagging genuinely huge result sets.
READABLE_BAR_LIMIT = 20


def _is_numeric(value) -> bool:
    """Check if a value is a number (int, float, or Decimal)."""
    return isinstance(value, (int, float, Decimal))


def _is_date_like(value) -> bool:
    """Check if a value looks like a date or datetime (true time series)."""
    if isinstance(value, (date, datetime)):
        return True
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m", "%B", "%b"):
            try:
                datetime.strptime(str(value), fmt)
                return True
            except ValueError:
                continue
    return False


def detect_widget_type(result: dict) -> str:
    """
    Auto-detect the best widget type for a result dict.

    Returns one of: 'semantic', 'stat_card', 'bar_chart', 'line_chart', 'table'

    Note: this returns bar_chart even for >20 rows. The "too many bars" case
    is handled as a warning in build_widget_config, not by switching to a
    table here — Option A renders what the data suggests and warns, rather
    than silently downgrading to a table.
    """
    if result.get("path") == "semantic":
        return "semantic"

    columns = result.get("columns", [])
    rows    = result.get("rows", [])

    if not columns or not rows:
        return "table"

    # Single value → stat card
    if len(columns) == 1 and len(rows) == 1:
        if _is_numeric(rows[0][0]):
            return "stat_card"

    # Two columns → chart candidate
    if len(columns) == 2 and len(rows) >= 1:
        first_val  = rows[0][0]
        second_val = rows[0][1]

        # Date + number → line chart (genuine time series)
        if _is_date_like(first_val) and _is_numeric(second_val):
            return "line_chart"

        # Label + number → bar chart (any row count; warning added later)
        if _is_numeric(second_val):
            return "bar_chart"

    return "table"


def _build_warning(widget_type: str, row_count: int) -> dict | None:
    """
    Build a readability warning if the chosen widget type doesn't suit the
    data size. Returns None if there's nothing to warn about.

    Returns a dict the template uses to render the Option A banner:
    {
        "message":  "16 categories may be hard to read as a bar chart...",
        "suggested": "table"   # always table for categorical — never line_chart
    }

    Accuracy guarantee: the suggested alternative is ALWAYS 'table'. We never
    suggest a line chart for a crowded bar chart, because a line chart implies
    a time sequence the categorical data does not have.
    """
    if widget_type == "bar_chart" and row_count > READABLE_BAR_LIMIT:
        return {
            "message": (
                f"{row_count} bars may be hard to read — labels can overlap "
                f"and bars get thin past ~{READABLE_BAR_LIMIT} categories. "
                f"A table shows this data more clearly."
            ),
            "suggested": "table"
        }
    return None


def build_widget_config(result: dict, widget_type: str = None, title: str = None) -> dict:
    """
    Build the full widget config dict for a Jinja2 template.

    Args:
        result:      Result dict from ai.main.answer()
        widget_type: Override auto-detected type (optional — from Explorer dropdown)
        title:       Display title (defaults to truncated query)

    Returns a config dict:
    {
        "type":     <widget type string>,
        "title":    <display title>,
        "error":    <error message or None>,
        "warning":  <warning dict or None>,   # Option A readability banner
        "data":     <type-specific data dict>
    }

    The "data" key shape varies by type:
        stat_card  → {"value": ..., "label": ...}
        bar_chart  → {"labels": [...], "values": [...], "x_label": ..., "y_label": ...}
        line_chart → same as bar_chart
        table      → {"columns": [...], "rows": [[...], ...]}
        semantic   → {"summary": ..., "city": ..., "reviews": [...], "total": ...}
        error      → {}
    """
    # Error state — return early
    if result.get("error"):
        return {
            "type":    "error",
            "title":   title or "Query Error",
            "error":   result["error"],
            "warning": None,
            "data":    {}
        }

    detected_type = widget_type or detect_widget_type(result)
    display_title = title or (result.get("query") or "")[:60]

    columns = result.get("columns", [])
    rows    = result.get("rows", [])

    config = {
        "type":    detected_type,
        "title":   display_title,
        "error":   None,
        "warning": _build_warning(detected_type, len(rows)),
        "data":    {}
    }

    if detected_type == "semantic":
        config["data"] = {
            "summary": result.get("summary", ""),
            "city":    result.get("city") or "All cities",
            "reviews": result.get("reviews", [])[:5],
            "total":   len(result.get("reviews", []))
        }

    elif detected_type == "stat_card":
        config["data"] = {
            "value": rows[0][0],
            "label": columns[0]
        }

    elif detected_type in ("bar_chart", "line_chart"):
        config["data"] = {
            "labels":  [str(r[0]) for r in rows],
            "values":  [float(r[1]) if _is_numeric(r[1]) else 0 for r in rows],
            "x_label": columns[0],
            "y_label": columns[1],
        }

    elif detected_type == "table":
        config["data"] = {
            "columns": columns,
            "rows":    [[str(v) for v in row] for row in rows]
        }

    return config
