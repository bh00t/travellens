"""
main.py — TravelLens AI Layer Entry Point
==========================================
Part of: TravelLens Phase 4 — AI Layer
File:    ai/main.py

What this file does:
    Single entry point for the entire AI layer. Takes a plain English query,
    routes it to the correct path, runs that path, and returns a result dict.

    Two ways to use it:

    1. CLI (development / testing):
         python ai/main.py "top 5 cities by revenue"
         python ai/main.py "complaints about AC in Goa hotels"

    2. As a function (Phase 5 calls this):
         from ai.main import answer
         result = answer("top 5 cities by revenue")
         # result is a dict — pass to render/render_output.py

    The result dict shape depends on which path was taken:
      SQL path:      {"path": "sql",      "sql": ..., "columns": ..., "rows": ...}
      Semantic path: {"path": "semantic", "reviews": ..., "summary": ..., "city": ...}
      Either path:   {"error": <message>} if something went wrong

Why this file exists (not calling sql/semantic directly):
    Phase 5 imports one thing: answer(). It doesn't need to know about routing,
    or which path ran, or how Ollama is called. This file is the seam between
    the AI layer and everything else. Keeping it thin means Phase 5 is isolated
    from any internal restructuring of Phase 4.
"""

import re
import sys
import logging
from ai.query_router    import route
from ai.text_to_sql     import run as sql_run
from ai.semantic_search import run as semantic_run, _detect_city, _load_cities, _resolve_hotel_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger(__name__)


# ── B-004: hybrid query filter detection ──────────────────────────────────────
# A hybrid query has BOTH a structured filter (rating / star / city) AND a
# semantic intent (review content). The router routes it to "semantic"; we then
# detect filters and pre-scope the review search to hotels matching those
# filters, so the semantic results are only drawn from the right population.
#
# Design (locked):
#   - Lightweight regex/keyword patterns, NOT a second Ollama call. Two LLM
#     calls per user request is exactly the latency B-022 removed.
#   - Patterns require explicit "rating"/"rated"/"star+hotel|property"
#     context. A bare number ("top 5 cities", "5 star service", "4am
#     checkout") MUST NOT match — false positives silently filter out
#     legitimate results, which is worse than not detecting at all.
#   - City detection reuses semantic_search._detect_city — single source
#     of truth, no drift between the router's known-city set and ours.

# "rating above 4", "rated over 3.5", "rating greater than 4", "rating >= 4"
_RATING_GTE_PATTERN = re.compile(
    r"\b(?:rating|rated)\s+(?:above|over|greater\s+than|at\s+least|>=?)\s+"
    r"(\d+(?:\.\d+)?)\b",
    re.IGNORECASE,
)

# "rating below 3", "rated under 2.5", "rating less than 3"
_RATING_LTE_PATTERN = re.compile(
    r"\b(?:rating|rated)\s+(?:below|under|less\s+than|<=?|at\s+most)\s+"
    r"(\d+(?:\.\d+)?)\b",
    re.IGNORECASE,
)

# "4+ rating", "4+ stars rating" — the "N+" shorthand. Requires the trailing
# "rating" keyword so "4+ guests" or "4+ nights" don't trigger.
_RATING_GTE_PLUS_PATTERN = re.compile(
    r"\b(\d+(?:\.\d+)?)\+\s*(?:stars?\s+)?rating\b",
    re.IGNORECASE,
)

# "5-star hotels", "5 star property", "five star hotels in Goa", "three-star".
# CRITICAL: requires "hotel"/"property"/"properties" RIGHT AFTER the "star"
# token so "5 star service was great" (no hotel-noun) does NOT match.
_STAR_PATTERN = re.compile(
    r"\b(\d|one|two|three|four|five)\s*-?\s*star\s+(?:hotel|propert)",
    re.IGNORECASE,
)

# Word-form numbers map (only 1–5 — hotel star ratings don't go higher).
_STAR_WORD_TO_INT = {
    "one":   1, "two":   2, "three": 3, "four":  4, "five":  5,
}


def detect_filters(query: str) -> dict:
    """
    Extract structured filters from a natural-language query.

    Returns a dict with any subset of:
        avg_rating_gte: float   — "rating above 4", "4+ rating", "rated over 3.5"
        avg_rating_lte: float   — "rating below 3", "rated under 2.5"
        star_category:  int     — "5-star hotels", "three star property"
        city:           str     — "complaints in Goa" → "goa" (lowercased)

    Returns {} when no filter is detected (→ pure semantic path).

    Design — false positives are worse than false negatives. A spurious
    filter silently drops legitimate results; a missed filter just means
    the search is broader than it could have been. All patterns require
    explicit keywords (rating / rated / star+noun) near the number.
    """
    filters: dict = {}

    m = _RATING_GTE_PATTERN.search(query)
    if m:
        filters["avg_rating_gte"] = float(m.group(1))

    m = _RATING_GTE_PLUS_PATTERN.search(query)
    if m and "avg_rating_gte" not in filters:
        filters["avg_rating_gte"] = float(m.group(1))

    m = _RATING_LTE_PATTERN.search(query)
    if m:
        filters["avg_rating_lte"] = float(m.group(1))

    m = _STAR_PATTERN.search(query)
    if m:
        token = m.group(1).lower()
        filters["star_category"] = (
            _STAR_WORD_TO_INT[token] if token in _STAR_WORD_TO_INT else int(token)
        )

    # City: reuse semantic_search's known-city list and detector. Requires the
    # city-list to be loaded first — semantic_search.run() loads it lazily on
    # its first invocation, but detect_filters() may run before that, so we
    # trigger the load here too. Both are idempotent / cached.
    _ensure_cities_loaded()
    city = _detect_city(query)
    if city:
        filters["city"] = city

    return filters


def _ensure_cities_loaded() -> None:
    """
    Trigger semantic_search._load_cities so _detect_city has its known-city
    set populated. Idempotent — _load_cities is a no-op after the first call.
    We open a short-lived connection rather than threading one through, since
    this only fires on the first detect_filters() call per process.
    """
    import psycopg2
    from ai.semantic_search import DB_CONFIG, _KNOWN_CITIES
    if _KNOWN_CITIES:
        return
    try:
        conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)
        try:
            _load_cities(conn)
        finally:
            conn.close()
    except Exception as e:
        # If the DB is down, filter detection just won't include city — the
        # other (regex-only) filters still work. Same defensive posture as
        # text_to_sql._load_schema.
        log.warning("detect_filters: city list unavailable (%s)", e)


def answer(query: str) -> dict:
    """
    Main entry point for Phase 5.

    Args:
        query: Plain English question from the user.

    Returns:
        Result dict from text_to_sql.run() or semantic_search.run().
        Always contains 'path' and 'error' keys.
        SQL path also has:      'sql', 'columns', 'rows'
        Semantic path also has: 'city', 'reviews', 'summary'
        Hybrid path also adds:  'filters' (the detected filter dict)
                                'hotel_id_count' (number of hotels matched)

    Hybrid routing (B-004):
        The keyword router still decides sql vs semantic. We then ADDITIONALLY
        run detect_filters on every query. If the router chose semantic AND
        detect_filters found a non-city structured filter (rating / star), we
        pre-resolve a hotel_id list from those filters and pass it to
        semantic_search so the review search is scoped to matching hotels.

        City-only filters do NOT trigger hybrid — semantic_search already
        scopes by city internally, and going through hotel_ids for a pure
        city query would add a round trip without changing results.

    Never raises — errors are captured in result['error'].
    """
    path = route(query)
    log.info("Routing '%s' → %s path", query, path)

    # SQL path is unchanged — B-004 only affects the semantic side.
    if path != "semantic":
        return sql_run(query)

    # ── Semantic path: detect structured filters before deciding hybrid ───
    filters = detect_filters(query)

    # Split out city — present on its own, it does NOT trigger hybrid
    # because semantic_search's own city scoping already covers it.
    non_city_filters = {k: v for k, v in filters.items() if k != "city"}

    if not non_city_filters:
        # Pure semantic (possibly city-scoped) — existing behaviour intact.
        return semantic_run(query)

    # ── Hybrid path: rating/star (optionally + city) → scope to hotel_ids ──
    log.info("Hybrid query detected — filters=%s", filters)
    hotel_ids = _resolve_hotel_ids(filters)
    log.info("Filters resolved to %d hotel_ids", len(hotel_ids))

    if not hotel_ids:
        # No hotels match the filters — short-circuit. Returning an
        # unscoped semantic search here would silently ignore the user's
        # filter, which is the failure mode B-004 was opened to fix.
        return {
            "path":           "semantic",
            "query":          query,
            "city":           filters.get("city"),
            "reviews":        [],
            "summary":        "No hotels match the requested filters.",
            "error":          None,
            "filters":        filters,
            "hotel_id_count": 0,
        }

    result = semantic_run(query, hotel_ids=hotel_ids)
    # Annotate the result so the dashboard / debug surfaces can see that
    # scoping was applied and what filters drove it. MERGE (not assign) so the
    # B-062 polarity keys semantic_run() may have already written into
    # result["filters"] survive alongside the B-004 rating/star/city keys —
    # the two key-sets are disjoint, so neither transparency is lost.
    result["filters"]        = {**result.get("filters", {}), **filters}
    result["hotel_id_count"] = len(hotel_ids)
    return result


# ── CLI entry point ───────────────────────────────────────────────────────────
# Used during development to test queries without building Phase 5.
# Prints a human-readable summary of the result dict.

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ai/main.py \"your question here\"")
        print()
        print("Examples:")
        print("  python ai/main.py \"top 5 cities by revenue\"")
        print("  python ai/main.py \"complaints about AC in Goa hotels\"")
        print("  python ai/main.py \"cancellation rate by customer segment\"")
        print("  python ai/main.py \"what are guests saying about cleanliness\"")
        sys.exit(1)

    query  = " ".join(sys.argv[1:])
    result = answer(query)

    # ── Print result ──────────────────────────────────────────────────────────
    print()
    print(f"Path  : {result['path']}")
    print(f"Query : {result['query']}")

    # Error — something went wrong in the pipeline
    if result.get("error"):
        print(f"Error : {result['error']}")
        sys.exit(1)

    # SQL path result
    if result["path"] == "sql":
        print(f"SQL   : {result['sql']}")
        print(f"Cols  : {result['columns']}")
        print(f"Rows  : {len(result['rows'])} returned (capped at 100)")
        print()
        # Print first 10 rows as a preview
        for i, row in enumerate(result["rows"][:10], 1):
            print(f"  {i:2d}. {row}")
        if len(result["rows"]) > 10:
            print(f"  ... {len(result['rows']) - 10} more rows")

    # Semantic path result
    elif result["path"] == "semantic":
        print(f"City  : {result['city'] or 'all cities (no city detected)'}")
        print(f"Reviews retrieved: {len(result['reviews'])}")
        print()
        print("Top reviews:")
        for i, r in enumerate(result["reviews"][:5], 1):
            print(f"  {i}. [{r['rating']}★] sim={r['similarity']}  {r['review_text'][:120]}")
        print()
        print("Ollama summary:")
        print(result["summary"])
