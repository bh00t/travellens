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

import sys
import logging
from ai.query_router    import route
from ai.text_to_sql     import run as sql_run
from ai.semantic_search import run as semantic_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def answer(query: str) -> dict:
    """
    Main entry point for Phase 5.

    Args:
        query: Plain English question from the user.

    Returns:
        Result dict from text_to_sql.run() or semantic_search.run().
        Always contains 'path' and 'error' keys.
        SQL path also has: 'sql', 'columns', 'rows'
        Semantic path also has: 'city', 'reviews', 'summary'

    Never raises — errors are captured in result['error'].
    """
    path = route(query)
    logging.getLogger(__name__).info("Routing '%s' → %s path", query, path)

    if path == "semantic":
        return semantic_run(query)
    return sql_run(query)


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
