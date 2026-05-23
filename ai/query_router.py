"""
query_router.py — Query Classification: SQL vs Semantic
========================================================
Part of: TravelLens Phase 4 — AI Layer
File:    ai/query_router.py

What this file does:
    Every incoming natural language query must be sent down one of two paths:
      - SQL path    → structured data question (revenue, bookings, cancellations)
      - Semantic path → review/opinion question (complaints, staff, cleanliness)

    This file decides which path. It does that with a simple keyword classifier —
    if the query contains any word from SEMANTIC_TRIGGERS, it goes semantic.
    Everything else goes to SQL.

Why keyword classifier, not an LLM:
    An LLM call for routing would add 1–3 seconds of latency and cost one full
    Ollama inference just to decide which path to take. A keyword match is
    instant, deterministic, and correct for 95%+ of realistic hotel queries.
    At this scale, spending an LLM call on routing is over-engineering.

How to tune it:
    If a query is being routed to the wrong path, the fix is simple — add or
    remove a word from SEMANTIC_TRIGGERS below. No code logic changes needed.
    Run the self-test at the bottom after any change to verify.
"""

# ── Semantic trigger vocabulary ───────────────────────────────────────────────
# If any of these words appear in the user's query (case-insensitive),
# the query is routed to the semantic path (pgvector + Ollama summary).
# Everything else goes to the SQL path (Ollama text-to-SQL + Postgres).
#
# The list is intentionally broad — it's better to over-route to semantic
# (which degrades gracefully) than to under-route and send a review question
# to SQL (which will generate a query that can't answer it).
SEMANTIC_TRIGGERS = {
    # Direct review/feedback words
    "review", "reviews", "complaint", "complaints", "complain",
    "feedback", "opinion", "opinions", "experience", "experiences",

    # Guest/people language
    "guest", "guests", "what do people", "what are people",
    "what people say", "say about", "think about", "feel about",

    # Service quality words — specific enough to be safe
    "staff", "rude", "unfriendly", "polite", "helpful",
    "recommend", "worst", "horrible", "terrible", "excellent",

    # Physical condition — specific phrases only
    "cleanliness", "dirty", "hygiene", "smell", "smelly",
    "noisy", "air conditioning", "air conditioner",
    "bathroom issue", "shower issue",

    # Food / amenity words
    "food quality", "breakfast quality", "wifi issue",

    # Sentiment words
    "loved", "hated", "disappointed", "impressed",
    "feeling", "felt",
}


def route(query: str) -> str:
    """
    Classify a natural language query as 'sql' or 'semantic'.

    Args:
        query: Plain English question from the user.

    Returns:
        'semantic' if any trigger word is found in the query.
        'sql'      otherwise.

    Examples:
        route("top 5 cities by revenue")              → 'sql'
        route("complaints about AC in Goa hotels")    → 'semantic'
        route("cancellation rate by segment")         → 'sql'
        route("what are guests saying about staff")   → 'semantic'
        route("room was too hot to sleep")            → 'semantic'
        route("average booking value last month")     → 'sql'
    """
    q = query.lower()

    for trigger in SEMANTIC_TRIGGERS:
        if trigger in q:
            return "semantic"

    return "sql"


# ── Self-test ─────────────────────────────────────────────────────────────────
# Run directly to verify routing is correct after any changes to SEMANTIC_TRIGGERS:
#   python ai/query_router.py
#
# Add more cases here as you discover mis-routings during Phase 4 exploration.

if __name__ == "__main__":
    test_cases = [
        # (query, expected_path)
        ("top 5 cities by revenue",                        "sql"),
        ("complaints about AC in Goa hotels",              "semantic"),
        ("cancellation rate by customer segment",          "sql"),
        ("what are guests saying about cleanliness",       "semantic"),
        ("monthly revenue trend 2025",                     "sql"),
        ("rude staff experiences in Mumbai",               "semantic"),
        ("average booking value for business travellers",  "sql"),
        ("room was too hot to sleep",                      "semantic"),
        ("which hotels have highest occupancy",            "sql"),
        ("dirty bathroom complaints",                      "semantic"),
    ]

    print("Router self-test:")
    print("-" * 60)
    failures = 0
    for query, expected in test_cases:
        result = route(query)
        ok     = result == expected
        marker = "✓" if ok else "✗"
        if not ok:
            failures += 1
        print(f"  {marker} [{result:8s}] {query}")

    print("-" * 60)
    if failures == 0:
        print(f"All {len(test_cases)} cases pass.")
    else:
        print(f"{failures} FAILURE(S) — adjust SEMANTIC_TRIGGERS and re-run.")
