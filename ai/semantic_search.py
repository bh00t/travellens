"""
semantic_search.py — pgvector Cosine Search + Ollama Theme Summary
===================================================================
Part of: TravelLens Phase 4 — AI Layer
File:    ai/semantic_search.py

What this file does:
    Takes a plain English question about reviews/opinions and returns:
      - The top 20 most semantically similar reviews from reviews_raw
      - An Ollama-generated summary of the top 3 themes in those reviews

    Flow:
      1. Detect if the query mentions a known city → scope the search
      2. Embed the query using all-MiniLM-L6-v2 → 384-dim vector
      3. pgvector cosine similarity search → top-20 most similar reviews
      4. Send those 20 reviews to Ollama → 3-theme summary

Why embed the query, not use keywords:
    A keyword search for "AC not working" would miss "room was too hot",
    "sweating all night", "no cooling in the room". Embedding the query
    produces a vector in the same semantic space as the review embeddings
    from Phase 3. Geometrically close vectors = similar meaning.
    That's what makes this powerful.

Why city scoping:
    Without it, "complaints in Goa" might return reviews from Mumbai hotels
    that happen to be semantically similar. We detect city mentions in the
    query and add a WHERE filter to restrict results to that city's hotels.

Why TOP_K = 20:
    Enough reviews to find real patterns (3+ reviews mentioning the same
    issue = a theme). Not so many that Ollama's context window is overwhelmed
    or the summary becomes generic. 20 is the right balance.

Why Ollama for summarisation, not just returning raw reviews:
    20 reviews at 200 chars each is a wall of text. Ollama extracts the 3
    most specific themes in 3-5 sentences. Phase 5 renders both the summary
    and the raw reviews so the user can verify.
"""

import os
import logging
import requests
import psycopg2
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")

# Number of reviews to retrieve from pgvector before summarisation.
# Rule of thumb: enough to find patterns, not so many that Ollama slows down.
TOP_K = 20

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

log = logging.getLogger(__name__)

# ── Model (loaded once at module import) ──────────────────────────────────────
# Loading SentenceTransformer takes ~2 seconds. Doing it at import time means
# the first query pays that cost, all subsequent queries reuse the loaded model.
# Do NOT move this inside run() — it would reload on every single query call.
_model = SentenceTransformer("all-MiniLM-L6-v2")

# Known cities for query scoping — populated from dim_location on first use.
# Using a set for O(1) membership checks.
_KNOWN_CITIES: set[str] = set()


# ── Helper functions ──────────────────────────────────────────────────────────

def _load_cities(conn) -> None:
    """
    Populate _KNOWN_CITIES from dim_location on first call.
    Subsequent calls are no-ops (set is already populated).

    Why dim_location and not hotel_master:
        Cities are the authoritative dimension. hotel_master references
        dim_location via location_id FK. Never read cities from hotel_master.
    """
    global _KNOWN_CITIES
    if _KNOWN_CITIES:
        return  # already loaded
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT LOWER(city) FROM dim_location")
        _KNOWN_CITIES = {row[0] for row in cur.fetchall()}
    log.info("Loaded %d known cities from dim_location", len(_KNOWN_CITIES))


def _detect_city(query: str) -> str | None:
    """
    Scan the query for any known city name (case-insensitive).
    Returns the first match, or None if no city is mentioned.

    This is used to scope the pgvector search to a specific city's hotels.
    Without scoping, "complaints in Goa" could return reviews from any city
    that happen to be semantically similar.
    """
    q = query.lower()
    for city in _KNOWN_CITIES:
        if city in q:
            return city
    return None


def _search_reviews(conn, query_vec: list, city: str | None) -> list[dict]:
    """
    Run pgvector cosine similarity search against reviews_raw.embedding.

    The <=> operator is pgvector's cosine distance operator.
    Cosine distance = 1 - cosine similarity.
    So ORDER BY embedding <=> query_vec ASC returns the most similar first.

    When city is provided:
        JOINs hotel_master and dim_location to filter by city.
        This ensures only reviews from that city's hotels are returned.

    When city is None:
        Searches all 30K reviews globally — no city filter.

    Returns a list of dicts, one per review.
    """
    if city:
        # City-scoped search — filter by hotel location
        sql = """
            SELECT
                r.hotel_id,
                r.rating,
                ROUND((1 - (r.embedding <=> %s::vector))::numeric, 4) AS similarity,
                r.review_text
            FROM reviews_raw r
            JOIN hotel_master h ON r.hotel_id   = h.hotel_id
            JOIN dim_location l ON h.location_id = l.location_id
            WHERE LOWER(l.city) = %s
            ORDER BY r.embedding <=> %s::vector
            LIMIT %s
        """
        params = [query_vec, city, query_vec, TOP_K]
    else:
        # Global search — no city filter
        sql = """
            SELECT
                r.hotel_id,
                r.rating,
                ROUND((1 - (r.embedding <=> %s::vector))::numeric, 4) AS similarity,
                r.review_text
            FROM reviews_raw r
            ORDER BY r.embedding <=> %s::vector
            LIMIT %s
        """
        params = [query_vec, query_vec, TOP_K]

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [
        {
            "hotel_id":    r[0],
            "rating":      float(r[1]),
            "similarity":  float(r[2]),
            "review_text": r[3],
        }
        for r in rows
    ]


def _summarise(query: str, reviews: list[dict]) -> str:
    """
    Send the retrieved reviews to Ollama and extract the top 3 themes.

    The prompt is structured to produce specific, actionable themes —
    not generic categories like "cleanliness" but actual patterns like
    "Multiple guests mentioned AC units that made loud rattling noises at night".

    Each review is truncated to 300 chars before sending to Ollama to keep
    the total prompt size manageable. The first 300 chars contain the key
    complaint/praise in most hotel reviews.

    Returns the Ollama response as a plain string.
    Returns a fallback message if Ollama is unreachable.
    """
    # Build the review block to send to Ollama
    review_block = "\n".join(
        f"[{r['rating']}★] {r['review_text'][:300]}"
        for r in reviews
    )

    prompt = f"""You are analysing hotel reviews from India.

User question: {query}

Here are the {len(reviews)} most relevant reviews:
{review_block}

Identify the top 3 specific themes that answer the user's question.
Be specific — mention actual issues or praises, not generic categories.
Keep each theme to 1-2 sentences.
Format as:
1. <theme>
2. <theme>
3. <theme>"""

    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()

    except requests.exceptions.ConnectionError:
        return "Summary unavailable — Ollama not reachable."
    except Exception as e:
        return f"Summary unavailable: {e}"


# ── Main entry point ──────────────────────────────────────────────────────────

def run(user_query: str) -> dict:
    """
    Full semantic search pipeline. Called by ai/main.py.

    Returns a result dict that Phase 5 renders into HTML:
    {
        "path":     "semantic",
        "query":    <original user question>,
        "city":     <detected city string, or None>,
        "reviews":  [
            {
                "hotel_id":    <str>,
                "rating":      <float>,
                "similarity":  <float 0-1>,
                "review_text": <str>
            },
            ...  (up to TOP_K items)
        ],
        "summary":  <Ollama 3-theme summary string>,
        "error":    None  — or error message string if anything failed
    }

    Design note: errors go into the dict, not raised as exceptions.
    Phase 5 renders a friendly error page instead of crashing.
    """
    result = {
        "path":    "semantic",
        "query":   user_query,
        "city":    None,
        "reviews": [],
        "summary": None,
        "error":   None,
    }

    try:
        # Step 1 — Connect to Postgres
        conn = psycopg2.connect(**DB_CONFIG)
        register_vector(conn)  # tells psycopg2 how to handle vector type

        # Step 2 — Load known cities for scoping (no-op after first call)
        _load_cities(conn)

        # Step 3 — Detect city in query
        city           = _detect_city(user_query)
        result["city"] = city
        if city:
            log.info("City detected in query: %s — scoping search", city)
        else:
            log.info("No city detected — global search")

        # Step 4 — Embed the user query
        # Same model used in Phase 3 to embed reviews_raw — vectors are in
        # the same geometric space, so cosine similarity is meaningful.
        query_vec = _model.encode(user_query).tolist()

        # Step 5 — pgvector cosine similarity search
        reviews        = _search_reviews(conn, query_vec, city)
        result["reviews"] = reviews
        log.info("Retrieved %d reviews (city=%s)", len(reviews), city or "all")

        conn.close()

        # Step 6 — Ollama theme summarisation
        if reviews:
            result["summary"] = _summarise(user_query, reviews)
        else:
            result["summary"] = "No relevant reviews found for this query."

    except Exception as e:
        result["error"] = str(e)
        log.error("Semantic search failed: %s", e)

    return result
