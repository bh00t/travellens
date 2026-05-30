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
import re
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

# Dedup key length for DISTINCT ON in _search_reviews (B-006 follow-up).
#
# This is NOT a magic number — it's data-derived. The Kaggle dataset contains
# clusters of review_text values that share a ~190-character opener and
# diverge only in a short closing sentence (e.g. 24 distinct full texts all
# start with "Honestly forgot we even booked this hotel until the bill came
# through..."). Byte-exact DISTINCT ON (review_text) sees those as distinct
# rows and lets them flood the top-K, producing visually duplicate results.
#
# Empirical sizing (against the live reviews_raw table):
#     30,000 total rows
#     27,608 distinct full review_text
#     26,066 distinct LEFT(review_text, 200)  ← chosen length
#     19,598 distinct LEFT(review_text, 150)  — too aggressive
#     10,408 distinct LEFT(review_text, 120)  — collapses real reviews
#
# LEFT(200) collapses ~1,500 near-duplicate clusters across the whole dataset
# while preserving 26,066 genuinely distinct reviews. Good balance: tight
# enough to make the "Honestly forgot" opener show at most once or twice in
# the top-K, loose enough not to merge unrelated reviews.
DEDUP_PREFIX_LEN = 200

# ── Polarity filter (B-062 — mitigates L-012) ──────────────────────────────────
#
# Semantic search matches a query's TOPIC but ignores its SENTIMENT: "cleanliness
# complaints" returns 4-5★ reviews PRAISING cleanliness, because both praise and
# complaints are *about* cleanliness and so embed close together. On the live
# corpus that's 19,661 high-rated cleanliness reviews drowning out 2,928 genuine
# ≤2★ complaints. This filter uses the rating already on each review (numeric(3,1)
# on reviews_raw) as a COARSE sentiment proxy: when the query implies negative
# intent we hard-filter to ≤2★, when positive to ≥4★, otherwise leave the search
# unchanged.
#
# This is a MITIGATION, not the fix. Rating is unreliable at the margins — real
# complaints live in mixed-sentiment 3★ reviews, and identical texts exist across
# stars (see L-012). The deeper fix is sentiment-at-embed-time (B-026). We trade
# recall for precision deliberately: flipping the dominant polarity is worth
# missing the minority of complaints buried in 3★ reviews.
#
# Why HARD filter, not a soft re-rank bias: high-rated topical matches vastly
# outnumber low-rated ones (cleanliness: 19.6K vs 2.9K), so a soft bias that
# retrieves more and re-ranks would still come out majority-praise after the
# TOP_K cut — it would not flip the polarity. A hard rating predicate does. The
# only downside (too few results in a narrow city scope) is handled by the
# relax-and-note fallback in run().
NEGATIVE_MAX_RATING  = 2.0   # negative-intent query → rating <= this
POSITIVE_MIN_RATING  = 4.0   # positive-intent query → rating >= this
MIN_POLARITY_RESULTS = 5     # below this, relax the polarity filter (and flag it)

# General keyword lexicons — NOT per-query patterns. The detector counts how many
# DISTINCT terms from each side appear in the query and picks the larger side;
# ties and no-matches default to neutral (no filter). "clean" is deliberately
# absent from both — it's a topic word, not a sentiment word.
_NEGATIVE_TERMS = [
    "complaint", "complaints", "complain", "complaining", "complained",
    "problem", "problems", "issue", "issues", "bad", "worst", "terrible",
    "awful", "poor", "horrible", "dirty", "filthy", "filth", "unhygienic",
    "disappointing", "disappointed", "dissatisfied", "unhappy", "avoid",
    "rude", "gripe", "gripes", "downside", "downsides", "drawback",
    "drawbacks", "dislike", "disliked", "hate", "hated", "negative",
    "nightmare", "smelly", "broken", "worse",
]
_POSITIVE_TERMS = [
    "best", "praise", "praised", "love", "loved", "great", "excellent",
    "amazing", "wonderful", "fantastic", "favourite", "favorite",
    "recommend", "recommended", "highlight", "highlights", "happy",
    "satisfied", "enjoyed", "delightful", "positive", "superb", "perfect",
    "lovely", "pleasant",
]

# Word-boundary alternation so "love" doesn't match inside "glove" and "best"
# doesn't match inside "bestseller". Non-capturing terms; findall returns the
# matched substrings so the caller can report which terms fired.
_NEG_RE = re.compile(r"\b(?:" + "|".join(map(re.escape, _NEGATIVE_TERMS)) + r")\b", re.IGNORECASE)
_POS_RE = re.compile(r"\b(?:" + "|".join(map(re.escape, _POSITIVE_TERMS)) + r")\b", re.IGNORECASE)


def _detect_polarity(query: str) -> tuple[str, list[str]]:
    """
    Classify a query's sentiment intent from general keyword rules (B-062).

    Returns (polarity, matched_terms) where polarity is one of:
        "negative" — more distinct negative terms than positive  → ≤2★ filter
        "positive" — more distinct positive terms than negative  → ≥4★ filter
        "neutral"  — tie or no sentiment terms                   → no filter

    matched_terms is the sorted, de-duplicated list of terms from the WINNING
    side (empty for neutral) — recorded in the result's `filters` field for
    transparency.

    Counts DISTINCT terms, not occurrences, so a single word repeated doesn't
    outweigh a different word on the other side. Default is neutral: when the
    query carries no clear sentiment, the search behaves exactly as before.
    """
    neg_terms = sorted({m.lower() for m in _NEG_RE.findall(query)})
    pos_terms = sorted({m.lower() for m in _POS_RE.findall(query)})

    if len(neg_terms) > len(pos_terms):
        return "negative", neg_terms
    if len(pos_terms) > len(neg_terms):
        return "positive", pos_terms
    return "neutral", []


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


def _resolve_hotel_ids(filters: dict) -> list[str]:
    """
    B-004 helper: convert a detect_filters() dict into a list of hotel_ids.

    Filters supported (all optional; ANDed together):
        avg_rating_gte: float  →  hotel_master.avg_rating >= %s
        avg_rating_lte: float  →  hotel_master.avg_rating <= %s
        star_category:  int    →  hotel_master.star_category = %s
        city:           str    →  dim_location.city = %s (lowercased)

    Schema reference (verified against \\d hotel_master and \\d dim_location;
    do NOT change these column names from memory):
        hotel_master.avg_rating    numeric(3,2)
        hotel_master.star_category smallint
        hotel_master.location_id   uuid → dim_location.location_id
        dim_location.city          varchar

    Returns the list of hotel_ids matching ALL filters. Empty list when
    filters select no hotels — the caller should short-circuit to a
    friendly "no matching hotels" result instead of running an unscoped
    semantic search (which would silently ignore the user's filter).

    Returns [] without hitting Postgres when `filters` is empty or
    contains no resolvable conditions.
    """
    if not filters:
        return []

    conditions: list[str] = []
    params: list = []

    if "avg_rating_gte" in filters:
        conditions.append("h.avg_rating >= %s")
        params.append(filters["avg_rating_gte"])

    if "avg_rating_lte" in filters:
        conditions.append("h.avg_rating <= %s")
        params.append(filters["avg_rating_lte"])

    if "star_category" in filters:
        conditions.append("h.star_category = %s")
        params.append(filters["star_category"])

    needs_loc_join = "city" in filters
    if needs_loc_join:
        conditions.append("LOWER(l.city) = %s")
        params.append(filters["city"])

    if not conditions:
        return []

    join_clause = (
        "JOIN dim_location l ON h.location_id = l.location_id"
        if needs_loc_join else ""
    )
    sql = f"""
        SELECT h.hotel_id
        FROM hotel_master h
        {join_clause}
        WHERE {' AND '.join(conditions)}
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _search_reviews(
    conn,
    query_vec: list,
    city: str | None,
    hotel_ids: list[str] | None = None,
    rating_max: float | None = None,
    rating_min: float | None = None,
) -> list[dict]:
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

    Deduplication (B-006 + B-006 follow-up):
        The Kaggle source has two layers of duplication:
          1. Byte-exact duplicates (same review_text on multiple hotel_ids).
          2. Near-duplicate clusters: 20+ distinct review_text values that
             share a ~190-char opener and diverge in a short closing
             sentence. These are visually identical to a user but distinct
             to Postgres.

        We dedup on LEFT(review_text, DEDUP_PREFIX_LEN) — see the constant
        at the top of this module for the data-derived rationale. Postgres
        requires the DISTINCT ON expression to lead the ORDER BY of the
        SAME query level, so the inner subquery orders by
        (LEFT(review_text, N), distance) — picking the SMALLEST-distance
        row per unique prefix. The outer query then re-orders the
        survivors by distance and caps at TOP_K, so the LIMIT applies to
        deduped rows, not the raw set.

        WHERE embedding IS NOT NULL is a safety filter — Phase 3 backfilled
        all rows, but the guard prevents a NULL embedding from ever
        sneaking past as cosine distance against NULL would be undefined.

    Hybrid scoping (B-004):
        When `hotel_ids` is a non-empty list, an additional
        `r.hotel_id = ANY(%s)` predicate is added to the SAME inner WHERE,
        so the dedup picks the closest match per unique review_text WITHIN
        the filtered hotel set. This shares the dedup path with the city
        filter rather than wrapping a second subquery around it — keeping
        one execution plan instead of nesting filters.

        The hotel_ids list comes from _resolve_hotel_ids(), which already
        encodes any geographic scope. When `hotel_ids` is None the existing
        global / city-scoped behaviour is preserved byte-for-byte.

    Polarity filter (B-062):
        `rating_max` / `rating_min`, when given, add `r.rating <= %s` /
        `r.rating >= %s` to the SAME inner WHERE as the city and hotel_ids
        filters — so the rating bound, geographic scope, and dedup all stay
        in one execution plan. The caller (run()) sets at most one of them
        from the detected query polarity (negative → rating_max=2.0,
        positive → rating_min=4.0). Both None → unchanged behaviour. This
        rides on the review-grain `reviews_raw.rating`; it is independent of
        and composes with B-004's hotel-grain `hotel_master.avg_rating`
        filter (different column, different grain).

    Returns a list of dicts, one per review.
    """
    # Build the inner subquery dynamically so the dedup + scoping stay in
    # one execution plan. There are four combinations (city × hotel_ids,
    # each present or absent); the dynamic build keeps them all in one
    # path instead of duplicating SQL with copy/paste drift risk.
    #
    # The dedup expression LEFT(r.review_text, %s) is bound TWICE in the
    # SQL: once as the DISTINCT ON key and once as the leading ORDER BY
    # term (Postgres requires they match). Both placeholders draw from
    # the same DEDUP_PREFIX_LEN constant — keep them in lockstep.
    inner_joins: list[str] = []
    inner_where: list[str] = ["r.embedding IS NOT NULL"]
    # Parameter order in the SQL below (left to right):
    #   1. LEFT() length for DISTINCT ON
    #   2. query_vec  (the <=> distance expression)
    #   3. (optional) city
    #   4. (optional) hotel_ids
    #   5. LEFT() length for inner ORDER BY (same value, second binding)
    #   6. TOP_K
    inner_params: list = [DEDUP_PREFIX_LEN, query_vec]

    if city:
        inner_joins.append("JOIN hotel_master h ON r.hotel_id    = h.hotel_id")
        inner_joins.append("JOIN dim_location l ON h.location_id = l.location_id")
        inner_where.append("LOWER(l.city) = %s")
        inner_params.append(city)

    if hotel_ids:
        # ANY(%s) with a Python list — psycopg2 adapts it to a typed array.
        # Empty list is short-circuited above (`if hotel_ids:`); the caller
        # should never let an empty list reach here because that would
        # silently bypass the filter.
        inner_where.append("r.hotel_id = ANY(%s)")
        inner_params.append(list(hotel_ids))

    # Polarity rating bound (B-062). Predicate + param appended together so
    # positional %s ordering stays in lockstep with the rest of inner_where,
    # ahead of the final DEDUP_PREFIX_LEN + TOP_K bindings below.
    if rating_max is not None:
        inner_where.append("r.rating <= %s")
        inner_params.append(rating_max)
    if rating_min is not None:
        inner_where.append("r.rating >= %s")
        inner_params.append(rating_min)

    sql = f"""
        SELECT hotel_id,
               rating,
               ROUND((1 - distance)::numeric, 4) AS similarity,
               review_text
        FROM (
            SELECT DISTINCT ON (LEFT(r.review_text, %s))
                   r.hotel_id,
                   r.rating,
                   r.review_text,
                   r.embedding <=> %s::vector AS distance
            FROM reviews_raw r
            {' '.join(inner_joins)}
            WHERE {' AND '.join(inner_where)}
            ORDER BY LEFT(r.review_text, %s), distance
        ) deduped
        ORDER BY distance
        LIMIT %s
    """
    # Bindings 5 (LEFT length, second occurrence) and 6 (TOP_K) — appended
    # in SQL textual order so positional %s placeholders match.
    inner_params.append(DEDUP_PREFIX_LEN)
    inner_params.append(TOP_K)

    with conn.cursor() as cur:
        cur.execute(sql, inner_params)
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

def run(user_query: str, hotel_ids: list[str] | None = None) -> dict:
    """
    Full semantic search pipeline. Called by ai/main.py.

    Args:
        user_query: Plain English question from the user.
        hotel_ids:  Optional pre-resolved hotel_id list (B-004 hybrid path).
                    When provided, the search is scoped to these hotels AND
                    the internal _detect_city pass is SKIPPED — callers
                    pre-encode any geographic constraint into the list, so
                    a second city filter would be redundant and (in some
                    cases) over-restrictive. When None, the existing
                    pure-semantic flow runs unchanged.

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

    When the query carries a sentiment intent (B-062), a "filters" key is
    added recording the applied polarity:
        "filters": {
            "polarity":           "negative" | "positive",
            "polarity_terms":     [<matched keywords>],
            "polarity_threshold": "<=2" | ">=4",
            "polarity_relaxed":   <bool — True if the bound was dropped because
                                   it left fewer than MIN_POLARITY_RESULTS>,
        }
    Neutral queries add no "filters" key (result shape unchanged). On the
    hybrid path, main.py merges its own B-004 filter keys into this same dict.

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

        # IVFFlat probes (B-051). Postgres default is 1 partition scanned per
        # query — fine at the original lists=30, but after B-051 raised lists
        # to 120 (rule of thumb: rows/1000 ≈ 134 for ~133K embedded reviews),
        # probes=1 would scan only ~1/120 of partitions and silently drop
        # recall vs. the previous index. probes ≈ sqrt(lists) ≈ 11 restores
        # coverage. Session GUC — set per-connection, not globally.
        with conn.cursor() as cur:
            cur.execute("SET ivfflat.probes = 11")

        # Step 2 — Load known cities for scoping (no-op after first call)
        _load_cities(conn)

        # Step 3 — Detect city in query
        # Skipped when the caller supplied hotel_ids — those already encode
        # any geographic scope and an additional city filter would either
        # be redundant (matches) or wrong (drops valid hotels in other
        # cities the caller intentionally included).
        if hotel_ids is None:
            city = _detect_city(user_query)
            result["city"] = city
            if city:
                log.info("City detected in query: %s — scoping search", city)
            else:
                log.info("No city detected — global search")
        else:
            city = None
            log.info(
                "Hybrid path: scoping to %d pre-resolved hotel_ids", len(hotel_ids)
            )

        # Step 4 — Embed the user query
        # Same model used in Phase 3 to embed reviews_raw — vectors are in
        # the same geometric space, so cosine similarity is meaningful.
        query_vec = _model.encode(user_query).tolist()

        # Step 4.5 — Detect sentiment polarity (B-062, mitigates L-012)
        # General keyword rules → negative / positive / neutral. Negative
        # biases retrieval to ≤2★ reviews (actual complaints), positive to
        # ≥4★, neutral leaves the search unchanged.
        polarity, polarity_terms = _detect_polarity(user_query)
        rating_max = NEGATIVE_MAX_RATING if polarity == "negative" else None
        rating_min = POSITIVE_MIN_RATING if polarity == "positive" else None
        if polarity != "neutral":
            log.info("Polarity '%s' detected (terms=%s) — biasing by rating",
                     polarity, polarity_terms)

        # Step 5 — pgvector cosine similarity search
        reviews = _search_reviews(
            conn, query_vec, city, hotel_ids=hotel_ids,
            rating_max=rating_max, rating_min=rating_min,
        )

        # Relax-and-note fallback (B-062): a hard polarity filter can starve a
        # narrow city/hotel scope of results. When it returns too few, re-run
        # WITHOUT the rating bound and flag it, rather than show an empty or
        # misleadingly thin result. Neutral queries never enter this branch.
        polarity_relaxed = False
        if polarity != "neutral" and len(reviews) < MIN_POLARITY_RESULTS:
            log.info(
                "Polarity filter returned %d (<%d) — relaxing rating bound",
                len(reviews), MIN_POLARITY_RESULTS,
            )
            reviews = _search_reviews(conn, query_vec, city, hotel_ids=hotel_ids)
            polarity_relaxed = True

        result["reviews"] = reviews

        # Record the applied polarity in the result's `filters` field for
        # transparency (only when a filter was actually chosen — neutral
        # queries stay byte-identical to the pre-B-062 result shape). The
        # hybrid path (main.py) merges its own B-004 keys into this dict.
        if polarity != "neutral":
            result.setdefault("filters", {})
            result["filters"]["polarity"]           = polarity
            result["filters"]["polarity_terms"]     = polarity_terms
            result["filters"]["polarity_threshold"] = (
                f"<={NEGATIVE_MAX_RATING:g}" if polarity == "negative"
                else f">={POSITIVE_MIN_RATING:g}"
            )
            result["filters"]["polarity_relaxed"]   = polarity_relaxed

        log.info(
            "Retrieved %d reviews (city=%s, hotel_ids=%s, polarity=%s%s)",
            len(reviews), city or "all", "scoped" if hotel_ids else "unscoped",
            polarity, " relaxed" if polarity_relaxed else "",
        )

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
