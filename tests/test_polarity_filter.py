"""
B-062 rating-based polarity filter — pytest regression suite
=============================================================
Mitigates L-012 (sentiment-topic conflation): semantic search matches a
query's TOPIC but not its SENTIMENT, so "cleanliness complaints" returns
4-5★ praise. B-062 detects sentiment intent from general keyword rules and
hard-filters the retrieval by the rating already stored on each review
(negative → ≤2★, positive → ≥4★, neutral → unchanged).

Three bands, increasing cost:

  1. Detector — pure keyword classification, no DB / no model. negative /
     positive / neutral + the matched terms. (fast)
  2. Predicate construction — _search_reviews builds the correct
     `r.rating <= / >= %s` SQL for a given bound, via a fake cursor that
     captures the SQL + params. No DB, no model. (fast)
  3. Live proof — run() end-to-end against the warehouse with Ollama stubbed
     out (deterministic): a negative query returns provably ≤2★ reviews, a
     positive query ≥4★, a neutral query is unconstrained. (slow)

Run only the fast logic:
    pytest tests/test_polarity_filter.py -v -m "not slow"

Run everything:
    pytest tests/test_polarity_filter.py -v
"""
import pytest

import ai.semantic_search as ss
from ai.semantic_search import (
    _detect_polarity,
    _search_reviews,
    NEGATIVE_MAX_RATING,
    POSITIVE_MIN_RATING,
    MIN_POLARITY_RESULTS,
)


# ══════════════════════════════════════════════════════════════════════════
# 1. DETECTOR — keyword classification (fast, no DB, no model)
# ══════════════════════════════════════════════════════════════════════════

def test_negative_complaints():
    polarity, terms = _detect_polarity("top 5 hotels with cleanliness complaints")
    assert polarity == "negative"
    assert "complaints" in terms


def test_negative_various_terms():
    for q in [
        "worst hotels for service",
        "hotels with terrible food",
        "dirty rooms in budget hotels",
        "what are the problems guests report",
    ]:
        polarity, _ = _detect_polarity(q)
        assert polarity == "negative", q


def test_positive_love():
    polarity, terms = _detect_polarity("what do guests love most about beach resorts")
    assert polarity == "positive"
    assert "love" in terms


def test_positive_best():
    polarity, terms = _detect_polarity("best things about heritage hotels")
    assert polarity == "positive"
    assert "best" in terms


def test_neutral_no_sentiment_terms():
    """The canonical L-012 'topic only' query — must stay neutral (unchanged)."""
    polarity, terms = _detect_polarity("what are guests saying about cleanliness")
    assert polarity == "neutral"
    assert terms == []


def test_neutral_on_tie():
    """Equal distinct terms each side → ambiguous → neutral (no over-filter)."""
    polarity, terms = _detect_polarity("the best and the worst things about hotels")
    assert polarity == "neutral"
    assert terms == []


def test_clean_is_not_a_polarity_term():
    """'cleanliness' / 'clean' are TOPIC words, never sentiment — no firing."""
    polarity, _ = _detect_polarity("cleanliness in Goa hotels")
    assert polarity == "neutral"


def test_word_boundary_no_false_match():
    """'glove' must not match 'love'; 'bestseller' must not match 'best'."""
    polarity, _ = _detect_polarity("hotels that provide gloves and bestseller books")
    assert polarity == "neutral"


def test_matched_terms_are_deduped_and_lowercased():
    polarity, terms = _detect_polarity("BAD bad terrible service")
    assert polarity == "negative"
    assert terms == sorted(set(terms))            # de-duplicated
    assert all(t == t.lower() for t in terms)     # lowercased
    assert "bad" in terms and "terrible" in terms


# ══════════════════════════════════════════════════════════════════════════
# 2. PREDICATE CONSTRUCTION — the rating bound becomes the right SQL
#    (fast: fake cursor captures SQL + params, no DB hit)
# ══════════════════════════════════════════════════════════════════════════

class _FakeCursor:
    """Captures the SQL + params handed to execute(); returns no rows."""
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        self._sink["sql"] = sql
        self._sink["params"] = params

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self):
        self.captured = {}

    def cursor(self):
        return _FakeCursor(self.captured)


_DUMMY_VEC = [0.0] * 384


def test_negative_bound_builds_lte_predicate():
    conn = _FakeConn()
    _search_reviews(conn, _DUMMY_VEC, None, rating_max=NEGATIVE_MAX_RATING)
    assert "r.rating <= %s" in conn.captured["sql"]
    assert "r.rating >= %s" not in conn.captured["sql"]
    assert NEGATIVE_MAX_RATING in conn.captured["params"]


def test_positive_bound_builds_gte_predicate():
    conn = _FakeConn()
    _search_reviews(conn, _DUMMY_VEC, None, rating_min=POSITIVE_MIN_RATING)
    assert "r.rating >= %s" in conn.captured["sql"]
    assert "r.rating <= %s" not in conn.captured["sql"]
    assert POSITIVE_MIN_RATING in conn.captured["params"]


def test_no_bound_builds_no_rating_predicate():
    """Neutral path: SQL must carry NO rating predicate (pre-B-062 shape)."""
    conn = _FakeConn()
    _search_reviews(conn, _DUMMY_VEC, None)
    assert "r.rating <= %s" not in conn.captured["sql"]
    assert "r.rating >= %s" not in conn.captured["sql"]


def test_bound_composes_with_city_and_hotel_ids():
    """Polarity predicate ANDs into the same inner WHERE as city + hotel_ids."""
    conn = _FakeConn()
    _search_reviews(
        conn, _DUMMY_VEC, "goa",
        hotel_ids=["HTL-000001"], rating_max=NEGATIVE_MAX_RATING,
    )
    sql = conn.captured["sql"]
    assert "LOWER(l.city) = %s" in sql
    assert "r.hotel_id = ANY(%s)" in sql
    assert "r.rating <= %s" in sql
    # params carry city, the hotel_ids list, and the rating bound
    assert "goa" in conn.captured["params"]
    assert ["HTL-000001"] in conn.captured["params"]
    assert NEGATIVE_MAX_RATING in conn.captured["params"]


# ══════════════════════════════════════════════════════════════════════════
# 3. LIVE PROOF — run() end-to-end vs warehouse, Ollama stubbed (slow)
#    Proves the returned reviews actually obey the polarity, by ratings.
# ══════════════════════════════════════════════════════════════════════════

# DB-reachability probe — a direct lightweight connect (NOT run(), which would
# fire Ollama at collection time and hang the fast tests). Skips the whole
# module's live band if the warehouse is down.
import psycopg2  # noqa: E402

try:
    _c = psycopg2.connect(connect_timeout=5, **ss.DB_CONFIG)
    _c.close()
except Exception as _e:  # pragma: no cover - environment-dependent
    pytest.skip(
        f"travellens-postgres unreachable ({_e}). Live polarity proof cannot run.",
        allow_module_level=True,
    )


@pytest.fixture
def _no_ollama(monkeypatch):
    """Stub the Ollama summary so the live tests are deterministic + fast."""
    monkeypatch.setattr(ss, "_summarise", lambda q, reviews: "(summary stubbed)")


@pytest.mark.slow
def test_live_negative_returns_low_rated(_no_ollama):
    res = ss.run("top 5 hotels with cleanliness complaints")
    assert res["error"] is None
    assert res["reviews"], "expected some low-rated complaint reviews"
    assert res["filters"]["polarity"] == "negative"
    if not res["filters"]["polarity_relaxed"]:
        ratings = [r["rating"] for r in res["reviews"]]
        assert max(ratings) <= NEGATIVE_MAX_RATING, ratings


@pytest.mark.slow
def test_live_positive_returns_high_rated(_no_ollama):
    res = ss.run("what do guests love most about beach resorts")
    assert res["error"] is None
    assert res["reviews"]
    assert res["filters"]["polarity"] == "positive"
    if not res["filters"]["polarity_relaxed"]:
        ratings = [r["rating"] for r in res["reviews"]]
        assert min(ratings) >= POSITIVE_MIN_RATING, ratings


@pytest.mark.slow
def test_live_neutral_is_unconstrained(_no_ollama):
    res = ss.run("what are guests saying about cleanliness")
    assert res["error"] is None
    assert res["reviews"]
    # Neutral adds NO filters key → result shape unchanged from pre-B-062.
    assert "filters" not in res or "polarity" not in res.get("filters", {})
    # And with no rating bound the retrieval spans the full star range — over a
    # broad topic the top-K should NOT be confined to a single polarity band.
    ratings = {r["rating"] for r in res["reviews"]}
    assert len(ratings) >= 2, f"neutral search looks rating-constrained: {ratings}"
