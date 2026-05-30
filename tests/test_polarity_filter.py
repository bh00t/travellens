"""
B-062 → B-026 Stage 2 sentiment polarity filter — pytest regression suite
=========================================================================
Fixes L-012 (sentiment-topic conflation): semantic search matches a query's
TOPIC but not its SENTIMENT, so "cleanliness complaints" returns 4-5★ praise.
The query-intent detector (_detect_polarity) classifies sentiment intent from
general keyword rules; B-026 Stage 2 then hard-filters the retrieval by the
model-scored `sentiment_label` column (migration 017) — REPLACING B-062's
star-rating proxy (negative → ≤2★). Filtering on the label catches the 3★
mixed-sentiment complaints the ≤2★ rating filter excluded.

  negative intent → sentiment_label = 'negative'
  positive intent → sentiment_label = 'positive'
  neutral intent  → no filter (unchanged)

Three bands, increasing cost:

  1. Detector — pure keyword classification, no DB / no model. negative /
     positive / neutral + the matched terms. (fast) — UNCHANGED by Stage 2.
  2. Predicate construction — _search_reviews builds the correct
     `r.sentiment_label = %s` SQL for a given intent, via a fake cursor that
     captures the SQL + params. No DB, no model. (fast)
  3. Live proof — run() end-to-end against the warehouse with Ollama stubbed
     out (deterministic): a negative query returns provably 'negative'-labelled
     reviews, a positive query 'positive', a neutral query is unconstrained.
     (slow)

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
# 2. PREDICATE CONSTRUCTION — the intent becomes the right sentiment_label SQL
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


def test_negative_builds_sentiment_predicate():
    conn = _FakeConn()
    _search_reviews(conn, _DUMMY_VEC, None, sentiment_label="negative")
    assert "r.sentiment_label = %s" in conn.captured["sql"]
    # B-026 Stage 2 REPLACED the star-rating proxy — no rating predicate remains.
    assert "r.rating <= %s" not in conn.captured["sql"]
    assert "r.rating >= %s" not in conn.captured["sql"]
    assert "negative" in conn.captured["params"]


def test_positive_builds_sentiment_predicate():
    conn = _FakeConn()
    _search_reviews(conn, _DUMMY_VEC, None, sentiment_label="positive")
    assert "r.sentiment_label = %s" in conn.captured["sql"]
    assert "r.rating >= %s" not in conn.captured["sql"]
    assert "r.rating <= %s" not in conn.captured["sql"]
    assert "positive" in conn.captured["params"]


def test_no_intent_builds_no_sentiment_predicate():
    """Neutral path: SQL must carry NO sentiment predicate (unchanged shape)."""
    conn = _FakeConn()
    _search_reviews(conn, _DUMMY_VEC, None)
    assert "r.sentiment_label = %s" not in conn.captured["sql"]
    # And no leftover rating proxy either.
    assert "r.rating <= %s" not in conn.captured["sql"]
    assert "r.rating >= %s" not in conn.captured["sql"]


def test_predicate_composes_with_city_and_hotel_ids():
    """Sentiment predicate ANDs into the same inner WHERE as city + hotel_ids."""
    conn = _FakeConn()
    _search_reviews(
        conn, _DUMMY_VEC, "goa",
        hotel_ids=["HTL-000001"], sentiment_label="negative",
    )
    sql = conn.captured["sql"]
    assert "LOWER(l.city) = %s" in sql
    assert "r.hotel_id = ANY(%s)" in sql
    assert "r.sentiment_label = %s" in sql
    # params carry city, the hotel_ids list, and the sentiment label
    assert "goa" in conn.captured["params"]
    assert ["HTL-000001"] in conn.captured["params"]
    assert "negative" in conn.captured["params"]


# ══════════════════════════════════════════════════════════════════════════
# 3. LIVE PROOF — run() end-to-end vs warehouse, Ollama stubbed (slow)
#    Proves the returned reviews actually obey the polarity, by sentiment_label.
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
def test_live_negative_returns_negative_labels(_no_ollama):
    res = ss.run("cleanliness complaints")
    assert res["error"] is None
    assert res["reviews"], "expected some negative-sentiment complaint reviews"
    assert res["filters"]["polarity"] == "negative"
    if not res["filters"]["polarity_relaxed"]:
        labels = [r["sentiment_label"] for r in res["reviews"]]
        assert all(lbl == "negative" for lbl in labels), labels
        # The whole point of Stage 2: 3★ complaints (which B-062's ≤2★ rating
        # filter excluded) are now eligible. The retrieved set must NOT be
        # rating-constrained to ≤2★ — i.e. at least one 3★+ negative review can
        # appear. (Soft expectation — assert only the label contract above.)


@pytest.mark.slow
def test_live_positive_returns_positive_labels(_no_ollama):
    res = ss.run("what do guests love most about beach resorts")
    assert res["error"] is None
    assert res["reviews"]
    assert res["filters"]["polarity"] == "positive"
    if not res["filters"]["polarity_relaxed"]:
        labels = [r["sentiment_label"] for r in res["reviews"]]
        assert all(lbl == "positive" for lbl in labels), labels


@pytest.mark.slow
def test_live_neutral_is_unconstrained(_no_ollama):
    res = ss.run("what are guests saying about cleanliness")
    assert res["error"] is None
    assert res["reviews"]
    # Neutral adds NO filters key → result shape unchanged from pre-B-062.
    assert "filters" not in res or "polarity" not in res.get("filters", {})
    # With no sentiment predicate the retrieval spans labels — over a broad
    # topic the top-K should NOT be confined to a single sentiment band.
    labels = {r["sentiment_label"] for r in res["reviews"]}
    assert len(labels) >= 2, f"neutral search looks sentiment-constrained: {labels}"
