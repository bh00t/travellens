"""
B-004 hybrid query support — pytest regression suite
=====================================================
Covers five bands, in increasing cost:

  1. Filter detection — positive cases (fast, no DB calls beyond a one-shot
     warmup that loads dim_location.city into the known-city set).
  2. Filter detection — FALSE POSITIVES. A number in the query is NOT always
     a filter. These must NOT fire; the conservative-skip rule is what keeps
     B-004 safe (a spurious filter silently drops valid results).
  3. Hybrid scoping — the actual fix. Asserts every returned review's
     hotel_id is in a ground-truth set computed by direct psql, not by the
     same Python code under test.
  4. Regression — pure-semantic, city-only, and SQL paths carry no hybrid
     keys (`filters`, `hotel_id_count`).
  5. Edge cases — zero-match filter, empty query, and the structured
     "5 star hotels in Mumbai" query (which must route SQL, not hybrid).

Stack-hitting tests are marked @pytest.mark.slow so:
    pytest tests/test_hybrid_queries.py -v -m "not slow"
runs only the detection logic in well under a second.

Run everything:
    pytest tests/test_hybrid_queries.py -v
"""
import subprocess
from collections import Counter

import pytest

from ai.main import detect_filters, answer
from ai.query_router import route


# DB-reachability probe via behaviour, NOT via inspecting _KNOWN_CITIES.
# semantic_search._load_cities REASSIGNS the module global rather than
# mutating it in place, so a `from … import _KNOWN_CITIES` binding in this
# test module would still point at the old empty set even after the load
# succeeded. Probing through detect_filters() avoids that pitfall — if
# city detection works on a known Indian city, the DB is up and the city
# cache is populated, regardless of how we got there.
_probe = detect_filters("complaints in Goa")
if _probe.get("city") != "goa":
    pytest.skip(
        "travellens-postgres unreachable or city cache unloadable — "
        "detect_filters can't see 'Goa' in 'complaints in Goa'. "
        "Hybrid scoping cannot be verified without the warehouse.",
        allow_module_level=True,
    )


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _ground_truth_hotel_ids(where_clause: str) -> set[str]:
    """
    Ground-truth lookup via direct psql. Deliberately uses a different code
    path than `_resolve_hotel_ids` (the function under test downstream) —
    catches the case where both the helper and the production path share
    the same bug.
    """
    sql = f"SELECT hotel_id FROM hotel_master WHERE {where_clause}"
    out = subprocess.run(
        ["docker", "exec", "travellens-postgres", "psql",
         "-U", "travellens", "-d", "travellens", "-At", "-c", sql],
        capture_output=True, text=True,
    ).stdout.strip()
    return set(out.splitlines()) if out else set()


# ══════════════════════════════════════════════════════════════════════════
# 1. FILTER DETECTION — positive cases
# ══════════════════════════════════════════════════════════════════════════

def test_rating_above_integer():
    assert detect_filters("hotels with rating above 4") == {"avg_rating_gte": 4.0}


def test_rating_above_decimal():
    assert detect_filters("rated over 3.5") == {"avg_rating_gte": 3.5}


def test_rating_below():
    assert detect_filters("rating below 3") == {"avg_rating_lte": 3.0}


def test_rating_greater_than_phrase():
    """The 'greater than' variant should produce the same gte as 'above'."""
    assert detect_filters("rating greater than 4") == {"avg_rating_gte": 4.0}


def test_rating_plus_form():
    """N+ shorthand requires the 'rating' suffix to disambiguate."""
    assert detect_filters("4+ stars rating") == {"avg_rating_gte": 4.0}


def test_star_numeric():
    """Numeric 5-star + city in one query — exercises both detectors."""
    f = detect_filters("5-star hotels in Goa")
    assert f["star_category"] == 5
    assert f["city"] == "goa"


def test_star_word_three():
    """Word-form star number ('three')."""
    f = detect_filters("three star hotels")
    assert f["star_category"] == 3


def test_star_word_five():
    """Word-form 'five-star property' — hyphen variant + 'property' noun."""
    f = detect_filters("five-star property")
    assert f["star_category"] == 5


def test_city_alone():
    """City-only — no rating/star."""
    assert detect_filters("complaints in Goa") == {"city": "goa"}


def test_combination_rating_and_city():
    """Compound filter: rating + city. Both must surface."""
    f = detect_filters("rude staff in Mumbai hotels rated over 4")
    assert f["avg_rating_gte"] == 4.0
    assert f["city"] == "mumbai"


def test_combination_star_and_rating():
    """Compound filter: star + rating. Both must surface."""
    f = detect_filters("five-star hotels with rating above 4.5")
    assert f["star_category"] == 5
    assert f["avg_rating_gte"] == 4.5


# ══════════════════════════════════════════════════════════════════════════
# 2. FILTER DETECTION — FALSE POSITIVES (critical)
# A number in the query is NOT always a filter. False positives silently
# drop legitimate results — strictly worse than missed detection.
# ══════════════════════════════════════════════════════════════════════════

def test_top_5_cities_is_not_a_star_filter():
    """'top 5 cities' — 5 is a LIMIT, not a hotel star_category."""
    assert detect_filters("top 5 cities") == {}


def test_top_10_hotels_is_not_a_filter():
    """'top 10 hotels' — 10 is a LIMIT, no rating/star intent."""
    assert detect_filters("top 10 hotels") == {}


def test_4am_checkout_is_not_a_rating():
    """'4am checkout' — 4 is a time-of-day, not a rating threshold."""
    assert detect_filters("reviews mentioning 4am checkout") == {}


def test_year_2015_is_not_a_rating():
    """'reviews from 2015' — 2015 is a year, not a rating."""
    assert detect_filters("reviews from 2015") == {}


def test_5_star_service_is_ambiguous_but_safe():
    """
    '5 star service was great' — '5 star' here describes service quality,
    not a hotel star_category. The acceptance bar:
      - either the detector doesn't fire (conservative, current impl), OR
      - if it fires, the value is EXACTLY 5 (no garbage extraction).
    Whichever the impl chooses is OK; what's not OK is e.g. star_category=0.
    """
    f = detect_filters("5 star service was great")
    if "star_category" in f:
        assert f["star_category"] == 5, \
            "if star_category fires here, value must be 5 — never garbage"
    # else: didn't fire — also acceptable, conservative is safer


# ══════════════════════════════════════════════════════════════════════════
# 3. HYBRID SCOPING — the actual fix B-004 was opened to land
# Ground truth comes from direct psql, NOT from _resolve_hotel_ids, so a
# shared bug between the helper and the production path would still
# surface here.
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.slow
def test_hybrid_rating_scoping_is_real():
    """
    The acceptance proof. Every returned review's hotel_id must belong to
    the set of hotels with avg_rating >= 4 — derived independently by psql.
    """
    result = answer("hotels with most negative reviews but rating above 4")

    # Hybrid path fired and recorded the filter
    assert result["path"] == "semantic"
    assert result.get("filters", {}).get("avg_rating_gte") == 4.0
    assert "hotel_id_count" in result
    assert result["hotel_id_count"] > 0

    # Ground truth via direct psql — does NOT call _resolve_hotel_ids
    ground_truth = _ground_truth_hotel_ids("avg_rating >= 4.0")
    assert ground_truth, "psql returned no hotels — fixture data missing?"

    # Every review returned must be from a hotel in the ground-truth set
    returned = {r["hotel_id"] for r in result.get("reviews", [])}
    assert returned, "hybrid path should return some reviews"
    leaked = returned - ground_truth
    assert not leaked, (
        f"hybrid scoping LEAKED — these hotel_ids do NOT satisfy "
        f"avg_rating >= 4.0: {sorted(leaked)}"
    )


@pytest.mark.slow
def test_hybrid_top20_dedup_is_prefix_unique():
    """
    B-006 follow-up regression test.

    The Kaggle dataset contains clusters of review_text values that share a
    ~190-character opener and diverge only in a short closing sentence. The
    first version of B-006's dedup used DISTINCT ON (review_text), which sees
    those as distinct rows and lets them flood the top-K — manual testing
    found the same review opener appearing 10× in 20 hybrid results.

    The fix (in ai/semantic_search.py) changes the dedup key to
    LEFT(review_text, DEDUP_PREFIX_LEN). This test guards against silent
    regression by asserting prefix-uniqueness, not byte-uniqueness, on the
    same hybrid query that originally surfaced the bug.

    Importing the constant from the module rather than hard-coding 200 keeps
    this test in lock-step with any future re-tuning of DEDUP_PREFIX_LEN.
    """
    from ai.semantic_search import DEDUP_PREFIX_LEN

    result = answer("hotels with most negative reviews but rating above 4")
    reviews = result.get("reviews", [])
    assert reviews, "hybrid path should return some reviews"

    prefixes = [r["review_text"][:DEDUP_PREFIX_LEN] for r in reviews]
    duplicates = [p for p, c in Counter(prefixes).items() if c > 1]
    assert not duplicates, (
        f"top-{len(reviews)} contains review_text prefixes that repeat — "
        f"dedup regression. Repeated prefixes ({len(duplicates)}): "
        + ", ".join(repr(p[:60] + "...") for p in duplicates[:3])
    )


@pytest.mark.slow
def test_hybrid_star_scoping_is_real():
    """Same proof for the star_category filter shape."""
    result = answer("rude staff in 5-star hotels")

    assert result["path"] == "semantic"
    assert result.get("filters", {}).get("star_category") == 5

    ground_truth = _ground_truth_hotel_ids("star_category = 5")
    assert ground_truth

    returned = {r["hotel_id"] for r in result.get("reviews", [])}
    leaked = returned - ground_truth
    assert not leaked, (
        f"star scoping LEAKED — these hotel_ids are NOT 5-star: {sorted(leaked)}"
    )


@pytest.mark.slow
def test_hybrid_result_dict_records_filter():
    """Independently of scoping, the result must surface the detected filter
    so dashboard / debug tooling can see scoping happened."""
    result = answer("negative reviews but rating above 4")
    assert result.get("filters") is not None
    assert isinstance(result.get("hotel_id_count"), int)
    assert result["filters"].get("avg_rating_gte") == 4.0


# ══════════════════════════════════════════════════════════════════════════
# 4. REGRESSION — unchanged paths
# Pure-semantic, city-only-semantic, and SQL paths MUST NOT acquire hybrid
# keys. A regression here means we're polluting unrelated result dicts.
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.slow
def test_pure_semantic_no_hybrid_keys():
    """'rude staff' — no filter, no hybrid keys, existing path intact."""
    result = answer("rude staff")
    assert result["path"] == "semantic"
    assert "filters" not in result
    assert "hotel_id_count" not in result


@pytest.mark.slow
def test_city_only_semantic_no_hybrid_keys():
    """
    City-only routes through the EXISTING semantic path (city detection
    handled inside semantic_search). Going through hotel_ids for a pure
    city query would be a behaviour change — explicitly out of scope.
    """
    result = answer("complaints in Goa")
    assert result["path"] == "semantic"
    assert result.get("city") == "goa"
    assert "filters" not in result
    assert "hotel_id_count" not in result


@pytest.mark.slow
def test_sql_path_no_hybrid_keys():
    """SQL path must be byte-for-byte unaffected."""
    result = answer("top 5 cities by revenue")
    assert result["path"] == "sql"
    assert "filters" not in result
    assert "hotel_id_count" not in result
    assert len(result.get("rows", [])) > 0


# ══════════════════════════════════════════════════════════════════════════
# 5. EDGE CASES
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.slow
def test_zero_match_filter_returns_gracefully():
    """
    rating > 4.99 matches zero hotels (max avg_rating in the dataset is
    well below 4.99). The hybrid path must short-circuit BEFORE running
    semantic_search with an empty hotel_ids list — never producing a
    broken ANY(empty array) query that silently bypasses the filter.
    """
    result = answer("negative reviews but rating above 4.99")
    assert result["path"] == "semantic"
    assert result.get("hotel_id_count") == 0
    assert result.get("reviews") == []
    assert result.get("error") is None  # not an error condition — just no matches


def test_empty_query_clean_dict():
    """Empty string must not crash detect_filters — must return {}."""
    assert detect_filters("") == {}


def test_whitespace_only_query_clean_dict():
    """Whitespace-only string must also return {}."""
    assert detect_filters("   \n\t  ") == {}


def test_structured_query_routes_sql_not_hybrid():
    """
    '5 star hotels in Mumbai' — structured listing question, no review
    intent. The router has no semantic trigger to grab, so it routes SQL.
    Hybrid only fires when the router said semantic, so this query never
    enters the hybrid branch even though detect_filters DOES find
    structured conditions in it.
    """
    # Router decides this is SQL — confirms hybrid won't fire
    assert route("5 star hotels in Mumbai") == "sql"

    # detect_filters still extracts what's there (decoupled from routing)
    f = detect_filters("5 star hotels in Mumbai")
    assert f["star_category"] == 5
    assert f["city"] == "mumbai"
