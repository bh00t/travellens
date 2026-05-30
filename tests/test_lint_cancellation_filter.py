"""
B-060 unit tests — lint_cancellation_filter_missing in ai/text_to_sql.py
========================================================================
Deterministic, direct-call tests for the post-execution cancellation-filter
lint. Same posture as tests/test_validate_columns.py: we call the function
directly with fixed (sql, user_query) strings — NO Ollama, NO database. The
lint is a pure regex predicate, so these run in milliseconds and never depend
on model non-determinism.

What the lint defends (two competing properties):
  1. It MUST fire on a fact_bookings aggregate that silently dropped the
     cancellation exclusion (the L-013 surface) — a false-NEGATIVE means the
     corrective retry never runs and the wrong numbers ship.
  2. It MUST NOT fire when excluding cancelled rows would be wrong or redundant
     (rate queries, cancellation-intent questions, SQL already referencing
     is_cancelled, non-fact_bookings queries) — a false-POSITIVE burns an Ollama
     round-trip and, worse, could STACK a second predicate (the B-048 bug).

Run with:
    pytest tests/test_lint_cancellation_filter.py -v
"""
from ai.text_to_sql import lint_cancellation_filter_missing


# ── 1. MUST fire (True) — the L-013 surface ──────────────────────────────────

def test_fire_bare_grouped_count():
    """Bare grouped COUNT over fact_bookings, no filter, not a rate, no cancel
    intent — the canonical dropped-filter case."""
    sql = (
        "SELECT c.customer_segment, COUNT(*) AS total_bookings "
        "FROM fact_bookings b "
        "JOIN dim_customer c ON b.customer_id = c.customer_id "
        "GROUP BY c.customer_segment"
    )
    assert lint_cancellation_filter_missing(sql, "total bookings by customer segment") is True


def test_fire_revenue_crore_conversion():
    """The /1e7 crore trap: a unit scale is NOT a rate, so a revenue aggregate
    with no filter MUST still fire."""
    sql = (
        "SELECT l.city, ROUND(SUM(b.revenue_inr) / 1e7, 2) AS total_revenue_crore "
        "FROM fact_bookings b "
        "JOIN hotel_master h ON b.hotel_id = h.hotel_id "
        "JOIN dim_location l ON h.location_id = l.location_id "
        "GROUP BY l.city ORDER BY total_revenue_crore DESC LIMIT 5"
    )
    assert lint_cancellation_filter_missing(sql, "top 5 cities by revenue") is True


def test_fire_single_join_avg():
    """AVG aggregate over fact_bookings + one dim, no filter."""
    sql = (
        "SELECT d.month, ROUND(AVG(b.nights_stayed), 2) AS avg_nights "
        "FROM fact_bookings b "
        "JOIN dim_date d ON b.date_id = d.date_id "
        "GROUP BY d.month"
    )
    assert lint_cancellation_filter_missing(sql, "average nights stayed by month") is True


# ── 2. MUST NOT fire (False) — each suppression reason ───────────────────────

def test_no_fire_rate_query():
    """A cancellation-rate query keeps cancelled rows in the denominator; the
    filter must NOT apply. _lint_is_rate_query suppresses it."""
    sql = (
        "SELECT c.customer_segment, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE b.is_cancelled) "
        "/ NULLIF(COUNT(*), 0), 2) AS cancellation_rate "
        "FROM fact_bookings b "
        "JOIN dim_customer c ON b.customer_id = c.customer_id "
        "GROUP BY c.customer_segment"
    )
    assert lint_cancellation_filter_missing(sql, "cancellation rate by customer segment") is False


def test_no_fire_already_has_filter():
    """SQL that already excludes cancelled rows must be left alone."""
    sql = (
        "SELECT h.star_category, ROUND(SUM(b.revenue_inr) / 1e7, 2) AS total_revenue_crore "
        "FROM fact_bookings b "
        "JOIN hotel_master h ON b.hotel_id = h.hotel_id "
        "WHERE NOT b.is_cancelled "
        "GROUP BY h.star_category"
    )
    assert lint_cancellation_filter_missing(sql, "total revenue by hotel star category") is False


def test_no_fire_cancellation_intent_question():
    """When the user asks ABOUT cancellations, excluding cancelled rows would be
    wrong — the lint must not fire (condition 4)."""
    sql = (
        "SELECT l.city, COUNT(*) AS cancelled_bookings "
        "FROM fact_bookings b "
        "JOIN hotel_master h ON b.hotel_id = h.hotel_id "
        "JOIN dim_location l ON h.location_id = l.location_id "
        "WHERE b.is_cancelled "
        "GROUP BY l.city"
    )
    assert lint_cancellation_filter_missing(sql, "how many cancelled bookings per city") is False


def test_no_fire_non_fact_bookings():
    """Entity count off a dimension — fact_bookings not in scope, nothing to
    filter (condition 1)."""
    sql = (
        "SELECT l.city, COUNT(*) AS hotel_count "
        "FROM hotel_master h "
        "JOIN dim_location l ON h.location_id = l.location_id "
        "GROUP BY l.city"
    )
    assert lint_cancellation_filter_missing(sql, "how many hotels per city") is False


# ── 3. B-048-guard — is_cancelled present in a non-rate form MUST NOT fire ────
# This is the must-fix that keeps the corrective retry from STACKING a second
# `WHERE NOT is_cancelled` on top of an existing cancellation measure and zeroing
# the count out. _lint_is_rate_query does NOT recognize a CASE-WHEN count with no
# rate alias, so condition 3 (bare is_cancelled presence) is what suppresses it.

def test_no_fire_case_when_is_cancelled_no_rate_alias():
    """SUM(CASE WHEN b.is_cancelled ...) with no rate-style alias and no 'cancel'
    in the question — must NOT fire (B-048 guard). If it fired, the retry would
    stack a contradictory exclusion predicate."""
    sql = (
        "SELECT c.customer_segment, "
        "SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END) AS lost "
        "FROM fact_bookings b "
        "JOIN dim_customer c ON b.customer_id = c.customer_id "
        "GROUP BY c.customer_segment"
    )
    assert lint_cancellation_filter_missing(sql, "lost bookings by customer segment") is False
