"""
B-003 regression tests — _validate_columns in ai/text_to_sql.py
================================================================
These tests defend the two competing properties of the validator:

  1. It MUST catch hallucinated column references (false-negative = bug).
  2. It MUST NOT block valid SQL (false-positive = worse bug — silently
     breaks user queries that would otherwise have run).

The tests run directly against `_validate_columns(sql)` with fixed SQL
strings. We deliberately do NOT go through `ai.main.answer()` here —
that would couple the test to Ollama's non-determinism and slow the
suite from milliseconds to minutes. The adversarial sweep against the
live LLM was a one-time verification; this file is the ongoing guard.

Requirements:
  - travellens Postgres must be reachable when these tests import.
    `_validate_columns` is a no-op when `_KNOWN_COLUMNS` is empty, which
    would silently pass every test. We skip the whole module instead.

Run with:
    pytest tests/test_validate_columns.py -v
"""
import pytest

from ai.text_to_sql import _validate_columns, _KNOWN_COLUMNS


# Skip the entire module if the schema map wasn't populated at import.
# Without it the validator is a no-op and every assertion would pass
# vacuously — much worse than a clear "DB unreachable" skip.
if not _KNOWN_COLUMNS:
    pytest.skip(
        "information_schema map not loaded — is travellens-postgres up?",
        allow_module_level=True,
    )


# ── 1. Hallucinated columns: MUST be rejected ────────────────────────────────

def test_bare_column_rejected_on_single_table():
    """Direct hallucination — the exact case from the B-003 acceptance test."""
    with pytest.raises(ValueError, match="occupancy_rate"):
        _validate_columns("SELECT occupancy_rate FROM agg_daily_hotel_kpi")


def test_qualified_column_rejected():
    """Qualified hallucination — alias.column form."""
    with pytest.raises(ValueError, match="occupancy_rate"):
        _validate_columns("SELECT a.occupancy_rate FROM agg_daily_hotel_kpi a")


def test_avg_occupancy_on_hotel_master_rejected():
    """The literal SQL pattern from the B-003 hallucination control case."""
    with pytest.raises(ValueError, match="avg_occupancy"):
        _validate_columns("SELECT avg_occupancy FROM hotel_master")


def test_qualified_hallucination_in_select_rejected():
    """Hallucinated column attached to a known table via alias."""
    with pytest.raises(ValueError, match="avg_occupancy"):
        _validate_columns("SELECT h.avg_occupancy FROM hotel_master h")


# ── 2. Valid SQL: MUST NOT be rejected (the false-positive guard) ────────────
# Every shape below appeared in the adversarial sweep against the live LLM.
# If any of these start raising, the validator has regressed into blocking
# legitimate queries.

def test_single_table_valid_bare_columns():
    _validate_columns("SELECT city FROM dim_location LIMIT 10")


def test_bare_columns_on_known_aggregate_table():
    _validate_columns(
        "SELECT total_bookings, total_revenue_inr FROM agg_daily_hotel_kpi"
    )


def test_multi_table_join_with_aliases():
    """The canonical 'top 5 cities by revenue' shape."""
    _validate_columns("""
        SELECT l.city, ROUND(SUM(b.revenue_inr) / 1e7, 2) AS total_revenue_crore
        FROM fact_bookings b
        JOIN hotel_master h ON b.hotel_id = h.hotel_id
        JOIN dim_location l ON h.location_id = l.location_id
        WHERE NOT b.is_cancelled
        GROUP BY l.city
        ORDER BY total_revenue_crore DESC
        LIMIT 5
    """)


def test_case_when_expression():
    """CASE WHEN inside SUM must be skipped, not treated as columns."""
    _validate_columns("""
        SELECT c.customer_segment,
               ROUND(100.0 * SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END)
                     / COUNT(*), 2) AS cancellation_rate
        FROM fact_bookings b
        JOIN dim_customer c ON b.customer_id = c.customer_id
        GROUP BY c.customer_segment
    """)


def test_count_star_with_group_by():
    """COUNT(*) and qualified GROUP BY columns must pass."""
    _validate_columns("""
        SELECT h.hotel_name
        FROM fact_bookings b
        JOIN hotel_master h ON b.hotel_id = h.hotel_id
        WHERE NOT b.is_cancelled
        GROUP BY h.hotel_id, h.hotel_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """)


def test_nullif_and_round_expressions():
    """NULLIF, ROUND, AVG — the arithmetic-expression shape."""
    _validate_columns("""
        SELECT ROUND(AVG(b.revenue_inr / NULLIF(b.nights_stayed, 0)), 2) AS adr
        FROM fact_bookings b
        JOIN hotel_master h ON b.hotel_id = h.hotel_id
        JOIN dim_location l ON h.location_id = l.location_id
        WHERE h.star_category = 5 AND l.city = 'Goa' AND NOT b.is_cancelled
    """)


def test_select_distinct():
    """SELECT DISTINCT with qualified column list."""
    _validate_columns("""
        SELECT DISTINCT h.hotel_name
        FROM fact_bookings b
        JOIN hotel_master h ON b.hotel_id = h.hotel_id
        JOIN dim_location l ON h.location_id = l.location_id
        WHERE l.state = 'Rajasthan' AND h.avg_rating > 4
    """)


def test_date_trunc_and_year_filter():
    """Date functions and dim_date join — common analytical shape."""
    _validate_columns("""
        SELECT d.month,
               ROUND(SUM(b.revenue_inr) / 1e7, 2) AS revenue_in_crore
        FROM fact_bookings b
        JOIN dim_date d ON b.date_id = d.date_id
        WHERE d.year = 2025
        GROUP BY d.month
        ORDER BY d.month
    """)


# ── 3. Conservative-skip rule: MUST NOT false-reject ambiguous refs ──────────
# These cases exist explicitly to force the validator to skip rather than
# raise. A regression here means the validator has gotten too eager.

def test_select_star_skipped():
    """SELECT * — wildcard, nothing to validate."""
    _validate_columns("SELECT * FROM dim_location LIMIT 5")


def test_qualified_select_star_skipped():
    """t.* — qualified wildcard."""
    _validate_columns(
        "SELECT l.* FROM dim_location l LIMIT 5"
    )


def test_multi_table_bare_unknown_column_skipped():
    """
    A bare reference that doesn't exist on ANY table, in a multi-table
    query, must be SKIPPED — not raise. Postgres's own name resolution
    handles ambiguous bare refs; the validator can't reproduce that
    cheaply and must err on the side of letting it through.
    """
    _validate_columns("""
        SELECT some_unknown_col
        FROM fact_bookings b
        JOIN dim_location l ON b.location_id = l.location_id
    """)


def test_no_from_clause_skipped():
    """SELECT NOW() — no table to validate against."""
    _validate_columns("SELECT NOW()")


def test_constant_expression_skipped():
    """SELECT 1 + 1 — no identifiers that could be columns."""
    _validate_columns("SELECT 1 + 1 AS sanity")


# ── 4. Known limitation (B-003a): bare ref inside WHERE on single-table ──────
# Documents the false-NEGATIVE gap. `_walk_columns` doesn't recurse into
# `sqlparse.sql.Comparison`, so a bare column ref inside a single-table
# WHERE clause slips past the validator. Postgres still catches it at
# execution. Marked xfail until B-003a lands. If this test starts PASSING
# (i.e. the validator catches it), the limitation has been closed and
# this test should be promoted to a regular assertion.

@pytest.mark.xfail(
    reason="B-003a: _walk_columns does not recurse into Comparison nodes; "
           "bare WHERE refs on single-table queries slip past. "
           "Promote to a real assertion once B-003a ships.",
    strict=True,
)
def test_bare_where_ref_caught_on_single_table():
    with pytest.raises(ValueError, match="avg_occupancy"):
        _validate_columns(
            "SELECT 1 FROM hotel_master WHERE avg_occupancy > 0.5"
        )
