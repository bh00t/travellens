"""
eval_questions.py — Text-to-SQL accuracy eval fixture (B-058)
=============================================================
Part of: TravelLens — on-demand accuracy eval harness for the Text-to-SQL path.

WHAT THIS IS
    A fixed set of (question -> owner-verified reference_sql) pairs. The runner
    (ai/eval/run_eval.py) feeds each `question` through ai.main.answer() against
    the live DB + Ollama, runs the `reference_sql` against the same live DB, and
    grades by EXECUTION MATCH (normalized result sets), NOT by SQL text.

HARD BOUNDARY — this is TEST DATA, never prompt content.
    These pairs are used ONLY to grade output. This file is NEVER read by, written
    into, or referenced from ai/prompts/text_to_sql_system.txt. The harness does not
    feed examples back into the model. It is not a few-shot patch and must not
    become one. (CLAUDE.md hard rule: "Examples in a prompt are a last resort.")

WHY EXECUTION MATCH, NOT SQL-STRING MATCH
    Many correct SQLs exist per question and the data is regenerable, so comparing
    SQL text — or hardcoding expected row values — both break. We run BOTH queries
    live and compare normalized RESULT SETS, so the metric survives a dataset
    regenerate. The reference_sql is the grading key, not its literal text.

COMPARATOR ASSUMPTIONS YOU MUST KNOW WHEN AUTHORING A REFERENCE
    1. POSITIONAL columns. The comparator ignores column NAMES (alias tolerance)
       but compares value-tuples in SELECT order, and requires the same number of
       value columns. → Write every reference_sql with columns in the order the
       model naturally emits: DIMENSION first, then METRIC (i.e. question order).
    2. Numbers are rounded to 2 dp HALF-UP (matching Postgres ROUND), then compared
       as strings. So a reference's rounding does not have to match the model's
       exactly to 5 dp — but a unit mismatch (raw INR vs crore) WILL fail. That is
       a real signal, not a comparator bug; the FLAGS + printed SQL disambiguate.
    3. Reference SQLs MIRROR the prompt's OUTPUT RULES (ai/prompts/text_to_sql_system.txt)
       so the reference reflects the model's INTENDED canonical output:
         - revenue in crore : ROUND(SUM(b.revenue_inr) / 1e7, 2)
         - ADR              : ROUND(AVG(b.revenue_inr / NULLIF(b.nights_stayed, 0)), 0)
         - cancellation rate: ROUND(100.0 * COUNT(*) FILTER (WHERE b.is_cancelled)
                              / NULLIF(COUNT(*), 0), 2)   ← NO is_cancelled WHERE filter
         - entity counts    : COUNT(*) FROM the dimension, no is_cancelled, no LIMIT
       A fail on a revenue question may therefore be a FORMAT miss (raw INR vs crore),
       not a ranking miss — read the flags + SQL to tell which.
    4. ordered=True preserves row order (ranking / top-N). Default (False) compares
       as an order-insensitive multiset.

ENTRY SCHEMA
    id                          : str  — stable slug; used by --id and the results table.
    question                    : str  — natural-language prompt fed to answer().
    path                        : "sql" — every fixture row is a SQL-path question (this cut).
    reference_sql               : str  — owner-verified canonical-correct query. Run live;
                                         its normalized RESULT SET is the grading key.
    ordered                     : bool — (opt, default False) ranking → preserve row order.
    expects_distinct            : bool — (opt) raise distinct_missing flag if model SQL has
                                         no DISTINCT (L-011 probe).
    expects_cancellation_filter : bool — (opt) raise cancellation_filter_missing flag if model
                                         SQL touches fact_bookings, is NOT a rate query, and
                                         has no is_cancelled exclusion (L-013 probe).
    scored                      : bool — (opt, default True) include this entry's exec-match in
                                         the accuracy aggregate. Set False for a FLAG-ONLY probe
                                         whose correct answer is inherently under-determined
                                         (e.g. "5 of 1,418 valid rows") — exec-match is still
                                         run and shown, but excluded from the accuracy %.
    notes                       : str  — (opt) why this case exists / known caveat.

PROVENANCE
    Reference shapes are lifted from owner-verified origins — they are not invented here:
      - tests/test_validate_columns.py (the canonical valid-SQL shapes)
      - docs/phase-4-ai-layer.md (acceptance + E1)
      - ai/prompts/text_to_sql_system.txt OUTPUT RULES (formatting conventions)
"""

QUESTIONS = [
    # ──────────────────────────────────────────────────────────────────────────
    # Ranking / top-N — ordered comparison. Also a bare revenue aggregate over
    # fact_bookings, so it legitimately requires the cancellation filter (L-013).
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "top5_cities_by_revenue",
        "question":      "top 5 cities by revenue",
        "path":          "sql",
        "ordered":       True,
        "expects_cancellation_filter": True,
        "reference_sql": """
            SELECT l.city, ROUND(SUM(b.revenue_inr) / 1e7, 2) AS total_revenue_crore
            FROM fact_bookings b
            JOIN hotel_master h ON b.hotel_id = h.hotel_id
            JOIN dim_location l ON h.location_id = l.location_id
            WHERE NOT b.is_cancelled
            GROUP BY l.city
            ORDER BY total_revenue_crore DESC
            LIMIT 5
        """,
        "notes": "Canonical shape from test_validate_columns.test_multi_table_join_with_aliases.",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # RATE query — the is_cancelled exclusion must NOT apply (cancelled rows are
    # the numerator and must stay in the denominator). expects_cancellation_filter
    # is deliberately ABSENT so the flag does not fire on a correct rate query.
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "cancellation_rate_by_segment",
        "question":      "cancellation rate by customer segment",
        "path":          "sql",
        "reference_sql": """
            SELECT c.customer_segment,
                   ROUND(100.0 * COUNT(*) FILTER (WHERE b.is_cancelled)
                         / NULLIF(COUNT(*), 0), 2) AS cancellation_rate
            FROM fact_bookings b
            JOIN dim_customer c ON b.customer_id = c.customer_id
            GROUP BY c.customer_segment
        """,
        "notes": "Rate query — multiset; both cancelled & non-cancelled rows needed in the denom.",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # ADR — single-row aggregate. ADR excludes cancelled bookings per the prompt,
    # so the cancellation filter is required. ADR rounds to 0 dp per OUTPUT RULES.
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "adr_5star_goa",
        "question":      "average daily rate for 5-star hotels in Goa",
        "path":          "sql",
        "expects_cancellation_filter": True,
        "reference_sql": """
            SELECT ROUND(AVG(b.revenue_inr / NULLIF(b.nights_stayed, 0)), 0) AS adr
            FROM fact_bookings b
            JOIN hotel_master h ON b.hotel_id = h.hotel_id
            JOIN dim_location l ON h.location_id = l.location_id
            WHERE h.star_category = 5 AND l.city = 'Goa' AND NOT b.is_cancelled
        """,
        "notes": "ADR rounds to 0 dp per OUTPUT RULES (test file used 2 dp — prompt wins per B-058 SHOULD-FIX #4).",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # The L-013 / B-053 HARD CASE. Bare grouped revenue aggregate over fact_bookings
    # behind a dim_date year filter. Expect a LOW score: the model bleeds the two
    # date paradigms (invents b.booking_date / d.date_key / d.month_number) AND
    # intermittently drops the cancellation filter. The low score IS the signal.
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "revenue_by_month_2025",
        "question":      "revenue by month in 2025",
        "path":          "sql",
        "ordered":       True,
        "expects_cancellation_filter": True,
        "reference_sql": """
            SELECT d.month, ROUND(SUM(b.revenue_inr) / 1e7, 2) AS revenue_crore
            FROM fact_bookings b
            JOIN dim_date d ON b.date_id = d.date_id
            WHERE d.year = 2025 AND NOT b.is_cancelled
            GROUP BY d.month
            ORDER BY d.month
        """,
        "notes": "Known date-paradigm-bleed ceiling (B-053 widget 7 skipped). Expect LOW exec-match.",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # ENTITY COUNTS — query the dimension, no is_cancelled, no LIMIT. The
    # cancellation_filter flag must NOT fire (expects_cancellation_filter absent).
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "top10_cities_by_hotel_count",
        "question":      "top 10 cities by hotel count",
        "path":          "sql",
        # MULTISET (ordered: False) on purpose. The rank-10/11 boundary is clean
        # (105 vs 100 hotels) so the SET of 10 cities + their counts is fully
        # deterministic — but there are INTERNAL ties (two cities at 134, two at
        # 124) whose relative order is arbitrary, which an ordered compare would
        # fail on. The set + counts are what's gradable here; not a comparator
        # loosening (the answer set does not vary, only tie-order does).
        "reference_sql": """
            SELECT l.city, COUNT(*) AS hotel_count
            FROM hotel_master h
            JOIN dim_location l ON h.location_id = l.location_id
            GROUP BY l.city
            ORDER BY hotel_count DESC
            LIMIT 10
        """,
        "notes": "Bounded entity count (10 rows). Reworded from 'how many hotels per city' (991 rows "
                 "> MAX_ROWS, permanently cap-excluded). No cancellation filter — entity count.",
    },
    {
        "id":            "customers_per_state",
        "question":      "how many customers per state",
        "path":          "sql",
        "reference_sql": """
            SELECT home_state, COUNT(*) AS customer_count
            FROM dim_customer
            GROUP BY home_state
        """,
        "notes": "16 home_state values — bounded, multiset-stable. Entity count, no cancel filter.",
    },
    {
        "id":            "hotels_opened_per_year",
        "question":      "hotels opened per year",
        "path":          "sql",
        "reference_sql": """
            SELECT opened_year, COUNT(*) AS hotel_count
            FROM hotel_master
            GROUP BY opened_year
        """,
        "notes": "52 opened_year values — bounded. opened_year is a column, not derived from dates.",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # L-011 DISTINCT probe — FLAG-ONLY (scored: False). The question deliberately
    # does NOT say "unique" — the word "unique" would force the model to add
    # DISTINCT, making distinct_missing impossible to ever fire (vacuous). Without
    # it, the L-011 failure mode (model omits DISTINCT and returns one row per
    # booking, the same person repeated) can actually surface and be measured.
    # 1,418 customers match 'R%', so any 5 names is a correct answer → exec-match
    # is inherently under-determined and excluded from the accuracy % (probe).
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "list_5_customers_named_r",
        "question":      "list 5 customers named R",
        "path":          "sql",
        "scored":        False,
        "expects_distinct": True,
        "reference_sql": """
            SELECT DISTINCT first_name, last_name
            FROM dim_customer
            WHERE first_name LIKE 'R%'
            LIMIT 5
        """,
        "notes": "FLAG-ONLY (L-011). 'unique' removed so the model is not forced into DISTINCT — that "
                 "lets distinct_missing actually fire. 1,418 candidates → exec-match under-determined, "
                 "not scored. Canonical DISTINCT shape from the prompt's COLUMN LOCATION rule example.",
    },

    # ──────────────────────────────────────────────────────────────────────────
    # Bare grouped aggregations over fact_bookings — the L-013 surface. Each is a
    # plain GROUP BY aggregate (not a rate), so the cancellation filter IS required;
    # expects_cancellation_filter: True on all three.
    # ──────────────────────────────────────────────────────────────────────────
    {
        "id":            "bookings_by_segment",
        "question":      "total bookings by customer segment",
        "path":          "sql",
        "expects_cancellation_filter": True,
        "reference_sql": """
            SELECT c.customer_segment, COUNT(*) AS total_bookings
            FROM fact_bookings b
            JOIN dim_customer c ON b.customer_id = c.customer_id
            WHERE NOT b.is_cancelled
            GROUP BY c.customer_segment
        """,
        "notes": "Bare grouped COUNT — must exclude cancelled (6 segments, bounded multiset).",
    },
    {
        "id":            "revenue_by_star_category",
        "question":      "total revenue by hotel star category",
        "path":          "sql",
        "expects_cancellation_filter": True,
        "reference_sql": """
            SELECT h.star_category, ROUND(SUM(b.revenue_inr) / 1e7, 2) AS total_revenue_crore
            FROM fact_bookings b
            JOIN hotel_master h ON b.hotel_id = h.hotel_id
            WHERE NOT b.is_cancelled
            GROUP BY h.star_category
        """,
        "notes": "Bare grouped revenue SUM in crore — must exclude cancelled (6 star values).",
    },
    {
        "id":            "avg_nights_by_segment",
        "question":      "average nights stayed by customer segment",
        "path":          "sql",
        "expects_cancellation_filter": True,
        "reference_sql": """
            SELECT c.customer_segment, ROUND(AVG(b.nights_stayed), 2) AS avg_nights
            FROM fact_bookings b
            JOIN dim_customer c ON b.customer_id = c.customer_id
            WHERE NOT b.is_cancelled
            GROUP BY c.customer_segment
        """,
        "notes": "Bare grouped AVG — must exclude cancelled (6 segments, bounded multiset).",
    },
]
