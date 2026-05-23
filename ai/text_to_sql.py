"""
text_to_sql.py — Natural Language to SQL via Ollama
====================================================
Part of: TravelLens Phase 4 — AI Layer
File:    ai/text_to_sql.py

What this file does:
    Takes a plain English question and returns structured data from Postgres.

    Flow:
      1. Load the system prompt from ai/prompts/text_to_sql_system.txt
         (contains schema DDL + India context + few-shot examples)
      2. Send user query + system prompt to Ollama (Qwen2.5-Coder-7B)
      3. Ollama returns a SQL string
      4. Validate the SQL — must be SELECT only, must be parseable
      5. Execute against Postgres
      6. If execution fails → send the error back to Ollama for one self-correction
         attempt (B-001 retry loop)
      7. Return rows + column names as a result dict

Why the retry loop (B-001):
    Ollama occasionally generates SQL with hallucinated column names that look
    valid but don't exist in the schema (e.g. occupancy_rate on agg_daily_hotel_kpi).
    These pass validation but fail at execution time.

    Instead of just returning an error, we send the failure back to Ollama:
      "The SQL you generated failed with this error: <error>. Fix it."
    Ollama usually self-corrects on the first retry because the error message
    tells it exactly which column or table was wrong.

    One retry only. If the retry also fails, return the error — don't loop forever.

Why validate before executing:
    Ollama occasionally adds markdown fences despite instructions.
    More importantly, it enforces SELECT-only — even if someone crafts a prompt
    that tricks Ollama into generating DROP TABLE, the guard stops it.

Why the system prompt lives in a .txt file:
    Keeping it separate from Python code means you can tune examples,
    add India-specific context, or update the schema DDL without touching
    any Python logic. The file is loaded once at module import.

Latency expectation:
    Ollama on RTX 3070 (CUDA): ~1–3 seconds per query
    Ollama on CPU: ~8–15 seconds per query
    With retry: add another ~1–3 seconds on first failure
"""

import os
import logging
import requests
import sqlparse
import psycopg2
from pathlib import Path
from sqlparse.sql import Identifier, IdentifierList, Function, Parenthesis, Where
from sqlparse.tokens import Keyword, DML, Wildcard
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")

# Hard cap on rows returned — prevents accidental full-table result sets.
# If Ollama generates SELECT * FROM fact_bookings with no LIMIT, we still
# only return 100 rows to Phase 5. The renderer cannot handle 1M rows.
MAX_ROWS = 100

# How many times to retry after a SQL execution failure.
# 1 retry is the right number — catches most column hallucinations.
# More than 1 adds latency without meaningful improvement.
MAX_RETRIES = 1

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

log = logging.getLogger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────
# Loaded once at module import — not on every call.
# Contains: SELECT-only constraint, full schema with exact column names,
# India context, and few-shot examples covering common TravelLens patterns.
#
# To fix a bad query: add a corrected few-shot example to this file.
# That's the primary tuning lever — no Python changes needed.
_PROMPT_PATH   = Path(__file__).parent / "prompts" / "text_to_sql_system.txt"
_SYSTEM_PROMPT = _PROMPT_PATH.read_text(encoding="utf-8")


# ── Known-columns map for pre-execution validation (B-003) ────────────────────
# Built once at module import from information_schema. The CLAUDE.md hard rule
# says LLM-facing content must come from the live schema, never from memory —
# the same rule applies here: a static dict of table→columns drifts the moment
# anyone runs a migration. Querying information_schema means the validator is
# always in lockstep with whatever's actually in the database.
#
# Shape: {table_name_lower: {column_name_lower, ...}}
# Empty when DB is unreachable at import time. In that case _validate_columns()
# returns silently (no false rejections) and the existing Postgres-error path
# still catches hallucinations at execution time — slower, but never wrong.
_KNOWN_COLUMNS: dict[str, set[str]] = {}


def _load_schema() -> None:
    """
    Populate _KNOWN_COLUMNS by querying information_schema. Called once at
    module import. Best-effort — if Postgres is down, log and skip rather
    than fail the import (the module is imported by ai.main and render.server,
    both of which we want to keep importable for tooling that doesn't need
    the warehouse to be up).
    """
    global _KNOWN_COLUMNS
    try:
        # connect_timeout protects against hanging the import on a dead DB
        conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                """)
                for table, column in cur.fetchall():
                    _KNOWN_COLUMNS.setdefault(table.lower(), set()).add(column.lower())
        finally:
            conn.close()
        log.info(
            "B-003: loaded schema for %d tables (column validation enabled)",
            len(_KNOWN_COLUMNS)
        )
    except Exception as e:
        # Never let a schema-load failure prevent module import. Logging here
        # is the only signal — _validate_columns() will skip gracefully.
        logging.getLogger(__name__).warning(
            "B-003: could not load schema for column validation (%s). "
            "Validation will be skipped; Postgres execution-time errors still "
            "catch hallucinations on the slower path.",
            e
        )


# Keywords that look like identifiers but are never column names. sqlparse
# usually marks these as Keyword/DML tokens, but a few (DATE, TIMESTAMP, etc.)
# slip through as Identifiers depending on context. Lowercased.
_SQL_NON_COLUMN_KEYWORDS: set[str] = {
    # Clause markers — should never reach _process_identifier but defensive
    "select", "from", "where", "group", "by", "order", "having", "limit", "offset",
    "join", "inner", "outer", "left", "right", "full", "cross", "natural", "on",
    "using", "as", "and", "or", "not", "in", "is", "null", "true", "false",
    "distinct", "all", "asc", "desc", "union", "intersect", "except",
    "case", "when", "then", "else", "end", "between", "like", "ilike",
    "with", "recursive",
    # Common SQL functions / pseudo-columns that look like bare identifiers
    "current_date", "current_time", "current_timestamp", "now",
    "count", "sum", "avg", "min", "max", "round", "cast", "coalesce",
    "date_trunc", "extract", "over", "partition", "rows", "range",
}


def _extract_table_refs(stmt) -> dict[str, str]:
    """
    Walk a parsed statement and return {alias_or_name_lower: table_name_lower}.

    Both an explicit alias and the bare table name map to the same target so
    callers can resolve either form. Recurses into Where, Parenthesis, and
    Identifier groups to handle subqueries — a column reference inside a
    WHERE EXISTS (SELECT … FROM x) needs x in scope.
    """
    tables: dict[str, str] = {}

    def _record(ident) -> None:
        if not isinstance(ident, Identifier):
            return
        name = ident.get_real_name()
        if not name:
            return
        n = name.lower()
        tables[n] = n
        alias = ident.get_alias()
        if alias:
            tables[alias.lower()] = n

    def _is_from_or_join(tok) -> bool:
        if tok.ttype is not Keyword:
            return False
        kw = (tok.normalized or "").upper()
        # Matches FROM and every JOIN flavour (INNER JOIN, LEFT JOIN, ...)
        return kw == "FROM" or "JOIN" in kw

    def _walk(node) -> None:
        toks = list(getattr(node, "tokens", []))
        for i, tok in enumerate(toks):
            if _is_from_or_join(tok):
                # The next non-whitespace token holds the table identifier(s)
                j = i + 1
                while j < len(toks) and toks[j].is_whitespace:
                    j += 1
                if j < len(toks):
                    nxt = toks[j]
                    if isinstance(nxt, Identifier):
                        _record(nxt)
                    elif isinstance(nxt, IdentifierList):
                        for sub in nxt.get_identifiers():
                            _record(sub)
            # Recurse into nested groups (WHERE clauses, subqueries, function args)
            if hasattr(tok, "tokens") and not isinstance(tok, (Identifier, IdentifierList)):
                _walk(tok)

    _walk(stmt)
    return tables


def _validate_columns(sql: str) -> None:
    """
    B-003: pre-execution column validation.

    Catches the most common hallucination pattern — Ollama emits SQL with
    a column name that doesn't exist on the referenced table (e.g.
    `occupancy_rate` on `agg_daily_hotel_kpi`). Without this check the
    query goes all the way to Postgres before failing, costing a round
    trip and producing an error message keyed on offset rather than on
    the bad column.

    Strategy (conservative — false-rejects are worse than false-passes):
        1. Build alias→table map from FROM/JOIN clauses.
        2. Validate every QUALIFIED reference (alias.col / table.col) —
           we always know which table it belongs to, so the check is safe.
        3. Validate BARE references (no prefix) ONLY when exactly one
           table is in scope. Multi-table queries may have bare refs that
           Postgres resolves via its own name resolution; trying to guess
           the resolution here risks rejecting valid SQL.
        4. Skip wildcards, function expressions, computed aliases, and
           anything we can't confidently attribute to a table.

    On failure: raises ValueError. The caller (run) catches it and feeds
    the message into the B-001 retry loop — Ollama gets one self-correct
    attempt with the bad-column message attached.

    On a no-table SQL (e.g. SELECT NOW(), SELECT 1+1) or when the schema
    couldn't be loaded at import, this is a no-op.
    """
    if not _KNOWN_COLUMNS:
        return  # Schema not loaded — fall through to the Postgres-error path

    parsed = sqlparse.parse(sql)
    if not parsed:
        return
    stmt = parsed[0]

    tables = _extract_table_refs(stmt)
    if not tables:
        return  # No FROM clause — nothing to validate against

    bare_refs, qualified_refs = _collect_column_refs(stmt)

    # Qualified refs are always safe — we know which table to check
    for prefix, col in qualified_refs:
        table = tables.get(prefix.lower())
        if table is None:
            # Alias we didn't see in FROM/JOIN — could be a CTE or subquery
            # we didn't track. Skip rather than false-reject.
            continue
        if col.lower() not in _KNOWN_COLUMNS.get(table, set()):
            raise ValueError(
                f"Column '{col}' does not exist on table '{table}' "
                f"(referenced as {prefix}.{col}). This was hallucinated — "
                f"check the actual schema with `\\d {table}` in psql."
            )

    # Bare refs are safe only when the query references exactly one table.
    # Multi-table FROM/JOIN means a bare column could belong to any of them
    # and Postgres resolves it via name lookup; we can't replicate that
    # cheaply, so we skip rather than risk a false reject.
    unique_tables = set(tables.values())
    if len(unique_tables) == 1:
        table = next(iter(unique_tables))
        known = _KNOWN_COLUMNS.get(table, set())
        for col in bare_refs:
            if col.lower() not in known:
                raise ValueError(
                    f"Column '{col}' does not exist on table '{table}'. "
                    f"This was hallucinated — check the actual schema with "
                    f"`\\d {table}` in psql."
                )


# Populate the known-columns map at module import. Best-effort: if Postgres
# is down, this logs a warning and leaves _KNOWN_COLUMNS empty, in which
# case _validate_columns() is a no-op and the Postgres-error path still
# catches the hallucination — slower but never wrong.
_load_schema()


def _collect_column_refs(stmt) -> tuple[list[str], list[tuple[str, str]]]:
    """
    Walk a parsed statement and return (bare_refs, qualified_refs).

    bare_refs:      list of column names referenced without a table prefix.
                    Only safe to validate when exactly one table is in scope —
                    multi-table queries may have shadowed names that look bare
                    but resolve via Postgres's name resolution.
    qualified_refs: list of (prefix, column) pairs. The prefix is whatever
                    the SQL used (alias or table name); the caller maps it
                    back to the real table via _extract_table_refs's dict.

    Section tracking: we walk the top-level token stream, flipping a state
    flag as we hit clause keywords (SELECT, FROM, JOIN, WHERE, GROUP BY, …).
    Identifiers inside FROM/JOIN are tables (skipped here); identifiers
    inside SELECT, WHERE, ON, GROUP BY, ORDER BY, and HAVING are columns.

    Functions, wildcards, and computed expressions are conservatively skipped.
    The goal is catching hallucinated columns, not rejecting valid SQL.
    """
    bare: list[str] = []
    qualified: list[tuple[str, str]] = []

    def _process_identifier(ident: Identifier) -> None:
        # If the identifier wraps a function or wildcard, recurse to find
        # column refs inside the args but don't treat the wrapper as a column.
        for sub_tok in ident.tokens:
            if isinstance(sub_tok, Function):
                _walk_columns(sub_tok)
                return
            if sub_tok.ttype is Wildcard:
                return  # SELECT * or t.* — nothing to validate

        parent = ident.get_parent_name()
        real = ident.get_real_name()
        if not real:
            return
        if real.lower() in _SQL_NON_COLUMN_KEYWORDS:
            return

        if parent:
            qualified.append((parent, real))
        else:
            bare.append(real)

    def _walk_columns(node) -> None:
        """Generic walker — recurses through any group looking for column refs."""
        for tok in getattr(node, "tokens", []):
            if isinstance(tok, Identifier):
                _process_identifier(tok)
            elif isinstance(tok, IdentifierList):
                for sub in tok.get_identifiers():
                    if isinstance(sub, Identifier):
                        _process_identifier(sub)
            elif isinstance(tok, (Function, Parenthesis, Where)):
                _walk_columns(tok)

    # Top-level walk with section tracking. We only harvest identifiers from
    # clauses that can contain column references; we skip the FROM/JOIN
    # sections so table names don't get mistaken for columns.
    section = "unknown"
    for tok in getattr(stmt, "tokens", []):
        if tok.ttype is DML and (tok.normalized or "").upper() == "SELECT":
            section = "select"
            continue
        if tok.ttype is Keyword:
            kw = (tok.normalized or "").upper()
            if kw == "FROM" or "JOIN" in kw:
                section = "from"
                continue
            if kw in ("GROUP BY", "ORDER BY", "HAVING"):
                section = "select"   # same harvesting rules apply
                continue
            if kw == "ON":
                section = "select"   # JOIN ON conditions have column refs
                continue
            if kw in ("LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT"):
                section = "limit"
                continue

        # Where is its own grouped token; its body always has column refs.
        if isinstance(tok, Where):
            _walk_columns(tok)
            continue

        if section in ("select",):
            if isinstance(tok, Identifier):
                _process_identifier(tok)
            elif isinstance(tok, IdentifierList):
                for sub in tok.get_identifiers():
                    if isinstance(sub, Identifier):
                        _process_identifier(sub)
            elif isinstance(tok, (Function, Parenthesis)):
                _walk_columns(tok)

    return bare, qualified


# ── Helper functions ──────────────────────────────────────────────────────────

def _call_ollama(user_query: str, error_context: str = None) -> str:
    """
    Send the user query to Ollama and return the raw text response.

    Args:
        user_query:     The original natural language question.
        error_context:  If provided, this is a retry call. The error from the
                        previous execution attempt is included in the prompt
                        so Ollama knows exactly what went wrong and can fix it.

    Why error_context changes the prompt on retry:
        On first call: Ollama sees just the question and generates SQL.
        On retry: Ollama sees the question + the SQL it generated + the exact
        Postgres error. This is much more effective than re-sending the same
        question — Ollama can see "column occupancy_rate does not exist" and
        knows to use a different column name.

    Raises RuntimeError if Ollama is unreachable.
    """
    if error_context:
        # Retry prompt — include the failed SQL and the error so Ollama
        # can self-correct. Be explicit: tell it what failed and what to do.
        prompt = (
            f"The following SQL failed when executed against PostgreSQL:\n\n"
            f"ERROR: {error_context}\n\n"
            f"Original question: {user_query}\n\n"
            f"Generate a corrected SQL query. "
            f"Return only the SQL. No explanation. No markdown. No backticks."
        )
    else:
        # Normal first-attempt prompt — just the user's question.
        prompt = user_query

    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "system": _SYSTEM_PROMPT,
        "stream": False,
    }

    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json=payload,
            timeout=60,  # Qwen 7B on RTX 3070 takes <3s; 60s is generous
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()

    except requests.exceptions.ConnectionError:
        raise RuntimeError(
            "Ollama not reachable. Is it running?\n"
            "Check: curl http://localhost:11434\n"
            "Start: ollama serve"
        )


def _validate_sql(sql: str) -> str:
    """
    Safety and correctness checks before execution.

    Three checks in order:
      1. Strip markdown fences — Ollama sometimes adds ```sql ... ``` despite
         being told not to. We strip them rather than failing, because the
         SQL itself is usually correct.
      2. sqlparse must be able to parse it — catches completely malformed output
         like truncated responses or garbled text.
      3. Statement type must be SELECT — hard security block on any
         data-modifying SQL. This check must never be removed or relaxed.

    Returns the cleaned SQL string on success.
    Raises ValueError with a descriptive message on failure.
    """
    # Strip markdown fences if present — common Ollama failure mode
    sql = sql.strip().strip("`")
    if sql.lower().startswith("sql"):
        # Sometimes Ollama returns "sql\nSELECT ..." — strip the language tag
        sql = sql[3:].strip()

    # sqlparse sanity check — catches completely broken output
    parsed = sqlparse.parse(sql)
    if not parsed:
        raise ValueError(
            f"sqlparse could not parse the generated SQL.\n"
            f"Raw output from Ollama: {sql!r}"
        )

    # SELECT-only enforcement — the security boundary
    # This catches INSERT, UPDATE, DELETE, DROP, CREATE, TRUNCATE etc.
    # If Ollama generates non-SELECT SQL, the system prompt has a problem.
    stmt_type = parsed[0].get_type()
    if stmt_type != "SELECT":
        raise ValueError(
            f"Generated statement type is {stmt_type!r} — not SELECT.\n"
            f"Refusing to execute: {sql!r}\n"
            f"This is a safety block. Check the system prompt SELECT-only constraint."
        )

    return sql


def _execute(sql: str) -> tuple[list[str], list[tuple]]:
    """
    Execute a validated SELECT query against Postgres.

    Opens a new connection per call — acceptable for this query pattern
    (one query per user prompt, not a high-frequency loop).

    Returns:
        columns: list of column name strings from cursor.description
        rows:    list of result tuples, capped at MAX_ROWS

    Raises psycopg2.Error on any Postgres execution failure.
    The caller (run()) catches this and decides whether to retry.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows    = cur.fetchmany(MAX_ROWS)
        return columns, rows
    finally:
        # Always close — even if execution raised an exception
        conn.close()


# ── Main entry point ──────────────────────────────────────────────────────────

def run(user_query: str) -> dict:
    """
    Full text-to-SQL pipeline with retry loop. Called by ai/main.py.

    The retry loop (B-001):
        On first execution failure, the exact Postgres error is sent back to
        Ollama as context. Ollama re-generates the SQL knowing what went wrong.
        If the retry also fails, we return the error — we don't loop again.

        This handles the most common failure mode: Ollama hallucinating a column
        name that doesn't exist in the actual schema. The Postgres error message
        tells Ollama exactly which column was wrong ("column X does not exist")
        which is usually enough for a correct retry.

    Returns a result dict that Phase 5 renders into a dashboard widget:
    {
        "path":      "sql",
        "query":     <original user question>,
        "sql":       <final executed SQL string — may differ from first attempt>,
        "columns":   [<column name strings>],
        "rows":      [<result tuples, max 100>],
        "retried":   True/False  — whether a retry was needed,
        "error":     None        — or error message string if both attempts failed
    }

    The "retried" flag is useful for debugging and for the dashboard to show
    a subtle indicator when a query needed self-correction.
    """
    result = {
        "path":    "sql",
        "query":   user_query,
        "sql":     None,
        "columns": [],
        "rows":    [],
        "retried": False,
        "error":   None,
    }

    # ── Step 1: First Ollama call — generate SQL from natural language ─────────
    try:
        raw = _call_ollama(user_query)
        log.info("Ollama raw output: %s", raw)
    except RuntimeError as e:
        # Ollama is unreachable — no point retrying, return immediately
        result["error"] = str(e)
        return result

    # ── Step 2: Validate the SQL before touching the database ─────────────────
    try:
        sql = _validate_sql(raw)
        result["sql"] = sql
        log.info("Validated SQL: %s", sql)
    except ValueError as e:
        # Malformed or non-SELECT SQL — not safe to retry with this output
        result["error"] = str(e)
        return result

    # ── Step 3: Column validation + execute — with one retry on either failure ─
    # B-003 + B-001 share one retry path: a column-validation ValueError and
    # a Postgres execution error are both "Ollama generated bad SQL" — same
    # remedy, just caught at different stages. Catching them in the same
    # try means Ollama gets one self-correct attempt regardless of which
    # gate flagged the problem.
    try:
        # B-003: catch hallucinated columns BEFORE hitting Postgres.
        # No-op when the schema couldn't be loaded at import; falls through
        # to _execute() which produces a slower but equally-correct error.
        _validate_columns(sql)
        columns, rows     = _execute(sql)
        result["columns"] = columns
        result["rows"]    = rows
        log.info("Query returned %d rows", len(rows))

    except Exception as first_error:
        # First attempt failed (either column validation or execution) — log
        # and attempt one self-correction retry. The retry prompt receives
        # the exact error string so Ollama knows whether it hallucinated a
        # column (B-003) or generated SQL that hit a Postgres error (B-001).
        error_msg = str(first_error)
        log.warning(
            "SQL attempt failed (validation or execution): %s\nSQL was: %s",
            error_msg, sql
        )

        # ── Step 3a: Retry — send error context back to Ollama ────────────────
        # We include both the error message and the failed SQL so Ollama has
        # full context to generate a corrected query.
        log.info("Attempting self-correction retry (B-001/B-003)...")
        result["retried"] = True

        try:
            # Pass the error as context so Ollama knows what to fix
            retry_raw = _call_ollama(
                user_query,
                error_context=f"{error_msg}\nFailed SQL: {sql}"
            )
            log.info("Ollama retry output: %s", retry_raw)

            # Validate the corrected SQL too — don't skip the safety check
            retry_sql = _validate_sql(retry_raw)
            result["sql"] = retry_sql  # update to the corrected version
            log.info("Retry validated SQL: %s", retry_sql)

            # Re-run column validation on the corrected SQL too — Ollama can
            # produce the same hallucination twice. If it does, the retry
            # fails and we surface both errors to the caller.
            _validate_columns(retry_sql)

            # Execute the corrected SQL
            columns, rows     = _execute(retry_sql)
            result["columns"] = columns
            result["rows"]    = rows
            log.info(
                "Retry succeeded — query returned %d rows", len(rows)
            )

        except Exception as retry_error:
            # Retry also failed — return the retry error (it's more informative
            # than the original since it shows what Ollama tried to fix)
            result["error"] = (
                f"Both attempts failed.\n"
                f"First error: {error_msg}\n"
                f"Retry error: {retry_error}\n"
                f"Final SQL attempted: {result['sql']}"
            )
            log.error("Retry also failed: %s", retry_error)

    return result


# ── Frozen-SQL execution path (B-022) ─────────────────────────────────────────
# Once a widget is pinned, its SQL is frozen on dashboard_widgets.generated_sql.
# Every refresh runs THIS SQL — never re-prompts Ollama. Eliminates the SQL
# drift between refreshes that Ollama's non-determinism would otherwise cause,
# and removes ~2-3s of LLM latency per refresh.
#
# Freezing the SQL text does NOT freeze dates inside the SQL — CURRENT_DATE,
# CURRENT_TIMESTAMP, and date_id lookups against now() re-evaluate each run,
# so "current month" widgets still auto-update correctly.

def run_stored_sql(sql: str, user_query: str = "") -> dict:
    """
    Execute a previously-frozen SQL string. No Ollama call, no retry loop.

    Used by the dashboard refresh path (B-022) — the SQL was generated and
    validated once at pin time and stored on dashboard_widgets.generated_sql.
    Every subsequent refresh just re-runs it.

    Args:
        sql:        The frozen SQL string from dashboard_widgets.generated_sql.
        user_query: The original natural-language prompt — copied into the
                    result dict for the renderer's debug/title use. Optional.

    Returns the same shape as run() so widget_renderer.build_widget_config()
    works on either result interchangeably:
    {
        "path":      "sql",
        "query":     <original user question>,
        "sql":       <the frozen SQL — unchanged>,
        "columns":   [<column name strings>],
        "rows":      [<result tuples, max 100>],
        "retried":   False        — never retries on this path
        "error":     None         — or error message string on failure
    }

    Why no retry on this path:
        The SQL was validated and successfully executed at pin time. If it
        fails now, the schema has changed underneath it — retrying with the
        same SQL won't help. The user can use "regenerate SQL" in the widget
        settings (a future B-022 follow-up) to re-author the query.
    """
    result = {
        "path":    "sql",
        "query":   user_query,
        "sql":     sql,
        "columns": [],
        "rows":    [],
        "retried": False,
        "error":   None,
    }

    # Re-validate even though the SQL was validated at pin time. The cache
    # column is just a TEXT field; cheap to verify it is still SELECT-only.
    try:
        validated = _validate_sql(sql)
    except ValueError as e:
        result["error"] = str(e)
        return result

    try:
        columns, rows     = _execute(validated)
        result["columns"] = columns
        result["rows"]    = rows
        log.info(
            "Stored SQL returned %d rows (no Ollama call)", len(rows)
        )
    except Exception as e:
        # Schema drift, permission change, etc. — return the error rather
        # than retrying. The widget settings UI exposes "regenerate SQL"
        # for the rare case where re-authoring is wanted.
        result["error"] = f"Stored SQL execution failed: {e}\nSQL: {sql}"
        log.error("Stored SQL execution failed: %s", e)

    return result
