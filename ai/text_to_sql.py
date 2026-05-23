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

    # ── Step 3: Execute — with one retry on failure ───────────────────────────
    try:
        columns, rows     = _execute(sql)
        result["columns"] = columns
        result["rows"]    = rows
        log.info("Query returned %d rows", len(rows))

    except Exception as first_error:
        # First execution failed — log it and attempt one self-correction retry
        error_msg = str(first_error)
        log.warning(
            "SQL execution failed on first attempt: %s\nSQL was: %s",
            error_msg, sql
        )

        # ── Step 3a: Retry — send error context back to Ollama ────────────────
        # We include both the error message and the failed SQL so Ollama has
        # full context to generate a corrected query.
        log.info("Attempting self-correction retry (B-001)...")
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
