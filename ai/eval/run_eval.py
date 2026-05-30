"""
run_eval.py — Text-to-SQL accuracy eval runner (B-058)
======================================================
Part of: TravelLens — on-demand accuracy eval harness for the Text-to-SQL path.

    Run:  python -m ai.eval.run_eval
          python -m ai.eval.run_eval --runs 5
          python -m ai.eval.run_eval --only sql
          python -m ai.eval.run_eval --id top5_cities_by_revenue

WHAT THIS IS (and is NOT)
    A MEASUREMENT tool. It feeds a fixed fixture of natural-language questions
    through ai.main.answer() against the live DB + Ollama, scores each by EXECUTION
    MATCH against an owner-verified reference query, flags the two known
    confident-wrong failure modes (L-011 missing DISTINCT, L-013 missing
    cancellation filter), and prints a results table + aggregate accuracy %.

    It is NOT a pytest regression suite and is NOT part of `pytest tests/`. Ollama
    non-determinism means a single run passes/fails by luck — so every question is
    run N times (default 3) and a per-question pass-rate is reported.

GRADING DESIGN (see ai/eval/eval_questions.py header for the full contract)
    - Execution match, not SQL text: run the model's SQL (via answer()) AND the
      reference_sql, compare normalized RESULT SETS. Both run against live data, so
      the metric survives a dataset regenerate.
    - Normalization: numbers rounded to 2 dp HALF-UP (matching Postgres ROUND),
      stringified, compared as a multiset of value-tuples (row-order-insensitive),
      OR as an ordered list when the fixture sets ordered: True. Positional columns
      (names ignored, count enforced).
    - Flags inspect the GENERATED SQL independently of execution match — a result
      can match by coincidence while the SQL is structurally wrong.

HONEST DENOMINATOR
    Two situations make an exec-match meaningless rather than failed, and both are
    EXCLUDED from the accuracy % (and reported separately) so a harness artifact
    never masquerades as a model error:
      - INDETERMINATE (cap): the reference OR model result set hit MAX_ROWS (100),
        so the compared rows are an arbitrary truncated/ordered subset.
      - PROBE (scored: False): the question's correct answer is inherently
        under-determined (e.g. "5 of 1,418 valid rows"); it exists for its flag only.

SKIP CLEANLY
    If Postgres or Ollama is unreachable the harness prints a clear SKIP and exits
    2 — it never reports a fake 100%. Mirrors the skip-don't-pass-vacuously posture
    of tests/test_validate_columns.py and tests/test_hybrid_queries.py.

EXIT CODES
    0  harness ran (any accuracy %, including 0%)
    2  clean skip (Postgres or Ollama down)
    other nonzero  harness crashed
"""

import argparse
import os
import re
import sys
import datetime
from collections import Counter
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
import requests

# Import & REUSE — never redefine DB config or the SELECT-only guard (CLAUDE.md).
from ai.text_to_sql import DB_CONFIG, _validate_sql, OLLAMA_HOST, MAX_ROWS
from ai.main import answer
from ai.eval.eval_questions import QUESTIONS

_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


# ══════════════════════════════════════════════════════════════════════════════
# Preflight — skip cleanly, never fake a result
# ══════════════════════════════════════════════════════════════════════════════

def check_postgres() -> bool:
    """True if the warehouse answers within a short timeout."""
    try:
        conn = psycopg2.connect(connect_timeout=5, **DB_CONFIG)
        conn.close()
        return True
    except Exception as e:
        print(f"SKIP - travellens-postgres unreachable ({e}). "
              f"Start the stack (docker compose up -d) and retry.")
        return False


def check_ollama() -> bool:
    """True if the Ollama daemon answers a tags probe."""
    try:
        resp = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"SKIP - Ollama unreachable at {OLLAMA_HOST} ({e}). "
              f"Start it (ollama serve) and retry.")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Reference execution — validated, capped, overflow-aware
# ══════════════════════════════════════════════════════════════════════════════

def run_reference_sql(conn, ref_sql: str):
    """
    Execute an owner-verified reference SQL on a live connection.

    Validates with ai.text_to_sql._validate_sql first (SELECT-only insurance —
    the reference is owner-authored, but we run the same guard the production
    path runs). Fetches MAX_ROWS + 1 rows so we can DETECT overflow: if the
    reference legitimately returns more than MAX_ROWS rows, exec-match against
    the model's MAX_ROWS-capped output is meaningless (an arbitrary subset), so
    the caller treats it as INDETERMINATE.

    Returns (columns, rows, overflow_bool).
    """
    validated = _validate_sql(ref_sql)
    with conn.cursor() as cur:
        cur.execute(validated)
        columns = [d[0] for d in cur.description]
        fetched = cur.fetchmany(MAX_ROWS + 1)
    overflow = len(fetched) > MAX_ROWS
    return columns, fetched[:MAX_ROWS], overflow


# ══════════════════════════════════════════════════════════════════════════════
# Comparator
# ══════════════════════════════════════════════════════════════════════════════

def _norm_cell(v):
    """
    Normalize one value for comparison.

    - None              -> a stable NULL token (so NULL == NULL, NULL != '').
    - bool              → its str ('True'/'False'). Checked BEFORE the numeric
                          branch because bool is a subclass of int in Python.
    - int/float/Decimal → rounded to 2 dp HALF-UP via Decimal(str(v)). Half-up
                          matches Postgres numeric ROUND() (Python's round() is
                          banker's rounding and disagrees on .xx5 boundaries).
                          Decimal(str(v)) also avoids float-binary artifacts.
                          ALL numerics go through this — so an int 16 and a
                          Decimal('16') both normalize to '16.00', killing
                          int-vs-Decimal cross-type false-negatives.
    - everything else   → str(v).
    """
    if v is None:
        return "<NULL>"  # stable NULL token (ASCII-safe for Windows cp1252 consoles)
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float, Decimal)):
        return str(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return str(v)


def _norm_rows(rows):
    """Normalize a list of row tuples into a list of value-tuples (column names
    are intentionally dropped — comparison is positional)."""
    return [tuple(_norm_cell(v) for v in row) for row in rows]


def exec_match(model_columns, model_rows, ref_columns, ref_rows, ordered: bool) -> bool:
    """
    True iff the model's result set matches the reference's after normalization.

    - Requires the same number of VALUE COLUMNS (positional compare; names ignored
      so alias differences don't matter).
    - ordered=True  → compare as ordered lists (ranking / top-N).
      ordered=False → compare as multisets (row-order-insensitive).
    """
    if len(model_columns) != len(ref_columns):
        return False
    m = _norm_rows(model_rows)
    r = _norm_rows(ref_rows)
    if ordered:
        return m == r
    return Counter(m) == Counter(r)


# ══════════════════════════════════════════════════════════════════════════════
# Flag checks — inspect the GENERATED SQL, independent of execution match.
# Small standalone checks. No lint module is imported (it does not exist yet —
# this harness ships first and will measure it later).
# ══════════════════════════════════════════════════════════════════════════════

def _touches_fact_bookings(sql: str) -> bool:
    return re.search(r"\bfact_bookings\b", sql, re.IGNORECASE) is not None


def _is_rate_query(sql: str) -> bool:
    """
    A RATE query computes a ratio/percentage and so legitimately keeps cancelled
    rows in the denominator. Defined NARROWLY so a unit conversion (/ 1e7 for
    crore) or a bare division is NOT mistaken for a rate — mistaking it would
    suppress cancellation_filter_missing on the exact L-013 cases this harness
    exists to catch.

    Matches only:
      - FILTER (WHERE ... is_cancelled)   — the canonical rate-numerator form
      - "100.0 *" / "100 *"               — percentage scaling
      - "/ NULLIF(COUNT("                 — ratio over a row count
      - a SELECT alias named rate|ratio|share|percent|pct
    Division by a literal (1e7, 1e5, 100000) is a unit scale, NOT a rate.
    """
    if re.search(r"FILTER\s*\(\s*WHERE[^)]*is_cancelled", sql, re.IGNORECASE):
        return True
    if re.search(r"\b100(?:\.0)?\s*\*", sql):
        return True
    if re.search(r"/\s*NULLIF\s*\(\s*COUNT", sql, re.IGNORECASE):
        return True
    if re.search(r"\bAS\s+\w*(?:rate|ratio|share|percent|pct)\w*", sql, re.IGNORECASE):
        return True
    return False


def _has_cancellation_filter(sql: str) -> bool:
    """True if the SQL excludes cancelled rows via NOT ...is_cancelled or a
    FILTER (WHERE ... is_cancelled) predicate."""
    return re.search(
        r"(NOT\s+[\w.]*is_cancelled)|(FILTER\s*\(\s*WHERE[^)]*is_cancelled)",
        sql, re.IGNORECASE,
    ) is not None


def flag_distinct_missing(sql: str, entry: dict) -> bool:
    """L-011: the fixture expects DISTINCT but the model SQL has none."""
    if not entry.get("expects_distinct"):
        return False
    return re.search(r"\bDISTINCT\b", sql, re.IGNORECASE) is None


def flag_cancellation_filter_missing(sql: str, entry: dict) -> bool:
    """
    L-013: a fact_bookings aggregate that should exclude cancelled rows but does
    not. Suppressed for rate queries (the filter must NOT apply there) and for
    queries that don't touch fact_bookings.
    """
    if not entry.get("expects_cancellation_filter"):
        return False
    if not _touches_fact_bookings(sql):
        return False
    if _is_rate_query(sql):
        return False
    return not _has_cancellation_filter(sql)


def compute_flags(sql: str, entry: dict) -> list:
    """Return the list of flag names that fire on this generated SQL."""
    flags = []
    if flag_cancellation_filter_missing(sql, entry):
        flags.append("cancellation_filter_missing")
    if flag_distinct_missing(sql, entry):
        flags.append("distinct_missing")
    return flags


# ══════════════════════════════════════════════════════════════════════════════
# Per-question evaluation
# ══════════════════════════════════════════════════════════════════════════════

# Per-run outcome categories (mutually exclusive).
_MATCH        = "match"          # exec-match passed (scored)
_NOMATCH      = "nomatch"        # exec-match failed (scored)
_INDETERMINATE = "indeterminate"  # cap overflow — not scored toward accuracy
_PROBE        = "probe"          # scored: False entry — not scored toward accuracy
_ERROR        = "error"          # answer() returned an error
_MISROUTE     = "misroute"       # answer() returned path != 'sql'

# Error subtypes — so a B-059-style validator false-positive is visibly separated
# from a genuine model failure or an infra outage. answer() never raises; the
# compound result["error"] string carries the root cause(s), which we key on.
_ERR_VALIDATOR = "validator_rejection"  # _validate_columns ValueError (B-003 / B-059 family)
_ERR_MODEL_SQL = "model_sql_error"      # model SQL hit a Postgres execution error
_ERR_OLLAMA    = "ollama_unreachable"   # Ollama daemon unreachable
_ERR_OTHER     = "other"                # parse / non-SELECT / answer() raised


def classify_error(error_text: str) -> str:
    """
    Bucket an answer()-error string by root cause.

    Priority order matters: the _validate_columns ValueError carries the unique
    phrase 'This was hallucinated', so we check it before the generic Postgres
    'does not exist' text (a validator rejection IS a 'does not exist', but we
    want it counted as a validator rejection, not a model SQL error).
    """
    t = (error_text or "").lower()
    if "ollama" in t and "reachable" in t:
        return _ERR_OLLAMA
    if "this was hallucinated" in t:
        return _ERR_VALIDATOR
    if "does not exist" in t or "line " in t or "syntax error" in t or "psycopg2" in t:
        return _ERR_MODEL_SQL
    return _ERR_OTHER


def eval_question(entry: dict, runs: int, conn) -> dict:
    """
    Run `entry` `runs` times through answer(), grade each run, and aggregate.

    Returns a per-question summary dict consumed by the report formatter.
    """
    ordered = entry.get("ordered", False)
    scored_entry = entry.get("scored", True)

    # Reference runs once — it is deterministic against live data.
    ref_columns, ref_rows, ref_overflow = run_reference_sql(conn, entry["reference_sql"])

    outcomes = []             # one category per run
    flag_counter = Counter()  # flag-name -> times fired across runs
    error_subtypes = Counter()  # error-subtype -> times across runs

    for _ in range(runs):
        try:
            res = answer(entry["question"])
        except Exception as e:  # answer() is documented not to raise, but be safe
            outcomes.append(_ERROR)
            error_subtypes[_ERR_OTHER] += 1
            print(f"    [{entry['id']}] answer() raised: {e}", file=sys.stderr)
            continue

        if res.get("path") != "sql":
            # Misroute also passively measures router accuracy (B-056 forward-compat:
            # a future hybrid_aggregation path would land here too — correct as-is).
            outcomes.append(_MISROUTE)
            continue
        if res.get("error"):
            outcomes.append(_ERROR)
            error_subtypes[classify_error(res["error"])] += 1
            continue

        model_sql = res.get("sql") or ""
        # Flags are recorded on EVERY valid SQL run, independent of exec-match.
        for f in compute_flags(model_sql, entry):
            flag_counter[f] += 1

        # Cap-awareness: if either side hit the row cap, the comparison is over an
        # arbitrary truncated subset — INDETERMINATE, not a pass/fail.
        model_rows = res.get("rows", [])
        model_overflow = len(model_rows) >= MAX_ROWS
        if not scored_entry:
            outcomes.append(_PROBE)
            continue
        if ref_overflow or model_overflow:
            outcomes.append(_INDETERMINATE)
            continue

        ok = exec_match(res.get("columns", []), model_rows,
                        ref_columns, ref_rows, ordered)
        outcomes.append(_MATCH if ok else _NOMATCH)

    counts = Counter(outcomes)
    scored_runs = counts[_MATCH] + counts[_NOMATCH]
    matches = counts[_MATCH]

    return {
        "id":             entry["id"],
        "path":           entry["path"],
        "runs":           runs,
        "outcomes":       counts,
        "scored_runs":    scored_runs,
        "matches":        matches,
        "flags":          flag_counter,
        "error_subtypes": error_subtypes,
        "ref_overflow":   ref_overflow,
        "scored_entry":   scored_entry,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Reporting
# ══════════════════════════════════════════════════════════════════════════════

def _exec_match_cell(summ: dict) -> str:
    """Render the EXEC-MATCH column for one question."""
    if not summ["scored_entry"]:
        return "probe"
    if summ["scored_runs"] == 0:
        # No scored runs — say why (cap vs error/misroute dominated).
        if summ["outcomes"].get(_INDETERMINATE):
            return "n/a (cap)"
        return "n/a"
    pct = 100.0 * summ["matches"] / summ["scored_runs"]
    return f"{pct:5.1f}%"


def _flags_cell(summ: dict) -> str:
    """Quality flags (L-011/L-013) plus per-run error subtypes (err:<subtype>),
    so a B-059 validator rejection is visibly distinct from a model failure."""
    parts = [f"{name}x{n}" for name, n in sorted(summ["flags"].items())]
    parts += [f"err:{name}x{n}" for name, n in sorted(summ["error_subtypes"].items())]
    return ", ".join(parts) if parts else "-"


def build_report(summaries: list, runs: int) -> str:
    """Assemble the full printable report (table + aggregate block) as text."""
    lines = []
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines.append("=" * 100)
    lines.append(f"TravelLens - Text-to-SQL accuracy eval (B-058)   {ts}   runs/question={runs}")
    lines.append("=" * 100)

    header = f"{'TEST':<34} {'PATH':<5} {'RUNS':>4} {'PASS/TOTAL':>10} {'EXEC-MATCH':>11}  FLAGS"
    lines.append(header)
    lines.append("-" * 100)

    for s in summaries:
        pass_total = f"{s['matches']}/{s['runs']}"
        row = (f"{s['id']:<34} {s['path']:<5} {s['runs']:>4} {pass_total:>10} "
               f"{_exec_match_cell(s):>11}  {_flags_cell(s)}")
        lines.append(row)

    lines.append("-" * 100)

    # ── Aggregate ─────────────────────────────────────────────────────────────
    total_runs   = sum(s["runs"] for s in summaries)
    scored_runs  = sum(s["scored_runs"] for s in summaries)
    total_match  = sum(s["matches"] for s in summaries)
    cat = Counter()
    for s in summaries:
        cat.update(s["outcomes"])
    flag_totals = Counter()
    err_totals = Counter()
    for s in summaries:
        flag_totals.update(s["flags"])
        err_totals.update(s["error_subtypes"])

    # HEADLINE — errors count as failures. Accuracy over ALL runs (passed / total),
    # so a both-attempts error or a misroute is a non-pass, not a free exclusion.
    headline = (100.0 * total_match / total_runs) if total_runs else 0.0
    # SECONDARY — among runs that produced runnable SQL and a determinate compare.
    among_scored = (100.0 * total_match / scored_runs) if scored_runs else 0.0
    err = cat.get(_ERROR, 0)
    mis = cat.get(_MISROUTE, 0)

    lines.append("AGGREGATE")
    lines.append(f"  execution accuracy (headline)    : {headline:.1f}%   "
                 f"({total_match}/{total_runs} of ALL runs - errors & misroutes count as fails)")
    lines.append(f"  among scored runs only           : {among_scored:.1f}%   "
                 f"({total_match}/{scored_runs}; excludes cap={cat.get(_INDETERMINATE, 0)} "
                 f"probe={cat.get(_PROBE, 0)} error={err} misroute={mis})")
    lines.append(f"  cancellation_filter_missing (L-013): {flag_totals.get('cancellation_filter_missing', 0)}")
    lines.append(f"  distinct_missing (L-011)         : {flag_totals.get('distinct_missing', 0)}")
    lines.append(f"  error rate                       : {err}/{total_runs} "
                 f"({(100.0*err/total_runs if total_runs else 0):.1f}%)")
    # Error subtype breakdown — separates B-059 validator false-positives from
    # genuine model SQL failures and infra outages.
    if err_totals:
        for name in (_ERR_VALIDATOR, _ERR_MODEL_SQL, _ERR_OLLAMA, _ERR_OTHER):
            if err_totals.get(name):
                lines.append(f"      - {name:<22}: {err_totals[name]}")
    lines.append(f"  misroute rate                    : {mis}/{total_runs} "
                 f"({(100.0*mis/total_runs if total_runs else 0):.1f}%)")
    lines.append(f"  indeterminate (cap) / probe runs : "
                 f"{cat.get(_INDETERMINATE, 0)} / {cat.get(_PROBE, 0)}  "
                 f"(excluded from both accuracy denominators above except headline total)")
    lines.append("=" * 100)
    return "\n".join(lines)


def write_results(report: str) -> str:
    """Write the report to ai/eval/results/eval_<UTC-timestamp>.txt; return path."""
    os.makedirs(_RESULTS_DIR, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(_RESULTS_DIR, f"eval_{stamp}.txt")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(report + "\n")
    return path


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="python -m ai.eval.run_eval",
        description="Text-to-SQL accuracy eval harness (B-058). Measurement tool, "
                    "not a pytest suite.",
    )
    p.add_argument("--runs", type=int, default=3,
                   help="runs per question (default 3 — averages out Ollama non-determinism)")
    p.add_argument("--only", choices=["sql"],
                   help="restrict to a path. Only 'sql' is supported this cut.")
    p.add_argument("--id",
                   help="run a single fixture question by its id")
    return p.parse_args(argv)


def select_questions(args) -> list:
    qs = QUESTIONS
    if args.only:
        qs = [q for q in qs if q.get("path") == args.only]
    if args.id:
        qs = [q for q in qs if q["id"] == args.id]
        if not qs:
            print(f"No fixture question with id '{args.id}'. "
                  f"Available: {', '.join(q['id'] for q in QUESTIONS)}")
            sys.exit(1)
    return qs


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.runs < 1:
        print("--runs must be >= 1")
        return 1

    # Preflight — clean skip (exit 2) so "infra down" never reads as a real result.
    if not check_postgres():
        return 2
    if not check_ollama():
        return 2

    questions = select_questions(args)
    print(f"Running {len(questions)} question(s) x {args.runs} run(s) "
          f"against live DB + Ollama. This calls the model - expect a few seconds "
          f"per run...\n")

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        summaries = []
        for q in questions:
            print(f"  - {q['id']} ...", flush=True)
            summaries.append(eval_question(q, args.runs, conn))
    finally:
        conn.close()

    report = build_report(summaries, args.runs)
    print("\n" + report)

    out_path = write_results(report)
    print(f"\nResults written to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
