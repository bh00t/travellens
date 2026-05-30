# Phase 4 — AI Layer: Text-to-SQL + Semantic Search

> **Stack:** Python 3.11 · Ollama (Qwen2.5-Coder-7B) · pgvector · sentence-transformers · Postgres 16  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Files:** `ai/query_router.py` · `ai/text_to_sql.py` · `ai/semantic_search.py` · `ai/main.py`  
> **Status:** [ ] In progress / [x] Complete  

> **HISTORY DOCUMENT** — This records how Phase 4 was originally built and how it evolved. For current behaviour of the AI layer, see [CLAUDE.md](../CLAUDE.md) · [backlog.md](backlog.md).

> For per-query accuracy levels and known caveats, see [`capabilities-and-limits.md`](./capabilities-and-limits.md).

---


## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Files this phase
creates / touches:

- **CREATE** `ai/__init__.py` (empty — required for Python package)
- **CREATE** `ai/main.py` (rename from `ai_main.py`)
- **CREATE** `ai/query_router.py`
- **CREATE** `ai/text_to_sql.py`
- **CREATE** `ai/semantic_search.py`
- **CREATE** `ai/prompts/text_to_sql_system.txt`
- **MODIFY** `.env` — add `OLLAMA_HOST`, `OLLAMA_MODEL`

## OBJECTIVE

Build a dual-path natural language query engine that takes a plain English question and
returns a structured result. SQL questions go to Ollama for query generation and execute
against Postgres. Review/opinion questions embed the query, search pgvector for similar
reviews, and summarise themes via Ollama.

End-to-end: `python ai/main.py "top 5 cities by revenue"` → structured result dict ready
for Phase 5 to render.

---

## PREREQUISITES

- [ ] Phase 3 complete: all 30K rows in `reviews_raw` have non-NULL embeddings,
  IVFFlat index `idx_reviews_embedding` exists
- [ ] Ollama running with `qwen2.5-coder:7b` pulled:
  ```bash
  ollama list                  # must show qwen2.5-coder:7b
  curl http://localhost:11434  # must return "Ollama is running"
  ```
- [ ] `.venv` active with `sentence-transformers`, `psycopg2-binary`, `pgvector`,
  `requests`, `sqlparse`, `python-dotenv` installed
- [ ] `.env` contains `OLLAMA_HOST=http://localhost:11434` and
  `OLLAMA_MODEL=qwen2.5-coder:7b`

---

## DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| `query_router.py` | `ai/` | Self-test passes all 10 cases |
| `text_to_sql.py` | `ai/` | Returns rows for known-good SQL queries |
| `semantic_search.py` | `ai/` | Returns reviews + summary for review queries |
| `main.py` | `ai/` | Single entry point routes and returns correct result |
| `text_to_sql_system.txt` | `ai/prompts/` | SELECT-only header + SCHEMA + JOIN MAP + COLUMN LOCATION + ENTITY COUNT RULE + OUTPUT RULES (general rules over the data model, not few-shot question→SQL pairs) |

---

## ARCHITECTURE DECISIONS (ORIGINAL)

### Keyword classifier router, not LLM router

The router classifies a query as SQL or semantic in microseconds with zero API cost.
An LLM call for routing adds 1–3 seconds latency and costs one full Ollama inference
just to decide which path to take. At this scale the keyword approach is not a
compromise — it's correct.

Trigger words are in `SEMANTIC_TRIGGERS` in `query_router.py`. If routing feels wrong
on any query, add or remove a word from that set — no other code changes needed.

### Ollama for both paths, same model

Qwen2.5-Coder-7B runs locally on RTX 3070. Used for SQL generation and review
summarisation. No API cost, no data leaving the machine, no rate limits.

Trade-off vs Claude API: lower accuracy on hard queries (~75–85% vs ~95% on Spider-hard).
For a portfolio project demoing realistic hotel queries, the system prompt with full
schema DDL plus general join, column-location, entity-count, and output rules closes
most of the gap. The prompt deliberately avoids few-shot question→SQL pairs — those
drift the moment a new query shape arrives; the general rules generalise.

### SELECT-only SQL guard

The system prompt hard-constrains Ollama to SELECT only. `text_to_sql.py` validates
with `sqlparse` before execution — any non-SELECT statement is rejected with an error.
This is the security boundary. Never remove it.

### Top-20 reviews for semantic summarisation

pgvector returns 20 most similar reviews. Ollama summarises the top 3 themes from
those 20. 20 is enough to find real patterns without overwhelming the model's context
window or slowing response time.

### City scoping in semantic search

If the query mentions a known city (from `dim_location`), the pgvector search adds a
JOIN to restrict results to that city's hotels. Prevents "complaints in Goa" returning
reviews from Mumbai hotels that happen to be semantically similar.

---

## STEPS

### Step 1 — Write `ai/prompts/text_to_sql_system.txt`

Create the system prompt file. It must contain:

1. Hard constraint at the top:
   ```
   You only generate SELECT statements.
   Never generate INSERT, UPDATE, DELETE, DROP, CREATE or any other statement.
   Return only the SQL query. No explanation. No markdown. No backticks.
   ```

2. Full schema DDL — copy from `db/schema.sql`. Ollama needs every table and column.

3. India-specific context:
   - Currency is INR (₹), stored as full rupee values in `revenue_inr`
   - Cities: Goa, Mumbai, Delhi, Jaipur, Udaipur, Manali, Kerala, Agra,
     Varanasi, Chennai, Hyderabad, Bangalore, Kolkata, Pune, Shimla
   - Seasons: peak (Dec–Jan), shoulder (Oct, Feb–Mar), monsoon trough (Jun–Sep)
   - Never use `hotel_master.city` — cities are in `dim_location`

4. General rule sections (NOT question→SQL example pairs):
   - **JOIN MAP** — copy-paste FROM/JOIN blocks for the common access patterns
     (need city/state → through `hotel_master`+`dim_location`; need customer
     attributes → through `dim_customer`; need date attributes → through
     `dim_date`), plus DATE HANDLING (`date_id` is a surrogate key, the real
     date is `dim_date.full_date`) and HARD JOIN RULES that forbid
     cross-type equality like `b.hotel_id = l.location_id`.
   - **COLUMN LOCATION** — which columns live on which table only, e.g.
     `is_cancelled` lives ONLY on `fact_bookings`; a dimension-only query
     must not reference it.
   - **ENTITY COUNT RULE** — to count entities (hotels, customers, cities),
     query the dimension table directly; never count over `fact_bookings`
     (which counts booking ROWS, not entities).
   - **OUTPUT RULES** — exclude cancelled bookings on `fact_bookings`
     queries; revenue in crore via `/ 1e7`; ADR; cancellation-rate formula;
     by-city/by-state grouping; LIMIT only for "top N"; `SELECT DISTINCT`
     when listing repeatable entities; `fact_bookings` is the analytics
     source — `agg_daily_hotel_kpi` / `agg_hourly_city_stats` /
     `pipeline_metrics` are operational tables and must NOT be used for
     business KPIs.

---

### Step 2 — Copy `ai/query_router.py`

Source file: `scripts/query_router.py` (provided separately)

Contains:
- `SEMANTIC_TRIGGERS` set — the full vocabulary of review-path trigger words
- `route(query)` function — returns `'sql'` or `'semantic'`
- Self-test block at the bottom with 10 labelled test cases

Read the comments in the file — they explain why keyword classifier over LLM,
and how to tune the trigger list.

---

### Step 3 — Copy `ai/text_to_sql.py`

Source file: `scripts/text_to_sql.py` (provided separately)

Contains:
- `_call_ollama(user_query)` — sends query + system prompt to Ollama
- `_validate_sql(sql)` — strips markdown fences, enforces SELECT-only via sqlparse
- `_execute(sql)` — runs validated SQL against Postgres, caps at 100 rows
- `run(user_query)` — full pipeline, returns result dict

Read the comments — they explain why validation runs before execution, why
`MAX_ROWS = 100` exists, and why the system prompt is loaded at module import.

---

### Step 4 — Copy `ai/semantic_search.py`

Source file: `scripts/semantic_search.py` (provided separately)

Contains:
- `_load_cities(conn)` — populates known city set from `dim_location` on first call
- `_detect_city(query)` — scans query for city names, returns match or None
- `_search_reviews(conn, query_vec, city)` — pgvector `<=>` cosine search with
  optional city scoping via JOIN on hotel_master + dim_location
- `_summarise(query, reviews)` — sends top-20 reviews to Ollama, returns 3 themes
- `run(user_query)` — full pipeline, returns result dict

Read the comments — they explain why city scoping matters, why TOP_K=20, and
why the SentenceTransformer model loads at module import not inside run().

---

### Step 5 — Copy `ai/main.py`

Source file: `scripts/ai_main.py` (provided separately — rename to `main.py`)

Contains:
- `answer(query)` — the function Phase 5 imports. Routes and returns result dict.
- CLI block — `python ai/main.py "question"` for development testing

Read the comments — they explain why this thin wrapper exists as a seam between
the AI layer and Phase 5.

---

### Step 6 — Parse-check all files

```bash
python -c "import ast; ast.parse(open('ai/query_router.py').read());    print('router OK')"
python -c "import ast; ast.parse(open('ai/text_to_sql.py').read());     print('text_to_sql OK')"
python -c "import ast; ast.parse(open('ai/semantic_search.py').read()); print('semantic_search OK')"
python -c "import ast; ast.parse(open('ai/main.py').read());            print('main OK')"
```

All must print `OK` before proceeding.

---

### Step 7 — Run router self-test

```bash
python ai/query_router.py
```

Expected: all 10 test cases show `✓`. If any fail, adjust `SEMANTIC_TRIGGERS`
in `query_router.py` and re-run until all pass.

---

### Step 8 — Test SQL path

```bash
python ai/main.py "top 5 cities by revenue"
python ai/main.py "cancellation rate by customer segment"
python ai/main.py "average daily rate for 5-star hotels in Goa"
```

Each must return rows with no error. Check the printed SQL — does it match what
you'd write by hand? If a query produces wrong SQL, copy it into psql to debug,
then tighten the matching rule section in `text_to_sql_system.txt` — the JOIN MAP
block if it used the wrong join shape, COLUMN LOCATION if it referenced a column
on the wrong table, ENTITY COUNT RULE if it counted bookings instead of entities,
or OUTPUT RULES otherwise. Adding a one-off question→SQL example for the failing
query is a last resort, not the first fix.

---

### Step 9 — Test semantic path

```bash
python ai/main.py "complaints about AC not working"
python ai/main.py "what are guests saying about cleanliness in Goa"
python ai/main.py "rude staff experiences"
```

Each must return reviews + a 3-theme Ollama summary. Check:
- Are the retrieved reviews actually topically relevant?
- Does the Ollama summary accurately reflect the review content?

---

### Step 10 — Commit

STOP — the owner commits manually. Do not run `git add` or `git commit` from
this agent session. The owner reviews the diff and commits themselves.

---

## ACCEPTANCE TESTS

```bash
# 1. Router self-test — all 10 cases must pass
python ai/query_router.py

# 2. SQL path returns rows for a known-good query
python -c "
from ai.main import answer
r = answer('top 5 cities by revenue')
assert r['path'] == 'sql',    'wrong path'
assert r['error'] is None,    f'error: {r[\"error\"]}'
assert len(r['rows']) > 0,    'no rows returned'
print('SQL path OK —', len(r['rows']), 'rows')
"

# 3. Semantic path returns reviews + summary
python -c "
from ai.main import answer
r = answer('complaints about dirty rooms')
assert r['path'] == 'semantic', 'wrong path'
assert r['error'] is None,      f'error: {r[\"error\"]}'
assert len(r['reviews']) > 0,   'no reviews returned'
assert r['summary'],            'empty summary'
print('Semantic path OK —', len(r['reviews']), 'reviews')
print('Summary preview:', r['summary'][:100])
"

# 4. City scoping works
python -c "
from ai.main import answer
r = answer('guest complaints in Goa')
assert r['city'] == 'goa', f'city not detected: {r[\"city\"]}'
print('City scoping OK — city =', r['city'])
"

# 5. SQL injection guard — non-SELECT must be rejected
python -c "
from ai.text_to_sql import _validate_sql
try:
    _validate_sql('DROP TABLE fact_bookings;')
    print('FAIL — should have raised ValueError')
except ValueError as e:
    print('SQL guard OK —', str(e)[:60])
"

# 6. End-to-end latency
time python ai/main.py "top 5 cities by revenue"
time python ai/main.py "complaints about AC not working"
# SQL path target: < 5s on GPU, < 20s on CPU
# Semantic path target: < 10s on GPU, < 30s on CPU
```

All 6 must pass before moving to Phase 5.

### Hardening tests (added in Phase 5)

The six tests above are the original Phase 4 acceptance bar and still apply.
Two additional pytest suites were added during Phase 5 hardening to lock in
the B-003 and B-004 fixes against regression. Run them in addition to the
six commands above before declaring the AI layer stable.

```bash
# B-003 — column-name validation (information_schema + sqlparse guard)
pytest tests/test_validate_columns.py -v

# B-004 — hybrid queries (filter detection + hotel_id scoping)
pytest tests/test_hybrid_queries.py -v

# Full suite — everything under tests/
pytest tests/ -v
```

These are regression suites, not replacements. The hybrid-queries file
includes both fast unit tests on `detect_filters()` and slower
stack-hitting tests marked `@pytest.mark.slow`; run
`pytest tests/test_hybrid_queries.py -v -m "not slow"` for the fast
detection layer alone (sub-second). Both suites skip cleanly when
`travellens-postgres` is unreachable rather than passing vacuously.

---

## EXPLORE

Use these after Phase 4 is working to understand how the AI layer behaves
and to tune it.

---

### E1 — Try SQL queries across different patterns

```bash
python ai/main.py "top 10 hotels by revenue in Mumbai"
python ai/main.py "monthly booking trend for 2025"
python ai/main.py "which customer segment has highest cancellation rate"
python ai/main.py "occupancy rate by star category"
python ai/main.py "average booking value for business travellers"
python ai/main.py "price changes in Goa last 30 days"
```

Look at the generated SQL in each output. Does it match what you'd write manually?
If not — copy the SQL into psql, figure out what it should be, then tighten the
matching rule section in `text_to_sql_system.txt` (JOIN MAP / COLUMN LOCATION /
ENTITY COUNT RULE / OUTPUT RULES). A one-off example for that exact query is a
last resort — the general rule generalises to every related query.

---

### E2 — Try semantic queries from different angles

```bash
python ai/main.py "AC not working"
python ai/main.py "room was too hot to sleep"
python ai/main.py "no cooling in the room"
python ai/main.py "what are guests saying about breakfast"
python ai/main.py "cleanliness issues in budget hotels"
python ai/main.py "best things about 5-star hotels in Goa"
python ai/main.py "rude or unhelpful staff"
python ai/main.py "staff was very polite and helpful"
```

The first three queries all describe the same problem in different words — they
should return overlapping reviews. That's semantic search working correctly.

---

### E3 — Test city scoping

```bash
python ai/main.py "complaints in Goa hotels"
python ai/main.py "guest experiences in Mumbai"
python ai/main.py "what do people say about Jaipur hotels"
```

Each result should show the detected city. Reviews should only come from
hotels in that city — verify by checking `hotel_id` values against
`hotel_master` in psql.

---

### E4 — Test router edge cases

```bash
python ai/main.py "best hotel in Goa"
python ai/main.py "revenue from Goa"
python ai/main.py "dirty rooms in Mumbai"
python ai/main.py "bookings in Mumbai"
```

`"best hotel in Goa"` is ambiguous — it could go either way. See where it routes.
`"revenue from Goa"` should go SQL despite mentioning a city.
`"dirty rooms"` should go semantic.
`"bookings in Mumbai"` should go SQL.

If any feel wrong, tune `SEMANTIC_TRIGGERS` in `query_router.py`.

---

### E5 — Check Ollama is using GPU

While a query is running, open a second terminal:

```bash
nvidia-smi
```

Should show `ollama` process using GPU memory. If not, Ollama is on CPU and
queries will be slow (~8–15s). Fix: update Nvidia drivers to CUDA 12.x.

---

## DO NOT

- Do not execute non-SELECT SQL — the `_validate_sql` guard must stay in place
- Do not remove `MAX_ROWS = 100` — prevents accidental full-table result sets
- Do not move `SentenceTransformer(...)` inside `run()` — it would reload the
  model on every query call (~2 second penalty each time)
- Do not hardcode the Ollama model name in code — always read from `.env`
- Do not use `hotel_master.city` in any SQL — cities are in `dim_location`
- Do not strip the general rule sections (SCHEMA / JOIN MAP / COLUMN LOCATION /
  ENTITY COUNT RULE / OUTPUT RULES) from the system prompt — those rules are the
  primary SQL-accuracy lever. The prompt is deliberately NOT a few-shot example
  list; adding one-off question→SQL pairs is a last resort, not a tuning method

---

## ROLLBACK

```bash
rm ai/query_router.py ai/text_to_sql.py ai/semantic_search.py ai/main.py
rm ai/prompts/text_to_sql_system.txt
```

Phase 3 embeddings and index are unaffected — rollback only removes Phase 4 files.

---

## LESSONS LEARNED

- SQL path accuracy — see `docs/backlog.md` L-004 (Ollama accuracy ~85–90%), L-011 (DISTINCT omission), L-013 (bare aggregation cancellation filter drop).
- Semantic path quality — see L-012 (topic vs polarity conflation in embeddings).
- Router misclassifications — see L-003 (short trigger words like "hot" matching inside "hotels").
- Actual Ollama latency on GPU:
- Any Ollama timeout issues:
- Rule sections that needed tightening (JOIN MAP / COLUMN LOCATION / ENTITY COUNT / OUTPUT RULES):

---


## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before writing any code
- Create every file in the REPO STATE FILE TREE — `ai/__init__.py` is mandatory, Python won't find the module without it
- Run using `python -m ai.main "question"` — not `python ai/main.py`
- `ai_main.py` source file must be renamed to `main.py` when placing in `ai/`
- Do NOT remove `_validate_sql` SELECT-only guard — it is the security boundary
- Do NOT move `SentenceTransformer(...)` inside `run()` — 2s penalty per call
- Do NOT use `hotel_master.city` in any SQL — cities are in `dim_location`
- Do NOT add `"hot"`, `"ac"`, `"bed"`, `"cold"` to `SEMANTIC_TRIGGERS` — they match inside common words like "hotels", "budget"
- `agg_daily_hotel_kpi` has no `occupancy_rate` column — use `total_bookings` as proxy
- Run all 6 acceptance tests before declaring done — including latency check
- Do not modify Phase 1, 2, or 3 files

---

## BUILD HISTORY / EVOLUTION

Changes to the Phase 4 AI layer after the original acceptance sign-off. Earliest first.

---

### B-003 — Column-name validation

Added `_load_schema()` and `_validate_columns()` to `text_to_sql.py`. Both are built from `information_schema.columns` at import time. The guard rejects Ollama-generated SQL that references columns not present on the named table, returning a structured validation error instead of a Postgres exception.

**Why:** Ollama occasionally hallucinated column names (`hotel_master.city` instead of `dim_location.city`). The guard surfaces the exact bad column rather than a cryptic SQL error.

**Regression suite:** `pytest tests/test_validate_columns.py -v` — see Hardening tests in ACCEPTANCE TESTS above.

---

### B-004 — Hybrid queries (filter detection + hotel_id scoping)

Added `detect_filters()` to `text_to_sql.py` and hotel-scoping logic to `ai/main.py`. Queries that combine a SQL aggregate with a city or hotel filter now correctly scope the WHERE clause rather than returning unfiltered results.

**Why:** Mixed queries ("top cities in Goa") produced unscoped SQL — the aggregate ran over all cities. `detect_filters()` is a thin pre-pass before SQL generation, not a replacement for it.

**Regression suite:** `pytest tests/test_hybrid_queries.py -v` — covers fast detection-layer unit tests and slower stack-hitting tests marked `@pytest.mark.slow`. See Hardening tests in ACCEPTANCE TESTS above.

---

### B-048 — Explore tab live-data awareness + cancellation-rate stacking fix

Two prompt-only edits to `ai/prompts/text_to_sql_system.txt` (no code change):

1. **`fact_booking_events` added to the SCHEMA section** and a new "HISTORICAL vs LIVE — pick the right fact table" section inserted between JOIN MAP and COLUMN LOCATION. Two-rule routing order: explicit live keywords (`today / now / current / live / streaming / so far today / right now / last <N> hour(s) / last <N> minute(s) / this hour / since midnight`) → `fact_booking_events` with `source='stream'`; everything else (including `this month / this year / in 2025 / monthly / by year`) → `fact_bookings`. `recent` left ambiguous (defaults to historical).
2. **Cancellation-rate stacking fixed.** Default `WHERE NOT b.is_cancelled` rule rewritten to NOT apply when the query computes a rate/ratio/percentage/share. Formula changed from `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` to `COUNT(*) FILTER (WHERE b.is_cancelled)` — same math, cleaner idiom, harder for the LLM to silently re-stack the default exclude on top.

**Why:** Live-stream data on `fact_booking_events` (B-039 silver + B-047 forward producer) was unreachable from the Explore tab — every "today" question went to the frozen 1M-row historical snapshot. Separately, "cancellation rate" always returned 0.00% because the default exclude-cancelled filter and the rate formula were both being emitted, filtering out the very rows the numerator counted.

**Verification (12 queries / 11 PASS / 1 PRE-EXISTING L-004 flake):** see [backlog B-048 acceptance table](backlog.md#b-048--explore-tab-fact_booking_events-live-data-routing--cancellation-rate-stacking-fix--done). Live cases route to `fact_booking_events` with `source='stream'`; historical guards stay on `fact_bookings`; cancellation rates now return real percentages (5-star 12.69 %, overall 11.81 %, Goa 11.83 %) matching DB to the last digit.

---

### B-058 — Text-to-SQL accuracy eval harness (`ai/eval/`)

Added an on-demand accuracy **measurement** tool under `ai/eval/` — a fixture of natural-language
questions (`eval_questions.py`) and a runner (`run_eval.py`, invoked `python -m ai.eval.run_eval`).
It runs each question through `ai.main.answer()` against the live DB + Ollama, grades by **execution
match** against an owner-verified reference query (N runs/question to average out non-determinism),
and flags L-011 (missing `DISTINCT`) + L-013 (missing cancellation filter) independently of the
match. It is a measurement tool, **not** a pytest regression suite — it is deliberately outside
`pytest tests/`.

**Why:** the existing pytest suites (`test_validate_columns.py`, `test_hybrid_queries.py`) guard
specific fixes against regression but do not answer "how accurate is the generated SQL, end-to-end,
right now?" L-004 quotes ~85–90% accuracy as a hand-wave; this harness makes that number
reproducible and attaches it to the two known confident-wrong failure modes.

**Design notes** (full rationale + the hard "fixture is test data, never prompt content" boundary
live in the backlog entry, not restated here): execution match (not SQL text) so the metric survives
a dataset regenerate; flags independent of exec-match so a coincidental row match can't hide a
structurally wrong query; narrow `is_rate_query` so a `/1e7` crore conversion isn't mistaken for a
rate and L-013 stays catchable; cap-aware + probe-aware honest denominator; skip-clean (exit 2) when
infra is down.

**Baseline:** 11 questions × 3 runs → 33.3% headline execution accuracy (11/33 of all runs; 45.8%
among scored runs), `cancellation_filter_missing` ×8, 24.2% error rate (validator_rejection 5 /
model_sql_error 1 / other 2), 0% misroute. Full table + acceptance matrix:
[backlog B-058](backlog.md#b-058--text-to-sql-accuracy-eval-harness-aieval--done).

---

### B-059 — `_validate_columns` false-positive on a SELECT alias reused in `ORDER BY`

The B-058 baseline surfaced a false-POSITIVE in the B-003 column validator (the inverse of B-003a's
false-negative): on a **single-table** query, a `SELECT`-list output alias referenced in
`ORDER BY` / `GROUP BY` / `HAVING` — e.g. `SELECT state, COUNT(*) AS customer_count FROM dim_customer
GROUP BY state ORDER BY customer_count DESC` — was harvested as a bare column ref, checked against the
one in-scope table, not found, and rejected with a `ValueError`. The same alias re-appears on the
B-001 retry, so a query Postgres would have run fine became a **both-attempts error** (the user got
nothing). It only bit single-table + aliased sort/group/having: bare refs are validated only when
exactly one table is in scope, so multi-table queries (`ORDER BY total_revenue_crore` over 3 tables)
were already skipped.

**Fix** (`ai/text_to_sql.py`): new `_collect_select_aliases(stmt)` gathers the top-level
projection's output aliases (sqlparse `Identifier.get_alias()`, lowercased, scoped to the tokens
between `SELECT` and the first `FROM`/`JOIN`); `_validate_columns` skips any bare ref whose name is
in that set. Provably introduces no false-negative — a bare ref matching a `SELECT` alias *is* a
valid alias reference; a genuinely hallucinated `ORDER BY` column that is NOT an alias still raises.
Only the live `run()` path is affected (the frozen-SQL `run_stored_sql` refresh uses `_validate_sql`,
not `_validate_columns`).

**New tests** (`tests/test_validate_columns.py`):
`test_b059_single_table_alias_in_order_by_not_rejected` (the repro must NOT raise) +
`test_b059_single_table_hallucinated_order_by_still_rejected` (a real hallucination in the same
position STILL raises). Suite: 19 passed, B-003a stays `xfail`.

**Verification:** re-ran `python -m ai.eval.run_eval`. The B-059 question `customers_per_state` went
from a baseline `1/3` pass with `validator_rejection×2` (the alias false-positive) to `3/3` pass with
0 rejections; the controlled A/B on that question (5 runs, fix disabled vs enabled) isolates it as
`validator_rejection` 1 → 0. The full-fixture aggregate `validator_rejection` is a *coincidental*
5 → 5 — the composition flipped: the after-run's 5 are all genuine B-003 catches (qualified-ref
hallucinations like `b.booking_date` / `c.segment`, plus a bare `customer_name` that truly doesn't
exist), NOT the B-059 alias pattern; their count tracks model non-determinism. Full before/after in
[backlog B-059](backlog.md#b-059--_validate_columns-false-positive-on-select-alias-reused-in-order-by--done).

---

### B-060 — post-generation cancellation-filter lint + corrective retry (mitigates L-013)

`run()`'s retry (B-001/B-003) fires only on an **exception** — it does nothing for SQL that executes
cleanly but is wrong. The B-058 eval baseline proved the most frequent such case: a bare grouped
aggregate over `fact_bookings` that silently drops `WHERE NOT is_cancelled` (`cancellation_filter_
missing` fired repeatedly) — valid SQL, wrong numbers, no error. That is **L-013**, and the live
Explore path (which runs unverified model SQL on every question) has no safeguard for it — B-022's
pin-time freeze only protects the dashboard read path.

**What it does** (`ai/text_to_sql.py`, no prompt-file edit, no per-query example): a deterministic
post-execution lint `lint_cancellation_filter_missing(sql, user_query)` that **mirrors the eval
harness's `flag_cancellation_filter_missing`** so the two agree on what "missing filter" means. On a
**clean first execute**, if a `fact_bookings` aggregate dropped the cancellation exclusion — not a
rate query (a `/1e7` crore conversion is a unit scale, NOT a rate, so it still catches), no
`is_cancelled` anywhere in the SQL, and the question isn't about cancellations — `run()` drives **one
corrective retry** that names the *general* schema rule alias-agnostically (regenerate excluding
cancelled rows from `fact_bookings`). The retry is adopted only if it is clean AND the lint no longer
fires; otherwise it **falls back to the original successful result — never degrading a working
query**. Outcome is recorded on `result["lint_cancellation_filter"]`. Bounded: at most one
error-retry OR one lint-retry, never stacked; `run_stored_sql` is untouched.

**Why condition 3 is a bare `is_cancelled`-presence check (B-048 guard):** the corrective retry
injects an exclusion predicate. If the SQL already expressed a cancellation measure in a form the
rate check misses — e.g. `SUM(CASE WHEN b.is_cancelled THEN 1 ELSE 0 END)` with no rate alias and no
"cancel" in the question — firing would **stack** a second predicate and zero the count out (the
B-048 bug). The bare-presence check is a strict safety superset: every genuine L-013 case has NO
`is_cancelled` at all, so no real fire is lost.

**Scope:** L-013 / cancellation-filter only — the L-011/`DISTINCT` lint is a deferred fast-follow
(the harness hasn't shown `distinct_missing` firing, and "should this be DISTINCT?" is
false-positive-prone).

**New tests** (`tests/test_lint_cancellation_filter.py` — direct-call, no Ollama, no DB): 3 fire
cases (bare grouped COUNT; the `/1e7` crore trap; single-join AVG), 4 don't-fire cases (rate;
already-has-filter; cancellation-intent; non-`fact_bookings`), and the B-048-guard
`test_no_fire_case_when_is_cancelled_no_rate_alias`. 8/8 pass; full suite 60 passed, 1 xfailed
(B-003a stays `xfail`). Full body + eval re-run:
[backlog B-060](backlog.md#b-060--post-generation-cancellation-filter-lint--corrective-retry-mitigates-l-013--done).

---

## NEXT

Phase 5 — HTML Output

Files to build:
- `render/render_output.py` — detects result shape, picks the right template
- `render/templates/` — Jinja2 templates: table, bar chart, line chart,
  stat card, semantic summary card
- `main.py` (repo root) — `answer()` → `render()` → write `query_results/output.html`
