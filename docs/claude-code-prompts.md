# Claude Code — Prompts to Run Each Phase

> Copy the prompt block for the phase you want to run. Paste into Claude Code. Wait for `PHASE N ACCEPTED`. Then move on.

---

## How this works

Each phase has one `.md` spec file in `docs/`. Each spec is self-contained — it lists
prerequisites, deliverables, steps, acceptance tests, and Claude Code instructions.
Claude Code reads the spec, executes the steps, runs the acceptance tests, and reports back.

You do **not** need to chain prompts or copy work between phases. Each phase ends with
the system in a defined state, and the next phase's spec verifies that state in its
`PREREQUISITES` section before doing anything new.

> **Phases are not strictly sequential.** Phase 7 (`/monitor`) shipped ahead of
> Phase 6 (Airflow, still in progress) because the monitor unblocks day-to-day
> visibility into the stream pipeline without needing the batch DAGs first.
> A `PREREQUISITES` chain may therefore reference a later-numbered phase
> (e.g. Phase 7's monitor presumes `pipeline_metrics` from migration 007 that
> shipped during Phase 7 itself, not as part of any earlier-numbered phase).
> Trust each spec's own `PREREQUISITES` block over the phase-number ordering.

---

## Before you start any phase

Make sure these are true once, before Phase 1:

- You are in the project root: `C:\Users\risha\Desktop\Code\repo\travellens`
- Your `.venv` is active — prompt shows `(.venv)`
- `CLAUDE.md` exists at repo root — Claude Code reads this automatically
- The phase `.md` files are in `docs/`:
  - `docs/phase-0-setup.md`
  - `docs/phase-1-postgres.md`
  - `docs/phase-2-streaming.md`
  - `docs/phase-3-embeddings.md`
  - `docs/phase-4-ai-layer.md`
  - `docs/phase-5-dashboard.md`
  - `docs/phase-6-airflow.md`
  - `docs/phase-7-monitor.md`
  - `docs/backlog.md`

---

## Phase 0 prompt (Day 0 setup)

> **Read this first.** Phase 0 has a manual section (Part A) that you must complete
> yourself before Claude Code can do its part. Part A includes installing Docker, Python,
> Ollama, and Git — these require GUI installers or a reboot. See the `PART A` section
> of `docs/phase-0-setup.md`. Once Part A is done and your venv is active, run the
> prompt below.

```
Read docs/phase-0-setup.md end to end. Pay special attention to the STRUCTURE section —
Part A is for the human and must already be complete before you do anything.

Then:
1. Run all PREREQUISITES checks from Part B. If any fails, stop and tell me which
   Part A step needs to be redone. Do not improvise a fix.
2. Ask me for the absolute path to my dataset files (13 CSV + 1 JSON from Phase 0
   dataset generation). Store it as DATA_SRC.
3. Execute the STEPS section in order.
4. When you reach Step 7 (pip install), ask me: "Do you have an Nvidia GPU? (y/n)".
   If yes, run the CUDA PyTorch upgrade command before continuing.
5. Run all 8 ACCEPTANCE TESTS. Report ✓/✗ per test with actual output.
6. If all 8 pass, output "PHASE 0 ACCEPTED" on its own line and stop.
7. Do not proceed to Phase 1.

Do not try to install Docker, Python, Ollama, or Git — those are Part A (human-only).
Do not commit .env to git.
```

---

## Phase 1 prompt

```
Read docs/phase-1-postgres.md end to end, including the REPO STATE and
CLAUDE CODE INSTRUCTIONS sections.

Then:
1. Verify every prerequisite. If any fails, stop and tell me which one. Do not proceed.
2. Execute the STEPS section in order. After each step, run the verify command listed
   and confirm it passed before moving to the next step.
3. Run every command in the ACCEPTANCE TESTS section.
4. Report ✓/✗ per test with the actual output value (not just pass/fail).
5. If all 7 acceptance tests pass, output "PHASE 1 ACCEPTED" on its own line and stop.
6. If any test fails, report which one and stop. Do not attempt to fix it without my
   confirmation.

Do not proceed to Phase 2 even if Phase 1 succeeds — wait for my instruction.
Do not modify any file in data/.
Do not create indexes inside schema.sql — they go in load_to_postgres.py post-load.
```

---

## Phase 2 prompt

```
Phase 1 has been accepted. Read docs/phase-2-streaming.md end to end, including
the REPO STATE and CLAUDE CODE INSTRUCTIONS sections.

Then:
1. Verify every prerequisite. Re-run `python scripts/validate_load.py` — it must
   still pass 20/20 before you touch anything.
2. Execute the STEPS section in order.
3. STOP before Step 6 (running the pipeline). Print the exact terminal commands for:
   - Terminal A: the consumer command
   - Terminal B: the producer command
   Then wait for me to confirm I have started both and seen at least one log line pair:
   "✓ Postgres upsert" + "✓ S3 archive". Only then continue.
4. Run the ACCEPTANCE TESTS section including the chaos validation.
5. Report ✓/✗ per test. Output "PHASE 2 ACCEPTED" if all pass.
6. Do not proceed to Phase 3.

Do not modify schema.sql, load_to_postgres.py, or validate_load.py.
Do not run any git commands — we commit after acceptance.
MODIFY docker-compose.yml — do not replace it entirely, add the 3 new services.
```

---

## Phase 3 prompt

```
Phase 2 has been accepted. Read docs/phase-3-embeddings.md end to end, including
the REPO STATE and CLAUDE CODE INSTRUCTIONS sections.

Then:
1. Verify every prerequisite. Check CUDA availability:
   python -c "import torch; print('CUDA:', torch.cuda.is_available())"
   If False, warn me about the ~25 minute CPU runtime before proceeding.
2. Execute the STEPS section in order.
3. Step 3 (generate_embeddings.py) is long-running — show progress and do not time out.
   Expected runtime: ~17 seconds on GPU, ~25 minutes on CPU.
4. Run all 4 ACCEPTANCE TESTS in psql.
5. Run the semantic playground manually:
   python scripts/semantic_playground.py
   Type "AC not working" and "dirty bathroom" — show me the top 5 results for each.
   Tell me whether the returned reviews are semantically relevant.
6. Report ✓/✗ per acceptance test. Output "PHASE 3 ACCEPTED" if all 4 pass.
7. Do not proceed to Phase 4.

Do not build the IVFFlat index before all embeddings are written.
Do not use lists=100 — the correct value is lists=30 for 30K rows.
```

---

## Phase 4 prompt

```
Phase 3 has been accepted. Read docs/phase-4-ai-layer.md end to end, including
the REPO STATE and CLAUDE CODE INSTRUCTIONS sections.

Then:
1. Verify every prerequisite. Confirm Ollama is responding:
   curl http://localhost:11434
   ollama list   # must show qwen2.5-coder:7b
2. Execute the STEPS section in order.
3. IMPORTANT: create ai/__init__.py (empty file) — Python cannot find the ai module
   without it. This is a common failure point.
4. Run using `python -m ai.main "question"` — not `python ai/main.py`.
5. Run the router self-test (Step 7). All 10 cases must show ✓ before continuing.
6. Run the SQL path tests (Step 8) and semantic path tests (Step 9).
7. Run all 6 ACCEPTANCE TESTS.
8. Report ✓/✗ per test. Output "PHASE 4 ACCEPTED" if all pass.
9. Do not proceed to Phase 5.

The acceptance threshold for SQL queries is 8 of 10 passing — LLMs have inherent
non-determinism. If 8 pass, that is a success. Do not edit the system prompt to force
a specific query to pass — that is overfitting.

If a query fails: show me the question, the SQL generated, and the error. Do not fix
without my confirmation.

Do not add "hot", "ac", "bed", or "cold" to SEMANTIC_TRIGGERS — they match inside
common words like "hotels" and "budget".
Do not use hotel_master.city in any SQL — cities are in dim_location.
```

---

## Phase 5 prompt

```
Phase 4 has been accepted. Read docs/phase-5-dashboard.md end to end, including
the REPO STATE and CLAUDE CODE INSTRUCTIONS sections.

Then:
1. Verify every prerequisite. Confirm:
   - python -m ai.main "top 5 cities by revenue" returns rows with no error
   - Flask is installed: python -c "import flask; print(flask.__version__)"
   - Docker stack is running: docker ps
2. Run the database migration (Step 1) before writing any code.
3. Execute the STEPS section in order.
4. IMPORTANT: create render/__init__.py (empty file) — Python cannot find the
   render module without it.
5. Templates are provided as separate files — copy them into render/templates/:
   - base.html, dashboard.html, explore.html, about.html
   Replace "yourusername" in about.html with the actual GitHub username.
6. Start the server: python -m render.server
   Confirm all 3 pages load: /dashboard, /explore, /about
7. Run the end-to-end test (Step 9) manually — walk through each of the 10 steps
   and confirm each works.
8. Run all 6 ACCEPTANCE TESTS using curl.
9. Report ✓/✗ per test. Output "PHASE 5 ACCEPTED" if all pass.
10. Do not proceed to Phase 6.

Do not query Postgres directly from server.py for data — only for dashboard_widgets
table CRUD. All data comes through ai.main.answer().
Do not render widgets server-side on page load — widgets load via /api/refresh.
Run the server as python -m render.server not python render/server.py.
```

---

## Re-running a phase

Each spec has a `ROLLBACK` section. Run the rollback commands, then start the phase
prompt fresh.

Example — redo Phase 3:
```
Run the ROLLBACK section of docs/phase-3-embeddings.md exactly as written.
Then start fresh from the Phase 3 prompt.
```

---

## Tips for working with Claude Code on this project

**CLAUDE.md is always active.** The repo root `CLAUDE.md` is read automatically at the
start of every Claude Code session. It contains the hard rules, repo layout, and common
mistakes. You do not need to repeat these in your prompt.

**Show your work.** Tell Claude Code to print intermediate output. The acceptance tests
are unambiguous, but seeing row counts and validator output helps you catch issues early.

**Don't auto-proceed across phases.** Each phase has acceptance criteria for a reason.
Resist chaining `Phase 1, then Phase 2, then Phase 3` in one prompt — you will miss
failures.

**Long-running steps need explicit waits.** Phase 3 embedding generation takes ~25
minutes on CPU. Make sure your Claude Code session doesn't time out. The spec's
"expected runtime" notes tell you what's normal.

**Phase 2 requires two terminals.** The consumer and producer must run simultaneously.
Claude Code cannot open a second terminal — it prints the commands, you run them.

**LLM non-determinism is real.** Phase 4 produces slightly different SQL on each run.
The 8/10 acceptance threshold absorbs this. If a different 8 pass on re-run, that is
still a pass.

**Idempotency is built in.** Every script is safe to re-run. Loaders truncate before
insert. Index creation uses `IF NOT EXISTS`. Embedding generation skips rows where
`embedding IS NOT NULL`. Kafka consumer uses `ON CONFLICT` upsert.

**STOP — the owner commits manually.** After each `PHASE N ACCEPTED`, do not run
any git commands. The owner reviews the diff and commits themselves. If a later
phase breaks something, the owner can run `git reset --hard` to the last
known-good commit; do not invoke `git reset` from an agent session.

**Fix the code, not the test.** If an acceptance test fails, fix the code until the
test passes. Never modify the acceptance test to make it pass — that defeats the point.

**Refer to backlog.md for known issues.** If something behaves unexpectedly, check
`docs/backlog.md` — it lists known limitations with resolution paths.

---

## File map

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). Don't duplicate
the tree here — keeping two copies in sync was the drift source. Per-phase
deltas (what each phase creates / touches) live in each phase doc's
"REPO STATE AFTER THIS PHASE" block.

---

## Phase status

Canonical phase status: see the **Phase status** table in [`CLAUDE.md`](../CLAUDE.md). Current phase: **6** (Airflow DAGs; infra up, 5 DAGs not built).
