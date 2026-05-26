# Phase 0 — Environment Setup

> **Stack:** Docker Desktop · Python 3.11 · Ollama · Git · Windows 11  
> **Hardware:** RTX 3070 8GB · 31GB RAM · Windows 11  
> **Status:** [ ] In progress / [x] Complete  

---


## REPO STATE AFTER THIS PHASE

Canonical repo layout: see [`CLAUDE.md`](../CLAUDE.md) (root). This phase
**creates** the repository skeleton — every file/directory below is new:

- `.gitignore`, `.env.example`, `.env` (gitignored), `requirements.txt`, `README.md`, `CLAUDE.md`
- `data/` (`.gitkeep` + 12 `.csv` files + `booking_events_seed.json` copied from the dataset source)
- Empty package/skeleton directories: `db/migrations/`, `docker/`, `scripts/`, `ai/prompts/`, `airflow/dags/`, `render/templates/`, `flink/`, `tests/`

## OBJECTIVE

Stand up a reproducible local development environment for the TravelLens build. By the
end of Phase 0: Docker is running, Python 3.11+ is in a virtual environment, Ollama has
the Qwen2.5-Coder-7B model loaded, the repository skeleton exists, dependencies are
installed, dataset files are in place, and all 8 sanity checks pass.

---

## STRUCTURE

Phase 0 has two distinct parts:

1. **Part A — Manual installs** (you do this) — requires GUI interaction, admin rights,
   or system reboot. ~30–60 minutes depending on what's already installed.

2. **Part B — Project bootstrap** (Claude Code does this) — file generation, package
   installs, sanity checks. ~10–15 minutes once the venv is active.

Complete Part A yourself before handing off to Claude Code for Part B.

---

## PART A — Manual installs (human only)

Do all steps in order. Do not let Claude Code do these — it cannot click GUI dialogs,
run sudo, or trigger a reboot.

### A.1 — Install Docker Desktop

**Windows / macOS:** download from https://www.docker.com/products/docker-desktop, run
the installer, accept defaults, restart machine, open Docker Desktop and wait for the
whale icon to turn steady.

**Linux (Ubuntu/Debian):**
```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER
# Log out and log back in for the group change to apply
```

Verify before continuing:
```bash
docker --version          # → Docker version 24.x or newer
docker compose version    # → Docker Compose version v2.x or newer
docker run hello-world    # → "Hello from Docker!" message
```

**Resource allocation:** Docker Desktop → Settings → Resources → set Memory to **8 GB
minimum** (12 GB recommended), CPUs ≥ 4. Apply & Restart.

---

### A.2 — Install Python 3.11+

**Windows:** download from https://www.python.org/downloads/release/python-3119/, run
the installer, check "Add Python 3.11 to PATH".

**macOS:** `brew install python@3.11`

**Linux (Ubuntu):**
```bash
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev
```

Verify:
```bash
python3.11 --version      # → Python 3.11.x
```

---

### A.3 — Install Ollama and pull the model

**Windows:** download installer from https://ollama.com/download/windows, run it. Ollama
runs as a background service.

**macOS:** download from https://ollama.com/download/mac. Or `brew install ollama`.

**Linux:** `curl -fsSL https://ollama.com/install.sh | sh`

Verify Ollama is responding:
```bash
ollama --version          # → ollama version 0.3.x or newer
curl http://localhost:11434  # → "Ollama is running"
```

Pull the model (~5 GB, takes 5–20 minutes):
```bash
ollama pull qwen2.5-coder:7b
ollama list   # confirm qwen2.5-coder:7b is listed
```

**GPU verification (recommended):** run `nvidia-smi` while a model query is active —
should show Ollama using GPU memory. If not, update Nvidia drivers (CUDA 12.x required).
CPU-only works but Phase 4 queries will take 8–15 seconds each instead of <1 second.

---

### A.4 — Install Git

```bash
git --version
# If missing:
# Windows: https://git-scm.com/download/win
# macOS:   xcode-select --install
# Linux:   sudo apt install git
```

Configure identity:
```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

---

### A.5 — Create the project folder and venv

```bash
cd C:\Users\risha\Desktop\Code\repo     # your preferred location
mkdir travellens && cd travellens

python3.11 -m venv .venv

# Activate — you MUST do this before running anything
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# Windows CMD:
.venv\Scripts\activate.bat
# Linux / macOS:
source .venv/bin/activate

# Your prompt should now show (.venv)
python --version          # → Python 3.11.x
```

Keep this terminal open. If you close it, re-activate before running anything.

---

### A.6 — Locate your dataset files

You need 13 files (12 CSV + 1 JSON) from the Phase 0 dataset generation, ~250 MB total.

```bash
ls /path/to/travellens_data/
# Expected:
#   ref_price_tiers.csv, ref_state_centroids.csv, india_states_zones.csv,
#   public_holidays.csv, dim_date.csv, dim_location.csv, dim_customer.csv,
#   hotel_master.csv, dim_room_type.csv, fact_bookings.csv,
#   fact_price_events.csv, reviews_raw.csv, booking_events_seed.json
```

---

### A.7 — End of Part A checklist

Before invoking Claude Code, all of these must be true:

- [ ] `docker --version` works, `docker run hello-world` succeeds, Docker Desktop has ≥ 8 GB memory
- [ ] `python --version` shows 3.11.x AND your prompt shows `(.venv)`
- [ ] `ollama list` shows `qwen2.5-coder:7b`
- [ ] `git --version` works and your identity is configured
- [ ] You are in your `travellens/` project folder
- [ ] You know the absolute path to your dataset files

If all are true, proceed to Part B. Otherwise finish Part A first.

---

## PART B — Project bootstrap (Claude Code)

Everything below is for Claude Code. It assumes Part A is complete.

### PREREQUISITES (Claude Code must verify before doing anything)

```bash
# Docker is running
docker info > /dev/null 2>&1 || echo "FAIL: Docker daemon not reachable"

# Python venv is active and is 3.11+
python -c "import sys; assert sys.version_info >= (3, 11), f'Need 3.11+, got {sys.version}'"
python -c "import sys; assert '.venv' in sys.prefix or 'venv' in sys.prefix, 'venv not active'"

# Ollama is responding and has the model
curl -fs http://localhost:11434/api/tags | grep -q "qwen2.5-coder" || echo "FAIL: Ollama not responding or model not pulled"

# Git is configured
git config user.name > /dev/null && git config user.email > /dev/null || echo "FAIL: git identity not set"
```

Ask the user: "What is the absolute path to your dataset files?" — store as `$DATA_SRC`.

---

### DELIVERABLES

| Deliverable | Location | Done when |
|---|---|---|
| `.gitignore` | repo root | Python, venv, env, data excluded |
| `requirements.txt` | repo root | All packages pinned |
| `.env.example` | repo root | All variable names present |
| `.env` | repo root | Generated password, gitignored |
| `README.md` | repo root | Project overview with phase checklist |
| `data/` | repo root | 13 dataset files copied in |
| Project skeleton | repo root | All dirs + `.gitkeep` files |

---

### STEPS

#### Step 1 — Create `.gitignore`

```gitignore
# Python
.venv/
__pycache__/
*.pyc
*.pyo
.pytest_cache/
*.egg-info/

# Environment
.env
.env.local

# Data — do not commit large CSVs to git
data/*.csv
data/*.json
!data/.gitkeep

# Docker
.docker/

# IDE
.vscode/
.idea/
*.swp
*.swo

# Build artifacts
*.parquet
*.duckdb
query_results/
logs/
*.log
```

---

#### Step 2 — Create `requirements.txt`

Pin every dependency. Floating versions will eventually break the build.

```
# Database
psycopg2-binary==2.9.9
pgvector==0.3.6
sqlalchemy==2.0.30
sqlparse==0.5.0

# Data processing
pandas==2.2.2
pyarrow==17.0.0
numpy==1.26.4

# Streaming
apache-flink==1.18.1
kafka-python==2.0.2

# AI / ML
sentence-transformers==3.0.1
torch==2.3.0
requests==2.32.3

# Cloud + storage
boto3==1.34.130
python-dotenv==1.0.1

# Rendering
jinja2==3.1.4

# Orchestration
apache-airflow==2.9.2

# Testing
pytest==8.2.2
pytest-cov==5.0.0

# Dev utilities
ipython==8.25.0
tqdm==4.66.4
```

---

#### Step 3 — Create `.env.example`

```
# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=travellens
POSTGRES_USER=travellens
POSTGRES_PASSWORD=changeme_in_real_setup

# Kafka (Phase 2)
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=booking-events

# S3 / MinIO (Phase 2)
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=travellens-data
S3_PREFIX_RAW=raw/
S3_PREFIX_REVIEWS=reviews/
S3_PREFIX_REFERENCE=reference/
S3_PREFIX_PROCESSED=processed/

# Ollama (Phase 4)
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=qwen2.5-coder:7b

# Application
DATA_DIR=./data
RESULTS_DIR=./query_results
```

---

#### Step 4 — Create `.env` with a generated password

```bash
cp .env.example .env

# Windows — use Python to replace the placeholder:
python -c "
import secrets
pw = secrets.token_urlsafe(18)
with open('.env','r') as f: s = f.read()
with open('.env','w') as f: f.write(s.replace('changeme_in_real_setup', pw))
print('Generated POSTGRES_PASSWORD:', pw)
"
```

Tell the user the generated password. It lives in `.env` (gitignored — never committed).

---

#### Step 5 — Create `README.md`

```markdown
# TravelLens India

Real-time hotel intelligence platform for the Indian hospitality market.

**Stack:** Postgres 16 + pgvector · Apache Kafka · Python stream consumer ·
sentence-transformers · Claude API · MinIO · Airflow · Jinja2 + Chart.js

**Build status:**
- [x] Phase 0 — Environment setup
- [ ] Phase 1 — Postgres foundation
- [ ] Phase 2 — Streaming pipeline
- [ ] Phase 3 — Embeddings + pgvector
- [ ] Phase 4 — AI layer (Text-to-SQL + semantic search)
- [ ] Phase 5 — HTML output

## Quick test

After Phase 1:
\`\`\`bash
python scripts/validate_load.py
# → PASSED: 20 / 20
\`\`\`
```

---

#### Step 6 — Create the project skeleton

```bash
mkdir -p data db/migrations docker scripts flink ai/prompts \
         airflow/dags render/templates tests docs

touch data/.gitkeep db/.gitkeep db/migrations/.gitkeep docker/.gitkeep \
      scripts/.gitkeep flink/.gitkeep ai/.gitkeep ai/prompts/.gitkeep \
      airflow/.gitkeep airflow/dags/.gitkeep render/.gitkeep \
      render/templates/.gitkeep tests/.gitkeep
```

Do not create `docs/.gitkeep` — that folder should already contain the phase spec `.md`
files.

---

#### Step 7 — Install Python dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Takes 5–15 minutes. PyTorch and PyFlink are the slow downloads.

**GPU PyTorch (Nvidia):** ask the user "Do you have an Nvidia GPU? (y/n)". If yes:
```bash
pip install torch==2.3.0 --index-url https://download.pytorch.org/whl/cu121
```

This swaps the CPU-only build for the CUDA 12.1 build. On Apple Silicon do nothing —
Metal works automatically. Skip this and Phase 3 will silently fall back to CPU (~25 min
instead of ~17 seconds).

---

#### Step 8 — Copy dataset files

```bash
cp "$DATA_SRC"/*.csv data/
cp "$DATA_SRC"/*.json data/

# Verify
ls data/*.csv | wc -l    # → 12
ls data/*.json | wc -l   # → 1
```

If any expected file is missing, abort and report which one.

---

#### Step 9 — Initialize git

STOP — the owner commits manually. Do not run `git init`, `git add`, or
`git commit` from this agent session. The owner initializes the repository,
reviews the skeleton, and creates the first commit themselves. Confirm that
the `.env` file is gitignored (it MUST NOT be tracked) and hand off.

---

## ACCEPTANCE TESTS

Run all 8 in sequence. Every command must succeed.

```bash
# 1. All key packages importable
python -c "import psycopg2, pgvector, pandas, sentence_transformers, jinja2, boto3, sqlparse; print('All imports OK')"

# 2. Docker daemon reachable
docker info > /dev/null 2>&1

# 3. Ollama serving the model
curl -fs http://localhost:11434/api/tags | grep -q "qwen2.5-coder"

# 4. Project skeleton complete
test $(find . -maxdepth 2 -type d -not -path './.venv*' -not -path './.git*' | wc -l) -ge 12

# 5. .env exists and is NOT tracked by git
test -f .env && ! git ls-files --error-unmatch .env 2>/dev/null

# 6. Dataset files in place
test $(ls data/*.csv 2>/dev/null | wc -l) -eq 12
test $(ls data/*.json 2>/dev/null | wc -l) -eq 1

# 7. Key packages at pinned versions
python -c "
import importlib.metadata
expected = {
    'psycopg2-binary': '2.9.9',
    'sentence-transformers': '3.0.1',
    'pgvector': '0.3.6',
}
for pkg, want in expected.items():
    got = importlib.metadata.version(pkg)
    assert got == want, f'{pkg}: want {want}, got {got}'
print('All key versions pinned correctly')
"

# 8. Ollama functional test
curl -s http://localhost:11434/api/generate \
  -d '{"model":"qwen2.5-coder:7b","prompt":"SELECT 1;","stream":false}' \
  | python -c "import sys,json; d=json.load(sys.stdin); assert d.get('response','').strip(), 'Empty response'; print('Ollama functional')"
```

After all 8 succeed, output `PHASE 0 ACCEPTED` and stop. Do not auto-proceed to Phase 1.

---

## DO NOT

- Do not try to install Docker, Python, Ollama, or Git yourself — these are Part A (human-only)
- Do not commit `.env` to git — it must stay gitignored
- Do not commit any file in `data/` — only `.gitkeep` should appear in the commit
- Do not float any package version in `requirements.txt` — pin every version
- Do not run `pip install` outside the venv — if `which python` doesn't point inside `.venv/`, abort
- Do not skip the GPU PyTorch question — wrong build silently falls back to CPU in Phase 3
- Do not create `docs/.gitkeep` — the user already placed phase spec `.md` files there

---

## ROLLBACK

```bash
# Inside the project folder:
rm -rf .git data db docker scripts flink ai airflow render tests
rm -f .gitignore requirements.txt .env .env.example README.md

# Leave .venv/ alone — slow to recreate
# Leave docs/ alone — those are your spec files
```

---


## CLAUDE CODE INSTRUCTIONS
> Customise before running — adjust paths, usernames, and any rules specific to your environment or workflow preferences.

- Read this entire file before doing anything — Part A must be complete before Part B starts
- Verify all Part A prerequisites before writing a single file — if any check fails, stop and tell the user
- Ask the user for `$DATA_SRC` (absolute path to dataset files) before Step 8
- Create every file in the REPO STATE FILE TREE — no extras
- Do not commit `.env` — verify with `git status` before committing
- Do not create `docs/.gitkeep` — docs/ already contains phase MD files
- GPU PyTorch question is mandatory — ask before pip install
- Run all 8 acceptance tests in sequence — report `PHASE 0 ACCEPTED` only after all pass
- Do not auto-proceed to Phase 1


## NEXT

Phase 1 — `docs/phase-1-postgres.md`
