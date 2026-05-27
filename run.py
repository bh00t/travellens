#!/usr/bin/env python3
"""
run.py — TravelLens dev launcher
================================
One command to bring the whole local stack up, and a clean teardown on Ctrl-C.

WHAT PLAIN `python run.py` DOES
    Brings the docker stack up (Postgres, Kafka, Zookeeper, MinIO, Airflow), then
    starts THREE host Python processes: the Kafka consumer, the event simulator,
    and the Flask dashboard at http://localhost:5000. Ctrl-C tears it all down
    cleanly. If docker was already up before you ran this, it is LEFT up.

FLAGS — pick the smallest mode that lets you do what you need
  --window MINUTES   Override the consumer's tumbling-window size for THIS run
                     only (e.g. --window 2 flushes aggregates in ~2 min, useful
                     for fast monitor testing). Scoped to the consumer subprocess;
                     never touches .env or your shell. Omit for the 60-min
                     production default.
                       python run.py --window 2

  --no-sim           Bring up docker + consumer + dashboard, but DON'T start the
                     event simulator. The monitor is still live (the consumer is
                     up), so you can send your own events from another terminal.
                       python run.py --no-sim
                       # then, in another shell:
                       python -m scripts.kafka_event_producer --rate 50 --duration 60

  --server-only      Start ONLY the Flask dashboard. No consumer, no simulator,
                     and no docker bring-up — assumes the stack (and Postgres)
                     is already up. For fast UI iteration when you don't care
                     about live streaming. If Postgres isn't reachable you get
                     ONE warning line, not a traceback, and the server still
                     starts (data calls just fail until the DB comes back).
                       python run.py --server-only

  --no-docker        Assume docker is already up; start only the python procs
                     (consumer + simulator + dashboard).
  --sim-rate N       Pass --rate N to the simulator (events per second).
  --chaos            Simulator injects malformed + late events at the bundled
                     5% / 2% / seed=42 testing defaults. Populates the monitor's
                     quarantine cards.
  --malformed-pct N  Custom malformed % (implies chaos; overrides --chaos default).
  --late-pct N       Custom late % (implies chaos; overrides --chaos default).
  --chaos-seed N     Reproducible chaos seed (default 42 when chaos is on).
  --down             Tear the docker stack down and exit.

PRECEDENCE
    --server-only is the most stripped mode and wins over --no-sim if BOTH are
    passed (server-only already implies no simulator AND no consumer).
    --window only matters when the consumer actually starts; under --server-only
    there is no consumer to receive it, so it becomes a no-op — the launcher
    will say so in the banner when both are given.

WHAT THIS SCRIPT MANAGES
    - Docker stack via `docker compose up -d`. Airflow's scheduler runs its DAGs
      on its own inside its container — this script does NOT schedule or trigger
      Airflow jobs. It only brings the containers up.
    - Six host Python processes that are NOT containerised:
        consumer    — drains the Kafka topic
        simulator   — produces booking events
        dashboard   — the Flask render server
        gold        — gold_lifecycle_updater micro-batch (pg_try_advisory_lock 7400040)
        embedder    — review_embedder micro-batch (pg_try_advisory_lock 7400050)
        quarantine  — quarantine_hourly_rollup 5-min loop (pg_try_advisory_lock 7400060)
      Advisory locks mean a stray manual copy exits immediately rather than racing
      — safe to have run.py always start all three maintenance procs.

DESIGN DECISIONS (so future-you knows why)
    - Idempotent: `docker compose up -d` is safe to run whether containers are up
      or down. The script also pre-checks the Docker daemon and polls health
      before starting Python.
    - Least-surprise teardown: on exit it only runs `docker compose down` IF THIS
      SCRIPT started Docker. If Docker was already up before you ran this, it's
      left running.
    - The teardown always fires (signal handler + finally), even on crash or
      double Ctrl-C, so you never end up with orphaned host processes.
    - Takeover startup: run.py always wins — on startup it kills any live prior
      run.py supervisor + all travellens children, waits for Postgres advisory
      locks to release, then starts a fresh stack. If :5000 is held by a
      non-travellens process, it stops cleanly instead of blind-killing an
      unrelated program.

------------------------------------------------------------------------------------
CONFIG — EDIT THESE THREE COMMANDS TO MATCH YOUR REPO IF THEY ARE WRONG.
They are best-guesses from the project layout. Each is a list (argv style).
------------------------------------------------------------------------------------
"""

import argparse
import atexit
import os
import shutil
import signal
import socket
import subprocess
import sys

# Subprocess output may contain non-ASCII or Unicode replacement characters
# (�) that cp1252 (Windows default stdout encoding) cannot encode.
# Reconfigure stdout to UTF-8 with replacement so stream_output never crashes.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import threading
import time

# Port bound as a process mutex — a second run.py trying to bind this address
# will get OSError and exit, so two instances can never race.  Nothing actually
# connects to this socket; it's only held open as long as the process lives.
_RUN_LOCK_PORT = 47_219


def _acquire_run_lock():
    """
    Bind to a local-only port as a process mutex.

    Returns the bound socket (caller must keep a reference for the lifetime
    of the process — the OS releases it on process exit or explicit close).
    Returns None if another run.py already holds the lock.

    Why a socket instead of a PID file: no cleanup step needed, no stale-file
    edge cases, and it works identically on Windows and Linux.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
    try:
        s.bind(("127.0.0.1", _RUN_LOCK_PORT))
        s.listen(1)  # puts port in LISTENING state so netstat can see it
        return s
    except OSError:
        s.close()
        return None



def _kill_proc_tree(name, proc):
    """Kill a process and ALL its descendants so no orphan ever holds a port or lock.

    Windows: taskkill /F /T kills the entire process tree rooted at proc.pid.
    POSIX:   falls back to proc.kill() (sufficient — our children don't spawn
             further sub-processes, so there is no tree to recurse into).
    """
    if proc.poll() is not None:
        return  # already dead
    log("system", f"killing {name} (pid {proc.pid})")
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
            )
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    else:
        try:
            proc.kill()
        except Exception:
            pass
    try:
        proc.wait(timeout=5)
    except Exception:
        pass

# --- Repo root (this file should live at the repo root) --------------------------
ROOT = os.path.dirname(os.path.abspath(__file__))

# --- Docker compose invocation (matches the Phase 6 setup) -----------------------
COMPOSE_FILE = os.path.join(ROOT, "docker", "docker-compose.yml")
ENV_FILE = os.path.join(ROOT, ".env")
COMPOSE_BASE = ["docker", "compose", "-f", COMPOSE_FILE, "--env-file", ENV_FILE]

# --- The three host Python processes --------------------------------------------
# Use the SAME interpreter that's running this script (your .venv python) so the
# child processes inherit the virtualenv. Run as modules (-m) from ROOT.
PY = sys.executable

CONSUMER = {
    "name": "consumer",
    "cmd": [PY, "-m", "scripts.stream_consumer"],
}

# The simulator takes an optional rate via --sim-rate (wired through below).
# If your producer script uses a different flag name, change "--rate" here.
SIMULATOR = {
    "name": "simulator",
    "cmd": [PY, "-m", "scripts.kafka_event_producer"],
    "rate_flag": "--rate",   # <-- the flag YOUR producer expects for events/sec, if any
}

# The dashboard / Flask server. Set DASHBOARD_PORT to whatever it binds.
DASHBOARD_PORT = 5000
DASHBOARD = {
    "name": "dashboard",
    "cmd": [PY, "-m", "render.server"],
    "port": DASHBOARD_PORT,
}

# Gold lifecycle updater — continuous micro-batch, pg_try_advisory_lock(7400040).
# Advisory lock means a stray manual copy exits immediately rather than racing.
GOLD_UPDATER = {
    "name": "gold",
    "cmd":  [PY, "-m", "scripts.gold_lifecycle_updater"],
}

# Review embedder — continuous micro-batch, pg_try_advisory_lock(7400050).
# Same idempotency guarantee: a second instance exits on the lock, never races.
EMBEDDER = {
    "name": "embedder",
    "cmd":  [PY, "-m", "scripts.review_embedder"],
}

# Quarantine hourly rollup — 5-min loop, pg_try_advisory_lock(7400060).
# Rolls MinIO quarantine hour-prefixes into quarantine_hourly_summary so
# /monitor reads Postgres (fast) instead of listing S3 on every request.
QUARANTINE_ROLLUP = {
    "name": "quarantine",
    "cmd":  [PY, "-m", "scripts.quarantine_hourly_rollup"],
}

# Services whose health we wait on before starting the Python processes.
# These are the docker compose SERVICE names — adjust to match your compose file.
HEALTH_WAIT_SERVICES = ["postgres", "kafka"]
HEALTH_TIMEOUT_SEC = 90

# Postgres TCP port — probed under --server-only so a clearly-down DB shows up
# as ONE warning line instead of a wall of dashboard tracebacks.
POSTGRES_PORT = 5432

# ----------------------------------------------------------------------------------
# Below here is generic machinery — you shouldn't need to edit it.
# ----------------------------------------------------------------------------------

# ANSI colours for tagged logs (per process, so you can tell streams apart)
COLORS = {
    "consumer":  "\033[36m",   # cyan
    "simulator": "\033[33m",   # yellow
    "dashboard": "\033[35m",   # magenta
    "gold":       "\033[34m",   # blue
    "embedder":   "\033[94m",   # bright blue
    "quarantine": "\033[93m",   # bright yellow
    "system":     "\033[32m",   # green
    "error":     "\033[31m",   # red
}
RESET = "\033[0m"


def log(tag, msg, level="system"):
    color = COLORS.get(tag if tag in COLORS else level, "")
    print(f"{color}[{tag}]{RESET} {msg}", flush=True)


def docker_daemon_up():
    """True if the Docker daemon is reachable."""
    if shutil.which("docker") is None:
        return False
    try:
        r = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15,
        )
        return r.returncode == 0
    except Exception:
        return False


def compose_running_count():
    """How many compose services are currently running (rough 'is anything up')."""
    try:
        r = subprocess.run(
            COMPOSE_BASE + ["ps", "-q"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=20,
        )
        return len([l for l in r.stdout.splitlines() if l.strip()])
    except Exception:
        return 0


def service_healthy(service):
    """True if a compose service reports healthy (or running, if no healthcheck)."""
    try:
        r = subprocess.run(
            COMPOSE_BASE + ["ps", service, "--format", "{{.Health}} {{.State}}"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=15,
        )
        out = r.stdout.strip().lower()
        if not out:
            return False
        # healthy if it says healthy, OR it's running with no healthcheck defined
        return "healthy" in out or ("running" in out and "unhealthy" not in out)
    except Exception:
        return False


def wait_for_health(services, timeout):
    log("system", f"waiting for services to be healthy: {', '.join(services)} ...")
    start = time.time()
    while time.time() - start < timeout:
        if all(service_healthy(s) for s in services):
            log("system", "all watched services healthy.")
            return True
        time.sleep(3)
    log("system", f"timed out after {timeout}s waiting for health — continuing anyway.", "error")
    return False


def port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def chaos_env_from_args(args):
    """
    Translate the chaos CLI flags into the CHAOS_* ENV VARS the producer reads.

    IMPORTANT — the producer takes chaos via ENVIRONMENT, not CLI. Verified
    against scripts/kafka_event_producer.py: its argparse only knows --rate
    and --duration; chaos is read from os.getenv("CHAOS_MALFORMED_PCT"),
    ("CHAOS_LATE_PCT"), ("CHAOS_SEED") at startup. (datamodel.md's
    --malformed-pct/--late-pct CLI flags do NOT exist in the real script —
    passing them would crash argparse.) So run.py injects chaos by setting
    these env vars on the SIMULATOR subprocess only, leaving the frozen
    Phase-2 producer untouched.

    Returns {} when no chaos was requested, so the producer stays inert
    (its own defaults are 0/0 = no corruption).

    Precedence: an explicit --malformed-pct / --late-pct / --chaos-seed
    always wins; --chaos alone supplies the sane testing defaults
    (5% malformed, 2% late, seed 42 — the "aggressive correctness testing"
    profile from the data model docs).
    """
    want_chaos = (
        args.chaos
        or args.malformed_pct is not None
        or args.late_pct is not None
    )
    if not want_chaos:
        return {}

    malformed = args.malformed_pct if args.malformed_pct is not None else 5.0
    late      = args.late_pct      if args.late_pct      is not None else 2.0
    seed      = args.chaos_seed    if args.chaos_seed    is not None else 42
    return {
        "CHAOS_MALFORMED_PCT": str(malformed),
        "CHAOS_LATE_PCT":      str(late),
        "CHAOS_SEED":          str(seed),
    }


def stream_output(proc, tag):
    """Pump a child process's combined stdout/stderr into the console, tagged."""
    color = COLORS.get(tag, "")
    for raw in iter(proc.stdout.readline, b""):
        line = raw.decode(errors="replace").rstrip("\n")
        if line:
            print(f"{color}[{tag}]{RESET} {line}", flush=True)


# --- Startup self-heal -----------------------------------------------------------

_TRAVELLENS_CMD_PATTERNS = [
    "scripts.stream_consumer",
    "scripts.kafka_event_producer",
    "scripts.gold_lifecycle_updater",
    "scripts.review_embedder",
    "scripts.quarantine_hourly_rollup",
    "render.server",
]

_TRAVELLENS_PROC_LABELS = {
    "scripts.stream_consumer":          "consumer",
    "scripts.kafka_event_producer":     "simulator",
    "scripts.gold_lifecycle_updater":   "gold updater",
    "scripts.review_embedder":          "embedder",
    "scripts.quarantine_hourly_rollup": "quarantine rollup",
    "render.server":                    f"dashboard on :{DASHBOARD_PORT}",
}


def _pid_cmdline_map():
    """Return {pid: cmdline} for all running processes on Windows. Best-effort."""
    result = {}
    if sys.platform != "win32":
        return result
    try:
        ps_script = (
            "Get-WmiObject Win32_Process | "
            "Where-Object { $_.CommandLine -ne $null } | "
            "Select-Object ProcessId,CommandLine | "
            "ConvertTo-Json -Compress"
        )
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_script],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=20,
        )
        if r.returncode == 0 and r.stdout.strip():
            import json as _json
            data = _json.loads(r.stdout.strip())
            if isinstance(data, dict):
                data = [data]
            for entry in data:
                try:
                    pid = int(entry.get("ProcessId", 0))
                    cmd = entry.get("CommandLine") or ""
                    if pid:
                        result[pid] = cmd
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass
    return result


def _find_pid_on_port(port):
    """Return PID (int) of the process holding TCP port, or None."""
    needle = f":{port} "
    try:
        r = subprocess.run(
            ["netstat", "-ano"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=5,
        )
        for line in r.stdout.splitlines():
            if needle in line:
                parts = line.split()
                if parts and parts[-1].isdigit():
                    pid = int(parts[-1])
                    if pid != 0:
                        return pid
    except Exception:
        pass
    return None


def _kill_pid_windows(pid, label):
    """Force-kill a process tree on Windows via taskkill /F /T."""
    log("system", f"stopping {label} (PID {pid})")
    try:
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
        )
    except Exception:
        pass


def _wait_advisory_locks_released(timeout=30):
    """
    Poll pg_locks until advisory locks 7400030/40/50/60 are all freed, then return.
    Falls back to a 3-second sleep if Postgres is not reachable.

    Session-level advisory locks release when Postgres detects the broken TCP
    connection of a killed process — normally within 1–2 seconds.
    """
    query = (
        "SELECT COUNT(*) FROM pg_locks "
        "WHERE locktype='advisory' AND granted=true AND classid=0 "
        "AND objid IN (7400030, 7400040, 7400050, 7400060);"
    )
    deadline = time.time() + timeout
    ever_reachable = False
    while time.time() < deadline:
        try:
            r = subprocess.run(
                ["docker", "exec", "travellens-postgres", "psql",
                 "-U", "travellens", "-d", "travellens", "-t", "-A", "-c", query],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, timeout=5,
            )
            if r.returncode == 0:
                count_str = r.stdout.strip()
                if count_str.isdigit():
                    ever_reachable = True
                    if int(count_str) == 0:
                        break  # all locks freed
        except Exception:
            pass

        if not ever_reachable:
            # Postgres not reachable — fixed grace period instead of polling.
            time.sleep(3)
            break
        time.sleep(0.5)

    log("system",
        "advisory locks 7400030/40/50/60 released "
        "(Postgres sessions closed on process kill).")


def _startup_cleanup():
    """
    TAKEOVER — runs BEFORE port 47219 is acquired.  Kills the live run.py
    supervisor (identified by who holds port 47219) and all travellens child
    processes, then waits until Postgres advisory locks 7400030/40/50/60 are
    released.  After this returns, port 47219 is free and all four locks are
    available for the fresh procs.

    Safety: if :5000 is held by a non-travellens PID, print a clear error and exit
    — never blind-kill an unrelated process.
    """
    if sys.platform != "win32":
        return

    own_pid = os.getpid()
    cmdmap = _pid_cmdline_map()

    # ── :5000 ownership check — error before killing anything ────────────────
    port5000_pid = _find_pid_on_port(DASHBOARD_PORT)
    if port5000_pid and port5000_pid != own_pid:
        cmd5000 = cmdmap.get(port5000_pid, "")
        is_travellens = any(pat in cmd5000 for pat in _TRAVELLENS_CMD_PATTERNS)
        if not is_travellens and cmdmap:
            # cmdmap is populated; we positively know this is a foreign process.
            log("system",
                f":5000 is held by PID {port5000_pid} "
                f"({'...' + cmd5000[-60:] if cmd5000 else 'unknown'}) "
                f"which is NOT a travellens process — "
                f"stop that process manually before starting run.py.",
                "error")
            sys.exit(1)

    found = {}  # pid -> human label

    # ── Find the live run.py supervisor (holds port 47219) ───────────────────
    supervisor_pid = _find_pid_on_port(_RUN_LOCK_PORT)
    if supervisor_pid and supervisor_pid != own_pid:
        found[supervisor_pid] = "run.py supervisor"

    # ── Sweep: find leftover travellens child PIDs by cmdline pattern ────────
    for pid, cmd in cmdmap.items():
        if pid == own_pid or pid in found:
            continue
        for pat in _TRAVELLENS_CMD_PATTERNS:
            if pat in cmd:
                found[pid] = _TRAVELLENS_PROC_LABELS.get(pat, pat)
                break

    if not found:
        return  # clean slate — silent pass

    for pid, label in found.items():
        _kill_pid_windows(pid, label)

    # If we killed the supervisor, poll until port 47219 is actually free —
    # the OS releases it a moment after the process dies.
    if supervisor_pid and supervisor_pid != own_pid:
        deadline47 = time.time() + 10
        while time.time() < deadline47:
            if _find_pid_on_port(_RUN_LOCK_PORT) is None:
                break
            time.sleep(0.2)

    # Wait for Postgres to detect broken connections and release advisory locks
    # before the new procs try to acquire them.
    _wait_advisory_locks_released()

    # ── Verify :5000 is free ──────────────────────────────────────────────────
    still5000 = _find_pid_on_port(DASHBOARD_PORT)
    if still5000 and still5000 != own_pid:
        log("system",
            f"WARNING: :5000 still held by PID {still5000} after cleanup — "
            f"dashboard may fail to bind.", "error")


class Launcher:
    def __init__(self, args):
        self.args = args
        self.procs = []          # list of (name, Popen)
        self.threads = []
        self.started_docker = False
        self._shutting_down = False   # suppress double "shutting down …" log
        self._cleanup_done   = False  # idempotence guard for _do_cleanup

    # -- Docker -------------------------------------------------------------------
    def bring_up_docker(self):
        # --server-only: we do NOT bring docker up. The whole point of this mode
        # is fast UI iteration against an already-running stack — running
        # `docker compose up -d` here would slow startup for nothing. Instead we
        # do a single TCP probe on Postgres: if it answers, great; if it doesn't,
        # print ONE clear warning line (no traceback) and continue — the dashboard
        # itself will surface friendlier errors per request than we can here.
        if self.args.server_only:
            log("system", "--server-only set; skipping docker bring-up.")
            if not port_in_use(POSTGRES_PORT):
                log("system",
                    f"Postgres is not reachable on localhost:{POSTGRES_PORT} — "
                    f"dashboard will still start, but data queries will fail until "
                    f"the DB is up. Bring docker up (or omit --server-only).",
                    "error")
            return
        if self.args.no_docker:
            log("system", "--no-docker set; assuming the stack is already up.")
            return
        if not docker_daemon_up():
            log("system", "Docker daemon not reachable. Start Docker Desktop and retry.", "error")
            sys.exit(1)

        already = compose_running_count() > 0
        log("system", "starting docker compose stack (idempotent) ...")
        r = subprocess.run(COMPOSE_BASE + ["up", "-d"])
        if r.returncode != 0:
            log("system", "docker compose up failed.", "error")
            sys.exit(1)

        # We only "started" docker (and thus own teardown) if nothing was up before.
        self.started_docker = not already
        if already:
            log("system", "docker was already running — will LEAVE it up on exit.")
        else:
            log("system", "docker stack started by this script — will tear it down on exit.")

        wait_for_health(HEALTH_WAIT_SERVICES, HEALTH_TIMEOUT_SEC)

    def tear_down_docker(self):
        if self.started_docker and not self.args.no_docker:
            log("system", "stopping docker stack (this script started it) ...")
            subprocess.run(COMPOSE_BASE + ["down"])
        else:
            log("system", "leaving docker running (it was up before, or --no-docker).")

    # -- Python processes ---------------------------------------------------------
    def start_process(self, spec, extra_args=None, env_extra=None):
        name = spec["name"]
        cmd = list(spec["cmd"]) + (extra_args or [])
        # env_extra layers on top of the inherited environment — used to
        # inject CHAOS_* vars into the simulator subprocess ONLY, without
        # polluting the consumer/dashboard or the parent shell.
        env = {**os.environ, **(env_extra or {})}
        log("system", f"starting {name}: {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd, cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=env,
        )
        self.procs.append((name, proc))
        t = threading.Thread(target=stream_output, args=(proc, name), daemon=True)
        t.start()
        self.threads.append(t)

    def start_python(self):
        # Dashboard — always started. _startup_cleanup() already killed any
        # stale travellens process that was holding :5000, so no port check needed.
        self.start_process(DASHBOARD)

        # --------------------------------------------------------------------
        # --server-only: we are done. No consumer, no simulator. This is the
        # stripped-down mode for fast UI work against an already-running stack.
        # If --window was also passed it has nothing to act on (no consumer),
        # so we call that out explicitly rather than silently dropping it —
        # otherwise a user might wonder why their window override "didn't work".
        # --------------------------------------------------------------------
        if self.args.server_only:
            banner = "SERVER ONLY — no consumer, no simulator (dashboard only)"
            if self.args.window is not None:
                banner += f"  |  --window {self.args.window}m is a NO-OP here (no consumer to receive it)"
            log("system", banner, "error")  # red so the abnormal mode is unmissable
            return

        # --------------------------------------------------------------------
        # Consumer — started in every mode EXCEPT --server-only. We need it
        # under --no-sim too so the monitor stays live while the user sends
        # their own events from another terminal.
        #
        # --window N: inject a smaller tumbling window into THIS consumer
        # subprocess ONLY (via env_extra, the same scoped mechanism used for
        # chaos). It never touches .env or the parent shell, so the production
        # default (60m / 300s) is restored the moment this run exits — it
        # cannot leak into a later "real" run. Omit --window for production
        # shape. When set below 5 min, grace is also lowered to 30s so a small
        # window actually closes during the run. A loud banner makes a
        # non-production window unmistakable.
        # --------------------------------------------------------------------
        consumer_env = None
        if self.args.window is not None:
            win = str(self.args.window)
            grace = "30" if self.args.window < 5 else "300"
            consumer_env = {
                "WINDOW_SIZE_MINUTES":     win,
                "WATERMARK_GRACE_SECONDS": grace,
            }
            log("system",
                f"--window {win}m (grace {grace}s) — NON-PRODUCTION window, scoped to this "
                f"run only, .env untouched. Omit --window for the 60m production default.",
                "error")  # 'error' colour = stands out; deliberate warning, not a fault
        self.start_process(CONSUMER, env_extra=consumer_env)

        # --------------------------------------------------------------------
        # Simulator — skipped under --no-sim. The consumer above keeps running
        # so the /monitor page is still live; you generate events yourself.
        # We print a loud, multi-line banner with the exact command to copy
        # because a quiet "skipping simulator" log gets lost in startup noise
        # and the most common follow-up question is "ok, how do I send events?".
        # --------------------------------------------------------------------
        if self.args.no_sim:
            log("system",
                "--no-sim: running WITHOUT the event simulator. Consumer is still "
                "up, so the monitor will reflect whatever events you send.",
                "error")
            log("system",
                "  send events manually, e.g.:",
                "error")
            log("system",
                "    python -m scripts.kafka_event_producer --rate 50 --duration 60",
                "error")
        else:
            extra = []
            if self.args.sim_rate is not None:
                extra = [SIMULATOR["rate_flag"], str(self.args.sim_rate)]
            chaos = chaos_env_from_args(self.args)
            if chaos:
                log("system",
                    f"--chaos: simulator will inject "
                    f"malformed={chaos['CHAOS_MALFORMED_PCT']}% "
                    f"late={chaos['CHAOS_LATE_PCT']}% (seed {chaos['CHAOS_SEED']}) "
                    f"— populates the monitor's quarantine cards")
            self.start_process(SIMULATOR, extra, env_extra=chaos)

        # --------------------------------------------------------------------
        # Background maintenance procs — started in every mode that includes
        # the consumer (i.e. everything except --server-only).  Advisory
        # locks prevent duplicates: if either is already running manually,
        # the new instance acquires no lock and exits with code 1 immediately
        # — the existing instance keeps running unaffected.
        # --------------------------------------------------------------------
        self.start_process(GOLD_UPDATER)
        self.start_process(EMBEDDER)
        self.start_process(QUARANTINE_ROLLUP)

    # -- Shutdown -----------------------------------------------------------------
    def _do_cleanup(self):
        """Kill all children and optionally tear down Docker. Idempotent.

        Called from three places so no child can ever be orphaned:
          1. shutdown() — the signal handler / explicit call path
          2. run()'s finally block — catches any exception that bypasses the handler
          3. atexit.register — last resort if the finally block is itself interrupted
             (e.g. a second Ctrl-C mid-cleanup on some platforms)
        """
        if self._cleanup_done:
            return
        self._cleanup_done = True
        for name, proc in self.procs:
            _kill_proc_tree(name, proc)
        self.tear_down_docker()
        log("system", "done.")

    def shutdown(self, *_):
        """Signal handler entry point (SIGINT / SIGTERM)."""
        if not self._shutting_down:
            self._shutting_down = True
            print()
            log("system", "shutting down — stopping host processes ...")
        self._do_cleanup()

    # -- Main loop ----------------------------------------------------------------
    def run(self):
        # Register _do_cleanup with atexit BEFORE the signal handlers so it fires
        # even if the signal handler is interrupted mid-execution (e.g. a second
        # Ctrl-C while proc.wait() is blocking) — Python always runs atexit on exit.
        atexit.register(self._do_cleanup)
        signal.signal(signal.SIGINT,  self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        try:
            self.bring_up_docker()
            self.start_python()
            log("system", "stack is up. Press Ctrl-C to stop.")
            # Watch: if any process dies on its own, report it but keep the rest alive.
            while True:
                for name, proc in self.procs:
                    code = proc.poll()
                    if code is not None:
                        log("system", f"{name} exited (code {code}).", "error")
                        self.procs.remove((name, proc))
                        break
                # Exit the watch loop when nothing is left to watch. Originally
                # this only fired when docker was OURS (i.e. not --no-docker), so
                # the script stayed alive holding the docker stack open. Under
                # --server-only there is only one process (the dashboard); if it
                # dies, there is genuinely nothing to do, so we break out too.
                if not self.procs and (not self.args.no_docker or self.args.server_only):
                    log("system", "all host processes have exited.", "error")
                    break
                time.sleep(2)
        finally:
            # Belt-and-suspenders: also call _do_cleanup here so that any exception
            # that bypasses the signal handler (e.g. an unhandled error in bring_up_docker)
            # still tears everything down.  _do_cleanup is idempotent — harmless if already done.
            self._do_cleanup()


def main():
    p = argparse.ArgumentParser(description="TravelLens dev launcher")

    # --no-sim: bring docker + consumer + dashboard up, but skip the event
    # simulator subprocess. The monitor stays live because the consumer is
    # still running; the user is expected to send events from another shell.
    p.add_argument("--no-sim", action="store_true",
                   help="Skip the event simulator. Docker + consumer + dashboard still "
                        "start; you send events yourself, e.g. "
                        "`python -m scripts.kafka_event_producer --rate 50 --duration 60`.")

    # --server-only: skip docker bring-up, skip consumer, skip simulator. Start
    # ONLY the Flask dashboard. Most-stripped mode; assumes the stack is
    # already up. Wins over --no-sim when both are passed (server-only is
    # strictly more restrictive — it already implies no simulator).
    p.add_argument("--server-only", action="store_true",
                   help="Start ONLY the Flask dashboard. No consumer, no simulator, no "
                        "docker bring-up (assumes the stack/DB is already up). Beats "
                        "--no-sim if both are passed. --window is a no-op under this mode "
                        "(no consumer to receive it).")

    p.add_argument("--sim-rate", type=int, default=None,
                   help="events/sec passed to the simulator")
    p.add_argument("--no-docker", action="store_true",
                   help="assume the docker stack is already up")
    p.add_argument("--chaos", action="store_true",
                   help="simulator injects malformed + late events "
                        "(defaults: 5%% malformed, 2%% late, seed 42) — "
                        "populates the /monitor quarantine cards")
    p.add_argument("--malformed-pct", type=float, default=None,
                   help="%% of events to corrupt (implies chaos; overrides the --chaos default)")
    p.add_argument("--late-pct", type=float, default=None,
                   help="%% of events to delay past the watermark (implies chaos; overrides the --chaos default)")
    p.add_argument("--chaos-seed", type=int, default=None,
                   help="reproducible chaos seed (default 42 when chaos is on)")
    p.add_argument("--down", action="store_true",
                   help="tear the docker stack down and exit")
    p.add_argument("--window", type=int, default=None, metavar="MINUTES",
                   help="Override the consumer's tumbling-window size for THIS run only "
                        "(e.g. --window 2 flushes aggregates in ~2 min for fast monitor "
                        "testing). Omit for the production default (60 min) — plain "
                        "`python run.py` is unchanged. Scoped to the consumer subprocess "
                        "this script launches: it does NOT touch your .env or shell, so it "
                        "can never leak into a later run. When set below 5, grace is also "
                        "lowered to 30s so a small window actually closes during the run.")
    args = p.parse_args()

    if args.down:
        if docker_daemon_up():
            log("system", "tearing down docker stack ...")
            subprocess.run(COMPOSE_BASE + ["down"])
            log("system", "done.")
        else:
            log("system", "docker daemon not reachable.", "error")
        return

    # ── Takeover: kill any prior run.py + children, wait for locks ───────
    # _startup_cleanup() runs FIRST — before we hold port 47219.  It kills
    # the live supervisor (port 47219 holder) + all travellens children, then
    # polls pg_locks until advisory locks 7400030/40/50/60 are free.
    _startup_cleanup()

    # ── Acquire singleton after takeover ──────────────────────────────────
    # After cleanup port 47219 should be free.  socket.bind() is atomic —
    # two simultaneous `python run.py` invocations race here; exactly one wins.
    _lock = _acquire_run_lock()
    if _lock is None:
        # Rare: another run.py started at nearly the same moment.
        log("system",
            f"port {_RUN_LOCK_PORT} still held after cleanup — "
            f"another run.py may have started simultaneously; try again.",
            "error")
        sys.exit(1)
    # Keep _lock referenced so the socket stays bound for our lifetime.

    Launcher(args).run()


if __name__ == "__main__":
    main()
