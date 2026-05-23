#!/usr/bin/env python3
"""
run.py — TravelLens dev launcher
================================
One command to bring the whole local stack up, and a clean teardown on Ctrl-C.

    python run.py                 # full stack: docker + consumer + simulator + dashboard
    python run.py --no-sim        # skip the event simulator
    python run.py --sim-rate 5    # pass a rate arg to the simulator (see SIMULATOR below)
    python run.py --no-docker     # assume docker is already up; just start the python processes
    python run.py --chaos         # simulator injects malformed + late events (5%/2%, seed 42)
    python run.py --down          # tear everything down and exit

WHAT IT MANAGES
    - Docker stack (Postgres, Kafka, Zookeeper, MinIO, Airflow) via `docker compose up -d`.
      Airflow's scheduler runs its DAGs on its own inside its container — this script does
      NOT schedule or trigger Airflow jobs. It only brings the containers up.
    - Three host Python processes that are NOT containerised:
        consumer  — drains the Kafka topic
        simulator — produces booking events
        dashboard — the Flask render server

DESIGN DECISIONS (so future-you knows why)
    - Idempotent: `docker compose up -d` is safe to run whether containers are up or down.
      The script also pre-checks the Docker daemon and polls health before starting Python.
    - Least-surprise teardown: on exit it only runs `docker compose down` IF THIS SCRIPT
      started Docker. If Docker was already up before you ran this, it's left running.
    - The teardown always fires (signal handler + finally), even on crash or double Ctrl-C,
      so you never end up with orphaned host processes.
    - Port/duplicate check: if the dashboard port is already bound, the script won't start a
      second dashboard — it warns and continues with the rest.

------------------------------------------------------------------------------------
CONFIG — EDIT THESE THREE COMMANDS TO MATCH YOUR REPO IF THEY ARE WRONG.
They are best-guesses from the project layout. Each is a list (argv style).
------------------------------------------------------------------------------------
"""

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

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

# Services whose health we wait on before starting the Python processes.
# These are the docker compose SERVICE names — adjust to match your compose file.
HEALTH_WAIT_SERVICES = ["postgres", "kafka"]
HEALTH_TIMEOUT_SEC = 90

# ----------------------------------------------------------------------------------
# Below here is generic machinery — you shouldn't need to edit it.
# ----------------------------------------------------------------------------------

# ANSI colours for tagged logs (per process, so you can tell streams apart)
COLORS = {
    "consumer":  "\033[36m",   # cyan
    "simulator": "\033[33m",   # yellow
    "dashboard": "\033[35m",   # magenta
    "system":    "\033[32m",   # green
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


class Launcher:
    def __init__(self, args):
        self.args = args
        self.procs = []          # list of (name, Popen)
        self.threads = []
        self.started_docker = False
        self._shutting_down = False

    # -- Docker -------------------------------------------------------------------
    def bring_up_docker(self):
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
        # Dashboard — skip if its port is already bound (a dashboard is already running)
        if port_in_use(DASHBOARD["port"]):
            log("system",
                f"port {DASHBOARD['port']} already in use — NOT starting a second dashboard.",
                "error")
        else:
            self.start_process(DASHBOARD)

        # Consumer
        self.start_process(CONSUMER)

        # Simulator (optional, with optional rate + optional chaos)
        if self.args.no_sim:
            log("system", "--no-sim set; not starting the event simulator.")
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

    # -- Shutdown -----------------------------------------------------------------
    def shutdown(self, *_):
        if self._shutting_down:
            return
        self._shutting_down = True
        print()
        log("system", "shutting down — stopping host processes ...")
        for name, proc in self.procs:
            if proc.poll() is None:
                log("system", f"terminating {name} (pid {proc.pid})")
                try:
                    proc.terminate()
                except Exception:
                    pass
        # give them a moment, then hard-kill stragglers
        deadline = time.time() + 8
        for name, proc in self.procs:
            remaining = max(0, deadline - time.time())
            try:
                proc.wait(timeout=remaining)
            except Exception:
                log("system", f"force-killing {name}", "error")
                try:
                    proc.kill()
                except Exception:
                    pass
        self.tear_down_docker()
        log("system", "done.")

    # -- Main loop ----------------------------------------------------------------
    def run(self):
        signal.signal(signal.SIGINT, self.shutdown)
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
                if not self.procs and not self.args.no_docker:
                    log("system", "all host processes have exited.", "error")
                    break
                time.sleep(2)
        finally:
            self.shutdown()


def main():
    p = argparse.ArgumentParser(description="TravelLens dev launcher")
    p.add_argument("--no-sim", action="store_true", help="don't start the event simulator")
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
    args = p.parse_args()

    if args.down:
        if docker_daemon_up():
            log("system", "tearing down docker stack ...")
            subprocess.run(COMPOSE_BASE + ["down"])
            log("system", "done.")
        else:
            log("system", "docker daemon not reachable.", "error")
        return

    Launcher(args).run()


if __name__ == "__main__":
    main()
