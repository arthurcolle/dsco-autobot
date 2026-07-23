#!/usr/bin/env python3
"""Credential-aware supervision tree for the Tool Management stack.

Owns three independent services and keeps each one serving:

  - registry          (FastAPI on :2016, started via ./start_registry.sh)
  - finance-fabric    (NativeMCPBase on :4009)
  - integration-fabric(NativeMCPBase on :4010)

Why a custom supervisor instead of launchd KeepAlive + a bash watchdog:
  * launchd only catches process death, not the "alive but wedged" asyncio
    state the registry can fall into; we probe HTTP /health instead.
  * The old bash watchdog nuked the whole process group on any failure, so a
    single wedged fabric forced a ~56s cold restart of everything. Here each
    service is restarted in isolation with its own exponential backoff, so one
    failure never cascades.
  * launchd-spawned processes don't inherit an interactive shell's env, so live
    API adapters fell back to sandbox mode. We load a gitignored .env up front
    and pass it to every child, so credentials flow everywhere.

The registry is told (via NATIVE_MCP_EXCLUDE) not to own the two fabrics, so the
supervisor is the single owner of each fabric process — no double-starts.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Optional

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / "data" / "logs"
STATUS_FILE = LOG_DIR / "supervisor_status.json"

PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "3"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "4"))
HEALTH_FAILS = int(os.environ.get("HEALTH_FAILS", "2"))
BOOT_GRACE = float(os.environ.get("BOOT_GRACE", "60"))
BACKOFF_MIN = 2.0
BACKOFF_MAX = 60.0


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[{ts}] {msg}", flush=True)


def load_dotenv(env: dict) -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip()
    log(f"loaded .env ({path})")


@dataclass
class Service:
    name: str
    cmd: list[str]
    port: int
    health_path: str = "/health"
    extra_env: dict = field(default_factory=dict)
    proc: Optional[subprocess.Popen] = None
    fails: int = 0
    started_at: float = 0.0
    backoff: float = BACKOFF_MIN
    next_start_allowed: float = 0.0
    log_fh: Optional[IO] = None

    def healthy(self) -> bool:
        # Fast TCP gate first; cheap and catches a dead listener immediately.
        try:
            with socket.create_connection(("127.0.0.1", self.port), timeout=PROBE_TIMEOUT):
                pass
        except OSError:
            return False
        if not self.health_path:
            return True
        url = f"http://127.0.0.1:{self.port}{self.health_path}"
        try:
            with urllib.request.urlopen(url, timeout=PROBE_TIMEOUT) as r:
                return 200 <= r.status < 300
        except Exception:
            return False

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self, base_env: dict) -> None:
        self.stop()
        env = dict(base_env)
        env.update(self.extra_env)
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.log_fh = open(LOG_DIR / f"{self.name}.log", "a")
        log(f"starting {self.name} on :{self.port}")
        self.proc = subprocess.Popen(
            self.cmd,
            cwd=str(ROOT),
            env=env,
            stdout=self.log_fh,
            stderr=subprocess.STDOUT,
        )
        self.started_at = time.time()
        self.fails = 0

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                try:
                    self.proc.wait(timeout=4)
                except subprocess.TimeoutExpired:
                    pass
        # Reap anything still bound to the port (reload workers, orphans).
        _kill_port(self.port)
        if self.log_fh:
            try:
                self.log_fh.close()
            except Exception:
                pass
            self.log_fh = None
        self.proc = None

    def note_failure(self) -> None:
        self.backoff = min(self.backoff * 2, BACKOFF_MAX)
        self.next_start_allowed = time.time() + self.backoff
        log(f"{self.name} unhealthy; backing off {self.backoff:.0f}s before restart")

    def note_recovery(self) -> None:
        if self.backoff != BACKOFF_MIN:
            log(f"{self.name} recovered; resetting backoff")
        self.backoff = BACKOFF_MIN


def _kill_port(port: int) -> None:
    try:
        out = subprocess.run(
            ["lsof", "-ti", f":{port}"], capture_output=True, text=True, timeout=8
        ).stdout
    except Exception:
        return
    for pid in out.split():
        try:
            os.kill(int(pid), signal.SIGKILL)
        except (ProcessLookupError, ValueError):
            pass


def build_services(base_env: dict) -> list[Service]:
    # Fabrics need the project venv (fastapi etc.), not whatever interpreter
    # launchd happened to spawn the supervisor with.
    venv_py = ROOT / os.environ.get("VENV_DIR", ".venv") / "bin" / "python"
    py = str(venv_py) if venv_py.exists() else sys.executable
    registry = Service(
        name="registry",
        cmd=["/bin/bash", str(ROOT / "start_registry.sh")],
        port=int(os.environ.get("PORT", "2016")),
        extra_env={
            "PORT": os.environ.get("PORT", "2016"),
            "RELOAD": "0",
            "AUTO_START_NATIVE_MCPS": "1",
            "ENABLE_ANNOUNCE_EMBED_SYNC": "0",
            # Supervisor owns these fabrics; registry must not start them.
            "NATIVE_MCP_EXCLUDE": "finance-fabric,integration-fabric",
        },
    )
    finance = Service(
        name="finance-fabric",
        cmd=[py, "-m", "native.finance_fabric.server"],
        port=int(os.environ.get("FINANCE_FABRIC_MCP_PORT", "4009")),
        extra_env={
            "PORT": os.environ.get("FINANCE_FABRIC_MCP_PORT", "4009"),
            "REGISTRY_URL": base_env.get("REGISTRY_URL", "http://localhost:2016"),
            "PYTHONPATH": f"{ROOT}:{ROOT}/src",
        },
    )
    integration = Service(
        name="integration-fabric",
        cmd=[py, "-m", "native.integration_fabric.server"],
        port=int(os.environ.get("INTEGRATION_FABRIC_MCP_PORT", "4010")),
        extra_env={
            "PORT": os.environ.get("INTEGRATION_FABRIC_MCP_PORT", "4010"),
            "REGISTRY_URL": base_env.get("REGISTRY_URL", "http://localhost:2016"),
            "PYTHONPATH": f"{ROOT}:{ROOT}/src",
        },
    )
    return [registry, finance, integration]


def write_status(services: list[Service]) -> None:
    data = {
        "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "services": {
            s.name: {
                "port": s.port,
                "alive": s.alive(),
                "pid": s.proc.pid if s.proc else None,
                "fails": s.fails,
                "backoff": s.backoff,
                "uptime_s": round(time.time() - s.started_at, 1) if s.started_at else 0,
            }
            for s in services
        },
    }
    try:
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATUS_FILE.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


def main() -> int:
    base_env = os.environ.copy()
    load_dotenv(base_env)
    base_env.setdefault("REGISTRY_URL", f"http://localhost:{os.environ.get('PORT', '2016')}")

    services = build_services(base_env)
    registry = services[0]

    stopping = {"flag": False}

    def handle_term(signum, frame):
        stopping["flag"] = True
        log(f"signal {signum}; tearing down")

    signal.signal(signal.SIGTERM, handle_term)
    signal.signal(signal.SIGINT, handle_term)

    # Dependency ordering: registry must be healthy before fabrics announce.
    registry.start(base_env)
    deadline = time.time() + BOOT_GRACE
    while not registry.healthy() and time.time() < deadline and not stopping["flag"]:
        time.sleep(2)
    if registry.healthy():
        log("registry healthy")
    else:
        log("WARN: registry not healthy within boot grace; continuing to probe")

    for svc in services[1:]:
        svc.start(base_env)

    write_status(services)

    try:
        while not stopping["flag"]:
            time.sleep(PROBE_INTERVAL)
            for svc in services:
                if stopping["flag"]:
                    break
                # Give a freshly started service its boot grace before probing.
                if svc.started_at and (time.time() - svc.started_at) < BOOT_GRACE \
                        and not svc.healthy() and svc.alive():
                    continue
                if svc.healthy():
                    svc.fails = 0
                    svc.note_recovery()
                    continue
                # In an active backoff window: hold off, don't keep penalizing.
                if not svc.alive() and time.time() < svc.next_start_allowed:
                    continue
                svc.fails += 1
                log(f"{svc.name} health probe failed ({svc.fails}/{HEALTH_FAILS})")
                if svc.fails >= HEALTH_FAILS:
                    svc.note_failure()
                    svc.start(base_env)
            write_status(services)
    finally:
        for svc in reversed(services):
            svc.stop()
        write_status(services)
        log("supervisor stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
