#!/usr/bin/env python3
"""
Dynamic integration probe for ToolManagement.

Starts native MCPs and optional Jina/Parallel service adapters in-memory,
probes /health and /tools, captures stdout/stderr, and writes a structured
report.  Nothing is persisted to the central registry unless explicitly
configured.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

# Ensure project paths are available
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

from native.launcher import NativeMCPLauncher, SERVERS

# Codex Apps catalog integration
sys.path.insert(0, str(PROJECT_ROOT / "src"))
from tool_management.integrations.codex_apps_adapter import (
    CodexAppsCatalogAdapter,
    COMMERCIAL_BUNDLES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("integration_probe")

# Map probed integration ids to the commercial bundles they cover.
# A single integration may contribute to multiple bundles.
BUNDLE_INFERENCE: Dict[str, List[str]] = {
    "search": ["engineering_product"],
    "research": ["engineering_product", "gtm_support"],
    "publish": ["gtm_support", "engineering_product"],
    "memory": ["governance_meta", "enterprise_knowledge"],
    "video": ["enterprise_knowledge"],
    "document": ["enterprise_knowledge"],
    "dsco-bridge": ["engineering_product"],
    "unit-fabric": ["governance_meta"],
    "finance-fabric": ["finance_data"],
    "integration-fabric": ["engineering_product", "gtm_support", "enterprise_knowledge"],
    "jina": ["engineering_product"],
    "parallel": ["engineering_product", "gtm_support"],
}


@dataclass
class ProbeResult:
    id: str
    name: str
    kind: str  # native | service
    port: int
    module: Optional[str] = None
    healthy: bool = False
    health_status: Dict[str, Any] = field(default_factory=dict)
    tools: List[Dict[str, Any]] = field(default_factory=list)
    tool_count: int = 0
    startup_ms: float = 0.0
    error: Optional[str] = None
    env_keys: List[str] = field(default_factory=list)
    stdout_tail: str = ""


class IntegrationProbe:
    """Runs integration servers dynamically and captures their surface."""

    def __init__(
        self,
        project_root: Path,
        native_keys: Optional[List[str]] = None,
        start_jina: bool = False,
        start_parallel: bool = False,
        timeout: float = 45.0,
        codex_adapter: Optional[CodexAppsCatalogAdapter] = None,
    ):
        self.project_root = project_root
        self.native_keys = native_keys or list(SERVERS.keys())
        self.start_jina = start_jina
        self.start_parallel = start_parallel
        self.timeout = timeout
        self.codex_adapter = codex_adapter or CodexAppsCatalogAdapter()
        self.launcher = NativeMCPLauncher()
        self.service_processes: Dict[str, subprocess.Popen] = {}
        self.results: List[ProbeResult] = []
        self._stdout_bufs: Dict[str, List[str]] = {}

    async def run(self) -> Dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        logger.info("Starting integration probe...")

        # 1. Native MCPs
        for key in self.native_keys:
            await self._start_native(key)

        # 2. Service adapters (Jina/Parallel)
        if self.start_jina:
            await self._start_service("jina", "deploy.jina_service", 8001, ["JINA_API_KEY"])
        if self.start_parallel:
            await self._start_service("parallel", "deploy.parallel_service", 8002, ["PARALLEL_API_KEY"])

        # 3. Probe all running services
        await self._probe_all()

        return self._build_report(started_at)

    async def _start_native(self, key: str) -> None:
        server = SERVERS.get(key)
        if not server:
            return
        t0 = time.time()
        proc = self.launcher.start_server(key)
        if not proc:
            self.results.append(ProbeResult(
                id=key,
                name=server["name"],
                kind="native",
                port=server["port"],
                module=server["module"],
                error="failed to start process",
                env_keys=[server.get("env_key")] if server.get("env_key") else [],
            ))
            return
        self._stdout_bufs[key] = []
        healthy = await self.launcher.wait_for_healthy(key, timeout=self.timeout)
        elapsed = (time.time() - t0) * 1000
        self.results.append(ProbeResult(
            id=key,
            name=server["name"],
            kind="native",
            port=server["port"],
            module=server["module"],
            healthy=healthy,
            startup_ms=elapsed,
            env_keys=[server.get("env_key")] if server.get("env_key") else [],
            stdout_tail=self._read_tail(proc, key),
        ))

    async def _start_service(
        self,
        name: str,
        module: str,
        port: int,
        env_keys: List[str],
    ) -> None:
        t0 = time.time()
        env = os.environ.copy()
        env["PORT"] = str(port)
        env["PYTHONPATH"] = f"{self.project_root}:{self.project_root}/src:{env.get('PYTHONPATH', '')}"
        # Do not auto-register with central registry during probing
        env["REGISTRY_URL"] = ""
        env["REGISTER_ON_STARTUP"] = "0"
        proc = subprocess.Popen(
            [sys.executable, "-m", module],
            cwd=str(self.project_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.service_processes[name] = proc
        self._stdout_bufs[name] = []
        healthy = await self._wait_for_healthy(port, timeout=self.timeout)
        elapsed = (time.time() - t0) * 1000
        self.results.append(ProbeResult(
            id=name,
            name=name,
            kind="service",
            port=port,
            module=module,
            healthy=healthy,
            startup_ms=elapsed,
            env_keys=env_keys,
            stdout_tail=self._read_tail(proc, name),
        ))

    async def _wait_for_healthy(self, port: int, timeout: float = 45.0) -> bool:
        url = f"http://localhost:{port}/health"
        deadline = time.time() + timeout
        async with httpx.AsyncClient() as client:
            while time.time() < deadline:
                try:
                    r = await client.get(url, timeout=2.0)
                    if r.status_code == 200:
                        return True
                except (httpx.ConnectError, httpx.TimeoutException):
                    pass
                await asyncio.sleep(0.5)
        return False

    async def _probe_all(self) -> None:
        async with httpx.AsyncClient() as client:
            for result in self.results:
                if not result.healthy:
                    continue
                url = f"http://localhost:{result.port}"
                try:
                    hr = await client.get(f"{url}/health", timeout=10.0)
                    result.health_status = hr.json() if hr.status_code == 200 else {"status_code": hr.status_code}
                except Exception as e:
                    result.health_status = {"error": str(e)}
                try:
                    tr = await client.get(f"{url}/tools", timeout=10.0)
                    data = tr.json() if tr.status_code == 200 else {}
                    tools = data.get("tools", data) if isinstance(data, dict) else data
                    result.tools = tools
                    result.tool_count = len(tools)
                except Exception as e:
                    result.error = f"tools fetch failed: {e}"

    def _read_tail(self, proc: subprocess.Popen, key: str, n: int = 2000) -> str:
        try:
            import fcntl
            fd = proc.stdout.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
            data = os.read(fd, n).decode("utf-8", errors="replace")
            self._stdout_bufs[key].append(data)
            return data
        except Exception as e:
            return f"(could not read stdout: {e})"

    def _build_report(self, started_at: datetime) -> Dict[str, Any]:
        healthy = [r for r in self.results if r.healthy]
        total_tools = sum(r.tool_count for r in self.results)
        codex_summary = self.codex_adapter.summary()
        coverage = self._build_coverage()
        return {
            "probe_id": started_at.strftime("%Y%m%d-%H%M%S"),
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "project_root": str(self.project_root),
            "summary": {
                "total_integrations": len(self.results),
                "healthy": len(healthy),
                "unhealthy": len(self.results) - len(healthy),
                "total_tools_discovered": total_tools,
                "codex_apps_connectors": codex_summary["total_connectors"],
                "codex_apps_curated_pointers": codex_summary["curated_pointers"],
                "codex_apps_stale_pointers": codex_summary["curated_stale"],
            },
            "codex_apps_summary": codex_summary,
            "coverage": coverage,
            "integrations": [asdict(r) for r in self.results],
        }

    def _build_coverage(self) -> Dict[str, Any]:
        """Cross-reference probed integrations against Codex Apps bundles."""
        coverage: Dict[str, Any] = {"bundles": {}, "total_matched_connectors": 0}
        matched_connector_ids: set[str] = set()

        for bundle_id, spec in COMMERCIAL_BUNDLES.items():
            bundle = self.codex_adapter.bundle(bundle_id)
            bundle_connector_ids = {c["connector_id"] for c in bundle["connectors"]}

            # Which probed integrations claim this bundle?
            live_providers = [
                r.id for r in self.results
                if r.healthy and bundle_id in BUNDLE_INFERENCE.get(r.id, [])
            ]

            # Mark catalog connectors as "has_live_equivalent" if a probed integration
            # covers this bundle. This is a coarse signal, not a 1:1 mapping.
            for conn in bundle["connectors"]:
                conn["has_live_equivalent"] = bool(live_providers)
                if live_providers:
                    matched_connector_ids.add(conn["connector_id"])

            coverage["bundles"][bundle_id] = {
                "name": spec["name"],
                "description": spec["description"],
                "catalog_total": bundle["total"],
                "catalog_live": bundle["live"],
                "catalog_only": bundle["catalog_only"],
                "catalog_missing": len(bundle["missing"]),
                "missing_names": bundle["missing"],
                "live_providers": live_providers,
                "connector_ids": sorted(bundle_connector_ids),
            }

        coverage["total_matched_connectors"] = len(matched_connector_ids)
        coverage["inference_map"] = dict(BUNDLE_INFERENCE)
        return coverage

    def shutdown(self) -> None:
        logger.info("Shutting down all probed integrations...")
        self.launcher.stop_all()
        for name, proc in self.service_processes.items():
            try:
                logger.info(f"Stopping service {name}...")
                proc.terminate()
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def main():
    parser = argparse.ArgumentParser(description="Probe ToolManagement integrations in-memory")
    parser.add_argument(
        "--natives",
        default=",".join(SERVERS.keys()),
        help="Comma-separated native MCP keys to probe (default: all)",
    )
    parser.add_argument("--jina", action="store_true", help="Probe Jina service adapter")
    parser.add_argument("--parallel", action="store_true", help="Probe Parallel service adapter")
    parser.add_argument("--timeout", type=float, default=45.0, help="Health wait timeout")
    parser.add_argument("--output", type=Path, default=None, help="JSON report path")
    parser.add_argument("--stdout", action="store_true", help="Print captured stdout tails")
    parser.add_argument("--no-codex", action="store_true", help="Skip Codex Apps catalog cross-reference")
    args = parser.parse_args()

    native_keys = [k.strip() for k in args.natives.split(",") if k.strip()]
    codex_adapter = None if args.no_codex else CodexAppsCatalogAdapter()
    probe = IntegrationProbe(
        project_root=PROJECT_ROOT,
        native_keys=native_keys,
        start_jina=args.jina,
        start_parallel=args.parallel,
        timeout=args.timeout,
        codex_adapter=codex_adapter,
    )
    try:
        report = asyncio.run(probe.run())
    finally:
        probe.shutdown()

    output_path = args.output or PROJECT_ROOT / "data" / f"integration_probe_{report['probe_id']}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    logger.info(f"Report written to {output_path}")

    print(json.dumps(report["summary"], indent=2))
    if "coverage" in report:
        print("\nBundle coverage:")
        for bid, bc in report["coverage"]["bundles"].items():
            providers = ", ".join(bc["live_providers"]) or "none"
            print(f"  {bid:25} catalog={bc['catalog_total']:3d}  live={bc['catalog_live']:3d}  missing={bc['catalog_missing']:3d}  providers=[{providers}]")
    if args.stdout:
        for r in report["integrations"]:
            print(f"\n--- {r['id']} stdout tail ---")
            print(r.get("stdout_tail", ""))


if __name__ == "__main__":
    main()
