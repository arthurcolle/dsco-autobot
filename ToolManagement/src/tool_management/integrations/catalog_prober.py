"""
Realtime MCP Catalog Prober — live connectivity mapping for 800+ integrations.

Probes every preset in the catalog across multiple dimensions:
  1. Package registry validation (npm/PyPI existence + version)
  2. GitHub repo liveness (stars, last push, open issues)
  3. MCP server spawn test (can we actually start it?)
  4. Transport health check (HTTP/SSE endpoint reachability)

Maintains a live topology map with:
  - Per-preset status (alive/degraded/dead/unknown)
  - Latency measurements
  - Version freshness
  - Dependency health propagation
  - Aggregate system health score

Usage:
    from tool_management.integrations.catalog_prober import CatalogProber

    prober = CatalogProber()
    report = await prober.probe_all(concurrency=50)
    topology = prober.get_topology()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

logger = logging.getLogger(__name__)


# ============================================================================
# Types
# ============================================================================

class ProbeStatus(str, Enum):
    ALIVE = "alive"             # Package exists, repo accessible
    DEGRADED = "degraded"       # Partially reachable (e.g., repo but no package)
    DEAD = "dead"               # Nothing reachable
    UNKNOWN = "unknown"         # Not yet probed
    SKIPPED = "skipped"         # Intentionally skipped (e.g., managed/cloud)
    TIMEOUT = "timeout"         # Probe timed out


class ProbeLayer(str, Enum):
    REGISTRY = "registry"       # npm/PyPI package exists
    REPO = "repo"               # GitHub repo accessible
    SPAWN = "spawn"             # Can start the MCP server process
    TRANSPORT = "transport"     # HTTP/SSE endpoint responds


@dataclass
class ProbeResult:
    """Result of probing a single preset across all layers."""
    preset_id: str
    status: ProbeStatus = ProbeStatus.UNKNOWN
    layers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    latency_ms: float = 0.0
    probed_at: str = ""
    error: str = ""
    package_version: str = ""
    repo_stars: int = 0
    repo_last_push: str = ""
    repo_open_issues: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "preset_id": self.preset_id,
            "status": self.status.value,
            "layers": self.layers,
            "latency_ms": round(self.latency_ms, 1),
            "probed_at": self.probed_at,
            "error": self.error,
            "package_version": self.package_version,
            "repo_stars": self.repo_stars,
            "repo_last_push": self.repo_last_push,
            "repo_open_issues": self.repo_open_issues,
        }


@dataclass
class TopologyNode:
    """A node in the live topology map."""
    preset_id: str
    name: str
    category: str
    tier: str
    status: ProbeStatus = ProbeStatus.UNKNOWN
    health_score: float = 0.0       # 0-100
    latency_ms: float = 0.0
    package_version: str = ""
    repo_stars: int = 0
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    last_probed: str = ""
    layers_ok: int = 0
    layers_total: int = 0
    error: str = ""


# ============================================================================
# CatalogProber
# ============================================================================

class CatalogProber:
    """
    Async prober that tests every MCP preset for reachability
    and builds a realtime topology map.
    """

    NPM_REGISTRY = "https://registry.npmjs.org"
    PYPI_API = "https://pypi.org/pypi"
    GITHUB_API = "https://api.github.com"

    def __init__(
        self,
        catalog: Optional[List[Dict[str, Any]]] = None,
        github_token: Optional[str] = None,
        timeout: float = 8.0,
    ):
        from .preset_catalog import PRESET_CATALOG, CatalogEngine
        self._catalog = catalog or PRESET_CATALOG
        self._engine = CatalogEngine(self._catalog)
        self._timeout = timeout
        self._github_token = github_token or os.environ.get("GITHUB_TOKEN", "")
        self._results: Dict[str, ProbeResult] = {}
        self._topology: Dict[str, TopologyNode] = {}
        self._probe_start: float = 0
        self._probe_end: float = 0
        self._init_topology()

    def _init_topology(self) -> None:
        """Pre-populate topology with all presets in UNKNOWN state."""
        for p in self._catalog:
            pid = p["id"]
            deps = self._engine.get_dependencies(pid)
            dependents = self._engine.get_dependents(pid)
            self._topology[pid] = TopologyNode(
                preset_id=pid,
                name=p["name"],
                category=p["category"],
                tier=p["tier"],
                dependencies=[d.target_id for d in deps],
                dependents=dependents,
            )

    # ------------------------------------------------------------------
    # Core probing
    # ------------------------------------------------------------------

    async def probe_all(
        self,
        concurrency: int = 40,
        layers: Optional[List[ProbeLayer]] = None,
        categories: Optional[List[str]] = None,
        tiers: Optional[List[str]] = None,
        preset_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Probe all (or filtered) presets concurrently.

        Returns aggregate report with per-preset results.
        """
        self._probe_start = time.time()

        targets = list(self._catalog)
        if categories:
            cat_set = set(categories)
            targets = [p for p in targets if p["category"] in cat_set]
        if tiers:
            tier_set = set(tiers)
            targets = [p for p in targets if p["tier"] in tier_set]
        if preset_ids:
            id_set = set(preset_ids)
            targets = [p for p in targets if p["id"] in id_set]

        if layers is None:
            layers = [ProbeLayer.REGISTRY, ProbeLayer.REPO]

        sem = asyncio.Semaphore(concurrency)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=20),
        ) as client:
            tasks = [
                self._probe_one(client, sem, preset, layers)
                for preset in targets
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                pid = targets[i]["id"]
                self._results[pid] = ProbeResult(
                    preset_id=pid,
                    status=ProbeStatus.DEAD,
                    error=str(result),
                    probed_at=datetime.now(timezone.utc).isoformat(),
                )

        self._probe_end = time.time()
        self._update_topology()
        return self._build_report(targets)

    async def _probe_one(
        self,
        client: httpx.AsyncClient,
        sem: asyncio.Semaphore,
        preset: Dict[str, Any],
        layers: List[ProbeLayer],
    ) -> ProbeResult:
        """Probe a single preset across requested layers."""
        pid = preset["id"]
        t0 = time.time()
        result = ProbeResult(
            preset_id=pid,
            probed_at=datetime.now(timezone.utc).isoformat(),
        )

        async with sem:
            layer_results: Dict[str, Dict[str, Any]] = {}

            # Layer 1: Package registry
            if ProbeLayer.REGISTRY in layers:
                reg = await self._probe_registry(client, preset)
                layer_results["registry"] = reg
                if reg.get("version"):
                    result.package_version = reg["version"]

            # Layer 2: GitHub repo
            if ProbeLayer.REPO in layers:
                repo = await self._probe_repo(client, preset)
                layer_results["repo"] = repo
                result.repo_stars = repo.get("stars", 0)
                result.repo_last_push = repo.get("last_push", "")
                result.repo_open_issues = repo.get("open_issues", 0)

            # Layer 3: Spawn test
            if ProbeLayer.SPAWN in layers:
                spawn = await self._probe_spawn(preset)
                layer_results["spawn"] = spawn

            # Layer 4: Transport
            if ProbeLayer.TRANSPORT in layers:
                transport = await self._probe_transport(client, preset)
                layer_results["transport"] = transport

            result.layers = layer_results
            result.latency_ms = (time.time() - t0) * 1000

            # Compute status
            ok_count = sum(1 for lr in layer_results.values() if lr.get("ok"))
            total = len(layer_results)
            if total == 0:
                result.status = ProbeStatus.SKIPPED
            elif ok_count == total:
                result.status = ProbeStatus.ALIVE
            elif ok_count > 0:
                result.status = ProbeStatus.DEGRADED
            else:
                result.status = ProbeStatus.DEAD

            self._results[pid] = result
            return result

    # ------------------------------------------------------------------
    # Layer probers
    # ------------------------------------------------------------------

    async def _probe_registry(
        self, client: httpx.AsyncClient, preset: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check if the package exists on npm or PyPI."""
        pkg = preset.get("package", "")
        install_type = preset.get("install_type", "")

        if not pkg:
            return {"ok": False, "reason": "no_package", "version": ""}

        try:
            if install_type == "npm":
                # Handle scoped packages
                encoded = pkg.replace("/", "%2F")
                url = f"{self.NPM_REGISTRY}/{encoded}"
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    version = data.get("dist-tags", {}).get("latest", "")
                    name = data.get("name", pkg)
                    return {
                        "ok": True,
                        "registry": "npm",
                        "version": version,
                        "name": name,
                        "description": data.get("description", "")[:200],
                    }
                return {"ok": False, "reason": f"npm_{resp.status_code}", "version": ""}

            elif install_type == "pip":
                # Normalize package name for PyPI
                normalized = re.sub(r"[-_.]+", "-", pkg).lower()
                url = f"{self.PYPI_API}/{normalized}/json"
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    version = data.get("info", {}).get("version", "")
                    return {
                        "ok": True,
                        "registry": "pypi",
                        "version": version,
                        "name": data.get("info", {}).get("name", pkg),
                        "description": data.get("info", {}).get("summary", "")[:200],
                    }
                return {"ok": False, "reason": f"pypi_{resp.status_code}", "version": ""}

            else:
                return {"ok": False, "reason": f"unsupported_type_{install_type}", "version": ""}

        except httpx.TimeoutException:
            return {"ok": False, "reason": "timeout", "version": ""}
        except Exception as e:
            return {"ok": False, "reason": str(e)[:100], "version": ""}

    async def _probe_repo(
        self, client: httpx.AsyncClient, preset: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check GitHub repo liveness."""
        repo_url = preset.get("repo", "")
        if not repo_url:
            return {"ok": False, "reason": "no_repo"}

        # Extract owner/repo from URL
        match = re.search(r"github\.com/([^/]+)/([^/\s]+)", repo_url)
        if not match:
            return {"ok": False, "reason": "not_github"}

        owner, repo = match.group(1), match.group(2).rstrip(".git")

        try:
            headers = {}
            if self._github_token:
                headers["Authorization"] = f"token {self._github_token}"

            resp = await client.get(
                f"{self.GITHUB_API}/repos/{owner}/{repo}",
                headers=headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "ok": True,
                    "stars": data.get("stargazers_count", 0),
                    "forks": data.get("forks_count", 0),
                    "open_issues": data.get("open_issues_count", 0),
                    "last_push": data.get("pushed_at", ""),
                    "archived": data.get("archived", False),
                    "default_branch": data.get("default_branch", ""),
                    "language": data.get("language", ""),
                    "license": (data.get("license") or {}).get("spdx_id", ""),
                }
            elif resp.status_code == 404:
                return {"ok": False, "reason": "repo_not_found"}
            elif resp.status_code == 403:
                return {"ok": False, "reason": "rate_limited"}
            else:
                return {"ok": False, "reason": f"github_{resp.status_code}"}

        except httpx.TimeoutException:
            return {"ok": False, "reason": "timeout"}
        except Exception as e:
            return {"ok": False, "reason": str(e)[:100]}

    async def _probe_spawn(self, preset: Dict[str, Any]) -> Dict[str, Any]:
        """Try to spawn the MCP server process (quick --help or --version check)."""
        install_cmd = preset.get("install_cmd", "")
        if not install_cmd:
            return {"ok": False, "reason": "no_install_cmd"}

        # Extract the binary/npx command
        parts = install_cmd.split()
        if not parts:
            return {"ok": False, "reason": "empty_cmd"}

        # We only test if the base command exists, don't actually run servers
        base_cmd = parts[0]
        try:
            proc = await asyncio.create_subprocess_exec(
                "which", base_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.0)
            if proc.returncode == 0:
                return {"ok": True, "path": stdout.decode().strip()}
            return {"ok": False, "reason": "not_installed"}
        except asyncio.TimeoutError:
            return {"ok": False, "reason": "timeout"}
        except Exception as e:
            return {"ok": False, "reason": str(e)[:100]}

    async def _probe_transport(
        self, client: httpx.AsyncClient, preset: Dict[str, Any]
    ) -> Dict[str, Any]:
        """For HTTP/SSE transports, check if the endpoint responds."""
        transport = preset.get("transport", "stdio")
        if transport == "stdio":
            return {"ok": False, "reason": "stdio_transport", "skipped": True}

        # For managed services, try the install_cmd URL if present
        install_cmd = preset.get("install_cmd", "")
        url_match = re.search(r"https?://[^\s]+", install_cmd)
        if not url_match:
            return {"ok": False, "reason": "no_url_found"}

        url = url_match.group(0)
        try:
            resp = await client.head(url)
            return {
                "ok": resp.status_code < 500,
                "status_code": resp.status_code,
                "url": url,
            }
        except httpx.TimeoutException:
            return {"ok": False, "reason": "timeout", "url": url}
        except Exception as e:
            return {"ok": False, "reason": str(e)[:100], "url": url}

    # ------------------------------------------------------------------
    # Topology building
    # ------------------------------------------------------------------

    def _update_topology(self) -> None:
        """Update topology nodes from probe results."""
        for pid, result in self._results.items():
            node = self._topology.get(pid)
            if not node:
                continue

            node.status = result.status
            node.latency_ms = result.latency_ms
            node.package_version = result.package_version
            node.repo_stars = result.repo_stars
            node.last_probed = result.probed_at
            node.error = result.error

            layers_ok = sum(1 for lr in result.layers.values() if lr.get("ok"))
            layers_total = sum(1 for lr in result.layers.values() if not lr.get("skipped"))
            node.layers_ok = layers_ok
            node.layers_total = layers_total

            # Health score
            base = (layers_ok / max(layers_total, 1)) * 60
            tier_bonus = {"official": 20, "vendor": 15, "community": 5}.get(node.tier, 0)
            star_bonus = min(result.repo_stars / 100, 15)
            node.health_score = round(min(base + tier_bonus + star_bonus, 100), 1)

        # Propagate: if a dependency is dead, degrade dependents
        for pid, node in self._topology.items():
            for dep_id in node.dependencies:
                dep_node = self._topology.get(dep_id)
                if dep_node and dep_node.status == ProbeStatus.DEAD:
                    if node.health_score > 20:
                        node.health_score = max(node.health_score - 20, 0)

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

    def _build_report(self, targets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build comprehensive probe report."""
        status_counts: Dict[str, int] = defaultdict(int)
        category_health: Dict[str, List[float]] = defaultdict(list)
        tier_health: Dict[str, List[float]] = defaultdict(list)
        alive_presets: List[Dict[str, Any]] = []
        dead_presets: List[Dict[str, Any]] = []
        degraded_presets: List[Dict[str, Any]] = []
        registry_versions: Dict[str, str] = {}

        for p in targets:
            pid = p["id"]
            result = self._results.get(pid)
            if not result:
                status_counts["unknown"] += 1
                continue

            status_counts[result.status.value] += 1
            node = self._topology.get(pid)
            health = node.health_score if node else 0

            category_health[p["category"]].append(health)
            tier_health[p["tier"]].append(health)

            entry = {
                "id": pid,
                "name": p["name"],
                "category": p["category"],
                "tier": p["tier"],
                "health": health,
                "latency_ms": round(result.latency_ms, 1),
                "version": result.package_version,
                "stars": result.repo_stars,
            }

            if result.status == ProbeStatus.ALIVE:
                alive_presets.append(entry)
            elif result.status == ProbeStatus.DEAD:
                dead_presets.append({**entry, "error": result.error})
            elif result.status == ProbeStatus.DEGRADED:
                degraded_presets.append({**entry, "layers": result.layers})

            if result.package_version:
                registry_versions[pid] = result.package_version

        # Category health averages
        cat_avg = {
            cat: round(sum(scores) / len(scores), 1)
            for cat, scores in category_health.items()
            if scores
        }
        tier_avg = {
            tier: round(sum(scores) / len(scores), 1)
            for tier, scores in tier_health.items()
            if scores
        }

        total = len(targets)
        alive = status_counts.get("alive", 0)
        degraded = status_counts.get("degraded", 0)

        return {
            "probe_summary": {
                "total_probed": total,
                "duration_seconds": round(self._probe_end - self._probe_start, 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status_distribution": dict(status_counts),
                "alive_pct": round(100 * alive / max(total, 1), 1),
                "reachable_pct": round(100 * (alive + degraded) / max(total, 1), 1),
                "system_health": round(
                    sum(n.health_score for n in self._topology.values() if n.last_probed)
                    / max(sum(1 for n in self._topology.values() if n.last_probed), 1),
                    1,
                ),
            },
            "category_health": dict(sorted(cat_avg.items(), key=lambda x: -x[1])),
            "tier_health": tier_avg,
            "alive": sorted(alive_presets, key=lambda x: -x["health"])[:50],
            "degraded": degraded_presets[:50],
            "dead": dead_presets[:50],
            "registry_versions": registry_versions,
        }

    # ------------------------------------------------------------------
    # Topology access
    # ------------------------------------------------------------------

    def get_topology(self) -> Dict[str, Any]:
        """Get the full live topology map."""
        nodes = []
        edges = []

        for pid, node in self._topology.items():
            nodes.append({
                "id": pid,
                "name": node.name,
                "category": node.category,
                "tier": node.tier,
                "status": node.status.value,
                "health": node.health_score,
                "latency_ms": node.latency_ms,
                "version": node.package_version,
                "stars": node.repo_stars,
                "layers_ok": node.layers_ok,
                "layers_total": node.layers_total,
                "last_probed": node.last_probed,
                "error": node.error,
            })
            for dep in node.dependencies:
                edges.append({"from": pid, "to": dep, "type": "depends_on"})
            for dependent in node.dependents:
                edges.append({"from": dependent, "to": pid, "type": "depended_by"})

        status_counts = defaultdict(int)
        for node in self._topology.values():
            status_counts[node.status.value] += 1

        return {
            "nodes": nodes,
            "edges": edges,
            "stats": {
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "status_distribution": dict(status_counts),
                "categories": len(set(n["category"] for n in nodes)),
            },
        }

    def get_node(self, preset_id: str) -> Optional[Dict[str, Any]]:
        """Get a single topology node with full probe data."""
        node = self._topology.get(preset_id)
        if not node:
            return None
        result = self._results.get(preset_id)
        return {
            "node": {
                "id": node.preset_id,
                "name": node.name,
                "category": node.category,
                "tier": node.tier,
                "status": node.status.value,
                "health": node.health_score,
                "latency_ms": node.latency_ms,
                "version": node.package_version,
                "stars": node.repo_stars,
                "dependencies": node.dependencies,
                "dependents": node.dependents,
                "last_probed": node.last_probed,
            },
            "probe": result.to_dict() if result else None,
        }

    def get_category_map(self) -> Dict[str, Any]:
        """Get topology grouped by category."""
        by_cat: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for pid, node in self._topology.items():
            by_cat[node.category].append({
                "id": pid,
                "name": node.name,
                "status": node.status.value,
                "health": node.health_score,
                "tier": node.tier,
            })

        result = {}
        for cat, nodes in sorted(by_cat.items()):
            avg_health = sum(n["health"] for n in nodes) / max(len(nodes), 1)
            alive = sum(1 for n in nodes if n["status"] == "alive")
            result[cat] = {
                "presets": sorted(nodes, key=lambda x: -x["health"]),
                "count": len(nodes),
                "alive": alive,
                "avg_health": round(avg_health, 1),
            }

        return result

    # ------------------------------------------------------------------
    # Targeted probes
    # ------------------------------------------------------------------

    async def probe_preset(self, preset_id: str) -> Optional[Dict[str, Any]]:
        """Probe a single preset and return its result."""
        preset = self._engine.get_by_id(preset_id)
        if not preset:
            return None

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
        ) as client:
            sem = asyncio.Semaphore(1)
            result = await self._probe_one(
                client, sem, preset, [ProbeLayer.REGISTRY, ProbeLayer.REPO]
            )
            self._update_topology()
            return result.to_dict()

    async def probe_category(self, category: str) -> Dict[str, Any]:
        """Probe all presets in a category."""
        return await self.probe_all(categories=[category])

    async def probe_tier(self, tier: str) -> Dict[str, Any]:
        """Probe all presets in a tier."""
        return await self.probe_all(tiers=[tier])

    # ------------------------------------------------------------------
    # Live monitoring
    # ------------------------------------------------------------------

    async def continuous_probe(
        self,
        interval_seconds: int = 300,
        concurrency: int = 30,
        callback: Optional[Any] = None,
    ) -> None:
        """
        Run continuous probing in a loop. Call this as a background task.

        callback(report) is called after each probe cycle.
        """
        while True:
            try:
                report = await self.probe_all(concurrency=concurrency)
                if callback:
                    await callback(report)
                logger.info(
                    "Probe cycle complete: %d probed, %.1f%% alive, health=%.1f",
                    report["probe_summary"]["total_probed"],
                    report["probe_summary"]["alive_pct"],
                    report["probe_summary"]["system_health"],
                )
            except Exception as e:
                logger.error("Probe cycle failed: %s", e)

            await asyncio.sleep(interval_seconds)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_topology_json(self) -> str:
        """Export full topology as JSON string."""
        return json.dumps(self.get_topology(), indent=2, default=str)

    def export_status_table(self) -> str:
        """Export a compact status table."""
        lines = [
            f"{'ID':35s} {'STATUS':10s} {'HEALTH':7s} {'VERSION':15s} {'STARS':6s} {'TIER':10s} {'CATEGORY':20s}",
            "-" * 110,
        ]
        for pid, node in sorted(self._topology.items(), key=lambda x: -x[1].health_score):
            if node.last_probed:
                lines.append(
                    f"{pid:35s} {node.status.value:10s} {node.health_score:6.1f}  "
                    f"{node.package_version:15s} {node.repo_stars:5d}  {node.tier:10s} {node.category:20s}"
                )
        return "\n".join(lines)
