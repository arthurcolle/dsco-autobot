#!/usr/bin/env python3
"""Self-introspection: snapshot the live Tool Management registry + swarm and
emit a structured self-portrait of what this system actually is right now.

It reads only public read endpoints (no mutation) and computes:
  - inventory          tools / backends / health ratio
  - capability coverage which capabilities exist and how many backends provide each
  - redundancy         tool names served by >1 backend, over-provided capabilities
  - gaps               unhealthy backends, declared-but-empty capabilities, dead tools
  - traffic            call volume / error rate per backend (who actually does work)
  - economy            swarm topology, tasks completed, payments processed
  - concentration      how much of the system rides on a few backends

Usage:
  python scripts/analyze_self.py                 # text report
  python scripts/analyze_self.py --json          # machine-readable
  REGISTRY_URL=http://host:2016 python scripts/analyze_self.py
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from typing import Any, Dict, List

import httpx

REGISTRY = os.getenv("REGISTRY_URL", "http://localhost:2016").rstrip("/")


def _get(client: httpx.Client, path: str) -> Any:
    try:
        r = client.get(f"{REGISTRY}{path}", timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {e}"}


def _as_list(v: Any, *keys: str) -> List[Any]:
    if isinstance(v, list):
        return v
    if isinstance(v, dict):
        for k in keys:
            if isinstance(v.get(k), list):
                return v[k]
    return []


def _caps(raw: Any) -> List[str]:
    """Backend capabilities arrive as a JSON-encoded string or a list."""
    if isinstance(raw, list):
        return [str(c) for c in raw]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return [str(c) for c in parsed] if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def gather(client: httpx.Client) -> Dict[str, Any]:
    return {
        "health": _get(client, "/health"),
        "backends": _as_list(_get(client, "/api/v1/tools/backends"), "backends", "items"),
        "catalog": _get(client, "/mcp/catalog"),
        "stats": _get(client, "/api/v1/discover/stats"),
        "swarms": _as_list(_get(client, "/api/v1/swarm/list"), "swarms", "items"),
    }


def analyze(data: Dict[str, Any]) -> Dict[str, Any]:
    backends = data["backends"]
    tools = _as_list(data["catalog"], "tools")
    stats = data["stats"] if isinstance(data["stats"], dict) else {}
    swarms = data["swarms"]

    healthy = sum(1 for b in backends if b.get("is_healthy"))
    unhealthy = [b.get("backend_id") for b in backends if not b.get("is_healthy")]

    # Capability coverage: capability -> set of backends providing it.
    cap_to_backends: Dict[str, set] = defaultdict(set)
    for b in backends:
        bid = b.get("backend_id", "?")
        for c in _caps(b.get("capabilities")):
            cap_to_backends[c].add(bid)

    # Tool-name collisions across backends.
    name_to_backends: Dict[str, set] = defaultdict(set)
    backend_tool_count: Counter = Counter()
    dead_tools = 0
    total_calls = 0
    total_errors = 0
    backend_calls: Counter = Counter()
    backend_errors: Counter = Counter()
    for t in tools:
        bid = t.get("backend_id", "?")
        name_to_backends[t.get("name", "?")].add(bid)
        backend_tool_count[bid] += 1
        st = t.get("stats") or {}
        calls = int(st.get("call_count", 0) or 0)
        errs = int(st.get("error_count", 0) or 0)
        total_calls += calls
        total_errors += errs
        backend_calls[bid] += calls
        backend_errors[bid] += errs
        if calls == 0:
            dead_tools += 1

    collisions = {n: sorted(bs) for n, bs in name_to_backends.items() if len(bs) > 1}
    over_provided = {c: sorted(bs) for c, bs in cap_to_backends.items() if len(bs) > 1}

    # Concentration: share of tools held by the top 3 backends.
    top = backend_tool_count.most_common()
    total_tools = sum(backend_tool_count.values()) or 1
    top3_share = sum(c for _, c in top[:3]) / total_tools

    # Economy from swarm topology.
    swarm_tasks = sum(int(s.get("total_tasks_completed", 0) or 0) for s in swarms)
    swarm_pay = sum(float(s.get("total_payments_processed", 0) or 0) for s in swarms)

    return {
        "registry": REGISTRY,
        "inventory": {
            "tools": len(tools),
            "backends": len(backends),
            "healthy_backends": healthy,
            "unhealthy_backends": unhealthy,
            "health_ratio": round(healthy / len(backends), 3) if backends else 0.0,
            "capabilities_distinct": len(cap_to_backends),
        },
        "traffic": {
            "total_calls": total_calls,
            "total_errors": total_errors,
            "error_rate": round(total_errors / total_calls, 4) if total_calls else 0.0,
            "dead_tools": dead_tools,
            "dead_tool_ratio": round(dead_tools / total_tools, 3),
            "busiest_backends": backend_calls.most_common(5),
        },
        "concentration": {
            "top_backends_by_tools": top[:5],
            "top3_tool_share": round(top3_share, 3),
        },
        "redundancy": {
            "tool_name_collisions": len(collisions),
            "collision_examples": dict(list(collisions.items())[:8]),
            "over_provided_capabilities": len(over_provided),
            "over_provided_examples": dict(list(over_provided.items())[:8]),
        },
        "gaps": {
            "unhealthy_backends": unhealthy,
            "singleton_capabilities": sorted(
                c for c, bs in cap_to_backends.items() if len(bs) == 1
            )[:20],
            "singleton_capability_count": sum(
                1 for bs in cap_to_backends.values() if len(bs) == 1
            ),
        },
        "economy": {
            "swarms": len(swarms),
            "tasks_completed": swarm_tasks,
            "payments_processed": round(swarm_pay, 4),
            "compositions": stats.get("total_compositions"),
            "executions_recorded": stats.get("total_executions"),
        },
    }


def render(report: Dict[str, Any]) -> str:
    inv, tr, conc = report["inventory"], report["traffic"], report["concentration"]
    red, gaps, eco = report["redundancy"], report["gaps"], report["economy"]
    L: List[str] = []
    L.append("=" * 66)
    L.append("  SELF-PORTRAIT  " + report["registry"])
    L.append("=" * 66)
    L.append("")
    L.append("INVENTORY")
    L.append(f"  tools .............. {inv['tools']}")
    L.append(f"  backends .......... {inv['backends']}  "
             f"({inv['healthy_backends']} healthy, ratio {inv['health_ratio']})")
    L.append(f"  capabilities ...... {inv['capabilities_distinct']} distinct")
    L.append("")
    L.append("TRAFFIC  (what actually runs)")
    L.append(f"  total calls ....... {tr['total_calls']}  "
             f"errors {tr['total_errors']} (rate {tr['error_rate']})")
    L.append(f"  dead tools ........ {tr['dead_tools']} / {inv['tools']} "
             f"({tr['dead_tool_ratio']*100:.0f}% never called)")
    if tr["busiest_backends"]:
        busy = ", ".join(f"{b}={n}" for b, n in tr["busiest_backends"] if n)
        L.append(f"  busiest ........... {busy or 'none — system is cold'}")
    L.append("")
    L.append("CONCENTRATION")
    L.append(f"  top-3 hold {conc['top3_tool_share']*100:.0f}% of all tools")
    for b, n in conc["top_backends_by_tools"]:
        L.append(f"    {n:>4}  {b}")
    L.append("")
    L.append("REDUNDANCY")
    L.append(f"  tool-name collisions ......... {red['tool_name_collisions']}")
    for n, bs in red["collision_examples"].items():
        L.append(f"    {n}: {', '.join(bs)}")
    L.append(f"  over-provided capabilities ... {red['over_provided_capabilities']}")
    L.append("")
    L.append("GAPS")
    L.append(f"  unhealthy backends .......... {gaps['unhealthy_backends'] or 'none'}")
    L.append(f"  singleton capabilities ...... {gaps['singleton_capability_count']} "
             f"(single point of failure)")
    L.append("")
    L.append("ECONOMY")
    L.append(f"  swarms ............ {eco['swarms']}")
    L.append(f"  tasks completed ... {eco['tasks_completed']}")
    L.append(f"  payments .......... {eco['payments_processed']}")
    L.append(f"  compositions ...... {eco['compositions']}   "
             f"executions {eco['executions_recorded']}")
    L.append("")
    # A one-line self-diagnosis.
    verdict = []
    if tr["dead_tool_ratio"] > 0.9:
        verdict.append("almost entirely cold — capability exists but isn't exercised")
    if inv["health_ratio"] < 0.5:
        verdict.append("majority of backends unhealthy")
    if conc["top3_tool_share"] > 0.5:
        verdict.append("tool surface concentrated in a few backends")
    if eco["payments_processed"] == 0:
        verdict.append("no economic activity yet — market layer is unwired")
    L.append("SELF-DIAGNOSIS: " + ("; ".join(verdict) if verdict else "balanced and active"))
    L.append("=" * 66)
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = ap.parse_args()

    with httpx.Client() as client:
        data = gather(client)
    if isinstance(data["health"], dict) and data["health"].get("_error"):
        print(f"Registry unreachable at {REGISTRY}: {data['health']['_error']}")
        return
    report = analyze(data)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(render(report))


if __name__ == "__main__":
    main()
