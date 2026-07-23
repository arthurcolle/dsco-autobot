"""
Tool space: a combinatorial, lazily-materialized universe of composed tools.

We do not store billions of tool definitions. Instead we define an *address
space* over the canonical operations of every fabric and expose:

  - the cardinality of that space (|U|^depth),
  - a bijection between an integer index and a concrete pipeline (mixed-radix),
  - a sampler, and
  - an executor that materializes any addressed pipeline on demand and runs it
    through the Tool Management registry, auto-piping each step's output into
    the next.

A "tool" at depth d is an ordered chain of d canonical ops
(op_1 -> op_2 -> ... -> op_d). Depth 1 is just the canonical op itself; deeper
chains are composed capabilities. With |U| canonical ops the space at depth d
has |U|^d members, so the addressable surface reaches billions by depth 4.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import httpx

from native.integration_fabric.catalog import (
    build_tools as build_prims_integration,
    build_canonical as build_canon_integration,
)
from native.finance_fabric.catalog import (
    build_tools as build_prims_finance,
    build_canonical as build_canon_finance,
)


def build_universe() -> List[str]:
    """The node set U: every addressable tool of every fabric, registry-qualified.

    Includes the full primitive surface (``{provider}_{domain}_{op}``) *and* the
    canonical aliases (``{domain}__{op}``) of both fabrics. Deterministically
    ordered (fabric-grouped, then sorted) so the index<->pipeline bijection is
    stable across processes.
    """
    nodes: List[str] = []
    for fabric, builders in (
        ("integration-fabric", (build_prims_integration, build_canon_integration)),
        ("finance-fabric", (build_prims_finance, build_canon_finance)),
    ):
        names: set[str] = set()
        for build in builders:
            tools, _ = build()
            for t in tools:
                names.add(t["name"])
        for name in sorted(names):
            nodes.append(f"{fabric}:{name}")
    return nodes


UNIVERSE: List[str] = build_universe()
U: int = len(UNIVERSE)


def cardinality(depth: int) -> int:
    return U ** depth


def total_upto(max_depth: int) -> int:
    return sum(U ** d for d in range(1, max_depth + 1))


def index_to_pipeline(index: int, depth: int) -> List[str]:
    """Mixed-radix (base |U|) decode of an integer index into a depth-long chain.

    Deterministic bijection over [0, |U|^depth): each address resolves to exactly
    one pipeline and vice-versa.
    """
    if depth < 1:
        raise ValueError("depth must be >= 1")
    size = U ** depth
    idx = index % size
    ops: List[str] = []
    for _ in range(depth):
        ops.append(UNIVERSE[idx % U])
        idx //= U
    return list(reversed(ops))


def pipeline_to_index(ops: List[str]) -> int:
    idx = 0
    for op in ops:
        idx = idx * U + UNIVERSE.index(op)
    return idx


def address(ops: List[str]) -> str:
    """Human-readable address for a pipeline."""
    short = [o.split(":", 1)[1] for o in ops]
    return "pipe::" + ">".join(short)


def sample(n: int, depth: int, seed: int = 0) -> List[Dict[str, Any]]:
    """Deterministically sample n pipeline addresses from the depth-d space."""
    size = U ** depth
    out: List[Dict[str, Any]] = []
    # Linear-congruential walk so samples are spread across the space.
    step = 2862933555777941757 % size or 1
    cur = seed % size
    for _ in range(n):
        ops = index_to_pipeline(cur, depth)
        out.append({"index": cur, "address": address(ops), "ops": ops})
        cur = (cur + step) % size
    return out


def _qualify_short(name: str) -> str:
    """Accept either a fully qualified node or a bare canonical op."""
    if ":" in name:
        return name
    # Try to find it in the universe by suffix match.
    for node in UNIVERSE:
        if node.split(":", 1)[1] == name:
            return node
    return name


class PipelineExecutor:
    """Materialize and run an addressed pipeline through the registry."""

    def __init__(self, registry_url: str):
        self.registry_url = registry_url.rstrip("/")

    async def _call(self, client: httpx.AsyncClient, tool: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        r = await client.post(f"{self.registry_url}/api/v1/tools/{tool}/execute",
                              json={"inputs": inputs}, timeout=30.0)
        r.raise_for_status()
        body = r.json()
        parsed: Any = None
        is_error = False
        out = body.get("output")
        if isinstance(out, dict):
            is_error = bool(out.get("isError"))
            content = out.get("content")
            if isinstance(content, list) and content:
                try:
                    parsed = json.loads(content[0].get("text"))
                except Exception:
                    parsed = content[0].get("text")
            else:
                parsed = out
        ok = (
            body.get("status") == "success"
            and not body.get("error")
            and not is_error
            and not (isinstance(parsed, dict) and "error" in parsed)
        )
        return {"ok": ok, "parsed": parsed}

    async def run(self, ops: List[str], inputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        ops = [_qualify_short(o) for o in ops]
        ctx: Dict[str, Any] = dict(inputs or {})
        trace: List[Dict[str, Any]] = []
        status = "success"
        async with httpx.AsyncClient(timeout=35.0) as client:
            for i, tool in enumerate(ops):
                res = await self._call(client, tool, ctx)
                trace.append({"step": i, "tool": tool, "status": "ok" if res["ok"] else "error",
                              "output": res["parsed"]})
                if res["ok"] and isinstance(res["parsed"], dict):
                    # Auto-pipe: merge scalar/string outputs forward so the next
                    # op can pick up ids, amounts, queries, etc.
                    for k, v in res["parsed"].items():
                        if isinstance(v, (str, int, float, bool)):
                            ctx[k] = v
                if not res["ok"]:
                    status = "failed"
                    break
        ok_steps = sum(1 for s in trace if s["status"] == "ok")
        return {
            "address": address(ops),
            "depth": len(ops),
            "status": status,
            "result": trace[-1]["output"] if trace else None,
            "receipt": {"steps": len(ops), "executed": len(trace), "ok": ok_steps,
                        "tools": [s["tool"] for s in trace]},
            "trace": trace,
        }
