"""
Unit Fabric MCP — the recursive spine for autonomous orgs.

Every actor — a tool, an agent, a team, an org — is the same shape:

    execute(goal, budget, policy) -> { result, receipt }

A *unit* is registered once and then becomes callable through the Tool
Management registry exactly like any other tool. Units compose recursively:
an agent calls registry tools; a team delegates to member units (agents or
other teams); an org is a team of teams. Composition is free because every
grain speaks one protocol.

Cross-cutting spine, enforced at every level:
  - identity/addressing  : every unit + work item has an id
  - work as an object     : a persistent work-graph with parent lineage
  - economics/metering    : every execution emits a receipt (cost, value, leverage)
  - governance/policy      : budgets are enforced; over-budget work is refused
  - observability/audit    : the work-graph traces any output to its leaf tool calls

Persistence is a single portable SQLite file (no external deps).
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase


REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:2016").rstrip("/")
# Simple, transparent cost model: a flat per-call price plus a latency term.
CALL_BASE_COST = float(os.getenv("UNIT_CALL_BASE_COST", "0.001"))
CALL_MS_COST = float(os.getenv("UNIT_CALL_MS_COST", "0.0000005"))
DEFAULT_MAX_DEPTH = int(os.getenv("UNIT_MAX_DEPTH", "8"))
DEFAULT_MAX_FANOUT = int(os.getenv("UNIT_MAX_FANOUT", "64"))
DEFAULT_MAX_CONCURRENCY = int(os.getenv("UNIT_MAX_CONCURRENCY", "16"))


class Policy:
    """Recursive governance object. Inherited from a parent and intersected with a
    unit's own charter — the most restrictive constraint always wins. This is the
    single mechanism that keeps a tree of 10^N units bounded and auditable."""

    def __init__(
        self,
        allowed_tools: Optional[List[str]] = None,
        deny_tools: Optional[List[str]] = None,
        max_spend: float = float("inf"),
        max_depth: int = DEFAULT_MAX_DEPTH,
        max_fanout: int = DEFAULT_MAX_FANOUT,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ):
        self.allowed_tools = allowed_tools  # None => unrestricted
        self.deny_tools = set(deny_tools or [])
        self.max_spend = max_spend
        self.max_depth = max_depth
        self.max_fanout = max_fanout
        self.max_concurrency = max_concurrency

    @classmethod
    def from_charter(cls, charter: Optional[Dict[str, Any]]) -> "Policy":
        charter = charter or {}
        return cls(
            allowed_tools=charter.get("allowed_tools"),
            deny_tools=charter.get("deny_tools"),
            max_spend=float(charter.get("max_spend", float("inf"))),
            max_depth=int(charter.get("max_depth", DEFAULT_MAX_DEPTH)),
            max_fanout=int(charter.get("max_fanout", DEFAULT_MAX_FANOUT)),
            max_concurrency=int(charter.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)),
        )

    def intersect(self, child: "Policy") -> "Policy":
        if self.allowed_tools is None:
            allowed = child.allowed_tools
        elif child.allowed_tools is None:
            allowed = self.allowed_tools
        else:
            allowed = [t for t in child.allowed_tools if t in set(self.allowed_tools)]
        return Policy(
            allowed_tools=allowed,
            deny_tools=list(self.deny_tools | child.deny_tools),
            max_spend=min(self.max_spend, child.max_spend),
            max_depth=min(self.max_depth, child.max_depth),
            max_fanout=min(self.max_fanout, child.max_fanout),
            max_concurrency=min(self.max_concurrency, child.max_concurrency),
        )

    def tool_allowed(self, tool: str) -> bool:
        if tool in self.deny_tools:
            return False
        if self.allowed_tools is None:
            return True
        return tool in set(self.allowed_tools)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "allowed_tools": self.allowed_tools,
            "deny_tools": sorted(self.deny_tools),
            "max_spend": None if self.max_spend == float("inf") else self.max_spend,
            "max_depth": self.max_depth,
            "max_fanout": self.max_fanout,
            "max_concurrency": self.max_concurrency,
        }


class Ledger:
    """Portable SQLite-backed work-graph + receipt ledger."""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS units (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                name TEXT,
                config TEXT NOT NULL,
                created_at REAL
            );
            CREATE TABLE IF NOT EXISTS work_items (
                id TEXT PRIMARY KEY,
                parent_id TEXT,
                unit_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                goal TEXT,
                status TEXT NOT NULL,
                cost REAL DEFAULT 0,
                value REAL DEFAULT 0,
                budget REAL DEFAULT 0,
                started_at REAL,
                ended_at REAL,
                result TEXT,
                error TEXT
            );
            """
        )
        self._conn.commit()

    # ---- units ----
    def put_unit(self, unit: Dict[str, Any]):
        self._conn.execute(
            "INSERT OR REPLACE INTO units(id,kind,name,config,created_at) VALUES(?,?,?,?,?)",
            (unit["id"], unit["kind"], unit.get("name", unit["id"]), json.dumps(unit), time.time()),
        )
        self._conn.commit()

    def get_unit(self, unit_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute("SELECT config FROM units WHERE id=?", (unit_id,)).fetchone()
        return json.loads(row["config"]) if row else None

    def list_units(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute("SELECT config FROM units ORDER BY created_at").fetchall()
        return [json.loads(r["config"]) for r in rows]

    # ---- work items ----
    def open_work(self, unit_id: str, kind: str, goal: str, budget: float, parent_id: Optional[str]) -> str:
        wid = uuid.uuid4().hex[:16]
        self._conn.execute(
            "INSERT INTO work_items(id,parent_id,unit_id,kind,goal,status,budget,started_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (wid, parent_id, unit_id, kind, goal, "running", budget, time.time()),
        )
        self._conn.commit()
        return wid

    def close_work(self, wid: str, status: str, cost: float, value: float, result: Any, error: Optional[str]):
        self._conn.execute(
            "UPDATE work_items SET status=?,cost=?,value=?,ended_at=?,result=?,error=? WHERE id=?",
            (status, cost, value, time.time(), json.dumps(result, default=str)[:200000], error, wid),
        )
        self._conn.commit()

    def subtree(self, wid: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        frontier = [wid]
        while frontier:
            cur = frontier.pop()
            row = self._conn.execute("SELECT * FROM work_items WHERE id=?", (cur,)).fetchone()
            if row:
                out.append(dict(row))
            kids = self._conn.execute("SELECT id FROM work_items WHERE parent_id=?", (cur,)).fetchall()
            frontier.extend(k["id"] for k in kids)
        return out

    def reputation(self, unit_id: str) -> Dict[str, Any]:
        row = self._conn.execute(
            "SELECT COUNT(*) n, "
            "SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) ok, "
            "COALESCE(SUM(cost),0) cost, COALESCE(SUM(value),0) value "
            "FROM work_items WHERE unit_id=?",
            (unit_id,),
        ).fetchone()
        n = row["n"] or 0
        ok = row["ok"] or 0
        cost = row["cost"] or 0.0
        return {
            "unit_id": unit_id,
            "runs": n,
            "success_rate": round(ok / n, 3) if n else None,
            "total_cost": round(cost, 6),
            "total_value": round(row["value"] or 0.0, 6),
            "leverage": round((row["value"] or 0.0) / cost, 3) if cost > 0 else None,
        }

    def summary(self) -> Dict[str, Any]:
        row = self._conn.execute(
            "SELECT COUNT(*) n, COALESCE(SUM(cost),0) cost, COALESCE(SUM(value),0) value FROM work_items"
        ).fetchone()
        cost = row["cost"] or 0.0
        value = row["value"] or 0.0
        by_unit = self._conn.execute(
            "SELECT unit_id, COUNT(*) n, COALESCE(SUM(cost),0) cost, COALESCE(SUM(value),0) value "
            "FROM work_items GROUP BY unit_id"
        ).fetchall()
        return {
            "work_items": row["n"],
            "total_cost": round(cost, 6),
            "total_value": round(value, 6),
            "leverage": round(value / cost, 3) if cost > 0 else None,
            "by_unit": [
                {
                    "unit_id": r["unit_id"],
                    "items": r["n"],
                    "cost": round(r["cost"], 6),
                    "value": round(r["value"], 6),
                    "leverage": round(r["value"] / r["cost"], 3) if r["cost"] > 0 else None,
                }
                for r in by_unit
            ],
        }


class UnitFabricMCP(NativeMCPBase):
    SERVICE_ID = "distributed-unit"
    SERVICE_NAME = "Distributed Unit MCP"
    VERSION = "1.0.0"
    CAPABILITIES = ["unit", "agent", "team", "org", "work_graph", "ledger", "economics"]

    TOOLS = [
        {
            "name": "unit_register",
            "description": "Register a unit (agent or team). Agents call a registry tool; teams "
                           "delegate a goal to member units. Returns the unit id, now callable via unit_execute.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "description": "Unique unit id"},
                    "kind": {"type": "string", "enum": ["agent", "team"]},
                    "name": {"type": "string"},
                    "tool": {"type": "string", "description": "agent: registry tool to call (e.g. distributed-dsco-bridge:code_execute_python)"},
                    "goal_arg": {"type": "string", "description": "agent: which tool input the goal text fills (e.g. code, command, query)"},
                    "arg_map": {"type": "object", "description": "agent: static inputs merged into every call"},
                    "steps": {"type": "array", "items": {"type": "object"}, "description": "agent: multi-step pipeline [{tool, goal_arg, arg_map}]; each step's text output feeds the next via {{prev}}"},
                    "members": {"type": "array", "items": {"type": "string"}, "description": "team: member unit ids"},
                    "strategy": {"type": "string", "enum": ["map", "split"], "description": "team: map=same goal to all; split=distribute list/lines round-robin"},
                    "charter": {"type": "object", "description": "governance charter (inherited+intersected downward): allowed_tools, deny_tools, max_spend, max_depth, max_fanout, max_concurrency"},
                },
                "required": ["id", "kind"],
            },
        },
        {
            "name": "unit_execute",
            "description": "Execute any unit by id: execute(goal, budget, policy) -> {result, receipt}. "
                           "Recursively drives agents (real tool calls) and teams (member delegation), "
                           "enforcing the budget and recording the work-graph + receipts.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "unit_id": {"type": "string"},
                    "goal": {"type": "string", "description": "The goal/task text (or JSON list / newlines for split teams)"},
                    "budget": {"type": "number", "description": "Spend ceiling for this execution (credits). Default 1.0", "default": 1.0},
                    "value": {"type": "number", "description": "Asserted value of a successful outcome, for leverage accounting", "default": 0},
                },
                "required": ["unit_id", "goal"],
            },
        },
        {
            "name": "unit_list",
            "description": "List all registered units.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "work_graph",
            "description": "Return the work-item subtree (provenance/lineage) for a work id.",
            "inputSchema": {
                "type": "object",
                "properties": {"work_id": {"type": "string"}},
                "required": ["work_id"],
            },
        },
        {
            "name": "ledger_summary",
            "description": "Economic rollup across all work: total cost, value, and leverage (value/cost), per unit.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "unit_reputation",
            "description": "Ledger-derived reputation for a unit: run count, success rate, cost, value, leverage.",
            "inputSchema": {
                "type": "object",
                "properties": {"unit_id": {"type": "string"}},
                "required": ["unit_id"],
            },
        },
        {
            "name": "unit_charter",
            "description": "Render a unit's effective operating charter — the governance guide it runs under "
                           "(allowed tools, spend ceiling, depth/fanout/concurrency limits, structure).",
            "inputSchema": {
                "type": "object",
                "properties": {"unit_id": {"type": "string"}},
                "required": ["unit_id"],
            },
        },
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ledger = Ledger(os.path.join(str(self.data_dir), "unit_fabric.sqlite"))

    # ------------------------------------------------------------------ tools
    async def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if tool_name == "unit_register":
                return self._ok(self._register(arguments))
            if tool_name == "unit_execute":
                res = await self._execute_unit(
                    arguments["unit_id"],
                    arguments.get("goal", ""),
                    float(arguments.get("budget", 1.0)),
                    float(arguments.get("value", 0) or 0),
                    parent_id=None,
                    depth=0,
                    policy=Policy(),
                )
                return self._ok(res)
            if tool_name == "unit_list":
                return self._ok({"units": self.ledger.list_units()})
            if tool_name == "work_graph":
                return self._ok({"nodes": self.ledger.subtree(arguments["work_id"])})
            if tool_name == "ledger_summary":
                return self._ok(self.ledger.summary())
            if tool_name == "unit_reputation":
                return self._ok(self.ledger.reputation(arguments["unit_id"]))
            if tool_name == "unit_charter":
                return self._ok(self._charter(arguments["unit_id"]))
        except Exception as e:
            return {"content": [{"type": "text", "text": f"unit-fabric error: {e}"}], "isError": True}
        return await super()._execute_tool(tool_name, arguments)

    def _register(self, args: Dict[str, Any]) -> Dict[str, Any]:
        kind = args["kind"]
        unit = {
            "id": args["id"],
            "kind": kind,
            "name": args.get("name", args["id"]),
        }
        if args.get("charter"):
            unit["charter"] = args["charter"]
        if kind == "agent":
            unit["tool"] = args.get("tool")
            unit["goal_arg"] = args.get("goal_arg")
            unit["arg_map"] = args.get("arg_map", {})
            if args.get("steps"):
                unit["steps"] = args["steps"]
        else:
            unit["members"] = args.get("members", [])
            unit["strategy"] = args.get("strategy", "map")
        self.ledger.put_unit(unit)
        return {"registered": unit["id"], "kind": kind, "callable_as": "unit_execute"}

    def _charter(self, unit_id: str) -> Dict[str, Any]:
        unit = self.ledger.get_unit(unit_id)
        if not unit:
            raise ValueError(f"Unit not found: {unit_id}")
        eff = Policy.from_charter(unit.get("charter"))
        guide = {
            "unit_id": unit_id,
            "name": unit.get("name", unit_id),
            "kind": unit["kind"],
            "effective_policy": eff.as_dict(),
            "structure": {},
        }
        if unit["kind"] == "agent":
            guide["structure"] = {
                "tool": unit.get("tool"),
                "steps": [s.get("tool") for s in unit.get("steps", [])] or None,
            }
        else:
            guide["structure"] = {
                "strategy": unit.get("strategy"),
                "members": unit.get("members", []),
            }
        return guide

    async def _execute_unit(
        self, unit_id: str, goal: str, budget: float, value: float,
        parent_id: Optional[str], depth: int, policy: Policy,
    ) -> Dict[str, Any]:
        unit = self.ledger.get_unit(unit_id)
        if not unit:
            raise ValueError(f"Unit not found: {unit_id}")

        # Recursive governance: this unit runs under parent_policy ∩ its own charter.
        eff = policy.intersect(Policy.from_charter(unit.get("charter")))

        # Governance gate: depth and budget ceilings.
        if depth > eff.max_depth:
            wid = self.ledger.open_work(unit_id, unit["kind"], goal, budget, parent_id)
            self.ledger.close_work(wid, "policy_denied", 0.0, 0.0, None, f"max_depth {eff.max_depth} exceeded")
            return {"work_id": wid, "unit": unit_id, "status": "policy_denied", "cost": 0.0, "error": "max_depth exceeded"}
        if budget <= 0:
            wid = self.ledger.open_work(unit_id, unit["kind"], goal, budget, parent_id)
            self.ledger.close_work(wid, "budget_exceeded", 0.0, 0.0, None, "budget exhausted")
            return {"work_id": wid, "unit": unit_id, "status": "budget_exceeded", "cost": 0.0, "result": None}

        wid = self.ledger.open_work(unit_id, unit["kind"], goal, budget, parent_id)
        if unit["kind"] == "agent":
            return await self._run_agent(unit, goal, value, wid, eff)
        return await self._run_team(unit, goal, budget, value, wid, depth, eff)

    async def _call_tool(self, tool: str, inputs: Dict[str, Any]):
        t0 = time.time()
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{REGISTRY_URL}/api/v1/tools/{tool}/execute", json={"inputs": inputs})
        payload = resp.json()
        latency_ms = (time.time() - t0) * 1000
        cost = CALL_BASE_COST + latency_ms * CALL_MS_COST
        success = resp.status_code < 400 and payload.get("status") != "error"
        return payload, cost, latency_ms, success

    async def _run_agent(self, unit, goal, value, wid, eff: Policy) -> Dict[str, Any]:
        # An agent is a deterministic pipeline of >=1 tool steps; each step's text
        # output feeds the next via the {{prev}} placeholder.
        steps = unit.get("steps")
        if not steps:
            steps = [{"tool": unit.get("tool"), "goal_arg": unit.get("goal_arg"), "arg_map": unit.get("arg_map", {})}]

        total_cost = 0.0
        prev_text = goal
        last_payload: Any = None
        step_log: List[Dict[str, Any]] = []
        try:
            for i, step in enumerate(steps):
                tool = step["tool"]
                # Governance: every leaf tool call is checked against the effective policy.
                if not eff.tool_allowed(tool):
                    self.ledger.close_work(wid, "policy_denied", total_cost, 0.0, None, f"tool not allowed: {tool}")
                    return {"work_id": wid, "unit": unit["id"], "kind": "agent", "status": "policy_denied",
                            "cost": round(total_cost, 6), "error": f"tool not allowed by charter: {tool}"}
                inputs = dict(step.get("arg_map") or {})
                ga = step.get("goal_arg")
                step_goal = (goal if i == 0 else prev_text)
                if ga:
                    inputs[ga] = step_goal.replace("{{prev}}", prev_text)
                payload, cost, latency_ms, success = await self._call_tool(tool, inputs)
                total_cost += cost
                last_payload = payload
                prev_text = self._extract_text(payload)
                step_log.append({"tool": tool, "status": "ok" if success else "error",
                                  "cost": round(cost, 6), "latency_ms": round(latency_ms, 2)})
                if not success:
                    self.ledger.close_work(wid, "failed", total_cost, 0.0, payload, payload.get("error"))
                    return {"work_id": wid, "unit": unit["id"], "kind": "agent", "status": "failed",
                            "cost": round(total_cost, 6), "steps": step_log, "result": payload}

            self.ledger.close_work(wid, "completed", total_cost, value, last_payload, None)
            return {
                "work_id": wid, "unit": unit["id"], "kind": "agent",
                "status": "completed", "cost": round(total_cost, 6), "steps": step_log,
                "result": last_payload.get("output", last_payload) if isinstance(last_payload, dict) else last_payload,
                "receipt": {"unit": unit["id"], "cost": round(total_cost, 6), "value": value, "steps": len(step_log)},
            }
        except Exception as e:
            total_cost += CALL_BASE_COST
            self.ledger.close_work(wid, "failed", total_cost, 0.0, None, str(e))
            return {"work_id": wid, "unit": unit["id"], "kind": "agent", "status": "failed",
                    "cost": round(total_cost, 6), "error": str(e)}

    async def _run_team(self, unit, goal, budget, value, wid, depth, eff: Policy) -> Dict[str, Any]:
        members = unit.get("members", [])
        strategy = unit.get("strategy", "map")
        if not members:
            self.ledger.close_work(wid, "failed", 0.0, 0.0, None, "team has no members")
            return {"work_id": wid, "status": "failed", "error": "team has no members"}

        # Decompose the goal into per-member sub-goals.
        if strategy == "split":
            items = self._split_goal(goal)
            assignments = [(members[i % len(members)], items[i]) for i in range(len(items))]
        else:  # map
            assignments = [(m, goal) for m in members]

        # Governance: enforce fanout ceiling.
        if len(assignments) > eff.max_fanout:
            assignments = assignments[: eff.max_fanout]

        # Scheduling: run members concurrently, bounded by the effective concurrency cap.
        per = budget / max(len(assignments), 1)
        sem = asyncio.Semaphore(max(eff.max_concurrency, 1))

        async def _one(member_id: str, subgoal: str):
            async with sem:
                return await self._execute_unit(member_id, subgoal, per, value, wid, depth + 1, eff)

        children = list(await asyncio.gather(*[_one(m, g) for m, g in assignments]))

        total_cost = sum(float(c.get("cost", 0.0)) for c in children)
        total_value = sum(
            float(c.get("receipt", {}).get("value", 0.0) or 0.0)
            for c in children if c.get("status") == "completed"
        )
        ok = any(c.get("status") == "completed" for c in children)
        # Bubble up why a team failed (e.g. every member policy_denied).
        from collections import Counter
        statuses = Counter(c.get("status") for c in children)
        fail_reason = None if ok else "members " + ", ".join(f"{n}×{s}" for s, n in statuses.items())
        self.ledger.close_work(
            wid, "completed" if ok else "failed", total_cost, total_value,
            {"children": [c.get("work_id") for c in children]}, fail_reason,
        )
        result = {
            "work_id": wid, "unit": unit["id"], "kind": "team", "strategy": strategy,
            "status": "completed" if ok else "failed",
            "cost": round(total_cost, 6), "concurrency": min(eff.max_concurrency, len(assignments)),
            "receipt": {"unit": unit["id"], "cost": round(total_cost, 6), "value": round(total_value, 6),
                        "leverage": round(total_value / total_cost, 3) if total_cost > 0 else None,
                        "members": len(children)},
            "children": children,
        }
        if fail_reason:
            result["error"] = fail_reason
        return result

    @staticmethod
    def _extract_text(payload: Any) -> str:
        """Pull a usable text output from a registry execute payload for step chaining."""
        if not isinstance(payload, dict):
            return str(payload)
        out = payload.get("output", payload)
        if isinstance(out, dict):
            content = out.get("content")
            if isinstance(content, list) and content and isinstance(content[0], dict):
                return str(content[0].get("text", ""))
            return json.dumps(out, default=str)
        return str(out)

    @staticmethod
    def _split_goal(goal: str) -> List[str]:
        s = goal.strip()
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(x) for x in arr]
            except Exception:
                pass
        lines = [ln for ln in s.splitlines() if ln.strip()]
        return lines if lines else [s]

    @staticmethod
    def _ok(payload: Any) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps(payload, indent=2, default=str)}], "isError": False}


def create_server(port: Optional[int] = None) -> UnitFabricMCP:
    port = port or int(os.environ.get("PORT", "4008"))
    return UnitFabricMCP(port=port)


# Module-level ASGI app for Modal @asgi_app import (factory target)
app = create_server().app


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unit Fabric MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4008")))
    args = parser.parse_args()
    create_server(args.port).run()
