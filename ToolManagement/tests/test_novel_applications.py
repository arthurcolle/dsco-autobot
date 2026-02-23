import asyncio
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes
from tool_management.api.tools_api import ToolResult
from tool_management.persistence.execution_store import ExecutionStore


class _NovelToolStore:
    def resolve_tool(self, tool_name: str):
        resolved_name = tool_name.split(":", 1)[-1]
        return "distributed-search-mcp", "https://example.com", resolved_name

    def get_tool_latency_metrics(self, tool_name: str, hours: int = 24):
        if "deep_search" in tool_name:
            return {"p95_latency_ms": 22000.0, "success_rate": 0.94, "calls": 40}
        if "search" in tool_name:
            return {"p95_latency_ms": 900.0, "success_rate": 0.99, "calls": 200}
        return {"p95_latency_ms": 2800.0, "success_rate": 0.97, "calls": 60}

    def get_tool_by_name(self, tool_ref: str):
        name = tool_ref.split(":", 1)[-1]
        if "deep_search" in name:
            return SimpleNamespace(avg_latency_ms=20000.0)
        if "search" in name:
            return SimpleNamespace(avg_latency_ms=900.0)
        return SimpleNamespace(avg_latency_ms=3000.0)


def test_novel_autonomous_research_market_generates_budget_followups(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "novel_market.db"))
    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())
    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"ok": True},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    calls = []
    for idx in range(30):
        tool = "deep_search" if idx % 3 == 0 else "search"
        calls.append(
            {
                "tool_name": tool,
                "execution_mode": "job" if tool == "deep_search" else "auto",
                "arguments": {"q": f"agent-market-{idx}", "num_results": 20},
            }
        )

    response = client.post(
        "/api/v1/execute/batch",
        json={
            "execution": "parallel",
            "dedupe_calls": False,
            "max_reintegration_cost": 6.0,
            "reintegration_overflow_policy": "truncate",
            "auto_follow_up": True,
            "calls": calls,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["batch_plan"]["reintegration_overflowed_calls"] > 0
    assert payload["batch_plan"]["total_calls"] == 30
    assert payload["batch_plan"]["estimated_reintegration_cost"] > 0
    assert payload["batch_plan"]["executed_canonical_calls"] == calls_seen["count"]

    action_types = {row["action_type"] for row in payload["follow_up_actions"]}
    assert "reintegration_budget_resegment" in action_types
    assert "tighten_reintegration_policy" in action_types


def test_novel_queue_pressure_auto_tunes_parallelism(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())
    pressure_executor = SimpleNamespace(
        get_stats=lambda: {
            "active_count": 140,
            "queue_depth": 190,
            "max_concurrent": 16,
            "max_pending": 200,
            "max_inflight": 400,
        }
    )
    monkeypatch.setattr(execution_routes, "get_streaming_executor", lambda: pressure_executor)
    inflight = {"count": 0, "max": 0}

    async def fake_execute(request):
        inflight["count"] += 1
        inflight["max"] = max(inflight["max"], inflight["count"])
        await asyncio.sleep(0.02)
        inflight["count"] -= 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"ok": True},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/execute/batch",
        json={
            "execution": "parallel",
            "max_parallel": 12,
            "auto_tune_parallel": True,
            "min_parallel": 1,
            "max_reintegration_cost": 500.0,
            "reintegration_overflow_policy": "truncate",
            "calls": [
                {"tool_name": "search", "arguments": {"q": f"pressure-{idx}", "num_results": 5}}
                for idx in range(12)
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    # Pressure-aware tuner should force conservative parallelism.
    assert payload["batch_plan"]["effective_parallel"] == 2
    assert inflight["max"] <= 2


def test_novel_admission_to_execution_economic_flow(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())

    async def fake_sync(**kwargs):
        return ToolResult(
            tool=kwargs["tool_name"],
            backend="distributed-search-mcp",
            success=True,
            result={"ok": True, "tool": kwargs["tool_name"]},
            latency_ms=24.0,
        )

    monkeypatch.setattr(execution_routes, "_execute_sync", fake_sync)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    admission = client.post(
        "/api/v1/execute/admission-check",
        json={
            "tool_name": "search",
            "arguments": {"query": "dc weather", "num_results": 5},
            "execution_mode": "auto",
            "stream": False,
            "max_cost_cents": 25,
        },
    )
    assert admission.status_code == 200
    admission_payload = admission.json()
    assert admission_payload["admitted"] is True
    assert admission_payload["estimated_cost_cents"] <= 25

    executed = client.post(
        "/api/v1/execute",
        json={
            "tool_name": "search",
            "arguments": {"query": "dc weather", "num_results": 5},
            "execution_mode": "auto",
            "stream": False,
            "max_cost_cents": 25,
        },
    )
    assert executed.status_code == 200
    execute_payload = executed.json()
    assert execute_payload["status"] == "completed"
    assert execute_payload["estimated_cost_cents"] <= 25
    assert execute_payload["policy_decision"]["estimated_cost_cents"] == execute_payload["estimated_cost_cents"]


def test_novel_batch_plan_preflight_surfaces_cost_and_pressure(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())
    monkeypatch.setattr(
        execution_routes,
        "get_streaming_executor",
        lambda: SimpleNamespace(
            get_stats=lambda: {
                "active_count": 10,
                "queue_depth": 60,
                "max_concurrent": 12,
                "max_pending": 64,
                "max_inflight": 120,
            }
        ),
    )

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/execute/batch/plan",
        json={
            "execution": "parallel",
            "max_total_cost_cents": 90,
            "max_reintegration_cost": 8.0,
            "reintegration_overflow_policy": "truncate",
            "calls": [
                {"tool_name": "search", "arguments": {"q": "x", "num_results": 8}},
                {"tool_name": "deep_search", "arguments": {"q": "y", "num_results": 15}},
                {"tool_name": "deep_search", "arguments": {"q": "z", "num_results": 15}},
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["plan"]["estimated_total_cost_cents"] > 0
    assert payload["plan"]["estimated_reintegration_cost"] > 0
    assert payload["plan"]["effective_parallel"] <= 3
    assert payload["plan"]["queue_pressure"]["queue_pressure_ratio"] > 0


def test_novel_anti_thundering_herd_generates_and_persists_batch_followups(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "novel_followups.db"))
    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())

    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"ok": True, "idx": calls_seen["count"]},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)
    trace_id = "tr_novel_herd"

    response = client.post(
        "/api/v1/execute/batch",
        json={
            "trace_id": trace_id,
            "execution": "parallel",
            "dedupe_calls": False,
            "max_canonical_calls": 100,
            "overflow_policy": "truncate",
            "reintegration_overflow_policy": "truncate",
            "max_reintegration_cost": 5000.0,
            "auto_follow_up": True,
            "calls": [
                {"tool_name": "search", "arguments": {"q": f"large-batch-{idx}", "num_results": 10}}
                for idx in range(130)
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    plan = payload["batch_plan"]

    assert plan["total_calls"] == 130
    assert plan["canonical_calls"] == 130
    assert plan["overflowed_canonical_calls"] == 30
    assert plan["executed_canonical_calls"] == 100
    assert calls_seen["count"] == 100

    action_types = {row["action_type"] for row in payload["follow_up_actions"]}
    assert "enable_batch_preflight_dedupe" in action_types
    assert "set_batch_canonical_cap" in action_types
    assert "resegment_batch_inputs" in action_types

    listed = client.get("/api/v1/follow-ups", params={"trace_id": trace_id, "limit": 100})
    assert listed.status_code == 200
    listed_types = {row["action_type"] for row in listed.json()["actions"]}
    assert "enable_batch_preflight_dedupe" in listed_types
    assert "set_batch_canonical_cap" in listed_types
    assert "resegment_batch_inputs" in listed_types


def test_novel_cost_truncation_prefers_high_leverage_calls(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())
    executed_tools = []

    async def fake_execute(request):
        tool = request.tool_name or request.tool or "unknown"
        executed_tools.append(tool)
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=tool,
            execution_mode=request.execution_mode,
            result={"ok": True, "tool": tool},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    calls = []
    calls.extend(
        [
            {"tool_name": "search", "estimated_cost_cents": 40, "arguments": {"q": "shared-a"}}
            for _ in range(4)
        ]
    )
    calls.extend(
        [
            {"tool_name": "search", "estimated_cost_cents": 35, "arguments": {"q": "shared-b"}}
            for _ in range(3)
        ]
    )
    calls.append({"tool_name": "deep_search", "estimated_cost_cents": 120, "arguments": {"q": "expensive-a"}})
    calls.append({"tool_name": "deep_search", "estimated_cost_cents": 80, "arguments": {"q": "expensive-b"}})

    response = client.post(
        "/api/v1/execute/batch",
        json={
            "execution": "sequential",
            "dedupe_calls": True,
            "max_total_cost_cents": 90,
            "reintegration_overflow_policy": "truncate",
            "calls": calls,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    plan = payload["batch_plan"]

    assert plan["canonical_calls"] == 4
    assert plan["executed_canonical_calls"] == 2
    assert plan["cost_overflowed_calls"] == 2
    assert len(executed_tools) == 2
    assert set(executed_tools) == {"search"}

    completed = [row for row in payload["results"] if row["status"] == "completed"]
    skipped = [row for row in payload["results"] if row["status"] == "skipped"]
    assert len(completed) == 7
    assert len(skipped) == 2
    assert all((row.get("error") or {}).get("code") == "BATCH_TOTAL_COST_BUDGET_OVERFLOW" for row in skipped)


def test_novel_admission_rejects_under_critical_backpressure(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _NovelToolStore())
    monkeypatch.setattr(
        execution_routes,
        "get_streaming_executor",
        lambda: SimpleNamespace(
            get_stats=lambda: {
                "active_count": 90,
                "queue_depth": 99,
                "max_concurrent": 16,
                "max_pending": 100,
                "max_inflight": 190,
            }
        ),
    )

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/execute/admission-check",
        json={
            "tool_name": "search",
            "arguments": {"query": "backpressure scenario", "num_results": 6},
            "execution_mode": "auto",
            "stream": False,
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["admitted"] is False
    assert "queue_backpressure_high" in payload["reasons"]
    assert "inflight_pressure_high" in payload["reasons"]
    assert payload["queue_pressure"]["queue_pressure_ratio"] >= 0.98
    assert payload["queue_pressure"]["inflight_pressure_ratio"] >= 0.98
