import asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes


class _FakePolicyToolStore:
    def resolve_tool(self, tool_name: str):
        resolved_name = tool_name.split(":", 1)[-1]
        return "distributed-search-mcp", "https://example.com", resolved_name

    def get_tool_latency_metrics(self, tool_name: str, hours: int = 24):
        if "deep_search" in tool_name:
            return {"p95_latency_ms": 20000.0, "success_rate": 0.95, "calls": 15}
        return {"p95_latency_ms": 1200.0, "success_rate": 0.99, "calls": 60}

    def get_tool_by_name(self, tool_ref: str):
        # Keep latency-class lookup simple for tests.
        return None


def test_execute_admission_check_reports_budget_exceeded(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _FakePolicyToolStore())

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/execute/admission-check",
        json={
            "tool_name": "deep_search",
            "arguments": {"query": "autonomous agents", "num_results": 25},
            "execution_mode": "job",
            "stream": False,
            "max_cost_cents": 5,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["admitted"] is False
    assert "cost_budget_exceeded" in payload["reasons"]
    assert payload["estimated_cost_cents"] > 5
    assert payload["selected_mode"] in {"job", "sync", "stream"}


def test_execute_batch_plan_surfaces_reintegration_pressure(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _FakePolicyToolStore())

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/execute/batch/plan",
        json={
            "execution": "parallel",
            "max_reintegration_cost": 4.0,
            "reintegration_overflow_policy": "truncate",
            "calls": [
                {"tool_name": "search", "arguments": {"q": "dc weather", "num_results": 12}},
                {"tool_name": "search", "arguments": {"q": "dc weather alerts", "num_results": 12}},
                {"tool_name": "deep_search", "arguments": {"q": "macro trends", "num_results": 20}},
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["admitted"] is False
    assert "reintegration_budget_exceeded" in payload["reasons"]
    plan = payload["plan"]
    assert plan["estimated_reintegration_cost"] > 0
    assert plan["reintegration_overflowed_calls_if_truncate"] >= 1
    assert "queue_pressure" in plan


def test_execute_batch_reintegration_budget_truncate_skips_calls(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _FakePolicyToolStore())
    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        await asyncio.sleep(0)
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"ok": True, "call": calls_seen["count"]},
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
            "reintegration_overflow_policy": "truncate",
            "max_reintegration_cost": 5.0,
            "dedupe_calls": False,
            "calls": [
                {"tool_name": "search", "arguments": {"q": "one", "num_results": 20}},
                {"tool_name": "search", "arguments": {"q": "two", "num_results": 20}},
                {"tool_name": "deep_search", "arguments": {"q": "three", "num_results": 20}},
                {"tool_name": "deep_search", "arguments": {"q": "four", "num_results": 20}},
            ],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["batch_plan"]["reintegration_overflowed_calls"] >= 1
    skipped = [
        row for row in data["results"]
        if row["status"] == "skipped" and row.get("error", {}).get("code") == "BATCH_REINTEGRATION_BUDGET_OVERFLOW"
    ]
    assert skipped
    assert calls_seen["count"] == data["batch_plan"]["executed_canonical_calls"]


def test_execute_batch_total_cost_budget_rejects_without_execution(monkeypatch):
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _FakePolicyToolStore())
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

    response = client.post(
        "/api/v1/execute/batch",
        json={
            "execution": "sequential",
            "max_total_cost_cents": 150,
            "reintegration_overflow_policy": "reject",
            "calls": [
                {"tool_name": "search", "estimated_cost_cents": 120, "arguments": {"q": "a"}},
                {"tool_name": "search", "estimated_cost_cents": 90, "arguments": {"q": "b"}},
            ],
        },
    )
    assert response.status_code == 402
    detail = response.json()["detail"]
    assert detail["code"] == "BATCH_TOTAL_COST_BUDGET_EXCEEDED"
    assert calls_seen["count"] == 0
