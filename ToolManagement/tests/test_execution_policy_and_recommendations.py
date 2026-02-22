import asyncio
import time
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes, tools_api
from tool_management.api.tools_api import ToolResult
from tool_management.persistence.execution_store import ExecutionStore


class _FakePolicyToolStore:
    def __init__(self, p95_latency_ms: float = 1200.0):
        self._p95 = p95_latency_ms

    def resolve_tool(self, tool_name: str):
        resolved_name = tool_name.split(":", 1)[-1]
        return "distributed-search-mcp", "https://example.com", resolved_name

    def get_tool_latency_metrics(self, tool_name: str, hours: int = 24):
        return {
            "p95_latency_ms": self._p95,
            "success_rate": 0.98,
            "calls": 30,
        }


def _build_execution_client(tmp_path, monkeypatch, *, p95_latency_ms: float = 1200.0):
    store = ExecutionStore(db_path=str(tmp_path / "execution_policy.db"))
    fake_tool_store = _FakePolicyToolStore(p95_latency_ms=p95_latency_ms)

    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: fake_tool_store)

    async def fake_sync(**kwargs):
        return ToolResult(
            tool=kwargs["tool_name"],
            backend="distributed-search-mcp",
            success=True,
            result={"ok": True, "tool": kwargs["tool_name"]},
            latency_ms=15.0,
        )

    async def fake_submit_job(**kwargs):
        return SimpleNamespace(
            execution_id="job_42",
            trace_id=kwargs.get("trace_id"),
            session_id=kwargs.get("session_id"),
            parent_execution_id=kwargs.get("parent_execution_id"),
            queued_at=time.time(),
            queue_priority=1,
        )

    monkeypatch.setattr(execution_routes, "_execute_sync", fake_sync)
    monkeypatch.setattr(execution_routes, "_submit_job", fake_submit_job)

    app = FastAPI()
    app.include_router(execution_routes.router)
    return TestClient(app), store


def test_execute_auto_persists_policy_decision(tmp_path, monkeypatch):
    client, store = _build_execution_client(tmp_path, monkeypatch)
    response = client.post(
        "/api/v1/execute",
        json={
            "tool_name": "search",
            "arguments": {"query": "dc weather"},
            "execution_mode": "auto",
            "stream": False,
            "policy_constraints": {
                "allow_stream": False,
                "allow_job": False,
            },
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "completed"
    assert data["execution_mode"] == "sync"
    assert data["policy_decision"]["selected_mode"] == "sync"

    listed = client.get("/api/v1/policy-decisions", params={"hours": 24, "limit": 10})
    assert listed.status_code == 200
    payload = listed.json()
    assert payload["count"] >= 1
    assert payload["decisions"][0]["tool_name"] == "search"

    analytics = client.get("/api/v1/policy-decisions/analytics", params={"hours": 24})
    assert analytics.status_code == 200
    analytics_data = analytics.json()
    assert analytics_data["count"] >= 1
    assert "selected_mode_mix" in analytics_data

    # Store APIs should also expose the persisted policy row.
    persisted = store.list_policy_decisions(hours=24, limit=10)
    assert persisted
    assert persisted[0]["decision_id"] == data["policy_decision"]["decision_id"]


def test_execute_auto_sync_timeout_escalates_to_job(tmp_path, monkeypatch):
    client, store = _build_execution_client(tmp_path, monkeypatch)

    async def slow_sync(**kwargs):
        await asyncio.sleep(0.25)
        return ToolResult(
            tool=kwargs["tool_name"],
            backend="distributed-search-mcp",
            success=True,
            result={"ok": True},
            latency_ms=50.0,
        )

    monkeypatch.setattr(execution_routes, "_execute_sync", slow_sync)

    response = client.post(
        "/api/v1/execute",
        json={
            "tool_name": "search",
            "arguments": {"query": "latency test"},
                "execution_mode": "auto",
                "stream": False,
                "sync_timeout_ms": 100,
                "policy_constraints": {
                    "allow_stream": False,
                    "allow_job": True,
                },
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "submitted"
    assert data["execution_mode"] == "job"
    assert data["error"]["code"] == "SYNC_TIMEOUT_ESCALATED"
    assert data["job_id"] == "job_42"
    assert data["policy_decision"]["selected_mode"] == "job"

    persisted = store.list_policy_decisions(hours=24, limit=10)
    assert persisted
    assert persisted[0]["selected_mode"] == "job"
    assert persisted[0]["execution_id"] == "job_42"


class _FakeRecommendationStore:
    def query_tools(self, **kwargs):
        rows = [
            {
                "tool_id": "distributed-search-mcp:search",
                "name": "search",
                "backend_id": "distributed-search-mcp",
                "category": "search",
                "description": "Search the web for recent information",
                "call_count": 120,
                "error_count": 2,
                "avg_latency_ms": 950.0,
            },
            {
                "tool_id": "distributed-research-mcp:deep_search",
                "name": "deep_search",
                "backend_id": "distributed-research-mcp",
                "category": "research",
                "description": "Run deep multi-source research analysis",
                "call_count": 35,
                "error_count": 4,
                "avg_latency_ms": 18000.0,
            },
            {
                "tool_id": "distributed-memory-mcp:store_belief",
                "name": "store_belief",
                "backend_id": "distributed-memory-mcp",
                "category": "memory",
                "description": "Store a memory belief record",
                "call_count": 80,
                "error_count": 1,
                "avg_latency_ms": 400.0,
            },
        ]
        return rows, len(rows)

    def list_tool_dependencies(self, **kwargs):
        return [
            {
                "source_tool_id": "distributed-search-mcp:search",
                "source_name": "search",
                "target_tool_id": "distributed-research-mcp:deep_search",
                "target_name": "deep_search",
                "dependency_type": "inferred",
                "confidence": 0.72,
                "call_count": 11,
                "metadata": {},
            }
        ]


def test_recommendation_and_feedback_endpoints(tmp_path, monkeypatch):
    execution_store = ExecutionStore(db_path=str(tmp_path / "recommendations.db"))
    monkeypatch.setattr(tools_api, "get_tool_store", lambda: _FakeRecommendationStore())
    monkeypatch.setattr(tools_api, "get_execution_store", lambda: execution_store)

    app = FastAPI()
    app.include_router(tools_api.router)
    client = TestClient(app)

    recommendation = client.post(
        "/api/v1/tools/recommendations",
        json={
            "query": "search weather updates",
            "intent": "query",
            "require_streaming": False,
            "limit": 2,
        },
    )
    assert recommendation.status_code == 200
    rec_data = recommendation.json()
    assert rec_data["count"] == 2
    assert rec_data["recommendations"][0]["name"] == "search"
    rec_id = rec_data["recommendation_id"]

    feedback = client.post(
        "/api/v1/tools/recommendations/feedback",
        json={
            "recommendation_id": rec_id,
            "chosen_tool": rec_data["recommendations"][0]["tool_id"],
            "accepted": True,
            "outcome_status": "completed",
            "latency_ms": 920.0,
            "context": {"task": "weather_lookup"},
            "recommendation": rec_data,
            "metadata": {"source": "unit_test"},
        },
    )
    assert feedback.status_code == 200
    feedback_payload = feedback.json()
    assert feedback_payload["status"] == "recorded"
    assert feedback_payload["feedback"]["accepted"] is True

    analytics = client.get("/api/v1/tools/recommendations/analytics", params={"hours": 24})
    assert analytics.status_code == 200
    analytics_payload = analytics.json()
    assert analytics_payload["totals"]["feedback"] == 1
    assert analytics_payload["totals"]["accepted"] == 1
