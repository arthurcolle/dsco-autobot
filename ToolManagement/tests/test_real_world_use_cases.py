import json
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes
from tool_management.persistence.execution_store import ExecutionStore


class _UseCaseToolStore:
    def resolve_tool(self, tool_name: str):
        resolved_name = tool_name.split(":", 1)[-1]
        return "distributed-search-mcp", "https://example.com", resolved_name

    def get_tool_latency_metrics(self, tool_name: str, hours: int = 24):
        if "deep_search" in tool_name:
            return {"p95_latency_ms": 20000.0, "success_rate": 0.96, "calls": 40}
        return {"p95_latency_ms": 1200.0, "success_rate": 0.99, "calls": 120}

    def get_tool_by_name(self, tool_ref: str):
        name = tool_ref.split(":", 1)[-1]
        if "deep_search" in name:
            return SimpleNamespace(avg_latency_ms=22000.0)
        if "search" in name:
            return SimpleNamespace(avg_latency_ms=900.0)
        return SimpleNamespace(avg_latency_ms=2500.0)


def _write_catalog(path):
    payload = {
        "version": "test-catalog-v1",
        "use_cases": [
            {
                "use_case_id": "market_brief",
                "title": "Market Brief",
                "domain": "market_intelligence",
                "summary": "Build a strategic market brief",
                "objective": "Produce a daily market brief",
                "tags": ["market", "briefing"],
                "outcomes": ["brief", "memory"],
                "data_assets": ["search_results", "brief_records"],
                "economic_hooks": ["max_total_cost_cents", "max_reintegration_cost"],
                "risk_controls": ["admission", "followups"],
                "defaults": {"query": "distributed systems", "num_results": 6},
                "default_template": "default",
                "templates": {
                    "default": {
                        "execution": "parallel",
                        "dedupe_calls": True,
                        "auto_tune_parallel": True,
                        "max_parallel": 4,
                        "max_total_cost_cents": 500,
                        "max_reintegration_cost": 40.0,
                        "reintegration_overflow_policy": "truncate",
                        "calls": [
                            {
                                "tool_name": "search",
                                "execution_mode": "auto",
                                "arguments": {
                                    "query": "{{query}}",
                                    "num_results": "{{num_results}}"
                                }
                            },
                            {
                                "tool_name": "synthesize",
                                "execution_mode": "auto",
                                "arguments": {
                                    "topic": "{{objective}}"
                                }
                            }
                        ]
                    },
                    "strict_missing": {
                        "execution": "sequential",
                        "max_total_cost_cents": 120,
                        "calls": [
                            {
                                "tool_name": "search",
                                "execution_mode": "auto",
                                "arguments": {
                                    "query": "{{missing_signal}}"
                                }
                            }
                        ]
                    }
                }
            },
            {
                "use_case_id": "ops_watch",
                "title": "Ops Watch",
                "domain": "operations",
                "summary": "Live operations monitoring",
                "templates": {
                    "default": {
                        "execution": "parallel",
                        "max_total_cost_cents": 120,
                        "calls": [
                            {
                                "tool_name": "search",
                                "execution_mode": "auto",
                                "arguments": {
                                    "query": "service incident"
                                }
                            }
                        ]
                    }
                }
            }
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build_client(tmp_path, monkeypatch):
    catalog_path = tmp_path / "use_case_catalog.json"
    _write_catalog(catalog_path)
    monkeypatch.setenv("EXECUTION_USE_CASE_CATALOG_PATH", str(catalog_path))

    store = ExecutionStore(db_path=str(tmp_path / "execution_store.db"))
    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _UseCaseToolStore())

    app = FastAPI()
    app.include_router(execution_routes.router)
    return TestClient(app)


def test_use_case_catalog_list_and_filter(tmp_path, monkeypatch):
    client = _build_client(tmp_path, monkeypatch)

    all_cases = client.get("/api/v1/use-cases", params={"limit": 10})
    assert all_cases.status_code == 200
    payload = all_cases.json()
    assert payload["version"] == "test-catalog-v1"
    assert payload["count"] == 2

    filtered = client.get(
        "/api/v1/use-cases",
        params={"domain": "market_intelligence", "q": "daily brief", "require_economic_hooks": True},
    )
    assert filtered.status_code == 200
    filtered_payload = filtered.json()
    assert filtered_payload["count"] == 1
    use_case = filtered_payload["use_cases"][0]
    assert use_case["use_case_id"] == "market_brief"
    assert "default" in use_case["template_names"]
    assert use_case["fit_score"] > 1.0


def test_use_case_detail_and_plan_renders_templates(tmp_path, monkeypatch):
    client = _build_client(tmp_path, monkeypatch)

    detail = client.get("/api/v1/use-cases/market_brief")
    assert detail.status_code == 200
    detail_payload = detail.json()
    assert detail_payload["use_case"]["domain"] == "market_intelligence"

    planned = client.post(
        "/api/v1/use-cases/market_brief/plan",
        json={
            "objective": "Summarize capital rotation",
            "overrides": {"query": "AI infrastructure funding", "num_results": 4},
            "max_total_cost_cents": 180,
            "max_reintegration_cost": 9.0,
        },
    )
    assert planned.status_code == 200
    plan_payload = planned.json()

    assert plan_payload["use_case_id"] == "market_brief"
    assert plan_payload["admitted"] in (True, False)
    assert plan_payload["rendered_batch"]["calls"][0]["arguments"]["query"] == "AI infrastructure funding"
    assert plan_payload["rendered_batch"]["calls"][0]["arguments"]["num_results"] == 4
    assert plan_payload["rendered_batch"]["calls"][1]["arguments"]["topic"] == "Summarize capital rotation"
    assert plan_payload["plan"]["estimated_total_cost_cents"] > 0


def test_use_case_plan_rejects_unresolved_placeholders(tmp_path, monkeypatch):
    client = _build_client(tmp_path, monkeypatch)

    response = client.post(
        "/api/v1/use-cases/market_brief/plan",
        json={
            "template": "strict_missing",
            "strict_placeholders": True,
        },
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["code"] == "USE_CASE_PLACEHOLDER_UNRESOLVED"


def test_use_case_execute_runs_batch_with_rendered_payload(tmp_path, monkeypatch):
    client = _build_client(tmp_path, monkeypatch)

    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"ok": True, "count": calls_seen["count"]},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    executed = client.post(
        "/api/v1/use-cases/market_brief/execute",
        json={
            "objective": "Assess hyperscaler capex",
            "overrides": {"query": "hyperscaler capex outlook", "num_results": 5},
            "execution": "parallel",
            "max_total_cost_cents": 200,
            "max_reintegration_cost": 10.0,
            "reintegration_overflow_policy": "truncate",
        },
    )
    assert executed.status_code == 200
    payload = executed.json()

    assert payload["use_case_id"] == "market_brief"
    assert payload["rendered_batch"]["calls"][0]["arguments"]["query"] == "hyperscaler capex outlook"
    assert payload["execution"]["total"] == 2
    assert payload["execution"]["failed"] == 0
    assert calls_seen["count"] == 2
