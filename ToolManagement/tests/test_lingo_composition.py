"""Opt-in profile contracts, real SQLite reservation, adversarial MCP transport.

Transport fixtures are not live backend execution proof. The CLI release harness
separately exercises the actual service and an actual repository worker.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import composition_routes as routes
from tool_management.api import lingo_composition as profile
from tool_management.persistence.execution_store import ExecutionStore


def plan():
    return {
        "execution_profile": "lingo.v1", "idempotency_key": "release.1",
        "workflow": {"id": "release.evidence", "name": "Release evidence", "version": "1.0",
                     "timeout_seconds": 2, "max_parallel_steps": 1,
                     "steps": [{"id": "collect", "name": "collect", "mode": "passthrough", "tool": "repo:collect"},
                               {"id": "seal", "name": "seal", "mode": "passthrough", "tool": "repo:seal",
                                "input_from": "collect", "input_mapping": {"evidence": "receipt"}}]},
        "inputs": {"revision": "source-sha"},
    }


@pytest.fixture
def service(tmp_path, monkeypatch):
    store = ExecutionStore(str(tmp_path / "executions.sqlite"))
    monkeypatch.setattr(routes, "get_execution_store", lambda: store)
    monkeypatch.setenv("REQUIRE_BEARER_AUTH", "0")
    calls = []
    mode = {"name": "success"}

    class Catalog:
        def resolve_tool(self, tool):
            backend, name = tool.split(":", 1)
            return backend, "http://127.0.0.1:5555", name

        def get_tool_by_name(self, name, backend_id=None):
            return None if name == "unregistered" else SimpleNamespace(name=name)

    monkeypatch.setattr("tool_management.persistence.get_tool_store", lambda: Catalog())
    monkeypatch.setattr("tool_management.api.tools_api._backend_headers", lambda backend_id: {})
    actual_async_client = httpx.AsyncClient

    async def handle(request):
        payload = json.loads(request.content)
        calls.append(payload)
        name, args = payload["params"]["name"], payload["params"]["arguments"]
        output = {"receipt": {"revision": args.get("revision", "source-sha"), "verified": True}} if name == "collect" else args
        result = {"jsonrpc": "2.0", "id": payload["id"],
                  "result": {"isError": False, "content": [{"type": "text", "text": json.dumps(output)}]}}
        selected = mode["name"]
        if selected == "timeout":
            await asyncio.sleep(0.2)
        elif selected == "array":
            result["result"]["content"][0]["text"] = json.dumps(mode.get("items", [{"n": 1}, {"n": 2}]) if name == "collect" else args)
        elif selected == "mcp_error":
            result["result"]["isError"] = True
        elif selected == "protocol_error":
            result = {"jsonrpc": "2.0", "id": payload["id"], "error": {"code": -32000, "message": "rejected"}}
        elif selected == "wrong_id":
            result["id"] = "unrelated"
        elif selected == "invalid_error_type":
            result["result"]["isError"] = "false"
        elif selected == "plain_text":
            result["result"]["content"][0]["text"] = "looks good"
        elif selected == "duplicate_result_json":
            result["result"]["content"][0]["text"] = '{"verified":true,"verified":false}'
        elif selected == "unsafe_number":
            result["result"]["content"][0]["text"] = '{"n":9007199254740993}'
        elif selected in ("soft_error", "false_ok", "failed_status"):
            output = {"error": "rejected"} if selected == "soft_error" else ({"ok": False} if selected == "false_ok" else {"status": "failed"})
            result["result"]["content"][0]["text"] = json.dumps(output)
        elif selected == "oversize":
            return httpx.Response(200, text="x" * (profile.MAX_JSON_BYTES + 1))
        elif selected == "redirect":
            return httpx.Response(307, headers={"location": "http://127.0.0.1:6666/mcp"})
        elif selected == "structured":
            result["result"]["structuredContent"] = output
        return httpx.Response(200, json=result)

    monkeypatch.setattr(profile.httpx, "AsyncClient", lambda **kwargs: actual_async_client(transport=httpx.MockTransport(handle), **kwargs))
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    with TestClient(app) as client:
        yield client, store, calls, mode


def test_ordered_mapping_receipt_and_replay_after_store_reopen(service, monkeypatch):
    client, store, calls, _ = service
    first = client.post("/api/v1/compose/execute", json=plan())
    assert first.status_code == 200, first.text
    receipt = first.json()
    assert receipt["status"] == "completed"
    assert receipt["execution_profile"] == "lingo.v1"
    assert len(receipt["request_hash"]) == 64
    assert [call["params"]["name"] for call in calls] == ["collect", "seal"]
    assert calls[1]["params"]["arguments"] == {"evidence": {"revision": "source-sha", "verified": True}}
    saved = client.get("/api/v1/compose/executions/" + receipt["execution_id"]).json()
    assert saved["metadata"]["request_hash"] == receipt["request_hash"]
    assert saved["metadata"]["execution_profile"] == "lingo.v1"
    assert saved["metadata"]["idempotency_retention"] == "execution_record_lifetime"
    assert "inputs_json" not in saved and "metadata_json" not in saved
    assert "output_json" not in saved["steps"][0]
    assert [step["metadata"]["attempts"][0]["mcp_request_id"] for step in saved["steps"]] == [call["id"] for call in calls]
    reopened = ExecutionStore(str(store.db_path))
    monkeypatch.setattr(routes, "get_execution_store", lambda: reopened)
    replay = client.post("/api/v1/compose/execute", json=plan()).json()
    assert replay["execution_id"] == receipt["execution_id"]
    assert replay["idempotent_replay"] is True
    assert replay["output"] == receipt["output"]
    assert len(calls) == 2


@pytest.mark.parametrize("change", ["definition", "inputs", "workflow_id"])
def test_key_binds_full_plan_and_inputs(service, change):
    client, _, calls, _ = service
    assert client.post("/api/v1/compose/execute", json=plan()).json()["status"] == "completed"
    changed = plan()
    if change == "definition":
        changed["workflow"]["steps"][0]["tool"] = "repo:other"
    elif change == "inputs":
        changed["inputs"]["revision"] = "another"
    else:
        changed["workflow"]["id"] = "another"
    response = client.post("/api/v1/compose/execute", json=changed)
    assert response.status_code == 409
    assert len(calls) == 2


@pytest.mark.parametrize("bad", ["forward_ref", "duplicate_step", "condition", "gate", "retry", "transform", "parallel", "bool_parallel", "empty_steps", "too_many_steps", "unknown_top", "unknown_workflow", "missing_key", "array_inputs", "false_inputs", "reserved_argument"])
def test_invalid_plans_never_dispatch(service, bad):
    client, _, calls, _ = service
    body = plan()
    step = body["workflow"]["steps"][0]
    if bad == "forward_ref": step["input_from"] = "seal"
    elif bad == "duplicate_step": body["workflow"]["steps"][1]["id"] = "collect"
    elif bad == "condition": step["condition"] = "True"
    elif bad == "gate": step["gate_type"] = "auto"
    elif bad == "retry": step["retry_on_failure"] = True
    elif bad == "transform": step["mode"] = "transform"
    elif bad == "parallel": body["workflow"]["max_parallel_steps"] = 2
    elif bad == "bool_parallel": body["workflow"]["max_parallel_steps"] = True
    elif bad == "empty_steps": body["workflow"]["steps"] = []
    elif bad == "too_many_steps": body["workflow"]["steps"] = [{**step, "id": str(i)} for i in range(17)]
    elif bad == "unknown_top": body["stream"] = False
    elif bad == "unknown_workflow": body["workflow"]["entry_step"] = "seal"
    elif bad == "missing_key": del body["idempotency_key"]
    elif bad == "array_inputs": body["inputs"] = []
    elif bad == "false_inputs": body["inputs"] = False
    else: body["inputs"] = {"nested": {"_tenant_id": "other"}}
    response = client.post("/api/v1/compose/execute", json=body)
    assert response.status_code == 422, response.text
    assert calls == []


@pytest.mark.parametrize("mode", ["mcp_error", "protocol_error", "wrong_id", "invalid_error_type", "plain_text", "duplicate_result_json", "unsafe_number", "soft_error", "false_ok", "failed_status", "oversize", "redirect"])
def test_failed_mcp_never_advances_or_retries(service, mode):
    client, store, calls, options = service
    options["name"] = mode
    receipt = client.post("/api/v1/compose/execute", json=plan()).json()
    assert receipt["status"] == "failed", receipt
    assert receipt["output"] is None
    assert receipt["step_results"] == {}
    assert len(calls) == 1
    stored = store.get_workflow_execution(receipt["execution_id"])
    assert stored["status"] == "failed"
    assert stored["steps"][0]["status"] == "failed"
    replay = client.post("/api/v1/compose/execute", json=plan()).json()
    assert replay["status"] == "failed"
    assert replay["error"] == receipt["error"]
    assert len(calls) == 1


def test_deadline_persists_unknown_attempt_and_stops(service):
    client, store, calls, mode = service
    mode["name"] = "timeout"
    body = plan()
    body["workflow"]["steps"][0]["timeout_seconds"] = 0.01
    receipt = client.post("/api/v1/compose/execute", json=body).json()
    assert receipt["status"] == "failed"
    assert receipt["error"] == "workflow_deadline_exceeded"
    assert len(calls) == 1
    row = store.get_workflow_execution(receipt["execution_id"])
    assert row["metadata"]["effect_unknown"] is True
    assert row["steps"][0]["metadata"]["attempts"][0]["status"] == "effect_unknown"


def test_missing_mapping_is_error_not_partial_arguments(service):
    client, _, calls, _ = service
    body = plan()
    body["workflow"]["steps"][1]["input_mapping"]["required"] = "absent"
    receipt = client.post("/api/v1/compose/execute", json=body).json()
    assert receipt["status"] == "failed"
    assert list(receipt["step_results"]) == ["collect"]
    assert len(calls) == 1


def test_map_applies_exact_mapping_per_item(service):
    client, _, calls, mode = service
    mode["name"] = "array"
    body = plan()
    body["workflow"]["steps"][1].update(mode="map", input_mapping={"value": "n", "original": "$"})
    receipt = client.post("/api/v1/compose/execute", json=body).json()
    assert receipt["status"] == "completed", receipt
    assert receipt["output"] == [{"value": 1, "original": {"n": 1}}, {"value": 2, "original": {"n": 2}}]
    assert len(calls) == 3


def test_map_limit_rejects_before_fanout(service):
    client, _, calls, mode = service
    mode.update(name="array", items=[{"n": i} for i in range(33)])
    body = plan()
    body["workflow"]["steps"][1].update(mode="map", input_mapping={"value": "n"})
    receipt = client.post("/api/v1/compose/execute", json=body).json()
    assert receipt["status"] == "failed"
    assert len(calls) == 1


def test_total_leaf_budget_stops_before_excess_step(service):
    client, _, calls, mode = service
    mode.update(name="array", items=[{"n": i} for i in range(32)])
    body = plan()
    body["workflow"]["timeout_seconds"] = 10
    body["workflow"]["steps"] = [body["workflow"]["steps"][0]] + [
        {"id": f"map{i}", "name": f"map{i}", "tool": "repo:seal", "mode": "map"} for i in range(4)
    ]
    receipt = client.post("/api/v1/compose/execute", json=body).json()
    assert receipt["status"] == "failed"
    assert receipt["error"] == "workflow exceeds 128 leaf calls"
    assert len(calls) == 97  # collect + three maps; fourth map would bring total to 129


def test_structured_mcp_result_is_accepted(service):
    client, _, calls, mode = service
    mode["name"] = "structured"
    receipt = client.post("/api/v1/compose/execute", json=plan()).json()
    assert receipt["status"] == "completed"
    assert len(calls) == 2


def test_duplicate_request_field_rejected(service):
    client, _, calls, _ = service
    body = json.dumps(plan())
    body = body[:-1] + ',"idempotency_key":"second"}'
    response = client.post("/api/v1/compose/execute", content=body, headers={"content-type": "application/json"})
    assert response.status_code == 422
    assert calls == []


def test_unregistered_qualified_tool_is_not_dispatch_authority(service):
    client, _, calls, _ = service
    body = plan()
    body["workflow"]["steps"][0]["tool"] = "repo:unregistered"
    assert client.post("/api/v1/compose/execute", json=body).json()["status"] == "failed"
    assert calls == []


def test_customer_cannot_execute_or_read_profile(service, monkeypatch):
    client, _, calls, _ = service
    receipt = client.post("/api/v1/compose/execute", json=plan()).json()
    monkeypatch.setenv("REQUIRE_BEARER_AUTH", "1")
    monkeypatch.setattr("tool_management.api.tools_api.resolve_execution_identity", lambda request: {"kind": "customer", "tenant_id": "tenant"})
    assert client.post("/api/v1/compose/execute", json=plan(), headers={"authorization": "Bearer customer-test"}).status_code == 403
    assert client.get("/api/v1/compose/executions/" + receipt["execution_id"], headers={"authorization": "Bearer customer-test"}).status_code == 403
    assert len(calls) == 2


def test_sqlite_claim_is_atomic_across_store_instances(tmp_path):
    path = str(tmp_path / "shared.sqlite")
    stores = [ExecutionStore(path), ExecutionStore(path)]
    request = profile.LingoRequest.model_validate(plan())
    digest = hashlib.sha256(b"definition").hexdigest()
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda store: profile._reserve(store, request, digest), stores))
    assert outcomes[0][0] == outcomes[1][0]
    assert sorted(replay for _, replay in outcomes) == [False, True]


def test_reconcile_key_never_submits_and_preserves_unknown_running(service):
    client, store, calls, _ = service
    assert client.get("/api/v1/compose/lingo/keys/absent").status_code == 404
    request = profile.LingoRequest.model_validate(plan())
    execution_id, replay = profile._reserve(store, request, "a" * 64)
    assert replay is False
    response = client.get("/api/v1/compose/lingo/keys/release.1")
    assert response.status_code == 200
    assert response.json()["status"] == "running"
    assert response.json()["execution_id"] == execution_id
    assert response.json()["idempotency_key"] == "release.1"
    assert calls == []


def test_reconcile_completed_receipt_matches_original(service):
    client, _, calls, _ = service
    original = client.post("/api/v1/compose/execute", json=plan()).json()
    reconciled = client.get("/api/v1/compose/lingo/keys/release.1").json()
    assert reconciled["execution_id"] == original["execution_id"]
    assert reconciled["request_hash"] == original["request_hash"]
    assert reconciled["output"] == original["output"]
    assert reconciled["idempotent_replay"] is True
    assert len(calls) == 2


def test_large_inspection_never_truncates_or_lies_about_receipt():
    execution = {"execution_id": "example", "inputs": {"payload": "x" * profile.MAX_JSON_BYTES}, "inputs_json": "redundant"}
    with pytest.raises(Exception) as raised:
        profile.inspection_receipt(execution)
    assert raised.value.status_code == 413
    assert raised.value.detail["reconcile_by_idempotency_key"] is True


def test_completed_claim_lifetime_follows_existing_retention(service):
    client, store, _, _ = service
    original = client.post("/api/v1/compose/execute", json=plan()).json()
    with store._conn() as conn:
        conn.execute("UPDATE workflow_executions SET completed_at=1,updated_at=1 WHERE execution_id=?", (original["execution_id"],))
    store.cleanup_old_workflows(max_age_seconds=1)
    assert client.get("/api/v1/compose/lingo/keys/release.1").status_code == 404
