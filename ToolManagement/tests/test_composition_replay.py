from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import composition_routes
from tool_management.persistence.execution_store import ExecutionStore


def _build_client(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "compose_replay.db"))

    monkeypatch.setattr(composition_routes, "get_execution_store", lambda: store)
    composition_routes._workflow_registry.clear()
    composition_routes._workflow_registry_loaded = True

    calls = []

    async def fake_execute_tool(tool_name, inputs):
        calls.append({"tool": tool_name, "inputs": inputs})
        return {
            "tool": tool_name,
            "inputs": inputs,
            "call_number": len(calls),
        }

    monkeypatch.setattr(composition_routes, "_execute_tool", fake_execute_tool)

    app = FastAPI()
    app.include_router(composition_routes.router, prefix="/api/v1")
    return TestClient(app), store, calls


def test_composition_replay_from_step_reuses_prior_outputs(tmp_path, monkeypatch):
    client, store, calls = _build_client(tmp_path, monkeypatch)

    workflow = {
        "id": "wf_replay_1",
        "name": "wf_replay",
        "steps": [
            {"id": "s1", "name": "step_one", "mode": "passthrough", "tool": "echo"},
            {"id": "s2", "name": "step_two", "mode": "passthrough", "tool": "echo"},
        ],
    }

    execute_payload = {
        "workflow": workflow,
        "inputs": {"input": "hello"},
        "stream": False,
        "trace_id": "tr_initial",
    }

    first = client.post("/api/v1/compose/execute", json=execute_payload)
    assert first.status_code == 200
    first_data = first.json()
    assert first_data["status"] == "completed"
    source_execution_id = first_data["execution_id"]
    assert len(calls) == 2

    replay = client.post(
        f"/api/v1/compose/executions/{source_execution_id}/replay",
        json={
            "from_step": "s2",
            "include_prior_steps": True,
            "fork": True,
            "trace_id": "tr_replay",
        },
    )
    assert replay.status_code == 200
    replay_data = replay.json()
    assert replay_data["source_execution_id"] == source_execution_id
    assert replay_data["status"] == "completed"
    assert "s1" in replay_data["reused_steps"]
    assert replay_data["trace_id"] == "tr_replay"

    # One additional call for replaying from step s2 only.
    assert len(calls) == 3

    replay_execution = store.get_workflow_execution(
        replay_data["execution_id"],
        include_steps=True,
        include_events=True,
        include_artifacts=True,
    )
    assert replay_execution is not None
    assert replay_execution["replay_of_execution_id"] == source_execution_id
    assert replay_execution["trace_id"] == "tr_replay"
    assert len(replay_execution.get("artifacts", [])) >= 2  # input + output artifacts


def test_composition_idempotency_returns_existing_execution(tmp_path, monkeypatch):
    client, _store, _calls = _build_client(tmp_path, monkeypatch)

    workflow = {
        "id": "wf_idempotent_1",
        "name": "wf_idempotent",
        "steps": [
            {"id": "s1", "name": "step_one", "mode": "passthrough", "tool": "echo"},
        ],
    }

    payload = {
        "workflow": workflow,
        "inputs": {"input": "same"},
        "stream": False,
        "idempotency_key": "idem-workflow-1",
    }

    first = client.post("/api/v1/compose/execute", json=payload)
    assert first.status_code == 200
    first_data = first.json()
    assert first_data["status"] == "completed"
    first_execution_id = first_data["execution_id"]

    second = client.post("/api/v1/compose/execute", json=payload)
    assert second.status_code == 200
    second_data = second.json()
    assert second_data["idempotent_replay"] is True
    assert second_data["execution_id"] == first_execution_id


def test_replay_blocks_side_effect_by_default(tmp_path, monkeypatch):
    client, _store, calls = _build_client(tmp_path, monkeypatch)

    workflow = {
        "id": "wf_side_effect_block",
        "name": "wf_side_effect_block",
        "steps": [
            {"id": "s1", "name": "step_one", "mode": "passthrough", "tool": "echo"},
            {"id": "s2", "name": "step_two", "mode": "passthrough", "tool": "store_belief"},
        ],
    }

    first = client.post(
        "/api/v1/compose/execute",
        json={"workflow": workflow, "inputs": {"input": "persist this"}},
    )
    assert first.status_code == 200
    source_execution_id = first.json()["execution_id"]
    assert len(calls) == 2

    replay = client.post(
        f"/api/v1/compose/executions/{source_execution_id}/replay",
        json={"from_step": "s2"},
    )
    assert replay.status_code == 409
    assert "side-effecting" in replay.json()["detail"]
    assert len(calls) == 2


def test_replay_reuses_side_effect_step_when_requested(tmp_path, monkeypatch):
    client, store, calls = _build_client(tmp_path, monkeypatch)

    workflow = {
        "id": "wf_side_effect_reuse",
        "name": "wf_side_effect_reuse",
        "steps": [
            {"id": "s1", "name": "step_one", "mode": "passthrough", "tool": "echo"},
            {"id": "s2", "name": "step_two", "mode": "passthrough", "tool": "store_belief"},
        ],
    }

    first = client.post(
        "/api/v1/compose/execute",
        json={"workflow": workflow, "inputs": {"input": "persist this"}},
    )
    assert first.status_code == 200
    source_execution_id = first.json()["execution_id"]
    assert len(calls) == 2

    replay = client.post(
        f"/api/v1/compose/executions/{source_execution_id}/replay",
        json={"from_step": "s2", "write_policy": "reuse"},
    )
    assert replay.status_code == 200
    replay_data = replay.json()
    assert replay_data["write_policy"] == "reuse"
    assert "s2" in replay_data["reused_steps"]
    assert len(calls) == 2  # no re-execution of write step

    lineage = client.get(f"/api/v1/compose/executions/{replay_data['execution_id']}/lineage")
    assert lineage.status_code == 200
    lineage_data = lineage.json()
    assert any(edge["type"] == "replay_of" for edge in lineage_data["edges"])

    replay_execution = store.get_workflow_execution(
        replay_data["execution_id"],
        include_steps=True,
        include_side_effects=True,
    )
    assert replay_execution is not None
    assert replay_execution["replay_of_execution_id"] == source_execution_id
    assert replay_execution.get("side_effects", []) == []


def test_replay_allow_executes_and_records_side_effects(tmp_path, monkeypatch):
    client, store, calls = _build_client(tmp_path, monkeypatch)

    workflow = {
        "id": "wf_side_effect_allow",
        "name": "wf_side_effect_allow",
        "steps": [
            {"id": "s1", "name": "step_one", "mode": "passthrough", "tool": "echo"},
            {"id": "s2", "name": "step_two", "mode": "passthrough", "tool": "store_belief"},
        ],
    }

    first = client.post(
        "/api/v1/compose/execute",
        json={"workflow": workflow, "inputs": {"input": "persist this"}},
    )
    assert first.status_code == 200
    source_execution_id = first.json()["execution_id"]
    assert len(calls) == 2

    replay = client.post(
        f"/api/v1/compose/executions/{source_execution_id}/replay",
        json={"from_step": "s2", "write_policy": "allow", "trace_id": "tr_sidefx_allow"},
    )
    assert replay.status_code == 200
    replay_data = replay.json()
    assert replay_data["write_policy"] == "allow"
    assert len(calls) == 3

    side_effects_resp = client.get(f"/api/v1/compose/executions/{replay_data['execution_id']}/side-effects")
    assert side_effects_resp.status_code == 200
    side_effects = side_effects_resp.json()
    assert side_effects["count"] >= 1
    assert side_effects["side_effects"][0]["tool_name"] == "store_belief"

    trace_side_effects_resp = client.get("/api/v1/compose/traces/tr_sidefx_allow/side-effects")
    assert trace_side_effects_resp.status_code == 200
    assert trace_side_effects_resp.json()["count"] >= 1

    storage_resp = client.get("/api/v1/compose/storage")
    assert storage_resp.status_code == 200
    storage = storage_resp.json()["storage"]
    assert storage["table_counts"]["workflow_side_effects"] >= 1

    replay_execution = store.get_workflow_execution(
        replay_data["execution_id"],
        include_side_effects=True,
    )
    assert replay_execution is not None
    assert len(replay_execution["side_effects"]) >= 1


def test_workflow_gate_decisions_are_recorded_and_listed(tmp_path, monkeypatch):
    client, _store, _calls = _build_client(tmp_path, monkeypatch)

    workflow = {
        "id": "wf_gate_1",
        "name": "wf_gate",
        "steps": [
            {"id": "s1", "name": "step_one", "mode": "passthrough", "tool": "echo"},
            {"id": "s2", "name": "step_two", "mode": "passthrough", "tool": "echo"},
        ],
    }

    run = client.post(
        "/api/v1/compose/execute",
        json={"workflow": workflow, "inputs": {"input": "hello"}, "trace_id": "tr_gate_1"},
    )
    assert run.status_code == 200
    execution_id = run.json()["execution_id"]

    decision = client.post(
        f"/api/v1/compose/executions/{execution_id}/gates",
        json={
            "step_key": "s1",
            "gate_type": "agent_approval",
            "action": "proceed",
            "actor": "agent",
            "status": "accepted",
            "reason": "first result looked sufficient",
            "payload": {"top_result": "https://example.com"},
        },
    )
    assert decision.status_code == 200
    decision_payload = decision.json()
    assert decision_payload["record"]["gate_action"] == "proceed"
    assert decision_payload["record"]["step_key"] == "s1"

    by_execution = client.get(f"/api/v1/compose/executions/{execution_id}/gates")
    assert by_execution.status_code == 200
    by_execution_payload = by_execution.json()
    assert by_execution_payload["count"] == 1
    assert by_execution_payload["gates"][0]["gate_type"] == "agent_approval"

    by_trace = client.get("/api/v1/compose/traces/tr_gate_1/gates")
    assert by_trace.status_code == 200
    by_trace_payload = by_trace.json()
    assert by_trace_payload["count"] >= 1
