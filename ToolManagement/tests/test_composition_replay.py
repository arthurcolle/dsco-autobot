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
