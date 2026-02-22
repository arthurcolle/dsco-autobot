import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes
from tool_management.core.streaming import StreamingExecutor
from tool_management.persistence.execution_store import ExecutionStore


async def _drain_execution(executor: StreamingExecutor, execution_id: str):
    async for _event in executor.stream_events(execution_id, include_history=True):
        pass


async def _shutdown_executor(executor: StreamingExecutor):
    executor._shutdown = True
    executor._queue_event.set()
    task = executor._scheduler_task
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_execution_group_state_materializes_from_partial_chunks(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "execution_groups.db"))

    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        if progress_callback:
            await progress_callback(progress=20, partial_data={"url": "https://a.example"})
            await progress_callback(progress=40, partial_data={"url": "https://b.example"})
            await progress_callback(progress=60, partial_data={"url": "https://a.example"})
        return {"ok": True}

    executor = StreamingExecutor(
        tool_executor=tool_executor,
        heartbeat_interval=60,
        max_concurrent=1,
        execution_store=store,
    )

    ex1 = await executor.execute(
        tool_name="search",
        inputs={"query": "dc weather"},
        convergence_key="weather:dc",
    )
    await _drain_execution(executor, ex1.execution_id)

    ex2 = await executor.execute(
        tool_name="search",
        inputs={"query": "dc weather followup"},
        convergence_key="weather:dc",
    )
    await _drain_execution(executor, ex2.execution_id)

    group = store.get_execution_group_state("weather:dc")
    assert group is not None
    assert group["tool_name"] == "search"
    assert int(group["chunk_count"]) >= 6
    assert int(group["execution_count"]) == 2
    state = group.get("state") if isinstance(group.get("state"), dict) else {}
    items = state.get("items") if isinstance(state.get("items"), list) else []
    # append_unique reducer should collapse duplicate chunk payloads.
    assert len(items) == 2

    await _shutdown_executor(executor)


def test_execution_group_api_list_get_delete(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "execution_groups_api.db"))
    store.upsert_execution_group_chunk(
        convergence_key="group_alpha",
        tool_name="search",
        chunk={"title": "alpha"},
        execution_id="exec_1",
        trace_id="tr_1",
        session_id="sess_1",
    )
    store.upsert_execution_group_chunk(
        convergence_key="group_beta",
        tool_name="deep_search",
        chunk={"title": "beta"},
        execution_id="exec_2",
        trace_id="tr_2",
        session_id="sess_2",
    )

    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    listed = client.get("/api/v1/execution-groups", params={"limit": 10})
    assert listed.status_code == 200
    payload = listed.json()
    assert payload["count"] == 2

    filtered = client.get("/api/v1/execution-groups", params={"tool_name": "search", "limit": 10})
    assert filtered.status_code == 200
    filtered_payload = filtered.json()
    assert filtered_payload["count"] == 1
    assert filtered_payload["groups"][0]["convergence_key"] == "group_alpha"

    fetched = client.get("/api/v1/execution-groups/group_alpha")
    assert fetched.status_code == 200
    fetched_payload = fetched.json()
    assert fetched_payload["tool_name"] == "search"
    assert fetched_payload["state"]["items"][0]["title"] == "alpha"

    deleted = client.delete("/api/v1/execution-groups/group_alpha")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True

    missing = client.get("/api/v1/execution-groups/group_alpha")
    assert missing.status_code == 404
