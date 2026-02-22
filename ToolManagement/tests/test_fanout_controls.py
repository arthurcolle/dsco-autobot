import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes
from tool_management.core.streaming import StreamingExecutor
from tool_management.persistence.execution_store import ExecutionStore


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
async def test_convergence_key_coalesces_active_jobs():
    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        if progress_callback:
            await progress_callback(progress=10, message="running")
        await asyncio.sleep(0.05)
        return {"ok": True}

    executor = StreamingExecutor(
        tool_executor=tool_executor,
        heartbeat_interval=60,
        max_concurrent=1,
        execution_store=None,
    )
    first = await executor.execute(
        tool_name="search",
        inputs={"query": "a"},
        convergence_key="weather_dc",
    )
    second = await executor.execute(
        tool_name="search",
        inputs={"query": "b"},
        convergence_key="weather_dc",
    )

    assert second.execution_id == first.execution_id
    assert int(first.control_state.get("coalesced_requests", 0)) >= 1
    assert int(first.control_state.get("submit_count", 1)) >= 2

    async for _event in executor.stream_events(first.execution_id, include_history=True):
        pass
    await _shutdown_executor(executor)


@pytest.mark.asyncio
async def test_fanout_budget_blocks_excess_jobs():
    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        await asyncio.sleep(0.1)
        return {"ok": True}

    executor = StreamingExecutor(
        tool_executor=tool_executor,
        heartbeat_interval=60,
        max_concurrent=1,
        execution_store=None,
    )

    first = await executor.execute(
        tool_name="deep_search",
        inputs={"q": "x", "_trace_id": "tr_fanout_1"},
        fanout_budget=1,
        trace_id="tr_fanout_1",
    )
    assert first.execution_id

    with pytest.raises(RuntimeError, match="FANOUT_LIMIT_EXCEEDED"):
        await executor.execute(
            tool_name="deep_search",
            inputs={"q": "y", "_trace_id": "tr_fanout_1"},
            fanout_budget=1,
            trace_id="tr_fanout_1",
        )

    executor.cancel_execution(first.execution_id)
    await _shutdown_executor(executor)


def test_execute_batch_rejects_oversized_payload(monkeypatch):
    monkeypatch.setenv("EXECUTE_BATCH_MAX_CALLS", "2")
    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    payload = {
        "execution": "parallel",
        "calls": [
            {"tool_name": "search", "arguments": {"q": "1"}},
            {"tool_name": "search", "arguments": {"q": "2"}},
            {"tool_name": "search", "arguments": {"q": "3"}},
        ],
    }
    response = client.post("/api/v1/execute/batch", json=payload)
    assert response.status_code == 400
    assert "Batch request too large" in response.json()["detail"]


def test_execute_batch_parallel_respects_max_parallel(monkeypatch):
    in_flight = {"count": 0, "max": 0}

    async def fake_execute(request):
        in_flight["count"] += 1
        in_flight["max"] = max(in_flight["max"], in_flight["count"])
        await asyncio.sleep(0.02)
        in_flight["count"] -= 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"ok": True},
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    calls = [{"tool_name": "search", "arguments": {"q": str(idx)}} for idx in range(10)]
    response = client.post(
        "/api/v1/execute/batch",
        json={"execution": "parallel", "max_parallel": 3, "calls": calls},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 10
    assert payload["failed"] == 0
    assert in_flight["max"] <= 3


def test_execute_batch_preflight_dedupe_preserves_order(monkeypatch):
    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        tool_name = request.tool_name or request.tool or "unknown"
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=tool_name,
            execution_mode=request.execution_mode,
            result={"seq": calls_seen["count"], "tool": tool_name, "args": request.arguments},
            trace_id=request.trace_id,
            session_id=request.session_id,
            deduplicated=False,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    payload = {
        "execution": "parallel",
        "dedupe_calls": True,
        "calls": [
            {"tool_name": "search", "arguments": {"q": "dc weather"}},
            {"tool_name": "search", "arguments": {"q": "dc weather"}},
            {"tool_name": "list_events", "arguments": {}},
        ],
    }
    response = client.post("/api/v1/execute/batch", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert calls_seen["count"] == 2  # canonical calls only
    assert data["batch_plan"]["canonical_calls"] == 2
    assert data["batch_plan"]["deduped_calls"] == 1
    assert data["results"][0]["result"]["seq"] == 1
    assert data["results"][1]["result"]["seq"] == 1
    assert data["results"][1]["deduplicated"] is True
    assert data["results"][2]["result"]["seq"] == 2


def test_execute_batch_truncate_marks_overflow_skipped(monkeypatch):
    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"seq": calls_seen["count"]},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    payload = {
        "execution": "sequential",
        "dedupe_calls": False,
        "max_canonical_calls": 2,
        "overflow_policy": "truncate",
        "calls": [
            {"tool_name": "search", "arguments": {"q": "1"}},
            {"tool_name": "search", "arguments": {"q": "2"}},
            {"tool_name": "search", "arguments": {"q": "3"}},
            {"tool_name": "search", "arguments": {"q": "4"}},
        ],
    }
    response = client.post("/api/v1/execute/batch", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert calls_seen["count"] == 2
    statuses = [row["status"] for row in data["results"]]
    assert statuses[:2] == ["completed", "completed"]
    assert statuses[2:] == ["skipped", "skipped"]
    assert data["batch_plan"]["overflowed_canonical_calls"] == 2


def test_execute_batch_auto_follow_up_for_large_undeduped_batch(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "batch_followups.db"))
    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)

    calls_seen = {"count": 0}

    async def fake_execute(request):
        calls_seen["count"] += 1
        return execution_routes.ExecuteResponse(
            status="completed",
            tool=request.tool_name or request.tool or "unknown",
            execution_mode=request.execution_mode,
            result={"seq": calls_seen["count"]},
            trace_id=request.trace_id,
            session_id=request.session_id,
        )

    monkeypatch.setattr(execution_routes, "execute", fake_execute)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    batch_calls = [
        {"tool_name": "search", "arguments": {"q": f"item-{idx}"}}
        for idx in range(55)
    ]
    response = client.post(
        "/api/v1/execute/batch",
        json={
            "execution": "sequential",
            "dedupe_calls": False,
            "auto_follow_up": True,
            "trace_id": "tr_batch_followup",
            "calls": batch_calls,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["batch_plan"]["total_calls"] == 55
    assert len(data["follow_up_actions"]) >= 1
    action_types = {item["action_type"] for item in data["follow_up_actions"]}
    assert "enable_batch_preflight_dedupe" in action_types

    listed = client.get("/api/v1/follow-ups", params={"trace_id": "tr_batch_followup", "limit": 20})
    assert listed.status_code == 200
    assert listed.json()["count"] >= 1
