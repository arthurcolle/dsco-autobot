import asyncio
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import job_routes
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
async def test_streaming_resume_since_event_id_filters_history(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "stream_resume.db"))

    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        if progress_callback:
            await progress_callback(progress=20, partial_data={"idx": 0}, message="chunk_0")
            await progress_callback(progress=40, partial_data={"idx": 1}, message="chunk_1")
            await progress_callback(progress=60, partial_data={"idx": 2}, message="chunk_2")
        return {"ok": True}

    executor = StreamingExecutor(
        tool_executor=tool_executor,
        heartbeat_interval=60,
        max_concurrent=1,
        execution_store=store,
    )
    execution = await executor.execute(tool_name="search", inputs={})

    first_pass = []
    async for event in executor.stream_events(execution.execution_id, include_history=True):
        first_pass.append(event)

    partials = [
        event for event in first_pass
        if event.type.value == "partial" and event.event_id is not None
    ]
    assert len(partials) >= 2
    cursor = int(partials[1].event_id)

    resumed = []
    async for event in executor.stream_events(
        execution.execution_id,
        include_history=True,
        since_event_id=cursor,
    ):
        resumed.append(event)

    assert resumed
    assert all(event.event_id is None or int(event.event_id) > cursor for event in resumed)
    assert any(event.replayed for event in resumed)

    await _shutdown_executor(executor)


def test_job_stream_route_forwards_resume_cursor(monkeypatch):
    captured = {}
    execution = SimpleNamespace(
        execution_id="job_123",
        status="running",
        trace_id="tr_job",
        session_id="sess_job",
        parent_execution_id=None,
    )

    class DummyExecutor:
        def get_execution(self, execution_id):
            return execution if execution_id == "job_123" else None

        async def stream_sse(
            self,
            execution_id,
            include_history=True,
            since_event_id=None,
            since_timestamp=None,
        ):
            captured["execution_id"] = execution_id
            captured["include_history"] = include_history
            captured["since_event_id"] = since_event_id
            captured["since_timestamp"] = since_timestamp
            yield "event: done\ndata: {\"ok\": true}\n\n"

    monkeypatch.setattr(job_routes, "get_streaming_executor", lambda: DummyExecutor())

    app = FastAPI()
    app.include_router(job_routes.router)
    client = TestClient(app)

    response = client.get(
        "/jobs/job_123/stream",
        params={
            "include_history": "true",
            "since_event_id": 9,
            "since_timestamp": 123.45,
        },
    )

    assert response.status_code == 200
    assert "event: done" in response.text
    assert captured == {
        "execution_id": "job_123",
        "include_history": True,
        "since_event_id": 9,
        "since_timestamp": 123.45,
    }


def test_job_detach_route_returns_background_handle(monkeypatch):
    execution = SimpleNamespace(
        execution_id="job_123",
        status="running",
        trace_id="tr_job",
        session_id="sess_job",
        parent_execution_id="parent_1",
    )

    class DummyExecutor:
        def get_execution(self, execution_id):
            return execution if execution_id == "job_123" else None

        async def detach_execution(self, execution_id):
            return execution_id == "job_123"

    monkeypatch.setattr(job_routes, "get_streaming_executor", lambda: DummyExecutor())

    app = FastAPI()
    app.include_router(job_routes.router)
    client = TestClient(app)

    response = client.post("/jobs/job_123/detach")
    assert response.status_code == 200
    payload = response.json()
    assert payload["job_id"] == "job_123"
    assert payload["detached"] is True
    assert payload["status"] == "running_background"
    assert payload["poll_url"] == "/api/v1/jobs/job_123"
    assert payload["stream_url"] == "/api/v1/jobs/job_123/stream"
    assert payload["trace_id"] == "tr_job"
    assert payload["session_id"] == "sess_job"
    assert payload["parent_execution_id"] == "parent_1"
