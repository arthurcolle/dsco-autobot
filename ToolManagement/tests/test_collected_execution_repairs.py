import asyncio
from types import SimpleNamespace

import pytest

from tool_management.api import execution_routes
from tool_management.core.streaming import StreamingExecutor
from tool_management.persistence.execution_store import ExecutionStore


class _PolicyToolStore:
    def __init__(self, p95_latency_ms=None):
        self.p95_latency_ms = p95_latency_ms

    def resolve_tool(self, tool_name: str):
        return "distributed-search-mcp", "https://example.com", tool_name.split(":", 1)[-1]

    def get_tool_latency_metrics(self, _tool_name: str, hours: int = 24):
        if self.p95_latency_ms is None:
            return {}
        return {
            "p95_latency_ms": float(self.p95_latency_ms),
            "success_rate": 0.99,
            "calls": 30,
        }

    def get_tool_by_name(self, _tool_ref: str):
        if self.p95_latency_ms is None:
            return None
        return SimpleNamespace(avg_latency_ms=float(self.p95_latency_ms))


def _install_executor(tmp_path, monkeypatch, tool_executor, *, p95_latency_ms=None):
    store = ExecutionStore(db_path=str(tmp_path / "collected_execution.db"))
    executor = StreamingExecutor(
        tool_executor=tool_executor,
        heartbeat_interval=60,
        max_concurrent=1,
        execution_store=store,
    )
    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    monkeypatch.setattr(execution_routes, "get_tool_store", lambda: _PolicyToolStore(p95_latency_ms))
    monkeypatch.setattr(execution_routes, "get_streaming_executor", lambda: executor)
    return executor, store


async def _drain_execution(executor: StreamingExecutor, execution_id: str):
    async for _event in executor.stream_events(execution_id, include_history=True):
        pass


async def _shutdown_executor(executor: StreamingExecutor):
    for execution in list(executor._executions.values()):
        if execution.status in {"pending", "queued", "running"}:
            executor.cancel_execution(execution.execution_id)
    await asyncio.sleep(0)
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
async def test_auto_collected_stream_selected_fast_call_returns_collected_result(tmp_path, monkeypatch):
    backend_calls = []

    async def tool_executor(tool_name, inputs, progress_callback=None, **_kwargs):
        backend_calls.append((tool_name, dict(inputs)))
        if progress_callback:
            await progress_callback(progress=25, message="running")
        return {"ok": True, "tool": tool_name}

    executor, store = _install_executor(
        tmp_path,
        monkeypatch,
        tool_executor,
        p95_latency_ms=None,
    )

    async def fail_sync(**_kwargs):
        raise AssertionError("_execute_sync should not run for auto collected durable execution")

    monkeypatch.setattr(execution_routes, "_execute_sync", fail_sync)

    try:
        response = await execution_routes.execute(
            execution_routes.ExecuteRequest(
                tool_name="weather_probe",
                arguments={"city": "washington"},
                execution_mode="auto",
                stream=False,
                sync_timeout_ms=500,
            )
        )

        assert response.status == "completed"
        assert response.execution_mode == "sync"
        assert response.job_id is None
        assert response.call_id
        assert response.result == {"ok": True, "tool": "weather_probe"}
        assert len(backend_calls) == 1

        persisted = store.list_policy_decisions(hours=24, limit=1)
        assert persisted[0]["selected_mode"] == "sync"
        assert "auto_collected_completed_within_timeout" in persisted[0]["reasons"]
    finally:
        await _shutdown_executor(executor)


@pytest.mark.asyncio
async def test_auto_collected_timeout_returns_same_submitted_job_without_resubmitting(tmp_path, monkeypatch):
    backend_calls = []

    async def tool_executor(tool_name, inputs, **_kwargs):
        backend_calls.append((tool_name, dict(inputs)))
        await asyncio.sleep(0.25)
        return {"ok": True}

    executor, store = _install_executor(
        tmp_path,
        monkeypatch,
        tool_executor,
        p95_latency_ms=500.0,
    )

    async def fail_sync(**_kwargs):
        raise AssertionError("_execute_sync should not run before job fallback")

    monkeypatch.setattr(execution_routes, "_execute_sync", fail_sync)

    try:
        response = await execution_routes.execute(
            execution_routes.ExecuteRequest(
                tool_name="fast_read",
                arguments={"q": "short"},
                execution_mode="auto",
                stream=False,
                sync_timeout_ms=100,
                timeout_ms=1000,
            )
        )

        assert response.status == "submitted"
        assert response.execution_mode == "job"
        assert response.job_id
        assert response.call_id == response.job_id
        assert response.error["code"] == "SYNC_TIMEOUT_ESCALATED"

        await _drain_execution(executor, response.job_id)
        assert len(backend_calls) == 1
        assert executor.get_execution(response.job_id).status == "completed"

        persisted = store.list_policy_decisions(hours=24, limit=1)
        assert persisted[0]["execution_id"] == response.job_id
        assert persisted[0]["selected_mode"] == "job"
        assert "auto_collected_timeout_returned_existing_job" in persisted[0]["reasons"]
    finally:
        await _shutdown_executor(executor)


@pytest.mark.asyncio
async def test_auto_collected_cancelled_execution_returns_cancelled_without_resubmitting(tmp_path, monkeypatch):
    backend_calls = []

    async def tool_executor(tool_name, inputs, **_kwargs):
        backend_calls.append((tool_name, dict(inputs)))
        await asyncio.sleep(0)
        raise asyncio.CancelledError("caller cancelled")

    executor, _store = _install_executor(
        tmp_path,
        monkeypatch,
        tool_executor,
        p95_latency_ms=700.0,
    )

    async def fail_sync(**_kwargs):
        raise AssertionError("_execute_sync should not run for collected cancellation")

    monkeypatch.setattr(execution_routes, "_execute_sync", fail_sync)

    try:
        response = await execution_routes.execute(
            execution_routes.ExecuteRequest(
                tool_name="fast_read",
                arguments={"q": "cancel"},
                execution_mode="auto",
                stream=False,
                sync_timeout_ms=500,
            )
        )

        assert response.status == "cancelled"
        assert response.error["code"] == "EXECUTION_CANCELLED"
        assert response.job_id is None
        assert len(backend_calls) == 1
    finally:
        await _shutdown_executor(executor)


@pytest.mark.asyncio
async def test_slow_auto_job_and_callbacks_stay_detached(tmp_path, monkeypatch):
    backend_calls = []
    release = asyncio.Event()
    deliveries = []

    async def tool_executor(tool_name, inputs, **_kwargs):
        backend_calls.append((tool_name, dict(inputs)))
        await release.wait()
        return {"ok": True}

    executor, _store = _install_executor(
        tmp_path,
        monkeypatch,
        tool_executor,
        p95_latency_ms=45_000.0,
    )

    async def fake_post_webhook(callback_url, payload, execution, callback_event):
        deliveries.append((callback_url, callback_event, payload["job_id"], execution.execution_id))

    executor._post_webhook = fake_post_webhook

    try:
        response = await asyncio.wait_for(
            execution_routes.execute(
                execution_routes.ExecuteRequest(
                    tool_name="deep_search",
                    arguments={"q": "long"},
                    execution_mode="auto",
                    stream=False,
                    sync_timeout_ms=500,
                    callback_url="https://events.example.com/hook",
                    callback_events=["complete"],
                )
            ),
            timeout=0.1,
        )

        assert response.status == "submitted"
        assert response.execution_mode == "job"
        assert response.callback_url == "https://events.example.com/hook"
        assert response.callback_events == ["complete"]
        assert response.job_id

        release.set()
        await _drain_execution(executor, response.job_id)
        await asyncio.sleep(0)
        assert len(backend_calls) == 1
        assert deliveries == [
            (
                "https://events.example.com/hook",
                "complete",
                response.job_id,
                response.job_id,
            )
        ]
    finally:
        release.set()
        await _shutdown_executor(executor)
