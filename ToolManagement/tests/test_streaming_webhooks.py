import asyncio

import pytest

from tool_management.core.streaming import StreamingExecutor


async def _drain_stream(executor: StreamingExecutor, execution_id: str):
    async for _event in executor.stream_events(execution_id, include_history=True):
        pass
    await asyncio.sleep(0.05)


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
async def test_streaming_executor_emits_progress_and_complete_webhooks(monkeypatch):
    deliveries = []

    async def fake_post(self, callback_url, payload, execution, callback_event):
        deliveries.append((callback_url, callback_event, payload))

    monkeypatch.setattr(StreamingExecutor, "_post_webhook", fake_post)
    monkeypatch.setenv("JOB_WEBHOOK_PROGRESS_INTERVAL_SECONDS", "0")

    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        if progress_callback:
            await progress_callback(progress=15, message="working")
        return {"ok": True}

    executor = StreamingExecutor(tool_executor, heartbeat_interval=60, max_concurrent=1, execution_store=None)
    execution = await executor.execute(
        tool_name="search",
        inputs={},
        callback_url="https://example.com/webhook",
        callback_events=["progress", "complete"],
    )

    await _drain_stream(executor, execution.execution_id)

    assert any(delivery[1] == "progress" for delivery in deliveries)
    assert any(delivery[1] == "complete" for delivery in deliveries)
    assert all(delivery[0] == "https://example.com/webhook" for delivery in deliveries)

    await _shutdown_executor(executor)


@pytest.mark.asyncio
async def test_streaming_executor_respects_webhook_event_filter(monkeypatch):
    deliveries = []

    async def fake_post(self, callback_url, payload, execution, callback_event):
        deliveries.append((callback_url, callback_event, payload))

    monkeypatch.setattr(StreamingExecutor, "_post_webhook", fake_post)
    monkeypatch.setenv("JOB_WEBHOOK_PROGRESS_INTERVAL_SECONDS", "0")

    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        if progress_callback:
            await progress_callback(progress=40, message="running")
        return {"ok": True}

    executor = StreamingExecutor(tool_executor, heartbeat_interval=60, max_concurrent=1, execution_store=None)
    execution = await executor.execute(
        tool_name="search",
        inputs={},
        callback_url="https://example.com/webhook",
        callback_events=["error"],
    )

    await _drain_stream(executor, execution.execution_id)

    assert deliveries == []

    await _shutdown_executor(executor)


@pytest.mark.asyncio
async def test_streaming_executor_emits_error_webhook(monkeypatch):
    deliveries = []

    async def fake_post(self, callback_url, payload, execution, callback_event):
        deliveries.append((callback_url, callback_event, payload))

    monkeypatch.setattr(StreamingExecutor, "_post_webhook", fake_post)

    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        raise RuntimeError("backend exploded")

    executor = StreamingExecutor(tool_executor, heartbeat_interval=60, max_concurrent=1, execution_store=None)
    execution = await executor.execute(
        tool_name="search",
        inputs={},
        callback_url="https://example.com/webhook",
        callback_events=["error"],
    )

    await _drain_stream(executor, execution.execution_id)

    assert any(delivery[1] == "error" for delivery in deliveries)

    await _shutdown_executor(executor)
