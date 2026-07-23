from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import pytest
from fastapi import HTTPException

from parallel_websearch_server.models import (
    BetaTaskRunInput,
    ManagedTaskRunResponse,
    ProcessorType,
    SearchRequest,
    TaskRun,
    TaskRunJsonOutput,
    TaskRunResult,
    TaskRunStatus,
    TaskRunTextOutput,
)
from parallel_websearch_server.routes import search as search_routes
from parallel_websearch_server.routes import tasks as tasks_routes
from parallel_websearch_server.state_store import ContextStateStore


class MockHTTPClient:
    """Minimal async-compatible HTTPX client stub."""

    def __init__(self, responses: List[httpx.Response]):
        self._responses = responses
        self.calls: List[Dict[str, Any]] = []

    async def post(self, path: str, json: Dict[str, Any], headers: Dict[str, str]):
        self.calls.append({"method": "POST", "path": path, "json": json})
        if not self._responses:
            raise RuntimeError("No mock responses left")
        return self._responses.pop(0)


def _make_response(status: int, data: Optional[Dict[str, Any]] = None) -> httpx.Response:
    request = httpx.Request("POST", "https://api.parallel.ai/test")
    return httpx.Response(status_code=status, json=data or {}, request=request)


def _make_task_run(status: TaskRunStatus) -> TaskRun:
    now = datetime.utcnow()
    return TaskRun(
        run_id="run-123",
        status=status,
        created_at=now,
        modified_at=now,
        processor=ProcessorType.LITE,
        metadata={"status": status.value},
    )


@pytest.mark.asyncio
async def test_search_records_successful_interaction(state_store: ContextStateStore):
    response_payload = {
        "results": [{"title": "Result A", "url": "https://example.com", "excerpts": ["foo"]}]
    }
    mock_client = MockHTTPClient([_make_response(200, response_payload)])

    request = SearchRequest(objective="find things")
    result = await search_routes.search(
        request=request,
        context_id="ctx-search",
        client=mock_client,
        store=state_store,
    )
    assert result.results[0].title == "Result A"

    history = await state_store.list_interactions("ctx-search")
    assert history[0]["interaction_type"] == "search"
    assert history[0]["status"] == "completed"
    assert "Result" in history[0]["summary"]


@pytest.mark.asyncio
async def test_search_records_failure_and_raises(state_store: ContextStateStore):
    mock_client = MockHTTPClient([_make_response(500, {"error": "boom"})])
    request = SearchRequest(objective="fail case")

    with pytest.raises(HTTPException) as exc:
        await search_routes.search(
            request=request,
            context_id="ctx-search-fail",
            client=mock_client,
            store=state_store,
        )
    assert exc.value.status_code == 500

    history = await state_store.list_interactions("ctx-search-fail")
    assert history[0]["interaction_type"] == "search"
    assert history[0]["status"] == "failed"


@pytest.mark.asyncio
async def test_create_task_run_records_interaction(state_store: ContextStateStore, monkeypatch: pytest.MonkeyPatch):
    async def fake_create_task_run_internal(request, client):
        return _make_task_run(TaskRunStatus.QUEUED)

    monkeypatch.setattr(tasks_routes, "_create_task_run_internal", fake_create_task_run_internal)

    task_request = BetaTaskRunInput(processor=ProcessorType.LITE, input="demo")
    run = await tasks_routes.create_task_run(
        request=task_request,
        context_id="ctx-task",
        client=None,
        store=state_store,
    )
    assert run.run_id == "run-123"

    history = await state_store.list_interactions("ctx-task")
    assert history[0]["interaction_type"] == "task.run"
    assert history[0]["remote_run_id"] == "run-123"
    assert history[0]["status"] == TaskRunStatus.QUEUED.value


@pytest.mark.asyncio
async def test_create_task_run_failure_is_cached(state_store: ContextStateStore, monkeypatch: pytest.MonkeyPatch):
    async def fake_create_task_run_internal(request, client):
        raise HTTPException(status_code=422, detail={"error": "bad config"})

    monkeypatch.setattr(tasks_routes, "_create_task_run_internal", fake_create_task_run_internal)

    task_request = BetaTaskRunInput(processor=ProcessorType.LITE, input="demo")
    with pytest.raises(HTTPException):
        await tasks_routes.create_task_run(
            request=task_request,
            context_id="ctx-task-fail",
            client=None,
            store=state_store,
        )

    history = await state_store.list_interactions("ctx-task-fail")
    assert history[0]["status"] == TaskRunStatus.FAILED.value
    assert history[0]["summary"].startswith("Task run request failed")


@pytest.mark.asyncio
async def test_create_task_run_rejects_disallowed_processor(
    state_store: ContextStateStore, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(tasks_routes.settings, "allowed_parallel_processors", ["lite"])

    task_request = BetaTaskRunInput(processor=ProcessorType.PRO, input="demo")
    mock_client = MockHTTPClient([])

    with pytest.raises(HTTPException) as exc:
        await tasks_routes.create_task_run(
            request=task_request,
            context_id=None,
            client=mock_client,
            store=state_store,
        )

    assert exc.value.status_code == 403
    assert "processor" in exc.value.detail["error"]


@pytest.mark.asyncio
async def test_get_task_run_result_updates_cache(state_store: ContextStateStore, monkeypatch: pytest.MonkeyPatch):
    await state_store.create_context(context_id="ctx-task-result")
    await state_store.record_interaction(
        context_id="ctx-task-result",
        interaction_type="task.run",
        status="running",
        input_payload={},
        summary="running",
        remote_run_id="run-123",
    )

    run = _make_task_run(TaskRunStatus.COMPLETED)
    result_model = TaskRunResult(run=run, output=TaskRunTextOutput(content="This is the final output."))

    async def fake_get_task_run_result_internal(run_id, client):
        return result_model

    monkeypatch.setattr(
        tasks_routes,
        "_get_task_run_result_internal",
        fake_get_task_run_result_internal,
    )

    response = await tasks_routes.get_task_run_result(
        run_id="run-123",
        client=None,
        store=state_store,
    )
    assert response.output.content.startswith("This is")

    history = await state_store.list_interactions("ctx-task-result")
    assert history[0]["status"] == TaskRunStatus.COMPLETED.value
    assert history[0]["summary"].startswith("This is")


@pytest.mark.asyncio
async def test_managed_task_run_lifecycle_updates_cache(
    state_store: ContextStateStore, monkeypatch: pytest.MonkeyPatch
):
    initial_run = _make_task_run(TaskRunStatus.QUEUED)
    status_queue = [
        _make_task_run(TaskRunStatus.RUNNING),
        _make_task_run(TaskRunStatus.COMPLETED),
    ]
    final_result = TaskRunResult(
        run=_make_task_run(TaskRunStatus.COMPLETED),
        output=TaskRunJsonOutput(type="json", content={"answer": "done"}),
    )

    async def fake_create_task_run_internal(request, client):
        return initial_run

    async def fake_get_task_run_internal(run_id, client):
        return status_queue.pop(0)

    async def fake_get_task_run_result_internal(run_id, client):
        return final_result

    async def fast_sleep(_seconds):
        return None

    monkeypatch.setattr(tasks_routes, "_create_task_run_internal", fake_create_task_run_internal)
    monkeypatch.setattr(tasks_routes, "_get_task_run_internal", fake_get_task_run_internal)
    monkeypatch.setattr(tasks_routes, "_get_task_run_result_internal", fake_get_task_run_result_internal)
    monkeypatch.setattr(tasks_routes.asyncio, "sleep", fast_sleep)

    task_request = BetaTaskRunInput(processor=ProcessorType.LITE, input="end-to-end")
    managed = await tasks_routes.create_managed_task_run(
        request=task_request,
        context_id="ctx-managed",
        poll_interval_seconds=0.01,
        timeout_seconds=5,
        client=None,
        store=state_store,
    )
    assert isinstance(managed, ManagedTaskRunResponse)
    assert managed.final_status == TaskRunStatus.COMPLETED

    history = await state_store.list_interactions("ctx-managed")
    assert len(history) == 1
    entry = history[0]
    assert entry["remote_run_id"] == "run-123"
    assert entry["status"] == TaskRunStatus.COMPLETED.value
    assert entry["summary"] == managed.summary
