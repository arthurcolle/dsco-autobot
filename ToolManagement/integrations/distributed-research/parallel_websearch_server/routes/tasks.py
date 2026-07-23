"""Task API routes."""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, Union

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from parallel_websearch_server.sse import EventSourceResponse

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.models import (
    BetaTaskRunInput,
    BetaTaskRunResult,
    ManagedTaskRunResponse,
    ManagedTaskStatusUpdate,
    TaskRun,
    TaskRunResult,
    TaskRunStatus,
)
# processor_policy archived - processors validated upstream
from parallel_websearch_server.state_store import ContextStateStore, get_state_store
from parallel_websearch_server.mcp_param_fixes import fix_task_input, fix_metadata_parameter
from parallel_websearch_server.mcp_state_store import get_mcp_state_store
from parallel_websearch_server.local_store import get_local_store
from parallel_websearch_server.events import emit_task_run_created, emit_task_run_status

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/tasks", tags=["Tasks"])


async def _create_task_run_internal(
    request: BetaTaskRunInput,
    client: httpx.AsyncClient,
) -> TaskRun:
    """Shared helper to create task runs."""
    try:
        response = await client.post(
            "/v1/tasks/runs",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        task_run = TaskRun(**response.json())

        # Persist to local store
        try:
            store = get_local_store()
            input_text = request.input if isinstance(request.input, str) else None
            input_json = request.input if isinstance(request.input, dict) else None
            processor_val = request.processor.value if hasattr(request.processor, 'value') else str(request.processor)
            store.save_task_run(
                run_id=task_run.run_id,
                processor=processor_val,
                status=task_run.status,
                input_text=input_text,
                input_json=input_json,
                task_spec=request.task_spec.model_dump() if request.task_spec else None,
                metadata=request.metadata,
            )
            # Emit event for UI updates
            emit_task_run_created(task_run.run_id, processor_val, None, input_text)
        except Exception as e:
            logger.warning("Failed to persist task run to local store: %s", e)

        return task_run
    except httpx.HTTPStatusError as exc:
        logger.error("Task creation failed: %s - %s", exc.response.status_code, exc.response.text)
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=exc.response.json() if exc.response.text else {"error": "Task creation failed"},
        )


async def _get_task_run_internal(
    run_id: str,
    client: httpx.AsyncClient,
) -> TaskRun:
    """Shared helper to fetch task run metadata."""
    try:
        response = await client.get(
            f"/v1/tasks/runs/{run_id}",
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        task_run = TaskRun(**response.json())

        # Sync status to local store (delta update)
        try:
            store = get_local_store()
            processor_val = task_run.processor.value if hasattr(task_run.processor, 'value') else str(task_run.processor)
            store.save_task_run(
                run_id=run_id,
                processor=processor_val,
                status=task_run.status,
            )
            # Emit status event for UI updates
            emit_task_run_status(run_id, task_run.status)
        except Exception as e:
            logger.warning("Failed to sync task status to local store: %s", e)

        return task_run
    except httpx.HTTPStatusError as exc:
        logger.error("Get task failed: %s - %s", exc.response.status_code, exc.response.text)
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.json())


def _parse_task_result(data: Dict[str, Any]) -> Union[TaskRunResult, BetaTaskRunResult]:
    """Parse result payload, upgrading to beta model when available."""
    if data.get("output", {}).get("mcp_tool_calls") or data.get("output", {}).get("beta_fields"):
        return BetaTaskRunResult(**data)
    return TaskRunResult(**data)


async def _get_task_run_result_internal(
    run_id: str,
    client: httpx.AsyncClient,
) -> Union[TaskRunResult, BetaTaskRunResult]:
    """Shared helper to fetch task results."""
    try:
        response = await client.get(
            f"/v1/tasks/runs/{run_id}/result",
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        result = _parse_task_result(response.json())

        # Update local store with result data
        try:
            store = get_local_store()
            output = getattr(result, "output", None)
            output_text = None
            output_json = None
            if output:
                output_type = getattr(output, "type", None)
                content = getattr(output, "content", None)
                if output_type == "text":
                    output_text = content
                elif output_type == "json":
                    output_json = content

            store.save_task_run(
                run_id=run_id,
                processor=result.run.processor.value if hasattr(result.run.processor, 'value') else str(result.run.processor),
                status=result.run.status,
                output_text=output_text,
                output_json=output_json,
            )
            # Emit status event for UI updates
            emit_task_run_status(run_id, result.run.status, output_text)
        except Exception as e:
            logger.warning("Failed to update task result in local store: %s", e)

        return result
    except httpx.HTTPStatusError as exc:
        logger.error("Task result fetch failed: %s - %s", exc.response.status_code, exc.response.text)
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.json())


_TERMINAL_TASK_STATUSES = {"completed", "failed", "cancelled"}


def _split_output_payload(
    result: Union[TaskRunResult, BetaTaskRunResult]
) -> Tuple[Optional[str], Optional[Any]]:
    """Extract normalized text/json payload from a task result.

    Returns:
        Tuple of (text_content, json_content) where json_content can be
        any JSON-serializable type (dict, list, str, int, bool, None, etc.)
    """
    output = getattr(result, "output", None)
    if not output:
        return None, None

    output_type = getattr(output, "type", None)
    if output_type == "text":
        return getattr(output, "content", None), None
    if output_type == "json":
        return None, getattr(output, "content", None)
    return None, None


def _truncate_text(text: str, limit: int = 500) -> str:
    """Trim text to a friendly summary length."""
    cleaned = text.strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _stringify_value(value: Any, limit: int = 160) -> str:
    """Convert JSON-friendly values to a truncated string preview."""
    if isinstance(value, (dict, list)):
        preview = json.dumps(value, ensure_ascii=False)
    else:
        preview = str(value)
    preview = preview.strip()
    if len(preview) <= limit:
        return preview
    return preview[: limit - 3].rstrip() + "..."


def _summarize_json_content(content: Any, max_items: int = 5) -> str:
    """Generate a short summary of JSON outputs.

    Handles various content types that may be returned as JSON output:
    - dict: Summarize key-value pairs
    - list: Summarize list items
    - str: Return truncated string (may be stringified JSON)
    - None/empty: Return appropriate message
    - Other types: Convert to string representation
    """
    if content is None:
        return "Task returned a null JSON payload."

    # Handle string content (possibly stringified JSON)
    if isinstance(content, str):
        content = content.strip()
        if not content:
            return "Task returned an empty string payload."
        # Try to parse as JSON if it looks like JSON
        if content.startswith('{') or content.startswith('['):
            try:
                parsed = json.loads(content)
                return _summarize_json_content(parsed, max_items)
            except (json.JSONDecodeError, ValueError):
                pass
        # Return truncated string
        return _truncate_text(content)

    # Handle list content
    if isinstance(content, list):
        if not content:
            return "Task returned an empty JSON array."
        parts: List[str] = []
        for idx, item in enumerate(content):
            if idx >= max_items:
                break
            parts.append(f"[{idx}]: {_stringify_value(item)}")
        if len(content) > max_items:
            parts.append(f"... (+{len(content) - max_items} more items)")
        return "; ".join(parts)

    # Handle dict content (original behavior)
    if isinstance(content, dict):
        if not content:
            return "Task returned an empty JSON object."
        parts = []
        for idx, (key, value) in enumerate(content.items()):
            if idx >= max_items:
                break
            parts.append(f"{key}: {_stringify_value(value)}")
        if len(content) > max_items:
            parts.append(f"... (+{len(content) - max_items} more keys)")
        return "; ".join(parts)

    # Handle other types (int, float, bool, etc.)
    return _stringify_value(content)


def _prepare_output_summary(
    result: Optional[Union[TaskRunResult, BetaTaskRunResult]]
) -> Tuple[Optional[str], Optional[Any], str]:
    """Return normalized outputs and a human-friendly summary.

    Returns:
        Tuple of (text_output, json_output, summary_string) where json_output
        can be any JSON-serializable type.
    """
    if not result:
        return None, None, "Task completed but no output payload was returned."

    text_payload, json_payload = _split_output_payload(result)
    normalized_text = text_payload.strip() if isinstance(text_payload, str) else None

    if normalized_text:
        summary = _truncate_text(normalized_text)
    elif json_payload:
        summary = _summarize_json_content(json_payload)
    else:
        summary = "Task completed without a text or JSON output payload."

    return normalized_text, json_payload, summary


def _summarize_task_run(run: TaskRun) -> str:
    """Compact description for caching task lifecycle."""
    processor = getattr(run.processor, "value", str(run.processor))
    return f"{processor} task is {run.status}"


def _normalize_error_detail(detail: Any) -> Dict[str, Any]:
    """Convert FastAPI/httpx error detail payloads into dictionaries."""
    if isinstance(detail, dict):
        return detail
    return {"error": detail}


@router.post("/runs", response_model=TaskRun, status_code=201, operation_id="create_task_run")
async def create_task_run(
    request: BetaTaskRunInput,
    context_id: Optional[str] = Query(
        default=None,
        description="Optional context identifier used to persist this task run.",
    ),
    client: httpx.AsyncClient = Depends(get_http_client),
    store: ContextStateStore = Depends(get_state_store),
) -> TaskRun:
    """Create task.

    Args:
        request: Task run configuration

    Returns:
        Created task run with status 'queued'
    """
    payload = request.model_dump(exclude_none=True)
    try:
        task_run = await _create_task_run_internal(request, client)
    except HTTPException as exc:
        if context_id:
            await store.ensure_context(context_id)
            await store.record_interaction(
                context_id=context_id,
                interaction_type="task.run",
                status="failed",
                input_payload=payload,
                output_payload=_normalize_error_detail(exc.detail),
                summary=f"Task run request failed ({exc.status_code}).",
            )
        raise

    if context_id:
        await store.ensure_context(context_id)
        await store.record_interaction(
            context_id=context_id,
            interaction_type="task.run",
            status=task_run.status,
            input_payload=payload,
            output_payload=task_run.model_dump(mode="json"),
            summary=_summarize_task_run(task_run),
            remote_run_id=task_run.run_id,
        )
    return task_run


@router.get("/runs/{run_id}", response_model=TaskRun, operation_id="get_task_run")
async def get_task_run(
    run_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
    store: ContextStateStore = Depends(get_state_store),
) -> TaskRun:
    """Get task run status.

    Args:
        run_id: Task run ID

    Returns:
        Task run metadata
    """
    run = await _get_task_run_internal(run_id, client)
    await store.update_run_status(
        run.run_id,
        status=run.status,
        output_payload=run.model_dump(mode="json"),
        summary=_summarize_task_run(run),
    )
    return run


@router.get("/runs/{run_id}/input", operation_id="get_task_run_input")
async def get_task_run_input(
    run_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> Dict[str, Any]:
    """Get task run input.

    Args:
        run_id: Task run ID

    Returns:
        Task input configuration
    """
    try:
        response = await client.get(
            f"/v1/tasks/runs/{run_id}/input",
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get(
    "/runs/{run_id}/result",
    response_model=Union[TaskRunResult, BetaTaskRunResult],
    operation_id="get_task_run_result",
)
async def get_task_run_result(
    run_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
    store: ContextStateStore = Depends(get_state_store),
) -> Union[TaskRunResult, BetaTaskRunResult]:
    """Get task run result (blocks until complete).

    Args:
        run_id: Task run ID

    Returns:
        Completed task result
    """
    result = await _get_task_run_result_internal(run_id, client)
    _, _, summary = _prepare_output_summary(result)
    await store.update_run_status(
        run_id,
        status=result.run.status,
        output_payload=result.model_dump(mode="json"),
        summary=summary,
    )
    return result


@router.post("/runs/managed", response_model=ManagedTaskRunResponse, operation_id="manage_task_run")
async def create_managed_task_run(
    request: BetaTaskRunInput,
    poll_interval_seconds: float = Query(2.0, ge=0.5, le=15.0),
    timeout_seconds: int = Query(900, ge=30, le=7200),
    client: httpx.AsyncClient = Depends(get_http_client),
    context_id: Optional[str] = Query(
        default=None,
        description="Optional context identifier used to persist this managed task run.",
    ),
    store: ContextStateStore = Depends(get_state_store),
) -> ManagedTaskRunResponse:
    """Create a task run and actively poll until completion.

    This endpoint lets MCP agents own the full lifecycle of a background task without
    sending humans to the Parallel UI to check status pages.
    """
    # Fix input format based on task_spec presence
    request.input = fix_task_input(request.input, request.task_spec.model_dump() if request.task_spec else None)
    if request.metadata:
        request.metadata = fix_metadata_parameter(request.metadata)

    # Store in MCP state for tracking across conversations
    mcp_store = get_mcp_state_store()

    payload = request.model_dump(exclude_none=True)
    try:
        task_run = await _create_task_run_internal(request, client)
        # Store successful task run
        mcp_store.save_task_run(
            task_run_id=task_run.run_id,
            processor=request.processor,
            input_data=request.input,
            task_spec=request.task_spec.model_dump() if request.task_spec else None,
            status=task_run.status,
            metadata=request.metadata
        )
    except HTTPException as exc:
        if context_id:
            await store.ensure_context(context_id)
            await store.record_interaction(
                context_id=context_id,
                interaction_type="task.run.managed",
                status="failed",
                input_payload=payload,
                output_payload=_normalize_error_detail(exc.detail),
                summary=f"Managed task creation failed ({exc.status_code}).",
            )
        raise

    if context_id:
        await store.ensure_context(context_id)
        await store.record_interaction(
            context_id=context_id,
            interaction_type="task.run.managed",
            status=task_run.status,
            input_payload=payload,
            output_payload=task_run.model_dump(mode="json"),
            summary="Managed task run created.",
            remote_run_id=task_run.run_id,
        )
    status_updates = [
        ManagedTaskStatusUpdate(
            status=task_run.status,
            timestamp=datetime.utcnow(),
            detail="Task run created",
        )
    ]

    start_time = time.monotonic()
    latest_status = task_run
    poll_count = 0

    try:
        while latest_status.status not in _TERMINAL_TASK_STATUSES:
            elapsed = time.monotonic() - start_time
            if elapsed > timeout_seconds:
                raise HTTPException(
                    status_code=504,
                    detail={
                        "error": "Task run monitoring timed out",
                        "run_id": task_run.run_id,
                        "status": latest_status.status,
                    },
                )

            await asyncio.sleep(poll_interval_seconds)
            poll_count += 1
            latest_status = await _get_task_run_internal(task_run.run_id, client)

            if status_updates[-1].status != latest_status.status:
                status_updates.append(
                    ManagedTaskStatusUpdate(
                        status=latest_status.status,
                        timestamp=datetime.utcnow(),
                        detail=f"Status changed to {latest_status.status}",
                    )
                )
    except HTTPException as exc:
        if context_id:
            await store.update_run_status(
                task_run.run_id,
                status="failed",
                output_payload=_normalize_error_detail(exc.detail),
                summary=f"Managed task run failed ({exc.status_code}).",
            )
        raise

    raw_result: Optional[Dict[str, Any]] = None
    output_text: Optional[str] = None
    output_json: Optional[Any] = None  # Can be any JSON-serializable type
    summary = f"Task ended in {latest_status.status} status."
    error_message: Optional[str] = None

    result_model: Optional[Union[TaskRunResult, BetaTaskRunResult]] = None

    if latest_status.status == "completed":
        result_model = await _get_task_run_result_internal(task_run.run_id, client)
    else:
        try:
            result_model = await _get_task_run_result_internal(task_run.run_id, client)
        except HTTPException as exc:
            detail = exc.detail
            if isinstance(detail, dict):
                error_message = json.dumps(detail)
            else:
                error_message = str(detail)

    if result_model:
        raw_result = result_model.model_dump(mode="json")
        output_text, output_json, summary = _prepare_output_summary(result_model)
    elif latest_status.status != "completed" and not error_message:
        error_message = "Task did not complete successfully."

    elapsed_seconds = round(time.monotonic() - start_time, 3)

    managed_response = ManagedTaskRunResponse(
        run=latest_status,
        run_id=task_run.run_id,
        final_status=latest_status.status,
        summary=summary,
        status_updates=status_updates,
        elapsed_seconds=elapsed_seconds,
        poll_count=poll_count,
        output_text=output_text,
        output_json=output_json,
        raw_result=raw_result,
        error=error_message,
    )

    if context_id:
        await store.update_run_status(
            task_run.run_id,
            status=managed_response.final_status,
            output_payload=managed_response.model_dump(mode="json"),
            summary=managed_response.summary,
        )

    return managed_response


async def stream_task_events(run_id: str, client: httpx.AsyncClient) -> AsyncGenerator[str, None]:
    """Stream task run events."""
    try:
        async with client.stream(
            "GET",
            f"/v1beta/tasks/runs/{run_id}/events",
            headers=get_auth_headers(),
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip():
                    yield line + "\n"
    except httpx.HTTPStatusError as e:
        error_data = {"error": f"Stream failed: {e.response.status_code}"}
        yield f"event: error\ndata: {json.dumps(error_data)}\n\n"


@router.get("/runs/{run_id}/events", operation_id="stream_task_run_events")
async def stream_task_run_events(
    run_id: str,
    req: Request,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Stream task run events via SSE.

    Args:
        run_id: Task run ID

    Returns:
        Server-sent events stream
    """
    return EventSourceResponse(stream_task_events(run_id, client))
