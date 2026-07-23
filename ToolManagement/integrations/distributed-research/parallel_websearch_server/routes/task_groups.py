"""Task Group API routes (Beta)."""

import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from parallel_websearch_server.sse import EventSourceResponse

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.models import (
    CreateTaskGroupRequest,
    TaskGroupResponse,
    TaskGroupRunRequest,
    TaskGroupRunResponse,
    TaskRun,
)
from parallel_websearch_server.mcp_param_fixes import fix_metadata_parameter, fix_runs_parameter
from parallel_websearch_server.mcp_state_store import get_mcp_state_store
from parallel_websearch_server.local_store import get_local_store
from parallel_websearch_server.events import emit_task_group_created, emit_task_group_updated, emit_task_run_created, emit_task_run_status

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1beta/tasks/groups", tags=["Task Groups"])


@router.post("", response_model=TaskGroupResponse, status_code=201, operation_id="create_group")
async def create_task_group(
    request: CreateTaskGroupRequest,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> TaskGroupResponse:
    """Create a new task group.

    Args:
        request: Task group configuration (metadata, tags, etc.)

    Returns:
        Created task group
    """
    if request.metadata:
        request.metadata = fix_metadata_parameter(request.metadata)

    mcp_store = get_mcp_state_store()

    try:
        response = await client.post(
            "/v1beta/tasks/groups",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        result = TaskGroupResponse(**response.json())

        # Store group in MCP state (in-memory)
        # Status is now a nested object with num_task_runs, task_run_status_counts, is_active
        status_counts = result.status.task_run_status_counts if result.status else {}
        mcp_store.save_task_group(
            taskgroup_id=result.taskgroup_id,
            status="active" if (result.status and result.status.is_active) else "inactive",
            metadata=request.metadata,
            total_runs=result.status.num_task_runs if result.status else 0,
            completed_runs=status_counts.get("completed", 0),
            failed_runs=status_counts.get("failed", 0)
        )

        # Persist to local SQLite
        try:
            local_store = get_local_store()
            local_store.save_task_group(
                taskgroup_id=result.taskgroup_id,
                status="active" if (result.status and result.status.is_active) else "inactive",
                metadata=request.metadata,
                num_task_runs=result.status.num_task_runs if result.status else 0,
                completed_runs=status_counts.get("completed", 0),
                failed_runs=status_counts.get("failed", 0)
            )
        except Exception as e:
            logger.warning("Failed to persist task group to local store: %s", e)

        # Emit event for UI updates
        emit_task_group_created(result.taskgroup_id, request.metadata)

        return result
    except httpx.HTTPStatusError as e:
        logger.error(f"Task group creation failed: {e.response.status_code}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.json() if e.response.text else {"error": "Group creation failed"},
        )


@router.get("/{taskgroup_id}", response_model=TaskGroupResponse, operation_id="get_group")
async def get_task_group(
    taskgroup_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> TaskGroupResponse:
    """Get task group status.

    Args:
        taskgroup_id: Task group ID

    Returns:
        Task group metadata and status
    """
    try:
        response = await client.get(
            f"/v1beta/tasks/groups/{taskgroup_id}",
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        result = TaskGroupResponse(**response.json())

        # Sync status to local storage (delta update)
        try:
            status_counts = result.status.task_run_status_counts if result.status else {}
            status_str = "active" if (result.status and result.status.is_active) else "completed"

            # Update MCP state store
            mcp_store = get_mcp_state_store()
            mcp_store.save_task_group(
                taskgroup_id=result.taskgroup_id,
                status=status_str,
                metadata=result.metadata,
                total_runs=result.status.num_task_runs if result.status else 0,
                completed_runs=status_counts.get("completed", 0),
                failed_runs=status_counts.get("failed", 0)
            )

            # Update local SQLite store
            local_store = get_local_store()
            local_store.save_task_group(
                taskgroup_id=result.taskgroup_id,
                status=status_str,
                metadata=result.metadata,
                num_task_runs=result.status.num_task_runs if result.status else 0,
                completed_runs=status_counts.get("completed", 0),
                failed_runs=status_counts.get("failed", 0)
            )

            # Emit status update event
            emit_task_group_updated(result.taskgroup_id, status_str, status_counts)
        except Exception as e:
            logger.warning(f"Failed to sync task group status to local store: {e}")

        return result
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.post("/{taskgroup_id}/runs", response_model=TaskGroupRunResponse, operation_id="add_group_runs")
async def add_runs_to_group(
    taskgroup_id: str,
    request: TaskGroupRunRequest,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> TaskGroupRunResponse:
    """Add task runs to a group.

    Args:
        taskgroup_id: Task group ID
        request: List of task run configurations

    Returns:
        Created run IDs
    """
    request.runs = fix_runs_parameter(request.runs)
    mcp_store = get_mcp_state_store()

    try:
        response = await client.post(
            f"/v1beta/tasks/groups/{taskgroup_id}/runs",
            json=request.model_dump(exclude_none=True, by_alias=True),
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        result = TaskGroupRunResponse(**response.json())

        # Store run IDs in group state
        group = mcp_store.get_task_group(taskgroup_id)
        if group:
            existing_runs = group.get("run_ids") or []
            existing_runs.extend(result.run_ids)
            mcp_store.save_task_group(
                taskgroup_id=taskgroup_id,
                status=group.get("status", "running"),
                metadata=group.get("metadata"),
                run_ids=existing_runs,
                total_runs=len(existing_runs)
            )

        # Store each run individually
        local_store = get_local_store()
        for i, run_id in enumerate(result.run_ids):
            if i < len(request.runs):
                run = request.runs[i]
                # run is a dict after fix_runs_parameter
                processor = run.get("processor") if isinstance(run, dict) else run.processor
                input_data = run.get("input") if isinstance(run, dict) else run.input
                task_spec = run.get("task_spec") if isinstance(run, dict) else run.task_spec
                metadata = run.get("metadata") if isinstance(run, dict) else run.metadata

                mcp_store.save_task_run(
                    task_run_id=run_id,
                    processor=processor,
                    input_data=input_data,
                    task_spec=task_spec.model_dump() if hasattr(task_spec, 'model_dump') else task_spec,
                    status="queued",
                    metadata=metadata
                )
                # Persist to local SQLite
                try:
                    processor_val = processor.value if hasattr(processor, 'value') else str(processor)
                    input_text = input_data if isinstance(input_data, str) else None
                    input_json = input_data if isinstance(input_data, dict) else None
                    local_store.save_task_run(
                        run_id=run_id,
                        processor=processor_val,
                        status="queued",
                        input_text=input_text,
                        input_json=input_json,
                        task_spec=task_spec.model_dump() if hasattr(task_spec, 'model_dump') else task_spec,
                        metadata=metadata,
                        taskgroup_id=taskgroup_id,
                    )
                    # Emit event for UI updates
                    emit_task_run_created(run_id, processor_val, taskgroup_id, input_text)
                except Exception as e:
                    logger.warning("Failed to persist task run to local store: %s", e)

        return result
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


async def stream_group_runs_with_filter(
    taskgroup_id: str,
    include_input: bool,
    include_output: bool,
    last_event_id: str,
    status: str,
    client: httpx.AsyncClient,
) -> AsyncGenerator[str, None]:
    """Stream task group runs with optional status filter."""
    params = {}
    if include_input:
        params["include_input"] = "true"
    if include_output:
        params["include_output"] = "true"
    if last_event_id:
        params["last_event_id"] = last_event_id
    if status:
        valid_statuses = ["queued", "action_required", "running", "completed", "failed", "cancelling", "cancelled"]
        if status not in valid_statuses:
            error_data = {"error": f"Invalid status filter. Must be one of: {', '.join(valid_statuses)}"}
            yield f"event: error\ndata: {json.dumps(error_data)}\n\n"
            return
        params["status"] = status

    try:
        async with client.stream(
            "GET",
            f"/v1beta/tasks/groups/{taskgroup_id}/runs",
            params=params,
            headers=get_auth_headers(),
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip():
                    yield line + "\n"
    except httpx.HTTPStatusError as e:
        error_data = {"error": f"Stream failed: {e.response.status_code}"}
        yield f"event: error\ndata: {json.dumps(error_data)}\n\n"


@router.get("/{taskgroup_id}/runs", operation_id="get_group_runs")
async def fetch_task_group_runs(
    taskgroup_id: str,
    include_input: bool = False,
    include_output: bool = False,
    last_event_id: str = None,
    status: str = None,
    req: Request = None,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Fetch task group runs (streaming).

    Args:
        taskgroup_id: Task group ID
        include_input: Include run inputs
        include_output: Include run outputs
        last_event_id: Resume from event ID
        status: Filter by status

    Returns:
        Server-sent events stream
    """
    return EventSourceResponse(
        stream_group_runs_with_filter(taskgroup_id, include_input, include_output, last_event_id, status, client)
    )


@router.get("/{taskgroup_id}/runs/{run_id}", response_model=TaskRun, operation_id="get_group_run")
async def get_task_group_run(
    taskgroup_id: str,
    run_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> TaskRun:
    """Get specific task run from group.

    Args:
        taskgroup_id: Task group ID
        run_id: Task run ID

    Returns:
        Task run metadata
    """
    try:
        response = await client.get(
            f"/v1beta/tasks/groups/{taskgroup_id}/runs/{run_id}",
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        return TaskRun(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


async def stream_group_events(
    taskgroup_id: str,
    last_event_id: str,
    timeout: int,
    client: httpx.AsyncClient
) -> AsyncGenerator[str, None]:
    """Stream task group events."""
    params = {}
    if last_event_id:
        params["last_event_id"] = last_event_id
    if timeout:
        params["timeout"] = timeout

    try:
        async with client.stream(
            "GET",
            f"/v1beta/tasks/groups/{taskgroup_id}/events",
            params=params,
            headers=get_auth_headers(),
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip():
                    yield line + "\n"
    except httpx.HTTPStatusError as e:
        error_data = {"error": f"Stream failed: {e.response.status_code}"}
        yield f"event: error\ndata: {json.dumps(error_data)}\n\n"


@router.get("/{taskgroup_id}/events", operation_id="stream_group_events")
async def stream_task_group_events(
    taskgroup_id: str,
    last_event_id: str = None,
    timeout: int = None,
    req: Request = None,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Stream task group events via SSE.

    Args:
        taskgroup_id: Task group ID
        last_event_id: Resume from event ID
        timeout: Connection timeout in seconds

    Returns:
        Server-sent events stream
    """
    return EventSourceResponse(stream_group_events(taskgroup_id, last_event_id, timeout, client))
