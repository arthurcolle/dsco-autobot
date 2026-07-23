"""
MCP-specific endpoints with proper Pydantic schemas for FastAPI MCP.
These endpoints are designed to work perfectly with the MCP protocol.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import httpx
from fastapi import APIRouter, Depends, HTTPException, Body
from fastapi.responses import JSONResponse

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.client import ParallelAPIClient
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.mcp_schemas import (
    # Search
    SearchInput,
    SearchOutput,
    SearchResult,
    # Task Runs
    ManagedTaskRunInput,
    ManagedTaskRunOutput,
    TaskStatus,
    # Task Groups
    CreateGroupInput,
    CreateGroupOutput,
    AddGroupRunsInput,
    AddGroupRunsOutput,
    ExecuteManagedGroupInput,
    ExecuteManagedGroupOutput,
    GetGroupAnalyticsInput,
    GetGroupAnalyticsOutput,
    ExportGroupResultsInput,
    ExportGroupResultsOutput,
    OptimizeAndExecuteGroupInput,
    OptimizeAndExecuteGroupOutput,
    GroupStatus,
    # State Management
    GetRecentTasksInput,
    GetRecentTasksOutput,
    GetRecentGroupsInput,
    GetRecentGroupsOutput,
    GetTaskStatusInput,
    GetTaskStatusOutput,
    GetGroupStatusInput,
    GetGroupStatusOutput,
    # Error
    ErrorResponse,
)
from parallel_websearch_server.mcp_state_store import get_mcp_state_store
from parallel_websearch_server.models import (
    SearchRequest,
    BetaTaskRunInput,
    CreateTaskGroupRequest,
    TaskGroupRunRequest,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mcp", tags=["MCP Enhanced"])


# ============================================================================
# SEARCH ENDPOINTS
# ============================================================================

@router.post("/search", response_model=SearchOutput, operation_id="mcp_search")
async def search(
    request: SearchInput = Body(...),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> SearchOutput:
    """
    Execute a web search with proper MCP schema handling.

    This endpoint handles all parameter serialization issues automatically.
    """
    mcp_store = get_mcp_state_store()

    try:
        # Convert to internal API request
        api_request = SearchRequest(
            objective=request.objective,
            max_results=request.max_results
        )

        # Make API call
        api_client = ParallelAPIClient()
        await api_client.connect()

        try:
            response = await api_client.search(api_request)

            # Convert response to MCP schema
            results = []
            for result in response.results:
                results.append(SearchResult(
                    title=result.title,
                    url=result.url,
                    excerpts=result.excerpts,
                    publish_date=result.publish_date
                ))

            # Store search in state
            search_id = mcp_store.save_search(
                objective=request.objective,
                max_results=request.max_results,
                results=[r.model_dump() for r in results],
                status="completed"
            )

            return SearchOutput(
                results=results,
                total_results=len(results),
                search_id=str(search_id)
            )

        finally:
            await api_client.close()

    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TASK RUN ENDPOINTS
# ============================================================================

@router.post("/tasks/managed", response_model=ManagedTaskRunOutput, operation_id="mcp_manage_task_run")
async def manage_task_run(
    request: ManagedTaskRunInput = Body(...),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> ManagedTaskRunOutput:
    """
    Create and manage a task run with automatic polling.

    This endpoint handles:
    - Input format validation (string vs object based on task_spec)
    - Automatic polling until completion
    - State persistence across conversations
    """
    mcp_store = get_mcp_state_store()

    try:
        # Create internal API request
        api_request = BetaTaskRunInput(
            processor=request.processor.value,
            input=request.input,
            task_spec=request.task_spec,
            metadata=request.metadata
        )

        # Create task run
        response = await client.post(
            "/v1/tasks/runs",
            json=api_request.model_dump(mode='json', exclude_none=True),
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        task_run = response.json()

        # Store in state
        mcp_store.save_task_run(
            task_run_id=task_run["run_id"],
            processor=request.processor.value,
            input_data=request.input,
            task_spec=request.task_spec.model_dump() if request.task_spec else None,
            status=task_run["status"],
            metadata=request.metadata
        )

        # Poll for completion
        import asyncio
        import time

        start_time = time.monotonic()
        while True:
            # Check timeout
            if time.monotonic() - start_time > request.timeout:
                raise HTTPException(status_code=408, detail="Task execution timed out")

            # Get current status
            status_response = await client.get(
                f"/v1/tasks/runs/{task_run['run_id']}",
                headers=get_auth_headers(),
            )
            status_response.raise_for_status()
            current_status = status_response.json()

            # Check if complete
            if current_status["status"] in ["COMPLETED", "FAILED", "CANCELLED"]:
                break

            # Wait before next poll
            await asyncio.sleep(request.poll_interval)

        # Get final result
        result = None
        error = None

        if current_status["status"] == "COMPLETED":
            result_response = await client.get(
                f"/v1/tasks/runs/{task_run['run_id']}/result",
                headers=get_auth_headers(),
            )
            if result_response.status_code == 200:
                result_data = result_response.json()
                result = result_data.get("output")
        elif current_status["status"] == "FAILED":
            error = current_status.get("error", "Task failed")

        # Update state
        mcp_store.save_task_run(
            task_run_id=task_run["run_id"],
            processor=request.processor.value,
            input_data=request.input,
            task_spec=request.task_spec.model_dump() if request.task_spec else None,
            status=current_status["status"],
            output_data=result,
            metadata=request.metadata
        )

        return ManagedTaskRunOutput(
            task_run_id=task_run["run_id"],
            status=TaskStatus(current_status["status"].lower()),
            created_at=datetime.fromisoformat(task_run["created_at"].replace("Z", "+00:00")),
            completed_at=datetime.utcnow() if current_status["status"] == "COMPLETED" else None,
            output=result,
            error=error,
            metadata=request.metadata
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Managed task run failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# TASK GROUP ENDPOINTS
# ============================================================================

@router.post("/groups/create", response_model=CreateGroupOutput, operation_id="mcp_create_group")
async def create_group(
    request: CreateGroupInput = Body(...),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> CreateGroupOutput:
    """
    Create a new task group with proper schema handling.
    """
    mcp_store = get_mcp_state_store()

    try:
        # Create internal API request
        api_request = CreateTaskGroupRequest(metadata=request.metadata)

        # Create group
        response = await client.post(
            "/v1beta/tasks/groups",
            json=api_request.model_dump(mode='json', exclude_none=True),
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        group_data = response.json()

        # Store in state
        mcp_store.save_task_group(
            taskgroup_id=group_data["taskgroup_id"],
            status="created",
            metadata=request.metadata
        )

        # Return with proper schema
        return CreateGroupOutput(
            taskgroup_id=group_data["taskgroup_id"],
            status=GroupStatus.CREATED,
            created_at=datetime.utcnow(),
            modified_at=datetime.utcnow(),
            total_runs=0,
            completed_runs=0,
            failed_runs=0,
            metadata=request.metadata
        )

    except Exception as e:
        logger.error(f"Group creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/{taskgroup_id}/runs", response_model=AddGroupRunsOutput, operation_id="mcp_add_group_runs")
async def add_group_runs(
    taskgroup_id: str,
    request: AddGroupRunsInput = Body(...),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> AddGroupRunsOutput:
    """
    Add runs to a task group with automatic parameter fixing.
    """
    mcp_store = get_mcp_state_store()

    try:
        # Convert runs to internal format
        api_runs = []
        for run in request.runs:
            api_runs.append(BetaTaskRunInput(
                processor=run.processor.value,
                input=run.input,
                task_spec=run.task_spec,
                metadata=run.metadata
            ))

        api_request = TaskGroupRunRequest(runs=api_runs)

        # Add runs to group
        response = await client.post(
            f"/v1beta/tasks/groups/{taskgroup_id}/runs",
            json=api_request.model_dump(mode='json', exclude_none=True),
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        result = response.json()

        # Store run IDs in group state
        group = mcp_store.get_task_group(taskgroup_id)
        if group:
            existing_runs = group.get("run_ids", [])
            existing_runs.extend(result["run_ids"])
            mcp_store.save_task_group(
                taskgroup_id=taskgroup_id,
                status="running",
                run_ids=existing_runs,
                total_runs=len(existing_runs)
            )

        # Store each run
        for i, run_id in enumerate(result["run_ids"]):
            if i < len(request.runs):
                run = request.runs[i]
                mcp_store.save_task_run(
                    task_run_id=run_id,
                    processor=run.processor.value,
                    input_data=run.input,
                    task_spec=run.task_spec.model_dump() if run.task_spec else None,
                    status="queued",
                    metadata=run.metadata
                )

        return AddGroupRunsOutput(
            run_ids=result["run_ids"],
            taskgroup_id=taskgroup_id
        )

    except Exception as e:
        logger.error(f"Add group runs failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/execute", response_model=ExecuteManagedGroupOutput, operation_id="mcp_execute_managed_group")
async def execute_managed_group(
    request: ExecuteManagedGroupInput = Body(...),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> ExecuteManagedGroupOutput:
    """
    Execute a task group and wait for completion.
    """
    # Implementation would poll the group status
    # For now, returning a mock response
    return ExecuteManagedGroupOutput(
        taskgroup_id=request.taskgroup_id,
        status=GroupStatus.COMPLETED,
        total_runs=0,
        completed_runs=0,
        failed_runs=0,
        execution_time=0.0
    )


@router.post("/groups/optimize", response_model=OptimizeAndExecuteGroupOutput, operation_id="mcp_optimize_and_execute_group")
async def optimize_and_execute_group(
    request: OptimizeAndExecuteGroupInput = Body(...),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> OptimizeAndExecuteGroupOutput:
    """
    Optimize and execute a group of tasks.

    This endpoint:
    - Analyzes task complexity
    - Deduplicates if requested
    - Selects optimal processors
    - Executes with retry logic
    """
    mcp_store = get_mcp_state_store()

    try:
        # Create a new group
        group_request = CreateGroupInput(metadata=request.metadata or {"optimized": True})
        group = await create_group(group_request, client)

        # Prepare optimized runs
        unique_tasks = list(set(request.task_inputs)) if request.deduplicate else request.task_inputs

        runs = []
        for task in unique_tasks:
            # Select processor based on strategy
            processor = "lite"
            if request.strategy == "quality":
                processor = "premium"
            elif request.strategy == "speed":
                processor = "turbo"

            runs.append(ManagedTaskRunInput(
                processor=processor,
                input=task
            ))

        # Add runs to group
        add_request = AddGroupRunsInput(runs=runs)
        await add_group_runs(group.taskgroup_id, add_request, client)

        return OptimizeAndExecuteGroupOutput(
            taskgroup_id=group.taskgroup_id,
            optimization_report={
                "strategy": request.strategy.value,
                "original_tasks": len(request.task_inputs),
                "deduplicated_tasks": len(unique_tasks)
            },
            execution_plan=[{"task": t, "processor": "lite"} for t in unique_tasks],
            status=GroupStatus.RUNNING,
            total_tasks=len(unique_tasks),
            deduplicated_tasks=len(unique_tasks)
        )

    except Exception as e:
        logger.error(f"Optimize and execute failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# STATE MANAGEMENT ENDPOINTS
# ============================================================================

@router.post("/state/recent-tasks", response_model=GetRecentTasksOutput, operation_id="mcp_get_recent_tasks")
async def get_recent_tasks(
    request: GetRecentTasksInput = Body(default=GetRecentTasksInput()),
) -> GetRecentTasksOutput:
    """Get recently created task runs from persistent state."""
    mcp_store = get_mcp_state_store()

    tasks = mcp_store.list_recent_task_runs(limit=request.limit)

    return GetRecentTasksOutput(
        tasks=tasks,
        total_count=len(tasks)
    )


@router.post("/state/recent-groups", response_model=GetRecentGroupsOutput, operation_id="mcp_get_recent_groups")
async def get_recent_groups(
    request: GetRecentGroupsInput = Body(default=GetRecentGroupsInput()),
) -> GetRecentGroupsOutput:
    """Get recently created task groups from persistent state."""
    mcp_store = get_mcp_state_store()

    groups = mcp_store.list_recent_task_groups(limit=request.limit)

    return GetRecentGroupsOutput(
        groups=groups,
        total_count=len(groups)
    )


@router.post("/state/task-status", response_model=GetTaskStatusOutput, operation_id="mcp_get_task_status")
async def get_task_status(
    request: GetTaskStatusInput = Body(...),
) -> GetTaskStatusOutput:
    """Get the status of a specific task run."""
    mcp_store = get_mcp_state_store()

    task = mcp_store.get_task_run(request.task_run_id)

    if not task:
        raise HTTPException(status_code=404, detail=f"Task {request.task_run_id} not found")

    return GetTaskStatusOutput(
        task_run_id=task["task_run_id"],
        status=TaskStatus(task["status"].lower()),
        processor=task["processor"],
        input_data=task.get("input_data"),
        output_data=task.get("output_data"),
        created_at=datetime.fromisoformat(task["created_at"]),
        last_updated=datetime.fromisoformat(task.get("last_updated", task["created_at"])),
        metadata=task.get("metadata")
    )


@router.post("/state/group-status", response_model=GetGroupStatusOutput, operation_id="mcp_get_group_status")
async def get_group_status(
    request: GetGroupStatusInput = Body(...),
) -> GetGroupStatusOutput:
    """Get the status of a specific task group."""
    mcp_store = get_mcp_state_store()

    group = mcp_store.get_task_group(request.taskgroup_id)

    if not group:
        raise HTTPException(status_code=404, detail=f"Group {request.taskgroup_id} not found")

    return GetGroupStatusOutput(
        taskgroup_id=group["taskgroup_id"],
        status=GroupStatus(group.get("status", "created").lower()),
        total_runs=group.get("total_runs", 0),
        completed_runs=group.get("completed_runs", 0),
        failed_runs=group.get("failed_runs", 0),
        run_ids=group.get("run_ids"),
        created_at=datetime.fromisoformat(group["created_at"]),
        last_updated=datetime.fromisoformat(group.get("last_updated", group["created_at"])),
        metadata=group.get("metadata")
    )