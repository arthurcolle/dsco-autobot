"""Storage API routes - Retrieve stored tasks, searches, and results from previous sessions."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from parallel_websearch_server.local_store import get_local_store
from parallel_websearch_server.mcp_state_store import get_mcp_state_store

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/stored", tags=["Storage"])


class StoredTaskRun(BaseModel):
    """Stored task run from database."""
    run_id: str
    processor: str
    status: str
    input_text: Optional[str] = None
    input_json: Optional[Any] = None  # Can be any JSON-serializable type
    output_text: Optional[str] = None
    output_json: Optional[Any] = None  # Can be any JSON-serializable type
    task_spec: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    modified_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None


class StoredSearch(BaseModel):
    """Stored search request from database."""
    id: Optional[int] = None
    request_id: Optional[str] = None
    objective: str
    search_queries: Optional[List[str]] = None
    max_results: Optional[int] = None
    results_count: Optional[int] = None
    results: Optional[List[Dict[str, Any]]] = None
    created_at: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None


class StoredFindAllRun(BaseModel):
    """Stored FindAll run from database."""
    findall_id: str
    objective: str
    status: str
    entity_type: Optional[str] = None
    generator: Optional[str] = None
    match_limit: Optional[int] = None
    candidates_found: int = 0
    candidates_matched: int = 0
    termination_reason: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    modified_at: Optional[str] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None


class StorageStats(BaseModel):
    """Statistics about stored data."""
    task_runs: Dict[str, int]
    findall_runs: Dict[str, int]
    chat_completions: Dict[str, int]
    search_requests: Dict[str, int]
    extract_requests: Dict[str, int]
    sqlite_size_mb: Optional[float] = None
    duckdb_size_mb: Optional[float] = None
    duckdb_available: Optional[bool] = None


@router.get("/tasks", response_model=List[StoredTaskRun], operation_id="list_stored_tasks")
async def list_stored_tasks(
    status: Optional[str] = Query(None, description="Filter by status"),
    processor: Optional[str] = Query(None, description="Filter by processor"),
    limit: int = Query(50, ge=1, le=200, description="Maximum results to return"),
) -> List[StoredTaskRun]:
    """List stored task runs from previous sessions.

    This allows retrieving tasks created in previous conversations.
    """
    try:
        store = get_local_store()
        tasks = store.list_task_runs(status=status, processor=processor, limit=limit)
        return [StoredTaskRun(**task) for task in tasks]
    except Exception as e:
        logger.error(f"Failed to list stored tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{run_id}", response_model=StoredTaskRun, operation_id="get_stored_task")
async def get_stored_task(run_id: str) -> StoredTaskRun:
    """Get a specific stored task run by ID.

    Useful for retrieving task details from a previous conversation.
    """
    try:
        store = get_local_store()
        task = store.get_task_run(run_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {run_id} not found")
        return StoredTaskRun(**task)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get stored task {run_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/ids/recent", response_model=List[str], operation_id="get_recent_task_ids")
async def get_recent_task_ids(
    limit: int = Query(20, ge=1, le=100, description="Maximum IDs to return"),
) -> List[str]:
    """Get recent task run IDs.

    Quick way to see what tasks exist without fetching full details.
    """
    try:
        store = get_local_store()
        return store.get_recent_task_ids(limit=limit)
    except Exception as e:
        logger.error(f"Failed to get recent task IDs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/searches", response_model=List[StoredSearch], operation_id="list_stored_searches")
async def list_stored_searches(
    limit: int = Query(50, ge=1, le=200, description="Maximum results to return"),
) -> List[StoredSearch]:
    """List stored search requests from previous sessions."""
    try:
        store = get_local_store()
        searches = store.list_search_requests(limit=limit)
        return [StoredSearch(**s) for s in searches]
    except Exception as e:
        logger.error(f"Failed to list stored searches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/findall", response_model=List[StoredFindAllRun], operation_id="list_stored_findall")
async def list_stored_findall(
    status: Optional[str] = Query(None, description="Filter by status"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(50, ge=1, le=200, description="Maximum results to return"),
) -> List[StoredFindAllRun]:
    """List stored FindAll runs from previous sessions."""
    try:
        store = get_local_store()
        runs = store.list_findall_runs(status=status, entity_type=entity_type, limit=limit)
        return [StoredFindAllRun(**r) for r in runs]
    except Exception as e:
        logger.error(f"Failed to list stored findall runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/findall/{findall_id}", response_model=StoredFindAllRun, operation_id="get_stored_findall")
async def get_stored_findall(findall_id: str) -> StoredFindAllRun:
    """Get a specific stored FindAll run by ID."""
    try:
        store = get_local_store()
        run = store.get_findall_run(findall_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"FindAll {findall_id} not found")
        return StoredFindAllRun(**run)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get stored findall {findall_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=StorageStats, operation_id="get_storage_stats")
async def get_storage_stats() -> StorageStats:
    """Get statistics about stored data."""
    try:
        store = get_local_store()
        stats = store.get_stats()
        return StorageStats(**stats)
    except Exception as e:
        logger.error(f"Failed to get storage stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search", operation_id="search_stored_data")
async def search_stored_data(
    query: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results per type"),
) -> Dict[str, List[Dict[str, Any]]]:
    """Search across all stored data (tasks, searches, findall runs).

    Searches by ID, input text, output text, objective, etc.
    """
    try:
        store = get_local_store()
        return store.search_all(query=query, limit=limit)
    except Exception as e:
        logger.error(f"Failed to search stored data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Local MCP state (SQLite) endpoints for faster access
@router.get("/local/tasks", operation_id="list_local_tasks")
async def list_local_tasks(
    limit: int = Query(10, ge=1, le=50),
) -> List[Dict[str, Any]]:
    """List recent tasks from local MCP state (faster than Turso)."""
    try:
        mcp_store = get_mcp_state_store()
        return mcp_store.list_recent_task_runs(limit=limit)
    except Exception as e:
        logger.error(f"Failed to list local tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/local/task-groups", operation_id="list_local_task_groups")
async def list_local_task_groups(
    limit: int = Query(10, ge=1, le=50),
) -> List[Dict[str, Any]]:
    """List recent task groups from local MCP state."""
    try:
        mcp_store = get_mcp_state_store()
        return mcp_store.list_recent_task_groups(limit=limit)
    except Exception as e:
        logger.error(f"Failed to list local task groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/local/searches", operation_id="list_local_searches")
async def list_local_searches(
    limit: int = Query(10, ge=1, le=50),
) -> List[Dict[str, Any]]:
    """List recent searches from local MCP state."""
    try:
        mcp_store = get_mcp_state_store()
        return mcp_store.get_recent_searches(limit=limit)
    except Exception as e:
        logger.error(f"Failed to list local searches: {e}")
        raise HTTPException(status_code=500, detail=str(e))
