"""
Admin API Routes for Task Queue and Backend Management.

Provides endpoints for:
- Task lifecycle management (cancel, retry, reprioritize)
- Bulk operations (cancel stale, cleanup old)
- Backend registration and health monitoring
- Queue statistics and diagnostics
"""

from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from ..queue import (
    get_task_queue,
    Task,
    TaskStatus,
    TaskPriority,
    MCPBackend,
)

router = APIRouter(prefix="/admin", tags=["admin"])


# =============================================================================
# Request/Response Models
# =============================================================================

class EnqueueTaskRequest(BaseModel):
    name: str = Field(..., description="Task name/tool to execute")
    input_data: dict = Field(default_factory=dict, description="Input parameters")
    priority: int = Field(default=5, ge=0, le=100, description="Priority (0-100)")
    backend: Optional[str] = Field(None, description="Specific backend to use")
    timeout_seconds: int = Field(default=300, ge=1, le=3600)
    max_retries: int = Field(default=3, ge=0, le=10)
    metadata: Optional[dict] = None


class RegisterBackendRequest(BaseModel):
    id: str = Field(..., description="Unique backend ID")
    name: str = Field(..., description="Display name")
    url: str = Field(..., description="Base URL (e.g., http://localhost:2040)")
    health_endpoint: str = Field(default="/health")
    mcp_endpoint: str = Field(default="/mcp")
    capabilities: List[str] = Field(default_factory=list)
    max_concurrent: int = Field(default=10, ge=1, le=100)
    priority: int = Field(default=0, ge=0, le=100)
    metadata: Optional[dict] = None


class CancelTasksRequest(BaseModel):
    status: Optional[str] = Field(None, description="Filter by status")
    backend: Optional[str] = Field(None, description="Filter by backend")
    older_than_hours: Optional[int] = Field(None, ge=1, description="Cancel tasks older than N hours")
    name_pattern: Optional[str] = Field(None, description="Filter by name pattern")


class TaskResponse(BaseModel):
    id: str
    name: str
    status: str
    priority: int
    backend: Optional[str]
    created_at: Optional[str]
    started_at: Optional[str]
    completed_at: Optional[str]
    error: Optional[str]
    retries: int

    @classmethod
    def from_task(cls, task: Task) -> "TaskResponse":
        return cls(
            id=task.id,
            name=task.name,
            status=task.status.value,
            priority=task.priority.value,
            backend=task.backend,
            created_at=task.created_at.isoformat() if task.created_at else None,
            started_at=task.started_at.isoformat() if task.started_at else None,
            completed_at=task.completed_at.isoformat() if task.completed_at else None,
            error=task.error,
            retries=task.retries,
        )


class BackendResponse(BaseModel):
    id: str
    name: str
    url: str
    is_healthy: bool
    current_load: int
    max_concurrent: int
    last_health_check: Optional[str]
    capabilities: List[str]


# =============================================================================
# Task Management Endpoints
# =============================================================================

@router.post("/tasks", response_model=TaskResponse)
async def enqueue_task(request: EnqueueTaskRequest):
    """Enqueue a new task."""
    queue = get_task_queue()
    task = queue.enqueue(
        name=request.name,
        input_data=request.input_data,
        priority=TaskPriority(request.priority),
        backend=request.backend,
        timeout_seconds=request.timeout_seconds,
        max_retries=request.max_retries,
        metadata=request.metadata,
    )
    return TaskResponse.from_task(task)


@router.get("/tasks", response_model=List[TaskResponse])
async def list_tasks(
    status: Optional[str] = Query(None, description="Filter by status"),
    backend: Optional[str] = Query(None, description="Filter by backend"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List tasks with optional filtering."""
    queue = get_task_queue()
    task_status = TaskStatus(status) if status else None
    tasks = queue.list_tasks(status=task_status, backend=backend, limit=limit, offset=offset)
    return [TaskResponse.from_task(t) for t in tasks]


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str):
    """Get a specific task."""
    queue = get_task_queue()
    task = queue.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskResponse.from_task(task)


@router.get("/tasks/{task_id}/full")
async def get_task_full(task_id: str):
    """Get full task details including output data."""
    queue = get_task_queue()
    task = queue.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task.to_dict()


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(task_id: str, limit: int = Query(100, ge=1, le=1000)):
    """Get logs for a specific task."""
    queue = get_task_queue()
    logs = queue.get_task_logs(task_id, limit=limit)
    return {"task_id": task_id, "logs": logs}


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, reason: str = Query("Cancelled by admin")):
    """Cancel a specific task."""
    queue = get_task_queue()
    success = queue.cancel_task(task_id, reason=reason)
    if not success:
        raise HTTPException(status_code=400, detail="Cannot cancel task (not found or already terminal)")
    return {"success": True, "task_id": task_id, "message": f"Task cancelled: {reason}"}


@router.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str):
    """Retry a failed task."""
    queue = get_task_queue()
    success = queue.retry_task(task_id)
    if not success:
        raise HTTPException(status_code=400, detail="Cannot retry task (not found or not failed)")
    return {"success": True, "task_id": task_id, "message": "Task queued for retry"}


@router.post("/tasks/{task_id}/priority")
async def set_task_priority(task_id: str, priority: int = Query(..., ge=0, le=100)):
    """Change priority of a queued task."""
    queue = get_task_queue()
    success = queue.reprioritize_task(task_id, TaskPriority(priority))
    if not success:
        raise HTTPException(status_code=400, detail="Cannot reprioritize task (not found or not queued)")
    return {"success": True, "task_id": task_id, "priority": priority}


# =============================================================================
# Bulk Operations
# =============================================================================

@router.post("/tasks/cancel-bulk")
async def cancel_tasks_bulk(request: CancelTasksRequest):
    """Cancel multiple tasks matching criteria."""
    queue = get_task_queue()

    older_than = None
    if request.older_than_hours:
        older_than = datetime.utcnow() - timedelta(hours=request.older_than_hours)

    status = TaskStatus(request.status) if request.status else None

    count = queue.cancel_tasks_by_filter(
        status=status,
        backend=request.backend,
        older_than=older_than,
        name_pattern=request.name_pattern,
    )

    return {"success": True, "cancelled_count": count}


@router.post("/tasks/cancel-stale")
async def cancel_stale_tasks(older_than_hours: int = Query(24, ge=1, le=720)):
    """Cancel all queued tasks older than specified hours."""
    queue = get_task_queue()
    count = queue.cancel_stale_tasks(older_than_hours=older_than_hours)
    return {"success": True, "cancelled_count": count, "older_than_hours": older_than_hours}


@router.post("/tasks/cleanup")
async def cleanup_old_tasks(days: int = Query(30, ge=1, le=365)):
    """Delete completed/cancelled tasks older than specified days."""
    queue = get_task_queue()
    count = queue.cleanup_old_tasks(days=days)
    return {"success": True, "deleted_count": count, "older_than_days": days}


# =============================================================================
# Queue Statistics
# =============================================================================

@router.get("/stats")
async def get_queue_stats():
    """Get queue statistics and health."""
    queue = get_task_queue()
    return queue.get_queue_stats()


@router.get("/stats/summary")
async def get_queue_summary():
    """Get a quick summary of queue health."""
    queue = get_task_queue()
    stats = queue.get_queue_stats()

    return {
        "queue_depth": stats["queue_depth"],
        "running": stats["running"],
        "oldest_queued": stats["oldest_queued"],
        "backends_healthy": sum(1 for b in stats["backends"].values() if b["healthy"]),
        "backends_total": len(stats["backends"]),
        "status_breakdown": stats["by_status"],
    }


# =============================================================================
# Backend Management
# =============================================================================

@router.post("/backends")
async def register_backend(request: RegisterBackendRequest, background_tasks: BackgroundTasks):
    """Register a new MCP backend."""
    queue = get_task_queue()

    backend = MCPBackend(
        id=request.id,
        name=request.name,
        url=request.url,
        health_endpoint=request.health_endpoint,
        mcp_endpoint=request.mcp_endpoint,
        capabilities=request.capabilities,
        max_concurrent=request.max_concurrent,
        priority=request.priority,
        metadata=request.metadata or {},
    )

    queue.register_backend(backend)

    # Schedule health check
    background_tasks.add_task(queue.check_backend_health, request.id)

    return {"success": True, "backend_id": request.id, "message": f"Backend {request.name} registered"}


@router.delete("/backends/{backend_id}")
async def unregister_backend(backend_id: str):
    """Unregister an MCP backend."""
    queue = get_task_queue()
    queue.unregister_backend(backend_id)
    return {"success": True, "backend_id": backend_id, "message": "Backend unregistered"}


@router.get("/backends", response_model=List[BackendResponse])
async def list_backends():
    """List all registered backends."""
    queue = get_task_queue()
    backends = queue.list_backends()
    return [
        BackendResponse(
            id=b.id,
            name=b.name,
            url=b.url,
            is_healthy=b.is_healthy,
            current_load=b.current_load,
            max_concurrent=b.max_concurrent,
            last_health_check=b.last_health_check.isoformat() if b.last_health_check else None,
            capabilities=b.capabilities,
        )
        for b in backends
    ]


@router.post("/backends/{backend_id}/health-check")
async def check_backend_health(backend_id: str):
    """Trigger health check for a specific backend."""
    queue = get_task_queue()
    is_healthy = await queue.check_backend_health(backend_id)
    return {"backend_id": backend_id, "is_healthy": is_healthy}


@router.post("/backends/health-check-all")
async def check_all_backends_health():
    """Trigger health check for all backends."""
    queue = get_task_queue()
    results = await queue.check_all_backends()
    return {"results": results}


# =============================================================================
# Queue Control
# =============================================================================

@router.post("/queue/start")
async def start_queue():
    """Start the queue processor."""
    queue = get_task_queue()
    await queue.start()
    return {"success": True, "message": "Queue processor started"}


@router.post("/queue/stop")
async def stop_queue():
    """Stop the queue processor."""
    queue = get_task_queue()
    await queue.stop()
    return {"success": True, "message": "Queue processor stopped"}


@router.post("/sync-mcp-tools")
async def sync_mcp_tools(background_tasks: BackgroundTasks):
    """
    Fetch tools from all native MCP backends and register them in the registry.
    This enables semantic search to find tools from distributed MCP servers.
    """
    import httpx
    from ..registry import get_registry

    queue = get_task_queue()
    registry = get_registry()

    backends = queue.list_backends()
    synced_tools = []

    for backend in backends:
        try:
            async with httpx.AsyncClient() as client:
                # Fetch tools from MCP backend
                response = await client.post(
                    f"{backend.url}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {}
                    },
                    timeout=10.0
                )

                if response.status_code == 200:
                    result = response.json()
                    tools = result.get("result", {}).get("tools", [])

                    # Register each tool in the registry
                    for tool in tools:
                        from ..models.tool import Tool, ToolInput

                        # Convert MCP inputSchema to ToolInput list
                        inputs = []
                        input_schema = tool.get("inputSchema", {})
                        properties = input_schema.get("properties", {})
                        required_fields = input_schema.get("required", [])

                        for prop_name, prop_def in properties.items():
                            inputs.append(ToolInput(
                                name=prop_name,
                                type=prop_def.get("type", "string"),
                                description=prop_def.get("description", ""),
                                required=prop_name in required_fields
                            ))

                        tool_obj = Tool(
                            name=tool.get("name"),
                            description=tool.get("description", ""),
                            inputs=inputs,
                            category="mcp-native",
                            service_url=backend.url,
                            endpoint="/mcp",
                            tags=["mcp", backend.id]
                        )

                        # Register (will update if exists)
                        registry.register(tool_obj)
                        synced_tools.append(f"{backend.id}:{tool_obj.name}")

        except Exception as e:
            print(f"Error syncing tools from {backend.id}: {e}")
            continue

    return {
        "success": True,
        "synced_tools": synced_tools,
        "count": len(synced_tools),
        "message": f"Synced {len(synced_tools)} tools from {len(backends)} backends"
    }


@router.post("/embed-all-tools")
async def embed_all_tools():
    """
    Generate embeddings for all tools in the registry.
    This enables semantic search via the search_tools meta-tool.
    """
    from ..embeddings.semantic_tool_store import SemanticToolStore
    from ..registry import get_registry

    registry = get_registry()
    store = SemanticToolStore()

    # Get all tools from registry
    all_tools = []
    for tool_obj in registry.list_all():
        # Convert Tool object to dict format expected by SemanticToolStore
        tool_dict = {
            "name": tool_obj.name,
            "description": tool_obj.description,
            "inputSchema": tool_obj.to_json_schema(),
            "tags": tool_obj.tags,
            "category": tool_obj.category
        }
        all_tools.append((tool_dict, tool_obj.service_url))

    # Batch index all tools
    embedded_count = 0
    for tool_dict, backend_url in all_tools:
        try:
            await store.index_tool(tool_dict, backend=backend_url)
            embedded_count += 1
        except Exception as e:
            print(f"Error embedding {tool_dict['name']}: {e}")

    return {
        "success": True,
        "embedded_count": embedded_count,
        "total_tools": len(all_tools),
        "message": f"Embedded {embedded_count}/{len(all_tools)} tools"
    }


@router.get("/search-tools-semantic")
async def search_tools_semantic(
    query: str,
    limit: int = 10,
    min_similarity: float = 0.5
):
    """
    Semantic search across all embedded tools.
    Uses vector similarity to find relevant tools.
    """
    from ..embeddings.semantic_tool_store import SemanticToolStore

    store = SemanticToolStore()

    results = await store.search_tools(
        query=query,
        limit=limit,
        min_similarity=min_similarity
    )

    return {
        "query": query,
        "results": [
            {
                "name": r["tool"].get("name", ""),
                "description": r["tool"].get("description", ""),
                "similarity": r["similarity"],
                "backend": r.get("backend"),
                "access_count": r.get("access_count", 0)
            }
            for r in results
        ],
        "count": len(results)
    }


@router.get("/list-all-tools")
async def list_all_tools(
    offset: int = 0,
    limit: int = 20,
    category: Optional[str] = None,
    backend: Optional[str] = None
):
    """
    List all tools with pagination support.

    Enables browsing the full catalog without context overflow.
    """
    from ..registry import get_registry

    registry = get_registry()
    all_tools = registry.list_all()

    # Filter by category if provided
    if category:
        all_tools = [t for t in all_tools if t.category == category]

    # Filter by backend if provided
    if backend:
        all_tools = [t for t in all_tools if t.service_url and backend in t.service_url]

    total = len(all_tools)

    # Apply pagination
    page_tools = all_tools[offset:offset + limit]

    return {
        "tools": [
            {
                "name": t.name,
                "description": t.description,
                "category": t.category,
                "tags": t.tags,
                "backend": t.service_url,
                "inputs": [
                    {
                        "name": inp.name,
                        "type": inp.type,
                        "description": inp.description,
                        "required": inp.required
                    }
                    for inp in t.inputs
                ]
            }
            for t in page_tools
        ],
        "pagination": {
            "offset": offset,
            "limit": limit,
            "total": total,
            "has_next": offset + limit < total,
            "next_offset": offset + limit if offset + limit < total else None
        }
    }


@router.get("/list-tool-names")
async def list_tool_names(
    category: Optional[str] = None
):
    """
    Get a quick list of all tool names (lightweight).

    Useful for autocomplete or quick reference.
    """
    from ..registry import get_registry

    registry = get_registry()
    all_tools = registry.list_all()

    if category:
        all_tools = [t for t in all_tools if t.category == category]

    return {
        "tool_names": [t.name for t in all_tools],
        "count": len(all_tools),
        "by_category": {
            cat: [t.name for t in all_tools if t.category == cat]
            for cat in set(t.category for t in all_tools if t.category)
        }
    }


@router.get("/tool-categories")
async def get_tool_categories():
    """
    Get all categories with tool counts.

    Enables category-based browsing.
    """
    from ..registry import get_registry

    registry = get_registry()
    all_tools = registry.list_all()

    categories = {}
    for tool in all_tools:
        cat = tool.category or "uncategorized"
        if cat not in categories:
            categories[cat] = {
                "count": 0,
                "tools": [],
                "backends": set()
            }
        categories[cat]["count"] += 1
        categories[cat]["tools"].append(tool.name)
        if tool.service_url:
            categories[cat]["backends"].add(tool.service_url)

    # Convert sets to lists for JSON serialization
    for cat in categories:
        categories[cat]["backends"] = list(categories[cat]["backends"])

    return {
        "categories": categories,
        "total_categories": len(categories),
        "total_tools": len(all_tools)
    }
