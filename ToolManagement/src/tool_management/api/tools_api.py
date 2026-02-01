"""
Clean Tools API - Robust tool execution with persistent storage.

This replaces the brittle batch endpoint with a properly designed API.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import httpx

from ..persistence import get_tool_store, RegisteredTool

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/tools", tags=["tools"])

# Shared HTTP client
_client: Optional[httpx.AsyncClient] = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )
    return _client


# ==================== Request/Response Models ====================

class ToolCall(BaseModel):
    """A single tool call request."""
    tool: str = Field(..., description="Tool name or 'backend:tool' format")
    arguments: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=30.0, description="Timeout in seconds")


class BatchRequest(BaseModel):
    """Batch tool execution request."""
    calls: List[ToolCall]
    parallel: bool = Field(default=True, description="Execute in parallel")


class ToolResult(BaseModel):
    """Result from a tool call."""
    tool: str
    backend: Optional[str] = None
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    latency_ms: float = 0


class BatchResponse(BaseModel):
    """Batch execution response."""
    results: List[ToolResult]
    total: int
    successful: int
    failed: int


class RegisterToolRequest(BaseModel):
    """Request to register a tool."""
    name: str
    backend_id: str
    backend_url: str
    description: str = ""
    input_schema: Dict = Field(default_factory=dict)
    output_schema: Dict = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    category: str = "general"


class RegisterBackendRequest(BaseModel):
    """Request to register a backend."""
    backend_id: str
    name: str
    url: str
    health_endpoint: str = "/health"
    mcp_endpoint: str = "/mcp"
    capabilities: List[str] = Field(default_factory=list)


# ==================== Tool Execution ====================

async def execute_tool(call: ToolCall) -> ToolResult:
    """Execute a single tool call."""
    store = get_tool_store()
    start = time.time()

    try:
        # Resolve the tool to backend
        backend_id, backend_url, tool_name = store.resolve_tool(call.tool)

        # Execute via MCP protocol
        client = get_client()
        response = await client.post(
            f"{backend_url}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": str(uuid4()),
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": call.arguments
                }
            },
            timeout=call.timeout
        )

        latency_ms = (time.time() - start) * 1000

        if response.status_code == 200:
            data = response.json()

            if "error" in data:
                store.record_call(f"{backend_id}:{tool_name}", latency_ms, False)
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=str(data["error"]),
                    latency_ms=latency_ms
                )

            store.record_call(f"{backend_id}:{tool_name}", latency_ms, True)
            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=True,
                result=data.get("result"),
                latency_ms=latency_ms
            )
        else:
            store.record_call(f"{backend_id}:{tool_name}", latency_ms, False)
            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=False,
                error=f"HTTP {response.status_code}: {response.text[:200]}",
                latency_ms=latency_ms
            )

    except ValueError as e:
        # Tool/backend not found
        return ToolResult(
            tool=call.tool,
            success=False,
            error=str(e),
            latency_ms=(time.time() - start) * 1000
        )
    except httpx.TimeoutException:
        return ToolResult(
            tool=call.tool,
            success=False,
            error=f"Timeout after {call.timeout}s",
            latency_ms=(time.time() - start) * 1000
        )
    except Exception as e:
        logger.exception(f"Tool execution error: {call.tool}")
        return ToolResult(
            tool=call.tool,
            success=False,
            error=str(e),
            latency_ms=(time.time() - start) * 1000
        )


# ==================== API Endpoints ====================

@router.post("/execute", response_model=ToolResult)
async def execute_single_tool(call: ToolCall):
    """
    Execute a single tool.

    Tool can be specified as:
    - "tool_name" - will look up backend automatically
    - "backend_id:tool_name" - explicit backend
    """
    return await execute_tool(call)


@router.post("/batch", response_model=BatchResponse)
async def execute_batch(request: BatchRequest):
    """
    Execute multiple tools in batch.

    Tools can be specified as:
    - "tool_name" - will look up backend automatically
    - "backend_id:tool_name" - explicit backend

    Set parallel=false for sequential execution.
    """
    if request.parallel:
        results = await asyncio.gather(*[execute_tool(call) for call in request.calls])
    else:
        results = []
        for call in request.calls:
            result = await execute_tool(call)
            results.append(result)

    successful = sum(1 for r in results if r.success)
    return BatchResponse(
        results=results,
        total=len(results),
        successful=successful,
        failed=len(results) - successful
    )


@router.post("/register")
async def register_tool(request: RegisterToolRequest):
    """Register a new tool."""
    store = get_tool_store()

    tool = RegisteredTool(
        tool_id=f"{request.backend_id}:{request.name}",
        name=request.name,
        backend_id=request.backend_id,
        backend_url=request.backend_url,
        description=request.description,
        input_schema=request.input_schema,
        output_schema=request.output_schema,
        tags=request.tags,
        category=request.category
    )

    return store.register_tool(tool)


@router.get("/debug/test")
async def debug_test():
    """Debug endpoint to test persistence."""
    import os
    data_dir = os.getenv("DATA_DIR", "data")
    try:
        store = get_tool_store()
        return {
            "data_dir": data_dir,
            "db_path": str(store.db_path),
            "store_initialized": store._initialized,
        }
    except Exception as e:
        return {"error": str(e), "data_dir": data_dir}


@router.post("/backends/register")
async def register_backend(request: RegisterBackendRequest):
    """Register a new backend."""
    try:
        store = get_tool_store()
        return store.register_backend(
            backend_id=request.backend_id,
            name=request.name,
            url=request.url,
            health_endpoint=request.health_endpoint,
            mcp_endpoint=request.mcp_endpoint,
            capabilities=request.capabilities
        )
    except Exception as e:
        logger.exception(f"Failed to register backend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backends")
async def list_backends():
    """List all registered backends."""
    store = get_tool_store()
    return store.list_backends()


@router.get("/backends/{backend_id}")
async def get_backend(backend_id: str):
    """Get a specific backend."""
    store = get_tool_store()
    backend = store.get_backend(backend_id)
    if not backend:
        raise HTTPException(status_code=404, detail=f"Backend not found: {backend_id}")
    return backend


@router.get("/backends/{backend_id}/tools")
async def list_backend_tools(backend_id: str):
    """List all tools for a backend."""
    store = get_tool_store()
    tools = store.list_tools_by_backend(backend_id)
    return {"backend_id": backend_id, "tools": [t.name for t in tools], "count": len(tools)}


@router.get("/")
async def list_tools(backend_id: str = None, category: str = None, limit: int = 100):
    """List registered tools."""
    store = get_tool_store()
    tools = store.list_tools(backend_id=backend_id, category=category, limit=limit)
    return {
        "tools": [{"tool_id": t.tool_id, "name": t.name, "backend": t.backend_id, "description": t.description} for t in tools],
        "count": len(tools)
    }


@router.get("/stats/overview")
async def get_stats():
    """Get tool store statistics."""
    store = get_tool_store()
    return store.get_stats()


@router.get("/{tool_name}")
async def get_tool(tool_name: str):
    """Get tool details by name."""
    store = get_tool_store()
    tool = store.get_tool_by_name(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    return {
        "tool_id": tool.tool_id,
        "name": tool.name,
        "backend": tool.backend_id,
        "backend_url": tool.backend_url,
        "description": tool.description,
        "input_schema": tool.input_schema,
        "tags": tool.tags,
        "category": tool.category,
        "call_count": tool.call_count,
        "avg_latency_ms": tool.avg_latency_ms
    }


@router.post("/sync/{backend_id}")
async def sync_backend_tools(backend_id: str):
    """
    Sync tools from a backend via MCP protocol.

    Fetches tools/list from the backend and registers them.
    """
    store = get_tool_store()
    backend = store.get_backend(backend_id)

    if not backend:
        raise HTTPException(status_code=404, detail=f"Backend not found: {backend_id}")

    try:
        client = get_client()
        response = await client.post(
            f"{backend['url']}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": "sync",
                "method": "tools/list",
                "params": {}
            },
            timeout=10.0
        )

        if response.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Backend returned {response.status_code}")

        data = response.json()
        tools = data.get("result", {}).get("tools", [])

        registered = 0
        for tool_data in tools:
            tool = RegisteredTool(
                tool_id=f"{backend_id}:{tool_data['name']}",
                name=tool_data["name"],
                backend_id=backend_id,
                backend_url=backend["url"],
                description=tool_data.get("description", ""),
                input_schema=tool_data.get("inputSchema", {}),
            )
            store.register_tool(tool)
            registered += 1

        store.update_backend_health(backend_id, True)
        return {"backend_id": backend_id, "synced": registered, "tools": [t["name"] for t in tools]}

    except httpx.TimeoutException:
        store.update_backend_health(backend_id, False)
        raise HTTPException(status_code=504, detail="Backend timeout")
    except Exception as e:
        store.update_backend_health(backend_id, False)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync-all")
async def sync_all_backends():
    """Sync tools from all registered backends."""
    store = get_tool_store()
    backends = store.list_backends()

    results = []
    for backend in backends:
        try:
            result = await sync_backend_tools(backend["backend_id"])
            results.append({"backend": backend["backend_id"], "status": "success", "synced": result["synced"]})
        except Exception as e:
            results.append({"backend": backend["backend_id"], "status": "error", "error": str(e)})

    total_synced = sum(r.get("synced", 0) for r in results if r["status"] == "success")
    return {"backends": results, "total_synced": total_synced}
