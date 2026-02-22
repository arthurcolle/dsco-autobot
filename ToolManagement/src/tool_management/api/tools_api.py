"""
Clean Tools API - Robust tool execution with persistent storage.

This replaces the brittle batch endpoint with a properly designed API.
"""

import os
import asyncio
import hashlib
import json
import logging
import time
from typing import Dict, Any, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import httpx

from ..persistence import get_tool_store, RegisteredTool

# Output guard for enforcing size limits and URL capture
try:
    from ..guards.output_guard import apply_output_guard, extract_urls, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/tools", tags=["tools"])

# Shared HTTP client
_client: Optional[httpx.AsyncClient] = None
_announce_last_seen: Dict[str, float] = {}
_WRITE_TOOL_NAMES = {
    name.strip()
    for name in os.getenv(
        "IDEMPOTENT_WRITE_TOOLS",
        "store_belief,publish_event,substack_create_draft",
    ).split(",")
    if name.strip()
}


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(None, connect=15.0),  # No read/write timeout
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )
    return _client


# ==================== Request/Response Models ====================

class ToolCall(BaseModel):
    """A single tool call request."""
    tool: str = Field(..., description="Tool name or 'backend:tool' format")
    arguments: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=60.0, description="Timeout in seconds")
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Idempotency key for write tools (prevents duplicate side effects)",
    )
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None


class BatchRequest(BaseModel):
    """Batch tool execution request."""
    calls: List[ToolCall]
    execution: str = Field(
        default="parallel",
        description="Batch mode: parallel or sequential",
    )
    parallel: Optional[bool] = Field(
        default=None,
        description="Deprecated compatibility flag. If set, overrides execution.",
    )


class ToolResult(BaseModel):
    """Result from a tool call."""
    tool: str
    backend: Optional[str] = None
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    latency_ms: float = 0
    idempotent_replay: bool = False


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


class AnnounceRequest(BaseModel):
    """Push-announce request: backend announces itself + its tools on startup."""
    backend_id: str
    name: str
    url: str
    health_endpoint: str = "/health"
    mcp_endpoint: str = "/mcp"
    capabilities: List[str] = Field(default_factory=list)
    tools: List[Dict[str, Any]] = Field(default_factory=list)


# ==================== Tool Execution ====================

async def execute_tool(call: ToolCall) -> ToolResult:
    """Execute a single tool call."""
    store = get_tool_store()
    start = time.time()

    try:
        # Resolve the tool to backend
        backend_id, backend_url, tool_name = store.resolve_tool(call.tool)
        tool_id = f"{backend_id}:{tool_name}"
        request_hash = None
        use_idempotency = bool(call.idempotency_key) and _is_write_tool(call.tool, tool_name)

        if use_idempotency:
            request_hash = _hash_arguments(call.arguments)
            try:
                existing = store.get_idempotent_response(
                    idempotency_key=call.idempotency_key,
                    tool_id=tool_id,
                    request_hash=request_hash,
                )
            except ValueError as exc:
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=str(exc),
                    latency_ms=(time.time() - start) * 1000,
                )
            if existing is not None:
                payload = existing.get("response") or {}
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=bool(existing.get("success", True)),
                    result=payload.get("result"),
                    error=payload.get("error"),
                    latency_ms=(time.time() - start) * 1000,
                    idempotent_replay=True,
                )

        # Execute via MCP protocol
        outbound_arguments = dict(call.arguments)
        if call.trace_id:
            outbound_arguments.setdefault("_trace_id", call.trace_id)
        if call.session_id:
            outbound_arguments.setdefault("_session_id", call.session_id)
        if call.parent_execution_id:
            outbound_arguments.setdefault("_parent_execution_id", call.parent_execution_id)

        client = get_client()
        response = await client.post(
            f"{backend_url}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": str(uuid4()),
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": outbound_arguments
                }
            },
            timeout=call.timeout,
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

            result_data = data.get("result")

            # Apply output guard: enforce size limits, capture URLs
            if OUTPUT_GUARD_AVAILABLE and result_data is not None:
                result_data = apply_output_guard(
                    result_data,
                    tool_name=f"{backend_id}:{tool_name}",
                    source_context={"backend": backend_id, "via": "tools_api"},
                )

            if use_idempotency and call.idempotency_key and request_hash:
                store.save_idempotent_response(
                    idempotency_key=call.idempotency_key,
                    tool_id=tool_id,
                    request_hash=request_hash,
                    response={"result": result_data, "error": None},
                    success=True,
                )

            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=True,
                result=result_data,
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
    execution_mode = request.execution.lower() if request.execution else "parallel"
    if request.parallel is not None:
        execution_mode = "parallel" if request.parallel else "sequential"
    if execution_mode not in {"parallel", "sequential"}:
        raise HTTPException(status_code=400, detail="execution must be 'parallel' or 'sequential'")

    if execution_mode == "parallel":
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


@router.post("/announce")
async def announce_backend(request: AnnounceRequest, background_tasks: BackgroundTasks):
    """
    Push-announce: a backend registers itself and its tools in one call.

    1. Registers the backend in DuckDB (synchronous)
    2. Registers each tool in DuckDB (synchronous)
    3. Queues background tasks for embedding generation + Neo4j sync
    """
    min_interval = int(os.getenv("ANNOUNCE_MIN_INTERVAL_SECONDS", "300"))
    now = time.time()
    last_seen = _announce_last_seen.get(request.backend_id, 0.0)

    # Drop high-frequency heartbeat announces to avoid blocking the event loop on DuckDB writes.
    if min_interval > 0 and (now - last_seen) < min_interval:
        return {
            "backend_id": request.backend_id,
            "registered": 0,
            "embedding_sync": "skipped_recent_announce",
            "min_interval_seconds": min_interval,
        }

    _announce_last_seen[request.backend_id] = now

    # Run blocking DuckDB writes in a worker thread so auth/MCP routes stay responsive.
    def _persist_announce() -> int:
        store = get_tool_store()
        store.register_backend(
            backend_id=request.backend_id,
            name=request.name,
            url=request.url,
            health_endpoint=request.health_endpoint,
            mcp_endpoint=request.mcp_endpoint,
            capabilities=request.capabilities,
        )

        registered_count = 0
        backend_category = request.backend_id.replace("distributed-", "").replace("-mcp", "").strip() or "general"
        for tool_data in request.tools:
            tags = tool_data.get("tags", [])
            if not tags:
                schema = tool_data.get("inputSchema", {})
                props = schema.get("properties", {})
                tags = list(props.keys())[:5]
            tool = RegisteredTool(
                tool_id=f"{request.backend_id}:{tool_data['name']}",
                name=tool_data["name"],
                backend_id=request.backend_id,
                backend_url=request.url,
                description=tool_data.get("description", ""),
                input_schema=tool_data.get("inputSchema", {}),
                tags=tags,
                category=tool_data.get("category", backend_category),
            )
            store.register_tool(tool)
            registered_count += 1

        store.update_backend_health(request.backend_id, True)
        return registered_count

    registered = await asyncio.to_thread(_persist_announce)

    # 3. Optional background: generate embeddings + sync to Neo4j
    embed_sync_enabled = os.getenv("ENABLE_ANNOUNCE_EMBED_SYNC", "1") == "1"
    if embed_sync_enabled:
        background_tasks.add_task(_embed_and_graph_sync, request.backend_id, request.tools, request.url)
        embed_sync_status = "queued"
    else:
        embed_sync_status = "disabled"

    return {"backend_id": request.backend_id, "registered": registered, "embedding_sync": embed_sync_status}


async def _embed_and_graph_sync(backend_id: str, tools: List[Dict], backend_url: str):
    """Background task: generate embeddings and sync to Neo4j."""
    # Embeddings
    try:
        from ..embeddings.advanced_tool_index import get_advanced_index
        index = get_advanced_index()
        await index.index_tools_batch(tools, backend_id)
        logger.info(f"Embedded {len(tools)} tools for {backend_id}")
    except Exception as e:
        logger.warning(f"Embedding failed for {backend_id}: {e}")

    # Neo4j
    try:
        import os
        neo4j_url = os.getenv("NEO4J_URL")
        if neo4j_url:
            from ..services.graph_client import GraphManagementClient
            graph = GraphManagementClient(base_url=neo4j_url)
            stmts = []
            for t in tools:
                stmts.append({
                    "query": "MERGE (tool:Tool {tool_id: $tid}) "
                             "SET tool.name=$name, tool.description=$desc, "
                             "tool.backend_id=$bid, tool.backend_url=$burl, "
                             "tool.updated_at=datetime()",
                    "parameters": {
                        "tid": f"{backend_id}:{t['name']}", "name": t["name"],
                        "desc": t.get("description", ""), "bid": backend_id, "burl": backend_url
                    }
                })
            await graph.execute_transaction(stmts)
            logger.info(f"Neo4j synced {len(tools)} tools for {backend_id}")
    except Exception as e:
        logger.warning(f"Neo4j sync failed for {backend_id}: {e}")


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
async def list_tools(backend_id: str = None, category: str = None, limit: int = 100, offset: int = 0):
    """List registered tools with pagination."""
    store = get_tool_store()
    tools = store.list_tools(backend_id=backend_id, category=category, limit=limit, offset=offset)
    total = store.count_tools(backend_id=backend_id, category=category)
    return {
        "tools": [{"tool_id": t.tool_id, "name": t.name, "backend": t.backend_id, "description": t.description} for t in tools],
        "count": len(tools),
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(tools) < total,
    }


@router.get("/stats/overview")
async def get_stats():
    """Get tool store statistics."""
    store = get_tool_store()
    return store.get_stats()


@router.get("/stats/categories")
async def get_categories():
    """Get distinct tool categories with counts."""
    store = get_tool_store()
    return {"categories": store.get_distinct_categories()}


@router.get("/stats/popular")
async def get_popular(limit: int = 10):
    """Get most-used tools by call count."""
    store = get_tool_store()
    return {"tools": store.get_popular_tools(limit=limit)}


@router.post("/query")
async def query_tools(
    search: Optional[str] = None,
    backend_id: Optional[str] = None,
    category: Optional[str] = None,
    tags: Optional[str] = None,
    sort_by: str = "name",
    sort_order: str = "asc",
    limit: int = 50,
    offset: int = 0,
    compact: bool = False,
):
    """
    Advanced tool query with text search, sorting, tag filtering, and compact mode.

    - search: Text search across name and description
    - tags: Comma-separated tag list (e.g., "web,api,search")
    - sort_by: name, call_count, updated_at, avg_latency_ms, error_count
    - compact: If true, omit full schemas — return only name, description, backend, signature
    """
    store = get_tool_store()
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    columns = [
        "tool_id",
        "name",
        "backend_id",
        "description",
        "call_count",
        "avg_latency_ms",
        "error_count",
        "category",
    ] if compact else None

    tools, total = store.query_tools(
        search=search,
        backend_id=backend_id,
        category=category,
        tags=tag_list,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        offset=offset,
        columns=columns,
    )

    return {
        "tools": tools,
        "count": len(tools),
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(tools) < total,
        "compact": compact,
    }


class BatchGetRequest(BaseModel):
    """Request to fetch multiple tools by name."""
    names: List[str] = Field(..., description="List of tool names to fetch")
    compact: bool = Field(default=False, description="Return compact summaries instead of full schemas")


@router.post("/batch-get")
async def batch_get_tools(request: BatchGetRequest):
    """
    Fetch multiple tools by name in one call.
    With compact=true, returns signature summaries instead of full JSON schemas.
    """
    store = get_tool_store()

    not_found = []
    if request.compact:
        results = []
        for name in request.names:
            tool = store.get_tool_by_name(name)
            if tool:
                summary = store.summarize_tool_schema(tool.tool_id)
                if summary:
                    results.append(summary)
                    continue
            not_found.append(name)
        return {"tools": results, "count": len(results), "not_found": not_found, "format": "compact"}
    else:
        tools = store.get_tools_batch(request.names)
        found_names = {t.get("name") or t.get("tool_id", "").split(":", 1)[-1] for t in tools} if isinstance(tools, list) else set()
        not_found = [n for n in request.names if n not in found_names]
        return {"tools": tools, "count": len(tools), "not_found": not_found, "format": "full"}


@router.get("/schema/{tool_name}")
async def get_tool_schema_summary(tool_name: str):
    """
    Get a compact schema summary for a tool: parameter names, types, required flags.
    Much smaller than full JSON schema — ideal for context-limited AI agents.
    """
    store = get_tool_store()
    tool = store.get_tool_by_name(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    summary = store.summarize_tool_schema(tool.tool_id)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Could not summarize: {tool_name}")
    return summary


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
        "avg_latency_ms": tool.avg_latency_ms,
        "error_count": tool.error_count,
        "error_rate": tool.error_count / max(1, tool.call_count),
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
        backend_category = backend_id.replace("distributed-", "").replace("-mcp", "").strip() or "general"
        for tool_data in tools:
            tags = tool_data.get("tags", [])
            if not tags:
                schema = tool_data.get("inputSchema", {})
                props = schema.get("properties", {})
                tags = list(props.keys())[:5]
            tool = RegisteredTool(
                tool_id=f"{backend_id}:{tool_data['name']}",
                name=tool_data["name"],
                backend_id=backend_id,
                backend_url=backend["url"],
                description=tool_data.get("description", ""),
                input_schema=tool_data.get("inputSchema", {}),
                tags=tags,
                category=tool_data.get("category", backend_category),
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


def _is_write_tool(tool_ref: str, resolved_name: str) -> bool:
    if resolved_name in _WRITE_TOOL_NAMES:
        return True
    if tool_ref in _WRITE_TOOL_NAMES:
        return True
    if ":" in tool_ref and tool_ref.split(":", 1)[-1] in _WRITE_TOOL_NAMES:
        return True
    return False


def _hash_arguments(arguments: Dict[str, Any]) -> str:
    stable_arguments = {
        k: v
        for k, v in arguments.items()
        if k not in {"_trace_id", "_session_id", "_parent_execution_id"}
    }
    stable_payload = json.dumps(stable_arguments, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()
