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
from datetime import datetime
from typing import Dict, Any, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel, Field
import httpx

from ..persistence import get_tool_store, RegisteredTool, get_execution_store
from ..core.circuit_breaker import get_backend_circuit_breaker

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


class DependencyUpsertRequest(BaseModel):
    source_tool: str = Field(..., description="Source tool name or backend:tool")
    target_tool: str = Field(..., description="Dependent tool name or backend:tool")
    dependency_type: str = Field(default="explicit", description="explicit|inferred|runtime")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    call_count: int = Field(default=1, ge=1, le=1_000_000)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InferDependenciesRequest(BaseModel):
    hours: int = Field(default=24, ge=1, le=720)
    limit: int = Field(default=100, ge=1, le=500)
    min_support: int = Field(default=2, ge=1, le=1000)
    persist: bool = True


class ToolRecommendationRequest(BaseModel):
    query: Optional[str] = Field(default=None, description="Natural-language objective")
    intent: Optional[str] = Field(default=None, description="query|store|publish|analyze|manage")
    backend_id: Optional[str] = None
    category: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    max_latency_ms: Optional[float] = Field(default=None, ge=0.0)
    require_streaming: bool = False
    include_dependencies: bool = True
    limit: int = Field(default=8, ge=1, le=50)
    trace_id: Optional[str] = None
    session_id: Optional[str] = None


class RecommendationFeedbackRequest(BaseModel):
    recommendation_id: str = Field(..., description="Recommendation request identifier")
    context: Dict[str, Any] = Field(default_factory=dict)
    recommendation: Dict[str, Any] = Field(default_factory=dict)
    chosen_tool: Optional[str] = None
    accepted: bool = False
    outcome_status: Optional[str] = Field(default=None, description="completed|failed|cancelled|timeout")
    latency_ms: Optional[float] = Field(default=None, ge=0.0)
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    trace_id: Optional[str] = None
    session_id: Optional[str] = None


# ==================== Tool Execution ====================

async def execute_tool(call: ToolCall) -> ToolResult:
    """Execute a single tool call."""
    store = get_tool_store()
    breaker = get_backend_circuit_breaker()
    start = time.time()
    backend_id: Optional[str] = None
    resolved_tool_id: Optional[str] = None

    try:
        # Resolve the tool to backend
        backend_id, backend_url, tool_name = store.resolve_tool(call.tool)
        resolved_tool_id = f"{backend_id}:{tool_name}"
        allowed, circuit_state = breaker.allow_request(backend_id)
        if not allowed:
            retry_after = circuit_state.get("retry_after_seconds")
            retry_suffix = f" retry_after={retry_after}s" if retry_after is not None else ""
            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=False,
                error=(
                    f"CIRCUIT_OPEN: backend={backend_id} is temporarily unavailable "
                    f"(status={circuit_state.get('status')}).{retry_suffix}"
                ),
                latency_ms=(time.time() - start) * 1000,
            )
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
                err_obj = data.get("error")
                err_code = err_obj.get("code") if isinstance(err_obj, dict) else None
                err_message = err_obj.get("message") if isinstance(err_obj, dict) else str(err_obj)
                store.record_call(
                    f"{backend_id}:{tool_name}",
                    latency_ms,
                    False,
                    execution_mode="sync",
                    trace_id=call.trace_id,
                    session_id=call.session_id,
                    error_code=err_code,
                    error_message=err_message,
                )
                breaker.record_failure(backend_id, error=str(data.get("error")))
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=str(data["error"]),
                    latency_ms=latency_ms
                )

            store.record_call(
                f"{backend_id}:{tool_name}",
                latency_ms,
                True,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
            )
            breaker.record_success(backend_id)

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
            store.record_call(
                f"{backend_id}:{tool_name}",
                latency_ms,
                False,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
                error_code=f"HTTP_{response.status_code}",
                error_message=response.text[:500],
            )
            breaker.record_failure(backend_id, error=f"http {response.status_code}")
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
        if backend_id:
            breaker.record_failure(backend_id, error=f"timeout after {call.timeout}s")
        if resolved_tool_id:
            store.record_call(
                resolved_tool_id,
                (time.time() - start) * 1000,
                False,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
                error_code="TIMEOUT",
                error_message=f"Timeout after {call.timeout}s",
            )
        return ToolResult(
            tool=call.tool,
            success=False,
            error=f"Timeout after {call.timeout}s",
            latency_ms=(time.time() - start) * 1000
        )
    except Exception as e:
        if backend_id:
            breaker.record_failure(backend_id, error=str(e))
        if resolved_tool_id:
            store.record_call(
                resolved_tool_id,
                (time.time() - start) * 1000,
                False,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
                error_code="EXCEPTION",
                error_message=str(e),
            )
        logger.exception(f"Tool execution error: {call.tool}")
        return ToolResult(
            tool=call.tool,
            success=False,
            error=str(e),
            latency_ms=(time.time() - start) * 1000
        )


_INTENT_HINTS: Dict[str, List[str]] = {
    "query": ["search", "read", "list", "query", "find", "lookup", "retrieve"],
    "store": ["store", "save", "record", "memory", "belief", "insert", "upsert"],
    "publish": ["publish", "event", "draft", "substack", "notify", "webhook"],
    "analyze": ["analyze", "synthesize", "deep", "summary", "compare", "embed"],
    "manage": ["register", "sync", "list", "get", "update", "admin", "control"],
}


def _tokenize(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return [token.strip().lower() for token in text.replace("/", " ").replace("_", " ").split() if token.strip()]


def _text_relevance_score(query: Optional[str], candidate_text: str) -> float:
    query_tokens = _tokenize(query)
    if not query_tokens:
        return 0.0
    lowered = candidate_text.lower()
    matches = sum(1 for token in query_tokens if token in lowered)
    return float(matches) / max(1, len(query_tokens))


def _intent_score(intent: Optional[str], candidate_text: str) -> float:
    if not intent:
        return 0.0
    normalized = intent.strip().lower()
    keywords = _INTENT_HINTS.get(normalized, [normalized])
    lowered = candidate_text.lower()
    hits = sum(1 for keyword in keywords if keyword in lowered)
    if not keywords:
        return 0.0
    return float(hits) / float(len(keywords))


def _execution_mode_hint(avg_latency_ms: float, require_streaming: bool) -> str:
    if require_streaming:
        return "stream"
    if avg_latency_ms <= 0:
        return "auto"
    if avg_latency_ms < 2000:
        return "sync"
    if avg_latency_ms <= 30000:
        return "stream"
    return "job"


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
async def list_backends(
    hours: int = Query(default=24, ge=1, le=720),
):
    """List all registered backends with rich latency/error and circuit-breaker metadata."""
    store = get_tool_store()
    breaker = get_backend_circuit_breaker()
    metrics_by_backend = store.get_backend_latency_metrics(hours=hours)
    circuits = breaker.get_all_states()

    enriched: List[Dict[str, Any]] = []
    for backend in store.list_backends():
        backend_id = backend.get("backend_id")
        enriched.append(
            {
                **backend,
                "metrics": metrics_by_backend.get(
                    backend_id,
                    {
                        "window_hours": hours,
                        "calls": 0,
                        "successes": 0,
                        "failures": 0,
                        "error_rate": 0.0,
                        "avg_latency_ms": 0.0,
                        "p50_latency_ms": 0.0,
                        "p95_latency_ms": 0.0,
                        "p99_latency_ms": 0.0,
                        "last_call_at": None,
                    },
                ),
                "circuit_breaker": circuits.get(backend_id, {"status": "closed"}),
            }
        )
    return enriched


@router.get("/backends/{backend_id}")
async def get_backend(
    backend_id: str,
    hours: int = Query(default=24, ge=1, le=720),
):
    """Get one backend enriched with latency/error metrics and circuit-breaker state."""
    store = get_tool_store()
    backend = store.get_backend(backend_id)
    if not backend:
        raise HTTPException(status_code=404, detail=f"Backend not found: {backend_id}")
    breaker = get_backend_circuit_breaker()
    return {
        **backend,
        "metrics": store.get_backend_latency_metrics(hours=hours).get(
            backend_id,
            {
                "window_hours": hours,
                "calls": 0,
                "successes": 0,
                "failures": 0,
                "error_rate": 0.0,
                "avg_latency_ms": 0.0,
                "p50_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "p99_latency_ms": 0.0,
                "last_call_at": None,
            },
        ),
        "circuit_breaker": breaker.get_state(backend_id),
    }


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


@router.get("/stats/backends")
async def get_backend_metrics(
    hours: int = Query(default=24, ge=1, le=720),
):
    """Get backend-level latency/error metrics and circuit-breaker state."""
    store = get_tool_store()
    breaker = get_backend_circuit_breaker()
    metrics = store.get_backend_latency_metrics(hours=hours)
    states = breaker.get_all_states()
    enriched = {
        backend_id: {
            **payload,
            "circuit_breaker": states.get(backend_id, {"status": "closed"}),
        }
        for backend_id, payload in metrics.items()
    }
    return {
        "window_hours": hours,
        "count": len(enriched),
        "backends": enriched,
    }


@router.get("/analytics/overview")
async def get_analytics_overview(
    hours: int = Query(default=24, ge=1, le=720),
    bucket_minutes: int = Query(default=60, ge=1, le=1440),
    chain_limit: int = Query(default=20, ge=1, le=200),
):
    """Get a rich cross-surface analytics snapshot for MCP and general clients."""
    store = get_tool_store()
    execution_store = get_execution_store()

    response: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat(),
        "window_hours": hours,
        "bucket_minutes": bucket_minutes,
        "registry": store.get_stats(),
        "backend_metrics": store.get_backend_latency_metrics(hours=hours),
        "usage_timeseries": store.get_tool_usage_timeseries(
            hours=hours,
            bucket_minutes=bucket_minutes,
            limit=5000,
        ),
        "failure_modes": store.get_failure_modes(hours=hours, limit=50),
        "dependencies": store.list_tool_dependencies(limit=100),
    }

    try:
        response["execution_analytics"] = execution_store.get_usage_stats(hours=hours)
        response["workflow_analytics"] = execution_store.get_workflow_analytics(hours=hours)
        response["chain_analytics"] = execution_store.get_workflow_chain_analytics(
            hours=hours,
            limit=chain_limit,
            min_chain_length=2,
        )
        response["recommendation_feedback"] = execution_store.get_recommendation_feedback_analytics(hours=hours)
    except Exception as exc:
        response["execution_analytics_error"] = str(exc)

    return response


@router.post("/recommendations")
async def recommend_tools(request: ToolRecommendationRequest):
    """
    Recommend tools with transparent scoring for MCP and general clients.

    Ranking combines intent relevance, text relevance, reliability, usage, and latency.
    """
    store = get_tool_store()
    candidate_limit = max(50, min(500, request.limit * 20))
    query_text = (request.query or "").strip()
    intent = (request.intent or "").strip().lower() or None
    tags_filter = [tag for tag in request.tags if str(tag).strip()]

    candidates, _total = store.query_tools(
        search=query_text or None,
        backend_id=request.backend_id,
        category=request.category,
        tags=tags_filter or None,
        sort_by="call_count",
        sort_order="desc",
        limit=candidate_limit,
        offset=0,
    )

    scored_rows: List[Dict[str, Any]] = []
    for row in candidates:
        name = str(row.get("name") or "")
        description = str(row.get("description") or "")
        tool_id = str(row.get("tool_id") or "")
        call_count = int(row.get("call_count") or 0)
        error_count = int(row.get("error_count") or 0)
        avg_latency_ms = float(row.get("avg_latency_ms") or 0.0)
        error_rate = (float(error_count) / call_count) if call_count else 0.0
        reliability = 1.0 - error_rate

        searchable_text = f"{name} {description} {tool_id}"
        score_components = {
            "text_relevance": _text_relevance_score(query_text, searchable_text),
            "intent_relevance": _intent_score(intent, searchable_text),
            "reliability": max(0.0, min(1.0, reliability)),
            "usage_strength": min(1.0, call_count / 100.0),
            "latency_fit": 1.0 if avg_latency_ms <= 0 else max(0.0, min(1.0, 1.0 - (avg_latency_ms / 30000.0))),
        }

        if request.max_latency_ms is not None and avg_latency_ms > 0 and avg_latency_ms > request.max_latency_ms:
            score_components["latency_fit"] *= 0.25

        if request.require_streaming and avg_latency_ms > 0:
            if avg_latency_ms < 2000:
                score_components["latency_fit"] *= 0.8
            elif avg_latency_ms <= 30000:
                score_components["latency_fit"] = min(1.0, score_components["latency_fit"] + 0.2)

        weighted_score = (
            (0.30 * score_components["text_relevance"]) +
            (0.22 * score_components["intent_relevance"]) +
            (0.18 * score_components["reliability"]) +
            (0.15 * score_components["usage_strength"]) +
            (0.15 * score_components["latency_fit"])
        )
        weighted_score = max(0.0, min(1.0, weighted_score))

        reason_bits: List[str] = []
        if score_components["text_relevance"] >= 0.5:
            reason_bits.append("query_match")
        if score_components["intent_relevance"] >= 0.3:
            reason_bits.append("intent_match")
        if reliability >= 0.9 and call_count > 0:
            reason_bits.append("high_reliability")
        if avg_latency_ms > 0 and avg_latency_ms < 2000:
            reason_bits.append("low_latency")
        if call_count >= 10:
            reason_bits.append("proven_usage")
        if request.require_streaming:
            reason_bits.append("streaming_requested")

        scored_rows.append(
            {
                "tool_id": tool_id,
                "name": name,
                "backend_id": row.get("backend_id"),
                "category": row.get("category"),
                "description": description,
                "call_count": call_count,
                "error_count": error_count,
                "error_rate": error_rate,
                "avg_latency_ms": avg_latency_ms,
                "score": weighted_score,
                "confidence": min(1.0, 0.35 + (weighted_score * 0.6) + min(0.05, call_count / 1000.0)),
                "score_components": score_components,
                "recommended_execution_mode": _execution_mode_hint(avg_latency_ms, request.require_streaming),
                "reasons": reason_bits,
            }
        )

    scored_rows.sort(key=lambda item: (item["score"], item["call_count"]), reverse=True)
    selected = scored_rows[: request.limit]

    dependency_index: Dict[str, List[Dict[str, Any]]] = {}
    if request.include_dependencies and selected:
        dependencies = store.list_tool_dependencies(limit=2000)
        for edge in dependencies:
            source_id = edge.get("source_tool_id")
            if not source_id:
                continue
            dependency_index.setdefault(source_id, []).append(edge)

    recommendation_id = f"rec_{uuid4().hex}"
    recommendations: List[Dict[str, Any]] = []
    for idx, row in enumerate(selected, start=1):
        tool_dependencies = dependency_index.get(row["tool_id"], [])[:10]
        recommendations.append(
            {
                "rank": idx,
                **row,
                "dependencies": tool_dependencies,
            }
        )

    return {
        "recommendation_id": recommendation_id,
        "generated_at": datetime.utcnow().isoformat(),
        "query": request.query,
        "intent": intent,
        "filters": {
            "backend_id": request.backend_id,
            "category": request.category,
            "tags": tags_filter,
            "max_latency_ms": request.max_latency_ms,
            "require_streaming": request.require_streaming,
        },
        "count": len(recommendations),
        "recommendations": recommendations,
    }


@router.post("/recommendations/feedback")
async def record_recommendation_feedback(request: RecommendationFeedbackRequest):
    """Persist recommendation outcome feedback for improving future routing."""
    execution_store = get_execution_store()
    record = execution_store.append_recommendation_feedback(
        recommendation_id=request.recommendation_id,
        context=request.context,
        recommendation=request.recommendation,
        chosen_tool=request.chosen_tool,
        accepted=request.accepted,
        outcome_status=request.outcome_status,
        latency_ms=request.latency_ms,
        error=request.error,
        metadata=request.metadata,
        trace_id=request.trace_id,
        session_id=request.session_id,
    )
    return {"status": "recorded", "feedback": record}


@router.get("/recommendations/analytics")
async def get_recommendation_feedback_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=25, ge=1, le=200),
):
    """Get recommendation acceptance/rejection analytics from durable feedback data."""
    execution_store = get_execution_store()
    return execution_store.get_recommendation_feedback_analytics(hours=hours, limit=limit)


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


@router.get("/metrics/{tool_name}")
async def get_tool_metrics(
    tool_name: str,
    hours: int = Query(default=24, ge=1, le=720),
):
    """Get durable latency/error metrics for one tool."""
    store = get_tool_store()
    metrics = store.get_tool_latency_metrics(tool_name, hours=hours)
    if not metrics:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    return metrics


@router.get("/analytics/usage")
async def get_tool_usage_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    bucket_minutes: int = Query(default=60, ge=1, le=1440),
    tool_name: Optional[str] = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=10000),
):
    """Get bucketed usage and latency trends from durable call history."""
    store = get_tool_store()
    return store.get_tool_usage_timeseries(
        hours=hours,
        bucket_minutes=bucket_minutes,
        tool_ref=tool_name,
        limit=limit,
    )


@router.get("/analytics/failures")
async def get_tool_failure_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    tool_name: Optional[str] = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
):
    """Get top failure modes from durable call history."""
    store = get_tool_store()
    return store.get_failure_modes(hours=hours, tool_ref=tool_name, limit=limit)


@router.post("/dependencies")
async def upsert_tool_dependency(request: DependencyUpsertRequest):
    """Create/update a dependency edge between two tools."""
    store = get_tool_store()
    try:
        return store.upsert_tool_dependency(
            source_tool_ref=request.source_tool,
            target_tool_ref=request.target_tool,
            dependency_type=request.dependency_type,
            confidence=request.confidence,
            call_count=request.call_count,
            metadata=request.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/dependencies")
async def list_tool_dependencies(
    tool_name: Optional[str] = Query(default=None),
    dependency_type: Optional[str] = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """List durable dependency edges for tools."""
    store = get_tool_store()
    dependencies = store.list_tool_dependencies(
        tool_ref=tool_name,
        dependency_type=dependency_type,
        min_confidence=min_confidence,
        limit=limit,
    )
    return {
        "count": len(dependencies),
        "dependencies": dependencies,
    }


@router.post("/dependencies/infer")
async def infer_tool_dependencies(request: InferDependenciesRequest):
    """
    Infer dependency edges from persisted workflow chains and optionally persist them.
    """
    store = get_tool_store()
    execution_store = get_execution_store()
    chain_data = execution_store.get_workflow_chain_analytics(
        hours=request.hours,
        limit=request.limit,
        min_chain_length=2,
    )
    inferred_map: Dict[str, Dict[str, Any]] = {}

    for chain in chain_data.get("chains", []):
        steps = chain.get("steps") or []
        runs = int(chain.get("runs") or 0)
        completed = int(chain.get("completed") or 0)
        failed = int(chain.get("failed") or 0)
        if len(steps) < 2:
            continue
        for idx in range(len(steps) - 1):
            source_name = str(steps[idx] or "").strip()
            target_name = str(steps[idx + 1] or "").strip()
            if not source_name or not target_name:
                continue
            source_tool_id = store.resolve_tool_id(source_name)
            target_tool_id = store.resolve_tool_id(target_name)
            if not source_tool_id or not target_tool_id:
                continue
            edge_key = f"{source_tool_id}->{target_tool_id}"
            bucket = inferred_map.setdefault(
                edge_key,
                {
                    "source_tool_id": source_tool_id,
                    "source_name": source_name,
                    "target_tool_id": target_tool_id,
                    "target_name": target_name,
                    "support": 0,
                    "completed": 0,
                    "failed": 0,
                },
            )
            bucket["support"] += runs
            bucket["completed"] += completed
            bucket["failed"] += failed

    inferred_edges: List[Dict[str, Any]] = []
    persisted = 0
    for edge in inferred_map.values():
        support = int(edge["support"])
        if support < request.min_support:
            continue
        confidence = float(edge["completed"]) / support if support else 0.0
        edge_record = {
            "source_tool_id": edge["source_tool_id"],
            "source_name": edge["source_name"],
            "target_tool_id": edge["target_tool_id"],
            "target_name": edge["target_name"],
            "dependency_type": "inferred",
            "confidence": confidence,
            "support": support,
            "completed": int(edge["completed"]),
            "failed": int(edge["failed"]),
            "window_hours": request.hours,
        }
        inferred_edges.append(edge_record)

        if request.persist:
            try:
                store.upsert_tool_dependency(
                    source_tool_ref=edge["source_tool_id"],
                    target_tool_ref=edge["target_tool_id"],
                    dependency_type="inferred",
                    confidence=confidence,
                    call_count=support,
                    metadata={
                        "window_hours": request.hours,
                        "completed": int(edge["completed"]),
                        "failed": int(edge["failed"]),
                    },
                )
                persisted += 1
            except Exception as exc:
                logger.warning(f"Failed to persist inferred dependency {edge['source_tool_id']}->{edge['target_tool_id']}: {exc}")

    inferred_edges.sort(key=lambda item: (-int(item["support"]), -float(item["confidence"])))
    return {
        "window_hours": request.hours,
        "min_support": request.min_support,
        "count": len(inferred_edges),
        "persisted": persisted,
        "dependencies": inferred_edges,
    }


@router.get("/export/schema-bundle")
async def export_schema_bundle(
    limit: int = Query(default=1000, ge=1, le=10000),
):
    """Export registry schemas as a JSON Schema bundle keyed by tool_id."""
    store = get_tool_store()
    tools = store.list_tools(limit=limit, offset=0)
    bundle: Dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "generated_at": datetime.utcnow().isoformat(),
        "count": len(tools),
        "tools": {},
    }
    for tool in tools:
        bundle["tools"][tool.tool_id] = {
            "name": tool.name,
            "backend_id": tool.backend_id,
            "description": tool.description,
            "input_schema": tool.input_schema or {"type": "object", "properties": {}},
            "output_schema": tool.output_schema or {"type": "object", "properties": {}},
            "tags": tool.tags or [],
            "category": tool.category,
            "version": tool.version,
        }
    return bundle


@router.get("/export/mcp-tools")
async def export_mcp_tools(
    limit: int = Query(default=1000, ge=1, le=10000),
):
    """Export durable registry entries in MCP tool-list format."""
    store = get_tool_store()
    tools = store.list_tools(limit=limit, offset=0)
    result: List[Dict[str, Any]] = []
    for tool in tools:
        result.append(
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema or {"type": "object", "properties": {}},
                "outputSchema": tool.output_schema or None,
                "annotations": {
                    "tool_id": tool.tool_id,
                    "backend": tool.backend_id,
                    "category": tool.category,
                    "tags": tool.tags or [],
                    "version": tool.version,
                    "avg_latency_ms": float(tool.avg_latency_ms or 0.0),
                    "error_rate": float(tool.error_count or 0) / max(1, int(tool.call_count or 0)),
                },
            }
        )
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "count": len(result),
        "tools": result,
    }


@router.get("/export/openapi")
async def export_openapi_tool_catalog(
    limit: int = Query(default=500, ge=1, le=5000),
):
    """
    Export an OpenAPI-compatible catalog for general REST clients.

    The exported schema focuses on dynamic execution endpoints while embedding
    per-tool argument schemas under components for client-side validation.
    """
    store = get_tool_store()
    tools = store.list_tools(limit=limit, offset=0)

    tool_components: Dict[str, Any] = {}
    tool_index: List[Dict[str, Any]] = []
    for tool in tools:
        component_name = tool.tool_id.replace(":", "__").replace("-", "_")
        tool_components[component_name] = {
            "type": "object",
            "properties": tool.input_schema.get("properties", {}) if isinstance(tool.input_schema, dict) else {},
            "required": tool.input_schema.get("required", []) if isinstance(tool.input_schema, dict) else [],
            "additionalProperties": True,
            "description": tool.description or f"Arguments for {tool.tool_id}",
        }
        tool_index.append(
            {
                "tool_id": tool.tool_id,
                "name": tool.name,
                "backend_id": tool.backend_id,
                "component": f"#/components/schemas/{component_name}",
            }
        )

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "DSCO Tool Management API - Tool Catalog",
            "version": "1.0.0",
            "description": "Dynamic execution API with durable tool registry metadata.",
        },
        "servers": [{"url": "/"}],
        "paths": {
            "/api/v1/execute": {
                "post": {
                    "summary": "Execute a tool (sync/stream/job/auto)",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "tool_name": {"type": "string"},
                                        "arguments": {"type": "object"},
                                        "execution_mode": {
                                            "type": "string",
                                            "enum": ["auto", "sync", "stream", "job"],
                                            "default": "auto",
                                        },
                                        "stream": {"type": "boolean", "default": False},
                                        "timeout_ms": {"type": "integer", "default": 30000},
                                        "sync_timeout_ms": {"type": "integer", "default": 5000},
                                        "idempotency_key": {"type": "string"},
                                    },
                                    "required": ["tool_name", "arguments"],
                                    "additionalProperties": False,
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Execution result or job handle",
                            "content": {"application/json": {"schema": {"type": "object"}}},
                        }
                    },
                }
            },
            "/api/v1/tools/execute": {
                "post": {
                    "summary": "Execute a single tool through tools API",
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {
                        "200": {
                            "description": "Tool result",
                            "content": {"application/json": {"schema": {"type": "object"}}},
                        }
                    },
                }
            },
        },
        "components": {
            "schemas": tool_components,
        },
        "x-tool-catalog": {
            "generated_at": datetime.utcnow().isoformat(),
            "count": len(tool_index),
            "tools": tool_index,
        },
    }


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
    window_metrics = store.get_tool_latency_metrics(tool.tool_id, hours=24) or {}
    dependencies = store.list_tool_dependencies(tool_ref=tool.tool_id, limit=20)
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
        "latency_metrics_24h": window_metrics,
        "dependencies": dependencies,
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
