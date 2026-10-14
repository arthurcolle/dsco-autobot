"""
Semantic MCP Routes - Intelligent Tool Discovery at Scale

Extends MCP protocol with semantic search capabilities.
Enables scaling to millions of tools through intelligent context management.
"""
from __future__ import annotations

import json
import asyncio
import logging
from typing import Dict, Any, List, Optional
from uuid import uuid4

from fastapi import APIRouter, Request, Query
from fastapi.responses import StreamingResponse

from ..oauth import require_bearer, make_401_response
import httpx

from ..registry import get_registry
from ..queue import get_task_queue
from ..integrations.mcp_server import MCPServer, MCPRequest, MCPResponse, MCPNotification
from ..embeddings.semantic_tool_store import get_tool_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/semantic-mcp", tags=["semantic-mcp"])


# This will be called from the main app startup
async def initialize_semantic_index():
    """Initialize semantic tool index (call from app startup)"""
    tool_store = get_tool_store(max_context_tools=50)

    # Index tools from registry
    registry = get_registry()
    registry_tools = registry.to_mcp_tools()

    logger.info(f"Indexing {len(registry_tools)} tools from registry...")
    await tool_store.index_tools_batch(registry_tools, backend="registry")

    # Index tools from backends
    queue = get_task_queue()
    backends = queue.list_backends()

    for backend in backends:
        is_native = backend.metadata.get("native", False) if hasattr(backend, 'metadata') else False
        if is_native and backend.is_healthy:
            try:
                # Fetch tools from backend
                backend_tools = await _fetch_backend_tools(backend.url)
                logger.info(f"Indexing {len(backend_tools)} tools from {backend.id}...")
                await tool_store.index_tools_batch(backend_tools, backend=backend.id)
            except Exception as e:
                logger.error(f"Failed to index tools from {backend.id}: {e}")

    stats = await tool_store.get_stats()
    logger.info(f"Tool indexing complete. Total tools: {stats['total_tools']}")


async def _fetch_backend_tools(backend_url: str, timeout: float = 5.0) -> List[Dict[str, Any]]:
    """Fetch tools from a backend MCP server"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{backend_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": str(uuid4()),
                    "method": "tools/list",
                    "params": {}
                },
                timeout=timeout
            )

            if response.status_code == 200:
                result = response.json()
                if "result" in result and "tools" in result["result"]:
                    return result["result"]["tools"]
    except Exception as e:
        logger.debug(f"Failed to fetch tools from {backend_url}: {e}")

    return []


@router.get("/sse")
async def semantic_mcp_sse_get(request: Request):
    """
    Semantic MCP SSE endpoint (GET).

    Like regular MCP but with semantic tool discovery.
    """
    if require_bearer(request) is None:
        return make_401_response(request)

    session_id = str(uuid4())

    async def event_generator():
        """Generate SSE events following MCP standard pattern"""
        try:
            # Send initial connection message (like native MCP servers)
            from datetime import datetime
            yield f"data: {json.dumps({'type': 'connected', 'session_id': session_id})}\n\n"

            while True:
                if await request.is_disconnected():
                    break

                # Send heartbeat (data event only, no event type)
                heartbeat_data = json.dumps({"type": "heartbeat", "timestamp": datetime.utcnow().isoformat()})
                yield f"data: {heartbeat_data}\n\n"

                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.info(f"SSE connection cancelled for session {session_id}")
        except Exception as e:
            logger.error(f"Error in SSE event generator: {e}")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@router.post("/sse")
async def semantic_mcp_sse_post(request: Request):
    """
    Semantic MCP JSON-RPC endpoint via SSE URL (POST).

    When using mcp-remote with an SSE URL, it POSTs JSON-RPC to the same URL.
    This endpoint handles those POST requests.
    """
    # Delegate to the main semantic MCP JSON-RPC handler
    return await semantic_mcp_jsonrpc(request)


@router.post("")
async def semantic_mcp_jsonrpc(request: Request):
    """
    Semantic MCP JSON-RPC endpoint.

    Supports standard MCP protocol + semantic search extensions.
    """
    if require_bearer(request) is None:
        return make_401_response(request)

    session_id = request.headers.get("X-Session-ID", str(uuid4()))

    body = {}
    try:
        body = await request.json()

        # Handle different MCP methods
        if "id" in body:
            mcp_request = MCPRequest(**body)

            if mcp_request.method == "tools/list":
                # Semantic tool discovery
                response = await _handle_semantic_tools_list(mcp_request, session_id)
            elif mcp_request.method == "tools/search":
                # New semantic search method
                response = await _handle_tools_search(mcp_request, session_id)
            elif mcp_request.method == "tools/call":
                # Execute tool with access tracking
                return await _handle_tool_call_tracked(mcp_request, session_id, request)
            elif mcp_request.method == "initialize":
                response = await _handle_initialize(mcp_request, session_id)
            else:
                # Preserve the original request and its tenant/authentication
                # context for standard MCP methods. The canonical handler
                # owns resources, prompts and future protocol extensions.
                from .mcp_routes import mcp_jsonrpc

                return await mcp_jsonrpc(request)

            return response.model_dump(exclude_none=True)
        else:
            # The canonical endpoint accepts notifications with HTTP 202;
            # a JSON success body breaks strict Streamable HTTP clients.
            from .mcp_routes import mcp_jsonrpc

            return await mcp_jsonrpc(request)

    except Exception as e:
        logger.error(f"Error handling semantic MCP request: {e}")
        return {
            "jsonrpc": "2.0",
            "id": body.get("id") if isinstance(body, dict) else None,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }


async def _handle_initialize(mcp_request: MCPRequest, session_id: str) -> MCPResponse:
    """Handle initialize with semantic capabilities"""
    return MCPResponse(
        id=mcp_request.id,
        result={
            "serverInfo": {
                "id": "mcpsrv_semantic_search",
                "name": "Semantic Tool Registry MCP Server",
                "version": "1.0.0"
            },
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {
                    "semanticSearch": True  # Extension capability
                },
                "prompts": {},
                "resources": {}
            }
        }
    )


async def _handle_semantic_tools_list(mcp_request: MCPRequest, session_id: str) -> MCPResponse:
    """
    Handle tools/list with semantic filtering.

    Params:
        - query (optional): Semantic search query
        - limit (optional): Max tools to return (default: 50)
    """
    tool_store = get_tool_store()
    params = mcp_request.params or {}

    query = params.get("query")
    limit = params.get("limit", 50)

    if query:
        # Semantic search
        results = await tool_store.search_tools(query, limit=limit)
        tools = [r["tool"] for r in results]

        # Load top results into context
        for result in results[:10]:  # Load top 10 into context
            tool_store.load_into_context(result["tool"]["name"], result["tool"])
    else:
        # Return active context
        tools = tool_store.get_active_tools()

        # If context empty, load most accessed tools
        if not tools:
            tools = await tool_store.get_relevant_tools(limit=limit)
            for tool in tools[:10]:
                tool_store.load_into_context(tool["name"], tool)

    return MCPResponse(
        id=mcp_request.id,
        result={
            "tools": tools,
            "metadata": {
                "total_available": (await tool_store.get_stats())["total_tools"],
                "returned": len(tools),
                "in_context": len(tool_store.context.active_tools)
            }
        }
    )


async def _handle_tools_search(mcp_request: MCPRequest, session_id: str) -> MCPResponse:
    """
    Handle semantic tool search.

    Params:
        - query: Search query (required)
        - limit: Max results (default: 10)
        - min_similarity: Minimum similarity threshold (default: 0.0)
    """
    tool_store = get_tool_store()
    params = mcp_request.params or {}

    query = params.get("query")
    if not query:
        return MCPResponse(
            id=mcp_request.id,
            error={
                "code": -32602,
                "message": "Missing required parameter: query"
            }
        )

    limit = params.get("limit", 10)
    min_similarity = params.get("min_similarity", 0.0)

    results = await tool_store.search_tools(query, limit=limit, min_similarity=min_similarity)

    return MCPResponse(
        id=mcp_request.id,
        result={
            "tools": [r["tool"] for r in results],
            "similarities": [r["similarity"] for r in results],
            "metadata": {
                "query": query,
                "returned": len(results)
            }
        }
    )


async def _handle_tool_call_tracked(
    mcp_request: MCPRequest, session_id: str, http_request: Request
):
    """Track access and execute through the canonical, tenant-aware MCP route.

    An unauthenticated localhost composition call loses the caller's identity
    and bypasses the credential/billing policy used by the normal MCP surface.
    Access telemetry must also never make a valid execution fail.
    """
    params = mcp_request.params or {}
    tool_name = params.get("name")
    if tool_name:
        try:
            get_tool_store().mark_accessed(tool_name)
        except Exception:
            logger.warning("Could not track semantic tool access", exc_info=True)

    from .mcp_routes import mcp_jsonrpc

    return await mcp_jsonrpc(http_request)


@router.get("/health")
async def semantic_mcp_health():
    """Check semantic MCP server health"""
    tool_store = get_tool_store()
    stats = await tool_store.get_stats()

    return {
        "status": "healthy",
        "protocol_version": "DRAFT-2025-v3-semantic",
        "total_tools": stats["total_tools"],
        "active_in_context": stats["active_in_context"],
        "max_context_size": stats["max_context_size"]
    }


@router.get("/stats")
async def get_semantic_stats():
    """Get detailed statistics about semantic tool store"""
    tool_store = get_tool_store()
    return await tool_store.get_stats()


@router.post("/search")
async def search_tools(
    query: str = Query(..., description="Semantic search query"),
    limit: int = Query(10, description="Maximum results"),
    min_similarity: float = Query(0.0, description="Minimum similarity threshold")
):
    """
    REST endpoint for semantic tool search.

    This is an alternative to the MCP protocol for direct HTTP access.
    """
    tool_store = get_tool_store()
    results = await tool_store.search_tools(query, limit=limit, min_similarity=min_similarity)

    return {
        "query": query,
        "results": results,
        "count": len(results)
    }


@router.post("/reindex")
async def reindex_tools():
    """
    Manually trigger re-indexing of all tools with embeddings.

    This fetches tools from all backends and generates embeddings.
    """
    await initialize_semantic_index()
    tool_store = get_tool_store()
    stats = await tool_store.get_stats()
    return {
        "status": "reindexed",
        "total_tools": stats["total_tools"],
        "backends": stats["backends"]
    }
