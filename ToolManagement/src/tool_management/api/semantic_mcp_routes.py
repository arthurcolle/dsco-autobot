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
    session_id = request.headers.get("X-Session-ID", str(uuid4()))
    tool_store = get_tool_store()

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
                response = await _handle_tool_call_tracked(mcp_request, session_id)
            elif mcp_request.method == "initialize":
                response = await _handle_initialize(mcp_request, session_id)
            else:
                # Delegate to standard MCP handler
                response = MCPResponse(
                    id=mcp_request.id,
                    error={
                        "code": -32601,
                        "message": f"Method not found: {mcp_request.method}"
                    }
                )

            return response.model_dump(exclude_none=True)
        else:
            # Notification
            return {"status": "ok"}

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
                    "listChanged": True,
                    "semanticSearch": True  # Extension capability
                },
                "resources": {"subscribe": True, "listChanged": True},
                "prompts": {"listChanged": True}
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


async def _handle_tool_call_tracked(mcp_request: MCPRequest, session_id: str) -> MCPResponse:
    """Handle tool call with access tracking"""
    tool_store = get_tool_store()
    params = mcp_request.params or {}

    tool_name = params.get("name")
    tool_args = params.get("arguments", {})

    # Mark tool as accessed
    if tool_name:
        tool_store.mark_accessed(tool_name)

    # Execute tool via composition API
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://localhost:2016/compose/seq",
                json={
                    "tools": [tool_name],
                    "arguments": [tool_args],
                    "inputs": {}
                },
                timeout=60.0
            )

            if response.status_code == 200:
                result = response.json()

                return MCPResponse(
                    id=mcp_request.id,
                    result={
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result.get("result"), indent=2)
                            }
                        ],
                        "isError": result.get("status") == "failed"
                    }
                )
            else:
                raise Exception(f"Tool execution failed: {response.text}")

    except Exception as e:
        logger.error(f"Error executing tool {tool_name}: {e}")
        return MCPResponse(
            id=mcp_request.id,
            result={
                "content": [
                    {
                        "type": "text",
                        "text": f"Error executing tool: {str(e)}"
                    }
                ],
                "isError": True
            }
        )


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
