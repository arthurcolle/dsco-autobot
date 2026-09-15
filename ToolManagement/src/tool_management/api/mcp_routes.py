"""
MCP Protocol Routes - Server-Sent Events and JSON-RPC endpoints

Exposes the entire tool registry as an MCP server that AI agents can connect to.
"""
from __future__ import annotations

import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, AsyncGenerator, List
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
import httpx

from ..registry import get_registry
from ..queue import get_task_queue
from ..integrations.mcp_server import MCPServer, MCPRequest, MCPResponse, MCPNotification
from ..oauth import get_token_identity, require_bearer, make_401_response
from ..persistence import get_tool_store

# Output guard for enforcing size limits and URL capture
try:
    from ..guards.output_guard import apply_output_guard
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/mcp", tags=["mcp"])

# Global MCP server instance
_mcp_server: MCPServer | None = None

# Customer MCP clients get the progressive-disclosure surface only. Execution
# is intercepted below and routed through the tenant-aware canonical tools API;
# operator/service tokens retain the complete control-plane MCP catalog.
_CUSTOMER_MCP_TOOLS = {"search_tools", "execute_tool"}


def _customer_id_for_request(request: Request, token: str) -> str | None:
    identity = get_token_identity(token) or {}
    if identity.get("token_format") == "zitadel_jwt":
        from ..auth.unified import enforce_audience, select_account_id, verify_token

        claims = verify_token(token)
        if claims is None or claims.token_format != "zitadel_jwt":
            raise PermissionError("customer_identity_invalid")
        enforce_audience(claims, "tools")
        requested_account_id = request.headers.get("x-dsco-account-id")
        if claims.is_platform_admin and not requested_account_id:
            return None
        return select_account_id(claims, requested_account_id)
    if identity.get("user_id"):
        return str(identity["user_id"])
    try:
        from .user_routes import resolve_user_from_request

        user = resolve_user_from_request(request)
    except Exception:
        user = None
    if user and user.get("user_id"):
        return str(user["user_id"])
    return None


def _customer_denied_response(request_id: Any, tool_name: str) -> MCPResponse:
    return MCPResponse(
        id=request_id,
        result={
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "error": "customer_mcp_tool_denied",
                            "tool": tool_name,
                            "allowed_tools": sorted(_CUSTOMER_MCP_TOOLS),
                        }
                    ),
                }
            ],
            "isError": True,
        },
    )


async def _handle_customer_execution(
    mcp_request: MCPRequest, http_request: Request
) -> MCPResponse:
    """Execute through the tenant-binding tools API, never the operator loopback."""
    params = mcp_request.params or {}
    tool_args = params.get("arguments") or {}
    target_tool = tool_args.get("tool") or tool_args.get("tool_name")
    arguments = tool_args.get("arguments") or {}
    if not target_tool or not isinstance(arguments, dict):
        return MCPResponse(
            id=mcp_request.id,
            result={
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "error": "invalid_customer_tool_call",
                                "detail": "execute_tool requires tool and object arguments",
                            }
                        ),
                    }
                ],
                "isError": True,
            },
        )

    from .tools_api import ToolCall, execute_single_tool

    try:
        timeout_ms = int(tool_args.get("timeout_ms", 60000))
        call = ToolCall(
            tool=str(target_tool),
            arguments=arguments,
            timeout=max(0.1, min(float(timeout_ms) / 1000.0, 600.0)),
            idempotency_key=tool_args.get("idempotency_key"),
            trace_id=tool_args.get("trace_id"),
            session_id=tool_args.get("session_id"),
        )
        result = await execute_single_tool(call, http_request)
        payload = result.model_dump(mode="json")
        return MCPResponse(
            id=mcp_request.id,
            result={
                "content": [{"type": "text", "text": json.dumps(payload)}],
                "isError": not bool(result.success),
            },
        )
    except HTTPException as exc:
        detail = exc.detail
        payload = detail if isinstance(detail, dict) else {"error": str(detail)}
        return MCPResponse(
            id=mcp_request.id,
            result={
                "content": [{"type": "text", "text": json.dumps(payload)}],
                "isError": True,
            },
        )


async def _fetch_backend_tools(backend_url: str, timeout: float = 5.0) -> List[Dict[str, Any]]:
    """Fetch tools from a backend MCP server"""
    try:
        async with httpx.AsyncClient() as client:
            # Try to get tools via MCP protocol
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


def get_mcp_server() -> MCPServer:
    """Get or create the MCP server instance"""
    global _mcp_server

    if _mcp_server is None:
        # Initialize with EMPTY tool storage - we use meta-tools for lazy discovery
        # This prevents loading hundreds of tools into memory upfront
        _mcp_server = MCPServer(tool_storage={})
        logger.info("Initialized MCP server with meta-tools for lazy discovery")

    return _mcp_server


async def refresh_mcp_tools():
    """Refresh the MCP server's tool storage from registry AND native MCP backends"""
    mcp_server = get_mcp_server()
    if mcp_server:
        registry = get_registry()
        queue = get_task_queue()

        # Get all tools from registry in MCP format
        mcp_tools = registry.to_mcp_tools()
        tools_dict = {tool["name"]: tool for tool in mcp_tools}

        # Fetch tools from all native MCP backends
        backends = queue.list_backends()
        for backend in backends:
            is_native = backend.metadata.get("native", False) if hasattr(backend, 'metadata') else False
            if is_native and hasattr(backend, 'url'):
                backend_tools = await _fetch_backend_tools(backend.url)
                for tool in backend_tools:
                    # Add backend prefix to avoid name collisions
                    tool_name = tool.get("name")
                    if tool_name and tool_name not in tools_dict:
                        # Add metadata about which backend this came from
                        if "annotations" not in tool:
                            tool["annotations"] = {}
                        tool["annotations"]["backend"] = backend.id
                        tool["annotations"]["backend_url"] = backend.url
                        tools_dict[tool_name] = tool

                logger.info(f"Added {len(backend_tools)} tools from {backend.id}")

        mcp_server.tool_storage = tools_dict
        logger.info(f"Refreshed MCP server with {len(tools_dict)} total tools")


@router.get("/sse")
async def mcp_sse_get(request: Request):
    """
    MCP Server-Sent Events endpoint (GET).

    This allows MCP clients to connect using SSE transport.
    AI agents can connect to this endpoint and access all registered tools.
    """
    # Auth gate — return 401 with RFC 9728 WWW-Authenticate if no valid Bearer token
    if require_bearer(request) is None:
        return make_401_response(request)

    session_id = str(uuid4())
    mcp_server = get_mcp_server()

    async def event_generator():
        """Generate SSE events following MCP standard pattern"""
        try:
            # Send initial connection message (like native MCP servers)
            yield f"data: {json.dumps({'type': 'connected', 'session_id': session_id})}\n\n"

            # Keep connection alive with heartbeats
            while True:
                if await request.is_disconnected():
                    break

                # Refresh tools periodically (non-blocking)
                asyncio.create_task(refresh_mcp_tools())

                # Send heartbeat (data event only, no event type)
                heartbeat_data = json.dumps({"type": "heartbeat", "timestamp": datetime.utcnow().isoformat()})
                yield f"data: {heartbeat_data}\n\n"

                await asyncio.sleep(30)  # Heartbeat every 30 seconds

        except asyncio.CancelledError:
            logger.info(f"SSE connection cancelled for session {session_id}")
        except Exception as e:
            logger.error(f"Error in SSE event generator: {e}")
        finally:
            # Cleanup session
            if session_id in mcp_server.sessions:
                del mcp_server.sessions[session_id]
            logger.info(f"SSE connection closed for session {session_id}")

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
async def mcp_sse_post(request: Request):
    """
    MCP JSON-RPC endpoint via SSE URL (POST).

    When using mcp-remote with an SSE URL, it POSTs JSON-RPC to the same URL.
    This endpoint handles those POST requests.
    """
    if require_bearer(request) is None:
        return make_401_response(request)
    return await mcp_jsonrpc(request)


@router.post("")
async def mcp_jsonrpc(request: Request):
    """
    MCP JSON-RPC endpoint for handling MCP protocol requests.

    This handles all MCP protocol methods:
    - initialize
    - tools/list
    - tools/call
    - resources/list
    - resources/read
    - etc.
    """
    token = require_bearer(request)
    if token is None:
        return make_401_response(request)

    logger.info(f"Received MCP request to {request.url.path} from {request.client}")
    session_id = request.headers.get("X-Session-ID", str(uuid4()))
    try:
        customer_id = _customer_id_for_request(request, token)
    except PermissionError as exc:
        return JSONResponse(
            status_code=403,
            content={"error": "forbidden", "detail": str(exc).split(":", 1)[0]},
        )
    if customer_id:
        session_id = f"customer:{customer_id}:{session_id}"
    mcp_server = get_mcp_server()

    body = {}
    try:
        # Parse the JSON-RPC message
        body = await request.json()

        # NOTE: NOT refreshing tools on every request - using lazy meta-tools instead
        # This prevents loading hundreds of tools into memory on each request

        # Check if it's a request or notification
        if "id" in body:
            # It's a request
            mcp_request = MCPRequest(**body)

            # Special handling for tools/call - actually execute the tool
            if mcp_request.method == "tools/call":
                tool_name = str((mcp_request.params or {}).get("name") or "")
                if customer_id and tool_name not in _CUSTOMER_MCP_TOOLS:
                    response = _customer_denied_response(mcp_request.id, tool_name)
                elif customer_id and tool_name == "execute_tool":
                    response = await _handle_customer_execution(mcp_request, request)
                else:
                    response = await _handle_tool_execution(mcp_request, session_id)
            else:
                response = await mcp_server.handle_request(mcp_request, session_id)
                if customer_id and mcp_request.method == "tools/list" and response.result:
                    tools = response.result.get("tools") or []
                    response.result = {
                        "tools": [
                            tool for tool in tools if tool.get("name") in _CUSTOMER_MCP_TOOLS
                        ]
                    }

            return response.model_dump(exclude_none=True)
        else:
            # It's a notification
            notification = MCPNotification(**body)
            await mcp_server.handle_notification(notification, session_id)
            # Streamable HTTP notifications are accepted without a JSON-RPC body.
            # Returning 200 + {status: ok} closes strict clients during initialize.
            return Response(status_code=202)

    except Exception as e:
        logger.error(f"Error handling MCP request: {e}")
        return {
            "jsonrpc": "2.0",
            "id": body.get("id") if isinstance(body, dict) else None,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }


async def _handle_tool_execution(mcp_request: MCPRequest, session_id: str) -> MCPResponse:
    """
    Handle actual tool execution - delegates to mcp_server which routes meta-tools properly.
    """
    # Delegate to the mcp_server's handler which knows about meta-tools
    mcp_server = get_mcp_server()
    return await mcp_server.handle_request(mcp_request, session_id)


@router.get("/health")
async def mcp_health():
    """Check MCP server health with aggregated stats from all backends"""
    mcp_server = get_mcp_server()
    queue = get_task_queue()
    registry = get_registry()

    # Get backend stats
    backends = queue.list_backends()
    healthy_backends = []
    total_backend_tools = 0

    for backend in backends:
        is_native = backend.metadata.get("native", False) if hasattr(backend, 'metadata') else False
        if is_native and hasattr(backend, 'url'):
            try:
                tools = await _fetch_backend_tools(backend.url, timeout=3.0)
                total_backend_tools += len(tools)
                healthy_backends.append({
                    "id": backend.id,
                    "name": backend.name,
                    "tools_count": len(tools),
                    "healthy": True
                })
            except Exception:
                healthy_backends.append({
                    "id": backend.id,
                    "name": backend.name,
                    "tools_count": 0,
                    "healthy": False
                })

    return {
        "status": "healthy",
        "protocol_version": "2024-11-05",
        "registry_tools": len(registry),
        "backend_tools": total_backend_tools,
        "total_tools": len(registry) + total_backend_tools,
        "meta_tools": 33,  # Includes discovery, execution, policy/gate telemetry, follow-up actions, recommendations, analytics, backend health, and export meta-tools.
        "active_sessions": len(mcp_server.sessions),
        "backends": healthy_backends
    }


@router.get("/tools")
async def mcp_aggregated_tools():
    """Get all tools aggregated from all backends"""
    queue = get_task_queue()
    registry = get_registry()
    store = get_tool_store()
    persisted_rows, _ = store.query_tools(
        limit=5000,
        offset=0,
        columns=[
            "tool_id",
            "name",
            "backend_id",
            "call_count",
            "error_count",
            "avg_latency_ms",
            "category",
            "tags",
            "description",
        ],
    )
    persisted_by_name = {row.get("name"): row for row in persisted_rows if row.get("name")}
    persisted_by_tool_id = {row.get("tool_id"): row for row in persisted_rows if row.get("tool_id")}

    all_tools = []

    # Registry tools
    registry_tools = registry.to_mcp_tools()
    for tool in registry_tools:
        tool["source"] = "registry"
        name = tool.get("name")
        persisted = persisted_by_name.get(name, {})
        calls = int(persisted.get("call_count", 0) or 0)
        errors = int(persisted.get("error_count", 0) or 0)
        tool["observability"] = {
            "tool_id": persisted.get("tool_id"),
            "backend_id": persisted.get("backend_id"),
            "call_count": calls,
            "error_count": errors,
            "error_rate": (float(errors) / calls) if calls else 0.0,
            "avg_latency_ms": float(persisted.get("avg_latency_ms", 0.0) or 0.0),
            "category": persisted.get("category"),
            "tags": persisted.get("tags", []),
        }
        all_tools.append(tool)

    # Backend tools
    backends = queue.list_backends()
    for backend in backends:
        is_native = backend.metadata.get("native", False) if hasattr(backend, 'metadata') else False
        if is_native and hasattr(backend, 'url'):
            try:
                tools = await _fetch_backend_tools(backend.url, timeout=5.0)
                for tool in tools:
                    tool["source"] = backend.id
                    tool["backend_url"] = backend.url
                    name = tool.get("name")
                    persisted = persisted_by_name.get(name)
                    if not persisted:
                        tool_id = f"{backend.id}:{name}" if name else None
                        persisted = persisted_by_tool_id.get(tool_id) if tool_id else None
                    if persisted:
                        calls = int(persisted.get("call_count", 0) or 0)
                        errors = int(persisted.get("error_count", 0) or 0)
                        tool["observability"] = {
                            "tool_id": persisted.get("tool_id"),
                            "backend_id": persisted.get("backend_id"),
                            "call_count": calls,
                            "error_count": errors,
                            "error_rate": (float(errors) / calls) if calls else 0.0,
                            "avg_latency_ms": float(persisted.get("avg_latency_ms", 0.0) or 0.0),
                            "category": persisted.get("category"),
                            "tags": persisted.get("tags", []),
                        }
                    all_tools.append(tool)
            except Exception as e:
                logger.warning(f"Failed to fetch tools from {backend.id}: {e}")

    return {
        "tools": all_tools,
        "count": len(all_tools),
        "sources": {
            "registry": len(registry_tools),
            **{b.id: 0 for b in backends if b.metadata.get("native", False)}
        }
    }


@router.get("/catalog")
async def mcp_catalog(
    hours: int = 24,
    limit: int = 500,
):
    """
    Rich MCP catalog payload for clients that need immediate routing/intelligence hints.
    """
    store = get_tool_store()
    rows, total = store.query_tools(
        limit=max(1, min(int(limit), 5000)),
        offset=0,
        sort_by="call_count",
        sort_order="desc",
    )
    backend_metrics = store.get_backend_latency_metrics(hours=max(1, int(hours)))
    dependencies = store.list_tool_dependencies(limit=500)

    dependency_index: Dict[str, List[Dict[str, Any]]] = {}
    for edge in dependencies:
        source_id = edge.get("source_tool_id")
        if not source_id:
            continue
        dependency_index.setdefault(source_id, []).append(edge)

    tools_payload: List[Dict[str, Any]] = []
    for row in rows:
        calls = int(row.get("call_count", 0) or 0)
        errors = int(row.get("error_count", 0) or 0)
        tools_payload.append(
            {
                "tool_id": row.get("tool_id"),
                "name": row.get("name"),
                "backend_id": row.get("backend_id"),
                "description": row.get("description"),
                "category": row.get("category"),
                "tags": row.get("tags", []),
                "stats": {
                    "call_count": calls,
                    "error_count": errors,
                    "error_rate": (float(errors) / calls) if calls else 0.0,
                    "avg_latency_ms": float(row.get("avg_latency_ms", 0.0) or 0.0),
                },
                "dependencies": dependency_index.get(row.get("tool_id"), []),
            }
        )

    return {
        "generated_at": datetime.utcnow().isoformat(),
        "count": len(tools_payload),
        "total": int(total),
        "tools": tools_payload,
        "backend_metrics": backend_metrics,
    }


@router.post("/batch")
async def mcp_batch_execute(request: Request):
    """
    Execute multiple tool calls in batch across backends.

    Request body:
    {
        "calls": [
            {"tool": "search", "arguments": {"query": "AI"}, "backend": "distributed-search-mcp"},
            {"tool": "distributed-search-mcp:search", "arguments": {"query": "AI"}},
            {"tool": "store_belief", "arguments": {...}, "backend": "distributed-memory-mcp"}
        ],
        "parallel": true
    }

    Tool names can be:
    - Simple name with explicit backend: {"tool": "search", "backend": "distributed-search-mcp"}
    - Namespaced: {"tool": "distributed-search-mcp:search"} - backend auto-detected
    """
    body = await request.json()
    calls = body.get("calls", [])
    parallel = body.get("parallel", True)

    queue = get_task_queue()
    backends_map = {b.id: b for b in queue.list_backends()}

    async def execute_single(call: dict) -> dict:
        tool_name = call.get("tool")
        arguments = call.get("arguments", {})
        backend_id = call.get("backend")

        # Auto-parse namespaced tool names: "backend-id:tool-name"
        if not backend_id and ":" in tool_name:
            backend_id, tool_name = tool_name.split(":", 1)

        if not backend_id or backend_id not in backends_map:
            return {"tool": tool_name, "error": f"Backend not found: {backend_id}"}

        backend = backends_map[backend_id]
        if not hasattr(backend, 'url'):
            return {"tool": tool_name, "error": "Backend has no URL"}

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{backend.url}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": str(uuid4()),
                        "method": "tools/call",
                        "params": {"name": tool_name, "arguments": arguments}
                    },
                    timeout=30.0
                )

                if response.status_code == 200:
                    result = response.json()
                    result_data = result.get("result")
                    if OUTPUT_GUARD_AVAILABLE and result_data is not None:
                        result_data = apply_output_guard(
                            result_data,
                            tool_name=f"{backend_id}:{tool_name}",
                            source_context={"backend": backend_id, "via": "mcp_batch"},
                        )
                    return {
                        "tool": tool_name,
                        "backend": backend_id,
                        "result": result_data,
                        "success": True
                    }
                else:
                    return {
                        "tool": tool_name,
                        "backend": backend_id,
                        "error": f"HTTP {response.status_code}",
                        "success": False
                    }
        except Exception as e:
            return {
                "tool": tool_name,
                "backend": backend_id,
                "error": str(e),
                "success": False
            }

    import asyncio
    if parallel:
        results = await asyncio.gather(*[execute_single(c) for c in calls])
    else:
        results = []
        for call in calls:
            results.append(await execute_single(call))

    return {
        "results": results,
        "total": len(calls),
        "successful": sum(1 for r in results if r.get("success")),
        "failed": sum(1 for r in results if not r.get("success"))
    }


@router.post("/pipeline")
async def mcp_pipeline_execute(request: Request):
    """
    Execute a pipeline of tool calls where output flows to next input.

    Request body:
    {
        "steps": [
            {"tool": "search", "arguments": {"query": "AI agents"}, "backend": "distributed-search-mcp"},
            {"tool": "store_belief", "arguments": {"belief": "$prev.results[0].title"}, "backend": "distributed-memory-mcp"}
        ]
    }
    """
    import re

    body = await request.json()
    steps = body.get("steps", [])

    queue = get_task_queue()
    backends_map = {b.id: b for b in queue.list_backends()}

    results = []
    prev_result = None

    def substitute_vars(obj, prev):
        """Substitute $prev references with actual values"""
        if isinstance(obj, str):
            if obj.startswith("$prev"):
                # Extract path like $prev.results[0].title
                path = obj[5:]  # Remove $prev
                if not path:
                    return prev

                value = prev
                for part in re.split(r'\.|\[|\]', path):
                    if not part:
                        continue
                    if part.isdigit():
                        value = value[int(part)]
                    elif isinstance(value, dict):
                        value = value.get(part)
                    else:
                        return obj  # Can't resolve
                return value
            return obj
        elif isinstance(obj, dict):
            return {k: substitute_vars(v, prev) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [substitute_vars(v, prev) for v in obj]
        return obj

    for i, step in enumerate(steps):
        tool_name = step.get("tool")
        arguments = step.get("arguments", {})
        backend_id = step.get("backend")

        # Auto-parse namespaced tool names: "backend-id:tool-name"
        if not backend_id and ":" in tool_name:
            backend_id, tool_name = tool_name.split(":", 1)

        # Substitute $prev references
        if prev_result:
            arguments = substitute_vars(arguments, prev_result)

        if not backend_id or backend_id not in backends_map:
            results.append({"step": i, "tool": tool_name, "error": f"Backend not found: {backend_id}"})
            break

        backend = backends_map[backend_id]

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{backend.url}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": str(uuid4()),
                        "method": "tools/call",
                        "params": {"name": tool_name, "arguments": arguments}
                    },
                    timeout=30.0
                )

                if response.status_code == 200:
                    result = response.json()
                    prev_result = result.get("result", {})
                    if OUTPUT_GUARD_AVAILABLE and prev_result is not None:
                        prev_result = apply_output_guard(
                            prev_result,
                            tool_name=f"{backend_id}:{tool_name}",
                            source_context={"backend": backend_id, "via": "mcp_pipeline", "step": i},
                        )
                    results.append({
                        "step": i,
                        "tool": tool_name,
                        "backend": backend_id,
                        "result": prev_result,
                        "success": True
                    })
                else:
                    results.append({
                        "step": i,
                        "tool": tool_name,
                        "backend": backend_id,
                        "error": f"HTTP {response.status_code}",
                        "success": False
                    })
                    break  # Stop pipeline on failure

        except Exception as e:
            results.append({
                "step": i,
                "tool": tool_name,
                "backend": backend_id,
                "error": str(e),
                "success": False
            })
            break

    return {
        "pipeline_results": results,
        "completed_steps": len(results),
        "total_steps": len(steps),
        "success": all(r.get("success") for r in results)
    }


@router.get("/capabilities")
async def mcp_capabilities():
    """Get MCP server capabilities"""
    return {
        "protocolVersion": "2024-11-05",
        "serverInfo": {
            "name": "Tool Registry MCP Server",
            "version": "1.0.0"
        },
        "capabilities": {
            "tools": {"listChanged": True},
            "resources": {"subscribe": True, "listChanged": True},
            "prompts": {"listChanged": True},
            "analytics": {
                "usage": True,
                "toolMetrics": True,
                "dependencyGraph": True,
                "backendHealth": True,
            },
            "catalog": {
                "exportFormats": ["mcp", "openapi", "schemas"],
            },
        }
    }
