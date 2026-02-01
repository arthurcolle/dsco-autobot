"""
API routes for Native MCP management.

Provides:
- Status endpoints for all native MCPs
- Start/stop/restart controls
- WebSocket sync endpoint for native MCPs
- Tool discovery aggregation
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Query
from pydantic import BaseModel

from ..native_mcp_manager import get_native_mcp_manager, NativeMCPStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/native", tags=["native-mcp"])


# =============================================================================
# MODELS
# =============================================================================

class NativeMCPInfo(BaseModel):
    id: str
    name: str
    port: int
    status: str
    started_at: Optional[str]
    last_health_check: Optional[str]
    tools_count: int
    capabilities: List[str]
    error: Optional[str]


class NativeMCPListResponse(BaseModel):
    running: bool
    servers: Dict[str, NativeMCPInfo]
    total_tools: int


class NativeMCPActionResponse(BaseModel):
    success: bool
    mcp_id: str
    action: str
    message: str


# =============================================================================
# STATUS ENDPOINTS
# =============================================================================

@router.get("", response_model=NativeMCPListResponse)
async def list_native_mcps():
    """List all native MCP servers and their status."""
    manager = get_native_mcp_manager()
    status = manager.get_status()

    return NativeMCPListResponse(
        running=status["running"],
        servers={
            mcp_id: NativeMCPInfo(**info)
            for mcp_id, info in status["servers"].items()
        },
        total_tools=sum(
            info["tools_count"]
            for info in status["servers"].values()
        ),
    )


@router.get("/{mcp_id}")
async def get_native_mcp(mcp_id: str):
    """Get status of a specific native MCP."""
    manager = get_native_mcp_manager()

    if mcp_id not in manager.servers:
        raise HTTPException(404, f"Native MCP not found: {mcp_id}")

    state = manager.servers[mcp_id]
    return {
        "id": state.config.id,
        "name": state.config.name,
        "port": state.config.port,
        "status": state.status.value,
        "started_at": state.started_at.isoformat() if state.started_at else None,
        "last_health_check": state.last_health_check.isoformat() if state.last_health_check else None,
        "tools": state.tools,
        "capabilities": state.config.capabilities,
        "error": state.error,
    }


@router.get("/{mcp_id}/tools")
async def get_native_mcp_tools(mcp_id: str):
    """Get tools from a specific native MCP."""
    manager = get_native_mcp_manager()

    if mcp_id not in manager.servers:
        raise HTTPException(404, f"Native MCP not found: {mcp_id}")

    state = manager.servers[mcp_id]
    return {
        "mcp_id": mcp_id,
        "tools": state.tools,
        "count": len(state.tools),
    }


@router.get("/tools/all")
async def get_all_native_tools():
    """Get all tools from all native MCPs."""
    manager = get_native_mcp_manager()
    tools = manager.get_all_tools()

    return {
        "tools": tools,
        "count": len(tools),
        "by_mcp": {
            mcp_id: len([t for t in tools if t.get("source_mcp") == mcp_id])
            for mcp_id in manager.servers.keys()
        },
    }


# =============================================================================
# CONTROL ENDPOINTS
# =============================================================================

@router.post("/{mcp_id}/start", response_model=NativeMCPActionResponse)
async def start_native_mcp(mcp_id: str):
    """Start a native MCP server."""
    manager = get_native_mcp_manager()

    if mcp_id not in manager.servers:
        raise HTTPException(404, f"Native MCP not found: {mcp_id}")

    success = await manager.start_server(mcp_id)

    return NativeMCPActionResponse(
        success=success,
        mcp_id=mcp_id,
        action="start",
        message=f"Started {mcp_id}" if success else f"Failed to start {mcp_id}",
    )


@router.post("/{mcp_id}/stop", response_model=NativeMCPActionResponse)
async def stop_native_mcp(mcp_id: str):
    """Stop a native MCP server."""
    manager = get_native_mcp_manager()

    if mcp_id not in manager.servers:
        raise HTTPException(404, f"Native MCP not found: {mcp_id}")

    success = await manager.stop_server(mcp_id)

    return NativeMCPActionResponse(
        success=success,
        mcp_id=mcp_id,
        action="stop",
        message=f"Stopped {mcp_id}" if success else f"Failed to stop {mcp_id}",
    )


@router.post("/{mcp_id}/restart", response_model=NativeMCPActionResponse)
async def restart_native_mcp(mcp_id: str):
    """Restart a native MCP server."""
    manager = get_native_mcp_manager()

    if mcp_id not in manager.servers:
        raise HTTPException(404, f"Native MCP not found: {mcp_id}")

    success = await manager.restart_server(mcp_id)

    return NativeMCPActionResponse(
        success=success,
        mcp_id=mcp_id,
        action="restart",
        message=f"Restarted {mcp_id}" if success else f"Failed to restart {mcp_id}",
    )


@router.post("/start-all")
async def start_all_native_mcps():
    """Start all native MCP servers."""
    manager = get_native_mcp_manager()
    results = {}

    for mcp_id in manager.servers:
        results[mcp_id] = await manager.start_server(mcp_id)

    return {
        "results": results,
        "all_success": all(results.values()),
    }


@router.post("/stop-all")
async def stop_all_native_mcps():
    """Stop all native MCP servers."""
    manager = get_native_mcp_manager()
    results = {}

    for mcp_id in manager.servers:
        results[mcp_id] = await manager.stop_server(mcp_id)

    return {
        "results": results,
        "all_success": all(results.values()),
    }


# =============================================================================
# WEBSOCKET SYNC ENDPOINT
# =============================================================================

@router.websocket("/sync")
async def native_mcp_sync_websocket(
    websocket: WebSocket,
    mcp_id: str = Query(..., description="Native MCP ID"),
):
    """
    WebSocket endpoint for native MCP synchronization.

    Native MCPs connect here to:
    - Register their tools and capabilities
    - Send heartbeats
    - Receive commands from the registry
    - Broadcast events
    """
    manager = get_native_mcp_manager()

    # Verify MCP exists
    if mcp_id not in manager.servers:
        await websocket.close(code=4004, reason=f"Unknown MCP: {mcp_id}")
        return

    # Connect via sync channel
    await manager.sync_channel.connect(websocket, mcp_id)

    try:
        while True:
            # Receive messages from native MCP
            data = await websocket.receive_json()
            await manager.sync_channel.handle_message(mcp_id, data)

    except WebSocketDisconnect:
        logger.info(f"Native MCP disconnected: {mcp_id}")
    except Exception as e:
        logger.error(f"WebSocket error for {mcp_id}: {e}")
    finally:
        await manager.sync_channel.disconnect(mcp_id)


# =============================================================================
# SSE ENDPOINT FOR EVENTS
# =============================================================================

@router.get("/events/stream")
async def native_mcp_events_stream():
    """
    Server-Sent Events stream for native MCP events.

    Subscribe to events from all native MCPs.
    """
    from fastapi.responses import StreamingResponse

    manager = get_native_mcp_manager()

    async def event_generator():
        # Send initial status
        yield f"data: {json.dumps({'type': 'connected', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"

        # Send current status
        status = manager.get_status()
        yield f"data: {json.dumps({'type': 'status', 'data': status})}\n\n"

        # Keep alive and send updates
        while True:
            await asyncio.sleep(30)

            # Send heartbeat with current status
            status = manager.get_status()
            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat(), 'servers': {mcp_id: info['status'] for mcp_id, info in status['servers'].items()}})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )
