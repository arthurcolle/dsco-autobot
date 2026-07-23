"""Core router for health checks, schema introspection, and query endpoints."""

from __future__ import annotations

from typing import Dict
import asyncio

from fastapi import APIRouter, Depends, Request, WebSocket, WebSocketDisconnect, HTTPException
from starlette.responses import StreamingResponse
import httpx
import aiohttp

from ..security import require_permissions, get_principal, Principal, principal_from_headers, has_permission
from ..schemas import (
    QueryRequest,
    QueryResponse,
    SingleResultResponse,
    TransactionRequest,
    SchemaResponse,
    IndexListResponse,
    ConstraintListResponse,
)
from .dependencies import (
    get as backend_get,
    post as backend_post,
    backend_url,
    ws_backend_url,
    is_write_cypher,
    env_allows_write,
    record_policy_decision,
    require_permissions_effective,
    enforce_abac,
)

router = APIRouter(tags=["core"])


@router.get("/health")
async def health() -> Dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@router.get("/v1/schema", response_model=SchemaResponse)
async def v1_schema(_: Principal = Depends(require_permissions(["schemas:read"]))):
    """Get the Neo4j graph schema."""
    data = await backend_get("/v1/schema")
    return SchemaResponse(**data)


@router.get("/v1/indexes", response_model=IndexListResponse)
async def v1_indexes(_: Principal = Depends(require_permissions(["schemas:read"]))):
    """List all Neo4j indexes."""
    data = await backend_get("/v1/indexes")
    return IndexListResponse(**data)


@router.get("/v1/constraints", response_model=ConstraintListResponse)
async def v1_constraints(_: Principal = Depends(require_permissions(["schemas:read"]))):
    """List all Neo4j constraints."""
    data = await backend_get("/v1/constraints")
    return ConstraintListResponse(**data)


@router.post("/v1/query", response_model=SingleResultResponse | QueryResponse)
async def v1_query(req: QueryRequest, request: Request, principal: Principal = Depends(get_principal)):
    """Execute a Cypher query with RBAC and environment-based access control."""
    # Classify query to gate read vs write
    needed = "query:write" if is_write_cypher(req.query) else "query:read"
    env_id = request.headers.get("X-Env-Id")
    if env_id and needed == "query:write":
        if not await env_allows_write(env_id):
            await record_policy_decision(action=needed, decision="deny", principal=principal, scope=f"env:{env_id}", reason="env write blocked")
            raise HTTPException(status_code=403, detail="Environment policy blocks write queries")
    if not has_permission(principal, needed):
        await record_policy_decision(action=needed, decision="deny", principal=principal, reason="missing permission")
        raise HTTPException(status_code=403, detail=f"Missing permission: {needed}")
    await record_policy_decision(action=needed, decision="allow", principal=principal)
    data = await backend_post("/v1/query", req.dict())
    # The backend already returns in the proper shape; validate and echo
    if "result" in data:
        return SingleResultResponse(**data)
    return QueryResponse(**data)


@router.post("/v1/tx", response_model=QueryResponse)
async def v1_tx(
    req: TransactionRequest,
    _rbac: None = Depends(require_permissions_effective(["query:write"])),
    _abac: None = Depends(enforce_abac("query:write")),
):
    """Execute a Neo4j transaction with multiple statements."""
    data = await backend_post("/v1/tx", req.dict())
    return QueryResponse(**data)


@router.post("/v1/query/stream")
async def v1_query_stream(req: QueryRequest, principal: Principal = Depends(get_principal)):
    """Execute a Cypher query and stream results as Server-Sent Events."""
    async def proxy_stream():
        url = f"{backend_url()}/v1/query/stream"
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("POST", url, json=req.dict()) as resp:
                if resp.status_code >= 400:
                    text = await resp.aread()
                    raise HTTPException(status_code=resp.status_code, detail=text.decode())
                async for chunk in resp.aiter_raw():
                    # pass-through SSE bytes
                    yield chunk

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(proxy_stream(), media_type="text/event-stream", headers=headers)


@router.websocket("/v1/query/ws")
async def v1_query_ws(ws: WebSocket):
    """WebSocket endpoint for streaming Cypher query results."""
    # Basic auth check for WS: require query:read at least
    try:
        principal = principal_from_headers(ws.headers)
        if not has_permission(principal, "query:read"):
            await ws.close(code=1008)
            return
    except HTTPException as exc:
        await ws.close(code=1008)
        return
    await ws.accept()
    backend_ws_url = f"{ws_backend_url()}/v1/query/ws"
    try:
        # Receive first message from client (query payload)
        first = await ws.receive_json()

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(backend_ws_url) as upstream:
                # Send the first message upstream
                await upstream.send_json(first)

                async def c2b():
                    """Client to backend relay."""
                    try:
                        while True:
                            msg = await ws.receive_text()
                            await upstream.send_str(msg)
                    except WebSocketDisconnect:
                        await upstream.close()
                    except Exception:
                        await upstream.close()

                async def b2c():
                    """Backend to client relay."""
                    try:
                        async for msg in upstream:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await ws.send_text(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await ws.send_bytes(msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
                    except Exception:
                        try:
                            await ws.close()
                        except Exception:
                            pass

                t1 = asyncio.create_task(c2b())
                t2 = asyncio.create_task(b2c())
                done, pending = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await ws.send_json({"type": "error", "error": str(exc)})
        finally:
            await ws.close(code=1011)
