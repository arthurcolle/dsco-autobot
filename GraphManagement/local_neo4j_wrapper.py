#!/usr/bin/env python3
"""
Local Neo4j Wrapper Service (Layer 1)

A lightweight FastAPI service that wraps a local Neo4j Desktop instance.
Provides HTTP endpoints compatible with the Graph Management Layer (Layer 2).

Usage:
    python local_neo4j_wrapper.py

Or with uvicorn:
    uvicorn local_neo4j_wrapper:app --host 0.0.0.0 --port 8001 --reload
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, time as time_, timedelta
from decimal import Decimal
from pathlib import Path
from typing import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.graph import Node as Neo4jNode, Path as Neo4jPath, Relationship as Neo4jRelationship
from neo4j.time import Date as Neo4jDate, DateTime as Neo4jDateTime, Duration as Neo4jDuration, Time as Neo4jTime

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from graph_management.schemas import (
    ConstraintInfo,
    ConstraintListResponse,
    IndexInfo,
    IndexListResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
    SingleResultResponse,
    TransactionRequest,
)

# -------------------------
# Configuration
# -------------------------

NEO4J_URI = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "GraphInstance2025!")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# -------------------------
# Driver Management
# -------------------------

_driver: AsyncDriver | None = None


def get_driver() -> AsyncDriver:
    """Create or return the Neo4j async driver."""
    global _driver
    if _driver is None:
        _driver = AsyncGraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            database=NEO4J_DATABASE,
        )
    return _driver


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Manage driver lifecycle."""
    global _driver
    driver = get_driver()
    _driver = driver
    print(f"✓ Connected to Neo4j at {NEO4J_URI}")
    try:
        yield
    finally:
        await driver.close()
        _driver = None
        print("✓ Closed Neo4j connection")


# -------------------------
# FastAPI App
# -------------------------

app = FastAPI(
    title="Neo4j Wrapper Service (Local)",
    description="HTTP wrapper for local Neo4j Desktop instance",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def driver_dep() -> AsyncDriver:
    """FastAPI dependency for Neo4j driver."""
    if _driver is not None:
        return _driver
    return get_driver()


# -------------------------
# Serialization Utilities
# -------------------------

try:
    from neo4j.spatial import Point as Neo4jPoint
except Exception:
    Neo4jPoint = None


def _serialize_value(val):
    """Serialize Neo4j values to JSON-compatible types."""
    if isinstance(val, (datetime, Neo4jDateTime)):
        return val.isoformat()
    if isinstance(val, (date, Neo4jDate)):
        return val.isoformat()
    if isinstance(val, (time_, Neo4jTime)):
        return val.isoformat()
    if isinstance(val, timedelta):
        return {
            "seconds": val.total_seconds(),
            "iso": f"PT{val.total_seconds()}S",
        }
    if isinstance(val, Neo4jDuration):
        return {
            "months": val.months,
            "days": val.days,
            "seconds": val.seconds,
            "nanoseconds": val.nanoseconds,
            "iso": val.iso_format(),
        }
    if Neo4jPoint is not None and isinstance(val, Neo4jPoint):
        return {
            "srid": getattr(val, "srid", None),
            "coordinates": [float(coord) for coord in val],
        }
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, Neo4jNode):
        return {
            "id": getattr(val, "id", None),
            "element_id": getattr(val, "element_id", None),
            "labels": list(getattr(val, "labels", []) or []),
            "properties": {k: _serialize_value(v) for k, v in dict(val).items()},
        }
    if isinstance(val, Neo4jRelationship):
        start_node = getattr(val, "start_node", None)
        end_node = getattr(val, "end_node", None)
        return {
            "id": getattr(val, "id", None),
            "element_id": getattr(val, "element_id", None),
            "type": getattr(val, "type", None),
            "start_node_id": getattr(start_node, "id", None) if start_node is not None else None,
            "start_element_id": getattr(start_node, "element_id", None) if start_node is not None else None,
            "end_node_id": getattr(end_node, "id", None) if end_node is not None else None,
            "end_element_id": getattr(end_node, "element_id", None) if end_node is not None else None,
            "properties": {k: _serialize_value(v) for k, v in dict(val).items()},
        }
    if isinstance(val, Neo4jPath):
        return {
            "nodes": [_serialize_value(n) for n in getattr(val, "nodes", [])],
            "relationships": [_serialize_value(r) for r in getattr(val, "relationships", [])],
        }
    if isinstance(val, tuple):
        return [_serialize_value(v) for v in val]
    if isinstance(val, list):
        return [_serialize_value(v) for v in val]
    if isinstance(val, dict):
        return {k: _serialize_value(v) for k, v in val.items()}
    return val


def _serialize_record(record) -> dict:
    """Serialize a Neo4j record to a dict."""
    try:
        data = record.data()
    except AttributeError:
        data = dict(record)
    return {k: _serialize_value(v) for k, v in data.items()}


# -------------------------
# Endpoints
# -------------------------


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/probe")
async def probe(driver: AsyncDriver = Depends(driver_dep)) -> dict[str, int]:
    """Probe endpoint - verifies Neo4j connectivity."""
    try:
        async with driver.session() as session:
            result = await session.run("RETURN 1 AS ok")
            record = await result.single()
            return {"ok": int(record["ok"])}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# -------------------------
# v1 Typed Endpoints
# -------------------------


@app.post("/v1/query", response_model=SingleResultResponse | QueryResponse)
async def v1_query(req: QueryRequest, driver: AsyncDriver = Depends(driver_dep)):
    """Execute a Cypher query with typed response."""
    try:
        async with driver.session() as session:
            if req.strategy == "one":
                res = await session.run(req.query, req.parameters)
                record = await res.single()
                if record is None:
                    return SingleResultResponse(result=None)
                return SingleResultResponse(result=_serialize_record(record))
            elif req.strategy == "all":
                res = await session.run(req.query, req.parameters)
                records = [_serialize_record(r) async for r in res]
                return QueryResponse(results=records)
            else:
                # default: run query but don't fetch results
                await session.run(req.query, req.parameters)
                return QueryResponse(results=[])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/v1/tx", response_model=QueryResponse)
async def v1_tx(req: TransactionRequest, driver: AsyncDriver = Depends(driver_dep)):
    """Execute a transaction with multiple statements."""
    if not req.statements:
        raise HTTPException(status_code=400, detail="statements must not be empty")
    try:
        async with driver.session() as session:
            tx = await session.begin_transaction()
            try:
                results = []
                for stmt in req.statements:
                    res = await tx.run(stmt.query, stmt.parameters)
                    results.append([_serialize_record(r) async for r in res])
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
            return QueryResponse(results=results)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/v1/schema", response_model=SchemaResponse)
async def v1_schema(driver: AsyncDriver = Depends(driver_dep)):
    """Get the Neo4j schema (labels, relationship types, property keys)."""
    try:
        async with driver.session() as session:
            labels_res = await session.run("CALL db.labels()")
            rels_res = await session.run("CALL db.relationshipTypes()")
            props_res = await session.run("CALL db.propertyKeys()")

            labels = []
            async for rec in labels_res:
                data = rec.data()
                labels.append(next(iter(data.values())))

            rels = []
            async for rec in rels_res:
                data = rec.data()
                rels.append(next(iter(data.values())))

            props = []
            async for rec in props_res:
                data = rec.data()
                props.append(next(iter(data.values())))

            return SchemaResponse(labels=labels, relationshipTypes=rels, propertyKeys=props)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/v1/indexes", response_model=IndexListResponse)
async def v1_indexes(driver: AsyncDriver = Depends(driver_dep)):
    """List all Neo4j indexes."""
    try:
        async with driver.session() as session:
            res = await session.run(
                "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties, options "
                "RETURN name, type, entityType, labelsOrTypes, properties, options"
            )
            indexes = []
            async for rec in res:
                data = rec.data()
                indexes.append(IndexInfo(**data))
            return IndexListResponse(indexes=indexes)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/v1/constraints", response_model=ConstraintListResponse)
async def v1_constraints(driver: AsyncDriver = Depends(driver_dep)):
    """List all Neo4j constraints."""
    try:
        async with driver.session() as session:
            res = await session.run(
                "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, options "
                "RETURN name, type, entityType, labelsOrTypes, properties, options"
            )
            constraints = []
            async for rec in res:
                data = rec.data()
                constraints.append(ConstraintInfo(**data))
            return ConstraintListResponse(constraints=constraints)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# -------------------------
# Streaming Endpoints
# -------------------------

from starlette.responses import StreamingResponse
import json as _json


@app.post("/v1/query/stream")
async def v1_query_stream(req: QueryRequest, driver: AsyncDriver = Depends(driver_dep)):
    """Stream query results as Server-Sent Events."""

    async def event_stream():
        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def heartbeat_task():
            try:
                while True:
                    await asyncio.sleep(10)
                    await queue.put(": ping\n\n")
            except asyncio.CancelledError:
                return

        async def producer():
            try:
                async with driver.session() as session:
                    res = await session.run(req.query, req.parameters)
                    async for record in res:
                        payload = _json.dumps(_serialize_record(record))
                        await queue.put(f"event: record\n")
                        await queue.put(f"data: {payload}\n\n")
                        if req.strategy == "one":
                            break
                    await queue.put("event: end\n")
                    await queue.put("data: {}\n\n")
            except Exception as exc:
                err = _json.dumps({"error": str(exc)})
                await queue.put("event: error\n")
                await queue.put(f"data: {err}\n\n")
            finally:
                await queue.put(None)

        hb = asyncio.create_task(heartbeat_task())
        prod = asyncio.create_task(producer())
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            for t in (hb, prod):
                t.cancel()

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)


@app.websocket("/v1/query/ws")
async def v1_query_ws(ws: WebSocket, driver: AsyncDriver = Depends(driver_dep)):
    """WebSocket endpoint for streaming query results."""
    await ws.accept()
    try:
        msg = await ws.receive_json()
        payload = msg.get("data") if isinstance(msg, dict) and "data" in msg else msg
        q = payload.get("query")
        params = payload.get("parameters") or {}
        strategy = payload.get("strategy", "all")
        if not q:
            await ws.send_json({"type": "error", "error": "query is required"})
            await ws.close(code=1003)
            return

        async def send_heartbeat():
            try:
                while True:
                    await asyncio.sleep(10)
                    await ws.send_json({"type": "heartbeat"})
            except asyncio.CancelledError:
                return

        hb_task = asyncio.create_task(send_heartbeat())
        try:
            async with driver.session() as session:
                res = await session.run(q, params)
                async for record in res:
                    await ws.send_json({"type": "record", "data": _serialize_record(record)})
                    if strategy == "one":
                        break
            await ws.send_json({"type": "end"})
        finally:
            hb_task.cancel()
    except WebSocketDisconnect:
        return
    except Exception as exc:
        try:
            await ws.send_json({"type": "error", "error": str(exc)})
        finally:
            await ws.close(code=1011)


# -------------------------
# Legacy Endpoints (for compatibility)
# -------------------------


@app.post("/query")
async def query(payload: dict, driver: AsyncDriver = Depends(driver_dep)) -> dict:
    """Legacy query endpoint."""
    query_str = payload.get("query")
    params = payload.get("parameters") or {}
    if not query_str:
        raise HTTPException(status_code=400, detail="query is required")
    try:
        async with driver.session() as session:
            result = await session.run(query_str, params)
            records = [_serialize_record(record) async for record in result]
            return {"results": records}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("Neo4j Wrapper Service (Layer 1)")
    print("=" * 60)
    print(f"Neo4j URI:      {NEO4J_URI}")
    print(f"Neo4j User:     {NEO4J_USER}")
    print(f"Neo4j Database: {NEO4J_DATABASE}")
    print("=" * 60)
    print("\nStarting server on http://0.0.0.0:8001")
    print("\nPress Ctrl+C to stop")
    print("=" * 60)

    uvicorn.run(
        "local_neo4j_wrapper:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info",
    )
