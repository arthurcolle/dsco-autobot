"""Top-level /api/v1/batch endpoint — fast in-memory registry execution."""

import asyncio
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ..registry import get_registry

batch_router = APIRouter(tags=["tools"])


class _CallItem(BaseModel):
    tool: str = Field(..., description="Tool name")
    inputs: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=30.0, description="Per-call timeout in seconds")


class _ResultItem(BaseModel):
    tool: str
    status: str
    output: Any = None
    error: Optional[str] = None
    latency_ms: float = 0.0


class _BatchRequest(BaseModel):
    calls: List[_CallItem]
    parallel: bool = Field(default=True)


class _BatchResponse(BaseModel):
    results: List[_ResultItem]
    total: int
    successful: int
    failed: int
    total_latency_ms: float


async def _run_call(item: _CallItem) -> _ResultItem:
    registry = get_registry()
    t0 = time.time()
    tool = registry.get_by_name(item.tool)
    if tool is None:
        # Not a local builtin — fall back to the tool_store / MCP backend execute path
        # so any registered backend tool (native MCPs, bridge, external) runs synchronously.
        return await _run_backend_call(item, t0)
    try:
        result = await asyncio.wait_for(tool.execute(**item.inputs), timeout=item.timeout)
        return _ResultItem(tool=item.tool, status="success", output=result, latency_ms=(time.time() - t0) * 1000)
    except asyncio.TimeoutError:
        return _ResultItem(tool=item.tool, status="error", error=f"Timeout after {item.timeout}s", latency_ms=(time.time() - t0) * 1000)
    except Exception as exc:
        return _ResultItem(tool=item.tool, status="error", error=str(exc), latency_ms=(time.time() - t0) * 1000)


async def _run_backend_call(item: _CallItem, t0: float) -> _ResultItem:
    """Resolve a tool against the tool_store and execute it over HTTP (MCP JSON-RPC)."""
    try:
        from .tools_api import execute_tool, ToolCall
    except Exception as exc:
        return _ResultItem(tool=item.tool, status="error", error=f"Tool not found: {item.tool} ({exc})", latency_ms=(time.time() - t0) * 1000)
    try:
        res = await asyncio.wait_for(
            execute_tool(ToolCall(tool=item.tool, arguments=item.inputs)),
            timeout=item.timeout,
        )
    except asyncio.TimeoutError:
        return _ResultItem(tool=item.tool, status="error", error=f"Timeout after {item.timeout}s", latency_ms=(time.time() - t0) * 1000)
    except Exception as exc:
        return _ResultItem(tool=item.tool, status="error", error=str(exc), latency_ms=(time.time() - t0) * 1000)

    success = getattr(res, "success", False)
    return _ResultItem(
        tool=item.tool,
        status="success" if success else "error",
        output=getattr(res, "result", None),
        error=getattr(res, "error", None),
        latency_ms=getattr(res, "latency_ms", (time.time() - t0) * 1000),
    )


@batch_router.post("/run", response_model=_ResultItem)
async def run_execute(item: _CallItem):
    """Execute a single tool synchronously against the in-memory registry."""
    return await _run_call(item)


@batch_router.post("/batch", response_model=_BatchResponse)
async def batch_execute(request: _BatchRequest):
    """Execute multiple tools in a single call (in-memory registry, fast)."""
    batch_start = time.time()
    _run = _run_call

    if request.parallel:
        results = list(await asyncio.gather(*[_run(c) for c in request.calls]))
    else:
        results = [await _run(c) for c in request.calls]

    successful = sum(1 for r in results if r.status == "success")
    return _BatchResponse(
        results=results,
        total=len(results),
        successful=successful,
        failed=len(results) - successful,
        total_latency_ms=(time.time() - batch_start) * 1000,
    )
