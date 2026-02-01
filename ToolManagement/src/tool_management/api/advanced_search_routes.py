"""
Advanced Search Routes - Sophisticated tool discovery API.

Exposes multi-faceted search, composition graphs, and learned affinities.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from ..embeddings.advanced_tool_index import get_advanced_index
from ..queue import get_task_queue
from ..persistence import get_tool_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/discover", tags=["discovery"])


class SearchRequest(BaseModel):
    """Advanced search request."""
    query: str = Field(..., description="Natural language search query")
    limit: int = Field(default=10, ge=1, le=50)
    backend: Optional[str] = Field(default=None, description="Filter by backend")
    intent_weight: float = Field(default=0.3, ge=0, le=1, description="Weight for intent matching")
    usage_weight: float = Field(default=0.2, ge=0, le=1, description="Weight for usage patterns")


class CompositionRequest(BaseModel):
    """Tool composition request."""
    tool_id: str = Field(..., description="Tool to find compositions for")
    direction: str = Field(default="forward", pattern="^(forward|backward)$")
    limit: int = Field(default=5, ge=1, le=20)


class ExecutionRecord(BaseModel):
    """Record of tool execution for learning."""
    session_id: str
    tool_sequence: List[str]
    success: bool
    total_latency_ms: float


@router.post("/search")
async def advanced_search(request: SearchRequest):
    """
    Multi-faceted semantic search with intent classification.

    Returns tools ranked by:
    - Semantic similarity across purpose, input, output, and example facets
    - Intent match (action, entities, constraints)
    - Usage patterns (call count, success rate)
    """
    index = get_advanced_index()

    results = await index.search(
        query=request.query,
        limit=request.limit,
        backend_filter=request.backend,
        intent_weight=request.intent_weight,
        usage_weight=request.usage_weight
    )

    return {
        "query": request.query,
        "results": results,
        "count": len(results)
    }


@router.get("/search")
async def advanced_search_get(
    query: str = Query(..., description="Natural language search query"),
    limit: int = Query(default=10, ge=1, le=50),
    backend: Optional[str] = Query(default=None),
    intent_weight: float = Query(default=0.3, ge=0, le=1),
    usage_weight: float = Query(default=0.2, ge=0, le=1)
):
    """GET version of advanced search."""
    index = get_advanced_index()

    results = await index.search(
        query=query,
        limit=limit,
        backend_filter=backend,
        intent_weight=intent_weight,
        usage_weight=usage_weight
    )

    return {
        "query": query,
        "results": results,
        "count": len(results)
    }


@router.post("/compose")
async def find_compositions(request: CompositionRequest):
    """
    Find tools that compose well with a given tool.

    Direction:
    - forward: Tools that can follow (use output of given tool)
    - backward: Tools that can precede (provide input to given tool)
    """
    index = get_advanced_index()

    results = await index.find_composable_tools(
        tool_id=request.tool_id,
        direction=request.direction,
        limit=request.limit
    )

    return {
        "tool_id": request.tool_id,
        "direction": request.direction,
        "composable_tools": results,
        "count": len(results)
    }


@router.get("/compose/{tool_id}")
async def find_compositions_get(
    tool_id: str,
    direction: str = Query(default="forward", pattern="^(forward|backward)$"),
    limit: int = Query(default=5, ge=1, le=20)
):
    """GET version of composition search."""
    index = get_advanced_index()

    results = await index.find_composable_tools(
        tool_id=tool_id,
        direction=direction,
        limit=limit
    )

    return {
        "tool_id": tool_id,
        "direction": direction,
        "composable_tools": results,
        "count": len(results)
    }


@router.post("/record-execution")
async def record_execution(record: ExecutionRecord):
    """
    Record a tool execution sequence for learning.

    This updates:
    - Tool usage statistics (call count, success rate)
    - Composition graph edges (which tools chain together)
    """
    index = get_advanced_index()

    await index.record_execution(
        session_id=record.session_id,
        tool_sequence=record.tool_sequence,
        success=record.success,
        total_latency_ms=record.total_latency_ms
    )

    return {"status": "recorded", "tools": record.tool_sequence}


@router.post("/reindex")
async def reindex_all():
    """
    Reindex all tools with multi-faceted embeddings.

    Fetches tools from all backends and generates:
    - Purpose embeddings
    - Input schema embeddings
    - Output schema embeddings
    - Example use case embeddings
    - Composition graph based on type compatibility
    """
    index = get_advanced_index()
    store = get_tool_store()

    # Get all backends
    backends = store.list_backends()

    total_indexed = 0
    backend_results = {}

    for backend in backends:
        backend_id = backend.get("backend_id") or backend.get("id")
        if not backend_id:
            continue

        # Get tools for this backend
        tools = store.list_tools_by_backend(backend_id)

        # Convert to dict format
        tool_dicts = []
        for tool in tools:
            tool_dicts.append({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
                "outputSchema": tool.output_schema,
                "tags": tool.tags,
                "category": tool.category
            })

        if tool_dicts:
            await index.index_tools_batch(tool_dicts, backend_id)
            total_indexed += len(tool_dicts)
            backend_results[backend_id] = len(tool_dicts)

    stats = await index.get_stats()

    return {
        "status": "reindexed",
        "total_indexed": total_indexed,
        "backends": backend_results,
        "stats": stats
    }


@router.get("/stats")
async def get_stats():
    """Get comprehensive statistics about the advanced index."""
    index = get_advanced_index()
    return await index.get_stats()


@router.get("/health")
async def health():
    """Health check for the advanced search system."""
    index = get_advanced_index()
    stats = await index.get_stats()

    return {
        "status": "healthy",
        "total_tools": stats["total_tools"],
        "total_compositions": stats["total_compositions"],
        "total_executions": stats["total_executions"],
        "backends": len(stats["backends"])
    }
