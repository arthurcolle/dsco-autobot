"""
API Routes for Tool Retrieval and Context Management.

Provides endpoints for:
- Semantic tool search
- Context-aware tool loading
- Tool indexing from backends
- Context budget management
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
import httpx
import os

from ..context.retrieval import (
    ToolRetriever,
    RetrievalStrategy,
    RetrievalResult,
    GraphSubConfig,
    get_tool_retriever,
)
from ..context.tool_context import (
    ToolContextManager,
    ContextBudget,
    get_tool_context_manager,
)

router = APIRouter(prefix="/tools", tags=["retrieval"])


# =============================================================================
# Request/Response Models
# =============================================================================

class RetrieveRequest(BaseModel):
    """Request for tool retrieval."""
    query: str = Field(..., description="Natural language query for tool capabilities")
    strategy: str = Field(default="hybrid", description="Retrieval strategy: exact, semantic, graph, hybrid, cluster")
    limit: int = Field(default=10, ge=1, le=50)
    backend_filter: Optional[str] = Field(None, description="Only return tools from this backend")
    category_filter: Optional[str] = Field(None, description="Only return tools in this category")
    min_score: float = Field(default=0.0, ge=0.0, le=1.0)


class ToolMatch(BaseModel):
    """A matched tool from retrieval."""
    tool_name: str
    backend_id: str
    score: float
    description: str
    reason: str
    input_schema: dict
    related_tools: List[str] = []


class RetrieveResponse(BaseModel):
    """Response from tool retrieval."""
    query: str
    strategy: str
    results: List[ToolMatch]
    total: int


class LoadToolsRequest(BaseModel):
    """Request to load specific tools into context."""
    tools: List[str] = Field(..., description="Tool keys (backend:name) or just names")
    token_budget: int = Field(default=4000, ge=100, le=50000)
    include_related: bool = Field(default=True)


class ContextResponse(BaseModel):
    """Response with loaded tool context."""
    tools: List[dict]
    tokens_used: int
    token_budget: int


class IndexBackendRequest(BaseModel):
    """Request to index tools from a backend."""
    backend_id: str
    openapi_url: Optional[str] = Field(None, description="URL to OpenAPI spec")
    mcp_endpoint: Optional[str] = Field(None, description="MCP endpoint for tools/list")
    category: Optional[str] = None


# =============================================================================
# Retrieval Endpoints
# =============================================================================

@router.post("/retrieve", response_model=RetrieveResponse)
async def retrieve_tools(request: RetrieveRequest):
    """
    Retrieve relevant tools for a natural language query.

    Uses semantic search + graph traversal to find tools
    without loading all schemas into context.
    """
    retriever = get_tool_retriever(
        jina_api_key=os.getenv("JINA_API_KEY"),
    )

    try:
        strategy = RetrievalStrategy(request.strategy)
    except ValueError:
        strategy = RetrievalStrategy.HYBRID

    results = await retriever.retrieve(
        query=request.query,
        strategy=strategy,
        limit=request.limit,
        backend_filter=request.backend_filter,
        category_filter=request.category_filter,
        min_score=request.min_score,
    )

    return RetrieveResponse(
        query=request.query,
        strategy=request.strategy,
        results=[
            ToolMatch(
                tool_name=r.tool_name,
                backend_id=r.backend_id,
                score=r.score,
                description=r.schema.get("description", ""),
                reason=r.reason,
                input_schema=r.schema.get("input_schema", {}),
                related_tools=r.related_tools,
            )
            for r in results
        ],
        total=len(results),
    )


@router.get("/search")
async def search_tools(
    q: str = Query(..., description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    backend: Optional[str] = Query(None),
):
    """Quick semantic search for tools."""
    retriever = get_tool_retriever(jina_api_key=os.getenv("JINA_API_KEY"))
    ctx_manager = get_tool_context_manager()

    # Try GraphSub first
    results = await retriever.retrieve(
        query=q,
        strategy=RetrievalStrategy.SEMANTIC,
        limit=limit,
        backend_filter=backend,
    )

    # Fallback to local context manager
    if not results:
        local_results = ctx_manager.search_tools(
            query=q,
            backend_id=backend,
            limit=limit,
        )
        return {
            "query": q,
            "source": "local",
            "results": [
                {
                    "name": s.name,
                    "backend": s.backend_id,
                    "score": 1.0,
                    "description": s.description[:200] if s.description else "",
                }
                for s in local_results
            ],
        }

    return {
        "query": q,
        "source": "graphsub",
        "results": [
            {
                "name": r.tool_name,
                "backend": r.backend_id,
                "score": r.score,
                "description": r.schema.get("description", "")[:200],
            }
            for r in results
        ],
    }


# =============================================================================
# Context Management Endpoints
# =============================================================================

@router.post("/context/load", response_model=ContextResponse)
async def load_tools_context(request: LoadToolsRequest):
    """
    Load specific tools into an optimized context window.

    Returns tool schemas that fit within the token budget,
    optionally including related tools.
    """
    retriever = get_tool_retriever(jina_api_key=os.getenv("JINA_API_KEY"))

    context = await retriever.get_tool_context(
        tools=request.tools,
        token_budget=request.token_budget,
        include_related=request.include_related,
    )

    return ContextResponse(**context)


@router.get("/context/stats")
async def get_context_stats():
    """Get tool context manager statistics."""
    ctx_manager = get_tool_context_manager()
    return ctx_manager.get_stats()


@router.post("/context/clear")
async def clear_context(backend_id: Optional[str] = Query(None)):
    """Clear loaded tools from context."""
    ctx_manager = get_tool_context_manager()

    if backend_id:
        ctx_manager.clear_cache(backend_id)
        return {"cleared": backend_id, "message": f"Cleared tools from {backend_id}"}
    else:
        ctx_manager.clear_loaded()
        return {"cleared": "all", "message": "Cleared all loaded tools"}


@router.get("/context/loaded")
async def get_loaded_tools():
    """Get currently loaded tools in MCP format."""
    ctx_manager = get_tool_context_manager()
    tools = ctx_manager.get_loaded_as_mcp()

    return {
        "tools": tools,
        "count": len(tools),
        "budget": {
            "used": ctx_manager.budget.loaded_tokens,
            "remaining": ctx_manager.budget.remaining(),
            "utilization": f"{ctx_manager.budget.utilization():.1%}",
        },
    }


# =============================================================================
# Indexing Endpoints
# =============================================================================

@router.post("/index/backend")
async def index_backend_tools(
    request: IndexBackendRequest,
    background_tasks: BackgroundTasks,
):
    """
    Index tools from a backend into GraphSub for retrieval.

    Supports:
    - OpenAPI spec URL
    - MCP tools/list endpoint
    """
    retriever = get_tool_retriever(jina_api_key=os.getenv("JINA_API_KEY"))
    await retriever.initialize()

    indexed = 0

    if request.openapi_url:
        # Fetch and index from OpenAPI
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                resp = await client.get(request.openapi_url)
                if resp.status_code == 200:
                    spec = resp.json()
                    indexed = await retriever.index_from_openapi(
                        backend_id=request.backend_id,
                        openapi_spec=spec,
                        category=request.category,
                    )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Failed to fetch OpenAPI: {e}")

    if request.mcp_endpoint:
        # Fetch tools via MCP
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                resp = await client.post(
                    request.mcp_endpoint,
                    json={"jsonrpc": "2.0", "method": "tools/list", "id": 1},
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    tools = data.get("result", {}).get("tools", [])

                    for tool in tools:
                        result = await retriever.index_tool(
                            backend_id=request.backend_id,
                            name=tool.get("name", ""),
                            description=tool.get("description", ""),
                            input_schema=tool.get("inputSchema", {}),
                            category=request.category,
                        )
                        if result:
                            indexed += 1
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Failed to fetch MCP tools: {e}")

    return {
        "backend_id": request.backend_id,
        "indexed": indexed,
        "message": f"Indexed {indexed} tools from {request.backend_id}",
    }


@router.post("/index/parallel-ai")
async def index_parallel_ai():
    """Index tools from Parallel AI backend."""
    parallel_url = os.getenv("PARALLEL_AI_URL", "http://localhost:2040")

    retriever = get_tool_retriever(jina_api_key=os.getenv("JINA_API_KEY"))
    ctx_manager = get_tool_context_manager()
    await retriever.initialize()

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(f"{parallel_url}/openapi.json")
            if resp.status_code == 200:
                spec = resp.json()

                # Index in GraphSub
                indexed = await retriever.index_from_openapi(
                    backend_id="parallel-ai",
                    openapi_spec=spec,
                    category="parallel-ai",
                )

                # Also index in local context manager as fallback
                paths = spec.get("paths", {})
                for path, methods in paths.items():
                    for method, op_spec in methods.items():
                        if method not in ("get", "post", "put", "delete", "patch"):
                            continue
                        name = op_spec.get("operationId", f"{method}_{path}")
                        desc = op_spec.get("summary", "") or op_spec.get("description", "")
                        tags = op_spec.get("tags", [])
                        ctx_manager.register_tool(
                            backend_id="parallel-ai",
                            name=name,
                            description=desc[:500] if desc else "",
                            input_schema={"type": "object"},
                            group=tags[0] if tags else "parallel-ai",
                            tags=tags,
                        )
                        indexed += 1

                return {"backend": "parallel-ai", "indexed": indexed}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to index: {e}")


@router.post("/index/jina-mcp")
async def index_jina_mcp():
    """Index tools from Jina MCP backend."""
    jina_url = os.getenv("JINA_MCP_URL", "http://localhost:8787")

    retriever = get_tool_retriever(jina_api_key=os.getenv("JINA_API_KEY"))
    ctx_manager = get_tool_context_manager()
    await retriever.initialize()

    # Jina exposes tool info in root endpoint
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(f"{jina_url}/")
            if resp.status_code == 200:
                # Core tools from Jina MCP
                core_tools = [
                    {"name": "read_url", "description": "Read and extract content from a URL", "category": "web"},
                    {"name": "search_web", "description": "Search the web for information", "category": "search"},
                    {"name": "search_arxiv", "description": "Search arXiv for academic papers", "category": "academic"},
                    {"name": "summarize_text", "description": "Summarize text content", "category": "rag"},
                    {"name": "answer_with_evidence", "description": "Answer questions with cited evidence", "category": "rag"},
                    {"name": "sort_by_relevance", "description": "Sort items by relevance to query", "category": "rag"},
                    {"name": "deduplicate_strings", "description": "Remove duplicate strings", "category": "util"},
                    {"name": "embed_text", "description": "Generate text embeddings", "category": "embeddings"},
                    {"name": "memory_store", "description": "Store information in memory", "category": "memory"},
                    {"name": "memory_recall", "description": "Recall information from memory", "category": "memory"},
                ]

                indexed = 0
                for tool in core_tools:
                    # Index in GraphSub
                    await retriever.index_tool(
                        backend_id="jina-mcp",
                        name=tool["name"],
                        description=tool["description"],
                        input_schema={},
                        category=tool["category"],
                        capabilities=[tool["category"]],
                    )

                    # Also index in local context manager
                    ctx_manager.register_tool(
                        backend_id="jina-mcp",
                        name=tool["name"],
                        description=tool["description"],
                        input_schema={"type": "object"},
                        group=tool["category"],
                        tags=[tool["category"]],
                    )
                    indexed += 1

                return {"backend": "jina-mcp", "indexed": indexed}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to index: {e}")
