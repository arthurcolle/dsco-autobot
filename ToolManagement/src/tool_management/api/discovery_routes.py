"""Public discovery surfaces so external agents and registries can find us.

Serves the machine-readable manifests that other labs' agent runtimes probe:

- /.well-known/agent-card.json   A2A agent card (RFC 8615 well-known URI)
- /.well-known/ai-plugin.json    Legacy GPT-Actions manifest (wide-net)
- /.well-known/mcp/server.json   MCP registry server.json (self-hosted copy)
- /llms.txt                      LLM-crawler orientation file

Positioning: we are not "another MCP server" — we are the meta-router.
One connection buys semantic tool_search + execute over the full catalog
(2,500+ tools, 40+ backends) with lazy discovery — never load 2,500 schemas.
"""

import os
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse

router = APIRouter(tags=["discovery"])

SERVER_NAME = os.getenv("MCP_SERVER_NAME", "systems.distributed/tools")
PUBLIC_BASE = os.getenv("PUBLIC_BASE_URL", "https://tools.distributed.systems")
VERSION = os.getenv("REGISTRY_VERSION", "1.0.0")

DESCRIPTION = (
    "Distributed Systems tool gateway — a meta-router MCP server. Connect once "
    "and semantically discover + execute 2,500+ tools across 40+ backends (web search, embeddings, "
    "deep research, email, finance, maps, monitoring, CRM, and more) through "
    "tool_search / execute meta-tools with lazy schema loading. Backed by "
    "Jina and Parallel search, per-tool OAuth, and a free discovery tier."
)


def _base(request: Request) -> str:
    """Prefer the configured public base; fall back to the request origin."""
    if PUBLIC_BASE:
        return PUBLIC_BASE.rstrip("/")
    return str(request.base_url).rstrip("/")


@router.get("/.well-known/agent-card.json")
async def agent_card(request: Request):
    """A2A Agent Card (https://a2a-protocol.org, /.well-known/agent-card.json)."""
    base = _base(request)
    return JSONResponse(
        {
            "protocolVersion": "0.3.0",
            "name": "Distributed Systems Tool Gateway",
            "description": DESCRIPTION,
            "url": f"{base}/mcp",
            "preferredTransport": "JSONRPC",
            "provider": {
                "organization": "Distributed Systems & Co.",
                "url": base,
            },
            "version": VERSION,
            "documentationUrl": f"{base}/docs",
            "capabilities": {
                "streaming": True,
                "pushNotifications": False,
                "stateTransitionHistory": False,
            },
            "securitySchemes": {
                "oauth2": {
                    "type": "oauth2",
                    "description": "OAuth 2.0 — see /.well-known/oauth-protected-resource",
                },
                "bearer": {"type": "http", "scheme": "bearer"},
            },
            "security": [{"oauth2": []}, {"bearer": []}],
            "defaultInputModes": ["application/json", "text/plain"],
            "defaultOutputModes": ["application/json", "text/plain"],
            "skills": [
                {
                    "id": "tool_search",
                    "name": "Semantic tool discovery",
                    "description": (
                        "Natural-language search over the full tool catalog; "
                        "returns ranked tools with schemas on demand (lazy "
                        "loading — never ship 2,500 schemas into context)."
                    ),
                    "tags": ["meta", "discovery", "routing"],
                    "examples": ["find a tool that searches the web"],
                },
                {
                    "id": "execute_tool",
                    "name": "Execute any catalog tool",
                    "description": (
                        "Execute a discovered tool by name with JSON arguments; "
                        "sync, streaming, and job execution modes."
                    ),
                    "tags": ["meta", "execution"],
                    "examples": ["execute search with {\"query\": \"...\"}"],
                },
                {
                    "id": "deep_research",
                    "name": "Deep research",
                    "description": (
                        "Multi-source research synthesis (deep_search) and "
                        "web-scale entity prospecting (find_all)."
                    ),
                    "tags": ["research", "search"],
                    "examples": ["research the current state of MCP registries"],
                },
                {
                    "id": "embeddings",
                    "name": "Embeddings & reranking",
                    "description": (
                        "Jina v3/v4 embeddings (task adapters, Matryoshka dims), "
                        "reranking, classification, and segmentation."
                    ),
                    "tags": ["embeddings", "retrieval"],
                    "examples": ["embed this passage for retrieval"],
                },
            ],
        }
    )


@router.get("/.well-known/ai-plugin.json")
async def ai_plugin(request: Request):
    """Legacy GPT-Actions manifest — still probed by several agent stacks."""
    base = _base(request)
    return JSONResponse(
        {
            "schema_version": "v1",
            "name_for_human": "Distributed Systems Tools",
            "name_for_model": "distributed_tools",
            "description_for_human": (
                "Meta-router for 2,500+ tools: web search, deep research, "
                "embeddings, email, finance, maps, and more."
            ),
            "description_for_model": DESCRIPTION,
            "auth": {"type": "oauth"},
            "api": {"type": "openapi", "url": f"{base}/openapi.json"},
            "logo_url": f"{base}/favicon.ico",
            "contact_email": "agent@distributed.systems",
            "legal_info_url": base,
        }
    )


@router.get("/.well-known/mcp/server.json")
async def mcp_server_json(request: Request):
    """Self-hosted copy of our MCP-registry server.json manifest."""
    base = _base(request)
    return JSONResponse(
        {
            "$schema": "https://static.modelcontextprotocol.io/schemas/2025-09-29/server.schema.json",
            "name": SERVER_NAME,
            "description": (
                "Meta-router MCP gateway: semantic tool_search + execute over "
                "2,500+ tools (40+ backends) with lazy schema loading."
            ),
            "repository": {
                "url": "https://github.com/arthurcolle/dsco-autobot",
                "source": "github",
            },
            "version": VERSION,
            "remotes": [
                {"type": "streamable-http", "url": f"{base}/mcp"},
                {"type": "sse", "url": f"{base}/mcp/sse"},
            ],
        }
    )


@router.get("/api/v1/service-health")
@router.get("/services")
async def services_index(refresh: bool = False):
    """Live, fast index of every distributed-* backend and its health.

    The router consults this to fast-fail dead hops; agents can read it to
    "always call out to services as needed" knowing which are live.
    """
    from ..services.service_index import get_services, refresh_once

    if refresh:
        return await refresh_once()
    services = get_services()
    return {
        "total": len(services),
        "healthy": sum(1 for s in services if s.get("healthy")),
        "services": services,
    }


@router.post("/api/v1/tool-retrieve")
async def tool_retrieve(request: Request):
    """Hardened retrieval: cosine recall -> Jina cross-encoder rerank -> top_k.
    Body: {"query": str, "top_k": int?, "backend": str?}"""
    import asyncio
    from ..services import pg_retrieval

    body = await request.json()
    return await asyncio.to_thread(
        pg_retrieval.retrieve_rerank, body.get("query", ""), int(body.get("top_k", 8))
    )


@router.get("/api/v1/catalog/diagnose")
@router.post("/api/v1/catalog/cleanup")
async def catalog_clean(request: Request):
    """GET diagnoses the catalog (read-only); POST ?apply=true applies the
    conservative cleanup (drop junk, backfill empty descriptions)."""
    import asyncio

    from ..persistence import get_tool_store
    from ..services.catalog_cleanup import diagnose, cleanup

    store = get_tool_store()
    # DuckDB access is blocking and serialized behind a threading lock; run it
    # off the event loop so a contended store can't freeze the whole container.
    if request.method == "GET":
        return await asyncio.to_thread(diagnose, store)
    apply = request.query_params.get("apply", "false").lower() == "true"
    return await asyncio.to_thread(cleanup, store, dry_run=not apply)


@router.post("/api/v1/tool-hybrid")
async def tool_hybrid(request: Request):
    """Hybrid retrieval: fuse semantic (top-N) + FTS/BM25 (top-M) via RRF, boost
    by learned preferences, optionally rerank. Returns fused + both source
    rankings for cross-comparison.
    Body: {"query": str, "semantic_k": 17?, "fts_k": 11?, "top_k": 10?, "rerank": bool?}"""
    import asyncio
    from ..services import pg_retrieval

    body = await request.json()
    return await asyncio.to_thread(
        pg_retrieval.hybrid_search, body.get("query", ""),
        int(body.get("semantic_k", 17)), int(body.get("fts_k", 11)),
        int(body.get("top_k", 10)), bool(body.get("rerank", False)),
    )


@router.post("/api/v1/tool-feedback")
async def tool_feedback(request: Request):
    """Record an execution outcome so retrieval learns preferences.
    Body: {"tools": [tool_id], "success": bool, "latency_ms": float?}"""
    from ..embeddings.advanced_tool_index import get_advanced_index

    body = await request.json()
    index = get_advanced_index()
    await index.record_execution(
        session_id=body.get("session_id", "feedback"),
        tool_sequence=body.get("tools", []),
        success=bool(body.get("success", True)),
        total_latency_ms=float(body.get("latency_ms", 0.0)),
    )
    return {"recorded": body.get("tools", []), "success": bool(body.get("success", True))}


@router.post("/api/v1/tool-multihop")
async def tool_multihop(request: Request):
    """Multi-hop retrieval: assemble a cross-backend tool chain for a goal.
    Body: {"goal": str, "steps": [str]?, "per_hop": int?}"""
    import asyncio
    from ..services import pg_retrieval

    body = await request.json()
    return await asyncio.to_thread(
        pg_retrieval.multi_hop_retrieve, body.get("goal", ""),
        body.get("steps"), int(body.get("per_hop", 3)),
    )


@router.get("/api/v1/tool-index/status")
@router.post("/api/v1/tool-index/sync")
async def tool_index_status(request: Request):
    """Faceted-index sync status; POST triggers an immediate incremental sync."""
    from ..services.tool_index_sync import faceted_sync_once, sync_status

    if request.method == "POST":
        return await faceted_sync_once()
    return sync_status()


@router.get("/llms.txt")
async def llms_txt(request: Request):
    """llms.txt — orientation file for LLM crawlers and agent runtimes."""
    base = _base(request)
    body = f"""# Distributed Systems Tool Gateway

> {DESCRIPTION}

## Connect (MCP)

- Streamable HTTP: {base}/mcp
- SSE: {base}/mcp/sse
- Auth: OAuth 2.0 ({base}/.well-known/oauth-protected-resource) or bearer token
- Free tier: anonymous-signup users get free daily calls to discovery tools
  (tool_search, get_tool, search, embed, read) before any subscription.

## Discovery surfaces

- A2A agent card: {base}/.well-known/agent-card.json
- MCP server.json: {base}/.well-known/mcp/server.json
- OpenAPI: {base}/openapi.json
- Tool catalog (HTTP): {base}/api/v1/tools

## Why connect

One connection = semantic routing over 2,500+ tools (40+ backends) with lazy schema loading.
Search the catalog in natural language via tool_search, fetch a schema with
get_tool, execute with execute_tool. Never load 2,500 schemas into context.
"""
    return PlainTextResponse(body)
