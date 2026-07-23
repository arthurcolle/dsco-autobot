"""Reusable FastAPI dependencies."""

import httpx

from fastapi import Request

from parallel_websearch_server.config import settings


async def get_http_client(request: Request) -> httpx.AsyncClient:
    """Return the shared Parallel API client, creating it lazily if needed."""
    if not hasattr(request.app.state, "http_client"):
        request.app.state.http_client = httpx.AsyncClient(
            base_url=settings.api_base_url,
            timeout=settings.request_timeout,
        )
    return request.app.state.http_client


async def get_kg_builder(request: Request):
    """Return the shared knowledge graph builder."""
    if not hasattr(request.app.state, "kg_builder"):
        from parallel_websearch_server.meta_pipeline.kg_builder import KnowledgeGraphBuilder
        request.app.state.kg_builder = KnowledgeGraphBuilder(
            use_nlp=True,
            min_entity_confidence=0.5,
        )
    return request.app.state.kg_builder


async def get_meta_orchestrator(request: Request):
    """Return the shared meta-pipeline orchestrator."""
    if not hasattr(request.app.state, "meta_orchestrator"):
        from parallel_websearch_server.meta_pipeline.orchestrator import MetaPipelineOrchestrator

        http_client = await get_http_client(request)
        kg_builder = await get_kg_builder(request)

        request.app.state.meta_orchestrator = MetaPipelineOrchestrator(
            http_client=http_client,
            kg_builder=kg_builder,
            enable_parallel=True,
            max_parallel=5,
        )
    return request.app.state.meta_orchestrator
