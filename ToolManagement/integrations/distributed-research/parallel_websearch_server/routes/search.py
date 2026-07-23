"""Search and Extract API routes."""

import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.models import (
    SearchRequest,
    SearchResponse,
    ExtractRequest,
    ExtractResponse,
    ErrorResponse,
)
from parallel_websearch_server.state_store import ContextStateStore, get_state_store
from parallel_websearch_server.local_store import get_local_store
from parallel_websearch_server.events import emit_search_event

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1beta", tags=["Search & Extract"])


def _preview_text(value: Optional[str], max_length: int = 160) -> str:
    """Compress whitespace and truncate long text for structured logging."""
    if not value:
        return ""
    collapsed = " ".join(value.split())
    if len(collapsed) <= max_length:
        return collapsed
    return f"{collapsed[: max_length - 3]}..."


def _shorten_body(body_text: str, limit: int = 400) -> str:
    """Keep only the first N characters of verbose upstream payloads."""
    if len(body_text) <= limit:
        return body_text
    return f"{body_text[: limit - 3]}..."


def _summarize_search_results(payload: Dict[str, Any]) -> str:
    """Build a compact description of search results."""
    results = payload.get("results") or []
    if not results:
        return "Search returned no results."
    titles = [result.get("title") for result in results[:3] if result.get("title")]
    summary = f"{len(results)} result(s)"
    if titles:
        summary += f"; top sources: {', '.join(titles)}"
    return summary

def _summarize_extract_results(payload: Dict[str, Any]) -> str:
    """Describe extract outcome for history view."""
    results = payload.get("results") or []
    errors = payload.get("errors") or []
    parts = [f"Extracted {len(results)} document(s)"]
    if errors:
        parts.append(f"{len(errors)} error(s)")
    return "; ".join(parts)

@router.post("/search", response_model=SearchResponse, operation_id="search")
async def search(
    request: SearchRequest,
    context_id: Optional[str] = Query(
        default=None,
        description="Optional context identifier used to persist this exchange.",
    ),
    client: httpx.AsyncClient = Depends(get_http_client),
    store: ContextStateStore = Depends(get_state_store),
) -> SearchResponse:
    """Execute web search query.

    Args:
        request: Search request with objective and parameters

    Returns:
        Search results with excerpts
    """
    payload = request.model_dump(exclude_none=True)
    objective_preview = _preview_text(request.objective)
    source_policy_keys = (
        sorted((request.source_policy or {}).keys())
        if request.source_policy
        else []
    )
    logger.info(
        "Dispatching /search request",
        extra={
            "context_id": context_id,
            "objective_preview": objective_preview,
            "max_results": payload.get("max_results"),
            "source_policy_keys": source_policy_keys,
        },
    )
    # Emit search started event
    emit_search_event("started", request.objective)
    try:
        response = await client.post(
            "/v1beta/search",
            json=payload,
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        body = response.json()
        logger.info(
            "Upstream /search completed",
            extra={
                "context_id": context_id,
                "objective_preview": objective_preview,
                "result_count": len(body.get("results") or []),
                "warning_count": len(body.get("warnings") or []),
                "upstream_request_id": response.headers.get("x-request-id"),
            },
        )
        # Persist to local store
        request_id = response.headers.get("x-request-id")
        results_count = len(body.get("results") or [])
        try:
            local_store = get_local_store()
            local_store.save_search_request(
                objective=request.objective,
                request_id=request_id,
                search_queries=request.search_queries,
                max_results=request.max_results,
                results_count=results_count,
                results=body.get("results"),
            )
        except Exception as e:
            logger.warning("Failed to persist search to local store: %s", e)
        # Emit search completed event
        emit_search_event("completed", request.objective, request_id, results_count)

        if context_id:
            await store.ensure_context(context_id)
            await store.record_interaction(
                context_id=context_id,
                interaction_type="search",
                status="completed",
                input_payload=payload,
                output_payload=body,
                summary=_summarize_search_results(body),
            )
        return SearchResponse(**body)
    except httpx.HTTPStatusError as e:
        response_text = e.response.text or ""
        logger.error(
            "Upstream /search error",
            extra={
                "context_id": context_id,
                "objective_preview": objective_preview,
                "status_code": e.response.status_code,
                "upstream_request_id": e.response.headers.get("x-request-id"),
                "response_excerpt": _shorten_body(response_text),
            },
        )
        error_payload = e.response.json() if e.response.text else {"error": "Search failed"}
        # Emit search failed event
        emit_search_event("failed", request.objective, error=str(error_payload))
        if context_id:
            await store.ensure_context(context_id)
            await store.record_interaction(
                context_id=context_id,
                interaction_type="search",
                status="failed",
                input_payload=payload,
                output_payload=error_payload,
                summary=f"Search failed ({e.response.status_code}).",
            )
        raise HTTPException(
            status_code=e.response.status_code,
            detail=error_payload,
        )
    except httpx.RequestError as e:
        logger.exception(
            "HTTP client error while calling /search",
            extra={
                "context_id": context_id,
                "objective_preview": objective_preview,
            },
        )
        raise HTTPException(
            status_code=502,
            detail={"error": "Unable to reach upstream search service"},
        ) from e

@router.post("/extract", response_model=ExtractResponse, operation_id="extract")
async def extract(
    request: ExtractRequest,
    context_id: Optional[str] = Query(
        default=None,
        description="Optional context identifier used to persist this exchange.",
    ),
    client: httpx.AsyncClient = Depends(get_http_client),
    store: ContextStateStore = Depends(get_state_store),
) -> ExtractResponse:
    """Extract content from URLs.

    Args:
        request: Extract request with URLs and settings

    Returns:
        Extracted content and metadata
    """
    payload = request.model_dump(exclude_none=True)
    objective_preview = _preview_text(request.objective)
    logger.info(
        "Dispatching /extract request",
        extra={
            "context_id": context_id,
            "objective_preview": objective_preview,
            "url_count": len(request.urls),
            "has_full_content": bool(request.full_content),
            "has_excerpts": bool(request.excerpts),
        },
    )
    try:
        response = await client.post(
            "/v1beta/extract",
            json=payload,
            headers=get_auth_headers(),
        )
        response.raise_for_status()
        body = response.json()
        logger.info(
            "Upstream /extract completed",
            extra={
                "context_id": context_id,
                "objective_preview": objective_preview,
                "url_count": len(request.urls),
                "result_count": len(body.get("results") or []),
                "error_count": len(body.get("errors") or []),
                "warning_count": len(body.get("warnings") or []),
                "upstream_request_id": response.headers.get("x-request-id"),
            },
        )
        # Persist to Turso
        try:
            store = get_local_store()
            store.save_extract_request(
                urls=request.urls,
                extract_id=response.headers.get("x-request-id"),
                objective=request.objective,
                results_count=len(body.get("results") or []),
                errors_count=len(body.get("errors") or []),
                results=body.get("results"),
            )
        except Exception as e:
            logger.warning("Failed to persist extract to Turso: %s", e)

        if context_id:
            await store.ensure_context(context_id)
            await store.record_interaction(
                context_id=context_id,
                interaction_type="extract",
                status="completed",
                input_payload=payload,
                output_payload=body,
                summary=_summarize_extract_results(body),
            )
        return ExtractResponse(**body)
    except httpx.HTTPStatusError as e:
        response_text = e.response.text or ""
        logger.error(
            "Upstream /extract error",
            extra={
                "context_id": context_id,
                "objective_preview": objective_preview,
                "url_count": len(request.urls),
                "status_code": e.response.status_code,
                "upstream_request_id": e.response.headers.get("x-request-id"),
                "response_excerpt": _shorten_body(response_text),
            },
        )
        error_payload = e.response.json() if e.response.text else {"error": "Extract failed"}
        if context_id:
            await store.ensure_context(context_id)
            await store.record_interaction(
                context_id=context_id,
                interaction_type="extract",
                status="failed",
                input_payload=payload,
                output_payload=error_payload,
                summary=f"Extract failed ({e.response.status_code}).",
            )
        raise HTTPException(
            status_code=e.response.status_code,
            detail=error_payload,
        )
    except httpx.RequestError as e:
        logger.exception(
            "HTTP client error while calling /extract",
            extra={
                "context_id": context_id,
                "objective_preview": objective_preview,
                "url_count": len(request.urls),
            },
        )
        raise HTTPException(
            status_code=502,
            detail={"error": "Unable to reach upstream extract service"},
        ) from e
