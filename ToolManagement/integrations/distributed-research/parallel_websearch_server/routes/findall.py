"""FindAll API routes (Beta)."""

import json
import logging
from typing import Any, AsyncGenerator, Dict

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from parallel_websearch_server.sse import EventSourceResponse

from parallel_websearch_server.auth import get_auth_headers_api
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.local_store import get_local_store
from parallel_websearch_server.events import emit_findall_event
from parallel_websearch_server.models import (
    IngestInput,
    FindAllRunInput,
    FindAllRun,
    FindAllRunResult,
    FindAllExtendInput,
    FindAllEnrichInput,
)
# processor_policy archived - processors validated upstream

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1beta/findall", tags=["FindAll"])


@router.post("/ingest", operation_id="findall_ingest")
async def ingest_findall(
    request: IngestInput,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> Dict[str, Any]:
    """Transform natural language objective to FindAll spec.

    Args:
        request: Objective string

    Returns:
        Generated FindAll specification
    """
    try:
        response = await client.post(
            "/v1beta/findall/ingest",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Ingest failed: {e.response.status_code}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.json() if e.response.text else {"error": "Ingest failed"},
        )


@router.post("/runs", response_model=FindAllRun, status_code=201, operation_id="create_findall")
async def create_findall_run(
    request: FindAllRunInput,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> FindAllRun:
    """Start a FindAll run.

    Args:
        request: FindAll run configuration

    Returns:
        Created FindAll run with status 'queued'
    """
    try:
        response = await client.post(
            "/v1beta/findall/runs",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        findall_run = FindAllRun(**response.json())

        # Persist to local store
        try:
            store = get_local_store()
            generator_val = request.generator.value if hasattr(request.generator, 'value') else str(request.generator)
            store.save_findall_run(
                findall_id=findall_run.findall_id,
                objective=request.objective,
                status=findall_run.status,
                entity_type=request.entity_type,
                generator=generator_val,
                match_limit=request.match_limit,
                metadata=request.metadata,
            )
            # Emit event for UI updates
            emit_findall_event("created", findall_run.findall_id, request.objective)
        except Exception as e:
            logger.warning(f"Failed to persist FindAll run: {e}")

        return findall_run
    except httpx.HTTPStatusError as e:
        logger.error(f"FindAll creation failed: {e.response.status_code}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.json() if e.response.text else {"error": "FindAll creation failed"},
        )


@router.get("/runs/{findall_id}", response_model=FindAllRun, operation_id="get_findall")
async def get_findall_run(
    findall_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> FindAllRun:
    """Get FindAll run status.

    Args:
        findall_id: FindAll run ID

    Returns:
        FindAll run metadata
    """
    try:
        response = await client.get(
            f"/v1beta/findall/runs/{findall_id}",
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return FindAllRun(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get("/runs/{findall_id}/result", response_model=FindAllRunResult, operation_id="get_findall_result")
async def get_findall_result(
    findall_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> FindAllRunResult:
    """Get FindAll run result snapshot.

    Args:
        findall_id: FindAll run ID

    Returns:
        Current candidates and results
    """
    try:
        response = await client.get(
            f"/v1beta/findall/runs/{findall_id}/result",
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return FindAllRunResult(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get("/runs/{findall_id}/schema", operation_id="get_findall_schema")
async def get_findall_schema(
    findall_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> Dict[str, Any]:
    """Get FindAll run schema.

    Retrieves the inferred schema for a FindAll run including entity type,
    match conditions, and output schema.

    Args:
        findall_id: FindAll run ID

    Returns:
        Schema specification for the FindAll run
    """
    try:
        response = await client.get(
            f"/v1beta/findall/runs/{findall_id}/schema",
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.post("/runs/{findall_id}/extend", response_model=FindAllRun, operation_id="extend_findall")
async def extend_findall_run(
    findall_id: str,
    request: FindAllExtendInput,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> FindAllRun:
    """Extend FindAll run with higher match limit.

    Args:
        findall_id: FindAll run ID
        request: New match limit

    Returns:
        Updated FindAll run
    """
    try:
        response = await client.post(
            f"/v1beta/findall/runs/{findall_id}/extend",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return FindAllRun(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.post("/runs/{findall_id}/enrich", response_model=FindAllRun, operation_id="enrich_findall")
async def enrich_findall_run(
    findall_id: str,
    request: FindAllEnrichInput,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> FindAllRun:
    """Add enrichment to FindAll candidates.

    Args:
        findall_id: FindAll run ID
        request: Enrichment configuration

    Returns:
        Updated FindAll run
    """
    try:
        response = await client.post(
            f"/v1beta/findall/runs/{findall_id}/enrich",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return FindAllRun(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.post("/runs/{findall_id}/cancel", status_code=204, operation_id="cancel_findall")
async def cancel_findall_run(
    findall_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Cancel a FindAll run.

    Args:
        findall_id: FindAll run ID
    """
    try:
        response = await client.post(
            f"/v1beta/findall/runs/{findall_id}/cancel",
            headers=get_auth_headers_api(beta_type="findall"),
        )
        response.raise_for_status()
        return None
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


async def stream_findall_events(
    findall_id: str, last_event_id: str, timeout: int, client: httpx.AsyncClient
) -> AsyncGenerator[str, None]:
    """Stream FindAll events."""
    params = {}
    if last_event_id:
        params["last_event_id"] = last_event_id
    if timeout:
        params["timeout"] = str(timeout)

    try:
        async with client.stream(
            "GET",
            f"/v1beta/findall/runs/{findall_id}/events",
            params=params,
            headers=get_auth_headers_api(beta_type="findall"),
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip():
                    yield line + "\n"
    except httpx.HTTPStatusError as e:
        error_data = {"error": f"Stream failed: {e.response.status_code}"}
        yield f"event: error\ndata: {json.dumps(error_data)}\n\n"


@router.get("/runs/{findall_id}/events", operation_id="stream_findall")
async def stream_findall_run_events(
    findall_id: str,
    last_event_id: str = None,
    timeout: int = None,
    req: Request = None,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Stream FindAll run events via SSE.

    Args:
        findall_id: FindAll run ID
        last_event_id: Resume from event ID
        timeout: Stream timeout in seconds

    Returns:
        Server-sent events stream
    """
    return EventSourceResponse(stream_findall_events(findall_id, last_event_id, timeout, client))
