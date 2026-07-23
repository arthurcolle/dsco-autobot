"""Context state management routes."""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query

from parallel_websearch_server.models import (
    ContextCreateRequest,
    ContextDetailResponse,
    ContextInteraction,
    ContextRecord,
    ContextUpdateRequest,
)
from parallel_websearch_server.state_store import ContextStateStore, get_state_store

router = APIRouter(prefix="/v1alpha/contexts", tags=["Context State"])


@router.post("", response_model=ContextRecord, status_code=201, operation_id="create_context")
async def create_context(
    request: ContextCreateRequest,
    store: ContextStateStore = Depends(get_state_store),
) -> ContextRecord:
    """Create a context and optional initial state."""
    try:
        data = await store.create_context(
            context_id=request.context_id,
            name=request.name,
            description=request.description,
            metadata=request.metadata,
            state=request.state,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ContextRecord(**data)


@router.get("", response_model=List[ContextRecord], operation_id="list_contexts")
async def list_contexts(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=1000),
    store: ContextStateStore = Depends(get_state_store),
) -> List[ContextRecord]:
    """List recent contexts ordered by last update."""
    records = await store.list_contexts(limit=limit, offset=offset)
    return [ContextRecord(**record) for record in records]


@router.get("/{context_id}", response_model=ContextDetailResponse, operation_id="get_context")
async def get_context(
    context_id: str,
    history_limit: int = Query(25, ge=1, le=200),
    store: ContextStateStore = Depends(get_state_store),
) -> ContextDetailResponse:
    """Fetch metadata/state plus recent interaction history."""
    record = await store.get_context(context_id)
    if not record:
        raise HTTPException(status_code=404, detail="Context not found")

    history = await store.list_interactions(context_id, limit=history_limit)
    return ContextDetailResponse(
        context=ContextRecord(**record),
        history=[ContextInteraction(**entry) for entry in history],
    )


@router.patch(
    "/{context_id}",
    response_model=ContextRecord,
    operation_id="update_context",
)
async def update_context(
    context_id: str,
    request: ContextUpdateRequest,
    store: ContextStateStore = Depends(get_state_store),
) -> ContextRecord:
    """Update context metadata or state."""
    try:
        data = await store.update_context(
            context_id,
            name=request.name,
            description=request.description,
            metadata=request.metadata,
            state=request.state,
            replace_metadata=request.replace_metadata,
            replace_state=request.replace_state,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Context not found") from exc
    return ContextRecord(**data)


@router.get(
    "/{context_id}/history",
    response_model=List[ContextInteraction],
    operation_id="get_context_history",
)
async def get_context_history(
    context_id: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0, le=2000),
    store: ContextStateStore = Depends(get_state_store),
) -> List[ContextInteraction]:
    """Return interaction history for a context."""
    record = await store.get_context(context_id)
    if not record:
        raise HTTPException(status_code=404, detail="Context not found")

    history = await store.list_interactions(context_id, limit=limit, offset=offset)
    return [ContextInteraction(**entry) for entry in history]
