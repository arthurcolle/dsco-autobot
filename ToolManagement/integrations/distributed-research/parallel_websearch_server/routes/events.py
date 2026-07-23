"""Event streaming API routes for real-time UI updates."""

import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from parallel_websearch_server.events import (
    Event,
    EventType,
    get_event_bus,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/events", tags=["Events"])


async def event_generator(
    event_types: Optional[List[str]] = None,
    resource_ids: Optional[List[str]] = None,
    last_event_id: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """Generate SSE events for streaming."""
    bus = get_event_bus()

    async for event in bus.subscribe(
        event_types=event_types,
        resource_ids=resource_ids,
        last_event_id=last_event_id,
    ):
        yield event.to_sse()


@router.get("/stream", operation_id="stream_events")
async def stream_events(
    types: Optional[str] = Query(
        None,
        description="Comma-separated event types to filter (e.g., 'task_run.completed,task_group.created')",
    ),
    resources: Optional[str] = Query(
        None,
        description="Comma-separated resource IDs to filter (e.g., 'tgrp_abc123,trun_xyz789')",
    ),
    last_event_id: Optional[str] = Query(
        None,
        description="Resume from this event ID (for reconnection)",
        alias="Last-Event-ID",
    ),
):
    """Stream real-time events via Server-Sent Events (SSE).

    Connect to this endpoint to receive live updates about:
    - Task group creation and updates
    - Task run status changes (queued -> running -> completed/failed)
    - Search completions
    - FindAll progress
    - System errors and warnings

    The stream sends heartbeat events every 30 seconds to keep the connection alive.

    Example usage with curl:
        curl -N http://localhost:2040/v1/events/stream

    Example usage with JavaScript:
        const eventSource = new EventSource('/v1/events/stream');
        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            console.log('Event:', data);
        };

    Filter by event type:
        curl -N "http://localhost:2040/v1/events/stream?types=task_run.completed,task_run.failed"

    Filter by resource:
        curl -N "http://localhost:2040/v1/events/stream?resources=tgrp_abc123"
    """
    event_types = types.split(",") if types else None
    resource_ids = resources.split(",") if resources else None

    return StreamingResponse(
        event_generator(event_types, resource_ids, last_event_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.get("/types", operation_id="list_event_types")
async def list_event_types() -> Dict[str, List[str]]:
    """List all available event types.

    Returns a categorized list of event types that can be used for filtering.
    """
    return {
        "task_group": [
            EventType.TASK_GROUP_CREATED.value,
            EventType.TASK_GROUP_UPDATED.value,
            EventType.TASK_GROUP_COMPLETED.value,
        ],
        "task_run": [
            EventType.TASK_RUN_CREATED.value,
            EventType.TASK_RUN_QUEUED.value,
            EventType.TASK_RUN_RUNNING.value,
            EventType.TASK_RUN_COMPLETED.value,
            EventType.TASK_RUN_FAILED.value,
            EventType.TASK_RUN_CANCELLED.value,
        ],
        "search": [
            EventType.SEARCH_STARTED.value,
            EventType.SEARCH_COMPLETED.value,
            EventType.SEARCH_FAILED.value,
        ],
        "extract": [
            EventType.EXTRACT_STARTED.value,
            EventType.EXTRACT_COMPLETED.value,
            EventType.EXTRACT_FAILED.value,
        ],
        "findall": [
            EventType.FINDALL_CREATED.value,
            EventType.FINDALL_RUNNING.value,
            EventType.FINDALL_PROGRESS.value,
            EventType.FINDALL_COMPLETED.value,
            EventType.FINDALL_FAILED.value,
        ],
        "system": [
            EventType.SYSTEM_ERROR.value,
            EventType.SYSTEM_WARNING.value,
            EventType.HEARTBEAT.value,
        ],
    }


@router.get("/stats", operation_id="get_event_stats")
async def get_event_stats() -> Dict[str, Any]:
    """Get event bus statistics.

    Returns metrics about event publishing and subscriptions.
    """
    bus = get_event_bus()
    return bus.get_stats()


@router.get("/history", operation_id="get_event_history")
async def get_event_history(
    limit: int = Query(50, ge=1, le=500, description="Number of events to return"),
    types: Optional[str] = Query(None, description="Comma-separated event types to filter"),
    source: str = Query("memory", description="Source: 'memory' for current session, 'db' for persistent storage"),
) -> List[Dict[str, Any]]:
    """Get recent event history.

    Useful for:
    - Catching up on missed events after reconnection
    - Debugging event flow
    - Building initial UI state

    Use source='memory' for current session events (fast, default).
    Use source='db' for persistent events across restarts (30 day rolling).

    Returns events in chronological order (oldest first).
    """
    if source == "db":
        # Fetch from persistent SQLite storage
        from parallel_websearch_server.local_store import get_local_store
        store = get_local_store()
        event_type = types.split(",")[0] if types else None  # SQLite supports single type filter
        db_events = store.list_events(event_type=event_type, limit=limit)
        # Transform to API format
        return [
            {
                "type": e["event_type"],
                "data": e["data"],
                "resource_id": e["resource_id"],
                "resource_type": e["resource_type"],
                "timestamp": e["created_at"],
                "event_id": e["event_id"],
            }
            for e in reversed(db_events)  # Reverse to oldest-first order
        ]
    else:
        # Use in-memory event bus (current session only)
        bus = get_event_bus()
        event_types = types.split(",") if types else None
        return bus.get_recent_events(limit=limit, event_types=event_types)
