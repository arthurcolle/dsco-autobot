"""
Real-Time Dashboard API Routes

Provides SSE streaming endpoints for live progress monitoring.
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Optional
import uuid
import logging

from parallel_websearch_server.dashboard import get_progress_tracker, ProgressMetrics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1beta/dashboard", tags=["dashboard"])


@router.get("/metrics/{scope}")
async def get_metrics(scope: str = "global"):
    """
    Get current progress metrics for a scope.

    Args:
        scope: Metric scope (e.g., "global", taskgroup_id, batch_id)

    Returns:
        Current progress metrics
    """
    tracker = get_progress_tracker()
    metrics = await tracker.get_metrics(scope)
    return metrics.to_dict()


@router.get("/batch/{batch_id}/metrics")
async def get_batch_metrics(batch_id: str):
    """
    Get progress metrics for a specific batch.

    Args:
        batch_id: Batch identifier

    Returns:
        Batch progress metrics
    """
    tracker = get_progress_tracker()
    metrics = await tracker.get_batch_metrics(batch_id)

    if metrics is None:
        raise HTTPException(status_code=404, detail=f"Batch not found: {batch_id}")

    return metrics.to_dict()


@router.get("/stream")
async def stream_progress(
    scope: Optional[str] = Query(None, description="Filter events by scope"),
    subscriber_id: Optional[str] = Query(None, description="Subscriber ID for resumable streams")
):
    """
    Stream real-time progress events via Server-Sent Events (SSE).

    This endpoint provides live updates for:
    - Task submissions and completions
    - Batch execution progress
    - System metrics and health
    - Optimization updates

    Args:
        scope: Optional scope filter (e.g., taskgroup_id)
        subscriber_id: Optional subscriber ID (auto-generated if not provided)

    Returns:
        SSE stream of progress events

    Example:
        ```bash
        curl -N http://localhost:2040/v1beta/dashboard/stream
        ```

        Or with EventSource in JavaScript:
        ```javascript
        const eventSource = new EventSource('/v1beta/dashboard/stream');
        eventSource.addEventListener('task.completed', (e) => {
            const data = JSON.parse(e.data);
            console.log('Task completed:', data);
        });
        ```
    """
    tracker = get_progress_tracker()

    # Generate subscriber ID if not provided
    if not subscriber_id:
        subscriber_id = str(uuid.uuid4())

    logger.info(f"Starting progress stream for subscriber {subscriber_id}, scope={scope}")

    async def event_generator():
        """Generate SSE events"""
        try:
            async for event in tracker.stream_events(subscriber_id, scope):
                yield event
        except Exception as e:
            logger.error(f"Error in event stream for {subscriber_id}: {e}")
            raise

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        }
    )


@router.get("/subscribers")
async def get_subscriber_count():
    """
    Get number of active dashboard subscribers.

    Returns:
        Active subscriber count
    """
    tracker = get_progress_tracker()
    count = await tracker.get_active_subscriber_count()

    return {
        "active_subscribers": count
    }


@router.post("/emit/health")
async def emit_health_update(health_data: dict):
    """
    Manually emit a system health update to all subscribers.

    Use this to broadcast system-wide health metrics.

    Args:
        health_data: Health metrics and status
    """
    tracker = get_progress_tracker()
    await tracker.emit_system_health(health_data)

    return {"emitted": True}


@router.post("/emit/optimization")
async def emit_optimization_update(optimization_data: dict):
    """
    Manually emit an optimization metrics update to all subscribers.

    Use this to broadcast cost/performance optimization metrics.

    Args:
        optimization_data: Optimization metrics
    """
    tracker = get_progress_tracker()
    await tracker.emit_optimization_update(optimization_data)

    return {"emitted": True}


@router.post("/reset/{scope}")
async def reset_scope(scope: str):
    """
    Reset metrics for a specific scope.

    Use this to clear tracking data for a completed batch or group.

    Args:
        scope: Scope to reset
    """
    tracker = get_progress_tracker()
    await tracker.reset_scope(scope)

    return {"scope": scope, "reset": True}


@router.get("/health")
async def dashboard_health():
    """
    Dashboard system health check.

    Returns:
        Health status and active connections
    """
    tracker = get_progress_tracker()
    subscriber_count = await tracker.get_active_subscriber_count()

    return {
        "status": "healthy",
        "active_subscribers": subscriber_count
    }
