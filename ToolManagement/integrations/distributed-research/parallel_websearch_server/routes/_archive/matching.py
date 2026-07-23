"""API routes for the Matching Engine - job submission and management."""

from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from fastapi.responses import StreamingResponse
import asyncio
import json

from parallel_websearch_server.matching_models import (
    JobInput,
    JobSubmission,
    JobResult,
    JobStatus,
    ExecutionStatus,
    SystemMetrics,
    WorkerMetrics
)
from parallel_websearch_server.job_manager import get_job_manager
from parallel_websearch_server.worker_registry import get_registry

router = APIRouter(prefix="/v1/matching", tags=["Matching Engine"])

# Initialize manager on first request
_job_manager = None
_worker_registry = None


def _get_manager():
    """Lazy initialization of job manager."""
    global _job_manager
    if _job_manager is None:
        _job_manager = get_job_manager()
    return _job_manager


def _get_registry():
    """Lazy initialization of worker registry."""
    global _worker_registry
    if _worker_registry is None:
        _worker_registry = get_registry()
    return _worker_registry


@router.post("/jobs", response_model=JobSubmission)
async def submit_job(job_input: JobInput, background_tasks: BackgroundTasks):
    """
    Submit a new job to the matching engine.

    The job will be automatically matched to the best available worker
    based on requirements, then queued for execution.

    **Example request:**
    ```json
    {
      "description": "Analyze the latest trends in electric vehicle market",
      "job_type": "research",
      "priority": "normal",
      "requirements": {
        "min_quality_score": 0.8,
        "max_cost_usd": 1.00,
        "preferred_domains": ["finance", "technology"]
      }
    }
    ```

    **Returns:**
    - Job ID for tracking
    - Matched worker(s)
    - Estimated cost and duration
    """
    manager = _get_manager()

    try:
        submission = await manager.submit_job(job_input)
        return submission

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=ExecutionStatus)
async def get_job_status(job_id: str):
    """
    Get the current status of a job.

    **Returns:**
    - Job status (pending, queued, in_progress, completed, failed)
    - Progress percentage
    - Current execution step
    - Result if completed
    """
    manager = _get_manager()

    status = manager.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    return status


@router.get("/jobs/{job_id}/result", response_model=JobResult)
async def get_job_result(job_id: str, wait: bool = Query(False)):
    """
    Get the result of a job.

    **Parameters:**
    - `wait`: If true, blocks until job completes (up to 5 minutes)

    **Returns:**
    - Job result with output data
    - Worker information
    - Quality score
    - Actual cost and duration
    """
    manager = _get_manager()

    if wait:
        # Poll for completion (max 5 minutes)
        for _ in range(150):  # 150 * 2s = 5 minutes
            status = manager.get_job_status(job_id)
            if not status:
                raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

            if status.status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                result = manager.get_job_result(job_id)
                if result:
                    return result

            await asyncio.sleep(2)

        raise HTTPException(status_code=408, detail="Job timeout")

    else:
        result = manager.get_job_result(job_id)
        if not result:
            raise HTTPException(status_code=404, detail=f"Result not available for job: {job_id}")

        return result


@router.get("/jobs/{job_id}/events")
async def stream_job_events(job_id: str):
    """
    Stream real-time updates for a job via Server-Sent Events.

    **Example:**
    ```javascript
    const eventSource = new EventSource('/v1/matching/jobs/{job_id}/events');
    eventSource.onmessage = (event) => {
      const update = JSON.parse(event.data);
      console.log(update);
    };
    ```
    """
    manager = _get_manager()

    # Verify job exists
    status = manager.get_job_status(job_id)
    if not status:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")

    async def event_generator():
        """Generate SSE events for job updates."""
        last_status = None

        while True:
            current_status = manager.get_job_status(job_id)

            if current_status and current_status.status != last_status:
                # Send status update
                event_data = {
                    "type": "status_update",
                    "job_id": job_id,
                    "status": current_status.status.value,
                    "progress_pct": current_status.progress_pct,
                    "current_step": current_status.current_step,
                    "timestamp": datetime.now().isoformat()
                }

                yield f"data: {json.dumps(event_data)}\n\n"
                last_status = current_status.status

            # Check if job is terminal
            if current_status and current_status.status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                # Send final result
                result = manager.get_job_result(job_id)
                if result:
                    final_data = {
                        "type": "completed",
                        "job_id": job_id,
                        "result": result.model_dump(),
                        "timestamp": datetime.now().isoformat()
                    }
                    yield f"data: {json.dumps(final_data)}\n\n"
                break

            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@router.get("/jobs", response_model=List[ExecutionStatus])
async def list_jobs(
    status: Optional[JobStatus] = Query(None),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    List jobs, optionally filtered by status.

    **Parameters:**
    - `status`: Filter by job status (pending, queued, in_progress, completed, failed)
    - `limit`: Maximum number of jobs to return (1-1000)

    **Returns:**
    - List of job statuses
    """
    manager = _get_manager()

    jobs = manager.list_jobs(status=status, limit=limit)
    return jobs


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str):
    """
    Cancel a pending or running job.

    **Returns:**
    - Success message
    """
    # TODO: Implement job cancellation
    raise HTTPException(status_code=501, detail="Job cancellation not yet implemented")


@router.get("/workers/{worker_id}")
async def get_worker_info(worker_id: str):
    """
    Get detailed information about a specific worker.

    **Returns:**
    - Worker domain and specialization
    - Capabilities and proficiency levels
    - Performance metrics
    - Current load and availability
    """
    registry = _get_registry()

    worker = registry.get_agent(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail=f"Worker not found: {worker_id}")

    return worker


@router.get("/workers")
async def list_workers(
    domain: Optional[str] = Query(None),
    capability: Optional[str] = Query(None),
    available_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=2300)
):
    """
    List workers, optionally filtered by domain and capability.

    **Parameters:**
    - `domain`: Filter by domain (e.g., "finance", "technology")
    - `capability`: Filter by capability (e.g., "research", "code_execution")
    - `available_only`: Only show workers currently available
    - `limit`: Maximum number of workers to return

    **Returns:**
    - List of workers with full details
    """
    registry = _get_registry()

    if domain:
        workers = registry.find_agents_by_domain(domain)
    elif capability:
        workers = registry.find_agents_by_capability(capability)
    elif available_only:
        workers = registry.find_available_agents()
    else:
        workers = list(registry.agents.values())

    return workers[:limit]


@router.get("/stats", response_model=SystemMetrics)
async def get_system_stats():
    """
    Get overall system statistics.

    **Returns:**
    - Total jobs by status
    - Worker availability
    - Performance metrics
    - Cost analytics
    """
    manager = _get_manager()
    registry = _get_registry()

    stats = manager.get_stats()

    # Count workers by status
    available_workers = len([
        w for w in registry.agents.values()
        if w.status.value == "available"
    ])
    busy_workers = len([
        w for w in registry.agents.values()
        if w.status.value == "busy"
    ])

    # Calculate average metrics
    completed_jobs = [
        j for j in manager.jobs.values()
        if j.status == JobStatus.COMPLETED
    ]

    avg_duration = 0
    total_cost = 0.0

    if completed_jobs:
        durations = [
            (j.completed_at - j.started_at).total_seconds()
            for j in completed_jobs
            if j.completed_at and j.started_at
        ]
        if durations:
            avg_duration = sum(durations) / len(durations)

        costs = [
            j.result.actual_cost_usd
            for j in completed_jobs
            if j.result and j.result.actual_cost_usd
        ]
        if costs:
            total_cost = sum(costs)

    return SystemMetrics(
        timestamp=datetime.now(),
        total_jobs_pending=stats["jobs_by_status"].get("pending", 0),
        total_jobs_in_progress=stats["jobs_by_status"].get("in_progress", 0),
        total_jobs_completed_today=len(completed_jobs),
        total_workers_available=available_workers,
        total_workers_busy=busy_workers,
        avg_match_time_ms=50.0,  # TODO: Track actual match time
        avg_job_duration_seconds=avg_duration,
        total_cost_today_usd=total_cost
    )


@router.get("/health")
async def health_check():
    """
    Health check endpoint.

    **Returns:**
    - System status
    - Worker registry status
    - Job manager status
    """
    try:
        registry = _get_registry()
        manager = _get_manager()

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "workers": {
                "total": len(registry.agents),
                "available": len(registry.find_available_agents())
            },
            "jobs": {
                "total": manager.total_submitted,
                "completed": manager.total_completed,
                "failed": manager.total_failed
            }
        }

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {e}")
