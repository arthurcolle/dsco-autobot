"""
Jobs API - Async job submission, polling, streaming, and cancellation.

Wraps the StreamingExecutor to provide a REST API for long-running tool executions.
"""

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..core.streaming import get_streaming_executor, StreamingExecution, EventType

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


# ==================== Request / Response Models ====================

class JobSubmitRequest(BaseModel):
    tool: str = Field(..., description="Tool name (or backend:tool)")
    arguments: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=300.0, description="Max execution time in seconds")
    client_id: Optional[str] = None


class JobStatusResponse(BaseModel):
    job_id: str
    tool: str
    status: str
    progress: float = 0
    stage: Optional[str] = None
    stages_completed: List[str] = Field(default_factory=list)
    stages_remaining: List[str] = Field(default_factory=list)
    estimated_completion_seconds: Optional[int] = None
    partial_results: List[Any] = Field(default_factory=list)
    result: Optional[Any] = None
    error: Optional[str] = None
    timestamps: Dict[str, Optional[float]] = Field(default_factory=dict)


class JobSubmitResponse(BaseModel):
    job_id: str
    status: str
    poll_url: str
    stream_url: str


# ==================== Helpers ====================

def _execution_to_status(ex: StreamingExecution) -> JobStatusResponse:
    partial_results = []
    result = None
    error = None
    stage_messages: List[str] = []
    stage = None

    expected_stages = [
        "resolving_backend",
        "invoking_backend",
        "processing_backend_response",
        "completed",
    ]

    for event in ex.events:
        if event.type == EventType.PARTIAL and event.data is not None:
            partial_results.append(event.data)
        elif event.type == EventType.PROGRESS:
            if event.message:
                stage = event.message
                stage_messages.append(event.message)
        elif event.type == EventType.COMPLETED and event.data is not None:
            result = event.data
            stage = "completed"
            stage_messages.append("completed")
        elif event.type == EventType.ERROR and event.error:
            error = event.error

    dedup_stages: List[str] = []
    for msg in stage_messages:
        if msg not in dedup_stages:
            dedup_stages.append(msg)
    remaining = [s for s in expected_stages if s not in dedup_stages]

    eta = None
    if ex.status == "running":
        if ex.progress <= 0:
            eta = None
        else:
            elapsed = max(0.0, (time.time() - (ex.started_at or time.time())))
            remaining_pct = max(0.0, 100.0 - ex.progress)
            eta = int((elapsed / max(1.0, ex.progress)) * remaining_pct)

    return JobStatusResponse(
        job_id=ex.execution_id,
        tool=ex.tool_name,
        status=ex.status,
        progress=ex.progress,
        stage=stage,
        stages_completed=dedup_stages,
        stages_remaining=remaining,
        estimated_completion_seconds=eta,
        partial_results=partial_results,
        result=result,
        error=error,
        timestamps={
            "started_at": ex.started_at,
            "completed_at": ex.completed_at,
        },
    )


# ==================== Endpoints ====================

@router.post("/submit", response_model=JobSubmitResponse)
async def submit_job(request: JobSubmitRequest):
    """
    Submit a long-running tool execution as a background job.

    Returns immediately with a job_id that can be polled or streamed.
    """
    try:
        executor = get_streaming_executor()
    except ValueError:
        raise HTTPException(
            status_code=503,
            detail="Streaming executor not initialized yet",
        )

    execution = await executor.execute(
        tool_name=request.tool,
        inputs={**request.arguments, "_timeout": request.timeout},
        client_id=request.client_id,
    )

    return JobSubmitResponse(
        job_id=execution.execution_id,
        status=execution.status,
        poll_url=f"/api/v1/jobs/{execution.execution_id}",
        stream_url=f"/api/v1/jobs/{execution.execution_id}/stream",
    )


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Poll the status of a submitted job."""
    try:
        executor = get_streaming_executor()
    except ValueError:
        raise HTTPException(503, "Streaming executor not initialized")

    ex = executor.get_execution(job_id)
    if not ex:
        raise HTTPException(404, f"Job not found: {job_id}")

    return _execution_to_status(ex)


@router.get("/{job_id}/stream")
async def stream_job_events(job_id: str):
    """Stream SSE events for a running job (includes history replay)."""
    try:
        executor = get_streaming_executor()
    except ValueError:
        raise HTTPException(503, "Streaming executor not initialized")

    ex = executor.get_execution(job_id)
    if not ex:
        raise HTTPException(404, f"Job not found: {job_id}")

    return StreamingResponse(
        executor.stream_sse(job_id, include_history=True),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running job."""
    try:
        executor = get_streaming_executor()
    except ValueError:
        raise HTTPException(503, "Streaming executor not initialized")

    cancelled = executor.cancel_execution(job_id)
    if not cancelled:
        ex = executor.get_execution(job_id)
        if not ex:
            raise HTTPException(404, f"Job not found: {job_id}")
        return {"job_id": job_id, "cancelled": False, "reason": f"Job is {ex.status}"}

    return {"job_id": job_id, "cancelled": True}


@router.get("/")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=200),
):
    """List all jobs with optional status filter."""
    try:
        executor = get_streaming_executor()
    except ValueError:
        raise HTTPException(503, "Streaming executor not initialized")

    jobs = []
    for ex in executor._executions.values():
        if status and ex.status != status:
            continue
        jobs.append({
            "job_id": ex.execution_id,
            "tool": ex.tool_name,
            "status": ex.status,
            "progress": ex.progress,
            "started_at": ex.started_at,
            "completed_at": ex.completed_at,
        })

    # Sort by started_at descending (most recent first)
    jobs.sort(key=lambda j: j.get("started_at") or 0, reverse=True)
    jobs = jobs[:limit]

    stats = executor.get_stats()

    return {
        "jobs": jobs,
        "count": len(jobs),
        "stats": stats,
    }
