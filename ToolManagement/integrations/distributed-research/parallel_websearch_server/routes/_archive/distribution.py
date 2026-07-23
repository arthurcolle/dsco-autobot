"""
Distribution System API Routes

Provides monitoring and control for intelligent task distribution.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
import logging

from parallel_websearch_server.orchestration.distributor import (
    TaskDistributor,
    DistributionTask,
    ProcessorHealth,
    BatchConfig,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1beta/distribution", tags=["distribution"])

# Global distributor instance
# In production, this would be dependency-injected or managed by app lifecycle
_distributor: Optional[TaskDistributor] = None


def get_distributor() -> TaskDistributor:
    """Get or create distributor instance"""
    global _distributor
    if _distributor is None:
        _distributor = TaskDistributor(
            processor_tiers=["lite", "base", "core", "pro", "ultra", "ultra2x"],
            batch_config=BatchConfig(
                min_batch_size=1,
                max_batch_size=50,
                max_wait_time=5.0,
                batch_by_processor=True,
                batch_by_priority=True
            ),
            max_queue_size=10000
        )
    return _distributor


# Request/Response Models

class SubmitTaskRequest(BaseModel):
    """Request to submit task for distribution"""
    task_id: str = Field(..., description="Unique task identifier")
    input_data: str = Field(..., description="Task input/query")
    processor_tier: Optional[str] = Field(None, description="Target processor (auto-select if None)")
    priority: int = Field(5, ge=1, le=10, description="Priority level (1-10, higher=more important)")
    complexity_score: int = Field(50, ge=0, le=100, description="Task complexity (0-100)")
    batch_id: Optional[str] = Field(None, description="Optional batch identifier")


class SetProcessorCapacityRequest(BaseModel):
    """Request to set processor capacity"""
    processor_tier: str = Field(..., description="Processor tier")
    capacity: int = Field(..., ge=1, description="Max concurrent tasks")


class SetProcessorHealthRequest(BaseModel):
    """Request to set processor health status"""
    processor_tier: str = Field(..., description="Processor tier")
    health: ProcessorHealth = Field(..., description="Health status")


class ReportCompletionRequest(BaseModel):
    """Request to report task completion"""
    task_id: str = Field(..., description="Task identifier")
    success: bool = Field(..., description="Whether task succeeded")
    execution_time: Optional[float] = Field(None, description="Execution time in seconds")


class BatchRequest(BaseModel):
    """Request to get next batch"""
    processor_tier: Optional[str] = Field(None, description="Filter by processor tier")
    max_batch_size: Optional[int] = Field(None, ge=1, le=1000, description="Max batch size")


# Endpoints

@router.post("/submit")
async def submit_task(request: SubmitTaskRequest):
    """
    Submit task for intelligent distribution.

    The distributor will:
    - Queue the task with specified priority
    - Auto-select optimal processor if not specified
    - Consider load balancing and health metrics
    - Apply batching strategies
    """
    distributor = get_distributor()

    task = DistributionTask(
        task_id=request.task_id,
        input_data=request.input_data,
        processor_tier=request.processor_tier,
        priority=request.priority,
        complexity_score=request.complexity_score,
        batch_id=request.batch_id
    )

    try:
        task_id = await distributor.submit_task(task)
        return {
            "task_id": task_id,
            "queued": True,
            "priority": request.priority,
            "processor_tier": request.processor_tier or "auto-select"
        }
    except ValueError as e:
        raise HTTPException(status_code=429, detail=str(e))


@router.post("/batch/next")
async def get_next_batch(request: BatchRequest):
    """
    Get next batch of tasks to execute.

    Returns prioritized batch considering:
    - Priority levels (highest first)
    - Processor affinity
    - Batching configuration
    - Load balancing
    """
    distributor = get_distributor()

    batch = await distributor.get_next_batch(
        processor_tier=request.processor_tier,
        max_batch_size=request.max_batch_size
    )

    return {
        "batch_size": len(batch),
        "tasks": [
            {
                "task_id": task.task_id,
                "processor_tier": task.processor_tier,
                "priority": task.priority,
                "wait_time": task.wait_time,
                "complexity_score": task.complexity_score
            }
            for task in batch
        ]
    }


@router.post("/completion")
async def report_completion(request: ReportCompletionRequest):
    """
    Report task completion to update metrics.

    Updates:
    - Processor load and availability
    - Success rates and health status
    - Execution time moving averages
    - Historical performance data
    """
    distributor = get_distributor()

    await distributor.report_completion(
        task_id=request.task_id,
        success=request.success,
        execution_time=request.execution_time
    )

    return {"acknowledged": True}


@router.get("/metrics")
async def get_metrics():
    """
    Get comprehensive distribution metrics.

    Returns:
    - Queue sizes and active tasks
    - Per-processor metrics (load, success rate, health)
    - Utilization and availability
    - Performance statistics
    """
    distributor = get_distributor()
    summary = await distributor.get_metrics_summary()
    return summary


@router.get("/metrics/{processor_tier}")
async def get_processor_metrics(processor_tier: str):
    """Get detailed metrics for specific processor"""
    distributor = get_distributor()
    summary = await distributor.get_metrics_summary()

    if processor_tier not in summary["processors"]:
        raise HTTPException(status_code=404, detail=f"Processor tier not found: {processor_tier}")

    return {
        "processor_tier": processor_tier,
        **summary["processors"][processor_tier]
    }


@router.post("/processor/capacity")
async def set_processor_capacity(request: SetProcessorCapacityRequest):
    """
    Set processor capacity (max concurrent tasks).

    Use this to:
    - Scale processors up/down
    - Respond to resource constraints
    - Implement custom scaling policies
    """
    distributor = get_distributor()

    await distributor.set_processor_capacity(
        processor_tier=request.processor_tier,
        capacity=request.capacity
    )

    return {
        "processor_tier": request.processor_tier,
        "capacity": request.capacity,
        "updated": True
    }


@router.post("/processor/health")
async def set_processor_health(request: SetProcessorHealthRequest):
    """
    Manually set processor health status.

    Use for:
    - Manual interventions
    - External health checks
    - Maintenance mode
    """
    distributor = get_distributor()

    await distributor.set_processor_health(
        processor_tier=request.processor_tier,
        health=request.health
    )

    return {
        "processor_tier": request.processor_tier,
        "health": request.health,
        "updated": True
    }


@router.get("/backpressure")
async def check_backpressure():
    """
    Check if system should apply backpressure.

    Returns whether new task submissions should be throttled based on:
    - Queue fullness
    - Processor health
    - Overall utilization
    """
    distributor = get_distributor()
    should_throttle = await distributor.apply_backpressure()

    return {
        "backpressure": should_throttle,
        "recommendation": "throttle_submissions" if should_throttle else "accept_submissions"
    }


@router.get("/health")
async def health_check():
    """
    Health check for distribution system.

    Returns overall system health and capacity.
    """
    distributor = get_distributor()
    summary = await distributor.get_metrics_summary()

    # Calculate overall health
    processor_healths = [
        p["health"] for p in summary["processors"].values()
    ]

    healthy_count = sum(
        1 for h in processor_healths
        if h in ("healthy", "degraded")
    )

    overall_health = "healthy" if healthy_count > 0 else "unhealthy"

    return {
        "status": overall_health,
        "queue_size": summary["queue_size"],
        "active_tasks": summary["active_tasks"],
        "healthy_processors": healthy_count,
        "total_processors": len(summary["processors"])
    }
