"""
FastAPI routes for the advanced task orchestration system.

Provides HTTP endpoints for:
- Creating orchestrated tasks with automatic polling
- Batch task execution
- Queue management and statistics
- Circuit breaker control
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
import httpx

from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.orchestration import (
    OrchestrationConfig,
    TaskExecutionRequest,
    TaskExecutionResult,
    TaskOrchestrator,
    TaskPriority,
    TaskQueueStats,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/orchestration", tags=["Orchestration"])

# Global orchestrator instance (will be initialized on startup)
_orchestrator: Optional[TaskOrchestrator] = None


def get_orchestrator(
    client: httpx.AsyncClient = Depends(get_http_client)
) -> TaskOrchestrator:
    """Get or create the global orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        config = OrchestrationConfig(
            # Production-optimized defaults
            max_concurrent_tasks=20,
            circuit_breaker_enabled=True,
            adaptive_polling=True,
            cache_results=True,
            deduplicate_tasks=True,
            enable_metrics=True
        )
        _orchestrator = TaskOrchestrator(client=client, config=config)
        logger.info("TaskOrchestrator initialized with production config")
    return _orchestrator


@router.post(
    "/tasks",
    response_model=TaskExecutionResult,
    operation_id="execute_orchestrated_task",
    summary="Execute task with full orchestration"
)
async def execute_orchestrated_task(
    request: TaskExecutionRequest,
    orchestrator: TaskOrchestrator = Depends(get_orchestrator)
) -> TaskExecutionResult:
    """
    Execute a task with advanced orchestration features.

    This endpoint provides superior reliability compared to basic task endpoints:
    - Automatic polling until completion
    - Circuit breaker protection
    - Exponential backoff with retries
    - Adaptive polling intervals
    - Result caching
    - Priority-based execution

    Results are always returned inline - NEVER requires visiting external URLs.

    **Example Request:**
    ```json
    {
      "processor": "lite",
      "input": "Analyze Tesla Q4 earnings",
      "task_spec": {
        "output_schema": {"type": "text"}
      },
      "priority": "high",
      "timeout": 600
    }
    ```

    **Example Response:**
    ```json
    {
      "run_id": "trun_abc123",
      "status": "completed",
      "output_text": "Tesla Q4 earnings analysis...",
      "summary": "Task completed successfully in 45.2s",
      "processor": "lite",
      "elapsed_seconds": 45.2,
      "poll_count": 23,
      "retry_count": 0,
      "estimated_cost": 0.006
    }
    ```
    """
    try:
        logger.info(
            f"Orchestrated task request: processor={request.processor}, "
            f"priority={request.priority}, timeout={request.timeout or 'default'}"
        )

        result = await orchestrator.execute_task(request)

        logger.info(
            f"Task {result.run_id} completed: status={result.status.value}, "
            f"duration={result.elapsed_seconds:.2f}s, polls={result.poll_count}"
        )

        return result

    except Exception as e:
        logger.error(f"Orchestrated task execution failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Task execution failed: {str(e)}"
        )


@router.post(
    "/tasks/batch",
    summary="Execute multiple tasks in parallel"
)
async def execute_batch_tasks(
    tasks: list[TaskExecutionRequest],
    fail_fast: bool = Query(False, description="Stop on first failure"),
    orchestrator: TaskOrchestrator = Depends(get_orchestrator)
):
    """
    Execute multiple tasks in parallel with coordinated orchestration.

    Perfect for:
    - Comparative research across multiple topics
    - Parallel data gathering
    - High-throughput workloads

    **Example Request:**
    ```json
    {
      "tasks": [
        {
          "processor": "lite",
          "input": "Latest AI research trends",
          "priority": "high"
        },
        {
          "processor": "lite",
          "input": "Climate tech startups 2025",
          "priority": "normal"
        }
      ],
      "fail_fast": false
    }
    ```
    """
    import asyncio

    try:
        logger.info(f"Executing batch of {len(tasks)} tasks (fail_fast={fail_fast})")

        execution_tasks = [
            orchestrator.execute_task(task_request)
            for task_request in tasks
        ]

        if fail_fast:
            results = await asyncio.gather(*execution_tasks)
        else:
            results = await asyncio.gather(*execution_tasks, return_exceptions=True)

        # Format response
        successful = sum(
            1 for r in results
            if not isinstance(r, Exception) and r.status.value == "completed"
        )
        failed = len(results) - successful

        return {
            "total": len(results),
            "successful": successful,
            "failed": failed,
            "results": [
                r.model_dump() if not isinstance(r, Exception) else {"error": str(r)}
                for r in results
            ]
        }

    except Exception as e:
        logger.error(f"Batch execution failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Batch execution failed: {str(e)}"
        )


@router.get(
    "/stats",
    response_model=TaskQueueStats,
    operation_id="get_orchestrator_stats",
    summary="Get orchestrator statistics"
)
async def get_stats(
    orchestrator: TaskOrchestrator = Depends(get_orchestrator)
) -> TaskQueueStats:
    """
    Get current orchestrator statistics and health metrics.

    Returns:
    - Queue depth and task counts
    - Success/failure rates
    - Circuit breaker status
    - Performance metrics
    """
    return await orchestrator.get_stats()


@router.post(
    "/circuit-breaker/reset",
    operation_id="reset_circuit_breaker",
    summary="Reset circuit breaker"
)
async def reset_circuit_breaker(
    orchestrator: TaskOrchestrator = Depends(get_orchestrator)
):
    """
    Manually reset the circuit breaker to CLOSED state.

    Use this when you know the API is healthy and want to resume operations
    after the circuit breaker has opened due to failures.
    """
    await orchestrator.reset_circuit_breaker()
    return {
        "message": "Circuit breaker reset successfully",
        "state": "closed"
    }


@router.get(
    "/health",
    operation_id="orchestrator_health",
    summary="Health check"
)
async def health_check(
    orchestrator: TaskOrchestrator = Depends(get_orchestrator)
):
    """
    Health check endpoint for orchestrator.

    Returns:
    - Orchestrator status
    - Circuit breaker state
    - Active task count
    """
    stats = await orchestrator.get_stats()

    return {
        "status": "healthy" if not stats.circuit_breaker_open else "degraded",
        "circuit_breaker": "open" if stats.circuit_breaker_open else "closed",
        "active_tasks": stats.total_running,
        "queued_tasks": stats.total_queued,
        "total_completed": stats.total_completed,
        "total_failed": stats.total_failed,
        "success_rate": stats.success_rate
    }
