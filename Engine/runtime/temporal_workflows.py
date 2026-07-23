"""
Temporal Workflow Definitions

Durable workflows for task execution, recovery, and orchestration.
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from .temporal_activities import (
    execute_code_activity,
    search_web_activity,
    call_llm_activity,
    aggregate_results_activity,
    update_agent_state_activity,
    deposit_pheromone_activity,
    handle_task_failure_activity,
    ActivityResult,
)

logger = logging.getLogger(__name__)


@workflow.defn
class TaskExecutionWorkflow:
    """
    Executes a single task with retry logic and failure handling.

    Handles code execution, web search, LLM calls, etc.
    """

    @workflow.run
    async def run(
        self,
        task_id: str,
        task_type: str,
        task_data: Dict[str, Any],
        agent_id: str,
        required_capabilities: List[str],
        estimated_cost: float,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        """
        Execute a task with retry logic.

        Args:
            task_id: Unique task identifier
            task_type: Type of task (code, search, llm, etc)
            task_data: Task-specific data
            agent_id: Agent executing task
            required_capabilities: Required agent capabilities
            estimated_cost: Estimated cost in USD
            max_retries: Max retry attempts

        Returns:
            Dict with execution result
        """
        logger.info(f"Starting execution of {task_id} (type={task_type})")

        result = None
        last_error = None

        # Retry logic
        for attempt in range(max_retries):
            try:
                result = await self._execute_task(
                    task_type, task_data, agent_id, required_capabilities
                )

                if result.success:
                    # Update agent state on success
                    await self._update_agent_state(
                        agent_id, required_capabilities, success=True
                    )

                    # Deposit success pheromone
                    await self._deposit_pheromone(
                        agent_id, task_id, "success", 0.9
                    )

                    logger.info(f"Task {task_id} completed successfully")
                    return {
                        "task_id": task_id,
                        "success": True,
                        "output": result.output,
                        "cost": result.cost_usd,
                        "tokens": result.tokens_used,
                        "attempts": attempt + 1,
                    }

                else:
                    # Task execution failed, retry
                    last_error = result.error
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed: {last_error}"
                    )

            except ActivityError as e:
                last_error = str(e)
                logger.error(f"Activity error on attempt {attempt + 1}: {e}")

        # All retries exhausted
        logger.error(f"Task {task_id} failed after {max_retries} attempts")

        # Handle failure
        await self._handle_failure(agent_id, task_id, last_error or "Unknown error")

        return {
            "task_id": task_id,
            "success": False,
            "error": last_error,
            "attempts": max_retries,
        }

    async def _execute_task(
        self,
        task_type: str,
        task_data: Dict[str, Any],
        agent_id: str,
        required_capabilities: List[str],
    ) -> ActivityResult:
        """Execute task based on type."""
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=10),
            maximum_attempts=3,
        )

        if task_type == "code":
            return await workflow.execute_activity(
                execute_code_activity,
                task_data.get("code", ""),
                task_data.get("language", "python"),
                task_data.get("timeout", 30.0),
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
            )

        elif task_type == "search":
            return await workflow.execute_activity(
                search_web_activity,
                task_data.get("query", ""),
                task_data.get("max_results", 10),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry_policy,
            )

        elif task_type == "llm":
            return await workflow.execute_activity(
                call_llm_activity,
                task_data.get("prompt", ""),
                task_data.get("model", "claude-3.5-sonnet"),
                task_data.get("max_tokens", 1024),
                task_data.get("temperature", 0.7),
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy,
            )

        else:
            raise ValueError(f"Unknown task type: {task_type}")

    async def _update_agent_state(
        self,
        agent_id: str,
        required_capabilities: List[str],
        success: bool,
    ) -> None:
        """Update agent state after execution."""
        for capability in required_capabilities:
            await workflow.execute_activity(
                update_agent_state_activity,
                agent_id,
                capability,
                success,
                confidence_adjustment=0.05 if success else -0.03,
                start_to_close_timeout=timedelta(seconds=10),
            )

    async def _deposit_pheromone(
        self,
        agent_id: str,
        task_id: str,
        signal_type: str,
        intensity: float,
    ) -> None:
        """Deposit stigmergic signal."""
        await workflow.execute_activity(
            deposit_pheromone_activity,
            agent_id,
            task_id,
            signal_type,
            intensity,
            300,
            start_to_close_timeout=timedelta(seconds=10),
        )

    async def _handle_failure(
        self,
        agent_id: str,
        task_id: str,
        error: str,
    ) -> None:
        """Handle task failure."""
        await workflow.execute_activity(
            handle_task_failure_activity,
            agent_id,
            task_id,
            error,
            1.0,  # Full refund on failure
            start_to_close_timeout=timedelta(seconds=10),
        )


@workflow.defn
class MultiAgentTaskWorkflow:
    """
    Execute task across multiple agents in parallel.

    Uses fan-out/fan-in pattern for resilience.
    """

    @workflow.run
    async def run(
        self,
        task_id: str,
        task_type: str,
        task_data: Dict[str, Any],
        agent_ids: List[str],
        required_capabilities: List[str],
    ) -> Dict[str, Any]:
        """
        Execute task on multiple agents and aggregate results.

        Args:
            task_id: Task identifier
            task_type: Type of task
            task_data: Task data
            agent_ids: Agents to execute on
            required_capabilities: Required capabilities

        Returns:
            Aggregated result
        """
        logger.info(
            f"Starting multi-agent task {task_id} on {len(agent_ids)} agents"
        )

        # Fan-out: Start all tasks in parallel
        parallel_tasks = []
        for agent_id in agent_ids:
            task_handle = workflow.execute_activity(
                execute_code_activity,
                task_data.get("code", ""),
                task_data.get("language", "python"),
                task_data.get("timeout", 30.0),
                start_to_close_timeout=timedelta(seconds=60),
            )
            parallel_tasks.append(task_handle)

        # Fan-in: Wait for all results
        results = await workflow.wait_all(parallel_tasks)

        # Aggregate results
        result_dicts = [
            {
                "success": r.success,
                "output": r.output,
                "error": r.error,
            }
            for r in results
        ]

        aggregated = await workflow.execute_activity(
            aggregate_results_activity,
            result_dicts,
            start_to_close_timeout=timedelta(seconds=10),
        )

        return {
            "task_id": task_id,
            "success": aggregated.success,
            "output": aggregated.output,
            "agent_count": len(agent_ids),
        }


@workflow.defn
class AgentHealthCheckWorkflow:
    """
    Periodic health check of agents and pool recovery.
    """

    @workflow.run
    async def run(
        self,
        agent_pool_id: str,
        agent_ids: List[str],
        check_interval_seconds: int = 30,
    ) -> Dict[str, Any]:
        """
        Check health of all agents and handle failures.

        Args:
            agent_pool_id: ID of agent pool
            agent_ids: List of agent IDs to check
            check_interval_seconds: Check interval

        Returns:
            Health check result
        """
        logger.info(f"Starting health check for pool {agent_pool_id}")

        healthy = 0
        degraded = 0
        unhealthy = 0

        for agent_id in agent_ids:
            try:
                # In real implementation:
                # - Query agent status
                # - Check heartbeat
                # - Verify state
                # - Mark dead agents
                # - Trigger recovery

                # For now, simulate checks
                if agent_id.endswith("-1"):
                    unhealthy += 1
                elif agent_id.endswith("-2"):
                    degraded += 1
                else:
                    healthy += 1

            except Exception as e:
                logger.error(f"Health check failed for {agent_id}: {e}")
                unhealthy += 1

        return {
            "pool_id": agent_pool_id,
            "healthy": healthy,
            "degraded": degraded,
            "unhealthy": unhealthy,
            "total": len(agent_ids),
        }


@workflow.defn
class ResourceAllocationWorkflow:
    """
    Allocate and reserve resources for task execution.
    """

    @workflow.run
    async def run(
        self,
        task_id: str,
        agent_id: str,
        estimated_cost: float,
        estimated_gpu_hours: float = 0.0,
        estimated_memory_gb: float = 0.1,
    ) -> Dict[str, Any]:
        """
        Reserve resources for a task.

        Args:
            task_id: Task ID
            agent_id: Agent ID
            estimated_cost: Estimated API cost
            estimated_gpu_hours: Estimated GPU hours
            estimated_memory_gb: Estimated memory

        Returns:
            Resource allocation result
        """
        logger.info(
            f"Allocating resources for {task_id}: "
            f"${estimated_cost}, {estimated_gpu_hours}h GPU, {estimated_memory_gb}GB RAM"
        )

        # In real implementation:
        # 1. Query agent budget
        # 2. Query GPU quota
        # 3. Check memory availability
        # 4. Create reservations
        # 5. Return allocation

        return {
            "task_id": task_id,
            "agent_id": agent_id,
            "allocated": True,
            "cost_reserved": estimated_cost,
            "gpu_reserved_hours": estimated_gpu_hours,
            "memory_reserved_gb": estimated_memory_gb,
        }
