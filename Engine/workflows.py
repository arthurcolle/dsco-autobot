"""
Engine API Temporal Workflows

Durable execution for agent lifecycle, task execution, and multi-agent coordination.

Key workflows:
- AgentLifecycleWorkflow: Manages single agent spawn/pause/resume/kill
- TaskExecutionWorkflow: Tracks task from assignment to completion
- MultiAgentCoordinationWorkflow: Fan-out/fan-in patterns
- ScheduledWorkWorkflow: Cron-based periodic tasks
"""

import asyncio
import logging
from datetime import timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from temporalio import workflow
from temporalio.common import RetryPolicy

# Import activity stubs - actual implementations in activities.py
with workflow.unsafe.imports_passed_through():
    from .models import (
        Tick,
        TickType,
        Signal,
        SignalType,
        Task,
        TaskStatus,
        AgentConfig,
        AgentStatus,
        AgentState,
        StateDelta,
    )

logger = logging.getLogger(__name__)


# ============================================================================
# Workflow Input/Output Types
# ============================================================================

@dataclass
class AgentLifecycleInput:
    """Input for agent lifecycle workflow."""
    agent_id: str
    config: Dict[str, Any]  # Serialized AgentConfig
    initial_state: Optional[Dict[str, Any]] = None


@dataclass
class TaskExecutionInput:
    """Input for task execution workflow."""
    task: Dict[str, Any]  # Serialized Task
    agent_id: str
    timeout_ms: int = 300000


@dataclass
class MultiAgentInput:
    """Input for multi-agent coordination workflow."""
    task: Dict[str, Any]  # Serialized complex task
    agent_pool: List[str]
    coordination_strategy: str = "round_robin"  # round_robin, least_loaded, capability_match


@dataclass
class ScheduledWorkInput:
    """Input for scheduled work workflow."""
    cron_expression: str
    task_template: Dict[str, Any]
    target_agents: List[str]


# ============================================================================
# Agent Lifecycle Workflow
# ============================================================================

@workflow.defn
class AgentLifecycleWorkflow:
    """
    Manages a single agent's lifecycle.

    Responsibilities:
    - Agent registration and initialization
    - Tick scheduling based on configured interval
    - Signal handling (interrupt, cancel, shutdown)
    - State checkpointing
    - Graceful shutdown

    This workflow runs for the entire lifetime of an agent connection.
    """

    def __init__(self):
        self.status = AgentStatus.INITIALIZING
        self.current_task: Optional[Dict[str, Any]] = None
        self.pending_signals: List[Dict[str, Any]] = []
        self.tasks_completed = 0
        self.last_heartbeat_ms = 0
        self.should_terminate = False

    @workflow.run
    async def run(self, input: AgentLifecycleInput) -> Dict[str, Any]:
        """Main agent lifecycle loop."""

        agent_id = input.agent_id
        config = input.config
        tick_interval_ms = config.get("tick_interval_ms", 1000)

        workflow.logger.info(f"Starting lifecycle for agent {agent_id}")

        # Phase 1: Registration
        try:
            await workflow.execute_activity(
                "register_agent",
                args=[agent_id, config],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            self.status = AgentStatus.IDLE
        except Exception as e:
            workflow.logger.error(f"Failed to register agent {agent_id}: {e}")
            self.status = AgentStatus.ERROR
            return {"error": str(e), "status": "registration_failed"}

        # Phase 2: Load initial state if provided
        if input.initial_state:
            await workflow.execute_activity(
                "restore_state",
                args=[agent_id, input.initial_state],
                start_to_close_timeout=timedelta(seconds=10),
            )

        # Phase 3: Main lifecycle loop
        while not self.should_terminate:
            try:
                # Wait for signal or tick interval timeout
                signal_received = await workflow.wait_condition(
                    lambda: len(self.pending_signals) > 0 or
                            self.current_task is not None or
                            self.should_terminate,
                    timeout=timedelta(milliseconds=tick_interval_ms),
                )

                if self.should_terminate:
                    break

                # Handle pending signals first (they're priority)
                while self.pending_signals:
                    signal = self.pending_signals.pop(0)
                    await self._handle_signal(agent_id, signal)

                # Execute current task if assigned
                if self.current_task and self.status != AgentStatus.WORKING:
                    await self._execute_task(agent_id, self.current_task)

                # Send regular tick if no task and signal wasn't received
                if not signal_received and self.status == AgentStatus.IDLE:
                    await workflow.execute_activity(
                        "send_tick",
                        args=[agent_id, TickType.POLL.value],
                        start_to_close_timeout=timedelta(seconds=5),
                    )

            except asyncio.TimeoutError:
                # Tick interval timeout - normal, send heartbeat
                await workflow.execute_activity(
                    "send_heartbeat",
                    args=[agent_id],
                    start_to_close_timeout=timedelta(seconds=5),
                )

            except Exception as e:
                workflow.logger.error(f"Error in lifecycle loop: {e}")
                self.status = AgentStatus.ERROR
                # Checkpoint on error
                await self._checkpoint(agent_id)
                await asyncio.sleep(1)  # Back off

        # Phase 4: Graceful shutdown
        workflow.logger.info(f"Shutting down agent {agent_id}")
        await self._checkpoint(agent_id)
        await workflow.execute_activity(
            "unregister_agent",
            args=[agent_id],
            start_to_close_timeout=timedelta(seconds=10),
        )

        return {
            "agent_id": agent_id,
            "status": "terminated",
            "tasks_completed": self.tasks_completed,
        }

    async def _handle_signal(self, agent_id: str, signal: Dict[str, Any]) -> None:
        """Handle an incoming signal."""
        signal_type = signal.get("signal_type")

        if signal_type == SignalType.SHUTDOWN.value:
            self.should_terminate = True
            graceful = signal.get("payload", {}).get("graceful", True)
            if not graceful:
                # Immediate shutdown - cancel current task
                self.current_task = None
                self.status = AgentStatus.TERMINATED

        elif signal_type == SignalType.INTERRUPT.value:
            # New high-priority task interrupts current
            if signal.get("preempt", False):
                self.current_task = None
                self.status = AgentStatus.IDLE
            # Assign new task
            self.current_task = signal.get("payload")

        elif signal_type == SignalType.CANCEL.value:
            # Cancel current task
            task_id = signal.get("payload", {}).get("task_id")
            if self.current_task and self.current_task.get("task_id") == task_id:
                self.current_task = None
                self.status = AgentStatus.IDLE
                await workflow.execute_activity(
                    "notify_task_cancelled",
                    args=[agent_id, task_id],
                    start_to_close_timeout=timedelta(seconds=5),
                )

        elif signal_type == SignalType.PRIORITY.value:
            # Update task priority
            if self.current_task:
                self.current_task["priority"] = signal.get("payload", {}).get("priority", 5)

        elif signal_type == SignalType.STATE_PUSH.value:
            # Immediate state update
            delta = signal.get("payload")
            await workflow.execute_activity(
                "apply_state_delta",
                args=[agent_id, delta],
                start_to_close_timeout=timedelta(seconds=5),
            )

    async def _execute_task(self, agent_id: str, task: Dict[str, Any]) -> None:
        """Execute a task and handle result."""
        self.status = AgentStatus.WORKING
        task_id = task.get("task_id")

        try:
            # Send task tick to agent
            await workflow.execute_activity(
                "send_task_tick",
                args=[agent_id, task],
                start_to_close_timeout=timedelta(seconds=5),
            )

            # Wait for result with timeout
            timeout_ms = task.get("timeout_ms", 300000)
            result = await workflow.execute_activity(
                "wait_for_task_result",
                args=[agent_id, task_id],
                start_to_close_timeout=timedelta(milliseconds=timeout_ms),
                retry_policy=RetryPolicy(maximum_attempts=1),  # No retry for task execution
            )

            # Task completed successfully
            self.tasks_completed += 1
            await workflow.execute_activity(
                "notify_task_completed",
                args=[agent_id, task_id, result],
                start_to_close_timeout=timedelta(seconds=10),
            )

        except Exception as e:
            workflow.logger.error(f"Task {task_id} failed: {e}")
            await workflow.execute_activity(
                "notify_task_failed",
                args=[agent_id, task_id, str(e)],
                start_to_close_timeout=timedelta(seconds=10),
            )

        finally:
            self.current_task = None
            self.status = AgentStatus.IDLE

    async def _checkpoint(self, agent_id: str) -> None:
        """Save agent state checkpoint."""
        state = {
            "status": self.status.value if hasattr(self.status, 'value') else self.status,
            "tasks_completed": self.tasks_completed,
            "current_task": self.current_task,
        }
        await workflow.execute_activity(
            "checkpoint_state",
            args=[agent_id, state],
            start_to_close_timeout=timedelta(seconds=10),
        )

    # ========================================================================
    # Signals
    # ========================================================================

    @workflow.signal
    async def assign_task(self, task: Dict[str, Any]) -> None:
        """Signal to assign a new task."""
        self.current_task = task
        workflow.logger.info(f"Task assigned: {task.get('task_id')}")

    @workflow.signal
    async def interrupt(self, signal: Dict[str, Any]) -> None:
        """Signal to interrupt with priority action."""
        self.pending_signals.append(signal)
        workflow.logger.info(f"Signal received: {signal.get('signal_type')}")

    @workflow.signal
    async def shutdown(self, graceful: bool = True) -> None:
        """Signal to shutdown the agent."""
        self.pending_signals.append({
            "signal_type": SignalType.SHUTDOWN.value,
            "payload": {"graceful": graceful},
        })

    # ========================================================================
    # Queries
    # ========================================================================

    @workflow.query
    def get_status(self) -> str:
        """Query current agent status."""
        return self.status.value if hasattr(self.status, 'value') else str(self.status)

    @workflow.query
    def get_current_task(self) -> Optional[Dict[str, Any]]:
        """Query current task."""
        return self.current_task

    @workflow.query
    def get_stats(self) -> Dict[str, Any]:
        """Query agent statistics."""
        return {
            "status": self.status.value if hasattr(self.status, 'value') else str(self.status),
            "tasks_completed": self.tasks_completed,
            "has_current_task": self.current_task is not None,
            "pending_signals": len(self.pending_signals),
        }


# ============================================================================
# Task Execution Workflow
# ============================================================================

@workflow.defn
class TaskExecutionWorkflow:
    """
    Tracks a single task from creation to completion.

    Separate from agent lifecycle - allows task tracking across agent failures.
    If agent fails, task can be reassigned to another agent.
    """

    def __init__(self):
        self.status = TaskStatus.PENDING
        self.assigned_agent: Optional[str] = None
        self.result: Optional[Any] = None
        self.error: Optional[str] = None
        self.retries = 0

    @workflow.run
    async def run(self, input: TaskExecutionInput) -> Dict[str, Any]:
        """Execute task with retry and reassignment support."""

        task = input.task
        task_id = task.get("task_id")
        max_retries = task.get("max_retries", 3)

        workflow.logger.info(f"Starting task execution: {task_id}")

        while self.retries < max_retries and self.status not in [
            TaskStatus.COMPLETED, TaskStatus.CANCELLED
        ]:
            try:
                # Find available agent if not assigned
                if not self.assigned_agent:
                    self.assigned_agent = await workflow.execute_activity(
                        "find_available_agent",
                        args=[task.get("required_capabilities", [])],
                        start_to_close_timeout=timedelta(seconds=30),
                    )

                if not self.assigned_agent:
                    workflow.logger.warning("No available agent, waiting...")
                    await asyncio.sleep(5)
                    continue

                # Assign task to agent's lifecycle workflow
                self.status = TaskStatus.ASSIGNED
                agent_handle = workflow.get_external_workflow_handle(
                    f"agent-lifecycle-{self.assigned_agent}"
                )
                await agent_handle.signal(
                    AgentLifecycleWorkflow.assign_task,
                    task,
                )

                # Wait for completion
                self.status = TaskStatus.RUNNING
                self.result = await workflow.execute_activity(
                    "wait_for_task_result",
                    args=[self.assigned_agent, task_id],
                    start_to_close_timeout=timedelta(milliseconds=input.timeout_ms),
                )

                self.status = TaskStatus.COMPLETED
                workflow.logger.info(f"Task {task_id} completed successfully")

            except asyncio.CancelledError:
                self.status = TaskStatus.CANCELLED
                break

            except Exception as e:
                self.retries += 1
                self.error = str(e)
                self.assigned_agent = None  # Try different agent
                workflow.logger.warning(
                    f"Task {task_id} attempt {self.retries} failed: {e}"
                )

                if self.retries < max_retries:
                    await asyncio.sleep(min(2 ** self.retries, 30))  # Exponential backoff

        if self.status not in [TaskStatus.COMPLETED, TaskStatus.CANCELLED]:
            self.status = TaskStatus.FAILED

        return {
            "task_id": task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "retries": self.retries,
            "assigned_agent": self.assigned_agent,
        }

    @workflow.signal
    async def cancel(self) -> None:
        """Cancel the task."""
        self.status = TaskStatus.CANCELLED
        if self.assigned_agent:
            # Signal agent to cancel
            try:
                agent_handle = workflow.get_external_workflow_handle(
                    f"agent-lifecycle-{self.assigned_agent}"
                )
                await agent_handle.signal(
                    AgentLifecycleWorkflow.interrupt,
                    {
                        "signal_type": SignalType.CANCEL.value,
                        "payload": {"task_id": self.task.get("task_id")},
                    },
                )
            except Exception:
                pass  # Agent may already be gone

    @workflow.query
    def get_status(self) -> str:
        """Query task status."""
        return self.status.value


# ============================================================================
# Multi-Agent Coordination Workflow
# ============================================================================

@workflow.defn
class MultiAgentCoordinationWorkflow:
    """
    Coordinates work across multiple agents.

    Patterns:
    - Fan-out: Distribute subtasks to multiple agents
    - Fan-in: Aggregate results from multiple agents
    - Pipeline: Sequential processing across agents
    - Consensus: Agents vote on decisions
    """

    def __init__(self):
        self.subtask_results: Dict[str, Any] = {}
        self.failed_subtasks: List[str] = []
        self.status = "initializing"

    @workflow.run
    async def run(self, input: MultiAgentInput) -> Dict[str, Any]:
        """Execute coordinated multi-agent task."""

        task = input.task
        agent_pool = input.agent_pool
        strategy = input.coordination_strategy

        workflow.logger.info(
            f"Starting multi-agent coordination: {len(agent_pool)} agents"
        )

        # Phase 1: Decompose task into subtasks
        self.status = "decomposing"
        subtasks = await workflow.execute_activity(
            "decompose_task",
            args=[task],
            start_to_close_timeout=timedelta(seconds=60),
        )

        if not subtasks:
            return {"error": "Failed to decompose task", "status": "failed"}

        workflow.logger.info(f"Decomposed into {len(subtasks)} subtasks")

        # Phase 2: Assign subtasks to agents
        self.status = "assigning"
        assignments = self._assign_subtasks(subtasks, agent_pool, strategy)

        # Phase 3: Fan-out execution
        self.status = "executing"
        execution_tasks = []

        for subtask, agent_id in assignments:
            execution_tasks.append(
                self._execute_subtask(subtask, agent_id)
            )

        # Wait for all with configurable failure handling
        await asyncio.gather(*execution_tasks, return_exceptions=True)

        # Phase 4: Aggregate results
        self.status = "aggregating"
        if self.failed_subtasks:
            workflow.logger.warning(
                f"{len(self.failed_subtasks)} subtasks failed"
            )

        aggregated = await workflow.execute_activity(
            "aggregate_results",
            args=[task, self.subtask_results, self.failed_subtasks],
            start_to_close_timeout=timedelta(seconds=60),
        )

        self.status = "completed"
        return {
            "status": "completed",
            "result": aggregated,
            "subtasks_total": len(subtasks),
            "subtasks_succeeded": len(self.subtask_results),
            "subtasks_failed": len(self.failed_subtasks),
        }

    def _assign_subtasks(
        self,
        subtasks: List[Dict],
        agent_pool: List[str],
        strategy: str,
    ) -> List[tuple]:
        """Assign subtasks to agents based on strategy."""
        assignments = []

        if strategy == "round_robin":
            for i, subtask in enumerate(subtasks):
                agent = agent_pool[i % len(agent_pool)]
                assignments.append((subtask, agent))

        elif strategy == "least_loaded":
            # Would query actual load, simplified here
            assignments = [(st, agent_pool[i % len(agent_pool)])
                          for i, st in enumerate(subtasks)]

        elif strategy == "capability_match":
            # Would match capabilities, simplified here
            assignments = [(st, agent_pool[0]) for st in subtasks]

        return assignments

    async def _execute_subtask(
        self,
        subtask: Dict[str, Any],
        agent_id: str,
    ) -> None:
        """Execute a single subtask on an agent."""
        subtask_id = subtask.get("subtask_id", subtask.get("task_id"))

        try:
            # Start child workflow for the subtask
            result = await workflow.execute_child_workflow(
                TaskExecutionWorkflow.run,
                TaskExecutionInput(
                    task=subtask,
                    agent_id=agent_id,
                    timeout_ms=subtask.get("timeout_ms", 300000),
                ),
                id=f"subtask-{subtask_id}",
            )

            if result.get("status") == "completed":
                self.subtask_results[subtask_id] = result.get("result")
            else:
                self.failed_subtasks.append(subtask_id)

        except Exception as e:
            workflow.logger.error(f"Subtask {subtask_id} failed: {e}")
            self.failed_subtasks.append(subtask_id)

    @workflow.query
    def get_progress(self) -> Dict[str, Any]:
        """Query coordination progress."""
        return {
            "status": self.status,
            "completed": len(self.subtask_results),
            "failed": len(self.failed_subtasks),
        }


# ============================================================================
# Scheduled Work Workflow
# ============================================================================

@workflow.defn
class ScheduledWorkWorkflow:
    """
    Cron-based scheduled task execution.

    Runs on a schedule and distributes work to agents.
    Uses Temporal's built-in cron scheduling.
    """

    def __init__(self):
        self.executions = 0
        self.last_run: Optional[str] = None

    @workflow.run
    async def run(self, input: ScheduledWorkInput) -> Dict[str, Any]:
        """Execute scheduled work."""

        self.executions += 1
        self.last_run = workflow.now().isoformat()

        workflow.logger.info(
            f"Scheduled execution #{self.executions} at {self.last_run}"
        )

        # Instantiate task from template
        task = {
            **input.task_template,
            "task_id": f"scheduled-{self.executions}-{workflow.now().timestamp()}",
            "scheduled_run": self.executions,
        }

        # Fan out to target agents or use multi-agent workflow
        if len(input.target_agents) > 1:
            result = await workflow.execute_child_workflow(
                MultiAgentCoordinationWorkflow.run,
                MultiAgentInput(
                    task=task,
                    agent_pool=input.target_agents,
                ),
                id=f"scheduled-multi-{self.executions}",
            )
        else:
            # Single agent execution
            result = await workflow.execute_child_workflow(
                TaskExecutionWorkflow.run,
                TaskExecutionInput(
                    task=task,
                    agent_id=input.target_agents[0],
                ),
                id=f"scheduled-single-{self.executions}",
            )

        return {
            "execution": self.executions,
            "timestamp": self.last_run,
            "result": result,
        }

    @workflow.query
    def get_execution_count(self) -> int:
        """Query total executions."""
        return self.executions


# ============================================================================
# Helper Workflows
# ============================================================================

@workflow.defn
class HealthCheckWorkflow:
    """
    Periodic health check workflow.

    Monitors agent health across the system.
    """

    @workflow.run
    async def run(self, agent_ids: List[str]) -> Dict[str, Any]:
        """Check health of all specified agents."""

        results = {}
        for agent_id in agent_ids:
            try:
                handle = workflow.get_external_workflow_handle(
                    f"agent-lifecycle-{agent_id}"
                )
                status = await handle.query(AgentLifecycleWorkflow.get_status)
                results[agent_id] = {"healthy": True, "status": status}
            except Exception as e:
                results[agent_id] = {"healthy": False, "error": str(e)}

        healthy_count = sum(1 for r in results.values() if r.get("healthy"))

        return {
            "timestamp": workflow.now().isoformat(),
            "total_agents": len(agent_ids),
            "healthy": healthy_count,
            "unhealthy": len(agent_ids) - healthy_count,
            "details": results,
        }
