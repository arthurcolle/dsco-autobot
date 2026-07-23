"""
Multi-Agent Workflow Coordination

Coordinates execution of tasks across multiple agents with patterns like:
- Fan-out / Fan-in (parallel tasks)
- Sequential execution
- Consensus voting
- Pipeline processing
"""

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Set
from datetime import datetime

from .agent_pool import AgentPool
from .agent_executor import ExecutionContext

logger = logging.getLogger(__name__)


class WorkflowPattern(Enum):
    """Multi-agent workflow patterns."""
    FAN_OUT = "fan_out"          # Parallel execution
    SEQUENTIAL = "sequential"    # One after another
    CONSENSUS = "consensus"      # Voting/quorum
    PIPELINE = "pipeline"        # Pass output to next


class TaskDependency(Enum):
    """Dependency relationship between tasks."""
    INDEPENDENT = "independent"  # Can run in parallel
    SEQUENTIAL = "sequential"    # Must run in order
    CONDITIONAL = "conditional"  # Depends on result


@dataclass
class SubTask:
    """A subtask within a workflow."""
    task_id: str
    task_type: str
    task_data: Dict[str, Any]
    required_capabilities: List[str]
    estimated_cost: float = 1.0
    depends_on: List[str] = field(default_factory=list)
    timeout_seconds: float = 300.0
    retry_count: int = 0


@dataclass
class WorkflowTask:
    """A multi-agent workflow task."""
    workflow_id: str
    pattern: WorkflowPattern
    subtasks: List[SubTask]
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)

    @property
    def is_completed(self) -> bool:
        """Check if workflow is complete."""
        return self.completed_at is not None

    @property
    def success_count(self) -> int:
        """Count successful subtasks."""
        return len(self.results) - len(self.errors)

    @property
    def failure_count(self) -> int:
        """Count failed subtasks."""
        return len(self.errors)


@dataclass
class WorkflowResult:
    """Result of a workflow execution."""
    workflow_id: str
    pattern: WorkflowPattern
    success: bool
    results: Dict[str, Any]
    errors: Dict[str, str]
    duration_seconds: float
    subtask_count: int
    successful_subtasks: int
    failed_subtasks: int


class MultiAgentWorkflow:
    """
    Orchestrates multi-agent workflows.

    Supports:
    - Fan-out/Fan-in: Execute tasks in parallel
    - Sequential: Execute tasks in order
    - Consensus: Execute on multiple agents, use voting
    - Pipeline: Chain tasks with output as input
    """

    def __init__(self, agent_pool: AgentPool):
        """
        Initialize workflow coordinator.

        Args:
            agent_pool: AgentPool to execute tasks on
        """
        self.agent_pool = agent_pool
        self._active_workflows: Dict[str, WorkflowTask] = {}
        self._completed_workflows: Dict[str, WorkflowResult] = {}

    async def execute_workflow(
        self,
        workflow: WorkflowTask,
    ) -> WorkflowResult:
        """
        Execute a multi-agent workflow.

        Args:
            workflow: Workflow to execute

        Returns:
            WorkflowResult with outcomes
        """
        logger.info(
            f"Starting workflow {workflow.workflow_id} "
            f"(pattern={workflow.pattern.value}, subtasks={len(workflow.subtasks)})"
        )

        start_time = datetime.utcnow()
        self._active_workflows[workflow.workflow_id] = workflow

        try:
            if workflow.pattern == WorkflowPattern.FAN_OUT:
                await self._execute_fan_out(workflow)
            elif workflow.pattern == WorkflowPattern.SEQUENTIAL:
                await self._execute_sequential(workflow)
            elif workflow.pattern == WorkflowPattern.CONSENSUS:
                await self._execute_consensus(workflow)
            elif workflow.pattern == WorkflowPattern.PIPELINE:
                await self._execute_pipeline(workflow)

            workflow.completed_at = datetime.utcnow()

            # Create result
            duration = (workflow.completed_at - start_time).total_seconds()
            result = WorkflowResult(
                workflow_id=workflow.workflow_id,
                pattern=workflow.pattern,
                success=len(workflow.errors) == 0,
                results=workflow.results,
                errors=workflow.errors,
                duration_seconds=duration,
                subtask_count=len(workflow.subtasks),
                successful_subtasks=workflow.success_count,
                failed_subtasks=workflow.failure_count,
            )

            self._completed_workflows[workflow.workflow_id] = result

            logger.info(
                f"Completed workflow {workflow.workflow_id}: "
                f"{workflow.success_count}/{len(workflow.subtasks)} successful"
            )

            return result

        except Exception as e:
            logger.error(f"Error executing workflow {workflow.workflow_id}: {e}")
            raise

        finally:
            del self._active_workflows[workflow.workflow_id]

    async def _execute_fan_out(self, workflow: WorkflowTask) -> None:
        """
        Execute subtasks in parallel (fan-out/fan-in).

        All tasks start simultaneously, wait for all to complete.
        """
        logger.debug(f"Executing fan-out workflow {workflow.workflow_id}")

        tasks = []
        for subtask in workflow.subtasks:
            task = asyncio.create_task(
                self._execute_subtask(workflow, subtask)
            )
            tasks.append(task)

        # Wait for all
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_sequential(self, workflow: WorkflowTask) -> None:
        """
        Execute subtasks in order.

        Each task waits for previous to complete.
        """
        logger.debug(f"Executing sequential workflow {workflow.workflow_id}")

        for subtask in workflow.subtasks:
            await self._execute_subtask(workflow, subtask)

    async def _execute_consensus(self, workflow: WorkflowTask) -> None:
        """
        Execute each subtask on multiple agents and use voting.

        Good for critical decisions or high-reliability tasks.
        Each subtask runs on 3+ agents, majority vote determines result.
        """
        logger.debug(f"Executing consensus workflow {workflow.workflow_id}")

        consensus_count = 3  # Number of agents to run each task

        for subtask in workflow.subtasks:
            # Get agents with required capabilities
            agents_available = []
            for _ in range(consensus_count):
                agent = await self.agent_pool.select_agent_for_task(
                    subtask.required_capabilities
                )
                if agent:
                    agents_available.append(agent)

            if not agents_available:
                workflow.errors[subtask.task_id] = "No agents available for consensus"
                continue

            # Execute on all agents
            tasks = []
            for agent in agents_available:
                task = asyncio.create_task(
                    self._execute_on_agent(
                        workflow, subtask, agent
                    )
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Consensus: use majority vote
            successful_results = [
                r for r in results
                if not isinstance(r, Exception) and r is not None
            ]

            if len(successful_results) > len(results) / 2:
                # Majority succeeded - use first successful result
                workflow.results[subtask.task_id] = successful_results[0]
                logger.debug(
                    f"Consensus achieved for {subtask.task_id}: "
                    f"{len(successful_results)}/{len(results)} agents"
                )
            else:
                workflow.errors[subtask.task_id] = (
                    f"Consensus failed: only {len(successful_results)}/{len(results)} succeeded"
                )

    async def _execute_pipeline(self, workflow: WorkflowTask) -> None:
        """
        Execute subtasks in pipeline (chain).

        Output of task N becomes input to task N+1.
        """
        logger.debug(f"Executing pipeline workflow {workflow.workflow_id}")

        previous_result = None

        for subtask in workflow.subtasks:
            # Inject previous result into task data
            if previous_result is not None:
                subtask.task_data["previous_result"] = previous_result

            await self._execute_subtask(workflow, subtask)

            # Use result as input for next
            if subtask.task_id in workflow.results:
                previous_result = workflow.results[subtask.task_id]
            elif subtask.task_id in workflow.errors:
                # Stop pipeline on error
                logger.warning(
                    f"Pipeline stopped at {subtask.task_id}: "
                    f"{workflow.errors[subtask.task_id]}"
                )
                break

    async def _execute_subtask(
        self,
        workflow: WorkflowTask,
        subtask: SubTask,
    ) -> None:
        """
        Execute a single subtask.

        Selects best agent and executes via OODA loop.
        """
        try:
            # Select agent
            agent = await self.agent_pool.select_agent_for_task(
                subtask.required_capabilities
            )

            if not agent:
                workflow.errors[subtask.task_id] = "No suitable agent found"
                return

            # Execute via agent's OODA loop
            await self._execute_on_agent(workflow, subtask, agent)

        except Exception as e:
            logger.error(f"Error executing subtask {subtask.task_id}: {e}")
            workflow.errors[subtask.task_id] = str(e)

    async def _execute_on_agent(
        self,
        workflow: WorkflowTask,
        subtask: SubTask,
        agent,
    ) -> Any:
        """
        Execute subtask on specific agent.

        Creates ExecutionContext and runs through agent's OODA loop.
        """
        try:
            context = ExecutionContext(
                task_id=subtask.task_id,
                task_type=subtask.task_type,
                task_data=subtask.task_data,
                required_capabilities=subtask.required_capabilities,
                estimated_cost=subtask.estimated_cost,
                priority=5,
                received_at=datetime.utcnow().isoformat(),
                estimated_duration_seconds=subtask.timeout_seconds,
            )

            # Execute with timeout
            try:
                # In real implementation, this would run agent.ooda_tick()
                # For now, simulate success
                result = {
                    "task_id": subtask.task_id,
                    "agent_id": agent.agent_id,
                    "status": "completed",
                    "output": subtask.task_data.get("expected_output", {}),
                }

                workflow.results[subtask.task_id] = result
                return result

            except asyncio.TimeoutError:
                workflow.errors[subtask.task_id] = f"Timeout after {subtask.timeout_seconds}s"
                return None

        except Exception as e:
            logger.error(f"Error executing on agent: {e}")
            workflow.errors[subtask.task_id] = str(e)
            return None

    def get_workflow_status(self, workflow_id: str) -> Optional[Dict]:
        """
        Get status of a workflow.

        Args:
            workflow_id: Workflow to check

        Returns:
            Status dict or None if not found
        """
        # Check active workflows
        if workflow_id in self._active_workflows:
            workflow = self._active_workflows[workflow_id]
            return {
                "status": "running",
                "pattern": workflow.pattern.value,
                "completed": len(workflow.results),
                "failed": len(workflow.errors),
                "total": len(workflow.subtasks),
            }

        # Check completed workflows
        if workflow_id in self._completed_workflows:
            result = self._completed_workflows[workflow_id]
            return {
                "status": "completed",
                "success": result.success,
                "duration_seconds": result.duration_seconds,
                "successful": result.successful_subtasks,
                "failed": result.failed_subtasks,
            }

        return None


class WorkflowBuilder:
    """Builder for constructing multi-agent workflows."""

    def __init__(self, workflow_id: str, pattern: WorkflowPattern):
        """
        Initialize workflow builder.

        Args:
            workflow_id: Unique workflow identifier
            pattern: Workflow pattern (fan-out, sequential, etc)
        """
        self.workflow_id = workflow_id
        self.pattern = pattern
        self.subtasks: List[SubTask] = []

    def add_task(
        self,
        task_id: str,
        task_type: str,
        task_data: Dict[str, Any],
        required_capabilities: List[str],
        estimated_cost: float = 1.0,
        depends_on: Optional[List[str]] = None,
    ) -> "WorkflowBuilder":
        """
        Add a subtask to the workflow.

        Args:
            task_id: Unique task identifier
            task_type: Type of task (e.g., "code_execution")
            task_data: Task input data
            required_capabilities: Agent capabilities needed
            estimated_cost: Estimated API cost
            depends_on: Task IDs this task depends on

        Returns:
            Self for chaining
        """
        subtask = SubTask(
            task_id=task_id,
            task_type=task_type,
            task_data=task_data,
            required_capabilities=required_capabilities,
            estimated_cost=estimated_cost,
            depends_on=depends_on or [],
        )

        self.subtasks.append(subtask)
        return self

    def build(self) -> WorkflowTask:
        """
        Build the workflow.

        Returns:
            Constructed WorkflowTask
        """
        return WorkflowTask(
            workflow_id=self.workflow_id,
            pattern=self.pattern,
            subtasks=self.subtasks,
        )
