"""
Advanced Task Group Management System.

Provides high-level orchestration for task groups including:
- Managed group execution (wait for completion)
- Batch result aggregation
- Status monitoring
- Analytics and reporting
- Export capabilities
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
from pydantic import BaseModel, Field

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.models import TaskGroupResponse, TaskRun

logger = logging.getLogger(__name__)


class GroupExecutionStatus(str, Enum):
    """Overall group execution status."""

    PENDING = "pending"  # Not yet started
    RUNNING = "running"  # Tasks in progress
    COMPLETED = "completed"  # All tasks finished
    PARTIAL_FAILURE = "partial_failure"  # Some tasks failed
    FAILED = "failed"  # All tasks failed
    TIMEOUT = "timeout"  # Exceeded time limit


@dataclass
class TaskGroupMetrics:
    """Aggregated metrics for a task group."""

    total_runs: int = 0
    completed_runs: int = 0
    failed_runs: int = 0
    running_runs: int = 0
    queued_runs: int = 0
    cancelled_runs: int = 0

    total_duration: float = 0.0  # Total time across all runs
    avg_duration: float = 0.0
    min_duration: float = 0.0
    max_duration: float = 0.0

    success_rate: float = 0.0
    estimated_total_cost: float = 0.0

    status_counts: Dict[str, int] = field(default_factory=dict)
    processor_distribution: Dict[str, int] = field(default_factory=dict)

    def calculate_derived_metrics(self):
        """Calculate derived metrics."""
        if self.total_runs > 0:
            self.success_rate = self.completed_runs / self.total_runs

        if self.completed_runs > 0 and self.total_duration > 0:
            self.avg_duration = self.total_duration / self.completed_runs


class ManagedGroupResult(BaseModel):
    """Result of managed group execution."""

    taskgroup_id: str
    final_status: GroupExecutionStatus
    metrics: Dict[str, Any]
    run_results: List[Dict[str, Any]] = Field(default_factory=list)
    elapsed_seconds: float
    summary: str
    error: Optional[str] = None


class TaskGroupManager:
    """
    Advanced task group management.

    Features:
    - Wait for group completion
    - Aggregate results
    - Monitor status
    - Generate analytics
    """

    def __init__(self, client: httpx.AsyncClient):
        """
        Initialize manager.

        Args:
            client: HTTP client for API calls
        """
        self.client = client

    async def wait_for_completion(
        self,
        taskgroup_id: str,
        poll_interval: float = 2.0,
        timeout: int = 3600,
        include_results: bool = True
    ) -> ManagedGroupResult:
        """
        Wait for all tasks in group to complete.

        Args:
            taskgroup_id: Task group ID
            poll_interval: Seconds between status checks
            timeout: Maximum wait time in seconds
            include_results: Whether to fetch individual run results

        Returns:
            Managed group result with aggregated data
        """
        start_time = time.time()
        logger.info(f"Waiting for task group {taskgroup_id} to complete (timeout={timeout}s)")

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.error(f"Task group {taskgroup_id} timed out after {elapsed:.2f}s")
                return ManagedGroupResult(
                    taskgroup_id=taskgroup_id,
                    final_status=GroupExecutionStatus.TIMEOUT,
                    metrics={},
                    elapsed_seconds=elapsed,
                    summary=f"Group execution timed out after {elapsed:.2f}s",
                    error=f"Exceeded timeout of {timeout}s"
                )

            # Get current status
            group_status = await self._get_group_status(taskgroup_id)

            # Check if all tasks are done
            if not group_status.is_active:
                logger.info(f"Task group {taskgroup_id} completed after {elapsed:.2f}s")

                # Collect results if requested
                run_results = []
                if include_results:
                    run_results = await self._collect_all_results(taskgroup_id)

                # Calculate metrics
                metrics = self._calculate_metrics(group_status, run_results)

                # Determine final status
                final_status = self._determine_final_status(metrics)

                # Generate summary
                summary = self._generate_summary(metrics, elapsed)

                return ManagedGroupResult(
                    taskgroup_id=taskgroup_id,
                    final_status=final_status,
                    metrics=metrics,
                    run_results=run_results,
                    elapsed_seconds=elapsed,
                    summary=summary
                )

            # Wait before next check
            await asyncio.sleep(poll_interval)

    async def get_group_analytics(
        self,
        taskgroup_id: str,
        include_run_details: bool = False
    ) -> Dict[str, Any]:
        """
        Get comprehensive analytics for a task group.

        Args:
            taskgroup_id: Task group ID
            include_run_details: Include individual run data

        Returns:
            Analytics dictionary
        """
        group_status = await self._get_group_status(taskgroup_id)

        analytics = {
            "taskgroup_id": taskgroup_id,
            "is_active": group_status.is_active,
            "created_at": group_status.created_at,
            "modified_at": group_status.modified_at,
            "total_runs": group_status.num_task_runs,
            "status_distribution": group_status.task_run_status_counts,
        }

        # Calculate percentages
        if group_status.num_task_runs > 0:
            analytics["completion_rate"] = (
                group_status.task_run_status_counts.get("completed", 0) / group_status.num_task_runs
            )
            analytics["failure_rate"] = (
                group_status.task_run_status_counts.get("failed", 0) / group_status.num_task_runs
            )

        if include_run_details:
            # Fetch all run details
            runs = await self._collect_all_results(taskgroup_id)
            analytics["runs"] = runs
            analytics["run_count"] = len(runs)

        return analytics

    async def export_group_results(
        self,
        taskgroup_id: str,
        format: str = "json",
        include_inputs: bool = True,
        include_outputs: bool = True
    ) -> str:
        """
        Export task group results in various formats.

        Args:
            taskgroup_id: Task group ID
            format: Export format (json, csv, markdown)
            include_inputs: Include task inputs
            include_outputs: Include task outputs

        Returns:
            Exported data as string
        """
        runs = await self._collect_all_results(taskgroup_id, include_inputs, include_outputs)

        if format == "json":
            return json.dumps(runs, indent=2)

        elif format == "csv":
            return self._export_csv(runs)

        elif format == "markdown":
            return self._export_markdown(runs, taskgroup_id)

        else:
            raise ValueError(f"Unsupported export format: {format}")

    async def _get_group_status(self, taskgroup_id: str) -> TaskGroupResponse:
        """Get task group status."""
        response = await self.client.get(
            f"/v1beta/tasks/groups/{taskgroup_id}",
            headers=get_auth_headers()
        )
        response.raise_for_status()
        return TaskGroupResponse(**response.json())

    async def _collect_all_results(
        self,
        taskgroup_id: str,
        include_inputs: bool = True,
        include_outputs: bool = True
    ) -> List[Dict[str, Any]]:
        """Collect all run results from a group."""
        results = []

        # Stream all runs
        params = {
            "include_input": str(include_inputs).lower(),
            "include_output": str(include_outputs).lower()
        }

        async with self.client.stream(
            "GET",
            f"/v1beta/tasks/groups/{taskgroup_id}/runs",
            params=params,
            headers=get_auth_headers()
        ) as response:
            response.raise_for_status()

            async for line in response.aiter_lines():
                if line.startswith("data:"):
                    try:
                        data = json.loads(line[5:])
                        results.append(data)
                    except json.JSONDecodeError:
                        pass

        return results

    def _calculate_metrics(
        self,
        group_status: TaskGroupResponse,
        run_results: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate aggregated metrics."""
        metrics = {
            "total_runs": group_status.num_task_runs,
            "status_counts": group_status.task_run_status_counts,
            "completed": group_status.task_run_status_counts.get("completed", 0),
            "failed": group_status.task_run_status_counts.get("failed", 0),
            "running": group_status.task_run_status_counts.get("running", 0),
            "queued": group_status.task_run_status_counts.get("queued", 0),
        }

        if group_status.num_task_runs > 0:
            metrics["success_rate"] = metrics["completed"] / group_status.num_task_runs
            metrics["failure_rate"] = metrics["failed"] / group_status.num_task_runs

        # Analyze run results if available
        if run_results:
            processors = {}
            for run in run_results:
                processor = run.get("processor", "unknown")
                processors[processor] = processors.get(processor, 0) + 1

            metrics["processor_distribution"] = processors

        return metrics

    def _determine_final_status(self, metrics: Dict[str, Any]) -> GroupExecutionStatus:
        """Determine final group status from metrics."""
        total = metrics["total_runs"]
        completed = metrics["completed"]
        failed = metrics["failed"]

        if total == 0:
            return GroupExecutionStatus.PENDING

        if completed == total:
            return GroupExecutionStatus.COMPLETED

        if failed == total:
            return GroupExecutionStatus.FAILED

        if completed + failed == total:
            return GroupExecutionStatus.PARTIAL_FAILURE

        return GroupExecutionStatus.RUNNING

    def _generate_summary(self, metrics: Dict[str, Any], elapsed: float) -> str:
        """Generate human-readable summary."""
        total = metrics["total_runs"]
        completed = metrics["completed"]
        failed = metrics["failed"]

        summary_parts = [
            f"Processed {total} task runs in {elapsed:.2f}s",
            f"✓ {completed} completed",
        ]

        if failed > 0:
            summary_parts.append(f"✗ {failed} failed")

        if metrics.get("success_rate"):
            summary_parts.append(f"Success rate: {metrics['success_rate']*100:.1f}%")

        return " | ".join(summary_parts)

    def _export_csv(self, runs: List[Dict[str, Any]]) -> str:
        """Export runs to CSV format."""
        if not runs:
            return "run_id,status,processor\n"

        # Build CSV
        lines = ["run_id,status,processor,created_at,completed_at"]

        for run in runs:
            lines.append(
                f"{run.get('run_id', 'N/A')},"
                f"{run.get('status', 'N/A')},"
                f"{run.get('processor', 'N/A')},"
                f"{run.get('created_at', 'N/A')},"
                f"{run.get('completed_at', 'N/A')}"
            )

        return "\n".join(lines)

    def _export_markdown(self, runs: List[Dict[str, Any]], taskgroup_id: str) -> str:
        """Export runs to Markdown format."""
        lines = [
            f"# Task Group Results: {taskgroup_id}",
            "",
            f"**Total Runs:** {len(runs)}",
            "",
            "## Individual Runs",
            ""
        ]

        for i, run in enumerate(runs, 1):
            lines.extend([
                f"### Run {i}: {run.get('run_id', 'N/A')}",
                "",
                f"- **Status:** {run.get('status', 'N/A')}",
                f"- **Processor:** {run.get('processor', 'N/A')}",
                f"- **Created:** {run.get('created_at', 'N/A')}",
                ""
            ])

            if run.get("output"):
                lines.extend([
                    "**Output:**",
                    "```",
                    str(run["output"])[:500],
                    "```",
                    ""
                ])

        return "\n".join(lines)
