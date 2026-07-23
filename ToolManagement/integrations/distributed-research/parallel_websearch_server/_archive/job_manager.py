"""Job Manager - Handles job submission, queuing, and execution lifecycle."""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import uuid4
from collections import defaultdict
import logging

from parallel_websearch_server.matching_models import (
    JobInput,
    JobStatus,
    JobSubmission,
    JobResult,
    ExecutionStatus,
    ExecutionBackend,
    ExecutionRequest
)
from parallel_websearch_server.mailbox import get_mailbox_manager
from parallel_websearch_server.matching_engine import MatchingEngine
from parallel_websearch_server.worker_registry import WorkerRegistry, get_registry

logger = logging.getLogger(__name__)


class Job:
    """Internal job representation with full state."""

    def __init__(
        self,
        job_id: str,
        input_spec: JobInput,
        worker_id: Optional[str] = None,
        status: JobStatus = JobStatus.PENDING
    ):
        self.job_id = job_id
        self.input_spec = input_spec
        self.worker_id = worker_id
        self.status = status
        self.created_at = datetime.now()
        self.matched_at: Optional[datetime] = None
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.result: Optional[JobResult] = None
        self.error: Optional[str] = None
        self.progress_pct: float = 0.0
        self.current_step: Optional[str] = None


class JobManager:
    """
    Manages the complete job lifecycle:
    1. Job submission and validation
    2. Matching to optimal workers
    3. Queue management
    4. Execution coordination
    5. Result aggregation
    6. Status tracking
    """

    def __init__(self, registry: Optional[WorkerRegistry] = None):
        """
        Initialize job manager.

        Args:
            registry: WorkerRegistry instance (creates new one if not provided)
        """
        self.registry = registry or get_registry()
        self.matching_engine = MatchingEngine(self.registry)
        self.mailbox = get_mailbox_manager()

        # Job storage
        self.jobs: Dict[str, Job] = {}
        self.job_queue: asyncio.Queue = asyncio.Queue()

        # Status tracking
        self.jobs_by_status: Dict[JobStatus, List[str]] = defaultdict(list)

        # Execution tracking
        self.active_executions: Dict[str, asyncio.Task] = {}

        # Stats
        self.total_submitted = 0
        self.total_completed = 0
        self.total_failed = 0

        logger.info("JobManager initialized with %d workers", len(self.registry.agents))

    async def submit_job(self, job_input: JobInput) -> JobSubmission:
        """
        Submit a new job.

        Args:
            job_input: Job specification

        Returns:
            JobSubmission with job_id and initial status
        """
        # Generate unique job ID
        job_id = f"job_{uuid4().hex[:12]}"

        # Create job
        job = Job(job_id=job_id, input_spec=job_input, status=JobStatus.PENDING)
        self.jobs[job_id] = job
        self._update_status_index(job_id, JobStatus.PENDING)

        self.total_submitted += 1

        logger.info(f"Job submitted: {job_id} (type: {job_input.job_type})")

        # Match to worker
        try:
            match_result = self.matching_engine.match_job(job_input, job_id)
            job.worker_id = match_result.selected_worker_id
            job.status = JobStatus.MATCHED
            job.matched_at = datetime.now()
            self._update_status_index(job_id, JobStatus.MATCHED)

            logger.info(f"Job {job_id} matched to worker {job.worker_id}")

            # Add to execution queue
            await self.job_queue.put(job_id)
            job.status = JobStatus.QUEUED
            self._update_status_index(job_id, JobStatus.QUEUED)

            return JobSubmission(
                job_id=job_id,
                status=job.status,
                created_at=job.created_at,
                estimated_cost_usd=match_result.estimated_cost_usd,
                estimated_duration_seconds=match_result.estimated_duration_seconds,
                matched_workers=[match_result.selected_worker_id]
            )

        except Exception as e:
            logger.error(f"Failed to match job {job_id}: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            self._update_status_index(job_id, JobStatus.FAILED)
            self.total_failed += 1

            return JobSubmission(
                job_id=job_id,
                status=job.status,
                created_at=job.created_at
            )

    async def execute_job(self, job_id: str) -> JobResult:
        """
        Execute a job.

        This is the main execution coordinator that:
        1. Gets the job and worker details
        2. Increments worker load
        3. Routes to appropriate backend
        4. Executes the job
        5. Collects results
        6. Updates job status
        7. Decrements worker load

        Args:
            job_id: Job identifier

        Returns:
            JobResult with execution outcome
        """
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        if not job.worker_id:
            raise ValueError(f"Job {job_id} has no assigned worker")

        # Get worker
        worker = self.registry.get_agent(job.worker_id)
        if not worker:
            raise ValueError(f"Worker not found: {job.worker_id}")

        # Increment worker load
        if not self.registry.increment_load(job.worker_id):
            raise ValueError(f"Worker {job.worker_id} at capacity")

        try:
            # Update status
            job.status = JobStatus.IN_PROGRESS
            job.started_at = datetime.now()
            self._update_status_index(job_id, JobStatus.IN_PROGRESS)

            logger.info(f"Executing job {job_id} on worker {job.worker_id} ({worker.backend_type})")

            # Route to appropriate backend
            backend = ExecutionBackend(worker.backend_type)

            if backend == ExecutionBackend.PARALLEL:
                result = await self._execute_parallel(job, worker)
            elif backend == ExecutionBackend.ANTHROPIC:
                result = await self._execute_anthropic(job, worker)
            elif backend == ExecutionBackend.CUSTOM_MCP:
                result = await self._execute_mcp(job, worker)
            else:
                raise ValueError(f"Unsupported backend: {backend}")

            # Update job with result
            job.result = result
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now()
            self._update_status_index(job_id, JobStatus.COMPLETED)
            self.total_completed += 1

            logger.info(f"Job {job_id} completed successfully")

            await self._notify_job_event(
                job,
                event_type="job.completed",
                payload={
                    "status": job.status.value,
                    "worker_id": job.worker_id,
                    "result": result.model_dump(),
                },
            )

            return result

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            job.completed_at = datetime.now()
            self._update_status_index(job_id, JobStatus.FAILED)
            self.total_failed += 1

            # Create error result
            failure_result = JobResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                output={"error": str(e)},
                worker_id=job.worker_id,
                completed_at=datetime.now()
            )
            job.result = failure_result

            await self._notify_job_event(
                job,
                event_type="job.failed",
                payload={
                    "status": job.status.value,
                    "worker_id": job.worker_id,
                    "error": job.error,
                },
            )

            return failure_result

        finally:
            # Always decrement worker load
            self.registry.decrement_load(job.worker_id)

    async def _execute_parallel(self, job: Job, worker: Any) -> JobResult:
        """
        Execute job using Parallel.ai backend.

        Uses the existing parallel_websearch_server client to run tasks.
        """
        from parallel_websearch_server.client import ParallelClient
        from parallel_websearch_server.config import settings
        from parallel_websearch_server.models import TaskRunInput, ProcessorType

        client = ParallelClient(
            api_key=settings.api_key,
            base_url=settings.api_base_url
        )

        # Map job to Parallel.ai task
        default_processor_name = getattr(settings, "default_parallel_processor", None) or ProcessorType.LITE.value
        try:
            default_processor = ProcessorType(default_processor_name)
        except ValueError:
            logger.warning(
                "Configured default processor '%s' is invalid, falling back to %s",
                default_processor_name,
                ProcessorType.LITE.value,
            )
            default_processor = ProcessorType.LITE

        processor_name = worker.backend_model or default_processor.value
        try:
            processor = ProcessorType(processor_name)
        except ValueError:
            logger.warning(
                "Worker %s reported unsupported processor '%s', defaulting to %s",
                worker.worker_id,
                processor_name,
                default_processor.value,
            )
            processor = default_processor

        task_input = TaskRunInput(
            processor=processor,
            input=job.input_spec.description,
            task_spec=job.input_spec.output_schema
        )

        # Submit task
        task_run = await client.create_task_run(task_input)

        # Poll for completion (in production, would use webhooks/SSE)
        while True:
            status = await client.get_task_run(task_run.run.run_id)

            job.progress_pct = 50.0  # Rough estimate
            job.current_step = f"Running on {worker.name}"

            if status.run.status == "completed":
                # Extract result
                output = status.output.model_dump() if status.output else {}

                return JobResult(
                    job_id=job.job_id,
                    status=JobStatus.COMPLETED,
                    output=output,
                    worker_id=job.worker_id,
                    quality_score=worker.avg_quality_score,
                    actual_cost_usd=worker.cost_per_task_usd,
                    actual_duration_seconds=int((datetime.now() - job.started_at).total_seconds()),
                    completed_at=datetime.now()
                )

            elif status.run.status in ["failed", "cancelled"]:
                raise Exception(f"Task failed: {status.run.status}")

            await asyncio.sleep(2)  # Poll every 2 seconds

    async def _execute_anthropic(self, job: Job, worker: Any) -> JobResult:
        """
        Execute job using Anthropic Claude backend.

        This would integrate with Anthropic's API directly.
        For now, simulating execution.
        """
        # Simulate execution
        await asyncio.sleep(2)

        # In production, would call Anthropic API:
        # - Format prompt from job description
        # - Include system prompt based on worker's domain expertise
        # - Enable computer use if required
        # - Stream results

        return JobResult(
            job_id=job.job_id,
            status=JobStatus.COMPLETED,
            output={
                "result": f"[Simulated] Task completed by {worker.name}",
                "description": job.input_spec.description,
                "worker_domain": worker.domain
            },
            worker_id=job.worker_id,
            quality_score=worker.avg_quality_score,
            actual_cost_usd=worker.cost_per_task_usd,
            actual_duration_seconds=int((datetime.now() - job.started_at).total_seconds()),
            completed_at=datetime.now()
        )

    async def _execute_mcp(self, job: Job, worker: Any) -> JobResult:
        """
        Execute job using custom MCP server backend.

        This would route to specialized MCP servers for specific capabilities.
        """
        await asyncio.sleep(1)

        return JobResult(
            job_id=job.job_id,
            status=JobStatus.COMPLETED,
            output={
                "result": f"[MCP] Task completed by {worker.name}",
                "mcp_servers": worker.mcp_servers
            },
            worker_id=job.worker_id,
            quality_score=worker.avg_quality_score,
            actual_cost_usd=worker.cost_per_task_usd,
            actual_duration_seconds=int((datetime.now() - job.started_at).total_seconds()),
            completed_at=datetime.now()
        )

    async def start_worker_pool(self, num_workers: int = 10):
        """
        Start a pool of workers to process jobs from the queue.

        Args:
            num_workers: Number of concurrent workers
        """
        workers = [
            asyncio.create_task(self._job_worker(i))
            for i in range(num_workers)
        ]

        logger.info(f"Started {num_workers} job workers")

        # Wait for all workers
        await asyncio.gather(*workers)

    async def _job_worker(self, worker_id: int):
        """Background worker that processes jobs from the queue."""
        logger.info(f"Job worker {worker_id} started")

        while True:
            try:
                # Get job from queue
                job_id = await self.job_queue.get()

                # Execute job
                await self.execute_job(job_id)

                # Mark as done
                self.job_queue.task_done()

            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")

    def get_job_status(self, job_id: str) -> Optional[ExecutionStatus]:
        """Get current status of a job."""
        job = self.jobs.get(job_id)
        if not job:
            return None

        return ExecutionStatus(
            job_id=job.job_id,
            worker_id=job.worker_id or "unassigned",
            status=job.status,
            progress_pct=job.progress_pct,
            current_step=job.current_step,
            started_at=job.started_at or job.created_at,
            updated_at=datetime.now(),
            result=job.result
        )

    def get_job_result(self, job_id: str) -> Optional[JobResult]:
        """Get result of a completed job."""
        job = self.jobs.get(job_id)
        if not job:
            return None

        return job.result

    def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        limit: int = 100
    ) -> List[ExecutionStatus]:
        """
        List jobs, optionally filtered by status.

        Args:
            status: Filter by job status
            limit: Maximum number of jobs to return

        Returns:
            List of job statuses
        """
        if status:
            job_ids = self.jobs_by_status.get(status, [])
        else:
            job_ids = list(self.jobs.keys())

        # Get most recent first
        job_ids = sorted(
            job_ids,
            key=lambda jid: self.jobs[jid].created_at,
            reverse=True
        )[:limit]

        return [self.get_job_status(jid) for jid in job_ids if jid in self.jobs]

    def get_stats(self) -> Dict[str, Any]:
        """Get job manager statistics."""
        return {
            "total_submitted": self.total_submitted,
            "total_completed": self.total_completed,
            "total_failed": self.total_failed,
            "success_rate": self.total_completed / self.total_submitted if self.total_submitted > 0 else 0,
            "jobs_by_status": {
                status.value: len(job_ids)
                for status, job_ids in self.jobs_by_status.items()
            },
            "queue_size": self.job_queue.qsize(),
            "active_executions": len(self.active_executions),
            "matching_stats": self.matching_engine.get_match_stats()
        }

    def _update_status_index(self, job_id: str, new_status: JobStatus):
        """Update the status index when a job's status changes."""
        job = self.jobs.get(job_id)
        if not job:
            return

        # Remove from old status list
        for status, job_list in self.jobs_by_status.items():
            if job_id in job_list:
                job_list.remove(job_id)

        # Add to new status list
        if new_status not in self.jobs_by_status:
            self.jobs_by_status[new_status] = []
        self.jobs_by_status[new_status].append(job_id)

    async def _notify_job_event(self, job: Job, event_type: str, payload: Dict[str, Any]):
        """Send a completion/failure notification to the agent mailbox."""
        channel = getattr(job.input_spec, "notification_channel", None)
        if not channel and job.input_spec.metadata:
            channel = job.input_spec.metadata.get("notification_channel")

        if not channel:
            return

        try:
            await self.mailbox.publish(
                channel_id=channel,
                event_type=event_type,
                job_id=job.job_id,
                payload=payload,
            )
        except Exception as exc:  # pragma: no cover - resilience guard
            logger.warning(
                "Failed to publish mailbox event for job %s on channel %s: %s",
                job.job_id,
                channel,
                exc,
            )


# Global job manager instance
_global_job_manager: Optional[JobManager] = None


def get_job_manager() -> JobManager:
    """Get or create the global job manager."""
    global _global_job_manager
    if _global_job_manager is None:
        _global_job_manager = JobManager()
    return _global_job_manager
