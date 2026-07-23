"""
Real-Time Progress Tracking System

Provides live monitoring and metrics for:
- Task group execution
- Processor distribution
- Optimization metrics
- System health

Uses Server-Sent Events (SSE) for streaming updates.
"""

import asyncio
import time
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, AsyncGenerator, Set
from collections import defaultdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ProgressEventType(str, Enum):
    """Progress event types for SSE streaming"""
    TASK_SUBMITTED = "task.submitted"
    TASK_STARTED = "task.started"
    TASK_PROGRESS = "task.progress"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    BATCH_STARTED = "batch.started"
    BATCH_COMPLETED = "batch.completed"
    METRICS_UPDATE = "metrics.update"
    SYSTEM_HEALTH = "system.health"
    OPTIMIZATION_UPDATE = "optimization.update"


@dataclass
class ProgressMetrics:
    """Real-time progress metrics"""
    total_tasks: int = 0
    queued: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0

    # Performance metrics
    throughput: float = 0.0  # tasks/second
    avg_execution_time: float = 0.0  # seconds
    success_rate: float = 1.0  # 0-1

    # Time metrics
    elapsed_seconds: float = 0.0
    eta_seconds: float = 0.0

    # Cost metrics
    estimated_cost: float = 0.0
    actual_cost: float = 0.0

    @property
    def progress_percent(self) -> float:
        """Overall progress percentage"""
        if self.total_tasks == 0:
            return 0.0
        return (self.completed / self.total_tasks) * 100

    @property
    def active_tasks(self) -> int:
        """Number of tasks currently active"""
        return self.queued + self.running

    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            **asdict(self),
            "progress_percent": self.progress_percent,
            "active_tasks": self.active_tasks
        }


@dataclass
class ProgressEvent:
    """Progress update event"""
    event_type: ProgressEventType
    timestamp: float = field(default_factory=time.time)
    data: Dict = field(default_factory=dict)
    task_id: Optional[str] = None
    batch_id: Optional[str] = None

    def to_sse(self) -> str:
        """Convert to Server-Sent Event format"""
        lines = []
        lines.append(f"event: {self.event_type}")
        lines.append(f"data: {json.dumps(self.data)}")
        if self.task_id:
            lines.append(f"id: {self.task_id}")
        lines.append("")  # Empty line to terminate event
        return "\n".join(lines)


class ProgressTracker:
    """
    Real-time progress tracking with SSE streaming.

    Tracks:
    - Individual task progress
    - Batch execution metrics
    - System-wide statistics
    - Optimization metrics
    - Health indicators
    """

    def __init__(self):
        # Task tracking
        self.tasks: Dict[str, Dict] = {}  # task_id -> task info
        self.batches: Dict[str, List[str]] = {}  # batch_id -> task_ids

        # Metrics tracking
        self.metrics: Dict[str, ProgressMetrics] = defaultdict(ProgressMetrics)
        self.start_times: Dict[str, float] = {}

        # Event streaming
        self.event_queues: Dict[str, asyncio.Queue] = {}  # subscriber_id -> queue
        self.active_streams: Set[str] = set()

        # Locks
        self._lock = asyncio.Lock()

    async def register_task(
        self,
        task_id: str,
        scope: str = "global",
        batch_id: Optional[str] = None,
        metadata: Optional[Dict] = None
    ):
        """Register a new task for tracking"""
        async with self._lock:
            self.tasks[task_id] = {
                "task_id": task_id,
                "scope": scope,
                "batch_id": batch_id,
                "status": "queued",
                "submitted_at": time.time(),
                "started_at": None,
                "completed_at": None,
                "metadata": metadata or {}
            }

            # Update metrics
            self.metrics[scope].total_tasks += 1
            self.metrics[scope].queued += 1

            # Track batch
            if batch_id:
                if batch_id not in self.batches:
                    self.batches[batch_id] = []
                self.batches[batch_id].append(task_id)

            # Set start time if first task
            if scope not in self.start_times:
                self.start_times[scope] = time.time()

        # Emit event
        await self._emit_event(ProgressEvent(
            event_type=ProgressEventType.TASK_SUBMITTED,
            task_id=task_id,
            batch_id=batch_id,
            data={
                "task_id": task_id,
                "scope": scope,
                "batch_id": batch_id
            }
        ))

    async def update_task_status(
        self,
        task_id: str,
        status: str,
        progress: Optional[float] = None,
        metadata: Optional[Dict] = None
    ):
        """Update task status and emit progress event"""
        async with self._lock:
            if task_id not in self.tasks:
                logger.warning(f"Task {task_id} not found for status update")
                return

            task = self.tasks[task_id]
            old_status = task["status"]
            task["status"] = status

            scope = task["scope"]
            metrics = self.metrics[scope]

            # Update status counts
            if old_status == "queued" and status == "running":
                metrics.queued -= 1
                metrics.running += 1
                task["started_at"] = time.time()
            elif old_status == "running" and status == "completed":
                metrics.running -= 1
                metrics.completed += 1
                task["completed_at"] = time.time()
            elif old_status == "running" and status == "failed":
                metrics.running -= 1
                metrics.failed += 1
                task["completed_at"] = time.time()
            elif old_status in ("queued", "running") and status == "cancelled":
                if old_status == "queued":
                    metrics.queued -= 1
                else:
                    metrics.running -= 1
                metrics.cancelled += 1

            # Update metadata
            if metadata:
                task["metadata"].update(metadata)

            # Update performance metrics
            if status in ("completed", "failed"):
                self._update_performance_metrics(scope)

        # Determine event type
        event_type_map = {
            "running": ProgressEventType.TASK_STARTED,
            "completed": ProgressEventType.TASK_COMPLETED,
            "failed": ProgressEventType.TASK_FAILED,
        }
        event_type = event_type_map.get(status, ProgressEventType.TASK_PROGRESS)

        # Emit event
        await self._emit_event(ProgressEvent(
            event_type=event_type,
            task_id=task_id,
            batch_id=task.get("batch_id"),
            data={
                "task_id": task_id,
                "status": status,
                "progress": progress,
                "metadata": metadata or {}
            }
        ))

    def _update_performance_metrics(self, scope: str):
        """Update performance metrics for scope"""
        metrics = self.metrics[scope]

        # Calculate elapsed time
        if scope in self.start_times:
            metrics.elapsed_seconds = time.time() - self.start_times[scope]

        # Calculate throughput
        if metrics.elapsed_seconds > 0:
            metrics.throughput = metrics.completed / metrics.elapsed_seconds

        # Calculate success rate
        total_finished = metrics.completed + metrics.failed
        if total_finished > 0:
            metrics.success_rate = metrics.completed / total_finished

        # Calculate average execution time
        execution_times = []
        for task_id, task in self.tasks.items():
            if task["scope"] == scope and task["started_at"] and task["completed_at"]:
                execution_times.append(task["completed_at"] - task["started_at"])

        if execution_times:
            metrics.avg_execution_time = sum(execution_times) / len(execution_times)

        # Calculate ETA
        remaining = metrics.total_tasks - metrics.completed
        if metrics.throughput > 0:
            metrics.eta_seconds = remaining / metrics.throughput
        else:
            metrics.eta_seconds = 0.0

    async def get_metrics(self, scope: str = "global") -> ProgressMetrics:
        """Get current metrics for scope"""
        async with self._lock:
            self._update_performance_metrics(scope)
            return self.metrics[scope]

    async def get_batch_metrics(self, batch_id: str) -> Optional[ProgressMetrics]:
        """Get metrics for specific batch"""
        async with self._lock:
            if batch_id not in self.batches:
                return None

            task_ids = self.batches[batch_id]
            metrics = ProgressMetrics(total_tasks=len(task_ids))

            for task_id in task_ids:
                task = self.tasks.get(task_id)
                if not task:
                    continue

                status = task["status"]
                if status == "queued":
                    metrics.queued += 1
                elif status == "running":
                    metrics.running += 1
                elif status == "completed":
                    metrics.completed += 1
                elif status == "failed":
                    metrics.failed += 1
                elif status == "cancelled":
                    metrics.cancelled += 1

            # Calculate derived metrics
            if metrics.total_tasks > 0:
                total_finished = metrics.completed + metrics.failed
                if total_finished > 0:
                    metrics.success_rate = metrics.completed / total_finished

            return metrics

    async def subscribe(self, subscriber_id: str) -> str:
        """
        Subscribe to progress events.

        Returns:
            subscriber_id for tracking
        """
        async with self._lock:
            if subscriber_id not in self.event_queues:
                self.event_queues[subscriber_id] = asyncio.Queue(maxsize=1000)
                self.active_streams.add(subscriber_id)
                logger.info(f"Subscriber {subscriber_id} registered")

        return subscriber_id

    async def unsubscribe(self, subscriber_id: str):
        """Unsubscribe from progress events"""
        async with self._lock:
            if subscriber_id in self.event_queues:
                del self.event_queues[subscriber_id]
                self.active_streams.discard(subscriber_id)
                logger.info(f"Subscriber {subscriber_id} unregistered")

    async def stream_events(
        self,
        subscriber_id: str,
        scope: Optional[str] = None
    ) -> AsyncGenerator[str, None]:
        """
        Stream progress events as SSE.

        Args:
            subscriber_id: Unique subscriber identifier
            scope: Optional scope filter

        Yields:
            SSE-formatted event strings
        """
        await self.subscribe(subscriber_id)

        try:
            queue = self.event_queues[subscriber_id]

            # Send initial state
            metrics = await self.get_metrics(scope or "global")
            initial_event = ProgressEvent(
                event_type=ProgressEventType.METRICS_UPDATE,
                data=metrics.to_dict()
            )
            yield initial_event.to_sse()

            # Stream events
            while subscriber_id in self.active_streams:
                try:
                    # Wait for event with timeout
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)

                    # Filter by scope if specified
                    if scope and event.data.get("scope") != scope:
                        continue

                    yield event.to_sse()

                except asyncio.TimeoutError:
                    # Send keepalive comment
                    yield ": keepalive\n\n"
                    continue

        except asyncio.CancelledError:
            logger.info(f"Stream cancelled for subscriber {subscriber_id}")
        finally:
            await self.unsubscribe(subscriber_id)

    async def _emit_event(self, event: ProgressEvent):
        """Emit event to all subscribers"""
        # Don't need lock here as we're just adding to queues
        for subscriber_id, queue in list(self.event_queues.items()):
            try:
                # Non-blocking put
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(f"Queue full for subscriber {subscriber_id}, dropping event")

    async def emit_metrics_update(self, scope: str = "global"):
        """Manually emit current metrics"""
        metrics = await self.get_metrics(scope)
        await self._emit_event(ProgressEvent(
            event_type=ProgressEventType.METRICS_UPDATE,
            data=metrics.to_dict()
        ))

    async def emit_system_health(self, health_data: Dict):
        """Emit system health update"""
        await self._emit_event(ProgressEvent(
            event_type=ProgressEventType.SYSTEM_HEALTH,
            data=health_data
        ))

    async def emit_optimization_update(self, optimization_data: Dict):
        """Emit optimization metrics update"""
        await self._emit_event(ProgressEvent(
            event_type=ProgressEventType.OPTIMIZATION_UPDATE,
            data=optimization_data
        ))

    async def reset_scope(self, scope: str):
        """Reset metrics for a scope"""
        async with self._lock:
            # Remove tasks for scope
            task_ids_to_remove = [
                task_id for task_id, task in self.tasks.items()
                if task["scope"] == scope
            ]
            for task_id in task_ids_to_remove:
                del self.tasks[task_id]

            # Reset metrics
            self.metrics[scope] = ProgressMetrics()

            # Remove start time
            if scope in self.start_times:
                del self.start_times[scope]

    async def get_active_subscriber_count(self) -> int:
        """Get number of active subscribers"""
        return len(self.active_streams)


# Global tracker instance
_tracker: Optional[ProgressTracker] = None


def get_progress_tracker() -> ProgressTracker:
    """Get or create global progress tracker"""
    global _tracker
    if _tracker is None:
        _tracker = ProgressTracker()
    return _tracker
