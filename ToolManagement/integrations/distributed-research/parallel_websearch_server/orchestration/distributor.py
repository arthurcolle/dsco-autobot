"""
Task Distribution System

Intelligent load balancing and routing for parallel task execution.
Features:
- Dynamic processor selection based on load and capacity
- Smart batching to optimize throughput
- Backpressure handling
- Health monitoring
- Queue management with priority
"""

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable, Awaitable
from enum import Enum
import hashlib
import logging

logger = logging.getLogger(__name__)


class ProcessorHealth(str, Enum):
    """Processor health states"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    OFFLINE = "offline"


@dataclass
class ProcessorMetrics:
    """Real-time processor performance metrics"""
    processor_tier: str
    total_assigned: int = 0
    total_completed: int = 0
    total_failed: int = 0
    current_load: int = 0  # Number of tasks currently running
    avg_execution_time: float = 0.0  # Moving average in seconds
    success_rate: float = 1.0
    last_failure_time: Optional[float] = None
    health: ProcessorHealth = ProcessorHealth.HEALTHY
    capacity: int = 100  # Max concurrent tasks

    @property
    def utilization(self) -> float:
        """Current utilization percentage (0-1)"""
        return self.current_load / self.capacity if self.capacity > 0 else 0.0

    @property
    def availability(self) -> float:
        """Available capacity percentage (0-1)"""
        return 1.0 - self.utilization

    def update_execution_time(self, duration: float, alpha: float = 0.2):
        """Update moving average execution time"""
        if self.avg_execution_time == 0:
            self.avg_execution_time = duration
        else:
            # Exponential moving average
            self.avg_execution_time = alpha * duration + (1 - alpha) * self.avg_execution_time

    def update_success_rate(self, success: bool, alpha: float = 0.1):
        """Update moving average success rate"""
        current_rate = 1.0 if success else 0.0
        self.success_rate = alpha * current_rate + (1 - alpha) * self.success_rate


@dataclass
class DistributionTask:
    """Task with routing metadata"""
    task_id: str
    input_data: str
    processor_tier: Optional[str] = None  # If None, distributor selects
    priority: int = 5  # 1-10, higher = more important
    complexity_score: int = 50  # 0-100
    batch_id: Optional[str] = None
    submitted_at: float = field(default_factory=time.time)
    assigned_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def wait_time(self) -> float:
        """Time spent waiting in queue"""
        end = self.assigned_at or time.time()
        return end - self.submitted_at

    @property
    def execution_time(self) -> Optional[float]:
        """Time spent executing"""
        if self.assigned_at and self.completed_at:
            return self.completed_at - self.assigned_at
        return None


@dataclass
class BatchConfig:
    """Batching configuration"""
    min_batch_size: int = 1
    max_batch_size: int = 50
    max_wait_time: float = 5.0  # Max seconds to wait for batch to fill
    batch_by_processor: bool = True  # Group tasks for same processor
    batch_by_priority: bool = True  # Don't mix priorities


class TaskDistributor:
    """
    Intelligent task distribution and load balancing system.

    Routes tasks to processors based on:
    - Processor availability and load
    - Task complexity and requirements
    - Historical performance metrics
    - Priority and batching constraints
    """

    def __init__(
        self,
        processor_tiers: List[str],
        batch_config: Optional[BatchConfig] = None,
        max_queue_size: int = 10000
    ):
        self.processor_tiers = processor_tiers
        self.batch_config = batch_config or BatchConfig()
        self.max_queue_size = max_queue_size

        # Processor tracking
        self.processor_metrics: Dict[str, ProcessorMetrics] = {
            tier: ProcessorMetrics(processor_tier=tier)
            for tier in processor_tiers
        }

        # Queue management - separate queues per priority level
        self.queues: Dict[int, deque] = defaultdict(deque)  # priority -> tasks
        self.queue_size = 0

        # Batch management
        self.pending_batches: Dict[str, List[DistributionTask]] = defaultdict(list)
        self.batch_timers: Dict[str, float] = {}

        # Active task tracking
        self.active_tasks: Dict[str, DistributionTask] = {}
        self.processor_assignments: Dict[str, Set[str]] = defaultdict(set)  # processor -> task_ids

        # Locks for thread safety
        self._queue_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()

    async def submit_task(self, task: DistributionTask) -> str:
        """
        Submit task for distribution.

        Returns:
            task_id for tracking

        Raises:
            ValueError if queue is full
        """
        async with self._queue_lock:
            if self.queue_size >= self.max_queue_size:
                raise ValueError(f"Queue full: {self.queue_size}/{self.max_queue_size}")

            # Add to priority queue
            self.queues[task.priority].append(task)
            self.queue_size += 1

            logger.info(f"Task {task.task_id} submitted to queue (priority={task.priority}, queue_size={self.queue_size})")

        return task.task_id

    async def get_next_batch(
        self,
        processor_tier: Optional[str] = None,
        max_batch_size: Optional[int] = None
    ) -> List[DistributionTask]:
        """
        Get next batch of tasks to execute.

        Args:
            processor_tier: If specified, only return tasks for this processor
            max_batch_size: Override default batch size

        Returns:
            List of tasks to execute together
        """
        batch_size = max_batch_size or self.batch_config.max_batch_size
        batch = []

        async with self._queue_lock:
            # Process queues in priority order (highest first)
            for priority in sorted(self.queues.keys(), reverse=True):
                queue = self.queues[priority]

                while queue and len(batch) < batch_size:
                    task = queue.popleft()
                    self.queue_size -= 1

                    # Select processor if not specified
                    if task.processor_tier is None:
                        task.processor_tier = await self._select_processor(task)

                    # Filter by processor if requested
                    if processor_tier and task.processor_tier != processor_tier:
                        # Put back in queue
                        queue.append(task)
                        self.queue_size += 1
                        continue

                    # Check if we should batch with different processors
                    if batch and self.batch_config.batch_by_processor:
                        if batch[0].processor_tier != task.processor_tier:
                            # Put back and return current batch
                            queue.append(task)
                            self.queue_size += 1
                            break

                    # Check priority mixing
                    if batch and self.batch_config.batch_by_priority:
                        if batch[0].priority != task.priority:
                            queue.append(task)
                            self.queue_size += 1
                            break

                    task.assigned_at = time.time()
                    batch.append(task)
                    self.active_tasks[task.task_id] = task

                    # Track assignment
                    if task.processor_tier:
                        async with self._metrics_lock:
                            self.processor_assignments[task.processor_tier].add(task.task_id)
                            self.processor_metrics[task.processor_tier].current_load += 1
                            self.processor_metrics[task.processor_tier].total_assigned += 1

                # Clean up empty queues
                if not queue:
                    del self.queues[priority]

                if batch:
                    break  # Return first batch found

        if batch:
            processor = batch[0].processor_tier or "auto"
            logger.info(f"Dispatching batch: {len(batch)} tasks for processor={processor}, priority={batch[0].priority}")

        return batch

    async def _select_processor(self, task: DistributionTask) -> str:
        """
        Intelligent processor selection based on:
        - Task complexity
        - Processor availability
        - Historical performance
        - Load balancing
        """
        async with self._metrics_lock:
            # Score each processor
            scores = {}

            for tier, metrics in self.processor_metrics.items():
                if metrics.health == ProcessorHealth.OFFLINE:
                    continue

                score = 0.0

                # Availability (0-40 points)
                score += metrics.availability * 40

                # Health status (0-20 points)
                health_score = {
                    ProcessorHealth.HEALTHY: 20,
                    ProcessorHealth.DEGRADED: 10,
                    ProcessorHealth.UNHEALTHY: 0,
                    ProcessorHealth.OFFLINE: -100
                }
                score += health_score[metrics.health]

                # Success rate (0-20 points)
                score += metrics.success_rate * 20

                # Execution speed (0-20 points)
                # Prefer faster processors for high-priority tasks
                if metrics.avg_execution_time > 0:
                    # Inverse relationship: faster = higher score
                    speed_score = 20 / (1 + metrics.avg_execution_time / 60)  # Normalize to ~1min
                    score += speed_score

                # Complexity matching (bonus points)
                # Match task complexity to processor tier capabilities
                tier_complexity = {
                    "lite": 20, "base": 35, "core": 55,
                    "pro": 70, "ultra": 85, "ultra2x": 95
                }
                tier_score = tier_complexity.get(tier, 50)
                complexity_match = 1.0 - abs(tier_score - task.complexity_score) / 100
                score += complexity_match * 10

                scores[tier] = score

            if not scores:
                # Fallback to default
                return "lite"

            # Select processor with highest score
            selected = max(scores.items(), key=lambda x: x[1])[0]

            logger.debug(f"Processor selection for task {task.task_id}: scores={scores}, selected={selected}")

            return selected

    async def report_completion(
        self,
        task_id: str,
        success: bool,
        execution_time: Optional[float] = None
    ):
        """Report task completion and update metrics"""
        async with self._queue_lock:
            task = self.active_tasks.pop(task_id, None)
            if not task:
                logger.warning(f"Completion reported for unknown task: {task_id}")
                return

            task.completed_at = time.time()

            processor = task.processor_tier
            if not processor:
                return

        async with self._metrics_lock:
            metrics = self.processor_metrics.get(processor)
            if not metrics:
                return

            # Update metrics
            metrics.current_load = max(0, metrics.current_load - 1)
            self.processor_assignments[processor].discard(task_id)

            if success:
                metrics.total_completed += 1
            else:
                metrics.total_failed += 1
                metrics.last_failure_time = time.time()

            # Update moving averages
            if execution_time:
                metrics.update_execution_time(execution_time)
            elif task.execution_time:
                metrics.update_execution_time(task.execution_time)

            metrics.update_success_rate(success)

            # Update health status based on success rate
            if metrics.success_rate < 0.5:
                metrics.health = ProcessorHealth.UNHEALTHY
            elif metrics.success_rate < 0.8:
                metrics.health = ProcessorHealth.DEGRADED
            else:
                metrics.health = ProcessorHealth.HEALTHY

            logger.info(
                f"Task {task_id} completed on {processor}: "
                f"success={success}, "
                f"exec_time={task.execution_time:.2f}s, "
                f"success_rate={metrics.success_rate:.2%}, "
                f"health={metrics.health}"
            )

    async def get_metrics_summary(self) -> Dict:
        """Get comprehensive metrics across all processors"""
        async with self._metrics_lock:
            summary = {
                "queue_size": self.queue_size,
                "active_tasks": len(self.active_tasks),
                "processors": {}
            }

            for tier, metrics in self.processor_metrics.items():
                summary["processors"][tier] = {
                    "health": metrics.health,
                    "current_load": metrics.current_load,
                    "utilization": metrics.utilization,
                    "total_assigned": metrics.total_assigned,
                    "total_completed": metrics.total_completed,
                    "total_failed": metrics.total_failed,
                    "success_rate": metrics.success_rate,
                    "avg_execution_time": metrics.avg_execution_time
                }

            return summary

    async def apply_backpressure(self) -> bool:
        """
        Check if system should apply backpressure.

        Returns:
            True if new submissions should be throttled
        """
        # Check queue size
        if self.queue_size > self.max_queue_size * 0.9:
            return True

        # Check processor health
        async with self._metrics_lock:
            healthy_count = sum(
                1 for m in self.processor_metrics.values()
                if m.health in (ProcessorHealth.HEALTHY, ProcessorHealth.DEGRADED)
            )

            if healthy_count == 0:
                return True

            # Check overall utilization
            avg_utilization = sum(
                m.utilization for m in self.processor_metrics.values()
            ) / len(self.processor_metrics)

            if avg_utilization > 0.95:
                return True

        return False

    async def set_processor_capacity(self, processor_tier: str, capacity: int):
        """Update processor capacity"""
        async with self._metrics_lock:
            if processor_tier in self.processor_metrics:
                self.processor_metrics[processor_tier].capacity = capacity
                logger.info(f"Processor {processor_tier} capacity set to {capacity}")

    async def set_processor_health(self, processor_tier: str, health: ProcessorHealth):
        """Manually set processor health status"""
        async with self._metrics_lock:
            if processor_tier in self.processor_metrics:
                self.processor_metrics[processor_tier].health = health
                logger.info(f"Processor {processor_tier} health set to {health}")
