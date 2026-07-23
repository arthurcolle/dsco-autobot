"""
GPU Task Scheduler

Manages GPU task allocation and scheduling via Modal.
Handles GPU-intensive tasks with quota management and cost tracking.
"""

import logging
import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class GPUType(Enum):
    """GPU types available for allocation."""
    H100 = "h100"          # NVIDIA H100
    A100 = "a100"          # NVIDIA A100
    L4 = "l4"              # NVIDIA L4
    T4 = "t4"              # NVIDIA T4
    CPU = "cpu"            # CPU-only


class GPUScheduleStatus(Enum):
    """Status of GPU task scheduling."""
    PENDING = "pending"        # Waiting for GPU
    SCHEDULED = "scheduled"    # GPU allocated
    RUNNING = "running"        # Currently executing
    COMPLETED = "completed"    # Finished successfully
    FAILED = "failed"          # Failed
    CANCELLED = "cancelled"    # Cancelled


@dataclass
class GPUTaskSpec:
    """Specification for a GPU-intensive task."""
    task_id: str
    agent_id: str
    gpu_type: GPUType
    gpu_count: int = 1
    memory_gb: int = 40
    estimated_duration_seconds: float = 300.0
    max_wait_seconds: float = 600.0  # 10 min max wait
    priority: int = 5


@dataclass
class GPUAllocation:
    """Allocated GPU resource."""
    allocation_id: str
    task_id: str
    agent_id: str
    gpu_type: GPUType
    gpu_count: int
    allocated_at: datetime
    expires_at: datetime
    status: GPUScheduleStatus = GPUScheduleStatus.SCHEDULED
    cost_per_hour: float = 0.0


@dataclass
class GPUQuota:
    """GPU quota for an agent."""
    agent_id: str
    gpu_hours_monthly: float = 10.0
    gpu_hours_used: float = 0.0
    gpu_budget_usd: float = 100.0
    gpu_budget_spent: float = 0.0

    @property
    def gpu_hours_remaining(self) -> float:
        """Remaining GPU hours."""
        return self.gpu_hours_monthly - self.gpu_hours_used

    @property
    def gpu_budget_remaining(self) -> float:
        """Remaining GPU budget."""
        return self.gpu_budget_usd - self.gpu_budget_spent

    def can_allocate(
        self,
        duration_hours: float,
        estimated_cost: float,
    ) -> Tuple[bool, str]:
        """Check if allocation is possible."""
        if duration_hours > self.gpu_hours_remaining:
            return False, f"Insufficient GPU hours: need {duration_hours}, have {self.gpu_hours_remaining}"

        if estimated_cost > self.gpu_budget_remaining:
            return False, f"Insufficient GPU budget: need ${estimated_cost}, have ${self.gpu_budget_remaining}"

        return True, "Can allocate"


@dataclass
class GPUClusterStatus:
    """Status of available GPU resources."""
    total_gpus: Dict[str, int]  # GPU type -> count
    available_gpus: Dict[str, int]
    pending_tasks: int
    running_tasks: int
    average_wait_seconds: float
    cluster_utilization: float  # 0-1


class GPUScheduler:
    """
    Schedules GPU-intensive tasks on available GPUs.

    Features:
    - Priority-based scheduling
    - Quota management
    - Cost tracking
    - Auto-release on timeout
    """

    def __init__(self, cluster_config: Optional[Dict] = None):
        """
        Initialize GPU scheduler.

        Args:
            cluster_config: Cluster configuration (GPUs available, pricing, etc)
        """
        self.cluster_config = cluster_config or {
            "h100": {"count": 4, "cost_per_hour": 2.0},
            "a100": {"count": 8, "cost_per_hour": 1.5},
            "l4": {"count": 16, "cost_per_hour": 0.5},
            "t4": {"count": 32, "cost_per_hour": 0.3},
        }

        # Quotas per agent
        self._quotas: Dict[str, GPUQuota] = {}

        # Active allocations
        self._allocations: Dict[str, GPUAllocation] = {}

        # Task queue (priority queue)
        self._pending_tasks: List[GPUTaskSpec] = []

        # Tracking
        self._completed_allocations: List[GPUAllocation] = []

        logger.info("Initialized GPU scheduler")

    async def request_gpu_allocation(
        self,
        task_spec: GPUTaskSpec,
    ) -> Optional[GPUAllocation]:
        """
        Request GPU allocation for a task.

        Args:
            task_spec: GPU task specification

        Returns:
            GPUAllocation if successful, None if cannot allocate
        """
        logger.info(
            f"Requesting GPU allocation: {task_spec.task_id} "
            f"({task_spec.gpu_count}x {task_spec.gpu_type.value})"
        )

        # Get or create quota for agent
        quota = await self._get_or_create_quota(task_spec.agent_id)

        # Check quota
        duration_hours = task_spec.estimated_duration_seconds / 3600.0
        estimated_cost = duration_hours * self.cluster_config.get(
            task_spec.gpu_type.value, {}
        ).get("cost_per_hour", 1.0)

        can_allocate, reason = quota.can_allocate(duration_hours, estimated_cost)
        if not can_allocate:
            logger.warning(f"Cannot allocate: {reason}")
            return None

        # Try immediate allocation
        allocation = await self._try_allocate(task_spec, quota, estimated_cost)

        if allocation:
            self._allocations[allocation.allocation_id] = allocation
            logger.info(f"GPU allocated: {allocation.allocation_id}")
            return allocation

        # Queue if not immediately available
        logger.info(f"Queuing GPU task: {task_spec.task_id}")
        self._pending_tasks.append(task_spec)
        return None

    async def release_gpu_allocation(
        self,
        allocation_id: str,
    ) -> bool:
        """
        Release GPU allocation and free resources.

        Args:
            allocation_id: Allocation to release

        Returns:
            True if released, False if not found
        """
        if allocation_id not in self._allocations:
            logger.warning(f"Allocation not found: {allocation_id}")
            return False

        allocation = self._allocations.pop(allocation_id)
        allocation.status = GPUScheduleStatus.COMPLETED

        # Move to completed
        self._completed_allocations.append(allocation)

        logger.info(f"Released GPU allocation: {allocation_id}")
        return True

    async def get_cluster_status(self) -> GPUClusterStatus:
        """Get current cluster status."""
        total_gpus = {k: v["count"] for k, v in self.cluster_config.items()}

        # Count allocated
        allocated = {k: 0 for k in total_gpus.keys()}
        for allocation in self._allocations.values():
            if allocation.status in (GPUScheduleStatus.RUNNING, GPUScheduleStatus.SCHEDULED):
                key = allocation.gpu_type.value
                allocated[key] += allocation.gpu_count

        available = {
            k: total_gpus[k] - allocated.get(k, 0)
            for k in total_gpus.keys()
        }

        # Calculate utilization
        total_available = sum(total_gpus.values())
        total_allocated = sum(allocated.values())
        utilization = total_allocated / total_available if total_available > 0 else 0.0

        # Count tasks
        pending = len(self._pending_tasks)
        running = sum(
            1 for a in self._allocations.values()
            if a.status == GPUScheduleStatus.RUNNING
        )

        # Average wait
        avg_wait = 0.0
        if self._pending_tasks:
            now = datetime.utcnow()
            wait_times = [
                (now - t.__dict__.get("queued_at", now)).total_seconds()
                for t in self._pending_tasks
            ]
            avg_wait = sum(wait_times) / len(wait_times) if wait_times else 0.0

        return GPUClusterStatus(
            total_gpus=total_gpus,
            available_gpus=available,
            pending_tasks=pending,
            running_tasks=running,
            average_wait_seconds=avg_wait,
            cluster_utilization=utilization,
        )

    async def get_allocation_status(
        self,
        allocation_id: str,
    ) -> Optional[Dict]:
        """Get status of an allocation."""
        if allocation_id in self._allocations:
            allocation = self._allocations[allocation_id]
            return {
                "allocation_id": allocation_id,
                "task_id": allocation.task_id,
                "gpu_type": allocation.gpu_type.value,
                "gpu_count": allocation.gpu_count,
                "status": allocation.status.value,
                "allocated_at": allocation.allocated_at.isoformat(),
                "expires_at": allocation.expires_at.isoformat(),
                "cost_per_hour": allocation.cost_per_hour,
            }

        return None

    async def get_agent_gpu_usage(self, agent_id: str) -> Dict:
        """Get GPU usage for an agent."""
        quota = self._quotas.get(agent_id)
        if not quota:
            return {"agent_id": agent_id, "no_quota": True}

        return {
            "agent_id": agent_id,
            "gpu_hours_monthly": quota.gpu_hours_monthly,
            "gpu_hours_used": quota.gpu_hours_used,
            "gpu_hours_remaining": quota.gpu_hours_remaining,
            "gpu_budget_usd": quota.gpu_budget_usd,
            "gpu_budget_spent": quota.gpu_budget_spent,
            "gpu_budget_remaining": quota.gpu_budget_remaining,
            "utilization_percent": (
                quota.gpu_hours_used / quota.gpu_hours_monthly * 100
                if quota.gpu_hours_monthly > 0
                else 0.0
            ),
        }

    # Private methods

    async def _get_or_create_quota(self, agent_id: str) -> GPUQuota:
        """Get or create GPU quota for agent."""
        if agent_id not in self._quotas:
            self._quotas[agent_id] = GPUQuota(agent_id=agent_id)
        return self._quotas[agent_id]

    async def _try_allocate(
        self,
        task_spec: GPUTaskSpec,
        quota: GPUQuota,
        estimated_cost: float,
    ) -> Optional[GPUAllocation]:
        """Try to allocate GPU immediately."""
        # Check availability
        cluster_status = await self.get_cluster_status()
        gpu_type = task_spec.gpu_type.value

        if cluster_status.available_gpus.get(gpu_type, 0) < task_spec.gpu_count:
            return None

        # Create allocation
        import uuid

        allocation_id = f"gpu-{uuid.uuid4().hex[:8]}"
        now = datetime.utcnow()
        duration = timedelta(seconds=task_spec.estimated_duration_seconds)

        allocation = GPUAllocation(
            allocation_id=allocation_id,
            task_id=task_spec.task_id,
            agent_id=task_spec.agent_id,
            gpu_type=task_spec.gpu_type,
            gpu_count=task_spec.gpu_count,
            allocated_at=now,
            expires_at=now + duration,
            cost_per_hour=self.cluster_config.get(gpu_type, {}).get(
                "cost_per_hour", 1.0
            ),
        )

        # Update quota
        duration_hours = task_spec.estimated_duration_seconds / 3600.0
        quota.gpu_hours_used += duration_hours
        quota.gpu_budget_spent += estimated_cost

        return allocation
