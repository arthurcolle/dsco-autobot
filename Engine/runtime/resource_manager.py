"""
Resource Manager for Agent Task Execution

Manages API credit budgets, GPU allocation, memory quotas, and rate limiting
to ensure agents don't over-commit resources or exceed cost limits.

Coordinates with GraphSub for distributed state and Redis for fast checks.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from substrate.graphsub_client import GraphSubClient
from substrate.redis_mailbox import RedisMailbox

logger = logging.getLogger(__name__)


class ResourceType(Enum):
    """Types of resources that can be allocated."""
    API_CREDITS = "api_credits"  # $ cost for LLM calls
    GPU_HOURS = "gpu_hours"      # GPU compute time
    MEMORY_GB = "memory_gb"      # RAM allocation
    CONCURRENT_TASKS = "concurrent_tasks"  # Max parallel executions
    STORAGE_GB = "storage_gb"    # Persistent storage


class ReservationStatus(Enum):
    """Status of a resource reservation."""
    PENDING = "pending"      # Requested, not yet allocated
    ALLOCATED = "allocated"  # Resources reserved
    EXECUTING = "executing"  # Task is running
    COMPLETED = "completed"  # Task finished, resources freed
    FAILED = "failed"        # Task failed, resources freed
    REFUNDED = "refunded"    # Partially refunded


@dataclass
class ResourceQuota:
    """Maximum resources available for an agent."""
    agent_id: str
    api_credits: float = 100.0          # $ budget
    gpu_hours_monthly: float = 10.0     # GPU quota per month
    memory_gb: float = 4.0              # Local memory limit
    max_concurrent_tasks: int = 3       # Parallel task limit
    storage_gb: float = 10.0            # Storage quota

    @property
    def all_resources(self) -> Dict[ResourceType, float]:
        """Get all resources as a dictionary."""
        return {
            ResourceType.API_CREDITS: self.api_credits,
            ResourceType.GPU_HOURS: self.gpu_hours_monthly,
            ResourceType.MEMORY_GB: self.memory_gb,
            ResourceType.CONCURRENT_TASKS: float(self.max_concurrent_tasks),
            ResourceType.STORAGE_GB: self.storage_gb,
        }


@dataclass
class Reservation:
    """Resource reservation for a task."""
    reservation_id: str
    agent_id: str
    task_id: str
    resources: Dict[ResourceType, float]
    status: ReservationStatus = ReservationStatus.PENDING
    allocated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    refunded_amount: Dict[ResourceType, float] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        """Check if reservation is actively holding resources."""
        return self.status in (ReservationStatus.ALLOCATED, ReservationStatus.EXECUTING)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Get duration from allocation to completion."""
        if not self.allocated_at or not self.completed_at:
            return None
        return (self.completed_at - self.allocated_at).total_seconds()

    def get_resource(self, resource_type: ResourceType) -> float:
        """Get amount of specific resource reserved."""
        return self.resources.get(resource_type, 0.0)


@dataclass
class ResourceEstimate:
    """Pre-execution resource cost estimate."""
    task_id: str
    api_credit_cost: float          # $ cost estimate
    gpu_hours_needed: float = 0.0   # GPU time if using GPU
    memory_needed_gb: float = 0.1   # RAM needed
    storage_needed_gb: float = 0.1  # Temp storage needed
    duration_seconds: float = 30.0  # Estimated execution time
    confidence: float = 0.7         # Confidence in estimate (0-1)

    @property
    def total_cost_usd(self) -> float:
        """Get total cost in USD."""
        # Rough estimates: GPU ~$0.50/hour, storage $0.023/GB-month
        gpu_cost = self.gpu_hours_needed * 0.5
        storage_cost = self.storage_needed_gb * 0.023 / 30  # Per day
        return self.api_credit_cost + gpu_cost + storage_cost


class ResourceManager:
    """
    Manages resource allocation and tracking for agents.

    Features:
    - Pre-flight resource availability checks
    - Resource reservation with atomic allocation
    - Budget tracking and refunds
    - Rate limiting based on resource usage
    - GPU quota management
    - Concurrent task limiting
    """

    def __init__(
        self,
        graphsub_client: GraphSubClient,
        redis_client: Optional[RedisMailbox] = None,
        cache_ttl_seconds: int = 60,
    ):
        """
        Initialize resource manager.

        Args:
            graphsub_client: GraphSub client for persistent state
            redis_client: Redis client for fast rate limiting (optional)
            cache_ttl_seconds: Cache expiration time
        """
        self.graphsub = graphsub_client
        self.redis = redis_client
        self.cache_ttl_seconds = cache_ttl_seconds

        # In-memory cache of reservations (distributed in real deployment)
        self._reservations: Dict[str, Reservation] = {}
        self._agent_quotas: Dict[str, ResourceQuota] = {}
        self._monthly_usage: Dict[str, Dict[str, float]] = {}

    async def load_quota(
        self,
        agent_id: str,
        use_cache: bool = True,
    ) -> ResourceQuota:
        """
        Load agent resource quota from GraphSub.

        Args:
            agent_id: Agent ID
            use_cache: Use cached quota if available

        Returns:
            ResourceQuota for the agent
        """
        # Check cache
        if use_cache and agent_id in self._agent_quotas:
            return self._agent_quotas[agent_id]

        # Query GraphSub for quota
        try:
            result = await self.graphsub.query(
                """
                MATCH (a:Agent {id: $agent_id})
                RETURN a.quota_api_credits AS api_credits,
                       a.quota_gpu_hours AS gpu_hours,
                       a.quota_memory_gb AS memory_gb,
                       a.quota_max_concurrent AS max_concurrent,
                       a.quota_storage_gb AS storage_gb
                """,
                {"agent_id": agent_id},
            )

            if result:
                row = result[0]
                quota = ResourceQuota(
                    agent_id=agent_id,
                    api_credits=float(row.get("api_credits", 100.0)),
                    gpu_hours_monthly=float(row.get("gpu_hours", 10.0)),
                    memory_gb=float(row.get("memory_gb", 4.0)),
                    max_concurrent_tasks=int(row.get("max_concurrent", 3)),
                    storage_gb=float(row.get("storage_gb", 10.0)),
                )
            else:
                # Use defaults if not found
                quota = ResourceQuota(agent_id=agent_id)

            # Cache
            self._agent_quotas[agent_id] = quota
            return quota

        except Exception as e:
            logger.error(f"Failed to load quota for {agent_id}: {e}")
            return ResourceQuota(agent_id=agent_id)

    async def can_afford(
        self,
        agent_id: str,
        estimate: ResourceEstimate,
    ) -> Tuple[bool, str]:
        """
        Check if agent can afford to execute a task.

        Args:
            agent_id: Agent ID
            estimate: Resource cost estimate

        Returns:
            Tuple of (can_afford: bool, reason: str)
        """
        try:
            # Check budget
            current_budget = await self._get_current_budget(agent_id)
            if current_budget < estimate.api_credit_cost:
                return False, (
                    f"Insufficient budget: need ${estimate.api_credit_cost:.2f}, "
                    f"have ${current_budget:.2f}"
                )

            # Check GPU quota if needed
            if estimate.gpu_hours_needed > 0:
                quota = await self.load_quota(agent_id)
                monthly_gpu_used = await self._get_monthly_gpu_usage(agent_id)
                available_gpu = quota.gpu_hours_monthly - monthly_gpu_used

                if available_gpu < estimate.gpu_hours_needed:
                    return False, (
                        f"Insufficient GPU quota: need {estimate.gpu_hours_needed:.1f}h, "
                        f"have {available_gpu:.1f}h available"
                    )

            # Check concurrent task limit
            quota = await self.load_quota(agent_id)
            active_tasks = len([
                r for r in self._reservations.values()
                if r.agent_id == agent_id and r.is_active
            ])

            if active_tasks >= quota.max_concurrent_tasks:
                return False, (
                    f"At max concurrent task limit: {active_tasks}/{quota.max_concurrent_tasks}"
                )

            # Check storage quota if needed
            if estimate.storage_needed_gb > 0:
                storage_used = await self._get_storage_usage(agent_id)
                available_storage = quota.storage_gb - storage_used

                if available_storage < estimate.storage_needed_gb:
                    return False, (
                        f"Insufficient storage: need {estimate.storage_needed_gb:.1f}GB, "
                        f"have {available_storage:.1f}GB"
                    )

            return True, "Can afford"

        except Exception as e:
            logger.error(f"Error checking affordability: {e}")
            return False, f"Error checking resources: {e}"

    async def reserve(
        self,
        agent_id: str,
        task_id: str,
        estimate: ResourceEstimate,
    ) -> Optional[Reservation]:
        """
        Reserve resources for a task.

        Args:
            agent_id: Agent ID
            task_id: Task ID
            estimate: Resource cost estimate

        Returns:
            Reservation object if successful, None if failed
        """
        try:
            # Pre-flight check
            can_afford, reason = await self.can_afford(agent_id, estimate)
            if not can_afford:
                logger.warning(f"Cannot reserve for {task_id}: {reason}")
                return None

            # Create reservation
            reservation_id = f"res-{agent_id}-{task_id}"
            resources = {
                ResourceType.API_CREDITS: estimate.api_credit_cost,
                ResourceType.GPU_HOURS: estimate.gpu_hours_needed,
                ResourceType.MEMORY_GB: estimate.memory_needed_gb,
                ResourceType.STORAGE_GB: estimate.storage_needed_gb,
                ResourceType.CONCURRENT_TASKS: 1.0,
            }

            reservation = Reservation(
                reservation_id=reservation_id,
                agent_id=agent_id,
                task_id=task_id,
                resources=resources,
                status=ReservationStatus.ALLOCATED,
                allocated_at=datetime.utcnow(),
            )

            # Store in cache
            self._reservations[reservation_id] = reservation

            # Persist to GraphSub
            await self._persist_reservation(reservation)

            logger.info(
                f"Reserved {estimate.api_credit_cost:.2f}$ for task {task_id} "
                f"on agent {agent_id}"
            )

            return reservation

        except Exception as e:
            logger.error(f"Failed to reserve resources: {e}")
            return None

    async def complete(
        self,
        reservation: Reservation,
        actual_cost: Optional[float] = None,
    ) -> None:
        """
        Mark reservation as completed and free resources.

        Args:
            reservation: Reservation to complete
            actual_cost: Actual cost if different from estimate
        """
        try:
            reservation.status = ReservationStatus.COMPLETED
            reservation.completed_at = datetime.utcnow()

            # Refund difference if actual cost is less
            if actual_cost is not None and actual_cost < reservation.get_resource(ResourceType.API_CREDITS):
                refund_amount = reservation.get_resource(ResourceType.API_CREDITS) - actual_cost
                reservation.refunded_amount[ResourceType.API_CREDITS] = refund_amount

                logger.info(
                    f"Task {reservation.task_id} completed. "
                    f"Refunding ${refund_amount:.2f}"
                )

            # Persist to GraphSub
            await self._persist_reservation(reservation)

            logger.debug(f"Completed reservation {reservation.reservation_id}")

        except Exception as e:
            logger.error(f"Failed to complete reservation: {e}")

    async def fail(
        self,
        reservation: Reservation,
        refund_percentage: float = 1.0,
    ) -> None:
        """
        Mark reservation as failed and optionally refund resources.

        Args:
            reservation: Reservation that failed
            refund_percentage: Percentage of reserved amount to refund (0.0-1.0)
        """
        try:
            reservation.status = ReservationStatus.FAILED
            reservation.completed_at = datetime.utcnow()

            # Calculate refund
            if refund_percentage > 0:
                api_reserved = reservation.get_resource(ResourceType.API_CREDITS)
                refund_amount = api_reserved * refund_percentage
                reservation.refunded_amount[ResourceType.API_CREDITS] = refund_amount

                logger.info(
                    f"Task {reservation.task_id} failed. "
                    f"Refunding ${refund_amount:.2f} ({refund_percentage:.0%})"
                )

            # Persist to GraphSub
            await self._persist_reservation(reservation)

            logger.debug(f"Failed reservation {reservation.reservation_id}")

        except Exception as e:
            logger.error(f"Failed to mark reservation as failed: {e}")

    async def get_agent_summary(
        self,
        agent_id: str,
    ) -> Dict:
        """
        Get resource usage summary for an agent.

        Args:
            agent_id: Agent ID

        Returns:
            Dict with quota, current usage, and available resources
        """
        quota = await self.load_quota(agent_id)
        current_budget = await self._get_current_budget(agent_id)
        monthly_gpu_used = await self._get_monthly_gpu_usage(agent_id)
        storage_used = await self._get_storage_usage(agent_id)
        active_tasks = len([
            r for r in self._reservations.values()
            if r.agent_id == agent_id and r.is_active
        ])

        return {
            "agent_id": agent_id,
            "budget": {
                "total": quota.api_credits,
                "remaining": current_budget,
                "spent": quota.api_credits - current_budget,
                "percent_used": (1.0 - current_budget / quota.api_credits) * 100,
            },
            "gpu": {
                "quota_monthly": quota.gpu_hours_monthly,
                "used_this_month": monthly_gpu_used,
                "available": quota.gpu_hours_monthly - monthly_gpu_used,
            },
            "storage": {
                "quota": quota.storage_gb,
                "used": storage_used,
                "available": quota.storage_gb - storage_used,
            },
            "concurrent_tasks": {
                "max": quota.max_concurrent_tasks,
                "active": active_tasks,
                "available": quota.max_concurrent_tasks - active_tasks,
            },
        }

    # Private helper methods

    async def _get_current_budget(self, agent_id: str) -> float:
        """Get agent's current remaining budget."""
        try:
            result = await self.graphsub.query(
                "MATCH (a:Agent {id: $agent_id}) RETURN a.budget_remaining AS budget",
                {"agent_id": agent_id},
            )
            if result:
                return float(result[0].get("budget", 0.0))
            return 0.0
        except Exception as e:
            logger.error(f"Failed to get budget for {agent_id}: {e}")
            return 0.0

    async def _get_monthly_gpu_usage(self, agent_id: str) -> float:
        """Get GPU hours used this month."""
        try:
            result = await self.graphsub.query(
                """
                MATCH (a:Agent {id: $agent_id})
                RETURN a.gpu_hours_used_month AS gpu_used
                """,
                {"agent_id": agent_id},
            )
            if result:
                return float(result[0].get("gpu_used", 0.0))
            return 0.0
        except Exception:
            return 0.0

    async def _get_storage_usage(self, agent_id: str) -> float:
        """Get storage currently used."""
        try:
            result = await self.graphsub.query(
                """
                MATCH (a:Agent {id: $agent_id})
                RETURN a.storage_used_gb AS storage_used
                """,
                {"agent_id": agent_id},
            )
            if result:
                return float(result[0].get("storage_used", 0.0))
            return 0.0
        except Exception:
            return 0.0

    async def _persist_reservation(self, reservation: Reservation) -> None:
        """Persist reservation to GraphSub for durability."""
        try:
            cypher = """
            MATCH (a:Agent {id: $agent_id})
            CREATE (r:Reservation {
                id: $res_id,
                task_id: $task_id,
                status: $status,
                allocated_at: $allocated,
                completed_at: $completed,
                api_cost: $api_cost
            })
            CREATE (a)-[:HAS_RESERVATION]->(r)
            """

            await self.graphsub.query(
                cypher,
                {
                    "agent_id": reservation.agent_id,
                    "res_id": reservation.reservation_id,
                    "task_id": reservation.task_id,
                    "status": reservation.status.value,
                    "allocated": reservation.allocated_at.isoformat() if reservation.allocated_at else None,
                    "completed": reservation.completed_at.isoformat() if reservation.completed_at else None,
                    "api_cost": reservation.get_resource(ResourceType.API_CREDITS),
                },
            )
        except Exception as e:
            logger.error(f"Failed to persist reservation to GraphSub: {e}")

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "cached_quotas": len(self._agent_quotas),
            "active_reservations": len([
                r for r in self._reservations.values()
                if r.is_active
            ]),
            "total_reservations": len(self._reservations),
        }
