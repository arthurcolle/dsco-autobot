"""
Swarm Coordination System.

Implements:
- SwarmNexus: Central coordination point
- SwarmAgent: Individual agent with wallet
- SwarmTask: Work unit distributed to agents

Based on:
- Swarm intelligence patterns (ACO, PSO)
- Multi-agent reinforcement learning
- Decentralized task markets

Key principle: Agents are economically motivated to
cooperate effectively, with value flowing to the host.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import logging

from .wallet import AgentWallet, WalletType, Chain, get_wallet_registry
from .economics import (
    BudgetConstraint,
    EconomicsEngine,
    PaymentChannel,
    StakingPool,
    RevenueShare,
)

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Status of a swarm task."""
    PENDING = "pending"
    CLAIMED = "claimed"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(str, Enum):
    """Priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class AgentStatus(str, Enum):
    """Status of a swarm agent."""
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"
    SUSPENDED = "suspended"


class AgentRole(str, Enum):
    """Roles agents can have in the swarm."""
    WORKER = "worker"           # Executes tasks
    COORDINATOR = "coordinator" # Routes tasks
    VALIDATOR = "validator"     # Validates results
    ORACLE = "oracle"          # Provides external data
    SPECIALIST = "specialist"  # Domain expert


@dataclass
class SwarmTask:
    """
    A unit of work in the swarm.

    Tasks flow through the swarm and are executed by
    the agent best suited for them. Payment happens
    on successful completion.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # What
    type: str = ""  # "research", "code", "analysis", "trade"
    description: str = ""
    input_data: Dict[str, Any] = field(default_factory=dict)
    expected_output: Optional[str] = None

    # Requirements
    required_capabilities: List[str] = field(default_factory=list)
    min_reputation: float = 0.0
    min_stake: Decimal = Decimal("0")

    # Payment
    bounty: Decimal = Decimal("10")  # Payment for completion
    escrow_id: Optional[str] = None

    # Timing
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    deadline: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Assignment
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    assigned_to: Optional[str] = None  # Agent ID

    # Results
    output_data: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    # Validation
    validated: bool = False
    validator_id: Optional[str] = None
    validation_score: Optional[float] = None

    # Parent task (for subtasks)
    parent_task_id: Optional[str] = None

    # Retry tracking
    attempt_count: int = 0
    max_attempts: int = 3

    def claim(self, agent_id: str):
        """Agent claims this task."""
        if self.status != TaskStatus.PENDING:
            raise ValueError(f"Task not pending: {self.status}")
        self.assigned_to = agent_id
        self.status = TaskStatus.CLAIMED
        self.started_at = datetime.now(timezone.utc)
        self.attempt_count += 1

    def start(self):
        """Mark task as in progress."""
        if self.status != TaskStatus.CLAIMED:
            raise ValueError(f"Task not claimed: {self.status}")
        self.status = TaskStatus.IN_PROGRESS

    def complete(self, output: Dict[str, Any]):
        """Mark task as completed."""
        self.status = TaskStatus.COMPLETED
        self.output_data = output
        self.completed_at = datetime.now(timezone.utc)

    def fail(self, error: str):
        """Mark task as failed."""
        self.status = TaskStatus.FAILED
        self.error = error
        self.completed_at = datetime.now(timezone.utc)

    def can_retry(self) -> bool:
        """Check if task can be retried."""
        return (
            self.status == TaskStatus.FAILED and
            self.attempt_count < self.max_attempts
        )

    def reset_for_retry(self):
        """Reset task for retry."""
        if not self.can_retry():
            raise ValueError("Task cannot be retried")
        self.status = TaskStatus.PENDING
        self.assigned_to = None
        self.started_at = None
        self.error = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "description": self.description,
            "status": self.status.value,
            "priority": self.priority.value,
            "bounty": str(self.bounty),
            "assigned_to": self.assigned_to,
            "created_at": self.created_at.isoformat(),
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "attempt_count": self.attempt_count,
            "validated": self.validated,
            "validation_score": self.validation_score,
        }


@dataclass
class SwarmAgent:
    """
    An agent in the swarm.

    Each agent has:
    - Identity (keypair for signing)
    - Wallet (for receiving payments)
    - Capabilities (what it can do)
    - Reputation (track record)
    - Stake (skin in the game)
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""

    # Role and capabilities
    role: AgentRole = AgentRole.WORKER
    capabilities: List[str] = field(default_factory=list)

    # Status
    status: AgentStatus = AgentStatus.IDLE
    current_task_id: Optional[str] = None

    # Wallet (link, not embed)
    wallet_id: Optional[str] = None

    # Reputation (0-1 scale)
    reputation: float = 0.5
    tasks_completed: int = 0
    tasks_failed: int = 0

    # Stake in swarm
    stake_amount: Decimal = Decimal("0")

    # Performance metrics
    avg_completion_time: float = 0.0  # seconds
    quality_score: float = 0.5

    # Registration
    joined_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_active: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Swarm membership
    swarm_id: Optional[str] = None

    def can_take_task(self, task: SwarmTask) -> Tuple[bool, str]:
        """Check if agent can take a task."""
        if self.status != AgentStatus.IDLE:
            return False, f"Agent not idle: {self.status}"

        if task.required_capabilities:
            missing = set(task.required_capabilities) - set(self.capabilities)
            if missing:
                return False, f"Missing capabilities: {missing}"

        if task.min_reputation > self.reputation:
            return False, f"Reputation too low: {self.reputation} < {task.min_reputation}"

        if task.min_stake > self.stake_amount:
            return False, f"Stake too low: {self.stake_amount} < {task.min_stake}"

        return True, "OK"

    def start_task(self, task_id: str):
        """Start working on a task."""
        self.status = AgentStatus.BUSY
        self.current_task_id = task_id
        self.last_active = datetime.now(timezone.utc)

    def complete_task(self, success: bool = True):
        """Finish working on a task."""
        self.status = AgentStatus.IDLE
        self.current_task_id = None
        self.last_active = datetime.now(timezone.utc)

        if success:
            self.tasks_completed += 1
            # Reputation increases on success
            self.reputation = min(1.0, self.reputation + 0.01)
        else:
            self.tasks_failed += 1
            # Reputation decreases on failure
            self.reputation = max(0.0, self.reputation - 0.02)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "role": self.role.value,
            "capabilities": self.capabilities,
            "status": self.status.value,
            "current_task_id": self.current_task_id,
            "wallet_id": self.wallet_id,
            "reputation": self.reputation,
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "stake_amount": str(self.stake_amount),
            "quality_score": self.quality_score,
            "swarm_id": self.swarm_id,
            "joined_at": self.joined_at.isoformat(),
            "last_active": self.last_active.isoformat(),
        }


class SwarmNexus:
    """
    Central coordination point for the swarm.

    The Nexus:
    1. Manages agent registration and staking
    2. Routes tasks to capable agents
    3. Handles payment flows
    4. Enforces swarm policies
    5. Reports to host/designer

    All value flows through the Nexus, ensuring the host
    receives their share of all economic activity.
    """

    def __init__(
        self,
        name: str,
        host_id: str,
        host_wallet: str,
    ):
        self.id = str(uuid.uuid4())
        self.name = name
        self.host_id = host_id
        self.host_wallet = host_wallet

        # Economics
        self.economics = EconomicsEngine(host_id, host_wallet)

        # Agents
        self.agents: Dict[str, SwarmAgent] = {}

        # Tasks
        self.tasks: Dict[str, SwarmTask] = {}
        self.task_queue: List[str] = []  # Task IDs in priority order

        # Channels between agents
        self.channels: Dict[str, PaymentChannel] = {}

        # Stats
        self.created_at = datetime.now(timezone.utc)
        self.total_tasks_completed = 0
        self.total_payments_processed = Decimal("0")

    def register_agent(
        self,
        name: str,
        capabilities: List[str],
        role: AgentRole = AgentRole.WORKER,
        initial_stake: Decimal = Decimal("0"),
    ) -> SwarmAgent:
        """
        Register a new agent in the swarm.

        Creates wallet and stakes if provided.
        """
        agent = SwarmAgent(
            name=name,
            role=role,
            capabilities=capabilities,
            swarm_id=self.id,
        )

        # Create wallet
        wallet_registry = get_wallet_registry()
        wallet = wallet_registry.create_wallet(
            agent_id=agent.id,
            wallet_type=WalletType.WARM,
            master_pubkey=self.host_wallet,
        )
        agent.wallet_id = wallet.id

        # Stake if provided
        if initial_stake > 0:
            if self.economics.staking_pool.stake(agent.id, initial_stake):
                agent.stake_amount = initial_stake

        self.agents[agent.id] = agent

        logger.info(
            f"Registered agent {name} ({agent.id}) with "
            f"capabilities: {capabilities}, stake: {initial_stake}"
        )

        return agent

    def submit_task(
        self,
        task_type: str,
        description: str,
        input_data: Dict[str, Any],
        bounty: Decimal,
        required_capabilities: List[str] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        deadline: Optional[datetime] = None,
        budget_id: Optional[str] = None,
    ) -> SwarmTask:
        """
        Submit a task to the swarm.

        The task goes into the queue and will be claimed
        by a capable agent.
        """
        task = SwarmTask(
            type=task_type,
            description=description,
            input_data=input_data,
            bounty=bounty,
            required_capabilities=required_capabilities or [],
            priority=priority,
            deadline=deadline,
        )

        # Check budget if specified
        if budget_id:
            budget = self.economics.get_budget(budget_id)
            if budget:
                ok, msg = budget.can_spend(bounty)
                if not ok:
                    raise ValueError(f"Budget check failed: {msg}")

        self.tasks[task.id] = task

        # Insert into queue by priority
        self._enqueue_task(task.id)

        logger.info(
            f"Task submitted: {task.type} ({task.id}) - "
            f"bounty: ${bounty}, priority: {priority.value}"
        )

        return task

    def _enqueue_task(self, task_id: str):
        """Insert task into queue by priority."""
        task = self.tasks[task_id]
        priority_order = {
            TaskPriority.URGENT: 0,
            TaskPriority.HIGH: 1,
            TaskPriority.NORMAL: 2,
            TaskPriority.LOW: 3,
        }

        # Find insertion point
        insert_at = len(self.task_queue)
        for i, tid in enumerate(self.task_queue):
            other = self.tasks.get(tid)
            if other and priority_order[task.priority] < priority_order[other.priority]:
                insert_at = i
                break

        self.task_queue.insert(insert_at, task_id)

    def find_agent_for_task(self, task_id: str) -> Optional[SwarmAgent]:
        """Find the best agent for a task."""
        task = self.tasks.get(task_id)
        if not task:
            return None

        candidates = []

        for agent in self.agents.values():
            can_take, _ = agent.can_take_task(task)
            if can_take:
                # Score by reputation * (1 + quality_score)
                score = agent.reputation * (1 + agent.quality_score)
                candidates.append((score, agent))

        if not candidates:
            return None

        # Return highest scored agent
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    async def assign_task(self, task_id: str, agent_id: str) -> bool:
        """Assign a task to a specific agent."""
        task = self.tasks.get(task_id)
        agent = self.agents.get(agent_id)

        if not task or not agent:
            return False

        can_take, msg = agent.can_take_task(task)
        if not can_take:
            logger.warning(f"Agent {agent_id} cannot take task {task_id}: {msg}")
            return False

        task.claim(agent_id)
        agent.start_task(task_id)

        # Remove from queue
        if task_id in self.task_queue:
            self.task_queue.remove(task_id)

        logger.info(f"Assigned task {task_id} to agent {agent_id}")
        return True

    async def complete_task(
        self,
        task_id: str,
        output: Dict[str, Any],
        success: bool = True,
    ):
        """
        Mark a task as completed and process payment.

        On success:
        1. Validate output (optional)
        2. Pay bounty to agent (minus host fee)
        3. Update reputation

        On failure:
        1. Retry if attempts remain
        2. Slash stake if policy requires
        3. Update reputation
        """
        task = self.tasks.get(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")

        agent = self.agents.get(task.assigned_to) if task.assigned_to else None
        if not agent:
            raise ValueError(f"No agent assigned to task {task_id}")

        if success:
            task.complete(output)

            # Process payment
            result = self.economics.process_payment(
                from_agent=self.host_id,  # Swarm treasury
                to_agent=agent.id,
                amount=task.bounty,
                purpose=f"Task completion: {task.type}",
                warrant_id=None,  # Could link to warrant
            )

            if result["success"]:
                self.total_payments_processed += task.bounty
                self.total_tasks_completed += 1

            agent.complete_task(success=True)

            logger.info(
                f"Task {task_id} completed by {agent.name} - "
                f"paid ${task.bounty}"
            )

        else:
            task.fail(output.get("error", "Unknown error"))
            agent.complete_task(success=False)

            if task.can_retry():
                task.reset_for_retry()
                self._enqueue_task(task_id)
                logger.info(f"Task {task_id} failed, queued for retry")
            else:
                # Optional: slash stake for repeated failures
                logger.warning(f"Task {task_id} failed permanently")

    async def run_task_matching(self):
        """
        Background task matching loop.

        Continuously matches pending tasks to idle agents.
        """
        while True:
            for task_id in list(self.task_queue):
                task = self.tasks.get(task_id)
                if not task or task.status != TaskStatus.PENDING:
                    continue

                agent = self.find_agent_for_task(task_id)
                if agent:
                    await self.assign_task(task_id, agent.id)

            await asyncio.sleep(1)

    def get_pending_tasks(self) -> List[SwarmTask]:
        """Get all pending tasks."""
        return [
            self.tasks[tid]
            for tid in self.task_queue
            if tid in self.tasks and self.tasks[tid].status == TaskStatus.PENDING
        ]

    def get_idle_agents(self) -> List[SwarmAgent]:
        """Get all idle agents."""
        return [
            a for a in self.agents.values()
            if a.status == AgentStatus.IDLE
        ]

    def get_swarm_stats(self) -> Dict[str, Any]:
        """Get swarm statistics."""
        return {
            "id": self.id,
            "name": self.name,
            "host_id": self.host_id,
            "agent_count": len(self.agents),
            "idle_agents": len(self.get_idle_agents()),
            "pending_tasks": len(self.get_pending_tasks()),
            "total_tasks_completed": self.total_tasks_completed,
            "total_payments_processed": str(self.total_payments_processed),
            "host_earnings": self.economics.get_host_earnings(),
            "staking_pool": self.economics.staking_pool.to_dict(),
            "created_at": self.created_at.isoformat(),
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            **self.get_swarm_stats(),
            "agents": [a.to_dict() for a in self.agents.values()],
            "recent_tasks": [
                self.tasks[tid].to_dict()
                for tid in list(self.task_queue)[:10]
            ],
        }


# Global swarm registry
_swarms: Dict[str, SwarmNexus] = {}


def create_swarm(
    name: str,
    host_id: str,
    host_wallet: str,
) -> SwarmNexus:
    """Create a new swarm."""
    swarm = SwarmNexus(name, host_id, host_wallet)
    _swarms[swarm.id] = swarm
    return swarm


def get_swarm(swarm_id: str) -> Optional[SwarmNexus]:
    """Get a swarm by ID."""
    return _swarms.get(swarm_id)


def list_swarms() -> List[Dict[str, Any]]:
    """List all swarms."""
    return [s.get_swarm_stats() for s in _swarms.values()]
