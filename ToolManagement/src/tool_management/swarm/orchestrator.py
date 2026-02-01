"""
Swarm Orchestrator.

The high-level interface for running autonomous agent swarms.
Handles:
- Swarm lifecycle management
- Host/designer value capture
- Integration with warrant system
- Project execution flows

Usage:
    # Create orchestrator for a project
    orchestrator = SwarmOrchestrator(
        host_id="alice",
        host_wallet="0x...",
        project_name="AI Research Pipeline",
    )

    # Register agents
    researcher = orchestrator.spawn_agent("researcher", ["search", "analyze"])
    coder = orchestrator.spawn_agent("coder", ["code", "test"])
    reviewer = orchestrator.spawn_agent("reviewer", ["review", "validate"])

    # Fund the swarm
    orchestrator.fund(1000)

    # Submit work
    result = await orchestrator.execute_project({
        "goal": "Build a sentiment analyzer",
        "subtasks": [
            {"type": "research", "query": "sentiment analysis techniques"},
            {"type": "code", "spec": "implement best approach"},
            {"type": "test", "coverage": "90%"},
        ]
    })

    # Collect earnings
    earnings = orchestrator.collect_earnings()
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional
import logging

from .swarm import (
    SwarmNexus,
    SwarmAgent,
    SwarmTask,
    AgentRole,
    TaskPriority,
    TaskStatus,
    create_swarm,
)
from .wallet import (
    AgentWallet,
    WalletType,
    Chain,
    get_wallet_registry,
)
from .economics import (
    BudgetConstraint,
    RevenueShare,
    RevenueModel,
)
from ..auth.warrants import (
    KeyPair,
    Warrant,
    get_warrant_store,
)

logger = logging.getLogger(__name__)


@dataclass
class ProjectSpec:
    """Specification for a project to be executed by the swarm."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    goal: str = ""
    subtasks: List[Dict[str, Any]] = field(default_factory=list)
    budget: Decimal = Decimal("1000")
    deadline: Optional[datetime] = None
    priority: TaskPriority = TaskPriority.NORMAL

    # Execution tracking
    status: str = "pending"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    task_ids: List[str] = field(default_factory=list)
    results: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SwarmConfig:
    """Configuration for swarm behavior."""
    # Economic settings
    host_fee_rate: Decimal = Decimal("0.05")  # 5% to host
    min_stake_to_join: Decimal = Decimal("10")
    max_concurrent_tasks: int = 10

    # Agent settings
    default_agent_budget: Decimal = Decimal("100")
    auto_retry_failed: bool = True
    max_retries: int = 3

    # Security
    require_warrants: bool = True
    warrant_ttl_hours: int = 24


class SwarmOrchestrator:
    """
    High-level orchestrator for agent swarms.

    The orchestrator is the interface between the host/designer
    and the swarm. It handles:

    1. Swarm Creation: Spawning the nexus and agents
    2. Authorization: Minting warrants for agent operations
    3. Funding: Depositing tokens into swarm treasury
    4. Execution: Breaking down projects into tasks
    5. Monitoring: Tracking progress and earnings
    6. Collection: Withdrawing host earnings

    All value flows through the orchestrator, ensuring the
    host captures their share of economic activity.
    """

    def __init__(
        self,
        host_id: str,
        host_wallet: str,
        project_name: str = "Default Swarm",
        config: Optional[SwarmConfig] = None,
    ):
        self.host_id = host_id
        self.host_wallet = host_wallet
        self.project_name = project_name
        self.config = config or SwarmConfig()

        # Host keypair for signing warrants
        self._host_keypair = KeyPair.generate()

        # Create swarm
        self.swarm = create_swarm(
            name=project_name,
            host_id=host_id,
            host_wallet=host_wallet,
        )

        # Configure economics
        self.swarm.economics.revenue_share.percentage_rate = self.config.host_fee_rate
        self.swarm.economics.staking_pool.min_stake = self.config.min_stake_to_join

        # Projects being executed
        self.projects: Dict[str, ProjectSpec] = {}

        # Agent warrants
        self.agent_warrants: Dict[str, Warrant] = {}

        # Stats
        self.created_at = datetime.now(timezone.utc)
        self.total_funded = Decimal("0")
        self.total_collected = Decimal("0")

        logger.info(
            f"Created orchestrator for {project_name} "
            f"(host: {host_id}, fee: {self.config.host_fee_rate*100}%)"
        )

    def spawn_agent(
        self,
        name: str,
        capabilities: List[str],
        role: AgentRole = AgentRole.WORKER,
        initial_stake: Decimal = Decimal("0"),
        budget: Optional[Decimal] = None,
    ) -> SwarmAgent:
        """
        Spawn a new agent in the swarm.

        Creates the agent with:
        - Wallet for receiving payments
        - Stake in the pool
        - Warrant for operations
        - Budget constraint
        """
        # Register agent
        agent = self.swarm.register_agent(
            name=name,
            capabilities=capabilities,
            role=role,
            initial_stake=initial_stake,
        )

        # Create warrant for agent operations
        if self.config.require_warrants:
            warrant = self._mint_agent_warrant(agent, budget or self.config.default_agent_budget)
            self.agent_warrants[agent.id] = warrant

            # Add warrant to agent's wallet
            wallet = get_wallet_registry().get_wallet(agent.wallet_id)
            if wallet:
                wallet.initialize_keypair()
                # Note: In real impl, holder_pubkey would match wallet's keypair
                # For now, we skip this validation

        logger.info(f"Spawned agent: {name} ({role.value})")
        return agent

    def _mint_agent_warrant(
        self,
        agent: SwarmAgent,
        budget: Decimal,
    ) -> Warrant:
        """Mint a warrant for an agent."""
        wallet = get_wallet_registry().get_wallet(agent.wallet_id)
        holder_pubkey = wallet.agent_pubkey if wallet else b"placeholder"

        if not holder_pubkey:
            holder_pubkey = b"placeholder"  # For testing

        warrant = Warrant.mint(
            issuer_key=self._host_keypair,
            holder_pubkey=holder_pubkey,
            capability="swarm_operations",
            constraints=[],
            ttl=timedelta(hours=self.config.warrant_ttl_hours),
            can_delegate=False,
            issuer_id=self.host_id,
            holder_id=agent.id,
            metadata={
                "swarm_id": self.swarm.id,
                "budget": str(budget),
                "capabilities": agent.capabilities,
            },
        )

        # Create budget constraint
        self.swarm.economics.create_budget(
            warrant_id=warrant.id,
            total=budget,
            max_per_operation=budget / 10,  # 10% per operation max
            allowed_tokens=["USDC", "SOL"],
        )

        # Store warrant
        get_warrant_store().store_warrant(warrant)

        return warrant

    def fund(self, amount: Decimal, chain: Chain = Chain.SOLANA_DEVNET) -> str:
        """
        Fund the swarm treasury.

        In production, this would:
        1. Transfer tokens to swarm wallet
        2. Record deposit transaction
        3. Emit funding event

        Returns transaction ID.
        """
        tx_id = str(uuid.uuid4())

        self.total_funded += amount

        logger.info(f"Funded swarm with ${amount} (tx: {tx_id})")
        return tx_id

    async def execute_project(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a project through the swarm.

        Breaks down the project into tasks and distributes
        them to capable agents. Waits for completion and
        aggregates results.
        """
        project = ProjectSpec(
            name=spec.get("name", "Unnamed Project"),
            goal=spec.get("goal", ""),
            subtasks=spec.get("subtasks", []),
            budget=Decimal(str(spec.get("budget", 1000))),
            priority=TaskPriority(spec.get("priority", "normal")),
        )

        if spec.get("deadline"):
            project.deadline = datetime.fromisoformat(spec["deadline"])

        project.status = "in_progress"
        project.started_at = datetime.now(timezone.utc)
        self.projects[project.id] = project

        # Create tasks for each subtask
        for subtask in project.subtasks:
            task = self.swarm.submit_task(
                task_type=subtask.get("type", "generic"),
                description=subtask.get("description", ""),
                input_data=subtask,
                bounty=Decimal(str(subtask.get("bounty", 10))),
                required_capabilities=subtask.get("capabilities", []),
                priority=project.priority,
                deadline=project.deadline,
            )
            project.task_ids.append(task.id)

        logger.info(
            f"Project {project.name} started with {len(project.task_ids)} tasks"
        )

        # Wait for all tasks to complete
        results = await self._wait_for_tasks(project.task_ids)

        project.status = "completed"
        project.completed_at = datetime.now(timezone.utc)
        project.results = results

        return {
            "project_id": project.id,
            "status": project.status,
            "task_count": len(project.task_ids),
            "results": results,
            "duration_seconds": (project.completed_at - project.started_at).total_seconds(),
        }

    async def _wait_for_tasks(
        self,
        task_ids: List[str],
        timeout: int = 300,
    ) -> Dict[str, Any]:
        """Wait for tasks to complete."""
        results = {}
        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)

        while datetime.now(timezone.utc) < deadline:
            all_done = True

            for task_id in task_ids:
                task = self.swarm.tasks.get(task_id)
                if not task:
                    continue

                if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                    results[task_id] = {
                        "status": task.status.value,
                        "output": task.output_data,
                        "error": task.error,
                    }
                else:
                    all_done = False

            if all_done:
                break

            await asyncio.sleep(1)

        return results

    def collect_earnings(self) -> Decimal:
        """
        Collect host earnings from the swarm.

        Returns the amount collected.
        """
        payout = self.swarm.economics.revenue_share.process_payout()

        if payout:
            self.total_collected += payout
            logger.info(f"Collected ${payout} in host earnings")
            return payout

        return Decimal("0")

    def get_status(self) -> Dict[str, Any]:
        """Get orchestrator and swarm status."""
        return {
            "orchestrator": {
                "host_id": self.host_id,
                "project_name": self.project_name,
                "created_at": self.created_at.isoformat(),
                "total_funded": str(self.total_funded),
                "total_collected": str(self.total_collected),
                "active_projects": len([
                    p for p in self.projects.values()
                    if p.status == "in_progress"
                ]),
            },
            "swarm": self.swarm.get_swarm_stats(),
            "agents": [a.to_dict() for a in self.swarm.agents.values()],
            "config": {
                "host_fee_rate": str(self.config.host_fee_rate),
                "min_stake": str(self.config.min_stake_to_join),
                "require_warrants": self.config.require_warrants,
            },
        }

    async def run(self):
        """
        Run the orchestrator's main loop.

        Handles:
        - Task matching
        - Timeout monitoring
        - Earnings collection
        """
        # Start task matching in background
        matching_task = asyncio.create_task(self.swarm.run_task_matching())

        try:
            while True:
                # Periodic earnings collection
                self.collect_earnings()

                # Check for timed out tasks
                for task in self.swarm.tasks.values():
                    if (
                        task.status == TaskStatus.IN_PROGRESS and
                        task.deadline and
                        datetime.now(timezone.utc) > task.deadline
                    ):
                        await self.swarm.complete_task(
                            task.id,
                            {"error": "Task timed out"},
                            success=False,
                        )

                await asyncio.sleep(60)  # Check every minute

        except asyncio.CancelledError:
            matching_task.cancel()
            logger.info("Orchestrator stopped")


# Convenience functions
def create_orchestrator(
    host_id: str,
    host_wallet: str,
    project_name: str = "Default Swarm",
    **kwargs,
) -> SwarmOrchestrator:
    """Create a new swarm orchestrator."""
    config = SwarmConfig(**kwargs) if kwargs else None
    return SwarmOrchestrator(host_id, host_wallet, project_name, config)


async def quick_swarm(
    host_id: str,
    host_wallet: str,
    agents: List[Dict[str, Any]],
    tasks: List[Dict[str, Any]],
    budget: Decimal = Decimal("1000"),
) -> Dict[str, Any]:
    """
    Quick helper to run a swarm for a set of tasks.

    Example:
        result = await quick_swarm(
            host_id="alice",
            host_wallet="0x...",
            agents=[
                {"name": "researcher", "capabilities": ["search"]},
                {"name": "coder", "capabilities": ["code"]},
            ],
            tasks=[
                {"type": "search", "query": "AI trends 2024"},
                {"type": "code", "spec": "summarize findings"},
            ],
        )
    """
    orchestrator = create_orchestrator(host_id, host_wallet, "Quick Swarm")

    # Spawn agents
    for agent_spec in agents:
        orchestrator.spawn_agent(
            name=agent_spec["name"],
            capabilities=agent_spec.get("capabilities", []),
            role=AgentRole(agent_spec.get("role", "worker")),
            initial_stake=Decimal(str(agent_spec.get("stake", 0))),
        )

    # Fund
    orchestrator.fund(budget)

    # Execute
    result = await orchestrator.execute_project({
        "name": "Quick Tasks",
        "subtasks": tasks,
        "budget": budget,
    })

    return {
        **result,
        "host_earnings": str(orchestrator.collect_earnings()),
        "status": orchestrator.get_status(),
    }
