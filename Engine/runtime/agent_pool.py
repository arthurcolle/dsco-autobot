"""
Agent Pool Management

Manages lifecycle, health, and coordination of agent collections.
Handles spawning, pruning, health checking, and distributed task allocation.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta

from .agent_executor import AgentExecutor
from substrate.graphsub_client import GraphSubClient
from substrate.redis_mailbox import RedisMailbox

logger = logging.getLogger(__name__)


class AgentHealthStatus(Enum):
    """Health status of an agent."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DEAD = "dead"


class PoolScalingPolicy(Enum):
    """Policy for scaling agent pool."""
    FIXED = "fixed"              # Fixed number of agents
    MANUAL = "manual"            # Manual scaling only
    AUTO_LOAD = "auto_load"      # Scale based on queue depth
    AUTO_RESOURCE = "auto_resource"  # Scale based on available resources


@dataclass
class AgentHealthCheckResult:
    """Result of agent health check."""
    agent_id: str
    status: AgentHealthStatus
    last_heartbeat: Optional[datetime] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    budget_remaining: float = 0.0
    error_message: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.tasks_completed + self.tasks_failed
        if total == 0:
            return 0.0
        return self.tasks_completed / total


@dataclass
class PoolMetrics:
    """Metrics for agent pool."""
    total_agents: int = 0
    healthy_agents: int = 0
    degraded_agents: int = 0
    unhealthy_agents: int = 0
    tasks_pending: int = 0
    tasks_executing: int = 0
    total_budget_remaining: float = 0.0
    avg_capability_confidence: float = 0.0
    pool_capacity_utilization: float = 0.0  # 0.0 to 1.0

    @property
    def available_agents(self) -> int:
        """Agents available for work."""
        return self.healthy_agents

    @property
    def degradation_percentage(self) -> float:
        """Percentage of agents not healthy."""
        if self.total_agents == 0:
            return 0.0
        return (self.degraded_agents + self.unhealthy_agents) / self.total_agents * 100


@dataclass
class PoolConfig:
    """Configuration for agent pool."""
    pool_name: str
    min_agents: int = 1
    max_agents: int = 10
    scaling_policy: PoolScalingPolicy = PoolScalingPolicy.FIXED
    health_check_interval_seconds: float = 30.0
    heartbeat_timeout_seconds: float = 60.0
    auto_scale_threshold: float = 0.8  # Scale up if utilization > 80%
    auto_scale_down_threshold: float = 0.2  # Scale down if < 20%
    agent_idle_timeout_seconds: float = 300.0  # Prune if idle > 5 min
    default_agent_budget: float = 100.0
    default_agent_model: str = "claude-3.5-sonnet"
    default_agent_capabilities: Dict[str, float] = field(default_factory=lambda: {
        "general": 0.5,
        "python": 0.7,
        "search": 0.6,
    })


class AgentPool:
    """
    Manages a pool of autonomous agents.

    Features:
    - Agent lifecycle (spawn, prune, health check)
    - Pool-level metrics and monitoring
    - Automatic scaling based on policy
    - Coordinated resource allocation
    - Health tracking and degradation handling
    """

    def __init__(
        self,
        graphsub_client: GraphSubClient,
        config: PoolConfig,
        temporal_url: str = "localhost:7233",
        graphsub_url: str = "http://localhost:7879",
        redis_url: str = "redis://localhost:6379",
    ):
        """
        Initialize agent pool.

        Args:
            graphsub_client: GraphSub client for state persistence
            config: Pool configuration
            temporal_url: Temporal server address
            graphsub_url: GraphSub server URL
            redis_url: Redis connection URL
        """
        self.graphsub = graphsub_client
        self.config = config
        self.temporal_url = temporal_url
        self.graphsub_url = graphsub_url
        self.redis_url = redis_url

        # Agent registry
        self._agents: Dict[str, AgentExecutor] = {}
        self._agent_health: Dict[str, AgentHealthCheckResult] = {}
        self._agent_tasks: Dict[str, int] = {}  # task count per agent

        # Pool state
        self._running = False
        self._last_health_check = datetime.utcnow()
        self._last_scale_check = datetime.utcnow()

        logger.info(
            f"Initialized AgentPool '{config.pool_name}' "
            f"({config.min_agents}-{config.max_agents} agents, "
            f"policy={config.scaling_policy.value})"
        )

    async def start(self) -> None:
        """Start the agent pool."""
        self._running = True
        logger.info(f"Starting AgentPool '{self.config.pool_name}'")

        try:
            # Spawn initial agents
            for i in range(self.config.min_agents):
                await self.spawn_agent()

            # Start background tasks
            asyncio.create_task(self._health_check_loop())
            asyncio.create_task(self._scaling_loop())

            logger.info(f"AgentPool started with {len(self._agents)} agents")

        except Exception as e:
            logger.error(f"Failed to start agent pool: {e}")
            raise

    async def stop(self) -> None:
        """Stop the agent pool gracefully."""
        self._running = False
        logger.info(f"Stopping AgentPool '{self.config.pool_name}'")

        try:
            # Stop all agents
            tasks = []
            for agent in self._agents.values():
                tasks.append(asyncio.create_task(self._stop_agent(agent)))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            logger.info(f"AgentPool stopped. Pruned {len(self._agents)} agents")

        except Exception as e:
            logger.error(f"Error stopping agent pool: {e}")

    async def spawn_agent(self) -> Optional[AgentExecutor]:
        """
        Spawn a new agent in the pool.

        Returns:
            AgentExecutor if successful, None if pool full or error
        """
        # Check max limit
        if len(self._agents) >= self.config.max_agents:
            logger.warning(f"Pool at max capacity ({self.config.max_agents})")
            return None

        try:
            agent_id = f"{self.config.pool_name}-{uuid.uuid4().hex[:8]}"

            agent = AgentExecutor(
                agent_id=agent_id,
                model=self.config.default_agent_model,
                capabilities=self.config.default_agent_capabilities.copy(),
                budget=self.config.default_agent_budget,
                temporal_url=self.temporal_url,
                graphsub_url=self.graphsub_url,
                redis_url=self.redis_url,
                pool_name=self.config.pool_name,
                tick_interval_seconds=1.0,
            )

            # Connect to services
            await agent.connect()

            # Register in pool
            self._agents[agent_id] = agent
            self._agent_health[agent_id] = AgentHealthCheckResult(
                agent_id=agent_id,
                status=AgentHealthStatus.HEALTHY,
                last_heartbeat=datetime.utcnow(),
            )
            self._agent_tasks[agent_id] = 0

            # Start OODA loop in background
            asyncio.create_task(agent.run())

            logger.info(f"Spawned agent {agent_id}")
            return agent

        except Exception as e:
            logger.error(f"Failed to spawn agent: {e}")
            return None

    async def prune_agent(self, agent_id: str) -> bool:
        """
        Remove an agent from the pool.

        Args:
            agent_id: Agent to remove

        Returns:
            True if pruned, False if not found
        """
        if agent_id not in self._agents:
            return False

        try:
            agent = self._agents[agent_id]
            await self._stop_agent(agent)

            del self._agents[agent_id]
            del self._agent_health[agent_id]
            del self._agent_tasks[agent_id]

            logger.info(f"Pruned agent {agent_id}")
            return True

        except Exception as e:
            logger.error(f"Error pruning agent {agent_id}: {e}")
            return False

    async def health_check(self, agent_id: Optional[str] = None) -> Dict[str, AgentHealthCheckResult]:
        """
        Check health of agent(s).

        Args:
            agent_id: Specific agent to check, or None for all

        Returns:
            Dict of agent_id -> health result
        """
        agents_to_check = (
            {agent_id: self._agents[agent_id]}
            if agent_id and agent_id in self._agents
            else self._agents
        )

        results = {}
        for aid, agent in agents_to_check.items():
            try:
                # Check if agent is running
                is_running = agent.running

                # Determine status
                if not is_running:
                    status = AgentHealthStatus.DEAD
                elif agent.tasks_failed > agent.tasks_completed * 2:
                    status = AgentHealthStatus.UNHEALTHY
                elif agent.tasks_failed > 0:
                    status = AgentHealthStatus.DEGRADED
                else:
                    status = AgentHealthStatus.HEALTHY

                result = AgentHealthCheckResult(
                    agent_id=aid,
                    status=status,
                    last_heartbeat=agent.last_heartbeat,
                    tasks_completed=agent.tasks_completed,
                    tasks_failed=agent.tasks_failed,
                    budget_remaining=agent.budget_remaining,
                )

                self._agent_health[aid] = result
                results[aid] = result

            except Exception as e:
                logger.error(f"Error checking health for {aid}: {e}")
                results[aid] = AgentHealthCheckResult(
                    agent_id=aid,
                    status=AgentHealthStatus.UNHEALTHY,
                    error_message=str(e),
                )

        return results

    async def get_metrics(self) -> PoolMetrics:
        """Get current pool metrics."""
        # Health check all agents
        await self.health_check()

        healthy = sum(
            1 for h in self._agent_health.values()
            if h.status == AgentHealthStatus.HEALTHY
        )
        degraded = sum(
            1 for h in self._agent_health.values()
            if h.status == AgentHealthStatus.DEGRADED
        )
        unhealthy = sum(
            1 for h in self._agent_health.values()
            if h.status == AgentHealthStatus.UNHEALTHY
        )

        total_budget = sum(
            h.budget_remaining for h in self._agent_health.values()
        )

        # Calculate average capability confidence
        total_confidence = 0.0
        capability_count = 0
        for agent in self._agents.values():
            for confidence in agent.capabilities.values():
                total_confidence += confidence
                capability_count += 1

        avg_confidence = (
            total_confidence / capability_count if capability_count > 0 else 0.0
        )

        # Calculate utilization
        max_concurrent = self.config.max_agents * self.config.auto_scale_threshold
        active_tasks = sum(self._agent_tasks.values())
        utilization = min(active_tasks / max_concurrent, 1.0) if max_concurrent > 0 else 0.0

        return PoolMetrics(
            total_agents=len(self._agents),
            healthy_agents=healthy,
            degraded_agents=degraded,
            unhealthy_agents=unhealthy,
            tasks_pending=0,  # Would query Redis in real implementation
            tasks_executing=active_tasks,
            total_budget_remaining=total_budget,
            avg_capability_confidence=avg_confidence,
            pool_capacity_utilization=utilization,
        )

    async def select_agent_for_task(
        self,
        required_capabilities: List[str],
        preferred_agent_id: Optional[str] = None,
    ) -> Optional[AgentExecutor]:
        """
        Select best agent for a task.

        Args:
            required_capabilities: Task's required capabilities
            preferred_agent_id: Preferred agent if available

        Returns:
            Selected agent or None if no suitable agent
        """
        # Prefer specified agent if healthy
        if preferred_agent_id and preferred_agent_id in self._agents:
            health = self._agent_health.get(preferred_agent_id)
            if health and health.status == AgentHealthStatus.HEALTHY:
                return self._agents[preferred_agent_id]

        # Find best healthy agent
        best_agent = None
        best_score = -1.0

        for agent_id, agent in self._agents.items():
            health = self._agent_health.get(agent_id)

            # Skip unhealthy agents
            if not health or health.status in (AgentHealthStatus.UNHEALTHY, AgentHealthStatus.DEAD):
                continue

            # Calculate match score
            matched = 0
            for cap in required_capabilities:
                if cap in agent.capabilities:
                    matched += agent.capabilities[cap]

            if required_capabilities:
                score = matched / len(required_capabilities)
            else:
                score = 0.5  # Default for no requirements

            # Penalize loaded agents
            task_load = self._agent_tasks.get(agent_id, 0)
            score *= (1.0 - task_load / self.config.auto_scale_threshold)

            if score > best_score:
                best_agent = agent
                best_score = score

        return best_agent

    async def get_agent_list(
        self,
        status_filter: Optional[AgentHealthStatus] = None,
    ) -> List[Tuple[str, AgentHealthCheckResult]]:
        """
        Get list of agents with optional filtering.

        Args:
            status_filter: Optional health status to filter by

        Returns:
            List of (agent_id, health_result) tuples
        """
        agents = []
        for agent_id, health in self._agent_health.items():
            if status_filter is None or health.status == status_filter:
                agents.append((agent_id, health))
        return agents

    async def get_pool_summary(self) -> Dict:
        """Get summary of pool state."""
        metrics = await self.get_metrics()

        return {
            "pool_name": self.config.pool_name,
            "scaling_policy": self.config.scaling_policy.value,
            "agent_count": metrics.total_agents,
            "min_agents": self.config.min_agents,
            "max_agents": self.config.max_agents,
            "healthy_agents": metrics.healthy_agents,
            "degraded_agents": metrics.degraded_agents,
            "unhealthy_agents": metrics.unhealthy_agents,
            "degradation_percent": metrics.degradation_percentage,
            "capacity_utilization": metrics.pool_capacity_utilization,
            "total_budget": metrics.total_budget_remaining,
            "avg_capability_confidence": metrics.avg_capability_confidence,
            "tasks_executing": metrics.tasks_executing,
            "status": "running" if self._running else "stopped",
        }

    # Private methods

    async def _stop_agent(self, agent: AgentExecutor) -> None:
        """Stop an agent gracefully."""
        try:
            agent.running = False
            # Give it a moment to finish current tick
            await asyncio.sleep(0.5)
            logger.debug(f"Stopped agent {agent.agent_id}")
        except Exception as e:
            logger.error(f"Error stopping agent: {e}")

    async def _health_check_loop(self) -> None:
        """Background task: periodic health checks."""
        while self._running:
            try:
                await asyncio.sleep(self.config.health_check_interval_seconds)

                # Prune dead agents
                dead_agents = await self.get_agent_list(AgentHealthStatus.DEAD)
                for agent_id, _ in dead_agents:
                    await self.prune_agent(agent_id)

                # Handle degraded agents (could try to restart)
                # For now, just log
                degraded = await self.get_agent_list(AgentHealthStatus.DEGRADED)
                if degraded:
                    logger.warning(
                        f"Pool has {len(degraded)} degraded agents"
                    )

            except Exception as e:
                logger.error(f"Error in health check loop: {e}")

    async def _scaling_loop(self) -> None:
        """Background task: automatic scaling based on policy."""
        while self._running:
            try:
                await asyncio.sleep(30.0)  # Check every 30 seconds

                if self.config.scaling_policy == PoolScalingPolicy.AUTO_LOAD:
                    metrics = await self.get_metrics()

                    # Scale up if high utilization
                    if (
                        metrics.pool_capacity_utilization > self.config.auto_scale_threshold
                        and len(self._agents) < self.config.max_agents
                    ):
                        await self.spawn_agent()
                        logger.info(
                            f"Auto-scaled up: {len(self._agents)}/"
                            f"{self.config.max_agents} agents"
                        )

                    # Scale down if low utilization
                    if (
                        metrics.pool_capacity_utilization < self.config.auto_scale_down_threshold
                        and len(self._agents) > self.config.min_agents
                    ):
                        # Prune least healthy agent
                        agents_by_health = sorted(
                            self._agent_health.items(),
                            key=lambda x: (
                                x[1].status.value,
                                -x[1].tasks_completed,
                            ),
                        )
                        if agents_by_health:
                            agent_id_to_prune = agents_by_health[0][0]
                            await self.prune_agent(agent_id_to_prune)
                            logger.info(
                                f"Auto-scaled down: {len(self._agents)}/"
                                f"{self.config.max_agents} agents"
                            )

            except Exception as e:
                logger.error(f"Error in scaling loop: {e}")
