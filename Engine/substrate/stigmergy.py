"""
Stigmergic Coordination - Pheromone-based agent coordination

Implements indirect coordination through environmental signals:
- Agents deposit pheromones (weighted edges) on GraphSub
- Other agents read pheromones to guide behavior
- Pheromones decay over time (natural evaporation)
- Multiple agents reinforce successful paths

This enables emergent swarm intelligence without centralized coordination.

Example Use Cases:
- Work distribution: High-intensity pheromone attracts available agents
- Path finding: Agents follow strongest pheromone trails
- Load balancing: Agents avoid saturated (weak pheromone) paths
- Failure recovery: Warning pheromones signal problems
"""

import logging
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import math

from .graphsub_client import GraphSubClient, PheromoneTrail

logger = logging.getLogger(__name__)


@dataclass
class PheromoneSignal:
    """Detected pheromone signal."""
    source_agent_id: str
    target_id: str
    signal_type: str
    current_intensity: float  # Takes decay into account
    original_intensity: float
    deposited_at: str
    time_to_expire_seconds: int


class StigmergicCoordinator:
    """
    Manages pheromone-based coordination for agents.

    Responsibilities:
    - Depositing pheromones after task completion
    - Scanning for high-intensity pheromones
    - Computing effective intensity (accounting for decay)
    - Recommending next actions based on pheromone landscape
    """

    # Pheromone signal types
    SIGNAL_PROGRESS = "progress"  # Task is being worked on
    SIGNAL_ATTRACTION = "attraction"  # "Come do this task"
    SIGNAL_WARNING = "warning"  # "Danger/failure here"
    SIGNAL_SUCCESS = "success"  # "Good path ahead"
    SIGNAL_HELP_NEEDED = "help_needed"  # "I'm stuck"
    SIGNAL_CAPACITY = "capacity"  # "I have free resources"

    # Default decay parameters
    DEFAULT_DECAY_RATE = 0.01  # e^(-0.01 * t)
    DEFAULT_TTL_SECONDS = 300  # 5 minutes
    DEFAULT_INITIAL_INTENSITY = 1.0

    def __init__(self, graphsub_client: GraphSubClient):
        """
        Initialize stigmergic coordinator.

        Args:
            graphsub_client: GraphSub client for pheromone operations
        """
        self.graphsub = graphsub_client

    async def deposit_progress_pheromone(
        self,
        agent_id: str,
        task_id: str,
        progress_fraction: float,  # 0.0 to 1.0
        estimated_remaining_seconds: int
    ) -> bool:
        """
        Deposit progress pheromone while executing a task.

        Signals: "Task in progress at X% completion, ~Y seconds remaining"
        This helps other agents decide whether to help or wait.

        Args:
            agent_id: Agent executing task
            task_id: Task being executed
            progress_fraction: Fraction of task complete (0.0 to 1.0)
            estimated_remaining_seconds: Estimated seconds until completion

        Returns:
            True if deposited successfully
        """
        # Intensity decreases as task nears completion
        # High intensity = far from done = potential for help
        intensity = (1.0 - progress_fraction) * self.DEFAULT_INITIAL_INTENSITY

        # TTL extends as more time is needed
        ttl_seconds = max(estimated_remaining_seconds * 2, 60)

        trail = PheromoneTrail(
            signal_type=self.SIGNAL_PROGRESS,
            intensity=intensity,
            decay_rate=self.DEFAULT_DECAY_RATE,
            ttl_seconds=ttl_seconds
        )

        return await self.graphsub.deposit_pheromone(agent_id, task_id, trail)

    async def deposit_attraction_pheromone(
        self,
        agent_id: str,
        task_id: str,
        attractiveness: float = 1.0,  # 0.0 to 1.0
        ttl_seconds: int = None
    ) -> bool:
        """
        Deposit attraction pheromone to encourage others to take a task.

        Used when agent completes a task or discovers valuable work.

        Args:
            agent_id: Agent depositing signal
            task_id: Task to attract others to
            attractiveness: How attractive this task is (0.0-1.0)
            ttl_seconds: How long before pheromone expires

        Returns:
            True if deposited successfully
        """
        trail = PheromoneTrail(
            signal_type=self.SIGNAL_ATTRACTION,
            intensity=attractiveness,
            decay_rate=self.DEFAULT_DECAY_RATE,
            ttl_seconds=ttl_seconds or self.DEFAULT_TTL_SECONDS
        )

        return await self.graphsub.deposit_pheromone(agent_id, task_id, trail)

    async def deposit_warning_pheromone(
        self,
        agent_id: str,
        target_id: str,
        severity: float = 0.8,  # 0.0 to 1.0
        reason: str = "Unknown"
    ) -> bool:
        """
        Deposit warning pheromone to signal failures or dangers.

        Other agents will avoid high-intensity warning pheromones.

        Args:
            agent_id: Agent issuing warning
            target_id: What to warn about (task, node, etc.)
            severity: How severe (0.0-1.0)
            reason: Human-readable reason

        Returns:
            True if deposited successfully
        """
        trail = PheromoneTrail(
            signal_type=self.SIGNAL_WARNING,
            intensity=severity,
            decay_rate=self.DEFAULT_DECAY_RATE * 0.5,  # Warnings decay slower
            ttl_seconds=600  # 10 minutes
        )

        return await self.graphsub.deposit_pheromone(agent_id, target_id, trail)

    async def deposit_capacity_signal(
        self,
        agent_id: str,
        available_capacity: float,  # 0.0 to 1.0
        ttl_seconds: int = 60
    ) -> bool:
        """
        Signal that this agent has available capacity for work.

        Other agents can find available workers by scanning capacity pheromones.

        Args:
            agent_id: Agent with available capacity
            available_capacity: Fraction of capacity available (0.0-1.0)
            ttl_seconds: How long signal remains valid

        Returns:
            True if signal deposited
        """
        trail = PheromoneTrail(
            signal_type=self.SIGNAL_CAPACITY,
            intensity=available_capacity,
            decay_rate=0.05,  # Capacity signals decay quickly (availability changes)
            ttl_seconds=ttl_seconds
        )

        return await self.graphsub.deposit_pheromone(agent_id, agent_id, trail)

    async def scan_attractive_tasks(
        self,
        agent_id: str,
        required_capabilities: List[str],
        limit: int = 10
    ) -> List[Tuple[str, float]]:
        """
        Scan for tasks with high attraction pheromones that match capabilities.

        Args:
            agent_id: Scanning agent (to exclude own signals)
            required_capabilities: Agent's capabilities
            limit: Maximum tasks to return

        Returns:
            List of (task_id, attraction_score) sorted by score descending
        """
        try:
            tasks = await self.graphsub.scan_high_pheromone_targets(
                signal_type=self.SIGNAL_ATTRACTION,
                agent_id=agent_id,
                limit=limit
            )
            return tasks
        except Exception as e:
            logger.error(f"Failed to scan attractive tasks: {e}")
            return []

    async def scan_available_workers(
        self,
        task_id: str,
        limit: int = 5
    ) -> List[Tuple[str, float]]:
        """
        Find agents with available capacity to help with a task.

        Args:
            task_id: Task needing help
            limit: Maximum workers to return

        Returns:
            List of (agent_id, available_capacity) sorted by capacity descending
        """
        try:
            workers = await self.graphsub.scan_high_pheromone_targets(
                signal_type=self.SIGNAL_CAPACITY,
                limit=limit
            )
            return workers
        except Exception as e:
            logger.error(f"Failed to scan available workers: {e}")
            return []

    async def detect_dangerous_paths(
        self,
        agent_id: str,
        limit: int = 10
    ) -> List[Tuple[str, float]]:
        """
        Scan for tasks with warning pheromones (to avoid).

        Args:
            agent_id: Scanning agent
            limit: Maximum warnings to return

        Returns:
            List of (target_id, warning_severity) sorted by severity descending
        """
        try:
            warnings = await self.graphsub.scan_high_pheromone_targets(
                signal_type=self.SIGNAL_WARNING,
                agent_id=agent_id,
                limit=limit
            )
            return warnings
        except Exception as e:
            logger.error(f"Failed to detect dangerous paths: {e}")
            return []

    def compute_decay(
        self,
        initial_intensity: float,
        deposited_at: str,
        decay_rate: float,
        current_time: Optional[str] = None
    ) -> float:
        """
        Compute effective pheromone intensity accounting for exponential decay.

        Formula: intensity(t) = intensity_0 * e^(-decay_rate * elapsed_seconds)

        Args:
            initial_intensity: Initial pheromone intensity
            deposited_at: When pheromone was deposited (ISO format)
            decay_rate: Exponential decay rate
            current_time: Current time (default: now)

        Returns:
            Effective intensity after decay
        """
        try:
            deposited = datetime.fromisoformat(deposited_at)
            now = (
                datetime.fromisoformat(current_time)
                if current_time
                else datetime.utcnow()
            )

            elapsed_seconds = (now - deposited).total_seconds()
            decayed_intensity = initial_intensity * math.exp(-decay_rate * elapsed_seconds)

            return max(0.0, decayed_intensity)
        except Exception as e:
            logger.error(f"Failed to compute decay: {e}")
            return 0.0

    async def recommend_next_action(
        self,
        agent_id: str,
        current_task_id: Optional[str] = None,
        required_capabilities: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Recommend next action based on pheromone landscape.

        Strategy:
        1. Check if current task has help available (capacity signals)
        2. Check for high-priority attractive tasks
        3. Check for warnings to avoid
        4. Recommend action based on environmental signals

        Args:
            agent_id: This agent
            current_task_id: Currently executing task (if any)
            required_capabilities: Agent's capabilities

        Returns:
            Recommended action dict, or None if no clear recommendation
        """
        recommendation = {
            "action": None,  # "continue", "seek_help", "switch_task", "rest"
            "target_id": None,
            "confidence": 0.0,
            "reasoning": ""
        }

        try:
            # Check for help availability on current task
            if current_task_id:
                helpers = await self.scan_available_workers(current_task_id, limit=1)
                if helpers and helpers[0][1] > 0.5:
                    recommendation["action"] = "seek_help"
                    recommendation["target_id"] = helpers[0][0]
                    recommendation["confidence"] = helpers[0][1]
                    recommendation["reasoning"] = f"Found helper with {helpers[0][1]:.0%} capacity"
                    return recommendation

            # Check for attractive tasks
            attractive_tasks = await self.scan_attractive_tasks(
                agent_id,
                required_capabilities or [],
                limit=3
            )
            if attractive_tasks:
                task_id, attraction = attractive_tasks[0]
                recommendation["action"] = "switch_task"
                recommendation["target_id"] = task_id
                recommendation["confidence"] = min(attraction / 10.0, 1.0)  # Normalize
                recommendation["reasoning"] = f"Found attractive task with {attraction:.1f} pheromone intensity"
                return recommendation

            # No strong signals
            recommendation["action"] = "rest"
            recommendation["reasoning"] = "No strong pheromone signals detected"
            return recommendation

        except Exception as e:
            logger.error(f"Failed to recommend action: {e}")
            return None
