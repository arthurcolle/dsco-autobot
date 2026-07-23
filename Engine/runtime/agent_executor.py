"""
Agent Executor - Core Agent Runtime

Implements the OODA Loop (Observe-Orient-Decide-Act) for autonomous agents.

Each agent runs this loop continuously:
1. OBSERVE: Check mailbox for new tasks, read environment state
2. ORIENT: Assess own capabilities, check budget, evaluate situation
3. DECIDE: Choose next action based on capabilities and pheromones
4. ACT: Execute task, update state, deposit pheromones

Agents coordinate through stigmergic signals (pheromones) in GraphSub,
not through direct messages or central coordination.
"""

import asyncio
import logging
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
import uuid

from temporalio.client import Client as TemporalClient
from temporalio.exceptions import WorkflowAlreadyStartedError

from substrate.graphsub_client import (
    GraphSubClient,
    PheromoneTrail
)
from substrate.redis_mailbox import RedisMailbox, Message
from substrate.stigmergy import StigmergicCoordinator
from substrate.capability_tokens import CapabilityTokenManager, Permissions
from .capability_matcher import (
    CapabilityMatcher,
    MatchStrategy,
)
from .resource_manager import (
    ResourceManager,
    ResourceEstimate,
)

logger = logging.getLogger(__name__)


class AgentAction(Enum):
    """Possible agent actions in the OODA loop."""
    EXECUTE = "execute"          # Execute a task
    DELEGATE = "delegate"        # Ask another agent for help
    WAIT = "wait"                # Wait for conditions to improve
    REST = "rest"                # No work available, idle
    ESCALATE = "escalate"        # Send to human review


@dataclass
class ExecutionContext:
    """Context for a single task execution."""
    task_id: str
    task_type: str
    task_data: Dict[str, Any]
    required_capabilities: List[str]
    estimated_cost: float
    priority: int
    received_at: str
    deadline: Optional[str] = None
    estimated_duration_seconds: float = 30.0

    def age_seconds(self) -> float:
        """How long since task was received."""
        received = datetime.fromisoformat(self.received_at)
        elapsed = datetime.utcnow() - received
        return elapsed.total_seconds()

    def is_overdue(self) -> bool:
        """Check if task has passed deadline."""
        if not self.deadline:
            return False
        deadline = datetime.fromisoformat(self.deadline)
        return datetime.utcnow() > deadline


@dataclass
class ActionDecision:
    """Decision made during Orient-Decide phase."""
    action: AgentAction
    confidence: float  # 0.0 to 1.0
    reasoning: str
    target_id: Optional[str] = None
    estimated_cost: float = 0.0


class AgentExecutor:
    """
    Core agent runtime implementing the OODA loop.

    Manages:
    - Task consumption from Redis mailbox
    - Capability assessment and matching
    - Budget tracking and resource allocation
    - Task execution via Temporal workflows
    - Pheromone coordination and learning
    - State management across restarts
    """

    def __init__(
        self,
        agent_id: str,
        model: str = "claude-3.5-sonnet",
        capabilities: Optional[Dict[str, float]] = None,
        budget: float = 100.0,
        temporal_url: str = "localhost:7233",
        graphsub_url: str = "http://localhost:7879",
        redis_url: str = "redis://localhost:6379",
        pool_name: str = "default",
        tick_interval_seconds: float = 1.0
    ):
        """
        Initialize agent executor.

        Args:
            agent_id: Unique agent identifier
            model: LLM model name
            capabilities: Dict of skill -> confidence level (0.0-1.0)
            budget: Starting API credit budget
            temporal_url: Temporal server address
            graphsub_url: GraphSub server URL
            redis_url: Redis connection URL
            pool_name: Agent pool name (for consumer groups)
            tick_interval_seconds: Time between OODA loop iterations
        """
        self.agent_id = agent_id
        self.model = model
        self.pool_name = pool_name
        self.tick_interval_seconds = tick_interval_seconds

        # Capabilities (learned/updated over time)
        self.capabilities = capabilities or {
            "general": 0.5,  # Default base capability
        }

        # Budget tracking
        self.budget_remaining = budget
        self.budget_spent = 0.0

        # Clients (initialized in connect())
        self.temporal_client: Optional[TemporalClient] = None
        self.graphsub_client = GraphSubClient(graphsub_url)
        self.mailbox = RedisMailbox(redis_url, agent_id=agent_id, pool_name=pool_name)
        self.coordinator = StigmergicCoordinator(self.graphsub_client)
        self.matcher = CapabilityMatcher(self.graphsub_client)
        self.resource_manager = ResourceManager(self.graphsub_client)
        self.token_manager = CapabilityTokenManager(agent_id)

        # State tracking
        self.current_task: Optional[ExecutionContext] = None
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.last_heartbeat = datetime.utcnow()
        self.running = False

        logger.info(
            f"Initialized AgentExecutor: {agent_id} "
            f"(model={model}, budget=${budget}, capabilities={len(self.capabilities)})"
        )

    async def connect(self):
        """Establish connections to all services."""
        try:
            # Connect to Temporal
            self.temporal_client = await TemporalClient.connect("localhost:7233")
            logger.info("Connected to Temporal")

            # Connect to Redis
            await self.mailbox.connect()
            logger.info("Connected to Redis")

            # Verify GraphSub
            if not await self.graphsub_client.health_check():
                logger.warning("GraphSub health check failed")

            # Register agent in GraphSub
            await self.register_agent()

            logger.info(f"Agent {self.agent_id} connected to all services")
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def register_agent(self):
        """Register this agent in GraphSub."""
        try:
            agent_node = await self.graphsub_client.create_agent_node(
                agent_id=self.agent_id,
                model=self.model,
                budget_remaining=self.budget_remaining,
                capabilities=self.capabilities
            )
            logger.info(f"Registered agent in GraphSub: {agent_node.id}")
        except Exception as e:
            logger.error(f"Failed to register agent: {e}")
            # Non-fatal, continue anyway

    async def run(self):
        """
        Run the agent's OODA loop continuously.

        This is the main execution function for the agent.
        """
        self.running = True
        logger.info(f"Starting OODA loop for {self.agent_id}")

        try:
            while self.running:
                try:
                    # Single OODA iteration
                    await self.ooda_tick()

                    # Sleep between ticks
                    await asyncio.sleep(self.tick_interval_seconds)
                except Exception as e:
                    logger.error(f"Error in OODA tick: {e}", exc_info=True)
                    # Continue despite errors
                    await asyncio.sleep(self.tick_interval_seconds)

        except KeyboardInterrupt:
            logger.info(f"Agent {self.agent_id} interrupted")
        finally:
            await self.shutdown()

    async def ooda_tick(self):
        """
        Single OODA loop iteration.

        1. OBSERVE: Check mailbox, read environment
        2. ORIENT: Assess capabilities, budget, situation
        3. DECIDE: Choose action
        4. ACT: Execute action
        """
        # 1. OBSERVE
        observed_task, observed_signals = await self.observe()

        # 2. ORIENT
        assessment = await self.orient(observed_task, observed_signals)

        # 3. DECIDE
        decision = await self.decide(observed_task, assessment)

        # 4. ACT
        await self.act(decision, observed_task)

        # Send heartbeat
        await self.send_heartbeat()

    async def observe(self) -> Tuple[Optional[ExecutionContext], Dict[str, Any]]:
        """
        OBSERVE phase: Check mailbox and read environment state.

        Returns:
            (task, environment_signals) - Task if available, and observed signals
        """
        task = None
        signals = {}

        try:
            # Check mailbox for tasks
            # This is async, but non-blocking
            mailbox = self.mailbox
            async for message in mailbox.consume_tasks(
                self.agent_id,
                pool_name=self.pool_name,
                block_ms=100,
                max_batch_size=1
            ):
                if message.type == "task":
                    task = ExecutionContext(
                        task_id=message.payload.get("task_id", str(uuid.uuid4())),
                        task_type=message.payload.get("type", "unknown"),
                        task_data=message.payload,
                        required_capabilities=message.payload.get("required_capabilities", []),
                        estimated_cost=message.payload.get("estimated_cost", 0.0),
                        priority=message.priority,
                        received_at=message.created_at or datetime.utcnow().isoformat(),
                        deadline=message.payload.get("deadline")
                    )
                    self.current_task = task
                    logger.debug(f"Observed task: {task.task_id} (priority={task.priority})")
                    break

            # Scan for pheromone signals
            if not task:
                attractive_tasks = await self.coordinator.scan_attractive_tasks(
                    agent_id=self.agent_id,
                    required_capabilities=list(self.capabilities.keys()),
                    limit=3
                )
                if attractive_tasks:
                    signals["attractive_tasks"] = attractive_tasks

                dangerous_paths = await self.coordinator.detect_dangerous_paths(
                    agent_id=self.agent_id,
                    limit=3
                )
                if dangerous_paths:
                    signals["dangerous_paths"] = dangerous_paths

            return task, signals

        except Exception as e:
            logger.error(f"Error in observe phase: {e}")
            return None, {}

    async def orient(
        self,
        task: Optional[ExecutionContext],
        signals: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        ORIENT phase: Assess capabilities, budget, and situation.

        Returns:
            Assessment dict with capability match, cost estimate, urgency, etc.
        """
        assessment = {
            "has_task": task is not None,
            "can_afford": False,
            "capability_match": 0.0,
            "urgency": 0,
            "risk_level": 0
        }

        if not task:
            return assessment

        try:
            # Check capability match using CapabilityMatcher
            can_exec, match_result = await self.matcher.can_execute(
                self.agent_id,
                task.required_capabilities or [],
                min_confidence=0.3,
                strategy=MatchStrategy.FUZZY
            )

            assessment["capability_match"] = match_result.match_score
            assessment["confidence_tier"] = match_result.confidence_tier.name
            assessment["match_reasoning"] = match_result.reasoning

            # Check if we can afford it using ResourceManager
            estimate = ResourceEstimate(
                task_id=task.task_id,
                api_credit_cost=task.estimated_cost,
                duration_seconds=task.estimated_duration_seconds or 30.0
            )
            can_afford, afford_reason = await self.resource_manager.can_afford(
                self.agent_id,
                estimate
            )
            assessment["can_afford"] = can_afford
            assessment["affordability_reason"] = afford_reason

            # Calculate urgency (older tasks = higher urgency)
            age = task.age_seconds()
            assessment["urgency"] = min(age / 60.0, 1.0)  # 0.0-1.0

            # Calculate risk (dangerous paths + missing capabilities)
            dangerous = len(signals.get("dangerous_paths", []))
            missing_capabilities = 1.0 - capability_match
            assessment["risk_level"] = min((dangerous + missing_capabilities) / 2.0, 1.0)

            logger.debug(
                f"Oriented task {task.task_id}: "
                f"match={assessment['capability_match']:.2f}, "
                f"afford={assessment['can_afford']}, "
                f"urgency={assessment['urgency']:.2f}"
            )

            return assessment

        except Exception as e:
            logger.error(f"Error in orient phase: {e}")
            return assessment

    async def decide(
        self,
        task: Optional[ExecutionContext],
        assessment: Dict[str, Any]
    ) -> ActionDecision:
        """
        DECIDE phase: Choose next action based on assessment.

        Returns:
            ActionDecision with chosen action and reasoning
        """
        if not task:
            # No task observed
            return ActionDecision(
                action=AgentAction.REST,
                confidence=1.0,
                reasoning="No tasks in mailbox"
            )

        # Decision criteria
        can_afford = assessment["can_afford"]
        capability_match = assessment["capability_match"]
        urgency = assessment["urgency"]
        risk_level = assessment["risk_level"]

        # High capability match + can afford = EXECUTE
        if capability_match >= 0.8 and can_afford:
            return ActionDecision(
                action=AgentAction.EXECUTE,
                confidence=capability_match,
                reasoning=f"High capability match ({capability_match:.1%}), can afford",
                target_id=task.task_id,
                estimated_cost=task.estimated_cost
            )

        # Medium capability match, low risk, can afford = EXECUTE cautiously
        if capability_match >= 0.5 and can_afford and risk_level < 0.3:
            return ActionDecision(
                action=AgentAction.EXECUTE,
                confidence=capability_match * 0.8,
                reasoning=f"Medium capability match ({capability_match:.1%}), manageable risk",
                target_id=task.task_id,
                estimated_cost=task.estimated_cost
            )

        # Can't afford = WAIT
        if not can_afford:
            return ActionDecision(
                action=AgentAction.WAIT,
                confidence=0.8,
                reasoning=f"Cannot afford (need ${task.estimated_cost}, have ${self.budget_remaining})",
                target_id=task.task_id
            )

        # Low match + high risk = DELEGATE
        if capability_match < 0.5 and risk_level > 0.5:
            return ActionDecision(
                action=AgentAction.DELEGATE,
                confidence=0.7,
                reasoning=f"Low capability match ({capability_match:.1%}), high risk, seeking help",
                target_id=task.task_id
            )

        # Low match = ESCALATE
        if capability_match < 0.3:
            return ActionDecision(
                action=AgentAction.ESCALATE,
                confidence=0.9,
                reasoning=f"Very low capability match ({capability_match:.1%}), need human review",
                target_id=task.task_id
            )

        # Default: WAIT
        return ActionDecision(
            action=AgentAction.WAIT,
            confidence=0.5,
            reasoning="Uncertain conditions, waiting",
            target_id=task.task_id
        )

    async def act(self, decision: ActionDecision, task: Optional[ExecutionContext]):
        """
        ACT phase: Execute the chosen action.

        Args:
            decision: ActionDecision from decide phase
            task: Task being acted upon (if any)
        """
        try:
            if decision.action == AgentAction.EXECUTE and task:
                await self.execute_task(task, decision)

            elif decision.action == AgentAction.DELEGATE and task:
                await self.delegate_task(task, decision)

            elif decision.action == AgentAction.ESCALATE and task:
                await self.escalate_task(task, decision)

            elif decision.action == AgentAction.WAIT and task:
                # Deposit help-seeking pheromone
                trail = PheromoneTrail(
                    signal_type="help_needed",
                    intensity=1.0 - decision.confidence,
                    decay_rate=0.02,
                    ttl_seconds=60
                )
                await self.graphsub_client.deposit_pheromone(
                    self.agent_id, task.task_id, trail
                )
                logger.debug(f"Deposited help-seeking pheromone for {task.task_id}")

            elif decision.action == AgentAction.REST:
                # Deposit capacity signal
                await self.coordinator.deposit_capacity_signal(
                    agent_id=self.agent_id,
                    available_capacity=1.0,
                    ttl_seconds=30
                )
                logger.debug("Deposited capacity signal (ready for work)")

        except Exception as e:
            logger.error(f"Error in act phase: {e}", exc_info=True)

    async def execute_task(self, task: ExecutionContext, decision: ActionDecision):
        """
        Execute a task via Temporal workflow.

        Args:
            task: Task to execute
            decision: Decision that led to execution
        """
        if not self.temporal_client:
            logger.error("Temporal client not initialized")
            return

        # Reserve resources
        estimate = ResourceEstimate(
            task_id=task.task_id,
            api_credit_cost=task.estimated_cost,
            duration_seconds=task.estimated_duration_seconds
        )
        reservation = await self.resource_manager.reserve(
            self.agent_id,
            task.task_id,
            estimate
        )
        if not reservation:
            logger.error(f"Failed to reserve resources for task {task.task_id}")
            return

        try:
            logger.info(
                f"Executing task {task.task_id} "
                f"(confidence={decision.confidence:.1%}, cost=${task.estimated_cost:.2f})"
            )

            # Update GraphSub
            await self.graphsub_client.update_agent_status(
                self.agent_id,
                status="executing",
                budget_remaining=self.budget_remaining
            )

            # Deposit progress pheromone
            trail = PheromoneTrail(
                signal_type="progress",
                intensity=1.0,
                decay_rate=0.01,
                ttl_seconds=300
            )
            await self.graphsub_client.deposit_pheromone(
                self.agent_id, task.task_id, trail
            )

            # Create task node in GraphSub
            task_node = await self.graphsub_client.create_task_node(
                task_id=task.task_id,
                task_type=task.task_type,
                priority=task.priority,
                required_capabilities=task.required_capabilities,
                estimated_cost_usd=task.estimated_cost
            )

            # In a real implementation, this would:
            # 1. Call a Temporal workflow with the task
            # 2. Wait for completion
            # 3. Handle success/failure
            # 4. Update GraphSub with results
            # 5. Learn from outcomes

            # For now, simulate success
            self.tasks_completed += 1
            logger.info(f"Task {task.task_id} completed (simulated)")

            # Complete resource reservation
            await self.resource_manager.complete(reservation)

            # Update capability scores based on success
            for capability in task.required_capabilities or []:
                await self.matcher.update_capability_score(
                    self.agent_id,
                    capability,
                    success=True
                )

            # Deposit success pheromone
            success_trail = PheromoneTrail(
                signal_type="success",
                intensity=0.9,
                decay_rate=0.01,
                ttl_seconds=600
            )
            await self.graphsub_client.deposit_pheromone(
                self.agent_id, task.task_id, success_trail
            )

            # Update agent status
            await self.graphsub_client.update_agent_status(
                self.agent_id,
                status="idle",
                budget_remaining=self.budget_remaining
            )

            self.current_task = None

        except Exception as e:
            logger.error(f"Failed to execute task {task.task_id}: {e}")
            self.tasks_failed += 1

            # Mark reservation as failed with 100% refund
            await self.resource_manager.fail(reservation, refund_percentage=1.0)

            # Update capability scores based on failure
            for capability in task.required_capabilities or []:
                await self.matcher.update_capability_score(
                    self.agent_id,
                    capability,
                    success=False
                )

    async def delegate_task(self, task: ExecutionContext, decision: ActionDecision):
        """
        Delegate a task to another agent.

        Args:
            task: Task to delegate
            decision: Decision that led to delegation
        """
        logger.info(f"Delegating task {task.task_id}")

        # Deposit help-needed pheromone with high intensity
        trail = PheromoneTrail(
            signal_type="help_needed",
            intensity=0.9,
            decay_rate=0.02,
            ttl_seconds=300
        )
        await self.graphsub_client.deposit_pheromone(
            self.agent_id, task.task_id, trail
        )

        # In a real implementation, would broadcast to agent pool
        # For now, just mark as delegated
        self.current_task = None

    async def escalate_task(self, task: ExecutionContext, decision: ActionDecision):
        """
        Escalate a task for human review.

        Args:
            task: Task to escalate
            decision: Decision that led to escalation
        """
        logger.info(f"Escalating task {task.task_id} for human review")

        # Mark in GraphSub
        await self.graphsub_client.update_agent_status(
            self.agent_id,
            status="waiting_for_human"
        )

        self.current_task = None

    async def send_heartbeat(self):
        """Send heartbeat signal to indicate agent is alive."""
        self.last_heartbeat = datetime.utcnow()

        # Update agent status in GraphSub
        await self.graphsub_client.update_agent_status(
            self.agent_id,
            status="idle" if not self.current_task else "executing"
        )

    async def shutdown(self):
        """Gracefully shutdown the agent."""
        self.running = False
        logger.info(f"Shutting down {self.agent_id}")

        try:
            await self.mailbox.close()
            await self.graphsub_client.close()

            if self.temporal_client:
                await self.temporal_client.close()

            logger.info(
                f"Agent {self.agent_id} shutdown complete. "
                f"Completed: {self.tasks_completed}, Failed: {self.tasks_failed}"
            )
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


async def run_agent(
    agent_id: str,
    model: str = "claude-3.5-sonnet",
    capabilities: Optional[Dict[str, float]] = None,
    budget: float = 100.0
):
    """
    Run a single agent with the OODA loop.

    Args:
        agent_id: Unique agent identifier
        model: LLM model to use
        capabilities: Dict of skill -> confidence level
        budget: Starting API credit budget
    """
    executor = AgentExecutor(
        agent_id=agent_id,
        model=model,
        capabilities=capabilities,
        budget=budget
    )

    await executor.connect()
    await executor.run()
