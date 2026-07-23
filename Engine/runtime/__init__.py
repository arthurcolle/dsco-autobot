"""
Agent Runtime Layer

Implements the core agent execution engine using OODA loop (Observe-Orient-Decide-Act).

Key components:
- AgentExecutor: Main agent runtime, manages OODA tick loop
- CapabilityMatcher: Matches tasks to agent capabilities with learning
- ResourceManager: Manages budget, GPU allocation, memory quotas
- AgentPool: Manages multiple agents, health checks, auto-scaling
- MultiAgentWorkflow: Coordinates multi-agent workflows (fan-out, sequential, consensus, pipeline)

This layer runs on top of the coordination substrate and coordinates with
Temporal workflows for durable execution.
"""

from .agent_executor import (
    AgentExecutor,
    AgentAction,
    ExecutionContext,
    ActionDecision,
)

from .capability_matcher import (
    CapabilityMatcher,
    CapabilityProfile,
    CapabilityScore,
    MatchResult,
    MatchStrategy,
    CapabilityTier,
)

from .resource_manager import (
    ResourceManager,
    ResourceEstimate,
    ResourceQuota,
    Reservation,
    ReservationStatus,
    ResourceType,
)

from .agent_pool import (
    AgentPool,
    PoolConfig,
    PoolScalingPolicy,
    PoolMetrics,
    AgentHealthStatus,
    AgentHealthCheckResult,
)

from .multi_agent_workflows import (
    MultiAgentWorkflow,
    WorkflowTask,
    WorkflowPattern,
    WorkflowResult,
    SubTask,
    TaskDependency,
    WorkflowBuilder,
)

__all__ = [
    # Agent Executor
    "AgentExecutor",
    "AgentAction",
    "ExecutionContext",
    "ActionDecision",
    # Capability Matcher
    "CapabilityMatcher",
    "CapabilityProfile",
    "CapabilityScore",
    "MatchResult",
    "MatchStrategy",
    "CapabilityTier",
    # Resource Manager
    "ResourceManager",
    "ResourceEstimate",
    "ResourceQuota",
    "Reservation",
    "ReservationStatus",
    "ResourceType",
    # Agent Pool
    "AgentPool",
    "PoolConfig",
    "PoolScalingPolicy",
    "PoolMetrics",
    "AgentHealthStatus",
    "AgentHealthCheckResult",
    # Multi-Agent Workflows
    "MultiAgentWorkflow",
    "WorkflowTask",
    "WorkflowPattern",
    "WorkflowResult",
    "SubTask",
    "TaskDependency",
    "WorkflowBuilder",
]
