"""Advanced task orchestration system for Parallel.ai integration.

This module provides a sophisticated task execution engine with:
- Automatic polling with adaptive intervals
- Circuit breaker pattern for resilience
- Exponential backoff with jitter
- Concurrent task execution with semaphores
- Priority-based task queuing
- Intelligent load balancing and distribution
- Processor health monitoring
- Comprehensive error handling and recovery
- State persistence and resumability
- Rich telemetry and observability
"""

from parallel_websearch_server.orchestration.orchestrator import TaskOrchestrator
from parallel_websearch_server.orchestration.models import (
    TaskExecutionRequest,
    TaskExecutionResult,
    TaskPriority,
    TaskState,
    OrchestrationConfig,
    TaskQueueStats,
)
from parallel_websearch_server.orchestration.circuit_breaker import CircuitBreaker
from parallel_websearch_server.orchestration.distributor import (
    TaskDistributor,
    DistributionTask,
    ProcessorMetrics,
    ProcessorHealth,
    BatchConfig,
)
from parallel_websearch_server.orchestration.dag import (
    TaskDAG,
    DAGNode,
    TaskStatus,
    DAGExecutionError,
    CyclicDependencyError,
)
from parallel_websearch_server.orchestration.dag_executor import DAGExecutor

__all__ = [
    "TaskOrchestrator",
    "TaskExecutionRequest",
    "TaskExecutionResult",
    "TaskPriority",
    "TaskState",
    "OrchestrationConfig",
    "TaskQueueStats",
    "CircuitBreaker",
    "TaskDistributor",
    "DistributionTask",
    "ProcessorMetrics",
    "ProcessorHealth",
    "BatchConfig",
    "TaskDAG",
    "DAGNode",
    "TaskStatus",
    "DAGExecutionError",
    "CyclicDependencyError",
    "DAGExecutor",
]
