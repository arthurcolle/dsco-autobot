"""Data models for advanced task orchestration."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class TaskPriority(str, Enum):
    """Task execution priority levels."""

    CRITICAL = "critical"  # Execute immediately, bypass queue limits
    HIGH = "high"  # Prioritize over normal tasks
    NORMAL = "normal"  # Default priority
    LOW = "low"  # Execute when resources available
    BACKGROUND = "background"  # Lowest priority, can be preempted


class TaskState(str, Enum):
    """Enhanced task state tracking."""

    PENDING = "pending"  # Queued, not yet submitted
    SUBMITTED = "submitted"  # Sent to Parallel.ai, waiting for queued status
    QUEUED = "queued"  # Acknowledged by Parallel.ai, in their queue
    RUNNING = "running"  # Actively executing
    POLLING = "polling"  # In polling loop, waiting for completion
    COMPLETED = "completed"  # Successfully finished
    FAILED = "failed"  # Task execution failed
    TIMEOUT = "timeout"  # Exceeded time limits
    CANCELLED = "cancelled"  # Explicitly cancelled
    CIRCUIT_OPEN = "circuit_open"  # Circuit breaker tripped, not executing
    RETRYING = "retrying"  # Failed but will retry


class RetryStrategy(str, Enum):
    """Retry behavior strategies."""

    NONE = "none"  # No retries
    EXPONENTIAL = "exponential"  # Exponential backoff
    LINEAR = "linear"  # Linear backoff
    IMMEDIATE = "immediate"  # Retry immediately
    ADAPTIVE = "adaptive"  # Adjust based on failure pattern


@dataclass
class OrchestrationConfig:
    """Configuration for task orchestration behavior."""

    # Polling configuration
    initial_poll_interval: float = 1.0  # Start with 1 second
    max_poll_interval: float = 10.0  # Cap at 10 seconds
    min_poll_interval: float = 0.5  # Minimum 0.5 seconds
    adaptive_polling: bool = True  # Adjust interval based on task progress

    # Timeout configuration
    default_timeout: int = 900  # 15 minutes default
    max_timeout: int = 7200  # 2 hours maximum
    submission_timeout: int = 30  # Timeout for initial submission

    # Retry configuration
    max_retries: int = 3
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retry_base_delay: float = 2.0  # Base delay for exponential backoff
    retry_max_delay: float = 60.0  # Max delay between retries
    retry_jitter: bool = True  # Add randomness to prevent thundering herd

    # Circuit breaker configuration
    circuit_breaker_enabled: bool = True
    circuit_breaker_threshold: int = 5  # Failures before opening
    circuit_breaker_timeout: int = 60  # Seconds before attempting reset
    circuit_breaker_half_open_max_calls: int = 3  # Test calls in half-open state

    # Concurrency configuration
    max_concurrent_tasks: int = 10  # Max parallel task executions
    max_queue_size: int = 100  # Max tasks waiting in queue
    queue_timeout: int = 300  # Max time task can wait in queue

    # Caching configuration
    cache_results: bool = True
    cache_ttl: int = 3600  # 1 hour cache TTL
    deduplicate_tasks: bool = True  # Skip identical in-flight tasks

    # Observability configuration
    enable_metrics: bool = True
    enable_detailed_logging: bool = True
    log_level: str = "INFO"


class TaskExecutionRequest(BaseModel):
    """Request to execute a task with orchestration."""

    # Task definition
    processor: str = Field(..., description="Parallel.ai processor tier")
    input: Union[str, Dict[str, Any]] = Field(..., description="Task input/prompt (string or structured object)")
    task_spec: Optional[Dict[str, Any]] = Field(None, description="Task specification")
    mcp_servers: Optional[List[Dict[str, Any]]] = Field(None, description="MCP server configs")

    # Orchestration options
    priority: TaskPriority = TaskPriority.NORMAL
    timeout: Optional[int] = Field(None, description="Override default timeout")
    context_id: Optional[str] = Field(None, description="Context for state persistence")
    idempotency_key: Optional[str] = Field(None, description="Key for deduplication")

    # Retry options
    retry_strategy: Optional[RetryStrategy] = None
    max_retries: Optional[int] = None

    # Advanced options
    tags: Dict[str, str] = Field(default_factory=dict, description="Metadata tags")
    callback_url: Optional[str] = Field(None, description="Webhook for completion")
    streaming: bool = Field(False, description="Enable progress streaming")


@dataclass
class TaskMetrics:
    """Performance and execution metrics for a task."""

    # Timing metrics
    queued_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    total_duration: Optional[float] = None
    polling_duration: Optional[float] = None
    api_call_duration: Optional[float] = None

    # Execution metrics
    poll_count: int = 0
    retry_count: int = 0
    api_call_count: int = 0
    status_transitions: List[Dict[str, Any]] = field(default_factory=list)

    # Resource metrics
    estimated_cost: Optional[float] = None  # Estimated API cost
    result_size_bytes: Optional[int] = None

    def record_state_transition(self, from_state: TaskState, to_state: TaskState) -> None:
        """Record a state transition with timestamp."""
        self.status_transitions.append({
            "from": from_state.value,
            "to": to_state.value,
            "timestamp": time.time(),
            "iso_timestamp": datetime.utcnow().isoformat()
        })

    def calculate_durations(self) -> None:
        """Calculate all duration metrics."""
        if self.completed_at and self.queued_at:
            self.total_duration = self.completed_at - self.queued_at
        if self.completed_at and self.started_at:
            self.polling_duration = self.completed_at - self.started_at


@dataclass
class TaskExecutionContext:
    """Runtime context for task execution."""

    request: TaskExecutionRequest
    config: OrchestrationConfig

    # State
    state: TaskState = TaskState.PENDING
    run_id: Optional[str] = None
    error: Optional[str] = None
    current_retry: int = 0

    # Metrics
    metrics: TaskMetrics = field(default_factory=TaskMetrics)

    # Polling state
    current_poll_interval: float = 1.0
    consecutive_same_status: int = 0
    last_known_status: Optional[str] = None

    def transition_to(self, new_state: TaskState) -> None:
        """Transition to a new state with metrics tracking."""
        old_state = self.state
        self.state = new_state
        self.metrics.record_state_transition(old_state, new_state)

        # Update timing metrics
        now = time.time()
        if new_state == TaskState.QUEUED and not self.metrics.queued_at:
            self.metrics.queued_at = now
        elif new_state == TaskState.RUNNING and not self.metrics.started_at:
            self.metrics.started_at = now
        elif new_state in [TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMEOUT]:
            if not self.metrics.completed_at:
                self.metrics.completed_at = now
                self.metrics.calculate_durations()


class TaskExecutionResult(BaseModel):
    """Result of task execution with comprehensive metadata."""

    # Core result
    run_id: str
    status: TaskState
    output_text: Optional[str] = None
    output_json: Optional[Any] = None  # Can be any JSON-serializable type

    # Metadata
    summary: str
    processor: str
    elapsed_seconds: float
    final_status: str  # Parallel.ai's final status

    # Execution details
    poll_count: int
    retry_count: int = 0
    status_history: List[Dict[str, Any]] = Field(default_factory=list)

    # Error handling
    error: Optional[str] = None
    error_code: Optional[str] = None
    recoverable: bool = True

    # Cost tracking
    estimated_cost: Optional[float] = None

    # Advanced metadata
    tags: Dict[str, str] = Field(default_factory=dict)
    context_id: Optional[str] = None
    cached: bool = False  # Whether result was from cache
    circuit_breaker_status: Optional[str] = None

    # Full response (for debugging)
    raw_response: Optional[Dict[str, Any]] = Field(None, exclude=True)


class TaskQueueStats(BaseModel):
    """Statistics about the task queue."""

    total_queued: int = 0
    total_running: int = 0
    total_completed: int = 0
    total_failed: int = 0

    by_priority: Dict[str, int] = Field(default_factory=dict)
    by_processor: Dict[str, int] = Field(default_factory=dict)

    avg_wait_time: Optional[float] = None
    avg_execution_time: Optional[float] = None
    success_rate: Optional[float] = None

    circuit_breaker_open: bool = False
    active_connections: int = 0


class ProgressUpdate(BaseModel):
    """Progress update for streaming tasks."""

    run_id: str
    state: TaskState
    message: str
    progress_percent: Optional[float] = None  # 0-100
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)
