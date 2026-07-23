"""
Engine API Models

Core data structures for the agent substrate.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Set
import uuid
import time


# ============================================================================
# Enums
# ============================================================================

class AgentStatus(str, Enum):
    """Agent lifecycle status"""
    INITIALIZING = "initializing"
    IDLE = "idle"
    WORKING = "working"
    PAUSED = "paused"
    ERROR = "error"
    TERMINATED = "terminated"


class SignalType(str, Enum):
    """Types of signals that can be sent to agents"""
    INTERRUPT = "interrupt"      # Stop current, do this instead
    PRIORITY = "priority"        # Change task priority
    CANCEL = "cancel"            # Abort current task
    REASSIGN = "reassign"        # Move task to another agent
    STATE_PUSH = "state_push"    # Immediate state update
    SHUTDOWN = "shutdown"        # Graceful shutdown
    PING = "ping"                # Health check


class TaskStatus(str, Enum):
    """Task execution status"""
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TickType(str, Enum):
    """Types of ticks"""
    POLL = "poll"           # Regular polling tick
    TASK = "task"           # Task assignment tick
    SYNC = "sync"           # State sync tick
    HEARTBEAT = "heartbeat" # Liveness check


# ============================================================================
# Core Models
# ============================================================================

@dataclass
class Tick:
    """
    A tick is the fundamental unit of time in the engine.

    Agents receive ticks at configured intervals. Each tick may contain:
    - Scheduled tasks to execute
    - State updates to apply
    - Deadline for response
    """
    tick_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tick_type: TickType = TickType.POLL
    timestamp: datetime = field(default_factory=datetime.utcnow)
    timestamp_ns: int = field(default_factory=time.time_ns)
    sequence: int = 0

    # What should the agent do?
    scheduled_tasks: List["Task"] = field(default_factory=list)

    # State snapshot (optional, for sync ticks)
    state_snapshot: Optional[bytes] = None

    # How long does agent have to respond?
    deadline_ms: int = 1000

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tick_id": self.tick_id,
            "tick_type": self.tick_type.value,
            "timestamp": self.timestamp.isoformat(),
            "timestamp_ns": self.timestamp_ns,
            "sequence": self.sequence,
            "scheduled_tasks": [t.to_dict() for t in self.scheduled_tasks],
            "deadline_ms": self.deadline_ms,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tick":
        return cls(
            tick_id=data["tick_id"],
            tick_type=TickType(data["tick_type"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            timestamp_ns=data.get("timestamp_ns", 0),
            sequence=data["sequence"],
            scheduled_tasks=[Task.from_dict(t) for t in data.get("scheduled_tasks", [])],
            deadline_ms=data.get("deadline_ms", 1000),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Signal:
    """
    A signal is an out-of-band interrupt that doesn't wait for the next tick.

    Used for:
    - Priority changes
    - Task cancellation
    - Immediate state pushes
    - Shutdown requests
    """
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    signal_type: SignalType = SignalType.INTERRUPT
    target_agent: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Signal payload
    payload: Any = None

    # For priority signals
    priority: int = 0  # 0 = highest
    preempt: bool = False  # Cancel current work?

    # Acknowledgment required?
    require_ack: bool = False
    ack_timeout_ms: int = 5000

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "signal_type": self.signal_type.value,
            "target_agent": self.target_agent,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
            "priority": self.priority,
            "preempt": self.preempt,
            "require_ack": self.require_ack,
            "ack_timeout_ms": self.ack_timeout_ms,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Signal":
        return cls(
            signal_id=data["signal_id"],
            signal_type=SignalType(data["signal_type"]),
            target_agent=data["target_agent"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            payload=data.get("payload"),
            priority=data.get("priority", 0),
            preempt=data.get("preempt", False),
            require_ack=data.get("require_ack", False),
            ack_timeout_ms=data.get("ack_timeout_ms", 5000),
        )


@dataclass
class Task:
    """
    A unit of work assigned to an agent.
    """
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = "generic"
    status: TaskStatus = TaskStatus.PENDING

    # Task definition
    name: str = ""
    description: str = ""
    input_data: Dict[str, Any] = field(default_factory=dict)

    # Assignment
    assigned_agent: Optional[str] = None
    assigned_at: Optional[datetime] = None

    # Execution
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None

    # Priority and deadlines
    priority: int = 5  # 0 = highest, 10 = lowest
    deadline: Optional[datetime] = None
    timeout_ms: int = 300000  # 5 minutes default

    # Dependencies
    depends_on: List[str] = field(default_factory=list)

    # Retry policy
    max_retries: int = 3
    retry_count: int = 0

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status.value,
            "name": self.name,
            "description": self.description,
            "input_data": self.input_data,
            "assigned_agent": self.assigned_agent,
            "assigned_at": self.assigned_at.isoformat() if self.assigned_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "error": self.error,
            "priority": self.priority,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "timeout_ms": self.timeout_ms,
            "depends_on": self.depends_on,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        return cls(
            task_id=data["task_id"],
            task_type=data.get("task_type", "generic"),
            status=TaskStatus(data["status"]),
            name=data.get("name", ""),
            description=data.get("description", ""),
            input_data=data.get("input_data", {}),
            assigned_agent=data.get("assigned_agent"),
            assigned_at=datetime.fromisoformat(data["assigned_at"]) if data.get("assigned_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            result=data.get("result"),
            error=data.get("error"),
            priority=data.get("priority", 5),
            deadline=datetime.fromisoformat(data["deadline"]) if data.get("deadline") else None,
            timeout_ms=data.get("timeout_ms", 300000),
            depends_on=data.get("depends_on", []),
            max_retries=data.get("max_retries", 3),
            retry_count=data.get("retry_count", 0),
            metadata=data.get("metadata", {}),
        )


# ============================================================================
# CRDT-based State
# ============================================================================

@dataclass
class StateDelta:
    """
    A delta representing changes to agent state.

    Uses vector clock for causality tracking.
    """
    delta_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Vector clock for causal ordering
    vector_clock: Dict[str, int] = field(default_factory=dict)

    # The actual changes
    changes: Dict[str, Any] = field(default_factory=dict)

    # Type of change for each key
    change_types: Dict[str, str] = field(default_factory=dict)  # "set", "increment", "add", "remove"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "delta_id": self.delta_id,
            "agent_id": self.agent_id,
            "timestamp": self.timestamp.isoformat(),
            "vector_clock": self.vector_clock,
            "changes": self.changes,
            "change_types": self.change_types,
        }


@dataclass
class AgentState:
    """
    Full agent state with CRDT semantics.

    This state can be safely merged with other versions without conflicts.
    """
    agent_id: str = ""

    # LWW (Last-Writer-Wins) registers
    status: AgentStatus = AgentStatus.INITIALIZING
    status_updated_at: datetime = field(default_factory=datetime.utcnow)

    current_task_id: Optional[str] = None
    current_task_updated_at: datetime = field(default_factory=datetime.utcnow)

    # Counters (grow-only, can only be merged by taking max)
    tasks_completed: int = 0
    tasks_failed: int = 0
    ticks_received: int = 0
    signals_received: int = 0

    # Sets (OR-Set semantics - add/remove with unique tags)
    capabilities: Set[str] = field(default_factory=set)
    active_tools: Set[str] = field(default_factory=set)

    # LWW Map for arbitrary metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    metadata_updated_at: Dict[str, datetime] = field(default_factory=dict)

    # Vector clock for causal ordering
    vector_clock: Dict[str, int] = field(default_factory=dict)

    # Last sync timestamp
    last_sync: datetime = field(default_factory=datetime.utcnow)

    def merge(self, other: "AgentState") -> "AgentState":
        """
        Merge two agent states using CRDT semantics.

        - LWW registers: take value with latest timestamp
        - Counters: take max
        - Sets: union
        - Maps: merge per-key using LWW
        """
        result = AgentState(agent_id=self.agent_id)

        # LWW: status
        if other.status_updated_at > self.status_updated_at:
            result.status = other.status
            result.status_updated_at = other.status_updated_at
        else:
            result.status = self.status
            result.status_updated_at = self.status_updated_at

        # LWW: current_task_id
        if other.current_task_updated_at > self.current_task_updated_at:
            result.current_task_id = other.current_task_id
            result.current_task_updated_at = other.current_task_updated_at
        else:
            result.current_task_id = self.current_task_id
            result.current_task_updated_at = self.current_task_updated_at

        # Counters: max
        result.tasks_completed = max(self.tasks_completed, other.tasks_completed)
        result.tasks_failed = max(self.tasks_failed, other.tasks_failed)
        result.ticks_received = max(self.ticks_received, other.ticks_received)
        result.signals_received = max(self.signals_received, other.signals_received)

        # Sets: union
        result.capabilities = self.capabilities | other.capabilities
        result.active_tools = self.active_tools | other.active_tools

        # Maps: merge per-key using LWW
        all_keys = set(self.metadata.keys()) | set(other.metadata.keys())
        for key in all_keys:
            self_time = self.metadata_updated_at.get(key, datetime.min)
            other_time = other.metadata_updated_at.get(key, datetime.min)

            if other_time > self_time:
                result.metadata[key] = other.metadata.get(key)
                result.metadata_updated_at[key] = other_time
            else:
                result.metadata[key] = self.metadata.get(key)
                result.metadata_updated_at[key] = self_time

        # Vector clock: merge by taking max per agent
        all_agents = set(self.vector_clock.keys()) | set(other.vector_clock.keys())
        for agent in all_agents:
            result.vector_clock[agent] = max(
                self.vector_clock.get(agent, 0),
                other.vector_clock.get(agent, 0)
            )

        result.last_sync = max(self.last_sync, other.last_sync)

        return result

    def apply_delta(self, delta: StateDelta) -> "AgentState":
        """Apply a state delta to this state."""
        # Check causality - delta must be causally ready
        for agent, clock in delta.vector_clock.items():
            if clock > self.vector_clock.get(agent, 0) + 1:
                # Missing intermediate deltas - buffer this one
                raise ValueError(f"Delta not causally ready: missing updates from {agent}")

        # Apply changes
        for key, value in delta.changes.items():
            change_type = delta.change_types.get(key, "set")

            if change_type == "set":
                if key == "status":
                    self.status = AgentStatus(value)
                    self.status_updated_at = delta.timestamp
                elif key == "current_task_id":
                    self.current_task_id = value
                    self.current_task_updated_at = delta.timestamp
                else:
                    self.metadata[key] = value
                    self.metadata_updated_at[key] = delta.timestamp

            elif change_type == "increment":
                if key == "tasks_completed":
                    self.tasks_completed += value
                elif key == "tasks_failed":
                    self.tasks_failed += value
                elif key == "ticks_received":
                    self.ticks_received += value
                elif key == "signals_received":
                    self.signals_received += value

            elif change_type == "add":
                if key == "capabilities":
                    self.capabilities.add(value)
                elif key == "active_tools":
                    self.active_tools.add(value)

            elif change_type == "remove":
                if key == "capabilities":
                    self.capabilities.discard(value)
                elif key == "active_tools":
                    self.active_tools.discard(value)

        # Update vector clock
        for agent, clock in delta.vector_clock.items():
            self.vector_clock[agent] = max(self.vector_clock.get(agent, 0), clock)

        self.last_sync = delta.timestamp

        return self

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "status": self.status.value,
            "status_updated_at": self.status_updated_at.isoformat(),
            "current_task_id": self.current_task_id,
            "current_task_updated_at": self.current_task_updated_at.isoformat(),
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "ticks_received": self.ticks_received,
            "signals_received": self.signals_received,
            "capabilities": list(self.capabilities),
            "active_tools": list(self.active_tools),
            "metadata": self.metadata,
            "vector_clock": self.vector_clock,
            "last_sync": self.last_sync.isoformat(),
        }


# ============================================================================
# Agent Configuration
# ============================================================================

@dataclass
class AgentConfig:
    """Configuration for an agent connection."""
    agent_id: str
    agent_type: str = "generic"

    # Tick configuration
    tick_interval_ms: int = 1000  # How often to receive ticks
    tick_timeout_ms: int = 5000   # How long to wait for tick response

    # Capabilities
    capabilities: Set[str] = field(default_factory=set)
    max_concurrent_tasks: int = 1

    # Connection settings
    heartbeat_interval_ms: int = 30000
    reconnect_delay_ms: int = 1000
    max_reconnect_attempts: int = 10

    # State sync
    state_sync_interval_ms: int = 5000
    checkpoint_interval_ms: int = 60000

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Presence
# ============================================================================

@dataclass
class AgentPresence:
    """Agent presence information."""
    agent_id: str
    agent_type: str = "generic"
    status: AgentStatus = AgentStatus.IDLE

    # Connection info
    connected: bool = True
    connected_at: Optional[datetime] = None
    last_seen: datetime = field(default_factory=datetime.utcnow)

    # Current work
    current_task_id: Optional[str] = None
    tasks_in_queue: int = 0

    # Capabilities
    capabilities: Set[str] = field(default_factory=set)

    # Instance info
    instance_id: str = ""  # Which Engine instance this agent is connected to

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "status": self.status.value,
            "connected": self.connected,
            "connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "last_seen": self.last_seen.isoformat(),
            "current_task_id": self.current_task_id,
            "tasks_in_queue": self.tasks_in_queue,
            "capabilities": list(self.capabilities),
            "instance_id": self.instance_id,
        }
