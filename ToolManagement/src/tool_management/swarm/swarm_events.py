"""
Swarm Event Types for Agent Coordination.

Defines events that agents can publish and subscribe to for coordination.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class SwarmEventType(str, Enum):
    """Types of events in the swarm system."""

    # Task lifecycle events
    TASK_CREATED = "swarm.task.created"
    TASK_CLAIMED = "swarm.task.claimed"
    TASK_STARTED = "swarm.task.started"
    TASK_COMPLETED = "swarm.task.completed"
    TASK_FAILED = "swarm.task.failed"
    TASK_CANCELLED = "swarm.task.cancelled"
    TASK_VALIDATED = "swarm.task.validated"

    # Agent lifecycle events
    AGENT_JOINED = "swarm.agent.joined"
    AGENT_LEFT = "swarm.agent.left"
    AGENT_IDLE = "swarm.agent.idle"
    AGENT_BUSY = "swarm.agent.busy"
    AGENT_SKILL_LEARNED = "swarm.agent.skill_learned"
    AGENT_REPUTATION_CHANGED = "swarm.agent.reputation_changed"

    # MCP-specific events
    MCP_TASK_STARTED = "swarm.mcp.task_started"
    MCP_TASK_COMPLETED = "swarm.mcp.task_completed"
    MCP_BACKEND_ERROR = "swarm.mcp.backend_error"
    MCP_SKILL_CAPTURED = "swarm.mcp.skill_captured"

    # Coordination events
    WORK_AVAILABLE = "swarm.coordination.work_available"
    WORK_NEEDED = "swarm.coordination.work_needed"
    HELP_REQUESTED = "swarm.coordination.help_requested"
    COALITION_FORMED = "swarm.coordination.coalition_formed"
    AUCTION_STARTED = "swarm.coordination.auction_started"
    AUCTION_ENDED = "swarm.coordination.auction_ended"

    # Economic events
    PAYMENT_SENT = "swarm.economy.payment_sent"
    PAYMENT_RECEIVED = "swarm.economy.payment_received"
    STAKE_DEPOSITED = "swarm.economy.stake_deposited"
    STAKE_WITHDRAWN = "swarm.economy.stake_withdrawn"
    EARNINGS_COLLECTED = "swarm.economy.earnings_collected"

    # Workflow events
    WORKFLOW_STARTED = "swarm.workflow.started"
    WORKFLOW_STEP_COMPLETED = "swarm.workflow.step_completed"
    WORKFLOW_COMPLETED = "swarm.workflow.completed"
    WORKFLOW_FAILED = "swarm.workflow.failed"


class SwarmEvent(BaseModel):
    """A swarm event that can be published and subscribed to."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: SwarmEventType
    swarm_id: str
    source_agent_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    task_id: Optional[str] = None
    workflow_id: Optional[str] = None
    data: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "event_type": self.event_type.value,
            "swarm_id": self.swarm_id,
            "source_agent_id": self.source_agent_id,
            "target_agent_id": self.target_agent_id,
            "task_id": self.task_id,
            "workflow_id": self.workflow_id,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SwarmEvent":
        """Create from dictionary."""
        return cls(
            id=data.get("id", str(uuid4())),
            event_type=SwarmEventType(data["event_type"]),
            swarm_id=data["swarm_id"],
            source_agent_id=data.get("source_agent_id"),
            target_agent_id=data.get("target_agent_id"),
            task_id=data.get("task_id"),
            workflow_id=data.get("workflow_id"),
            data=data.get("data", {}),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.now(timezone.utc),
            metadata=data.get("metadata", {}),
        )


class EventSubscription(BaseModel):
    """A subscription to swarm events."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    swarm_id: str
    subscriber_id: str  # Agent ID or external subscriber
    event_types: List[SwarmEventType] = Field(default_factory=list)
    webhook_url: Optional[str] = None
    filter_criteria: Dict[str, Any] = Field(default_factory=dict)
    active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def matches(self, event: SwarmEvent) -> bool:
        """Check if this subscription matches an event."""
        if not self.active:
            return False

        if event.swarm_id != self.swarm_id:
            return False

        if self.event_types and event.event_type not in self.event_types:
            return False

        # Apply filter criteria
        for key, value in self.filter_criteria.items():
            if key == "source_agent_id" and event.source_agent_id != value:
                return False
            if key == "target_agent_id" and event.target_agent_id != value:
                return False
            if key == "task_id" and event.task_id != value:
                return False

        return True


# Event factory functions
def task_created_event(swarm_id: str, task_id: str, task_type: str, bounty: float, **kwargs) -> SwarmEvent:
    """Create a task created event."""
    return SwarmEvent(
        event_type=SwarmEventType.TASK_CREATED,
        swarm_id=swarm_id,
        task_id=task_id,
        data={"task_type": task_type, "bounty": bounty, **kwargs},
    )


def task_completed_event(
    swarm_id: str,
    task_id: str,
    agent_id: str,
    output: Dict[str, Any],
    validation_score: Optional[float] = None,
    **kwargs
) -> SwarmEvent:
    """Create a task completed event."""
    return SwarmEvent(
        event_type=SwarmEventType.TASK_COMPLETED,
        swarm_id=swarm_id,
        task_id=task_id,
        source_agent_id=agent_id,
        data={
            "output": output,
            "validation_score": validation_score,
            **kwargs,
        },
    )


def agent_idle_event(swarm_id: str, agent_id: str, capabilities: List[str], **kwargs) -> SwarmEvent:
    """Create an agent idle event."""
    return SwarmEvent(
        event_type=SwarmEventType.AGENT_IDLE,
        swarm_id=swarm_id,
        source_agent_id=agent_id,
        data={"capabilities": capabilities, **kwargs},
    )


def skill_learned_event(swarm_id: str, agent_id: str, skill_id: str, pattern: Dict[str, Any], **kwargs) -> SwarmEvent:
    """Create a skill learned event."""
    return SwarmEvent(
        event_type=SwarmEventType.AGENT_SKILL_LEARNED,
        swarm_id=swarm_id,
        source_agent_id=agent_id,
        data={"skill_id": skill_id, "pattern": pattern, **kwargs},
    )


def work_available_event(swarm_id: str, task_ids: List[str], required_capabilities: List[str], **kwargs) -> SwarmEvent:
    """Create a work available event."""
    return SwarmEvent(
        event_type=SwarmEventType.WORK_AVAILABLE,
        swarm_id=swarm_id,
        data={
            "task_ids": task_ids,
            "required_capabilities": required_capabilities,
            **kwargs,
        },
    )


def workflow_completed_event(
    swarm_id: str,
    workflow_id: str,
    steps_completed: int,
    total_steps: int,
    results: List[Dict[str, Any]],
    **kwargs
) -> SwarmEvent:
    """Create a workflow completed event."""
    return SwarmEvent(
        event_type=SwarmEventType.WORKFLOW_COMPLETED,
        swarm_id=swarm_id,
        workflow_id=workflow_id,
        data={
            "steps_completed": steps_completed,
            "total_steps": total_steps,
            "results": results,
            **kwargs,
        },
    )


def mcp_skill_captured_event(
    swarm_id: str,
    agent_id: str,
    skill_id: str,
    task_type: str,
    input_pattern: Dict[str, Any],
    output_pattern: Dict[str, Any],
    score: float,
    **kwargs
) -> SwarmEvent:
    """Create an MCP skill captured event."""
    return SwarmEvent(
        event_type=SwarmEventType.MCP_SKILL_CAPTURED,
        swarm_id=swarm_id,
        source_agent_id=agent_id,
        data={
            "skill_id": skill_id,
            "task_type": task_type,
            "input_pattern": input_pattern,
            "output_pattern": output_pattern,
            "score": score,
            **kwargs,
        },
    )
