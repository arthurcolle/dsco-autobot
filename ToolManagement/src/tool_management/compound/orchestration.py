"""
Advanced Pipeline Orchestration - DAGs, Checkpointing, Webhooks, Collaboration.

Features:
- DAG-based pipeline orchestration with parallel branches
- Checkpointing and recovery for long-running pipelines
- Webhook event delivery
- Result caching with TTL
- Priority execution queue
- Real-time collaboration rooms
- Cost estimation and resource limits
- Conditional branching and dynamic routing
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import json
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple, Union
)
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

from .pipelines import PipelineEvent, PipelineEventType, PipelineState

logger = logging.getLogger(__name__)


# =============================================================================
# DAG PIPELINE DEFINITIONS
# =============================================================================

class DAGNodeType(str, Enum):
    """Type of node in a DAG pipeline."""
    PIPELINE = "pipeline"
    PARALLEL = "parallel"  # Run children in parallel
    CONDITIONAL = "conditional"  # Branch based on condition
    MERGE = "merge"  # Merge results from parallel branches
    TRANSFORM = "transform"  # Transform data between nodes
    WAIT = "wait"  # Wait for external event
    LOOP = "loop"  # Loop until condition


class DAGNode(BaseModel):
    """A node in a DAG pipeline."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    type: DAGNodeType = DAGNodeType.PIPELINE

    # For pipeline nodes
    pipeline: Optional[str] = None
    inputs: Dict[str, Any] = Field(default_factory=dict)
    input_mappings: Dict[str, str] = Field(default_factory=dict)

    # For conditional nodes
    condition: Optional[str] = None  # Python expression
    true_branch: Optional[str] = None  # Node ID
    false_branch: Optional[str] = None  # Node ID

    # For parallel nodes
    children: List[str] = Field(default_factory=list)  # Node IDs

    # For transform nodes
    transform_expression: Optional[str] = None

    # For loop nodes
    loop_condition: Optional[str] = None
    loop_body: Optional[str] = None  # Node ID
    max_iterations: int = 100

    # Dependencies
    depends_on: List[str] = Field(default_factory=list)  # Node IDs

    # Resource limits
    timeout_seconds: Optional[float] = None
    max_retries: int = 0
    retry_delay_seconds: float = 1.0

    # Cost estimation
    estimated_cost_cents: int = 0
    estimated_duration_seconds: float = 30.0


class DAGPipeline(BaseModel):
    """A DAG-based pipeline definition."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str = ""
    version: str = "1.0.0"

    # Nodes
    nodes: Dict[str, DAGNode] = Field(default_factory=dict)
    entry_node: str = ""  # Starting node ID
    exit_nodes: List[str] = Field(default_factory=list)  # Terminal node IDs

    # Global settings
    max_parallel: int = 10
    global_timeout_seconds: Optional[float] = None

    # Metadata
    tags: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    def add_node(self, node: DAGNode) -> str:
        """Add a node to the DAG."""
        self.nodes[node.id] = node
        return node.id

    def get_execution_order(self) -> List[List[str]]:
        """Get topologically sorted execution order (levels for parallel execution)."""
        # Kahn's algorithm with level tracking
        in_degree = {nid: 0 for nid in self.nodes}
        for node in self.nodes.values():
            for dep in node.depends_on:
                if dep in in_degree:
                    in_degree[node.id] += 1

        levels = []
        current_level = [nid for nid, deg in in_degree.items() if deg == 0]

        while current_level:
            levels.append(current_level)
            next_level = []

            for nid in current_level:
                for node in self.nodes.values():
                    if nid in node.depends_on:
                        in_degree[node.id] -= 1
                        if in_degree[node.id] == 0:
                            next_level.append(node.id)

            current_level = next_level

        return levels

    def estimate_cost(self) -> Dict[str, Any]:
        """Estimate total cost and duration."""
        total_cost = sum(n.estimated_cost_cents for n in self.nodes.values())

        # Calculate critical path for duration
        levels = self.get_execution_order()
        total_duration = 0
        for level in levels:
            level_duration = max(
                self.nodes[nid].estimated_duration_seconds
                for nid in level
            )
            total_duration += level_duration

        return {
            "estimated_cost_cents": total_cost,
            "estimated_duration_seconds": total_duration,
            "node_count": len(self.nodes),
            "parallel_levels": len(levels),
        }


# =============================================================================
# CHECKPOINTING
# =============================================================================

class CheckpointStorage(ABC):
    """Abstract checkpoint storage."""

    @abstractmethod
    async def save(self, execution_id: str, checkpoint: Dict[str, Any]):
        pass

    @abstractmethod
    async def load(self, execution_id: str) -> Optional[Dict[str, Any]]:
        pass

    @abstractmethod
    async def delete(self, execution_id: str):
        pass

    @abstractmethod
    async def list_checkpoints(self) -> List[str]:
        pass


class InMemoryCheckpointStorage(CheckpointStorage):
    """In-memory checkpoint storage."""

    def __init__(self):
        self._checkpoints: Dict[str, Dict[str, Any]] = {}

    async def save(self, execution_id: str, checkpoint: Dict[str, Any]):
        self._checkpoints[execution_id] = {
            **checkpoint,
            "saved_at": datetime.utcnow().isoformat(),
        }

    async def load(self, execution_id: str) -> Optional[Dict[str, Any]]:
        return self._checkpoints.get(execution_id)

    async def delete(self, execution_id: str):
        self._checkpoints.pop(execution_id, None)

    async def list_checkpoints(self) -> List[str]:
        return list(self._checkpoints.keys())


@dataclass
class Checkpoint:
    """Execution checkpoint."""
    execution_id: str
    dag_id: str
    completed_nodes: Set[str] = field(default_factory=set)
    node_results: Dict[str, Any] = field(default_factory=dict)
    current_inputs: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "dag_id": self.dag_id,
            "completed_nodes": list(self.completed_nodes),
            "node_results": self.node_results,
            "current_inputs": self.current_inputs,
            "events": self.events,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        return cls(
            execution_id=data["execution_id"],
            dag_id=data["dag_id"],
            completed_nodes=set(data.get("completed_nodes", [])),
            node_results=data.get("node_results", {}),
            current_inputs=data.get("current_inputs", {}),
            events=data.get("events", []),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
        )


# =============================================================================
# WEBHOOKS
# =============================================================================

class WebhookTrigger(str, Enum):
    """When to trigger webhook."""
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"
    EXECUTION_FAILED = "execution_failed"
    NODE_COMPLETED = "node_completed"
    STEP_COMPLETED = "step_completed"
    ALL_EVENTS = "all_events"


class WebhookConfig(BaseModel):
    """Webhook configuration."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    url: str
    triggers: List[WebhookTrigger] = Field(default_factory=lambda: [WebhookTrigger.EXECUTION_COMPLETED])
    headers: Dict[str, str] = Field(default_factory=dict)
    secret: Optional[str] = None  # For HMAC signing
    retry_count: int = 3
    timeout_seconds: float = 10.0
    enabled: bool = True


class WebhookDelivery(BaseModel):
    """Record of webhook delivery attempt."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    webhook_id: str
    execution_id: str
    trigger: WebhookTrigger
    payload: Dict[str, Any]
    status_code: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 0
    delivered_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class WebhookManager:
    """Manage webhook deliveries."""

    def __init__(self):
        self.webhooks: Dict[str, WebhookConfig] = {}
        self.deliveries: List[WebhookDelivery] = []
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self):
        self._client = httpx.AsyncClient(timeout=30.0)

    async def stop(self):
        if self._client:
            await self._client.aclose()

    def register(self, webhook: WebhookConfig) -> str:
        """Register a webhook."""
        self.webhooks[webhook.id] = webhook
        return webhook.id

    def unregister(self, webhook_id: str):
        """Unregister a webhook."""
        self.webhooks.pop(webhook_id, None)

    async def trigger(
        self,
        trigger: WebhookTrigger,
        execution_id: str,
        payload: Dict[str, Any],
    ):
        """Trigger webhooks for an event."""
        for webhook in self.webhooks.values():
            if not webhook.enabled:
                continue

            if trigger not in webhook.triggers and WebhookTrigger.ALL_EVENTS not in webhook.triggers:
                continue

            delivery = WebhookDelivery(
                webhook_id=webhook.id,
                execution_id=execution_id,
                trigger=trigger,
                payload=payload,
            )

            # Fire and forget
            asyncio.create_task(self._deliver(webhook, delivery))

    async def _deliver(self, webhook: WebhookConfig, delivery: WebhookDelivery):
        """Deliver webhook with retries."""
        headers = {**webhook.headers}

        # Sign payload if secret configured
        if webhook.secret:
            payload_json = json.dumps(delivery.payload, sort_keys=True)
            signature = hashlib.hmac.new(
                webhook.secret.encode(),
                payload_json.encode(),
                hashlib.sha256,
            ).hexdigest()
            headers["X-Webhook-Signature"] = f"sha256={signature}"

        for attempt in range(webhook.retry_count):
            delivery.attempts = attempt + 1

            try:
                response = await self._client.post(
                    webhook.url,
                    json={
                        "trigger": delivery.trigger.value,
                        "execution_id": delivery.execution_id,
                        "payload": delivery.payload,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                    headers=headers,
                    timeout=webhook.timeout_seconds,
                )

                delivery.status_code = response.status_code
                delivery.response_body = response.text[:1000]  # Truncate

                if response.is_success:
                    delivery.delivered_at = datetime.utcnow()
                    break

            except Exception as e:
                delivery.error = str(e)

            # Exponential backoff
            if attempt < webhook.retry_count - 1:
                await asyncio.sleep(2 ** attempt)

        self.deliveries.append(delivery)

        # Keep only last 1000 deliveries
        if len(self.deliveries) > 1000:
            self.deliveries = self.deliveries[-1000:]


# =============================================================================
# RESULT CACHING
# =============================================================================

@dataclass
class CacheEntry:
    """Cached result entry."""
    key: str
    value: Any
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    hits: int = 0

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at


class ResultCache:
    """Cache for pipeline results."""

    def __init__(self, max_size: int = 1000, default_ttl_seconds: int = 3600):
        self.max_size = max_size
        self.default_ttl_seconds = default_ttl_seconds
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []

    def _make_key(self, pipeline: str, inputs: Dict[str, Any]) -> str:
        """Generate cache key from pipeline and inputs."""
        input_hash = hashlib.sha256(
            json.dumps(inputs, sort_keys=True).encode()
        ).hexdigest()[:16]
        return f"{pipeline}:{input_hash}"

    def get(self, pipeline: str, inputs: Dict[str, Any]) -> Optional[Any]:
        """Get cached result."""
        key = self._make_key(pipeline, inputs)
        entry = self._cache.get(key)

        if entry is None:
            return None

        if entry.is_expired():
            del self._cache[key]
            return None

        entry.hits += 1
        return entry.value

    def set(
        self,
        pipeline: str,
        inputs: Dict[str, Any],
        value: Any,
        ttl_seconds: Optional[int] = None,
    ):
        """Cache a result."""
        key = self._make_key(pipeline, inputs)
        ttl = ttl_seconds or self.default_ttl_seconds

        self._cache[key] = CacheEntry(
            key=key,
            value=value,
            expires_at=datetime.utcnow() + timedelta(seconds=ttl),
        )

        # Evict if over size
        if len(self._cache) > self.max_size:
            self._evict_oldest()

    def _evict_oldest(self):
        """Evict oldest entries."""
        # Sort by creation time and remove oldest 10%
        entries = sorted(
            self._cache.items(),
            key=lambda x: x[1].created_at,
        )
        to_remove = len(entries) // 10 or 1

        for key, _ in entries[:to_remove]:
            del self._cache[key]

    def invalidate(self, pipeline: Optional[str] = None):
        """Invalidate cache entries."""
        if pipeline is None:
            self._cache.clear()
        else:
            to_remove = [k for k in self._cache if k.startswith(f"{pipeline}:")]
            for k in to_remove:
                del self._cache[k]

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_hits = sum(e.hits for e in self._cache.values())
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "total_hits": total_hits,
            "entries": [
                {
                    "key": e.key,
                    "hits": e.hits,
                    "expires_at": e.expires_at.isoformat() if e.expires_at else None,
                }
                for e in list(self._cache.values())[:20]
            ],
        }


# =============================================================================
# PRIORITY QUEUE
# =============================================================================

class ExecutionPriority(int, Enum):
    """Execution priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass(order=True)
class PriorityItem:
    """Item in priority queue."""
    priority: int
    created_at: float = field(compare=True)
    execution_id: str = field(compare=False)
    pipeline: str = field(compare=False)
    inputs: Dict[str, Any] = field(compare=False, default_factory=dict)
    config: Dict[str, Any] = field(compare=False, default_factory=dict)


class PriorityExecutionQueue:
    """Priority queue for pipeline executions."""

    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self._queue: List[PriorityItem] = []
        self._running: Dict[str, PriorityItem] = {}
        self._completed: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._has_items = asyncio.Event()

    async def enqueue(
        self,
        pipeline: str,
        inputs: Dict[str, Any],
        priority: ExecutionPriority = ExecutionPriority.NORMAL,
        config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add execution to queue."""
        execution_id = str(uuid4())

        item = PriorityItem(
            priority=priority.value,
            created_at=time.time(),
            execution_id=execution_id,
            pipeline=pipeline,
            inputs=inputs,
            config=config or {},
        )

        async with self._lock:
            heapq.heappush(self._queue, item)
            self._has_items.set()

        return execution_id

    async def dequeue(self) -> Optional[PriorityItem]:
        """Get next execution from queue."""
        async with self._lock:
            if len(self._running) >= self.max_concurrent:
                return None

            if not self._queue:
                self._has_items.clear()
                return None

            item = heapq.heappop(self._queue)
            self._running[item.execution_id] = item

            if not self._queue:
                self._has_items.clear()

            return item

    async def complete(self, execution_id: str, result: Any):
        """Mark execution as complete."""
        async with self._lock:
            item = self._running.pop(execution_id, None)
            if item:
                self._completed[execution_id] = {
                    "item": item,
                    "result": result,
                    "completed_at": datetime.utcnow().isoformat(),
                }

    async def wait_for_items(self):
        """Wait until items are available."""
        await self._has_items.wait()

    def stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "queued": len(self._queue),
            "running": len(self._running),
            "completed": len(self._completed),
            "max_concurrent": self.max_concurrent,
            "queue_by_priority": {
                p.name: sum(1 for item in self._queue if item.priority == p.value)
                for p in ExecutionPriority
            },
        }


# =============================================================================
# COLLABORATION ROOMS
# =============================================================================

@dataclass
class RoomParticipant:
    """Participant in a collaboration room."""
    id: str
    websocket: Any
    name: str = "anonymous"
    joined_at: datetime = field(default_factory=datetime.utcnow)
    events_received: int = 0
    cursor_position: Optional[Dict[str, Any]] = None  # For shared cursors


@dataclass
class CollaborationRoom:
    """Room for real-time collaboration on pipeline execution."""
    id: str
    execution_id: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    participants: Dict[str, RoomParticipant] = field(default_factory=dict)

    # Shared state
    annotations: List[Dict[str, Any]] = field(default_factory=list)
    chat_messages: List[Dict[str, Any]] = field(default_factory=list)

    # Event buffer for late joiners
    event_buffer: List[PipelineEvent] = field(default_factory=list)
    max_buffer_size: int = 100


class CollaborationManager:
    """Manage collaboration rooms."""

    def __init__(self):
        self.rooms: Dict[str, CollaborationRoom] = {}

    def create_room(self, execution_id: str) -> str:
        """Create a collaboration room for an execution."""
        room_id = str(uuid4())
        self.rooms[room_id] = CollaborationRoom(
            id=room_id,
            execution_id=execution_id,
        )
        return room_id

    def get_or_create_room(self, execution_id: str) -> CollaborationRoom:
        """Get or create room for execution."""
        for room in self.rooms.values():
            if room.execution_id == execution_id:
                return room
        room_id = self.create_room(execution_id)
        return self.rooms[room_id]

    async def join(
        self,
        room_id: str,
        participant_id: str,
        websocket: Any,
        name: str = "anonymous",
    ) -> bool:
        """Join a room."""
        room = self.rooms.get(room_id)
        if not room:
            return False

        room.participants[participant_id] = RoomParticipant(
            id=participant_id,
            websocket=websocket,
            name=name,
        )

        # Notify others
        await self.broadcast(room_id, {
            "type": "participant_joined",
            "participant": {
                "id": participant_id,
                "name": name,
            },
            "total_participants": len(room.participants),
        }, exclude={participant_id})

        # Send buffered events to new participant
        for event in room.event_buffer:
            try:
                await websocket.send_json(event.model_dump(mode="json"))
            except:
                pass

        return True

    async def leave(self, room_id: str, participant_id: str):
        """Leave a room."""
        room = self.rooms.get(room_id)
        if not room:
            return

        participant = room.participants.pop(participant_id, None)
        if participant:
            await self.broadcast(room_id, {
                "type": "participant_left",
                "participant_id": participant_id,
                "total_participants": len(room.participants),
            })

        # Clean up empty rooms
        if not room.participants:
            del self.rooms[room_id]

    async def broadcast(
        self,
        room_id: str,
        message: Union[Dict[str, Any], PipelineEvent],
        exclude: Optional[Set[str]] = None,
    ):
        """Broadcast message to all room participants."""
        room = self.rooms.get(room_id)
        if not room:
            return

        exclude = exclude or set()

        if isinstance(message, PipelineEvent):
            # Buffer for late joiners
            room.event_buffer.append(message)
            if len(room.event_buffer) > room.max_buffer_size:
                room.event_buffer = room.event_buffer[-room.max_buffer_size:]
            data = message.model_dump(mode="json")
        else:
            data = message

        for pid, participant in list(room.participants.items()):
            if pid in exclude:
                continue

            try:
                await participant.websocket.send_json(data)
                participant.events_received += 1
            except Exception:
                # Remove dead connections
                room.participants.pop(pid, None)

    async def add_annotation(
        self,
        room_id: str,
        participant_id: str,
        annotation: Dict[str, Any],
    ):
        """Add an annotation to the room."""
        room = self.rooms.get(room_id)
        if not room:
            return

        annotation_data = {
            "id": str(uuid4()),
            "participant_id": participant_id,
            "content": annotation,
            "created_at": datetime.utcnow().isoformat(),
        }

        room.annotations.append(annotation_data)

        await self.broadcast(room_id, {
            "type": "annotation_added",
            "annotation": annotation_data,
        })

    async def send_chat(
        self,
        room_id: str,
        participant_id: str,
        message: str,
    ):
        """Send chat message to room."""
        room = self.rooms.get(room_id)
        if not room:
            return

        participant = room.participants.get(participant_id)
        name = participant.name if participant else "unknown"

        chat_data = {
            "id": str(uuid4()),
            "participant_id": participant_id,
            "participant_name": name,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
        }

        room.chat_messages.append(chat_data)

        # Keep last 100 messages
        if len(room.chat_messages) > 100:
            room.chat_messages = room.chat_messages[-100:]

        await self.broadcast(room_id, {
            "type": "chat_message",
            "chat": chat_data,
        })

    def get_room_state(self, room_id: str) -> Optional[Dict[str, Any]]:
        """Get current room state."""
        room = self.rooms.get(room_id)
        if not room:
            return None

        return {
            "room_id": room.id,
            "execution_id": room.execution_id,
            "participants": [
                {
                    "id": p.id,
                    "name": p.name,
                    "joined_at": p.joined_at.isoformat(),
                    "events_received": p.events_received,
                }
                for p in room.participants.values()
            ],
            "annotations_count": len(room.annotations),
            "chat_messages_count": len(room.chat_messages),
            "buffered_events": len(room.event_buffer),
        }


# =============================================================================
# RESOURCE LIMITS
# =============================================================================

class ResourceLimits(BaseModel):
    """Resource limits for execution."""
    max_duration_seconds: Optional[float] = 300.0  # 5 minutes
    max_tokens: Optional[int] = None
    max_cost_cents: Optional[int] = None
    max_tool_calls: Optional[int] = 100
    max_memory_mb: Optional[int] = None
    max_output_size_bytes: Optional[int] = 10_000_000  # 10MB


class ResourceTracker:
    """Track resource usage during execution."""

    def __init__(self, limits: ResourceLimits):
        self.limits = limits
        self.start_time = time.time()
        self.tokens_used = 0
        self.cost_cents = 0
        self.tool_calls = 0
        self.output_bytes = 0

    def add_tokens(self, count: int):
        self.tokens_used += count
        if self.limits.max_tokens and self.tokens_used > self.limits.max_tokens:
            raise ResourceLimitExceeded("Token limit exceeded")

    def add_cost(self, cents: int):
        self.cost_cents += cents
        if self.limits.max_cost_cents and self.cost_cents > self.limits.max_cost_cents:
            raise ResourceLimitExceeded("Cost limit exceeded")

    def add_tool_call(self):
        self.tool_calls += 1
        if self.limits.max_tool_calls and self.tool_calls > self.limits.max_tool_calls:
            raise ResourceLimitExceeded("Tool call limit exceeded")

    def add_output(self, size_bytes: int):
        self.output_bytes += size_bytes
        if self.limits.max_output_size_bytes and self.output_bytes > self.limits.max_output_size_bytes:
            raise ResourceLimitExceeded("Output size limit exceeded")

    def check_duration(self):
        if self.limits.max_duration_seconds:
            elapsed = time.time() - self.start_time
            if elapsed > self.limits.max_duration_seconds:
                raise ResourceLimitExceeded("Duration limit exceeded")

    def get_usage(self) -> Dict[str, Any]:
        return {
            "duration_seconds": time.time() - self.start_time,
            "tokens_used": self.tokens_used,
            "cost_cents": self.cost_cents,
            "tool_calls": self.tool_calls,
            "output_bytes": self.output_bytes,
        }


class ResourceLimitExceeded(Exception):
    """Raised when resource limit is exceeded."""
    pass


# =============================================================================
# DAG EXECUTOR
# =============================================================================

class DAGExecutor:
    """Execute DAG-based pipelines."""

    def __init__(
        self,
        registry,
        checkpoint_storage: Optional[CheckpointStorage] = None,
        webhook_manager: Optional[WebhookManager] = None,
        cache: Optional[ResultCache] = None,
        collaboration_manager: Optional[CollaborationManager] = None,
    ):
        self.registry = registry
        self.checkpoint_storage = checkpoint_storage or InMemoryCheckpointStorage()
        self.webhook_manager = webhook_manager or WebhookManager()
        self.cache = cache or ResultCache()
        self.collaboration = collaboration_manager or CollaborationManager()

    async def execute(
        self,
        dag: DAGPipeline,
        inputs: Dict[str, Any],
        limits: Optional[ResourceLimits] = None,
        resume_from: Optional[str] = None,  # Execution ID to resume
        enable_caching: bool = True,
        room_id: Optional[str] = None,
    ) -> AsyncIterator[PipelineEvent]:
        """Execute a DAG pipeline."""
        execution_id = resume_from or str(uuid4())
        limits = limits or ResourceLimits()
        tracker = ResourceTracker(limits)

        # Load checkpoint if resuming
        checkpoint: Optional[Checkpoint] = None
        if resume_from:
            saved = await self.checkpoint_storage.load(resume_from)
            if saved:
                checkpoint = Checkpoint.from_dict(saved)

        if checkpoint is None:
            checkpoint = Checkpoint(
                execution_id=execution_id,
                dag_id=dag.id,
                current_inputs=inputs,
            )

        # Emit started
        start_event = PipelineEvent(
            event_type=PipelineEventType.PIPELINE_STARTED,
            pipeline_id=execution_id,
            pipeline_name=dag.name,
            data={
                "dag_id": dag.id,
                "node_count": len(dag.nodes),
                "estimate": dag.estimate_cost(),
                "resumed_from": resume_from,
            },
        )
        yield start_event

        await self.webhook_manager.trigger(
            WebhookTrigger.EXECUTION_STARTED,
            execution_id,
            {"dag": dag.name, "inputs": inputs},
        )

        if room_id:
            await self.collaboration.broadcast(room_id, start_event)

        try:
            # Get execution order
            levels = dag.get_execution_order()
            node_results: Dict[str, Any] = checkpoint.node_results.copy()

            for level_idx, level_nodes in enumerate(levels):
                # Skip completed nodes
                nodes_to_run = [
                    nid for nid in level_nodes
                    if nid not in checkpoint.completed_nodes
                ]

                if not nodes_to_run:
                    continue

                # Check duration limit
                tracker.check_duration()

                # Execute level in parallel
                tasks = []
                for node_id in nodes_to_run:
                    node = dag.nodes[node_id]
                    task = asyncio.create_task(
                        self._execute_node(
                            dag, node, node_results, checkpoint.current_inputs,
                            tracker, enable_caching,
                        )
                    )
                    tasks.append((node_id, task))

                # Collect results
                for node_id, task in tasks:
                    node = dag.nodes[node_id]

                    try:
                        async for event in await task:
                            yield event

                            if room_id:
                                await self.collaboration.broadcast(room_id, event)

                            if event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                                result = event.data.get("final_result") if event.data else None
                                node_results[node_id] = result
                                checkpoint.completed_nodes.add(node_id)
                                checkpoint.node_results[node_id] = result

                                await self.webhook_manager.trigger(
                                    WebhookTrigger.NODE_COMPLETED,
                                    execution_id,
                                    {"node_id": node_id, "result": result},
                                )

                    except ResourceLimitExceeded as e:
                        yield PipelineEvent(
                            event_type=PipelineEventType.PIPELINE_FAILED,
                            pipeline_id=execution_id,
                            pipeline_name=dag.name,
                            error=f"Resource limit: {str(e)}",
                        )
                        return

                    except Exception as e:
                        if node.max_retries > 0:
                            # Retry logic would go here
                            pass
                        raise

                # Checkpoint after each level
                checkpoint.updated_at = datetime.utcnow()
                await self.checkpoint_storage.save(
                    execution_id,
                    checkpoint.to_dict(),
                )

            # Completed
            complete_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_COMPLETED,
                pipeline_id=execution_id,
                pipeline_name=dag.name,
                data={
                    "final_result": node_results,
                    "nodes_completed": len(checkpoint.completed_nodes),
                    "resource_usage": tracker.get_usage(),
                },
            )
            yield complete_event

            if room_id:
                await self.collaboration.broadcast(room_id, complete_event)

            await self.webhook_manager.trigger(
                WebhookTrigger.EXECUTION_COMPLETED,
                execution_id,
                {"result": node_results, "usage": tracker.get_usage()},
            )

            # Clean up checkpoint
            await self.checkpoint_storage.delete(execution_id)

        except Exception as e:
            error_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_FAILED,
                pipeline_id=execution_id,
                pipeline_name=dag.name,
                error=str(e),
            )
            yield error_event

            if room_id:
                await self.collaboration.broadcast(room_id, error_event)

            await self.webhook_manager.trigger(
                WebhookTrigger.EXECUTION_FAILED,
                execution_id,
                {"error": str(e)},
            )

    async def _execute_node(
        self,
        dag: DAGPipeline,
        node: DAGNode,
        node_results: Dict[str, Any],
        current_inputs: Dict[str, Any],
        tracker: ResourceTracker,
        enable_caching: bool,
    ) -> AsyncIterator[PipelineEvent]:
        """Execute a single DAG node."""
        if node.type == DAGNodeType.PIPELINE:
            # Build inputs
            node_inputs = {**current_inputs, **node.inputs}

            # Apply mappings from previous node results
            for target, source in node.input_mappings.items():
                parts = source.split(".")
                value = node_results
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        value = None
                        break
                if value is not None:
                    node_inputs[target] = value

            # Check cache
            if enable_caching:
                cached = self.cache.get(node.pipeline, node_inputs)
                if cached is not None:
                    yield PipelineEvent(
                        event_type=PipelineEventType.PIPELINE_COMPLETED,
                        pipeline_id=node.id,
                        pipeline_name=node.pipeline,
                        data={"final_result": cached, "from_cache": True},
                    )
                    return

            # Execute pipeline
            tracker.add_tool_call()

            async for event in self.registry.execute_stream(node.pipeline, **node_inputs):
                yield event

                # Track output size
                event_size = len(event.model_dump_json())
                tracker.add_output(event_size)

                # Cache result
                if enable_caching and event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                    if event.data:
                        self.cache.set(
                            node.pipeline,
                            node_inputs,
                            event.data.get("final_result"),
                        )

        elif node.type == DAGNodeType.CONDITIONAL:
            # Evaluate condition
            condition_result = self._evaluate_condition(
                node.condition, node_results, current_inputs
            )

            next_node_id = node.true_branch if condition_result else node.false_branch

            if next_node_id and next_node_id in dag.nodes:
                next_node = dag.nodes[next_node_id]
                async for event in self._execute_node(
                    dag, next_node, node_results, current_inputs,
                    tracker, enable_caching,
                ):
                    yield event

        elif node.type == DAGNodeType.TRANSFORM:
            # Transform data
            result = self._evaluate_transform(
                node.transform_expression, node_results, current_inputs
            )

            yield PipelineEvent(
                event_type=PipelineEventType.PIPELINE_COMPLETED,
                pipeline_id=node.id,
                pipeline_name=f"transform:{node.name}",
                data={"final_result": result},
            )

    def _evaluate_condition(
        self,
        condition: str,
        node_results: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> bool:
        """Evaluate condition expression."""
        try:
            return eval(
                condition,
                {"__builtins__": {}},
                {"results": node_results, "inputs": inputs},
            )
        except Exception:
            return False

    def _evaluate_transform(
        self,
        expression: str,
        node_results: Dict[str, Any],
        inputs: Dict[str, Any],
    ) -> Any:
        """Evaluate transform expression."""
        try:
            return eval(
                expression,
                {"__builtins__": {"len": len, "str": str, "int": int, "float": float, "list": list, "dict": dict}},
                {"results": node_results, "inputs": inputs},
            )
        except Exception as e:
            return {"error": str(e)}


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # DAG
    "DAGNodeType",
    "DAGNode",
    "DAGPipeline",
    # Checkpointing
    "CheckpointStorage",
    "InMemoryCheckpointStorage",
    "Checkpoint",
    # Webhooks
    "WebhookTrigger",
    "WebhookConfig",
    "WebhookDelivery",
    "WebhookManager",
    # Caching
    "ResultCache",
    # Priority Queue
    "ExecutionPriority",
    "PriorityItem",
    "PriorityExecutionQueue",
    # Collaboration
    "RoomParticipant",
    "CollaborationRoom",
    "CollaborationManager",
    # Resource Limits
    "ResourceLimits",
    "ResourceTracker",
    "ResourceLimitExceeded",
    # Executor
    "DAGExecutor",
]
