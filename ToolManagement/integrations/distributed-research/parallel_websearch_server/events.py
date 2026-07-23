"""Real-time event streaming for UI updates.

Provides Server-Sent Events (SSE) for real-time notifications when:
- Task groups are created/updated
- Task runs change status (queued -> running -> completed/failed)
- Searches complete
- FindAll runs progress
- Errors occur

Usage:
    # Subscribe to all events
    GET /v1/events/stream

    # Subscribe to specific event types
    GET /v1/events/stream?types=task_run,task_group

    # Subscribe to events for a specific resource
    GET /v1/events/stream?resource_id=tgrp_abc123
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from weakref import WeakSet

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """Types of events that can be emitted."""
    # Task Group events
    TASK_GROUP_CREATED = "task_group.created"
    TASK_GROUP_UPDATED = "task_group.updated"
    TASK_GROUP_COMPLETED = "task_group.completed"

    # Task Run events
    TASK_RUN_CREATED = "task_run.created"
    TASK_RUN_QUEUED = "task_run.queued"
    TASK_RUN_RUNNING = "task_run.running"
    TASK_RUN_COMPLETED = "task_run.completed"
    TASK_RUN_FAILED = "task_run.failed"
    TASK_RUN_CANCELLED = "task_run.cancelled"

    # Search events
    SEARCH_STARTED = "search.started"
    SEARCH_COMPLETED = "search.completed"
    SEARCH_FAILED = "search.failed"

    # Extract events
    EXTRACT_STARTED = "extract.started"
    EXTRACT_COMPLETED = "extract.completed"
    EXTRACT_FAILED = "extract.failed"

    # FindAll events
    FINDALL_CREATED = "findall.created"
    FINDALL_RUNNING = "findall.running"
    FINDALL_PROGRESS = "findall.progress"
    FINDALL_COMPLETED = "findall.completed"
    FINDALL_FAILED = "findall.failed"

    # System events
    SYSTEM_ERROR = "system.error"
    SYSTEM_WARNING = "system.warning"
    HEARTBEAT = "heartbeat"


@dataclass
class Event:
    """A single event to be broadcast to subscribers."""
    type: EventType
    data: Dict[str, Any]
    resource_id: Optional[str] = None
    resource_type: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def to_sse(self) -> str:
        """Format as Server-Sent Event."""
        payload = {
            "type": self.type.value if isinstance(self.type, EventType) else self.type,
            "data": self.data,
            "resource_id": self.resource_id,
            "resource_type": self.resource_type,
            "timestamp": self.timestamp,
            "event_id": self.event_id,
        }
        return f"id: {self.event_id}\nevent: {self.type.value if isinstance(self.type, EventType) else self.type}\ndata: {json.dumps(payload)}\n\n"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.type.value if isinstance(self.type, EventType) else self.type,
            "data": self.data,
            "resource_id": self.resource_id,
            "resource_type": self.resource_type,
            "timestamp": self.timestamp,
            "event_id": self.event_id,
        }


@dataclass
class Subscriber:
    """A client subscribed to events."""
    id: str
    queue: asyncio.Queue
    event_types: Optional[Set[str]] = None  # None means all events
    resource_ids: Optional[Set[str]] = None  # None means all resources
    created_at: float = field(default_factory=time.time)
    last_event_id: Optional[str] = None

    def matches(self, event: Event) -> bool:
        """Check if this subscriber should receive the event."""
        # Filter by event type
        if self.event_types:
            event_type = event.type.value if isinstance(event.type, EventType) else event.type
            if event_type not in self.event_types:
                return False

        # Filter by resource ID
        if self.resource_ids and event.resource_id:
            if event.resource_id not in self.resource_ids:
                return False

        return True


class EventBus:
    """Central event bus for publishing and subscribing to events.

    Thread-safe singleton that manages all event subscriptions and broadcasts.

    Example:
        bus = get_event_bus()

        # Publish an event
        bus.publish(Event(
            type=EventType.TASK_RUN_COMPLETED,
            data={"run_id": "trun_123", "output": "..."},
            resource_id="trun_123",
            resource_type="task_run"
        ))

        # Subscribe to events (in an async context)
        async for event in bus.subscribe():
            print(event)
    """

    _instance: Optional["EventBus"] = None

    def __init__(self):
        self._subscribers: Dict[str, Subscriber] = {}
        self._event_history: List[Event] = []
        self._history_limit = 1000
        self._lock = asyncio.Lock()
        self._stats = {
            "events_published": 0,
            "events_delivered": 0,
            "subscribers_total": 0,
            "subscribers_active": 0,
        }

    @classmethod
    def get_instance(cls) -> "EventBus":
        """Get the singleton EventBus instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def publish(self, event: Event) -> int:
        """Publish an event to all matching subscribers.

        Args:
            event: The event to publish

        Returns:
            Number of subscribers that received the event
        """
        delivered = 0

        async with self._lock:
            # Store in history
            self._event_history.append(event)
            if len(self._event_history) > self._history_limit:
                self._event_history = self._event_history[-self._history_limit:]

            self._stats["events_published"] += 1

            # Deliver to matching subscribers
            for subscriber in list(self._subscribers.values()):
                if subscriber.matches(event):
                    try:
                        await subscriber.queue.put(event)
                        delivered += 1
                        self._stats["events_delivered"] += 1
                    except Exception as e:
                        logger.warning(f"Failed to deliver event to subscriber {subscriber.id}: {e}")

        logger.debug(f"Published event {event.type} to {delivered} subscribers")
        return delivered

    def publish_sync(self, event: Event) -> None:
        """Synchronous wrapper to publish events from non-async code.

        Stores event in history immediately for synchronous access,
        persists to SQLite for durability,
        then schedules delivery to subscribers asynchronously.
        """
        # Store in memory history immediately (synchronous)
        self._event_history.append(event)
        if len(self._event_history) > self._history_limit:
            self._event_history = self._event_history[-self._history_limit:]
        self._stats["events_published"] += 1

        # Persist to SQLite for durability (30-day rolling)
        try:
            from parallel_websearch_server.local_store import get_local_store
            store = get_local_store()
            event_type = event.type.value if isinstance(event.type, EventType) else event.type
            store.save_event(
                event_id=event.event_id,
                event_type=event_type,
                data=event.data,
                resource_id=event.resource_id,
                resource_type=event.resource_type,
                created_at=event.timestamp,
            )
        except Exception as e:
            logger.warning(f"Failed to persist event to SQLite: {e}")

        # Schedule async delivery to subscribers
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self._deliver_to_subscribers(event))
        except RuntimeError:
            # No running loop - no subscribers to deliver to anyway
            pass

    async def _deliver_to_subscribers(self, event: Event) -> int:
        """Deliver event to matching subscribers (internal helper)."""
        delivered = 0
        async with self._lock:
            for subscriber in list(self._subscribers.values()):
                if subscriber.matches(event):
                    try:
                        await subscriber.queue.put(event)
                        delivered += 1
                        self._stats["events_delivered"] += 1
                    except Exception as e:
                        logger.warning(f"Failed to deliver event to subscriber {subscriber.id}: {e}")
        logger.debug(f"Delivered event {event.type} to {delivered} subscribers")
        return delivered

    async def subscribe(
        self,
        event_types: Optional[List[str]] = None,
        resource_ids: Optional[List[str]] = None,
        last_event_id: Optional[str] = None,
    ) -> AsyncGenerator[Event, None]:
        """Subscribe to events.

        Args:
            event_types: List of event types to receive (None for all)
            resource_ids: List of resource IDs to filter by (None for all)
            last_event_id: Resume from this event ID (for reconnection)

        Yields:
            Events matching the subscription criteria
        """
        subscriber_id = str(uuid.uuid4())
        subscriber = Subscriber(
            id=subscriber_id,
            queue=asyncio.Queue(maxsize=100),
            event_types=set(event_types) if event_types else None,
            resource_ids=set(resource_ids) if resource_ids else None,
            last_event_id=last_event_id,
        )

        async with self._lock:
            self._subscribers[subscriber_id] = subscriber
            self._stats["subscribers_total"] += 1
            self._stats["subscribers_active"] = len(self._subscribers)

        logger.info(f"New subscriber {subscriber_id} (types={event_types}, resources={resource_ids})")

        try:
            # Replay missed events if last_event_id provided
            if last_event_id:
                found = False
                for event in self._event_history:
                    if found and subscriber.matches(event):
                        yield event
                    if event.event_id == last_event_id:
                        found = True

            # Stream new events
            while True:
                try:
                    event = await asyncio.wait_for(subscriber.queue.get(), timeout=30.0)
                    yield event
                except asyncio.TimeoutError:
                    # Send heartbeat to keep connection alive
                    yield Event(
                        type=EventType.HEARTBEAT,
                        data={"subscriber_id": subscriber_id, "timestamp": time.time()},
                    )
        finally:
            async with self._lock:
                self._subscribers.pop(subscriber_id, None)
                self._stats["subscribers_active"] = len(self._subscribers)
            logger.info(f"Subscriber {subscriber_id} disconnected")

    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        return {
            **self._stats,
            "history_size": len(self._event_history),
            "history_limit": self._history_limit,
        }

    def get_recent_events(self, limit: int = 50, event_types: Optional[List[str]] = None) -> List[Dict]:
        """Get recent events from history."""
        events = self._event_history[-limit:]
        if event_types:
            type_set = set(event_types)
            events = [e for e in events if (e.type.value if isinstance(e.type, EventType) else e.type) in type_set]
        return [e.to_dict() for e in events]


# Singleton accessor
def get_event_bus() -> EventBus:
    """Get the global EventBus instance."""
    return EventBus.get_instance()


# Convenience functions for publishing events
def emit_task_group_created(taskgroup_id: str, metadata: Optional[Dict] = None) -> None:
    """Emit event when a task group is created."""
    get_event_bus().publish_sync(Event(
        type=EventType.TASK_GROUP_CREATED,
        data={"taskgroup_id": taskgroup_id, "metadata": metadata},
        resource_id=taskgroup_id,
        resource_type="task_group",
    ))


def emit_task_group_updated(taskgroup_id: str, status: str, run_counts: Optional[Dict] = None) -> None:
    """Emit event when a task group is updated."""
    get_event_bus().publish_sync(Event(
        type=EventType.TASK_GROUP_UPDATED,
        data={"taskgroup_id": taskgroup_id, "status": status, "run_counts": run_counts},
        resource_id=taskgroup_id,
        resource_type="task_group",
    ))


def emit_task_run_created(run_id: str, processor: str, taskgroup_id: Optional[str] = None, input_preview: Optional[str] = None) -> None:
    """Emit event when a task run is created."""
    get_event_bus().publish_sync(Event(
        type=EventType.TASK_RUN_CREATED,
        data={
            "run_id": run_id,
            "processor": processor,
            "taskgroup_id": taskgroup_id,
            "input_preview": input_preview[:200] if input_preview else None,
        },
        resource_id=run_id,
        resource_type="task_run",
    ))


def emit_task_run_status(run_id: str, status: str, output_preview: Optional[str] = None, error: Optional[str] = None) -> None:
    """Emit event when a task run status changes."""
    status_to_event = {
        "queued": EventType.TASK_RUN_QUEUED,
        "running": EventType.TASK_RUN_RUNNING,
        "completed": EventType.TASK_RUN_COMPLETED,
        "failed": EventType.TASK_RUN_FAILED,
        "cancelled": EventType.TASK_RUN_CANCELLED,
    }
    event_type = status_to_event.get(status, EventType.TASK_RUN_QUEUED)

    get_event_bus().publish_sync(Event(
        type=event_type,
        data={
            "run_id": run_id,
            "status": status,
            "output_preview": output_preview[:500] if output_preview else None,
            "error": error,
        },
        resource_id=run_id,
        resource_type="task_run",
    ))


def emit_search_event(event_type: str, objective: str, request_id: Optional[str] = None, results_count: Optional[int] = None, error: Optional[str] = None) -> None:
    """Emit search-related event."""
    type_map = {
        "started": EventType.SEARCH_STARTED,
        "completed": EventType.SEARCH_COMPLETED,
        "failed": EventType.SEARCH_FAILED,
    }
    get_event_bus().publish_sync(Event(
        type=type_map.get(event_type, EventType.SEARCH_STARTED),
        data={
            "objective": objective[:200] if objective else None,
            "request_id": request_id,
            "results_count": results_count,
            "error": error,
        },
        resource_id=request_id,
        resource_type="search",
    ))


def emit_findall_event(event_type: str, findall_id: str, objective: Optional[str] = None,
                       candidates_found: int = 0, candidates_matched: int = 0, error: Optional[str] = None) -> None:
    """Emit FindAll-related event."""
    type_map = {
        "created": EventType.FINDALL_CREATED,
        "running": EventType.FINDALL_RUNNING,
        "progress": EventType.FINDALL_PROGRESS,
        "completed": EventType.FINDALL_COMPLETED,
        "failed": EventType.FINDALL_FAILED,
    }
    get_event_bus().publish_sync(Event(
        type=type_map.get(event_type, EventType.FINDALL_CREATED),
        data={
            "findall_id": findall_id,
            "objective": objective[:200] if objective else None,
            "candidates_found": candidates_found,
            "candidates_matched": candidates_matched,
            "error": error,
        },
        resource_id=findall_id,
        resource_type="findall",
    ))


def emit_error(message: str, details: Optional[Dict] = None, resource_id: Optional[str] = None) -> None:
    """Emit a system error event."""
    get_event_bus().publish_sync(Event(
        type=EventType.SYSTEM_ERROR,
        data={"message": message, "details": details},
        resource_id=resource_id,
    ))


def emit_warning(message: str, details: Optional[Dict] = None) -> None:
    """Emit a system warning event."""
    get_event_bus().publish_sync(Event(
        type=EventType.SYSTEM_WARNING,
        data={"message": message, "details": details},
    ))
