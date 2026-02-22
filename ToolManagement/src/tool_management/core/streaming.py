"""
Streaming Execution for Long-Running Tools.

Provides:
- Server-Sent Events (SSE) for real-time updates
- Progress tracking
- Partial results
- Cancellation support
"""

from __future__ import annotations

import asyncio
import hashlib
import heapq
import os
import uuid
import time
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """Types of streaming events."""
    STARTED = "started"
    PROGRESS = "progress"
    PARTIAL = "partial"
    SIGNAL = "signal"
    LOG = "log"
    PAUSED = "paused"
    RESUMED = "resumed"
    DETACHED = "detached"
    COMPLETED = "completed"
    ERROR = "error"
    CANCELLED = "cancelled"
    HEARTBEAT = "heartbeat"


@dataclass
class ToolEvent:
    """An event from a streaming tool execution."""
    type: EventType
    execution_id: str
    timestamp: float = field(default_factory=time.time)

    # Progress info
    progress: Optional[float] = None  # 0-100
    message: Optional[str] = None

    # Data
    data: Any = None

    # Error info
    error: Optional[str] = None

    def to_sse(self) -> str:
        """Format as Server-Sent Event."""
        event_data = {
            "type": self.type.value,
            "execution_id": self.execution_id,
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat(),
        }

        if self.progress is not None:
            event_data["progress"] = self.progress
        if self.message:
            event_data["message"] = self.message
        if self.data is not None:
            event_data["data"] = self.data
        if self.error:
            event_data["error"] = self.error

        return f"event: {self.type.value}\ndata: {json.dumps(event_data)}\n\n"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type.value,
            "execution_id": self.execution_id,
            "timestamp": self.timestamp,
            "progress": self.progress,
            "message": self.message,
            "data": self.data,
            "error": self.error,
        }


@dataclass
class StreamingExecution:
    """State for a streaming execution."""
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tool_name: str = ""
    client_id: Optional[str] = None
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None

    # State
    status: str = "pending"  # pending, running, completed, error, cancelled
    progress: float = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    queued_at: Optional[float] = None
    queue_priority: int = 0

    # Events (bounded to prevent memory leak)
    events: List[ToolEvent] = field(default_factory=list)
    _max_events: int = 1000

    # Subscribers
    _subscribers: List[asyncio.Queue] = field(default_factory=list)

    # Cancellation
    _cancelled: bool = False
    _cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    _task: Optional[asyncio.Task] = None

    # Flow control
    _paused: bool = False
    _pause_event: asyncio.Event = field(default_factory=asyncio.Event)
    detached: bool = False
    dedupe_key: Optional[str] = None
    control_state: Dict[str, Any] = field(default_factory=dict)
    _event_persistor: Optional[Callable[["StreamingExecution", ToolEvent], None]] = None

    def __post_init__(self):
        # Not paused by default.
        self._pause_event.set()

    def subscribe(self) -> asyncio.Queue:
        """Subscribe to events."""
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        """Unsubscribe from events."""
        if queue in self._subscribers:
            self._subscribers.remove(queue)

    async def emit(self, event: ToolEvent):
        """Emit an event to all subscribers."""
        self.events.append(event)
        # Trim old non-terminal events to prevent unbounded memory growth
        if len(self.events) > self._max_events:
            # Keep last _max_events entries, always preserving STARTED and terminal events
            trimmed = [e for e in self.events[:len(self.events) - self._max_events]
                       if e.type in (EventType.STARTED, EventType.COMPLETED, EventType.ERROR, EventType.CANCELLED)]
            self.events = trimmed + self.events[len(self.events) - self._max_events:]

        for queue in self._subscribers:
            try:
                await queue.put(event)
            except Exception:
                pass

        if self._event_persistor:
            try:
                self._event_persistor(self, event)
            except Exception as exc:
                logger.debug(f"Failed to persist execution event {self.execution_id}: {exc}")

    def cancel(self):
        """Request cancellation."""
        self._cancelled = True
        self._cancel_event.set()
        if self._task and not self._task.done():
            self._task.cancel()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def pause(self):
        """Pause event emission."""
        self._paused = True
        self._pause_event.clear()

    def resume(self):
        """Resume event emission."""
        self._paused = False
        self._pause_event.set()

    @property
    def is_paused(self) -> bool:
        return self._paused

    async def wait_if_paused(self):
        """Block while paused."""
        await self._pause_event.wait()

    def detach(self):
        """Detach execution from the active client stream."""
        self.detached = True


class StreamingExecutor:
    """
    Executes tools with streaming support.

    Features:
    - Real-time progress updates
    - Partial results
    - Cancellation
    - Heartbeat for connection keepalive
    """

    def __init__(
        self,
        tool_executor: Callable,
        heartbeat_interval: float = 15.0,
        max_concurrent: int = 8,
        execution_store: Any = None,
    ):
        """
        Initialize executor.

        Args:
            tool_executor: Async function(tool_name, inputs, progress_callback) -> result
            heartbeat_interval: Seconds between heartbeat events
            max_concurrent: Maximum number of concurrent running executions
        """
        self.tool_executor = tool_executor
        self.heartbeat_interval = heartbeat_interval
        self.max_concurrent = max(1, int(max_concurrent))

        self._executions: Dict[str, StreamingExecution] = {}
        self._pending_queue: List[tuple[int, float, str]] = []
        self._execution_inputs: Dict[str, Dict[str, Any]] = {}
        self._active_count: int = 0
        self._queue_event: asyncio.Event = asyncio.Event()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._dedupe_index: Dict[str, tuple[str, float]] = {}
        self._shutdown: bool = False
        self._execution_store = execution_store

        if self._execution_store is None and os.getenv("ENABLE_DURABLE_EXECUTIONS", "1") == "1":
            try:
                from ..persistence import get_execution_store
                self._execution_store = get_execution_store()
            except Exception as exc:
                logger.warning(f"Durable execution store unavailable: {exc}")
                self._execution_store = None

    async def execute(
        self,
        tool_name: str,
        inputs: Dict[str, Any],
        client_id: str = None,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        parent_execution_id: Optional[str] = None,
        priority: int | str | None = None,
        dedupe_window_seconds: int = 0,
        dedupe_key: Optional[str] = None,
        callback_url: Optional[str] = None,
        callback_events: Optional[List[str]] = None,
    ) -> StreamingExecution:
        """
        Start a streaming execution.

        Args:
            tool_name: Tool to execute
            inputs: Tool inputs
            client_id: Client identifier

        Returns:
            StreamingExecution that can be subscribed to
        """
        normalized_priority = self._normalize_priority(priority)
        now = time.time()
        effective_dedupe_key = dedupe_key or self._build_dedupe_key(tool_name, inputs)
        dedupe_window_seconds = max(0, int(dedupe_window_seconds))
        if dedupe_window_seconds > 0:
            self._prune_dedupe_index(now, dedupe_window_seconds)
            existing = self._dedupe_index.get(effective_dedupe_key)
            if existing:
                existing_id, created_at = existing
                existing_execution = self._executions.get(existing_id)
                if existing_execution and (now - created_at) <= dedupe_window_seconds:
                    if existing_execution.status not in {"error", "cancelled"}:
                        existing_execution.control_state["submit_count"] = (
                            int(existing_execution.control_state.get("submit_count", 1)) + 1
                        )
                        existing_execution.control_state["deduplicated_requests"] = (
                            int(existing_execution.control_state.get("deduplicated_requests", 0)) + 1
                        )
                        existing_execution.control_state["last_dedupe_reuse_at"] = now
                        return existing_execution
            if self._execution_store:
                stored = self._execution_store.find_recent_by_dedupe_key(
                    effective_dedupe_key,
                    now - dedupe_window_seconds,
                )
                if stored:
                    existing_id = stored.get("execution_id")
                    if existing_id:
                        existing_execution = self._executions.get(existing_id) or self._hydrate_execution(existing_id)
                        if existing_execution and existing_execution.status not in {"error", "cancelled"}:
                            existing_execution.control_state["submit_count"] = (
                                int(existing_execution.control_state.get("submit_count", 1)) + 1
                            )
                            existing_execution.control_state["deduplicated_requests"] = (
                                int(existing_execution.control_state.get("deduplicated_requests", 0)) + 1
                            )
                            existing_execution.control_state["last_dedupe_reuse_at"] = now
                            self._persist_execution(existing_execution)
                            return existing_execution

        execution = StreamingExecution(
            tool_name=tool_name,
            client_id=client_id,
            trace_id=trace_id or (inputs.get("_trace_id") if isinstance(inputs, dict) else None),
            session_id=session_id or (inputs.get("_session_id") if isinstance(inputs, dict) else None),
            parent_execution_id=parent_execution_id or (inputs.get("_parent_execution_id") if isinstance(inputs, dict) else None),
            status="queued",
            queued_at=now,
            queue_priority=normalized_priority,
            dedupe_key=effective_dedupe_key,
        )
        execution.control_state["submit_count"] = 1
        if execution.trace_id:
            execution.control_state["trace_id"] = execution.trace_id
        if execution.session_id:
            execution.control_state["session_id"] = execution.session_id
        if execution.parent_execution_id:
            execution.control_state["parent_execution_id"] = execution.parent_execution_id
        if callback_url:
            execution.control_state["callback_url"] = callback_url
            execution.control_state["callback_events"] = self._normalize_callback_events(callback_events)
        execution._event_persistor = self._persist_event

        self._executions[execution.execution_id] = execution
        if dedupe_window_seconds > 0:
            self._dedupe_index[effective_dedupe_key] = (execution.execution_id, now)
        self._persist_execution(execution)

        # Queue execution and let scheduler honor priority + concurrency.
        heapq.heappush(
            self._pending_queue,
            (-normalized_priority, now, execution.execution_id),
        )
        self._execution_inputs[execution.execution_id] = dict(inputs)
        await execution.emit(ToolEvent(
            type=EventType.PROGRESS,
            execution_id=execution.execution_id,
            progress=0.0,
            message="queued",
        ))
        self._ensure_scheduler()
        self._queue_event.set()

        return execution

    def _ensure_scheduler(self):
        if self._scheduler_task and not self._scheduler_task.done():
            return
        self._shutdown = False
        self._scheduler_task = asyncio.create_task(self._schedule_loop())

    async def _schedule_loop(self):
        """Dispatch queued executions according to priority and concurrency limits."""
        while not self._shutdown:
            await self._queue_event.wait()
            self._queue_event.clear()

            while self._pending_queue and self._active_count < self.max_concurrent:
                _neg_priority, _enqueued_at, execution_id = heapq.heappop(self._pending_queue)
                execution = self._executions.get(execution_id)
                if not execution:
                    continue
                if execution.status in {"cancelled", "completed", "error"}:
                    continue
                if execution.is_cancelled:
                    execution.status = "cancelled"
                    execution.completed_at = time.time()
                    continue

                inputs = self._execution_inputs.get(execution_id, {})
                self._active_count += 1
                execution._task = asyncio.create_task(self._run_execution(execution, inputs))

            if self._pending_queue and self._active_count < self.max_concurrent:
                # Re-trigger loop to avoid starvation on race edges.
                self._queue_event.set()

    async def _run_execution(
        self,
        execution: StreamingExecution,
        inputs: Dict[str, Any],
    ):
        """Run the actual execution with event emission."""
        execution.status = "running"
        execution.started_at = time.time()
        self._persist_execution(execution)

        # Emit started event
        await execution.emit(ToolEvent(
            type=EventType.STARTED,
            execution_id=execution.execution_id,
            message=f"Starting {execution.tool_name}",
        ))

        # Progress callback for the tool
        async def progress_callback(
            progress: float = None,
            message: str = None,
            partial_data: Any = None,
        ):
            await execution.wait_if_paused()
            if execution.is_cancelled:
                raise asyncio.CancelledError("Execution cancelled")

            if progress is not None:
                execution.progress = progress

            if partial_data is not None:
                await execution.emit(ToolEvent(
                    type=EventType.PARTIAL,
                    execution_id=execution.execution_id,
                    progress=progress,
                    message=message,
                    data=partial_data,
                ))
            elif progress is not None or message:
                await execution.emit(ToolEvent(
                    type=EventType.PROGRESS,
                    execution_id=execution.execution_id,
                    progress=progress,
                    message=message,
                ))

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(execution)
        )

        try:
            # Execute with progress callback
            result = await self.tool_executor(
                execution.tool_name,
                inputs,
                progress_callback=progress_callback,
            )

            execution.status = "completed"
            execution.completed_at = time.time()
            execution.progress = 100

            await execution.emit(ToolEvent(
                type=EventType.COMPLETED,
                execution_id=execution.execution_id,
                progress=100,
                message="Completed",
                data=result,
            ))

        except asyncio.CancelledError:
            execution.status = "cancelled"
            execution.completed_at = time.time()

            await execution.emit(ToolEvent(
                type=EventType.CANCELLED,
                execution_id=execution.execution_id,
                message="Execution cancelled",
            ))

        except Exception as e:
            execution.status = "error"
            execution.completed_at = time.time()

            await execution.emit(ToolEvent(
                type=EventType.ERROR,
                execution_id=execution.execution_id,
                error=str(e),
                message=f"Error: {e}",
            ))

        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            if execution.status in {"completed", "error", "cancelled"}:
                self._execution_inputs.pop(execution.execution_id, None)
                self._persist_execution(execution)
            self._active_count = max(0, self._active_count - 1)
            self._queue_event.set()

    async def _heartbeat_loop(self, execution: StreamingExecution):
        """Send periodic heartbeats."""
        while execution.status == "running":
            await asyncio.sleep(self.heartbeat_interval)
            if execution.status == "running":
                await execution.emit(ToolEvent(
                    type=EventType.HEARTBEAT,
                    execution_id=execution.execution_id,
                    progress=execution.progress,
                ))

    def get_execution(self, execution_id: str) -> Optional[StreamingExecution]:
        """Get an execution by ID."""
        execution = self._executions.get(execution_id)
        if execution:
            return execution
        return self._hydrate_execution(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel an execution."""
        execution = self._executions.get(execution_id)
        if not execution:
            return False
        if execution.status in {"running", "queued", "pending"}:
            execution.cancel()
            if execution.status in {"queued", "pending"}:
                execution.status = "cancelled"
                execution.completed_at = time.time()
                self._execution_inputs.pop(execution.execution_id, None)
                self._persist_execution(execution)
                try:
                    asyncio.create_task(execution.emit(ToolEvent(
                        type=EventType.CANCELLED,
                        execution_id=execution.execution_id,
                        message="Execution cancelled",
                    )))
                except RuntimeError:
                    pass
            self._queue_event.set()
            return True
        return False

    async def pause_execution(self, execution_id: str) -> bool:
        """Pause event emission for an execution."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == "running" and not execution.is_paused:
            execution.pause()
            await execution.emit(ToolEvent(
                type=EventType.PAUSED,
                execution_id=execution.execution_id,
                message="Execution paused",
                progress=execution.progress,
            ))
            return True
        return False

    async def resume_execution(self, execution_id: str) -> bool:
        """Resume event emission for an execution."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == "running" and execution.is_paused:
            execution.resume()
            await execution.emit(ToolEvent(
                type=EventType.RESUMED,
                execution_id=execution.execution_id,
                message="Execution resumed",
                progress=execution.progress,
            ))
            return True
        return False

    async def detach_execution(self, execution_id: str) -> bool:
        """Mark a running execution as detached from the current stream consumer."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == "running" and not execution.detached:
            execution.detach()
            await execution.emit(ToolEvent(
                type=EventType.DETACHED,
                execution_id=execution.execution_id,
                message="Execution detached",
                progress=execution.progress,
            ))
            return True
        return False

    async def emit_signal(
        self,
        execution_id: str,
        signal_type: str,
        data: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
    ) -> bool:
        """Emit an advisory signal event for an execution."""
        execution = self._executions.get(execution_id)
        if not execution:
            return False
        payload: Dict[str, Any] = {"type": signal_type}
        if data:
            payload.update(data)
        await execution.emit(ToolEvent(
            type=EventType.SIGNAL,
            execution_id=execution.execution_id,
            message=message,
            data=payload,
            progress=execution.progress,
        ))
        return True

    def update_control_state(self, execution_id: str, values: Dict[str, Any]) -> bool:
        """Attach control metadata to an execution (priority, filters, etc.)."""
        execution = self._executions.get(execution_id)
        if not execution:
            return False
        execution.control_state.update(values)
        return True

    def reprioritize_execution(self, execution_id: str, priority: int | str) -> bool:
        """Reprioritize a queued execution."""
        execution = self._executions.get(execution_id)
        if not execution:
            return False
        if execution.status not in {"pending", "queued"}:
            return False

        new_priority = self._normalize_priority(priority)
        execution.queue_priority = new_priority
        execution.control_state["priority"] = priority
        self._persist_execution(execution)
        heapq.heappush(
            self._pending_queue,
            (-new_priority, time.time(), execution.execution_id),
        )
        self._queue_event.set()
        return True

    async def stream_events(
        self,
        execution_id: str,
        include_history: bool = True,
    ) -> AsyncIterator[ToolEvent]:
        """
        Stream events for an execution.

        Args:
            execution_id: Execution to stream
            include_history: Include events that already occurred

        Yields:
            ToolEvent objects
        """
        execution = self._executions.get(execution_id)
        if not execution:
            return

        # Subscribe to new events
        queue = execution.subscribe()

        try:
            # Send historical events
            if include_history:
                for event in execution.events:
                    yield event

            # Stream new events
            while execution.status in {"pending", "queued", "running"} or not queue.empty():
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    yield event

                    # Stop on terminal events
                    if event.type in (EventType.COMPLETED, EventType.ERROR, EventType.CANCELLED):
                        break
                except asyncio.TimeoutError:
                    # Yield heartbeat on timeout
                    yield ToolEvent(
                        type=EventType.HEARTBEAT,
                        execution_id=execution_id,
                    )

        finally:
            execution.unsubscribe(queue)

    async def stream_sse(
        self,
        execution_id: str,
        include_history: bool = True,
    ) -> AsyncIterator[str]:
        """
        Stream events as SSE format.

        Args:
            execution_id: Execution to stream
            include_history: Include historical events

        Yields:
            SSE-formatted strings
        """
        async for event in self.stream_events(execution_id, include_history):
            yield event.to_sse()

    def get_stats(self) -> Dict[str, Any]:
        """Get executor statistics."""
        statuses = {}
        for ex in self._executions.values():
            statuses[ex.status] = statuses.get(ex.status, 0) + 1

        return {
            "total_executions": len(self._executions),
            "by_status": statuses,
            "active_count": self._active_count,
            "queue_depth": len(self._pending_queue),
            "max_concurrent": self.max_concurrent,
            "durable_store_enabled": bool(self._execution_store),
        }

    def list_executions(self, status: Optional[str] = None, limit: int = 100) -> List[StreamingExecution]:
        """List executions from memory plus durable storage."""
        merged: Dict[str, StreamingExecution] = {}
        for execution in self._executions.values():
            if status and execution.status != status:
                continue
            merged[execution.execution_id] = execution

        if self._execution_store:
            try:
                rows = self._execution_store.list_executions(status=status, limit=max(limit, 200))
                for row in rows:
                    execution_id = row.get("execution_id")
                    if not execution_id or execution_id in merged:
                        continue
                    hydrated = self._hydrate_execution(execution_id, row=row)
                    if hydrated:
                        merged[execution_id] = hydrated
                    if len(merged) >= limit:
                        break
            except Exception as exc:
                logger.debug(f"Failed to load durable executions: {exc}")

        values = list(merged.values())
        values.sort(key=lambda e: (e.started_at or e.queued_at or 0), reverse=True)
        return values[:limit]

    def cleanup_old(self, max_age_seconds: int = 3600):
        """Remove old completed executions."""
        now = time.time()
        to_remove = []

        for ex_id, ex in self._executions.items():
            if ex.completed_at and (now - ex.completed_at) > max_age_seconds:
                to_remove.append(ex_id)

        for ex_id in to_remove:
            del self._executions[ex_id]
            self._execution_inputs.pop(ex_id, None)

        # Drop stale dedupe mappings that no longer resolve.
        stale_keys = [
            key for key, (ex_id, created_at) in self._dedupe_index.items()
            if ex_id not in self._executions or (now - created_at) > max_age_seconds
        ]
        for key in stale_keys:
            self._dedupe_index.pop(key, None)

        if self._execution_store:
            try:
                self._execution_store.cleanup_old(max_age_seconds=max_age_seconds)
            except Exception as exc:
                logger.debug(f"Durable execution cleanup failed: {exc}")

        return len(to_remove)

    def _persist_event(self, execution: StreamingExecution, event: ToolEvent):
        if not self._execution_store:
            self._schedule_webhook(execution, event)
            return
        self._execution_store.append_event(
            execution_id=execution.execution_id,
            trace_id=execution.trace_id,
            session_id=execution.session_id,
            event_type=event.type.value,
            timestamp=event.timestamp,
            progress=event.progress,
            message=event.message,
            data=event.data,
            error=event.error,
        )
        self._persist_execution(execution, last_event=event)
        self._schedule_webhook(execution, event)

    def _persist_execution(self, execution: StreamingExecution, last_event: Optional[ToolEvent] = None):
        if not self._execution_store:
            return

        result = None
        error = execution.error if hasattr(execution, "error") else None
        last_message = None
        if last_event:
            last_message = last_event.message
            if last_event.type == EventType.COMPLETED:
                result = last_event.data
            if last_event.type == EventType.ERROR:
                error = last_event.error or last_event.message
            if last_event.type == EventType.CANCELLED and not error:
                error = "cancelled"

        self._execution_store.upsert_execution(
            execution_id=execution.execution_id,
            tool_name=execution.tool_name,
            client_id=execution.client_id,
            trace_id=execution.trace_id,
            session_id=execution.session_id,
            parent_execution_id=execution.parent_execution_id,
            status=execution.status,
            progress=execution.progress,
            queue_priority=execution.queue_priority,
            dedupe_key=execution.dedupe_key,
            queued_at=execution.queued_at,
            started_at=execution.started_at,
            completed_at=execution.completed_at,
            last_message=last_message,
            error=error,
            result=result,
            control_state=execution.control_state,
        )

    def _hydrate_execution(
        self,
        execution_id: str,
        row: Optional[Dict[str, Any]] = None,
    ) -> Optional[StreamingExecution]:
        if not self._execution_store:
            return None
        if row is None:
            row = self._execution_store.get_execution(execution_id)
        if not row:
            return None

        execution = StreamingExecution(
            execution_id=row["execution_id"],
            tool_name=row.get("tool_name", ""),
            client_id=row.get("client_id"),
            trace_id=row.get("trace_id"),
            session_id=row.get("session_id"),
            parent_execution_id=row.get("parent_execution_id"),
        )
        execution.status = row.get("status", "pending")
        execution.progress = float(row.get("progress") or 0.0)
        execution.queue_priority = int(row.get("queue_priority") or 0)
        execution.dedupe_key = row.get("dedupe_key")
        execution.queued_at = row.get("queued_at")
        execution.started_at = row.get("started_at")
        execution.completed_at = row.get("completed_at")
        execution.control_state = row.get("control_state", {}) or {}
        execution._event_persistor = self._persist_event

        try:
            persisted_events = self._execution_store.get_events(execution.execution_id)
            for persisted in persisted_events:
                event_type_value = persisted.get("event_type", EventType.LOG.value)
                try:
                    event_type = EventType(event_type_value)
                except Exception:
                    event_type = EventType.LOG
                execution.events.append(
                    ToolEvent(
                        type=event_type,
                        execution_id=execution.execution_id,
                        timestamp=float(persisted.get("timestamp") or time.time()),
                        progress=persisted.get("progress"),
                        message=persisted.get("message"),
                        data=persisted.get("data"),
                        error=persisted.get("error"),
                    )
                )
        except Exception as exc:
            logger.debug(f"Failed to hydrate events for {execution.execution_id}: {exc}")

        self._executions[execution.execution_id] = execution
        return execution

    def _normalize_callback_events(self, callback_events: Optional[List[str]]) -> List[str]:
        if not callback_events:
            return ["complete", "error"]
        normalized: List[str] = []
        for raw in callback_events:
            value = str(raw).strip().lower()
            if not value:
                continue
            if value in {"completed", "completion"}:
                value = "complete"
            if value not in {"progress", "complete", "error", "cancelled", "all"}:
                continue
            if value not in normalized:
                normalized.append(value)
        return normalized or ["complete", "error"]

    def _map_event_to_callback(self, event_type: EventType) -> Optional[str]:
        if event_type in {EventType.PROGRESS, EventType.PARTIAL}:
            return "progress"
        if event_type == EventType.COMPLETED:
            return "complete"
        if event_type == EventType.ERROR:
            return "error"
        if event_type == EventType.CANCELLED:
            return "cancelled"
        return None

    def _schedule_webhook(self, execution: StreamingExecution, event: ToolEvent):
        callback_url = str(execution.control_state.get("callback_url") or "").strip()
        if not callback_url:
            return

        callback_event = self._map_event_to_callback(event.type)
        if not callback_event:
            return

        subscribed_events = self._normalize_callback_events(execution.control_state.get("callback_events"))
        if "all" not in subscribed_events and callback_event not in subscribed_events:
            return

        now = time.time()
        if callback_event == "progress":
            progress_interval = float(os.getenv("JOB_WEBHOOK_PROGRESS_INTERVAL_SECONDS", "2.0"))
            last_progress_at = float(execution.control_state.get("last_progress_webhook_at") or 0.0)
            if (now - last_progress_at) < max(0.0, progress_interval):
                return
            execution.control_state["last_progress_webhook_at"] = now

        payload = {
            "event": callback_event,
            "event_type": event.type.value,
            "job_id": execution.execution_id,
            "call_id": execution.execution_id,
            "tool": execution.tool_name,
            "status": execution.status,
            "progress": execution.progress,
            "trace_id": execution.trace_id,
            "session_id": execution.session_id,
            "parent_execution_id": execution.parent_execution_id,
            "timestamp": event.timestamp,
            "message": event.message,
            "data": event.data,
            "error": event.error,
        }

        try:
            asyncio.get_running_loop().create_task(
                self._post_webhook(callback_url, payload, execution, callback_event)
            )
        except RuntimeError:
            # No running loop (e.g., synchronous shutdown path) - skip webhook delivery.
            return

    async def _post_webhook(
        self,
        callback_url: str,
        payload: Dict[str, Any],
        execution: StreamingExecution,
        callback_event: str,
    ):
        import httpx

        timeout_seconds = float(os.getenv("JOB_WEBHOOK_TIMEOUT_SECONDS", "5.0"))
        max_attempts = max(1, int(os.getenv("JOB_WEBHOOK_RETRY_ATTEMPTS", "2")))
        backoff_seconds = max(0.0, float(os.getenv("JOB_WEBHOOK_RETRY_BACKOFF_SECONDS", "1.0")))

        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                    response = await client.post(callback_url, json=payload)

                execution.control_state["last_webhook_event"] = callback_event
                execution.control_state["last_webhook_status_code"] = int(response.status_code)
                execution.control_state["last_webhook_at"] = time.time()
                execution.control_state["webhook_attempts"] = (
                    int(execution.control_state.get("webhook_attempts", 0)) + 1
                )
                if 200 <= response.status_code < 300:
                    execution.control_state["last_webhook_success"] = True
                    execution.control_state.pop("last_webhook_error", None)
                    self._persist_execution(execution)
                    return

                execution.control_state["last_webhook_success"] = False
                execution.control_state["last_webhook_error"] = f"HTTP {response.status_code}"
                self._persist_execution(execution)
            except Exception as exc:
                execution.control_state["last_webhook_event"] = callback_event
                execution.control_state["last_webhook_at"] = time.time()
                execution.control_state["last_webhook_success"] = False
                execution.control_state["last_webhook_error"] = str(exc)
                execution.control_state["webhook_attempts"] = (
                    int(execution.control_state.get("webhook_attempts", 0)) + 1
                )
                self._persist_execution(execution)

            if attempt < max_attempts and backoff_seconds > 0:
                await asyncio.sleep(backoff_seconds * attempt)

    def _normalize_priority(self, priority: int | str | None) -> int:
        if priority is None:
            return 50
        if isinstance(priority, int):
            return max(0, min(100, priority))
        p = str(priority).strip().lower()
        mapping = {
            "p0": 100,
            "critical": 100,
            "urgent": 90,
            "p1": 75,
            "high": 75,
            "p2": 50,
            "normal": 50,
            "medium": 50,
            "p3": 25,
            "background": 25,
            "low": 25,
        }
        return mapping.get(p, 50)

    def _build_dedupe_key(self, tool_name: str, inputs: Dict[str, Any]) -> str:
        stable_inputs = {
            k: v for k, v in inputs.items()
            if k not in {"_submitted_at", "_trace_id", "_session_id", "_parent_execution_id"}
        }
        payload = json.dumps(
            {"tool_name": tool_name, "arguments": stable_inputs},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _prune_dedupe_index(self, now: float, window_seconds: int):
        expired = [
            key for key, (_execution_id, created_at) in self._dedupe_index.items()
            if (now - created_at) > window_seconds
        ]
        for key in expired:
            self._dedupe_index.pop(key, None)


# Singleton instance
_executor: Optional[StreamingExecutor] = None


def get_streaming_executor(tool_executor: Callable = None) -> StreamingExecutor:
    """Get or create the global streaming executor."""
    global _executor
    if _executor is None:
        if tool_executor is None:
            raise ValueError("tool_executor required for first initialization")
        max_concurrent = int(os.getenv("STREAMING_MAX_CONCURRENT", "8"))
        _executor = StreamingExecutor(tool_executor, max_concurrent=max_concurrent)
    return _executor


def create_tool_executor_for_jobs():
    """
    Factory that returns an async callback suitable for StreamingExecutor.

    The callback resolves tool_name → backend via DuckDB tool store,
    sends an MCP tools/call to the backend with a 300s timeout,
    applies the output guard, and returns the MCP result.

    Signature: async (tool_name, inputs, progress_callback) -> result
    """
    import httpx as _httpx
    from uuid import uuid4 as _uuid4

    # Shared long-lived client — NO read timeout, connections stay open indefinitely
    _shared_client = _httpx.AsyncClient(
        timeout=_httpx.Timeout(None, connect=15.0),  # No read/write/pool timeout
        limits=_httpx.Limits(max_keepalive_connections=20, max_connections=100),
    )

    async def _tool_executor(
        tool_name: str,
        inputs: Dict[str, Any],
        progress_callback: Callable = None,
    ) -> Any:
        from ..persistence import get_tool_store
        from .circuit_breaker import get_backend_circuit_breaker

        # Pop internal meta-keys (no longer used for timeout, kept for compat)
        inputs.pop("_timeout", None)
        inputs.pop("_max_output_chars", None)
        inputs.pop("_max_links", None)

        # Report dispatch progress
        if progress_callback:
            await progress_callback(progress=10, message="resolving_backend")

        store = get_tool_store()
        backend_id, backend_url, resolved_name = store.resolve_tool(tool_name)
        tool_id = f"{backend_id}:{resolved_name}"
        breaker = get_backend_circuit_breaker()
        allowed, state = breaker.allow_request(backend_id)
        if not allowed:
            retry_after = state.get("retry_after_seconds")
            retry_suffix = f" retry_after={retry_after}s" if retry_after is not None else ""
            raise RuntimeError(
                f"CIRCUIT_OPEN: backend {backend_id} temporarily unavailable.{retry_suffix}"
            )

        if progress_callback:
            await progress_callback(progress=30, message="invoking_backend")

        started = time.time()
        try:
            response = await _shared_client.post(
                f"{backend_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": str(_uuid4()),
                    "method": "tools/call",
                    "params": {
                        "name": resolved_name,
                        "arguments": inputs,
                    },
                },
            )
        except Exception as exc:
            breaker.record_failure(backend_id, error=str(exc))
            store.record_call(
                tool_id,
                (time.time() - started) * 1000,
                False,
                execution_mode="job",
                trace_id=inputs.get("_trace_id"),
                session_id=inputs.get("_session_id"),
                error_code="TRANSPORT_ERROR",
                error_message=str(exc),
            )
            raise

        if progress_callback:
            await progress_callback(progress=80, message="processing_backend_response")

        if response.status_code != 200:
            breaker.record_failure(backend_id, error=f"http {response.status_code}")
            store.record_call(
                tool_id,
                (time.time() - started) * 1000,
                False,
                execution_mode="job",
                trace_id=inputs.get("_trace_id"),
                session_id=inputs.get("_session_id"),
                error_code=f"HTTP_{response.status_code}",
                error_message=response.text[:500],
            )
            raise RuntimeError(f"Backend {backend_id} returned HTTP {response.status_code}: {response.text[:300]}")

        data = response.json()
        if "error" in data and data["error"]:
            breaker.record_failure(backend_id, error=str(data["error"]))
            err_obj = data.get("error")
            err_code = err_obj.get("code") if isinstance(err_obj, dict) else None
            err_message = err_obj.get("message") if isinstance(err_obj, dict) else str(err_obj)
            store.record_call(
                tool_id,
                (time.time() - started) * 1000,
                False,
                execution_mode="job",
                trace_id=inputs.get("_trace_id"),
                session_id=inputs.get("_session_id"),
                error_code=err_code,
                error_message=err_message,
            )
            raise RuntimeError(f"MCP error from {backend_id}: {data['error']}")

        result = data.get("result")
        breaker.record_success(backend_id)
        store.record_call(
            tool_id,
            (time.time() - started) * 1000,
            True,
            execution_mode="job",
            trace_id=inputs.get("_trace_id"),
            session_id=inputs.get("_session_id"),
        )

        # Apply output guard if available
        try:
            from ..guards.output_guard import apply_output_guard
            if result is not None:
                result = apply_output_guard(
                    result,
                    tool_name=f"{backend_id}:{resolved_name}",
                    source_context={"backend": backend_id, "via": "job_executor"},
                )
        except ImportError:
            pass

        return result

    return _tool_executor
