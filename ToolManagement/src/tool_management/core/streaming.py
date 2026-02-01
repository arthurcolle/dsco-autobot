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
    LOG = "log"
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

    # State
    status: str = "pending"  # pending, running, completed, error, cancelled
    progress: float = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    # Events
    events: List[ToolEvent] = field(default_factory=list)

    # Subscribers
    _subscribers: List[asyncio.Queue] = field(default_factory=list)

    # Cancellation
    _cancelled: bool = False
    _cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

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

        for queue in self._subscribers:
            try:
                await queue.put(event)
            except Exception:
                pass

    def cancel(self):
        """Request cancellation."""
        self._cancelled = True
        self._cancel_event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled


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
    ):
        """
        Initialize executor.

        Args:
            tool_executor: Async function(tool_name, inputs, progress_callback) -> result
            heartbeat_interval: Seconds between heartbeat events
        """
        self.tool_executor = tool_executor
        self.heartbeat_interval = heartbeat_interval

        self._executions: Dict[str, StreamingExecution] = {}

    async def execute(
        self,
        tool_name: str,
        inputs: Dict[str, Any],
        client_id: str = None,
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
        execution = StreamingExecution(
            tool_name=tool_name,
            client_id=client_id,
        )

        self._executions[execution.execution_id] = execution

        # Start execution in background
        asyncio.create_task(self._run_execution(execution, inputs))

        return execution

    async def _run_execution(
        self,
        execution: StreamingExecution,
        inputs: Dict[str, Any],
    ):
        """Run the actual execution with event emission."""
        execution.status = "running"
        execution.started_at = time.time()

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
        return self._executions.get(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel an execution."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == "running":
            execution.cancel()
            return True
        return False

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
            while execution.status == "running" or not queue.empty():
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
        }

    def cleanup_old(self, max_age_seconds: int = 3600):
        """Remove old completed executions."""
        now = time.time()
        to_remove = []

        for ex_id, ex in self._executions.items():
            if ex.completed_at and (now - ex.completed_at) > max_age_seconds:
                to_remove.append(ex_id)

        for ex_id in to_remove:
            del self._executions[ex_id]

        return len(to_remove)


# Singleton instance
_executor: Optional[StreamingExecutor] = None


def get_streaming_executor(tool_executor: Callable = None) -> StreamingExecutor:
    """Get or create the global streaming executor."""
    global _executor
    if _executor is None:
        if tool_executor is None:
            raise ValueError("tool_executor required for first initialization")
        _executor = StreamingExecutor(tool_executor)
    return _executor
