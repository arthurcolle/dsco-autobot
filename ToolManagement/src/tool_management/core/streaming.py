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
import ipaddress
import os
import socket
import uuid
import time
import json
import logging
import math
import inspect
from urllib.parse import urlsplit, urlunsplit
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class BackendExecutionError(RuntimeError):
    """A terminal backend failure that must not replay an accepted call."""

    def __init__(
        self,
        message: str,
        *,
        backend: str,
        code: str = "TOOL_EXECUTION_FAILED",
        category: str = "provider",
        cause: Any = None,
    ):
        super().__init__(message)
        self.error = {
            "code": code,
            "message": message,
            "category": category,
            "retryable": False,
            "backend": backend,
        }
        if cause is not None:
            self.error["cause"] = cause


def _require_successful_tool_result(result: Any, backend: str) -> Any:
    """MCP isError is a definitive tool failure, not transport uncertainty."""
    if isinstance(result, dict) and result.get("isError") is True:
        message = "Backend returned a tool error"
        content = result.get("content")
        for block in content if isinstance(content, list) else []:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                message = block["text"]
                break
        raise BackendExecutionError(message, backend=backend, cause=result)
    return result


def validate_callback_url(url: str) -> str:
    """Validate a public HTTPS webhook target before it enters durable state.

    Every resolved address must be globally routable. This rejects loopback,
    private, link-local, reserved, multicast, and unspecified targets instead
    of letting callback delivery become an SSRF path.
    """
    candidate = str(url or "").strip()
    if not candidate or len(candidate) > 2048:
        raise ValueError("callback URL is missing or too long")
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("callback URL is malformed") from exc
    if parsed.scheme.lower() != "https":
        raise ValueError("callback URL must use https")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise ValueError("callback URL must have a host and no embedded credentials")
    if parsed.fragment:
        raise ValueError("callback URL fragments are not supported")

    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == "localhost" or hostname.endswith((".localhost", ".local")):
        raise ValueError("callback URL host is not public")

    addresses = set()
    try:
        addresses.add(ipaddress.ip_address(hostname))
    except ValueError:
        try:
            for family, _socktype, _proto, _canonname, sockaddr in socket.getaddrinfo(
                hostname, port or 443, type=socket.SOCK_STREAM
            ):
                if family in (socket.AF_INET, socket.AF_INET6):
                    addresses.add(ipaddress.ip_address(sockaddr[0]))
        except OSError as exc:
            raise ValueError("callback URL host could not be resolved") from exc

    if not addresses or any(not address.is_global for address in addresses):
        raise ValueError("callback URL host is not public")

    # urlsplit lower-cases hostname access but retains the caller's original
    # netloc and path. Return a normalized scheme with the validated target.
    return urlunsplit(("https", parsed.netloc, parsed.path or "/", parsed.query, ""))


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
    event_id: Optional[int] = None
    replayed: bool = False

    def to_sse(self) -> str:
        """Format as Server-Sent Event."""
        event_data = {
            "type": self.type.value,
            "execution_id": self.execution_id,
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc).isoformat(),
        }
        if self.event_id is not None:
            event_data["event_id"] = int(self.event_id)
        if self.replayed:
            event_data["replayed"] = True

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
            "event_id": self.event_id,
            "replayed": self.replayed,
        }


@dataclass
class StreamingExecution:
    """State for a streaming execution."""
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tool_name: str = ""
    client_id: Optional[str] = None
    tenant_id: Optional[str] = None
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
    _execution_identity: Dict[str, Any] = field(default_factory=dict, repr=False)
    _event_persistor: Optional[Callable[["StreamingExecution", ToolEvent], None]] = None
    _backend_canceller: Optional[Callable[[], Awaitable[None]]] = None

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
        self.control_state["cancel_requested_at"] = time.time()
        if self._backend_canceller:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._backend_canceller())
            except RuntimeError:
                # Best-effort only when no event loop is active.
                pass
        if self._task and not self._task.done():
            self._task.cancel()

    def set_backend_canceller(self, canceller: Optional[Callable[[], Awaitable[None]]]):
        self._backend_canceller = canceller

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
        self._fanout_default_budget = max(0, int(os.getenv("STREAMING_FANOUT_DEFAULT_BUDGET", "0")))
        self._fanout_max_active_per_trace = max(0, int(os.getenv("STREAMING_FANOUT_MAX_ACTIVE_PER_TRACE", "256")))
        self._fanout_max_active_per_session = max(0, int(os.getenv("STREAMING_FANOUT_MAX_ACTIVE_PER_SESSION", "512")))
        self._fanout_max_active_per_parent = max(0, int(os.getenv("STREAMING_FANOUT_MAX_ACTIVE_PER_PARENT", "256")))
        self._max_pending = max(0, int(os.getenv("STREAMING_MAX_PENDING", "4096")))
        self._max_inflight = max(0, int(os.getenv("STREAMING_MAX_INFLIGHT", "8192")))

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
        tenant_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        session_id: Optional[str] = None,
        parent_execution_id: Optional[str] = None,
        priority: int | str | None = None,
        dedupe_window_seconds: int = 0,
        dedupe_key: Optional[str] = None,
        convergence_key: Optional[str] = None,
        fanout_budget: Optional[int] = None,
        group_reducer: Optional[str] = None,
        group_reducer_config: Optional[Dict[str, Any]] = None,
        callback_url: Optional[str] = None,
        callback_events: Optional[List[str]] = None,
        execution_identity: Optional[Dict[str, Any]] = None,
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
        identity_snapshot: Dict[str, Any] = {}
        if execution_identity is not None:
            from .delegated_execution import validated_identity_snapshot

            identity_snapshot = validated_identity_snapshot(execution_identity)
            identity_tenant = identity_snapshot.get("tenant_id")
            if tenant_id is not None and tenant_id != identity_tenant:
                raise ValueError("Execution tenant does not match its authenticated identity")
            tenant_id = identity_tenant
        now = time.time()
        resolved_trace_id = trace_id or (inputs.get("_trace_id") if isinstance(inputs, dict) else None)
        resolved_session_id = session_id or (inputs.get("_session_id") if isinstance(inputs, dict) else None)
        resolved_parent_execution_id = parent_execution_id or (
            inputs.get("_parent_execution_id") if isinstance(inputs, dict) else None
        )
        normalized_convergence_key = self._normalize_convergence_key(
            convergence_key
            if convergence_key is not None
            else (inputs.get("_convergence_key") if isinstance(inputs, dict) else None)
        )
        normalized_group_reducer = str(
            group_reducer
            if group_reducer is not None
            else (inputs.get("_group_reducer") if isinstance(inputs, dict) else "append_unique")
        ).strip().lower() or "append_unique"
        resolved_group_reducer_config = (
            dict(group_reducer_config)
            if isinstance(group_reducer_config, dict)
            else (
                dict(inputs.get("_group_reducer_config"))
                if isinstance(inputs, dict) and isinstance(inputs.get("_group_reducer_config"), dict)
                else {}
            )
        )
        effective_fanout_budget = self._resolve_fanout_budget(
            fanout_budget
            if fanout_budget is not None
            else (inputs.get("_fanout_budget") if isinstance(inputs, dict) else None)
        )
        effective_dedupe_key = dedupe_key or self._build_dedupe_key(tool_name, inputs)
        memory_dedupe_key = f"{tenant_id or ''}\0{effective_dedupe_key}"
        dedupe_window_seconds = max(0, int(dedupe_window_seconds))
        if dedupe_window_seconds > 0:
            self._prune_dedupe_index(now, dedupe_window_seconds)
            existing = self._dedupe_index.get(memory_dedupe_key)
            if existing:
                existing_id, created_at = existing
                existing_execution = self._executions.get(existing_id)
                if existing_execution and (now - created_at) <= dedupe_window_seconds:
                    if existing_execution.status not in {"error", "cancelled"}:
                        self._mark_execution_reused(existing_execution, reason="dedupe", timestamp=now)
                        return existing_execution
            if self._execution_store:
                stored = self._execution_store.find_recent_by_dedupe_key(
                    effective_dedupe_key,
                    now - dedupe_window_seconds,
                    tenant_id=tenant_id,
                )
                if stored:
                    existing_id = stored.get("execution_id")
                    if existing_id:
                        existing_execution = self._executions.get(existing_id) or self._hydrate_execution(
                            existing_id,
                            tenant_id=tenant_id,
                        )
                        if existing_execution and existing_execution.status not in {"error", "cancelled"}:
                            self._mark_execution_reused(existing_execution, reason="dedupe", timestamp=now)
                            return existing_execution

        if normalized_convergence_key:
            converged = self._find_convergence_execution(
                tool_name=tool_name,
                convergence_key=normalized_convergence_key,
                tenant_id=tenant_id,
            )
            if converged:
                self._mark_execution_reused(converged, reason="coalesced", timestamp=now)
                return converged

        self._enforce_global_fanout_limits(
            trace_id=resolved_trace_id,
            session_id=resolved_session_id,
            parent_execution_id=resolved_parent_execution_id,
        )

        fanout_scope_key = self._derive_fanout_scope_key(
            tool_name=tool_name,
            trace_id=resolved_trace_id,
            session_id=resolved_session_id,
            parent_execution_id=resolved_parent_execution_id,
            convergence_key=normalized_convergence_key,
        )
        if effective_fanout_budget > 0:
            active_in_scope = self._count_active_executions_for_scope(fanout_scope_key)
            if active_in_scope >= effective_fanout_budget:
                if normalized_convergence_key:
                    converged = self._find_convergence_execution(
                        tool_name=tool_name,
                        convergence_key=normalized_convergence_key,
                        tenant_id=tenant_id,
                    )
                    if converged:
                        self._mark_execution_reused(converged, reason="coalesced", timestamp=now)
                        return converged
                raise RuntimeError(
                    "FANOUT_LIMIT_EXCEEDED:"
                    f"scope={fanout_scope_key},active={active_in_scope},budget={effective_fanout_budget}"
                )

        queue_depth = len(self._pending_queue)
        inflight = self._active_count + queue_depth
        if self._max_pending > 0 and queue_depth >= self._max_pending:
            raise RuntimeError(
                "QUEUE_BACKPRESSURE:"
                f"queue_depth={queue_depth},max_pending={self._max_pending}"
            )
        if self._max_inflight > 0 and inflight >= self._max_inflight:
            raise RuntimeError(
                "INFLIGHT_LIMIT_EXCEEDED:"
                f"inflight={inflight},max_inflight={self._max_inflight}"
            )

        execution = StreamingExecution(
            tool_name=tool_name,
            client_id=client_id,
            tenant_id=tenant_id,
            trace_id=resolved_trace_id,
            session_id=resolved_session_id,
            parent_execution_id=resolved_parent_execution_id,
            status="queued",
            queued_at=now,
            queue_priority=normalized_priority,
            dedupe_key=effective_dedupe_key,
            _execution_identity=identity_snapshot,
        )
        execution.control_state["submit_count"] = 1
        if execution.trace_id:
            execution.control_state["trace_id"] = execution.trace_id
        if execution.session_id:
            execution.control_state["session_id"] = execution.session_id
        if execution.parent_execution_id:
            execution.control_state["parent_execution_id"] = execution.parent_execution_id
        execution.control_state["fanout_scope_key"] = fanout_scope_key
        if effective_fanout_budget > 0:
            execution.control_state["fanout_budget"] = effective_fanout_budget
        if normalized_convergence_key:
            execution.control_state["convergence_key"] = normalized_convergence_key
            execution.control_state["group_reducer"] = normalized_group_reducer
            if resolved_group_reducer_config:
                execution.control_state["group_reducer_config"] = resolved_group_reducer_config
        if callback_url:
            execution.control_state["callback_url"] = callback_url
            execution.control_state["callback_events"] = self._normalize_callback_events(callback_events)
        execution._event_persistor = self._persist_event

        self._executions[execution.execution_id] = execution
        if dedupe_window_seconds > 0:
            self._dedupe_index[memory_dedupe_key] = (execution.execution_id, now)
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
            stage: Optional[str] = None,
            stages_completed: Optional[List[str]] = None,
            stages_remaining: Optional[List[str]] = None,
            estimated_completion_seconds: Optional[int] = None,
            metadata: Optional[Dict[str, Any]] = None,
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
            elif progress is not None or message or stage or stages_completed or stages_remaining:
                progress_data: Dict[str, Any] = {}
                if stage:
                    progress_data["stage"] = stage
                if stages_completed:
                    progress_data["stages_completed"] = list(stages_completed)
                if stages_remaining:
                    progress_data["stages_remaining"] = list(stages_remaining)
                if estimated_completion_seconds is not None:
                    progress_data["estimated_completion_seconds"] = int(estimated_completion_seconds)
                if metadata:
                    progress_data["metadata"] = metadata
                await execution.emit(ToolEvent(
                    type=EventType.PROGRESS,
                    execution_id=execution.execution_id,
                    progress=progress,
                    message=message,
                    data=progress_data or None,
                ))

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(execution)
        )

        try:
            # Execute with progress callback
            executor_kwargs = {
                "progress_callback": progress_callback,
                "cancel_event": execution._cancel_event,
                "execution_id": execution.execution_id,
                "tenant_id": execution.tenant_id,
                "execution_identity": execution._execution_identity,
            }
            # Negotiate legacy callable signatures before invocation. Catching
            # a body TypeError and invoking again can duplicate external writes.
            try:
                parameters = inspect.signature(self.tool_executor).parameters
            except (TypeError, ValueError):
                parameters = None
            if parameters is not None and not any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            ):
                identity_parameter = parameters.get("execution_identity")
                if execution._execution_identity and (
                    identity_parameter is None or identity_parameter.kind not in {
                        inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY,
                    }
                ):
                    raise RuntimeError("Executor does not support authenticated execution identity")
                executor_kwargs = {
                    key: value for key, value in executor_kwargs.items()
                    if key in parameters and parameters[key].kind in {
                        inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY,
                    }
                }
            result = await self.tool_executor(execution.tool_name, inputs, **executor_kwargs)

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
            requested_at = float(execution.control_state.get("cancel_requested_at") or 0.0)
            if requested_at > 0:
                execution.control_state["cancel_ack_ms"] = round(
                    max(0.0, (execution.completed_at - requested_at) * 1000.0),
                    2,
                )

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
                data=getattr(e, "error", None),
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

    def get_execution(
        self,
        execution_id: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[StreamingExecution]:
        """Get an execution by ID."""
        execution = self._executions.get(execution_id)
        if execution:
            if tenant_id is not None and execution.tenant_id != tenant_id:
                return None
            return execution
        return self._hydrate_execution(execution_id, tenant_id=tenant_id)

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
        if "_execution_identity" in values:
            raise ValueError("Execution identity cannot be modified through control state")
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
        since_event_id: Optional[int] = None,
        since_timestamp: Optional[float] = None,
    ) -> AsyncIterator[ToolEvent]:
        """
        Stream events for an execution.

        Args:
            execution_id: Execution to stream
            include_history: Include events that already occurred
            since_event_id: Resume cursor (exclusive) for durable history replay
            since_timestamp: Optional timestamp filter for replay history

        Yields:
            ToolEvent objects
        """
        execution = self._executions.get(execution_id) or self._hydrate_execution(execution_id)
        if not execution:
            return

        # Subscribe to new events
        queue = execution.subscribe()

        try:
            # Send historical events
            if include_history:
                replayed_event_ids: set[int] = set()
                if self._execution_store:
                    persisted = self._execution_store.get_events(
                        execution_id,
                        limit=10000,
                        since_event_id=since_event_id,
                        since_timestamp=since_timestamp,
                    )
                    for item in persisted:
                        event_type_value = item.get("event_type", EventType.LOG.value)
                        try:
                            event_type = EventType(event_type_value)
                        except Exception:
                            event_type = EventType.LOG
                        persisted_event = ToolEvent(
                            type=event_type,
                            execution_id=execution_id,
                            timestamp=float(item.get("timestamp") or time.time()),
                            progress=item.get("progress"),
                            message=item.get("message"),
                            data=item.get("data"),
                            error=item.get("error"),
                            event_id=int(item.get("id") or 0) or None,
                            replayed=True,
                        )
                        if persisted_event.event_id is not None:
                            replayed_event_ids.add(int(persisted_event.event_id))
                        yield persisted_event

                for event in execution.events:
                    if since_event_id is not None and event.event_id is not None and event.event_id <= since_event_id:
                        continue
                    if since_timestamp is not None and float(event.timestamp) < float(since_timestamp):
                        continue
                    if event.event_id is not None and int(event.event_id) in replayed_event_ids:
                        continue
                    yield event

            # Stream new events
            while execution.status in {"pending", "queued", "running"} or not queue.empty():
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    if since_event_id is not None and event.event_id is not None and event.event_id <= since_event_id:
                        continue
                    if since_timestamp is not None and float(event.timestamp) < float(since_timestamp):
                        continue
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
        since_event_id: Optional[int] = None,
        since_timestamp: Optional[float] = None,
    ) -> AsyncIterator[str]:
        """
        Stream events as SSE format.

        Args:
            execution_id: Execution to stream
            include_history: Include historical events
            since_event_id: Resume cursor (exclusive) for durable replay
            since_timestamp: Timestamp-based replay cursor

        Yields:
            SSE-formatted strings
        """
        async for event in self.stream_events(
            execution_id,
            include_history,
            since_event_id=since_event_id,
            since_timestamp=since_timestamp,
        ):
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
            "max_pending": self._max_pending,
            "max_inflight": self._max_inflight,
            "durable_store_enabled": bool(self._execution_store),
        }

    def list_executions(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        tenant_id: Optional[str] = None,
    ) -> List[StreamingExecution]:
        """List executions from memory plus durable storage."""
        merged: Dict[str, StreamingExecution] = {}
        for execution in self._executions.values():
            if status and execution.status != status:
                continue
            if tenant_id is not None and execution.tenant_id != tenant_id:
                continue
            merged[execution.execution_id] = execution

        if self._execution_store:
            try:
                rows = self._execution_store.list_executions(
                    status=status,
                    limit=max(limit, 200),
                    tenant_id=tenant_id,
                )
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
                group_ttl = int(os.getenv("EXECUTION_GROUP_TTL_SECONDS", "0"))
                if group_ttl > 0:
                    self._execution_store.cleanup_old_execution_group_states(max_age_seconds=group_ttl)
            except Exception as exc:
                logger.debug(f"Durable execution cleanup failed: {exc}")

        return len(to_remove)

    def _persist_event(self, execution: StreamingExecution, event: ToolEvent):
        if not self._execution_store:
            self._schedule_webhook(execution, event)
            return
        event_id = self._execution_store.append_event(
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
        if event_id:
            event.event_id = int(event_id)
        if event.type == EventType.PARTIAL:
            self._persist_execution_group_chunk(execution, event)
        self._persist_execution(execution, last_event=event)
        self._schedule_webhook(execution, event)

    def _persist_execution_group_chunk(self, execution: StreamingExecution, event: ToolEvent):
        if not self._execution_store:
            return
        convergence_key = self._normalize_convergence_key(execution.control_state.get("convergence_key"))
        if not convergence_key:
            return
        reducer = str(execution.control_state.get("group_reducer") or "append_unique").strip().lower()
        reducer_config = (
            execution.control_state.get("group_reducer_config")
            if isinstance(execution.control_state.get("group_reducer_config"), dict)
            else {}
        )
        max_items = max(1, int(os.getenv("STREAMING_GROUP_MAX_ITEMS", "2000")))
        try:
            snapshot = self._execution_store.upsert_execution_group_chunk(
                convergence_key=convergence_key,
                tool_name=execution.tool_name,
                chunk=event.data,
                execution_id=execution.execution_id,
                trace_id=execution.trace_id,
                session_id=execution.session_id,
                reducer=reducer,
                max_items=max_items,
                reducer_config=reducer_config,
            )
            execution.control_state["group_convergence_key"] = convergence_key
            execution.control_state["group_reducer"] = reducer
            if reducer_config:
                execution.control_state["group_reducer_config"] = reducer_config
            execution.control_state["group_chunk_count"] = int(snapshot.get("chunk_count") or 0)
            state = snapshot.get("state") if isinstance(snapshot.get("state"), dict) else {}
            items = state.get("items") if isinstance(state.get("items"), list) else []
            execution.control_state["group_unique_items"] = len(items)
        except Exception as exc:
            logger.debug(f"Failed to persist execution group chunk ({execution.execution_id}): {exc}")

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
            tenant_id=execution.tenant_id,
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
            control_state={**execution.control_state, "_execution_identity": execution._execution_identity},
        )

    def _hydrate_execution(
        self,
        execution_id: str,
        row: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
    ) -> Optional[StreamingExecution]:
        if not self._execution_store:
            return None
        if row is None:
            row = self._execution_store.get_execution(execution_id, tenant_id=tenant_id)
        if not row:
            return None
        if tenant_id is not None and row.get("tenant_id") != tenant_id:
            return None

        execution = StreamingExecution(
            execution_id=row["execution_id"],
            tool_name=row.get("tool_name", ""),
            client_id=row.get("client_id"),
            tenant_id=row.get("tenant_id"),
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
        execution.control_state = dict(row.get("control_state", {}) or {})
        persisted_identity = execution.control_state.pop("_execution_identity", None)
        if persisted_identity:
            from .delegated_execution import validated_identity_snapshot

            execution._execution_identity = validated_identity_snapshot(persisted_identity)
            if execution._execution_identity.get("tenant_id") != execution.tenant_id:
                raise ValueError("Persisted execution tenant does not match its authenticated identity")
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
                        event_id=int(persisted.get("id") or 0) or None,
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

    def _normalize_convergence_key(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _resolve_fanout_budget(self, value: Any) -> int:
        if value is None:
            return self._fanout_default_budget
        try:
            return max(0, int(value))
        except Exception:
            return self._fanout_default_budget

    def _derive_fanout_scope_key(
        self,
        *,
        tool_name: str,
        trace_id: Optional[str],
        session_id: Optional[str],
        parent_execution_id: Optional[str],
        convergence_key: Optional[str],
    ) -> str:
        if convergence_key:
            return f"tool:{tool_name}|convergence:{convergence_key}"
        if parent_execution_id:
            return f"tool:{tool_name}|parent:{parent_execution_id}"
        if trace_id:
            return f"tool:{tool_name}|trace:{trace_id}"
        if session_id:
            return f"tool:{tool_name}|session:{session_id}"
        return f"tool:{tool_name}|global"

    def _execution_scope_key(self, execution: StreamingExecution) -> str:
        stored = execution.control_state.get("fanout_scope_key")
        if isinstance(stored, str) and stored.strip():
            return stored
        convergence_key = self._normalize_convergence_key(execution.control_state.get("convergence_key"))
        return self._derive_fanout_scope_key(
            tool_name=execution.tool_name,
            trace_id=execution.trace_id,
            session_id=execution.session_id,
            parent_execution_id=execution.parent_execution_id,
            convergence_key=convergence_key,
        )

    def _count_active_where(self, predicate: Callable[[StreamingExecution], bool]) -> int:
        active_statuses = {"pending", "queued", "running"}
        count = 0
        for execution in self._executions.values():
            if execution.status not in active_statuses:
                continue
            if predicate(execution):
                count += 1
        return count

    def _count_active_executions_for_scope(self, scope_key: str) -> int:
        return self._count_active_where(
            lambda execution: self._execution_scope_key(execution) == scope_key
        )

    def _enforce_global_fanout_limits(
        self,
        *,
        trace_id: Optional[str],
        session_id: Optional[str],
        parent_execution_id: Optional[str],
    ):
        if trace_id and self._fanout_max_active_per_trace > 0:
            active_trace = self._count_active_where(lambda execution: execution.trace_id == trace_id)
            if active_trace >= self._fanout_max_active_per_trace:
                raise RuntimeError(
                    "FANOUT_LIMIT_EXCEEDED:"
                    f"trace={trace_id},active={active_trace},budget={self._fanout_max_active_per_trace}"
                )

        if session_id and self._fanout_max_active_per_session > 0:
            active_session = self._count_active_where(lambda execution: execution.session_id == session_id)
            if active_session >= self._fanout_max_active_per_session:
                raise RuntimeError(
                    "FANOUT_LIMIT_EXCEEDED:"
                    f"session={session_id},active={active_session},budget={self._fanout_max_active_per_session}"
                )

        if parent_execution_id and self._fanout_max_active_per_parent > 0:
            active_parent = self._count_active_where(
                lambda execution: execution.parent_execution_id == parent_execution_id
            )
            if active_parent >= self._fanout_max_active_per_parent:
                raise RuntimeError(
                    "FANOUT_LIMIT_EXCEEDED:"
                    f"parent={parent_execution_id},active={active_parent},budget={self._fanout_max_active_per_parent}"
                )

    def _find_convergence_execution(
        self,
        *,
        tool_name: str,
        convergence_key: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[StreamingExecution]:
        normalized = self._normalize_convergence_key(convergence_key)
        if not normalized:
            return None

        active_statuses = {"pending", "queued", "running"}
        for execution in self._executions.values():
            if execution.status not in active_statuses:
                continue
            if execution.tool_name != tool_name:
                continue
            if execution.tenant_id != tenant_id:
                continue
            if self._normalize_convergence_key(execution.control_state.get("convergence_key")) == normalized:
                return execution
        return None

    def _mark_execution_reused(self, execution: StreamingExecution, *, reason: str, timestamp: Optional[float] = None):
        ts = float(timestamp if timestamp is not None else time.time())
        execution.control_state["submit_count"] = int(execution.control_state.get("submit_count", 1)) + 1
        execution.control_state["last_reused_at"] = ts
        execution.control_state["last_reuse_reason"] = reason
        if reason == "dedupe":
            execution.control_state["deduplicated_requests"] = (
                int(execution.control_state.get("deduplicated_requests", 0)) + 1
            )
            execution.control_state["last_dedupe_reuse_at"] = ts
        elif reason == "coalesced":
            execution.control_state["coalesced_requests"] = (
                int(execution.control_state.get("coalesced_requests", 0)) + 1
            )
            execution.control_state["last_coalesced_reuse_at"] = ts
        self._persist_execution(execution)

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
    from ..api.tools_api import _backend_headers

    _execution_timeout_seconds = float(os.getenv("BACKEND_EXECUTION_TIMEOUT_SECONDS", "300"))
    if not math.isfinite(_execution_timeout_seconds) or _execution_timeout_seconds <= 0:
        raise ValueError("BACKEND_EXECUTION_TIMEOUT_SECONDS must be finite and positive")
    _shared_client = _httpx.AsyncClient(
        timeout=_httpx.Timeout(_execution_timeout_seconds, connect=min(15.0, _execution_timeout_seconds)),
        limits=_httpx.Limits(max_keepalive_connections=50, max_connections=200),
    )

    _warm_backend_until: Dict[str, float] = {}
    _backend_latency_ms: Dict[str, float] = {}
    _backend_failures: Dict[str, int] = {}
    _warm_ttl_seconds = float(os.getenv("BACKEND_WARM_TTL_SECONDS", "180"))
    _health_probe_timeout = float(os.getenv("BACKEND_HEALTH_PROBE_TIMEOUT_SECONDS", "2.5"))
    _stream_backend_default = os.getenv("ENABLE_BACKEND_STREAMING_PATH", "1") == "1"
    # Max seconds to wait between two SSE frames from a backend stream before
    # we treat the stream as stalled. An accepted stream must not be replayed
    # through the non-streaming path, since tools can perform external writes.
    # Must exceed a backend's worst-case time-to-first-chunk (search backends
    # run the full handler before streaming, ~20s), so default generously.
    _stream_idle_timeout = float(os.getenv("BACKEND_STREAM_IDLE_TIMEOUT_SECONDS", "45"))

    def _load_replica_map() -> Dict[str, List[str]]:
        raw = os.getenv("BACKEND_REPLICA_URLS_JSON", "").strip()
        if not raw:
            return {}
        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                return {}
            out: Dict[str, List[str]] = {}
            for key, value in data.items():
                if isinstance(value, list):
                    out[str(key)] = [str(v).rstrip("/") for v in value if str(v).strip()]
                elif isinstance(value, str):
                    out[str(key)] = [str(value).rstrip("/")]
            return out
        except Exception:
            return {}

    _replica_map = _load_replica_map()

    def _candidate_urls(backend_id: str, primary_url: str) -> List[str]:
        urls: List[str] = []
        primary = str(primary_url).rstrip("/")
        if primary:
            urls.append(primary)

        env_key = f"BACKEND_REPLICA_URLS_{backend_id.upper().replace('-', '_')}"
        env_urls = os.getenv(env_key, "").strip()
        if env_urls:
            urls.extend([u.strip().rstrip("/") for u in env_urls.split(",") if u.strip()])

        urls.extend(_replica_map.get(backend_id, []))
        deduped: List[str] = []
        for url in urls:
            if url and url not in deduped:
                deduped.append(url)
        return deduped

    async def _probe_backend(
        url: str,
        backend_id: str,
        tenant_id: Optional[str] = None,
    ) -> Optional[float]:
        started = time.time()
        try:
            response = await _shared_client.get(
                f"{url}/health",
                timeout=_health_probe_timeout,
                headers=_backend_headers(backend_id, tenant_id),
            )
            if response.status_code == 200:
                latency_ms = max(1.0, (time.time() - started) * 1000.0)
                _warm_backend_until[url] = time.time() + _warm_ttl_seconds
                _backend_latency_ms[url] = latency_ms
                _backend_failures[url] = 0
                return latency_ms
        except Exception:
            pass
        _backend_failures[url] = int(_backend_failures.get(url, 0)) + 1
        return None

    async def _rank_backend_urls(
        backend_id: str,
        primary_url: str,
        tenant_id: Optional[str] = None,
    ) -> List[str]:
        now = time.time()
        urls = _candidate_urls(backend_id, primary_url)
        scored: List[tuple[int, float, str]] = []
        for url in urls:
            if now > float(_warm_backend_until.get(url, 0.0)):
                await _probe_backend(url, backend_id, tenant_id)
            failures = int(_backend_failures.get(url, 0))
            latency = float(_backend_latency_ms.get(url, 999999.0))
            scored.append((failures, latency, url))
        scored.sort(key=lambda item: (item[0], item[1]))
        return [url for _, _, url in scored]

    async def _emit_stage(
        progress_callback: Optional[Callable[..., Awaitable[None]]],
        *,
        progress: float,
        stage: str,
        stages_completed: List[str],
        stages_remaining: List[str],
        eta_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if not progress_callback:
            return
        await progress_callback(
            progress=progress,
            message=stage,
            stage=stage,
            stages_completed=stages_completed,
            stages_remaining=stages_remaining,
            estimated_completion_seconds=eta_seconds,
            metadata=metadata,
        )

    def _normalize_progress(value: Any) -> Optional[float]:
        if not isinstance(value, (int, float)):
            return None
        progress = float(value)
        if progress <= 1.0:
            progress *= 100.0
        return max(0.0, min(100.0, progress))

    async def _stream_backend_execute(
        *,
        base_url: str,
        backend_id: str,
        tenant_id: Optional[str],
        resolved_name: str,
        inputs: Dict[str, Any],
        progress_callback: Optional[Callable[..., Awaitable[None]]],
        cancel_event: Optional[asyncio.Event],
        execution_id: Optional[str],
        timeout_seconds: float,
    ) -> Optional[Any]:
        stream_url = f"{base_url.rstrip('/')}/execute/stream"
        headers = {
            **_backend_headers(backend_id, tenant_id),
            "Accept": "text/event-stream",
        }
        call_id: Optional[str] = None
        chunks: List[Any] = []
        done_payload: Optional[Dict[str, Any]] = None

        async with _shared_client.stream(
            "POST",
            stream_url,
            json={"name": resolved_name, "arguments": inputs},
            headers=headers,
            timeout=_httpx.Timeout(timeout_seconds, connect=min(15.0, timeout_seconds)),
        ) as response:
            content_type = str(response.headers.get("content-type", "")).lower()
            if response.status_code in {404, 405}:
                return None
            if response.status_code >= 400:
                text = (await response.aread()).decode(errors="ignore")
                raise RuntimeError(f"Streaming endpoint failed ({response.status_code}): {text[:300]}")
            if "text/event-stream" not in content_type:
                # HTTP 200 can mean that the tool already ran. A malformed
                # stream response is not proof that replay is safe.
                raise BackendExecutionError(
                    "Backend accepted a stream request but returned a non-SSE response",
                    backend=backend_id,
                    code="BACKEND_STREAM_UNCERTAIN",
                    category="transport",
                    cause={"reason": "unexpected_content_type", "content_type": content_type},
                )

            await _emit_stage(
                progress_callback,
                progress=45.0,
                stage="streaming_results",
                stages_completed=["resolve_backend", "route_backend", "invoke_backend"],
                stages_remaining=["process_response", "completed"],
                eta_seconds=10,
                metadata={"stream_url": stream_url},
            )

            current_event = "message"
            data_lines: List[str] = []
            # Bound the wait between SSE frames. A backend that emits `meta`
            # then stalls (or a proxy that drops the long-lived connection
            # without a terminal `done`/`error` frame) must NOT wedge the job
            # forever at `stream_meta_received`. Without a terminal frame the
            # outcome is unknown; cancellation is best-effort and does not make
            # replay safe.
            _line_iter = response.aiter_lines().__aiter__()
            while True:
                try:
                    line = await asyncio.wait_for(
                        _line_iter.__anext__(),
                        timeout=_stream_idle_timeout,
                    )
                except StopAsyncIteration:
                    break
                except _httpx.TransportError as exc:
                    raise BackendExecutionError(
                        "Backend stream connection failed before a terminal result",
                        backend=backend_id,
                        code="BACKEND_STREAM_UNCERTAIN",
                        category="transport",
                        cause={"reason": type(exc).__name__, "backend_call_id": call_id},
                    ) from exc
                except asyncio.TimeoutError:
                    logger.warning(
                        "Backend stream idle >%.1fs (tool=%s call_id=%s); "
                        "execution outcome is uncertain",
                        _stream_idle_timeout,
                        resolved_name,
                        call_id,
                    )
                    if call_id:
                        try:
                            await _shared_client.post(
                                f"{base_url.rstrip('/')}/api/v1/calls/{call_id}/control",
                                json={"action": "stop"},
                                headers=_backend_headers(backend_id, tenant_id),
                                timeout=2.0,
                            )
                        except Exception:
                            pass
                    raise BackendExecutionError(
                        "Backend stream stalled before a terminal result",
                        backend=backend_id,
                        code="BACKEND_STREAM_UNCERTAIN",
                        category="transport",
                        cause={"reason": "idle_timeout", "backend_call_id": call_id},
                    )

                if cancel_event and cancel_event.is_set():
                    if call_id:
                        try:
                            await _shared_client.post(
                                f"{base_url.rstrip('/')}/api/v1/calls/{call_id}/control",
                                json={"action": "stop"},
                                headers=_backend_headers(backend_id, tenant_id),
                                timeout=2.0,
                            )
                        except Exception:
                            pass
                    raise asyncio.CancelledError("Execution cancelled during backend stream")

                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip() or "message"
                    continue
                if line.startswith("data:"):
                    data_lines.append(line.split(":", 1)[1].lstrip())
                    continue
                if line.strip():
                    continue

                if not data_lines:
                    current_event = "message"
                    continue

                raw_payload = "\n".join(data_lines)
                data_lines = []
                try:
                    payload = json.loads(raw_payload)
                except Exception:
                    payload = {"raw": raw_payload}

                if current_event == "meta":
                    if isinstance(payload, dict):
                        call_id = payload.get("call_id") or call_id
                        if progress_callback:
                            await progress_callback(
                                progress=50.0,
                                message="stream_meta_received",
                                stage="streaming_results",
                                metadata={
                                    "backend_call_id": call_id,
                                    "execution_id": execution_id,
                                },
                            )
                elif current_event in {"chunk", "partial"}:
                    chunk_payload = payload if isinstance(payload, dict) else {"data": payload}
                    progress = _normalize_progress(chunk_payload.get("progress"))
                    data = chunk_payload.get("data", chunk_payload)
                    chunks.append(data)
                    if progress_callback:
                        await progress_callback(
                            progress=progress,
                            message="stream_chunk",
                            partial_data=data,
                            stage="streaming_results",
                            metadata={
                                "chunk_index": len(chunks) - 1,
                                "backend_call_id": call_id,
                            },
                        )
                elif current_event == "signal":
                    if progress_callback:
                        signal_payload = payload if isinstance(payload, dict) else {"payload": payload}
                        await progress_callback(
                            message=f"signal:{signal_payload.get('type', 'unknown')}",
                            stage="streaming_results",
                            metadata={"signal": signal_payload},
                        )
                elif current_event == "heartbeat":
                    if progress_callback:
                        await progress_callback(
                            message="heartbeat",
                            stage="streaming_results",
                            metadata={"heartbeat": payload},
                        )
                elif current_event == "error":
                    if isinstance(payload, dict):
                        err = payload.get("error", payload)
                    else:
                        err = payload
                    message = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                    raise BackendExecutionError(message, backend=backend_id, cause=err)
                elif current_event == "done":
                    done_payload = payload if isinstance(payload, dict) else {"result": payload}
                    break

                current_event = "message"

        if done_payload is None:
            raise BackendExecutionError(
                "Backend stream ended before a terminal result",
                backend=backend_id,
                code="BACKEND_STREAM_UNCERTAIN",
                category="transport",
                cause={"reason": "missing_terminal_frame", "backend_call_id": call_id},
            )
        if done_payload.get("data") is not None:
            return done_payload.get("data")
        if done_payload.get("result") is not None:
            return done_payload.get("result")
        if done_payload.get("all_results") is not None:
            return done_payload.get("all_results")
        if chunks:
            return {"chunks": chunks, "count": len(chunks), "success": True}
        return done_payload

    async def _emit_synthetic_chunks(
        result: Any,
        progress_callback: Optional[Callable[..., Awaitable[None]]],
    ):
        if not progress_callback:
            return
        if not isinstance(result, dict):
            return
        list_payload = None
        for key in ("results", "findings", "items", "documents", "sources"):
            value = result.get(key)
            if isinstance(value, list):
                list_payload = value
                break
        if not list_payload:
            return
        total = max(1, len(list_payload))
        for idx, item in enumerate(list_payload):
            progress = 75.0 + ((idx + 1) / total) * 20.0
            await progress_callback(
                progress=progress,
                message="synthetic_chunk",
                partial_data=item,
                stage="process_response",
                metadata={"synthetic": True, "chunk_index": idx},
            )

    async def _execute_tool(
        tool_name: str,
        inputs: Dict[str, Any],
        progress_callback: Callable = None,
        cancel_event: Optional[asyncio.Event] = None,
        execution_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        *,
        timeout_seconds: float,
        execution_context: Dict[str, Any],
        execution_identity: Dict[str, Any],
    ) -> Any:
        from ..persistence import get_tool_store
        from .circuit_breaker import get_backend_circuit_breaker

        # The public wrapper applies _timeout to the entire execution.
        inputs.pop("_max_output_chars", None)
        inputs.pop("_max_links", None)

        await _emit_stage(
            progress_callback,
            progress=10.0,
            stage="resolve_backend",
            stages_completed=[],
            stages_remaining=["route_backend", "invoke_backend", "process_response", "completed"],
            eta_seconds=20,
        )

        from .delegated_execution import validated_identity_snapshot

        identity = validated_identity_snapshot(execution_identity)
        if str(identity["kind"]).startswith("customer"):
            # Customer jobs use the canonical tenant policy path, including
            # private-backend access, BYOK envelopes and pooled add-on gates.
            # Native direct SSE remains an operator/service execution path.
            from ..api.tools_api import ToolCall, assert_safe_tool_arguments, execute_tool

            internal_keys = {
                "_trace_id", "_session_id", "_parent_execution_id", "_convergence_key",
                "_fanout_budget", "_group_reducer", "_group_reducer_config",
            }
            public_inputs = {key: value for key, value in inputs.items() if key not in internal_keys}
            assert_safe_tool_arguments(public_inputs)
            call = ToolCall(
                tool=tool_name,
                arguments=public_inputs,
                timeout=timeout_seconds,
                trace_id=inputs.get("_trace_id"),
                session_id=inputs.get("_session_id"),
                parent_execution_id=inputs.get("_parent_execution_id"),
            )
            call.bind_identity(identity)
            canonical_result = await execute_tool(call)
            execution_context["call_recorded"] = True
            execution_context["backend_id"] = canonical_result.backend or "unknown"
            if not canonical_result.success:
                raise BackendExecutionError(
                    canonical_result.error or "Customer tool execution failed",
                    backend=canonical_result.backend or "unknown",
                    cause={"error": canonical_result.error},
                )
            return _require_successful_tool_result(canonical_result.result, canonical_result.backend or "unknown")

        # In-process registry tools (built-ins) are not in the backend tool
        # store; execute them directly instead of routing to a backend.
        if ":" not in tool_name:
            from ..registry import get_registry as get_local_registry

            local_registry = get_local_registry()
            if local_registry.get_by_name(tool_name):
                execution_context["backend_id"] = "local-registry"
                await _emit_stage(
                    progress_callback,
                    progress=50.0,
                    stage="invoke_backend",
                    stages_completed=["resolve_backend", "route_backend"],
                    stages_remaining=["process_response", "completed"],
                    eta_seconds=5,
                    metadata={"backend_id": "local-registry"},
                )
                local_inputs = {
                    k: v for k, v in inputs.items() if not k.startswith("_")
                }
                result = await local_registry.execute(tool_name, **local_inputs)
                await _emit_stage(
                    progress_callback,
                    progress=90.0,
                    stage="process_response",
                    stages_completed=["resolve_backend", "route_backend", "invoke_backend"],
                    stages_remaining=["completed"],
                    eta_seconds=1,
                    metadata={"backend_id": "local-registry"},
                )
                return result

        store = get_tool_store()
        backend_id, backend_url, resolved_name = store.resolve_tool(tool_name)
        tool_id = f"{backend_id}:{resolved_name}"
        execution_context.update({"backend_id": backend_id, "tool_id": tool_id, "store": store})
        from .delegated_execution import prepare_delegated_arguments

        outbound_inputs = prepare_delegated_arguments(
            backend_id,
            inputs,
            execution_identity,
            trace_id=inputs.get("_trace_id"),
            session_id=inputs.get("_session_id"),
        )
        ranked_urls = await _rank_backend_urls(backend_id, backend_url, tenant_id)
        if not ranked_urls:
            ranked_urls = [backend_url.rstrip("/")]
        selected_backend_url = ranked_urls[0]
        breaker = get_backend_circuit_breaker()
        execution_context["breaker"] = breaker
        allowed, state = breaker.allow_request(backend_id)
        if not allowed:
            retry_after = state.get("retry_after_seconds")
            retry_suffix = f" retry_after={retry_after}s" if retry_after is not None else ""
            raise RuntimeError(
                f"CIRCUIT_OPEN: backend {backend_id} temporarily unavailable.{retry_suffix}"
            )

        await _emit_stage(
            progress_callback,
            progress=25.0,
            stage="route_backend",
            stages_completed=["resolve_backend"],
            stages_remaining=["invoke_backend", "process_response", "completed"],
            eta_seconds=15,
            metadata={
                "backend_id": backend_id,
                "selected_url": selected_backend_url,
                "candidates": ranked_urls,
            },
        )

        if cancel_event and cancel_event.is_set():
            raise asyncio.CancelledError("Execution cancelled before backend invocation")

        started = time.time()
        last_error: Optional[Exception] = None
        result: Any = None
        for idx, candidate_url in enumerate(ranked_urls):
            selected_backend_url = candidate_url.rstrip("/")
            try:
                await _emit_stage(
                    progress_callback,
                    progress=35.0,
                    stage="invoke_backend",
                    stages_completed=["resolve_backend", "route_backend"],
                    stages_remaining=["process_response", "completed"],
                    eta_seconds=12,
                    metadata={
                        "backend_id": backend_id,
                        "candidate_index": idx,
                        "candidate_url": selected_backend_url,
                    },
                )

                if _stream_backend_default:
                    streamed = await _stream_backend_execute(
                        base_url=selected_backend_url,
                        backend_id=backend_id,
                        tenant_id=tenant_id,
                        resolved_name=resolved_name,
                        inputs=outbound_inputs,
                        progress_callback=progress_callback,
                        cancel_event=cancel_event,
                        execution_id=execution_id,
                        timeout_seconds=timeout_seconds,
                    )
                    if streamed is not None:
                        result = _require_successful_tool_result(streamed, backend_id)
                        break

                if cancel_event and cancel_event.is_set():
                    raise asyncio.CancelledError("Execution cancelled before MCP fallback")

                response = await _shared_client.post(
                    f"{selected_backend_url}/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": str(_uuid4()),
                        "method": "tools/call",
                        "params": {
                            "name": resolved_name,
                            "arguments": outbound_inputs,
                        },
                    },
                    timeout=_httpx.Timeout(timeout_seconds, connect=min(15.0, timeout_seconds)),
                    headers=_backend_headers(backend_id, tenant_id),
                )

                await _emit_stage(
                    progress_callback,
                    progress=80.0,
                    stage="process_response",
                    stages_completed=["resolve_backend", "route_backend", "invoke_backend"],
                    stages_remaining=["completed"],
                    eta_seconds=3,
                    metadata={"backend_id": backend_id, "candidate_url": selected_backend_url},
                )

                if response.status_code != 200:
                    raise RuntimeError(
                        f"Backend {backend_id} returned HTTP {response.status_code}: {response.text[:300]}"
                    )

                data = response.json()
                if "error" in data and data["error"]:
                    error = data["error"]
                    message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                    raise BackendExecutionError(
                        message,
                        backend=backend_id,
                        code="MCP_PROTOCOL_ERROR",
                        category="protocol",
                        cause=error,
                    )
                result = _require_successful_tool_result(data.get("result"), backend_id)
                await _emit_synthetic_chunks(result, progress_callback)
                break
            except asyncio.CancelledError:
                raise
            except BackendExecutionError as exc:
                # A returned provider error proves that transport worked. It
                # must not open the backend circuit or be retried on a replica.
                # Missing stream completion proves neither success nor failure,
                # so that case must not replay the accepted call either.
                latency_ms = (time.time() - started) * 1000
                if exc.error["category"] == "transport":
                    breaker.record_failure(backend_id, error=str(exc))
                else:
                    breaker.record_success(backend_id)
                store.record_call(
                    tool_id,
                    latency_ms,
                    False,
                    execution_mode="job",
                    trace_id=inputs.get("_trace_id"),
                    session_id=inputs.get("_session_id"),
                    error_code=exc.error["code"],
                    error_message=str(exc),
                )
                execution_context["call_recorded"] = True
                raise
            except (_httpx.ConnectError, _httpx.ConnectTimeout) as exc:
                _backend_failures[selected_backend_url] = int(_backend_failures.get(selected_backend_url, 0)) + 1
                last_error = exc
                result = None
                if idx == len(ranked_urls) - 1:
                    break
            except Exception as exc:
                # Once a request may have been submitted, neither HTTP failure
                # nor parsing failure proves that replay on a replica is safe.
                result = None
                last_error = BackendExecutionError(
                    f"Backend execution outcome is uncertain: {exc}",
                    backend=backend_id,
                    code="BACKEND_EXECUTION_UNCERTAIN",
                    category="transport",
                    cause={"reason": type(exc).__name__},
                )
                break

        latency_ms = (time.time() - started) * 1000
        if result is None:
            breaker.record_failure(backend_id, error=str(last_error or "unknown backend error"))
            store.record_call(
                tool_id,
                latency_ms,
                False,
                execution_mode="job",
                trace_id=inputs.get("_trace_id"),
                session_id=inputs.get("_session_id"),
                error_code=(last_error.error["code"] if isinstance(last_error, BackendExecutionError) else "TRANSPORT_ERROR"),
                error_message=str(last_error or "backend execution failed"),
            )
            execution_context["call_recorded"] = True
            if isinstance(last_error, BackendExecutionError):
                raise last_error
            raise RuntimeError(str(last_error or "backend execution failed"))

        breaker.record_success(backend_id)
        store.record_call(
            tool_id,
            latency_ms,
            True,
            execution_mode="job",
            trace_id=inputs.get("_trace_id"),
            session_id=inputs.get("_session_id"),
        )
        execution_context["call_recorded"] = True

        # Apply output guard if available
        try:
            from ..guards.output_guard import apply_output_guard
            if result is not None:
                result = apply_output_guard(
                    result,
                    tool_name=f"{backend_id}:{resolved_name}",
                    source_context={
                        "backend": backend_id,
                        "via": "job_executor",
                        "backend_url": selected_backend_url,
                    },
                )
        except ImportError:
            pass

        await _emit_stage(
            progress_callback,
            progress=95.0,
            stage="completed",
            stages_completed=["resolve_backend", "route_backend", "invoke_backend", "process_response"],
            stages_remaining=[],
            eta_seconds=0,
            metadata={"backend_id": backend_id, "backend_url": selected_backend_url},
        )
        return result

    async def _tool_executor(
        tool_name: str,
        inputs: Dict[str, Any],
        progress_callback: Callable = None,
        cancel_event: Optional[asyncio.Event] = None,
        execution_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        execution_identity: Optional[Dict[str, Any]] = None,
    ) -> Any:
        execution_inputs = dict(inputs)
        timeout_hint = execution_inputs.pop("_timeout", None)
        timeout_seconds = _execution_timeout_seconds if timeout_hint is None else float(timeout_hint)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("_timeout must be finite and positive")
        context: Dict[str, Any] = {}
        started = time.monotonic()
        deadline = asyncio.timeout(timeout_seconds)
        try:
            async with deadline:
                return await _execute_tool(
                    tool_name,
                    execution_inputs,
                    progress_callback=progress_callback,
                    cancel_event=cancel_event,
                    execution_id=execution_id,
                    tenant_id=tenant_id,
                    timeout_seconds=timeout_seconds,
                    execution_context=context,
                    execution_identity=execution_identity or {},
                )
        except TimeoutError as exc:
            if not deadline.expired():
                raise
            error = BackendExecutionError(
                f"Tool execution exceeded its {timeout_seconds:g}s deadline; outcome may be unknown",
                backend=context.get("backend_id", "unknown"),
                code="BACKEND_TIMEOUT",
                category="transport",
                cause={"reason": "execution_deadline", "timeout_seconds": timeout_seconds},
            )
            if not context.get("call_recorded"):
                if context.get("breaker"):
                    context["breaker"].record_failure(context["backend_id"], error=str(error))
                if context.get("store"):
                    context["store"].record_call(
                        context["tool_id"],
                        (time.monotonic() - started) * 1000,
                        False,
                        execution_mode="job",
                        trace_id=execution_inputs.get("_trace_id"),
                        session_id=execution_inputs.get("_session_id"),
                        error_code="BACKEND_TIMEOUT",
                        error_message=str(error),
                    )
            raise error from exc

    return _tool_executor
