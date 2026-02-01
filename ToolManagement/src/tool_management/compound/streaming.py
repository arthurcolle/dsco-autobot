"""
Advanced Streaming Infrastructure for Compound Tools.

Features:
- Parallel pipeline execution
- Pipeline chaining (output → input)
- Cancellation support
- Event filtering and subscriptions
- Execution replay
- Backpressure handling
- Real-time metrics
- Connection health monitoring
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple, Union
)
from uuid import uuid4

from pydantic import BaseModel, Field

from .pipelines import BasePipeline, PipelineEvent, PipelineEventType, PipelineState

logger = logging.getLogger(__name__)


# =============================================================================
# STREAMING CONFIGURATION
# =============================================================================

class StreamConfig(BaseModel):
    """Configuration for streaming execution."""
    # Event filtering
    event_types: Optional[List[PipelineEventType]] = None  # None = all events
    include_state_snapshots: bool = False
    include_tool_results: bool = True

    # Backpressure
    buffer_size: int = 100
    drop_on_overflow: bool = False  # If False, block; if True, drop oldest

    # Heartbeat
    heartbeat_interval_seconds: float = 30.0

    # Compression
    compress_large_payloads: bool = True
    compression_threshold_bytes: int = 10000


# =============================================================================
# EXECUTION CONTEXT
# =============================================================================

class ExecutionStatus(str, Enum):
    """Status of a streaming execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class ExecutionContext:
    """Context for a streaming execution."""
    id: str = field(default_factory=lambda: str(uuid4()))
    pipeline_name: str = ""
    inputs: Dict[str, Any] = field(default_factory=dict)
    config: StreamConfig = field(default_factory=StreamConfig)

    # State
    status: ExecutionStatus = ExecutionStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Control
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    pause_event: asyncio.Event = field(default_factory=asyncio.Event)

    # Results
    events: List[PipelineEvent] = field(default_factory=list)
    final_state: Optional[PipelineState] = None
    error: Optional[str] = None

    # Metrics
    events_emitted: int = 0
    bytes_emitted: int = 0

    def __post_init__(self):
        self.pause_event.set()  # Not paused by default


# =============================================================================
# CHAINED PIPELINE DEFINITION
# =============================================================================

class PipelineChainStep(BaseModel):
    """A step in a pipeline chain."""
    pipeline: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    # Map outputs from previous step to inputs for this step
    input_mappings: Dict[str, str] = Field(default_factory=dict)
    # Condition to execute this step (evaluated against previous output)
    condition: Optional[str] = None  # e.g., "result.confidence > 0.8"


class PipelineChain(BaseModel):
    """Definition of chained pipelines."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str = ""
    steps: List[PipelineChainStep]

    # Execution options
    stop_on_failure: bool = True
    parallel_steps: List[List[int]] = Field(default_factory=list)  # Groups of step indices to run in parallel


# =============================================================================
# PARALLEL EXECUTION
# =============================================================================

class ParallelExecutionRequest(BaseModel):
    """Request to execute multiple pipelines in parallel."""
    executions: List[Dict[str, Any]]  # [{pipeline, inputs}, ...]
    merge_strategy: str = "collect"  # collect, merge, first_success
    max_concurrent: int = 10
    timeout_seconds: Optional[float] = None


# =============================================================================
# STREAMING EXECUTOR
# =============================================================================

class StreamingExecutor:
    """
    Advanced streaming executor for compound tools.

    Features:
    - Cancellation support
    - Event filtering
    - Backpressure handling
    - Metrics collection
    - Execution history with replay
    """

    def __init__(self, registry):
        self.registry = registry
        self.active_executions: Dict[str, ExecutionContext] = {}
        self.execution_history: Dict[str, ExecutionContext] = {}
        self.metrics = ExecutionMetrics()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start background tasks."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self):
        """Stop background tasks."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self):
        """Periodically clean up old executions."""
        while True:
            await asyncio.sleep(300)  # Every 5 minutes
            cutoff = datetime.utcnow() - timedelta(hours=1)
            to_remove = [
                eid for eid, ctx in self.execution_history.items()
                if ctx.completed_at and ctx.completed_at < cutoff
            ]
            for eid in to_remove:
                del self.execution_history[eid]

    # =========================================================================
    # SINGLE EXECUTION
    # =========================================================================

    async def execute(
        self,
        pipeline_name: str,
        inputs: Dict[str, Any],
        config: Optional[StreamConfig] = None,
    ) -> AsyncIterator[PipelineEvent]:
        """
        Execute a pipeline with advanced streaming.

        Supports:
        - Event filtering
        - Cancellation
        - Pause/resume
        - Metrics
        """
        config = config or StreamConfig()
        ctx = ExecutionContext(
            pipeline_name=pipeline_name,
            inputs=inputs,
            config=config,
        )

        self.active_executions[ctx.id] = ctx
        self.metrics.executions_started += 1

        try:
            ctx.status = ExecutionStatus.RUNNING
            ctx.started_at = datetime.utcnow()

            # Yield execution started event
            start_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_STARTED,
                pipeline_id=ctx.id,
                pipeline_name=pipeline_name,
                data={
                    "execution_id": ctx.id,
                    "inputs": inputs,
                    "config": config.model_dump(),
                },
            )
            yield start_event
            ctx.events.append(start_event)
            ctx.events_emitted += 1

            # Stream pipeline events
            async for event in self.registry.execute_stream(pipeline_name, **inputs):
                # Check for cancellation
                if ctx.cancel_event.is_set():
                    ctx.status = ExecutionStatus.CANCELLED
                    cancel_event = PipelineEvent(
                        event_type=PipelineEventType.PIPELINE_FAILED,
                        pipeline_id=ctx.id,
                        pipeline_name=pipeline_name,
                        error="Execution cancelled",
                    )
                    yield cancel_event
                    break

                # Check for pause
                await ctx.pause_event.wait()

                # Filter events
                if config.event_types and event.event_type not in config.event_types:
                    continue

                # Strip state snapshots if not requested
                if not config.include_state_snapshots:
                    event.state_snapshot = None

                # Strip tool results if not requested
                if not config.include_tool_results:
                    event.tool_result = None

                # Track
                ctx.events.append(event)
                ctx.events_emitted += 1
                event_json = event.model_dump_json()
                ctx.bytes_emitted += len(event_json)

                # Capture final state
                if event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                    if event.state_snapshot:
                        ctx.final_state = PipelineState(**event.state_snapshot)
                    ctx.status = ExecutionStatus.COMPLETED
                elif event.event_type == PipelineEventType.PIPELINE_FAILED:
                    ctx.status = ExecutionStatus.FAILED
                    ctx.error = event.error

                yield event

            ctx.completed_at = datetime.utcnow()
            self.metrics.executions_completed += 1

        except Exception as e:
            ctx.status = ExecutionStatus.FAILED
            ctx.error = str(e)
            ctx.completed_at = datetime.utcnow()
            self.metrics.executions_failed += 1

            error_event = PipelineEvent(
                event_type=PipelineEventType.PIPELINE_FAILED,
                pipeline_id=ctx.id,
                pipeline_name=pipeline_name,
                error=str(e),
            )
            yield error_event

        finally:
            # Move to history
            del self.active_executions[ctx.id]
            self.execution_history[ctx.id] = ctx

    # =========================================================================
    # CANCELLATION
    # =========================================================================

    def cancel(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        if execution_id in self.active_executions:
            self.active_executions[execution_id].cancel_event.set()
            return True
        return False

    def pause(self, execution_id: str) -> bool:
        """Pause a running execution."""
        if execution_id in self.active_executions:
            ctx = self.active_executions[execution_id]
            ctx.pause_event.clear()
            ctx.status = ExecutionStatus.PAUSED
            return True
        return False

    def resume(self, execution_id: str) -> bool:
        """Resume a paused execution."""
        if execution_id in self.active_executions:
            ctx = self.active_executions[execution_id]
            ctx.pause_event.set()
            ctx.status = ExecutionStatus.RUNNING
            return True
        return False

    # =========================================================================
    # PARALLEL EXECUTION
    # =========================================================================

    async def execute_parallel(
        self,
        requests: List[Tuple[str, Dict[str, Any]]],
        config: Optional[StreamConfig] = None,
        max_concurrent: int = 10,
    ) -> AsyncIterator[Tuple[int, PipelineEvent]]:
        """
        Execute multiple pipelines in parallel.

        Yields (index, event) tuples to identify which pipeline produced each event.
        """
        config = config or StreamConfig()
        semaphore = asyncio.Semaphore(max_concurrent)

        async def run_one(idx: int, pipeline_name: str, inputs: Dict[str, Any]):
            async with semaphore:
                async for event in self.execute(pipeline_name, inputs, config):
                    yield (idx, event)

        # Create tasks for all pipelines
        generators = [
            run_one(i, name, inputs)
            for i, (name, inputs) in enumerate(requests)
        ]

        # Merge streams
        async for item in self._merge_async_iterators(generators):
            yield item

    async def _merge_async_iterators(
        self,
        iterators: List[AsyncIterator],
    ) -> AsyncIterator:
        """Merge multiple async iterators into one."""
        pending = set()
        iterator_map = {}

        for it in iterators:
            task = asyncio.create_task(self._next_item(it))
            pending.add(task)
            iterator_map[id(task)] = it

        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                try:
                    item, it = task.result()
                    yield item

                    # Schedule next item from same iterator
                    new_task = asyncio.create_task(self._next_item(it))
                    pending.add(new_task)
                    iterator_map[id(new_task)] = it

                except StopAsyncIteration:
                    pass

    async def _next_item(self, iterator: AsyncIterator) -> Tuple[Any, AsyncIterator]:
        """Get next item from iterator."""
        item = await iterator.__anext__()
        return (item, iterator)

    # =========================================================================
    # PIPELINE CHAINING
    # =========================================================================

    async def execute_chain(
        self,
        chain: PipelineChain,
        initial_inputs: Dict[str, Any],
        config: Optional[StreamConfig] = None,
    ) -> AsyncIterator[PipelineEvent]:
        """
        Execute a chain of pipelines where outputs feed into inputs.
        """
        config = config or StreamConfig()
        chain_id = str(uuid4())

        # Emit chain started
        yield PipelineEvent(
            event_type=PipelineEventType.PIPELINE_STARTED,
            pipeline_id=chain_id,
            pipeline_name=chain.name,
            data={
                "chain_id": chain_id,
                "steps": len(chain.steps),
                "initial_inputs": initial_inputs,
            },
        )

        current_inputs = initial_inputs.copy()
        previous_result: Optional[Dict[str, Any]] = None

        for step_idx, step in enumerate(chain.steps):
            # Check condition
            if step.condition and previous_result:
                if not self._evaluate_condition(step.condition, previous_result):
                    continue

            # Build step inputs
            step_inputs = step.inputs.copy()
            step_inputs.update(current_inputs)

            # Apply input mappings from previous result
            if previous_result and step.input_mappings:
                for target_key, source_path in step.input_mappings.items():
                    value = self._get_nested_value(previous_result, source_path)
                    if value is not None:
                        step_inputs[target_key] = value

            # Emit step started
            yield PipelineEvent(
                event_type=PipelineEventType.STEP_STARTED,
                pipeline_id=chain_id,
                pipeline_name=chain.name,
                step_index=step_idx + 1,
                step_name=f"chain_step_{step_idx + 1}_{step.pipeline}",
                data={"pipeline": step.pipeline, "inputs": step_inputs},
            )

            # Execute step
            step_result = None
            try:
                async for event in self.execute(step.pipeline, step_inputs, config):
                    # Tag event with chain info
                    event.data = event.data or {}
                    event.data["chain_id"] = chain_id
                    event.data["chain_step"] = step_idx + 1

                    yield event

                    # Capture result
                    if event.event_type == PipelineEventType.PIPELINE_COMPLETED:
                        if event.data:
                            step_result = event.data.get("final_result")
                    elif event.event_type == PipelineEventType.PIPELINE_FAILED:
                        if chain.stop_on_failure:
                            yield PipelineEvent(
                                event_type=PipelineEventType.PIPELINE_FAILED,
                                pipeline_id=chain_id,
                                pipeline_name=chain.name,
                                error=f"Chain failed at step {step_idx + 1}: {event.error}",
                            )
                            return

                previous_result = step_result

            except Exception as e:
                if chain.stop_on_failure:
                    yield PipelineEvent(
                        event_type=PipelineEventType.PIPELINE_FAILED,
                        pipeline_id=chain_id,
                        pipeline_name=chain.name,
                        error=f"Chain failed at step {step_idx + 1}: {str(e)}",
                    )
                    return

        # Emit chain completed
        yield PipelineEvent(
            event_type=PipelineEventType.PIPELINE_COMPLETED,
            pipeline_id=chain_id,
            pipeline_name=chain.name,
            data={
                "chain_id": chain_id,
                "steps_completed": len(chain.steps),
                "final_result": previous_result,
            },
        )

    def _evaluate_condition(self, condition: str, result: Dict[str, Any]) -> bool:
        """Evaluate a simple condition against a result."""
        try:
            # Simple path-based conditions: "result.confidence > 0.8"
            # This is a simplified evaluator - production should use a safe expression parser
            local_vars = {"result": type("Result", (), result)()}
            return eval(condition, {"__builtins__": {}}, local_vars)
        except Exception:
            return True  # Default to executing on error

    def _get_nested_value(self, obj: Dict[str, Any], path: str) -> Any:
        """Get a nested value from a dict using dot notation."""
        parts = path.split(".")
        current = obj
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current

    # =========================================================================
    # REPLAY
    # =========================================================================

    async def replay(
        self,
        execution_id: str,
        speed: float = 1.0,
    ) -> AsyncIterator[PipelineEvent]:
        """
        Replay a past execution's events.

        Args:
            execution_id: ID of the execution to replay
            speed: Playback speed multiplier (2.0 = 2x speed)
        """
        ctx = self.execution_history.get(execution_id)
        if not ctx:
            raise ValueError(f"Execution not found: {execution_id}")

        if not ctx.events:
            return

        last_time = ctx.events[0].timestamp

        for event in ctx.events:
            # Calculate delay
            if speed > 0:
                delay = (event.timestamp - last_time).total_seconds() / speed
                if delay > 0:
                    await asyncio.sleep(delay)

            last_time = event.timestamp
            yield event

    # =========================================================================
    # STATUS & METRICS
    # =========================================================================

    def get_execution(self, execution_id: str) -> Optional[ExecutionContext]:
        """Get execution context by ID."""
        return (
            self.active_executions.get(execution_id) or
            self.execution_history.get(execution_id)
        )

    def list_active(self) -> List[Dict[str, Any]]:
        """List active executions."""
        return [
            {
                "id": ctx.id,
                "pipeline": ctx.pipeline_name,
                "status": ctx.status.value,
                "started_at": ctx.started_at.isoformat() if ctx.started_at else None,
                "events_emitted": ctx.events_emitted,
            }
            for ctx in self.active_executions.values()
        ]

    def get_metrics(self) -> Dict[str, Any]:
        """Get executor metrics."""
        return self.metrics.to_dict()


# =============================================================================
# METRICS
# =============================================================================

@dataclass
class ExecutionMetrics:
    """Metrics for streaming executor."""
    executions_started: int = 0
    executions_completed: int = 0
    executions_failed: int = 0
    executions_cancelled: int = 0
    total_events_emitted: int = 0
    total_bytes_emitted: int = 0

    # Timing
    _start_time: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        uptime = (datetime.utcnow() - self._start_time).total_seconds()
        return {
            "executions": {
                "started": self.executions_started,
                "completed": self.executions_completed,
                "failed": self.executions_failed,
                "cancelled": self.executions_cancelled,
                "success_rate": (
                    self.executions_completed / max(1, self.executions_started)
                ),
            },
            "throughput": {
                "events_emitted": self.total_events_emitted,
                "bytes_emitted": self.total_bytes_emitted,
                "events_per_second": self.total_events_emitted / max(1, uptime),
            },
            "uptime_seconds": uptime,
        }


# =============================================================================
# CONNECTION MANAGER (ENHANCED)
# =============================================================================

class StreamSubscription(BaseModel):
    """Client subscription configuration."""
    event_types: Optional[List[PipelineEventType]] = None
    pipelines: Optional[List[str]] = None
    include_heartbeat: bool = True


@dataclass
class ClientConnection:
    """Enhanced client connection tracking."""
    id: str
    websocket: Any  # WebSocket instance
    connected_at: datetime = field(default_factory=datetime.utcnow)
    subscription: StreamSubscription = field(default_factory=StreamSubscription)

    # State
    active_executions: Set[str] = field(default_factory=set)

    # Metrics
    events_sent: int = 0
    bytes_sent: int = 0
    last_activity: datetime = field(default_factory=datetime.utcnow)

    # Buffer for backpressure
    buffer: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=100))

    # Health
    missed_heartbeats: int = 0


class AdvancedConnectionManager:
    """
    Enhanced WebSocket connection manager.

    Features:
    - Client subscriptions
    - Backpressure handling
    - Connection health monitoring
    - Broadcast capabilities
    """

    def __init__(self):
        self.connections: Dict[str, ClientConnection] = {}
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._sender_tasks: Dict[str, asyncio.Task] = {}

    async def start(self):
        """Start background tasks."""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Stop background tasks."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        for task in self._sender_tasks.values():
            task.cancel()

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to all connections."""
        while True:
            await asyncio.sleep(30)

            for client_id, conn in list(self.connections.items()):
                if conn.subscription.include_heartbeat:
                    try:
                        await conn.websocket.send_json({
                            "type": "heartbeat",
                            "timestamp": datetime.utcnow().isoformat(),
                            "stats": {
                                "events_sent": conn.events_sent,
                                "active_executions": len(conn.active_executions),
                            },
                        })
                        conn.missed_heartbeats = 0
                    except Exception:
                        conn.missed_heartbeats += 1
                        if conn.missed_heartbeats >= 3:
                            await self.disconnect(client_id)

    async def connect(
        self,
        websocket: Any,
        client_id: Optional[str] = None,
    ) -> str:
        """Accept a new connection."""
        client_id = client_id or str(uuid4())

        await websocket.accept()

        conn = ClientConnection(
            id=client_id,
            websocket=websocket,
        )
        self.connections[client_id] = conn

        # Start sender task for this connection
        self._sender_tasks[client_id] = asyncio.create_task(
            self._sender_loop(client_id)
        )

        return client_id

    async def disconnect(self, client_id: str):
        """Disconnect a client."""
        if client_id in self.connections:
            del self.connections[client_id]

        if client_id in self._sender_tasks:
            self._sender_tasks[client_id].cancel()
            del self._sender_tasks[client_id]

    async def _sender_loop(self, client_id: str):
        """Send queued events to a client."""
        while client_id in self.connections:
            conn = self.connections.get(client_id)
            if not conn:
                break

            try:
                event = await asyncio.wait_for(
                    conn.buffer.get(),
                    timeout=1.0,
                )
                await conn.websocket.send_json(event)
                conn.events_sent += 1
                conn.last_activity = datetime.utcnow()

            except asyncio.TimeoutError:
                continue
            except Exception:
                await self.disconnect(client_id)
                break

    def subscribe(self, client_id: str, subscription: StreamSubscription):
        """Update client subscription."""
        if client_id in self.connections:
            self.connections[client_id].subscription = subscription

    async def send_event(
        self,
        client_id: str,
        event: Union[PipelineEvent, Dict[str, Any]],
    ):
        """Queue an event for sending to a client."""
        conn = self.connections.get(client_id)
        if not conn:
            return

        # Filter by subscription
        if isinstance(event, PipelineEvent):
            sub = conn.subscription

            if sub.event_types and event.event_type not in sub.event_types:
                return

            if sub.pipelines and event.pipeline_name not in sub.pipelines:
                return

            event_data = event.model_dump(mode="json")
        else:
            event_data = event

        # Queue with backpressure handling
        try:
            conn.buffer.put_nowait(event_data)
        except asyncio.QueueFull:
            # Drop oldest event
            try:
                conn.buffer.get_nowait()
                conn.buffer.put_nowait(event_data)
            except asyncio.QueueEmpty:
                pass

    async def broadcast(
        self,
        event: Union[PipelineEvent, Dict[str, Any]],
        pipeline_name: Optional[str] = None,
    ):
        """Broadcast event to all subscribed clients."""
        for client_id in list(self.connections.keys()):
            await self.send_event(client_id, event)

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        return {
            "total_connections": len(self.connections),
            "connections": [
                {
                    "id": conn.id,
                    "connected_at": conn.connected_at.isoformat(),
                    "events_sent": conn.events_sent,
                    "active_executions": len(conn.active_executions),
                    "buffer_size": conn.buffer.qsize(),
                }
                for conn in self.connections.values()
            ],
        }


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    "StreamConfig",
    "ExecutionStatus",
    "ExecutionContext",
    "PipelineChainStep",
    "PipelineChain",
    "ParallelExecutionRequest",
    "StreamingExecutor",
    "ExecutionMetrics",
    "StreamSubscription",
    "ClientConnection",
    "AdvancedConnectionManager",
]
