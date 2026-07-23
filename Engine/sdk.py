"""
Engine Agent SDK

The client library agents use to connect to the Engine.

Usage:
    from engine.sdk import EngineClient, AgentConfig

    async def main():
        config = AgentConfig(
            agent_id="my-agent",
            agent_type="worker",
            capabilities=["code", "search"],
            tick_interval_ms=1000,
        )

        async with EngineClient(config) as engine:
            async for message in engine:
                if message.type == "tick":
                    result = await do_work(message.tick)
                    await engine.send_result(message.tick.tick_id, result)

                elif message.type == "signal":
                    await handle_signal(message.signal)

                elif message.type == "state_delta":
                    engine.state.merge(message.delta)
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import (
    Optional, Dict, Any, List, Callable, Awaitable,
    AsyncIterator, Union,
)
from enum import Enum
import uuid
import websockets
from websockets.client import WebSocketClientProtocol

from .models import (
    Tick,
    TickType,
    Signal,
    SignalType,
    Task,
    TaskStatus,
    AgentConfig,
    AgentStatus,
    AgentState,
    AgentPresence,
    StateDelta,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Message Types
# ============================================================================

class MessageType(str, Enum):
    """Types of messages from the Engine."""
    TICK = "tick"
    SIGNAL = "signal"
    STATE_DELTA = "state_delta"
    HEARTBEAT = "heartbeat"
    PRESENCE = "presence"
    ACK = "ack"
    ERROR = "error"


@dataclass
class EngineMessage:
    """A message received from the Engine."""
    type: MessageType
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)

    # Payload varies by type
    tick: Optional[Tick] = None
    signal: Optional[Signal] = None
    delta: Optional[StateDelta] = None
    presence: Optional[List[AgentPresence]] = None
    error: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EngineMessage":
        """Parse message from JSON dict."""
        msg_type = MessageType(data.get("type"))
        msg = cls(
            type=msg_type,
            message_id=data.get("message_id", str(uuid.uuid4())),
        )

        if msg_type == MessageType.TICK:
            tick_data = data.get("tick", {})
            msg.tick = Tick(
                tick_id=tick_data.get("tick_id"),
                tick_type=TickType(tick_data.get("tick_type", "poll")),
                timestamp=datetime.fromisoformat(tick_data.get("timestamp", datetime.utcnow().isoformat())),
                timestamp_ns=tick_data.get("timestamp_ns", time.time_ns()),
                sequence=tick_data.get("sequence", 0),
                deadline_ms=tick_data.get("deadline_ms", 100),
            )
            # Parse scheduled tasks if present
            if "scheduled_tasks" in tick_data:
                msg.tick.scheduled_tasks = [
                    Task(**t) if isinstance(t, dict) else t
                    for t in tick_data["scheduled_tasks"]
                ]

        elif msg_type == MessageType.SIGNAL:
            signal_data = data.get("signal", {})
            msg.signal = Signal(
                signal_id=signal_data.get("signal_id"),
                signal_type=SignalType(signal_data.get("signal_type")),
                target_agent=signal_data.get("target_agent", ""),
                payload=signal_data.get("payload"),
                preempt=signal_data.get("preempt", False),
                priority=signal_data.get("priority", 5),
            )

        elif msg_type == MessageType.STATE_DELTA:
            msg.delta = StateDelta(**data.get("delta", {}))

        elif msg_type == MessageType.PRESENCE:
            msg.presence = [
                AgentPresence(**p) for p in data.get("agents", [])
            ]

        elif msg_type == MessageType.ERROR:
            msg.error = data.get("error")

        return msg


# ============================================================================
# Client Configuration
# ============================================================================

from dsco_shared.config.service_registry import ServiceName, resolve


@dataclass
class ClientConfig:
    """Configuration for the Engine client."""
    engine_url: str = resolve(ServiceName.ENGINE, "ws")
    reconnect_delay_ms: int = 1000
    max_reconnect_delay_ms: int = 30000
    heartbeat_interval_ms: int = 30000
    auto_reconnect: bool = True
    auto_ack: bool = True


# ============================================================================
# Engine Client
# ============================================================================

class EngineClient:
    """
    Client for connecting to the Engine API.

    Provides:
    - Persistent WebSocket connection
    - Auto-reconnection with backoff
    - Message streaming
    - State synchronization
    - Tool invocation
    """

    def __init__(
        self,
        agent_config: AgentConfig,
        client_config: Optional[ClientConfig] = None,
    ):
        self.agent_config = agent_config
        self.client_config = client_config or ClientConfig()

        # Connection state
        self._ws: Optional[WebSocketClientProtocol] = None
        self._connected = False
        self._reconnect_attempt = 0

        # Local state (CRDT)
        self.state = AgentState(
            agent_id=agent_config.agent_id,
            status=AgentStatus.IDLE,
        )

        # Message queue
        self._message_queue: asyncio.Queue[EngineMessage] = asyncio.Queue()
        self._pending_acks: Dict[str, asyncio.Future] = {}

        # Background tasks
        self._recv_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Callbacks
        self._on_tick: Optional[Callable[[Tick], Awaitable[Any]]] = None
        self._on_signal: Optional[Callable[[Signal], Awaitable[None]]] = None
        self._on_error: Optional[Callable[[str], Awaitable[None]]] = None

    @property
    def connected(self) -> bool:
        return self._connected and self._ws is not None

    @property
    def agent_id(self) -> str:
        return self.agent_config.agent_id

    # ========================================================================
    # Connection Lifecycle
    # ========================================================================

    async def connect(self) -> None:
        """Connect to the Engine."""
        url = (
            f"{self.client_config.engine_url}/ws/{self.agent_config.agent_id}"
            f"?agent_type={self.agent_config.agent_type}"
            f"&tick_interval_ms={self.agent_config.tick_interval_ms}"
        )

        logger.info(f"Connecting to Engine: {url}")

        try:
            self._ws = await websockets.connect(
                url,
                ping_interval=self.client_config.heartbeat_interval_ms / 1000,
                ping_timeout=10,
            )
            self._connected = True
            self._reconnect_attempt = 0

            # Start background tasks
            self._recv_task = asyncio.create_task(self._receive_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            logger.info(f"Connected to Engine as {self.agent_id}")

            # Update state
            self.state.status = AgentStatus.IDLE
            self.state.connected_at = datetime.utcnow()

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            if self.client_config.auto_reconnect:
                await self._reconnect()
            else:
                raise

    async def disconnect(self) -> None:
        """Disconnect from the Engine."""
        logger.info(f"Disconnecting agent {self.agent_id}")

        self._connected = False

        # Cancel background tasks
        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None

        self.state.status = AgentStatus.TERMINATED

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff."""
        while self.client_config.auto_reconnect:
            self._reconnect_attempt += 1
            delay = min(
                self.client_config.reconnect_delay_ms * (2 ** self._reconnect_attempt),
                self.client_config.max_reconnect_delay_ms,
            ) / 1000

            logger.info(f"Reconnecting in {delay}s (attempt {self._reconnect_attempt})")
            await asyncio.sleep(delay)

            try:
                await self.connect()
                return
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")

    # ========================================================================
    # Context Manager
    # ========================================================================

    async def __aenter__(self) -> "EngineClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()

    # ========================================================================
    # Message Streaming
    # ========================================================================

    def __aiter__(self) -> AsyncIterator[EngineMessage]:
        return self

    async def __anext__(self) -> EngineMessage:
        if not self._connected and self._message_queue.empty():
            raise StopAsyncIteration
        return await self._message_queue.get()

    async def _receive_loop(self) -> None:
        """Background loop to receive messages."""
        while self._connected and self._ws:
            try:
                raw = await self._ws.recv()
                data = json.loads(raw)
                message = EngineMessage.from_dict(data)

                # Auto-ack if enabled
                if self.client_config.auto_ack:
                    await self._send_ack(message.message_id)

                # Handle message types
                if message.type == MessageType.TICK:
                    if self._on_tick:
                        result = await self._on_tick(message.tick)
                        if result is not None:
                            await self.send_result(message.tick.tick_id, result)
                    else:
                        await self._message_queue.put(message)

                elif message.type == MessageType.SIGNAL:
                    if self._on_signal:
                        await self._on_signal(message.signal)
                    else:
                        await self._message_queue.put(message)

                elif message.type == MessageType.STATE_DELTA:
                    # Always merge state deltas
                    if message.delta:
                        self._apply_delta(message.delta)
                    await self._message_queue.put(message)

                elif message.type == MessageType.ACK:
                    # Resolve pending ack
                    ack_id = data.get("ack_id")
                    if ack_id in self._pending_acks:
                        self._pending_acks[ack_id].set_result(True)
                        del self._pending_acks[ack_id]

                elif message.type == MessageType.ERROR:
                    if self._on_error:
                        await self._on_error(message.error)
                    else:
                        await self._message_queue.put(message)

                else:
                    await self._message_queue.put(message)

            except websockets.ConnectionClosed:
                logger.warning("WebSocket connection closed")
                self._connected = False
                if self.client_config.auto_reconnect:
                    await self._reconnect()
                break

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON received: {e}")

            except Exception as e:
                logger.error(f"Error in receive loop: {e}")
                await asyncio.sleep(0.1)

    async def _heartbeat_loop(self) -> None:
        """Background loop to send heartbeats."""
        interval = self.client_config.heartbeat_interval_ms / 1000

        while self._connected:
            await asyncio.sleep(interval)
            if self._connected and self._ws:
                try:
                    await self._ws.ping()
                except Exception as e:
                    logger.warning(f"Heartbeat failed: {e}")

    # ========================================================================
    # Sending Messages
    # ========================================================================

    async def _send(self, message: Dict[str, Any]) -> None:
        """Send a message to the Engine."""
        if not self._ws:
            raise RuntimeError("Not connected")
        await self._ws.send(json.dumps(message))

    async def _send_ack(self, message_id: str) -> None:
        """Send acknowledgment for a message."""
        await self._send({
            "type": "ack",
            "message_id": message_id,
        })

    async def send_result(self, task_id: str, result: Any) -> None:
        """Send task result."""
        await self._send({
            "type": "result",
            "task_id": task_id,
            "result": result,
        })
        self.state.tasks_completed += 1

    async def send_state_mutation(self, mutation: Dict[str, Any]) -> None:
        """Send a state mutation."""
        await self._send({
            "type": "state_mutation",
            "mutation": mutation,
            "timestamp_ns": time.time_ns(),
        })

    async def request_tool(
        self,
        tool_id: str,
        params: Dict[str, Any],
    ) -> Any:
        """Request tool execution and wait for result."""
        request_id = str(uuid.uuid4())

        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self._pending_acks[request_id] = future

        await self._send({
            "type": "tool_request",
            "request_id": request_id,
            "tool_id": tool_id,
            "params": params,
        })

        # Wait for result
        try:
            result = await asyncio.wait_for(future, timeout=60)
            return result
        except asyncio.TimeoutError:
            del self._pending_acks[request_id]
            raise

    async def request_checkpoint(self) -> str:
        """Request a state checkpoint."""
        await self._send({"type": "checkpoint_request"})
        return f"ckpt-{self.agent_id}-{int(time.time())}"

    # ========================================================================
    # State Management
    # ========================================================================

    def _apply_delta(self, delta: StateDelta) -> None:
        """Apply a state delta to local state."""
        if delta.changes:
            for key, value in delta.changes.items():
                setattr(self.state, key, value)

    async def update_status(self, status: AgentStatus) -> None:
        """Update agent status."""
        self.state.status = status
        await self.send_state_mutation({
            "status": status.value,
        })

    async def update_metadata(self, key: str, value: Any) -> None:
        """Update agent metadata."""
        self.state.metadata[key] = value
        await self.send_state_mutation({
            "metadata": {key: value},
        })

    # ========================================================================
    # Callback Registration
    # ========================================================================

    def on_tick(
        self,
        handler: Callable[[Tick], Awaitable[Any]],
    ) -> "EngineClient":
        """Register tick handler. If handler returns a value, it's sent as result."""
        self._on_tick = handler
        return self

    def on_signal(
        self,
        handler: Callable[[Signal], Awaitable[None]],
    ) -> "EngineClient":
        """Register signal handler."""
        self._on_signal = handler
        return self

    def on_error(
        self,
        handler: Callable[[str], Awaitable[None]],
    ) -> "EngineClient":
        """Register error handler."""
        self._on_error = handler
        return self


# ============================================================================
# Convenience Functions
# ============================================================================

async def run_agent(
    agent_config: AgentConfig,
    on_tick: Callable[[Tick], Awaitable[Any]],
    on_signal: Optional[Callable[[Signal], Awaitable[None]]] = None,
    client_config: Optional[ClientConfig] = None,
) -> None:
    """
    Run an agent with the given handlers.

    Example:
        async def handle_tick(tick: Tick) -> Any:
            if tick.scheduled_tasks:
                task = tick.scheduled_tasks[0]
                return await do_work(task)
            return None

        await run_agent(
            AgentConfig(agent_id="worker-1"),
            on_tick=handle_tick,
        )
    """
    client = EngineClient(agent_config, client_config)
    client.on_tick(on_tick)

    if on_signal:
        client.on_signal(on_signal)

    async with client:
        # The client will run until disconnected
        async for message in client:
            # Messages not handled by callbacks end up here
            logger.debug(f"Unhandled message: {message.type}")


# ============================================================================
# Agent Base Class
# ============================================================================

class Agent:
    """
    Base class for agents.

    Subclass this for structured agent implementations.

    Example:
        class MyAgent(Agent):
            async def on_tick(self, tick: Tick) -> Any:
                if tick.tick_type == TickType.TASK:
                    return await self.process_task(tick.scheduled_tasks[0])
                return None

            async def process_task(self, task: Task) -> Any:
                # Do work
                return {"result": "done"}

        agent = MyAgent(AgentConfig(agent_id="my-agent"))
        await agent.run()
    """

    def __init__(
        self,
        config: AgentConfig,
        client_config: Optional[ClientConfig] = None,
    ):
        self.config = config
        self.client_config = client_config
        self._client: Optional[EngineClient] = None

    @property
    def client(self) -> EngineClient:
        if not self._client:
            raise RuntimeError("Agent not running")
        return self._client

    @property
    def state(self) -> AgentState:
        return self.client.state

    async def on_tick(self, tick: Tick) -> Any:
        """Override to handle ticks. Return value is sent as result."""
        return None

    async def on_signal(self, signal: Signal) -> None:
        """Override to handle signals."""
        pass

    async def on_error(self, error: str) -> None:
        """Override to handle errors."""
        logger.error(f"Agent error: {error}")

    async def on_start(self) -> None:
        """Override for startup logic."""
        pass

    async def on_stop(self) -> None:
        """Override for cleanup logic."""
        pass

    async def run(self) -> None:
        """Run the agent."""
        self._client = EngineClient(self.config, self.client_config)
        self._client.on_tick(self.on_tick)
        self._client.on_signal(self.on_signal)
        self._client.on_error(self.on_error)

        try:
            async with self._client:
                await self.on_start()

                async for message in self._client:
                    # Handle any messages not caught by callbacks
                    if message.type == MessageType.PRESENCE:
                        logger.debug(f"Presence update: {len(message.presence or [])} agents")

        finally:
            await self.on_stop()
            self._client = None

    async def invoke_tool(self, tool_id: str, params: Dict[str, Any]) -> Any:
        """Invoke a tool via the Engine."""
        return await self.client.request_tool(tool_id, params)

    async def checkpoint(self) -> str:
        """Request a state checkpoint."""
        return await self.client.request_checkpoint()
