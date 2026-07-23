"""
Engine API Connection Management

Handles WebSocket/gRPC connections from agents.
Each agent maintains a single persistent connection to the Engine.
"""

import asyncio
import logging
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, Set, Callable, Awaitable
from weakref import WeakValueDictionary
import uuid

from dsco_shared.config.service_registry import ServiceName, resolve

from .models import (
    Tick,
    Signal,
    SignalType,
    AgentStatus,
    AgentState,
    AgentConfig,
    AgentPresence,
    Task,
    StateDelta,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Message Types
# ============================================================================

@dataclass
class EngineMessage:
    """Message from Engine to Agent"""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    message_type: str = "unknown"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    payload: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "type": self.message_type,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


@dataclass
class AgentMessage:
    """Message from Agent to Engine"""
    message_id: str = ""
    message_type: str = "unknown"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    payload: Any = None

    # For responses
    in_reply_to: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentMessage":
        return cls(
            message_id=data.get("message_id", str(uuid.uuid4())),
            message_type=data.get("type", "unknown"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if "timestamp" in data else datetime.utcnow(),
            payload=data.get("payload"),
            in_reply_to=data.get("in_reply_to"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "AgentMessage":
        return cls.from_dict(json.loads(json_str))


# ============================================================================
# Agent Connection
# ============================================================================

class AgentConnection:
    """
    Represents a single agent's connection to the Engine.

    Manages:
    - Bidirectional message stream
    - Tick delivery
    - Signal handling
    - State synchronization
    - Heartbeat/liveness
    """

    def __init__(
        self,
        agent_id: str,
        config: AgentConfig,
        send_func: Callable[[str], Awaitable[None]],
        on_disconnect: Optional[Callable[[], Awaitable[None]]] = None,
    ):
        self.agent_id = agent_id
        self.config = config
        self._send = send_func
        self._on_disconnect = on_disconnect

        # State
        self.state = AgentState(agent_id=agent_id)
        self.state.capabilities = config.capabilities
        self.state.status = AgentStatus.INITIALIZING

        # Presence
        self.presence = AgentPresence(
            agent_id=agent_id,
            agent_type=config.agent_type,
            status=AgentStatus.INITIALIZING,
            connected=True,
            connected_at=datetime.utcnow(),
            capabilities=config.capabilities,
        )

        # Message tracking
        self._pending_acks: Dict[str, asyncio.Future] = {}
        self._tick_sequence = 0
        self._last_tick_sent: Optional[datetime] = None
        self._last_tick_ack: Optional[datetime] = None
        self._last_message_received: datetime = datetime.utcnow()

        # Connection state
        self._connected = True
        self._closed = False

        # Callbacks
        self._message_handlers: Dict[str, Callable[[AgentMessage], Awaitable[None]]] = {}

        logger.info(f"Agent {agent_id} connected")

    @property
    def connected(self) -> bool:
        return self._connected and not self._closed

    async def send_tick(self, tick: Tick) -> bool:
        """Send a tick to the agent."""
        if not self.connected:
            return False

        tick.sequence = self._tick_sequence
        self._tick_sequence += 1

        message = EngineMessage(
            message_type="tick",
            payload=tick.to_dict(),
        )

        try:
            await self._send(message.to_json())
            self._last_tick_sent = datetime.utcnow()
            self.state.ticks_received += 1
            logger.debug(f"Sent tick {tick.tick_id} to agent {self.agent_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send tick to agent {self.agent_id}: {e}")
            await self._handle_disconnect()
            return False

    async def send_signal(self, signal: Signal) -> bool:
        """Send a signal to the agent."""
        if not self.connected:
            return False

        message = EngineMessage(
            message_type="signal",
            payload=signal.to_dict(),
        )

        try:
            await self._send(message.to_json())
            self.state.signals_received += 1
            logger.debug(f"Sent signal {signal.signal_type} to agent {self.agent_id}")

            if signal.require_ack:
                # Wait for acknowledgment
                future = asyncio.Future()
                self._pending_acks[signal.signal_id] = future
                try:
                    await asyncio.wait_for(future, timeout=signal.ack_timeout_ms / 1000)
                    return True
                except asyncio.TimeoutError:
                    logger.warning(f"Signal {signal.signal_id} ack timeout from agent {self.agent_id}")
                    return False
                finally:
                    self._pending_acks.pop(signal.signal_id, None)

            return True
        except Exception as e:
            logger.error(f"Failed to send signal to agent {self.agent_id}: {e}")
            await self._handle_disconnect()
            return False

    async def send_state_delta(self, delta: StateDelta) -> bool:
        """Send a state delta to the agent."""
        if not self.connected:
            return False

        message = EngineMessage(
            message_type="state_delta",
            payload=delta.to_dict(),
        )

        try:
            await self._send(message.to_json())
            logger.debug(f"Sent state delta to agent {self.agent_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send state delta to agent {self.agent_id}: {e}")
            return False

    async def send_heartbeat(self) -> bool:
        """Send a heartbeat to the agent."""
        if not self.connected:
            return False

        message = EngineMessage(
            message_type="heartbeat",
            payload={"timestamp": time.time_ns()},
        )

        try:
            await self._send(message.to_json())
            return True
        except Exception as e:
            logger.error(f"Failed to send heartbeat to agent {self.agent_id}: {e}")
            await self._handle_disconnect()
            return False

    async def send_presence(self, agents: list[AgentPresence]) -> bool:
        """Send presence information to the agent."""
        if not self.connected:
            return False

        message = EngineMessage(
            message_type="presence",
            payload=[a.to_dict() for a in agents],
        )

        try:
            await self._send(message.to_json())
            return True
        except Exception as e:
            logger.error(f"Failed to send presence to agent {self.agent_id}: {e}")
            return False

    async def handle_message(self, raw_message: str) -> None:
        """Handle an incoming message from the agent."""
        self._last_message_received = datetime.utcnow()
        self.presence.last_seen = datetime.utcnow()

        try:
            message = AgentMessage.from_json(raw_message)
        except Exception as e:
            logger.error(f"Failed to parse message from agent {self.agent_id}: {e}")
            return

        logger.debug(f"Received {message.message_type} from agent {self.agent_id}")

        # Handle acknowledgments
        if message.message_type == "ack":
            if message.in_reply_to and message.in_reply_to in self._pending_acks:
                self._pending_acks[message.in_reply_to].set_result(True)
            return

        # Handle specific message types
        if message.message_type == "result":
            await self._handle_result(message)
        elif message.message_type == "state_mutation":
            await self._handle_state_mutation(message)
        elif message.message_type == "tool_request":
            await self._handle_tool_request(message)
        elif message.message_type == "checkpoint_request":
            await self._handle_checkpoint_request(message)
        elif message.message_type == "error":
            await self._handle_error(message)

        # Call registered handlers
        if message.message_type in self._message_handlers:
            await self._message_handlers[message.message_type](message)

    async def _handle_result(self, message: AgentMessage) -> None:
        """Handle a task result from the agent."""
        payload = message.payload or {}
        task_id = payload.get("task_id")
        result = payload.get("result")
        error = payload.get("error")

        logger.info(f"Agent {self.agent_id} completed task {task_id}")

        if error:
            self.state.tasks_failed += 1
        else:
            self.state.tasks_completed += 1

        self.state.current_task_id = None
        self.state.current_task_updated_at = datetime.utcnow()
        self.state.status = AgentStatus.IDLE
        self.state.status_updated_at = datetime.utcnow()

        self.presence.status = AgentStatus.IDLE
        self.presence.current_task_id = None

    async def _handle_state_mutation(self, message: AgentMessage) -> None:
        """Handle a state mutation from the agent."""
        payload = message.payload or {}

        delta = StateDelta(
            agent_id=self.agent_id,
            changes=payload.get("changes", {}),
            change_types=payload.get("change_types", {}),
        )

        try:
            self.state = self.state.apply_delta(delta)
            logger.debug(f"Applied state mutation from agent {self.agent_id}")
        except ValueError as e:
            logger.warning(f"Failed to apply state mutation: {e}")

    async def _handle_tool_request(self, message: AgentMessage) -> None:
        """
        Handle a tool execution request from the agent.

        Routes the request to the Tool Gateway service for execution
        and sends the result back to the agent.
        """
        import httpx
        import os

        payload = message.payload or {}
        tool_id = payload.get("tool_id")
        tool_name = payload.get("tool_name")
        params = payload.get("params", {})
        timeout_ms = payload.get("timeout_ms", 30000)

        logger.info(f"Agent {self.agent_id} requesting tool execution: {tool_id or tool_name}")

        # Get Tool Gateway URL from config or environment
        tool_gateway_url = os.getenv("TOOL_GATEWAY_URL") or resolve(ServiceName.TOOL_GATEWAY)

        try:
            async with httpx.AsyncClient(timeout=timeout_ms / 1000) as client:
                # Execute tool via Tool Gateway
                response = await client.post(
                    f"{tool_gateway_url}/tools/{tool_name or tool_id}/execute",
                    json={"inputs": params}
                )
                response.raise_for_status()
                result = response.json()

                # Send result back to agent
                await self._send(EngineMessage(
                    message_type="tool_result",
                    payload={
                        "tool_id": tool_id,
                        "tool_name": tool_name,
                        "status": result.get("status", "success"),
                        "output": result.get("output"),
                        "error": result.get("error"),
                        "in_reply_to": message.message_id,
                    }
                ).to_json())

                logger.info(f"Tool execution completed for agent {self.agent_id}: {tool_id or tool_name}")

        except httpx.HTTPStatusError as e:
            error_msg = f"Tool execution failed: HTTP {e.response.status_code}"
            logger.error(f"Tool execution failed for agent {self.agent_id}: {error_msg}")
            await self._send(EngineMessage(
                message_type="tool_result",
                payload={
                    "tool_id": tool_id,
                    "tool_name": tool_name,
                    "status": "error",
                    "error": error_msg,
                    "in_reply_to": message.message_id,
                }
            ).to_json())
        except Exception as e:
            error_msg = f"Tool execution error: {str(e)}"
            logger.error(f"Tool execution error for agent {self.agent_id}: {error_msg}")
            await self._send(EngineMessage(
                message_type="tool_result",
                payload={
                    "tool_id": tool_id,
                    "tool_name": tool_name,
                    "status": "error",
                    "error": error_msg,
                    "in_reply_to": message.message_id,
                }
            ).to_json())

    async def _handle_checkpoint_request(self, message: AgentMessage) -> None:
        """
        Handle a checkpoint request from the agent.

        Saves the agent state to both Redis (cache) and persistent storage.
        """
        import httpx
        import os

        logger.info(f"Agent {self.agent_id} requesting checkpoint")

        payload = message.payload or {}
        reason = payload.get("reason", "manual")
        metadata = payload.get("metadata", {})

        # Get current state
        state_dict = self.state.to_dict()

        # Store checkpoint via Engine API (which handles Redis + persistent storage)
        engine_api_url = os.getenv("ENGINE_API_URL") or resolve(ServiceName.ENGINE)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{engine_api_url}/agents/{self.agent_id}/checkpoint",
                    json={
                        "state": state_dict,
                        "metadata": {
                            "reason": reason,
                            **metadata
                        }
                    }
                )
                response.raise_for_status()
                result = response.json()
                checkpoint_id = result.get("checkpoint_id")

                # Send confirmation to agent
                await self._send(EngineMessage(
                    message_type="checkpoint_result",
                    payload={
                        "status": "success",
                        "checkpoint_id": checkpoint_id,
                        "in_reply_to": message.message_id,
                    }
                ).to_json())

                logger.info(f"Checkpoint created for agent {self.agent_id}: {checkpoint_id}")

        except Exception as e:
            error_msg = f"Checkpoint failed: {str(e)}"
            logger.error(f"Checkpoint failed for agent {self.agent_id}: {error_msg}")
            await self._send(EngineMessage(
                message_type="checkpoint_result",
                payload={
                    "status": "error",
                    "error": error_msg,
                    "in_reply_to": message.message_id,
                }
            ).to_json())

    async def _handle_error(self, message: AgentMessage) -> None:
        """Handle an error report from the agent."""
        payload = message.payload or {}
        error = payload.get("error")
        context = payload.get("context")

        logger.error(f"Agent {self.agent_id} reported error: {error} (context: {context})")

        self.state.status = AgentStatus.ERROR
        self.state.status_updated_at = datetime.utcnow()
        self.presence.status = AgentStatus.ERROR

    async def _handle_disconnect(self) -> None:
        """Handle agent disconnection."""
        if self._closed:
            return

        self._connected = False
        self.presence.connected = False

        logger.info(f"Agent {self.agent_id} disconnected")

        if self._on_disconnect:
            await self._on_disconnect()

    async def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return

        self._closed = True
        self._connected = False
        self.presence.connected = False

        # Cancel pending acks
        for future in self._pending_acks.values():
            future.cancel()
        self._pending_acks.clear()

        logger.info(f"Agent {self.agent_id} connection closed")

    def register_handler(
        self,
        message_type: str,
        handler: Callable[[AgentMessage], Awaitable[None]]
    ) -> None:
        """Register a handler for a message type."""
        self._message_handlers[message_type] = handler


# ============================================================================
# Connection Manager
# ============================================================================

class ConnectionManager:
    """
    Manages all agent connections.

    Provides:
    - Connection lifecycle management
    - Broadcast capabilities
    - Presence tracking
    - Connection health monitoring
    """

    def __init__(self):
        self._connections: Dict[str, AgentConnection] = {}
        self._presence: Dict[str, AgentPresence] = {}
        self._lock = asyncio.Lock()

        # Background tasks
        self._health_check_task: Optional[asyncio.Task] = None
        self._presence_broadcast_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the connection manager."""
        if self._running:
            return

        self._running = True

        # Start background tasks
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._presence_broadcast_task = asyncio.create_task(self._presence_broadcast_loop())

        logger.info("ConnectionManager started")

    async def stop(self) -> None:
        """Stop the connection manager."""
        self._running = False

        if self._health_check_task:
            self._health_check_task.cancel()
        if self._presence_broadcast_task:
            self._presence_broadcast_task.cancel()

        # Close all connections
        async with self._lock:
            for conn in self._connections.values():
                await conn.close()
            self._connections.clear()
            self._presence.clear()

        logger.info("ConnectionManager stopped")

    async def register(
        self,
        agent_id: str,
        config: AgentConfig,
        send_func: Callable[[str], Awaitable[None]],
    ) -> AgentConnection:
        """Register a new agent connection."""
        async with self._lock:
            # Close existing connection if any
            if agent_id in self._connections:
                await self._connections[agent_id].close()

            # Create new connection
            async def on_disconnect():
                await self.unregister(agent_id)

            conn = AgentConnection(
                agent_id=agent_id,
                config=config,
                send_func=send_func,
                on_disconnect=on_disconnect,
            )

            self._connections[agent_id] = conn
            self._presence[agent_id] = conn.presence

            logger.info(f"Registered agent {agent_id} (total: {len(self._connections)})")

            return conn

    async def unregister(self, agent_id: str) -> None:
        """Unregister an agent connection."""
        async with self._lock:
            if agent_id in self._connections:
                await self._connections[agent_id].close()
                del self._connections[agent_id]

            if agent_id in self._presence:
                self._presence[agent_id].connected = False

            logger.info(f"Unregistered agent {agent_id} (total: {len(self._connections)})")

    def get(self, agent_id: str) -> Optional[AgentConnection]:
        """Get a connection by agent ID."""
        return self._connections.get(agent_id)

    def get_all(self) -> list[AgentConnection]:
        """Get all active connections."""
        return list(self._connections.values())

    def get_presence(self) -> list[AgentPresence]:
        """Get presence info for all agents."""
        return list(self._presence.values())

    async def broadcast_tick(self, tick: Tick) -> int:
        """Broadcast a tick to all connected agents."""
        count = 0
        for conn in self._connections.values():
            if await conn.send_tick(tick):
                count += 1
        return count

    async def broadcast_signal(self, signal: Signal) -> int:
        """Broadcast a signal to all connected agents."""
        count = 0
        for conn in self._connections.values():
            if await conn.send_signal(signal):
                count += 1
        return count

    async def broadcast_state_delta(self, delta: StateDelta) -> int:
        """Broadcast a state delta to all connected agents."""
        count = 0
        for conn in self._connections.values():
            if await conn.send_state_delta(delta):
                count += 1
        return count

    async def send_to(self, agent_id: str, message_type: str, payload: Any) -> bool:
        """Send a message to a specific agent."""
        conn = self.get(agent_id)
        if not conn:
            return False

        message = EngineMessage(
            message_type=message_type,
            payload=payload,
        )

        try:
            await conn._send(message.to_json())
            return True
        except Exception:
            return False

    async def _health_check_loop(self) -> None:
        """Periodically check connection health."""
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                now = datetime.utcnow()
                stale_timeout = 60  # Seconds

                async with self._lock:
                    stale_agents = []
                    for agent_id, conn in self._connections.items():
                        age = (now - conn._last_message_received).total_seconds()
                        if age > stale_timeout:
                            stale_agents.append(agent_id)

                # Close stale connections
                for agent_id in stale_agents:
                    logger.warning(f"Agent {agent_id} connection stale, closing")
                    await self.unregister(agent_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")

    async def _presence_broadcast_loop(self) -> None:
        """Periodically broadcast presence to all agents."""
        while self._running:
            try:
                await asyncio.sleep(10)  # Broadcast every 10 seconds

                presence_list = self.get_presence()
                for conn in self._connections.values():
                    await conn.send_presence(presence_list)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Presence broadcast error: {e}")

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    def get_stats(self) -> Dict[str, Any]:
        """Get connection manager statistics."""
        by_status = {}
        for conn in self._connections.values():
            status = conn.state.status.value
            by_status[status] = by_status.get(status, 0) + 1

        return {
            "total_connections": len(self._connections),
            "by_status": by_status,
            "total_presence": len(self._presence),
        }
