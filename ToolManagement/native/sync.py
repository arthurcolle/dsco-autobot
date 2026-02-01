"""
Native MCP Sync - WebSocket synchronization with the central registry.

Provides bidirectional communication for:
- Tool registration/updates
- Health status reporting
- Event broadcasting
- Command reception
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class SyncStatus(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"


@dataclass
class SyncState:
    status: SyncStatus = SyncStatus.DISCONNECTED
    connected_at: Optional[datetime] = None
    last_message_at: Optional[datetime] = None
    reconnect_attempts: int = 0
    error: Optional[str] = None


class RegistrySync:
    """
    Maintains WebSocket connection to the central registry for synchronization.

    Features:
    - Auto-reconnection with exponential backoff
    - Heartbeat keep-alive
    - Message queuing during disconnection
    - Event subscription
    """

    def __init__(
        self,
        mcp_id: str,
        mcp_name: str,
        port: int,
        registry_url: str = "http://localhost:2016",
        ws_path: str = "/api/v1/native/sync",
    ):
        self.mcp_id = mcp_id
        self.mcp_name = mcp_name
        self.port = port
        self.registry_url = registry_url.rstrip("/")
        self.ws_url = self.registry_url.replace("http://", "ws://").replace("https://", "wss://") + ws_path

        self._state = SyncState()
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._message_handlers: Dict[str, Callable] = {}
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._running = False

        self._receive_task: Optional[asyncio.Task] = None
        self._send_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None

        # Tools to sync
        self._tools: List[Dict[str, Any]] = []
        self._capabilities: List[str] = []

    def set_tools(self, tools: List[Dict[str, Any]]):
        """Set the tools to advertise to the registry."""
        self._tools = tools

    def set_capabilities(self, capabilities: List[str]):
        """Set the capabilities to advertise."""
        self._capabilities = capabilities

    def on_message(self, msg_type: str, handler: Callable):
        """Register a handler for a specific message type."""
        self._message_handlers[msg_type] = handler

    async def start(self):
        """Start the sync connection."""
        if self._running:
            return

        self._running = True
        logger.info(f"Starting registry sync for {self.mcp_name}...")

        # Start connection loop
        self._reconnect_task = asyncio.create_task(self._connection_loop())

    async def stop(self):
        """Stop the sync connection."""
        self._running = False

        # Cancel tasks
        for task in [self._receive_task, self._send_task, self._heartbeat_task, self._reconnect_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Close websocket
        if self._websocket:
            await self._websocket.close()
            self._websocket = None

        self._state.status = SyncStatus.DISCONNECTED
        logger.info(f"Registry sync stopped for {self.mcp_name}")

    async def _connection_loop(self):
        """Main connection loop with reconnection logic."""
        while self._running:
            try:
                self._state.status = SyncStatus.CONNECTING
                logger.info(f"Connecting to registry at {self.ws_url}...")

                async with websockets.connect(
                    self.ws_url,
                    extra_headers={"X-MCP-ID": self.mcp_id},
                    ping_interval=20,
                    ping_timeout=10,
                ) as websocket:
                    self._websocket = websocket
                    self._state.status = SyncStatus.CONNECTED
                    self._state.connected_at = datetime.now(timezone.utc)
                    self._state.reconnect_attempts = 0
                    self._state.error = None

                    logger.info(f"✓ Connected to registry")

                    # Send registration
                    await self._send_registration()

                    # Start tasks
                    self._receive_task = asyncio.create_task(self._receive_loop())
                    self._send_task = asyncio.create_task(self._send_loop())
                    self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                    # Wait for tasks
                    await asyncio.gather(
                        self._receive_task,
                        self._send_task,
                        self._heartbeat_task,
                        return_exceptions=True,
                    )

            except ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                self._state.error = str(e)

            # Reconnect logic
            if self._running:
                self._state.status = SyncStatus.RECONNECTING
                self._state.reconnect_attempts += 1

                # Exponential backoff: 1s, 2s, 4s, 8s, max 30s
                delay = min(30, 2 ** self._state.reconnect_attempts)
                logger.info(f"Reconnecting in {delay}s (attempt {self._state.reconnect_attempts})...")
                await asyncio.sleep(delay)

    async def _send_registration(self):
        """Send registration message to registry."""
        await self.send({
            "type": "register",
            "mcp_id": self.mcp_id,
            "mcp_name": self.mcp_name,
            "port": self.port,
            "url": f"http://localhost:{self.port}",
            "tools": self._tools,
            "capabilities": self._capabilities,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def _receive_loop(self):
        """Receive messages from the registry."""
        while self._running and self._websocket:
            try:
                message = await self._websocket.recv()
                data = json.loads(message)
                self._state.last_message_at = datetime.now(timezone.utc)

                msg_type = data.get("type")
                if msg_type in self._message_handlers:
                    try:
                        await self._message_handlers[msg_type](data)
                    except Exception as e:
                        logger.error(f"Handler error for {msg_type}: {e}")
                else:
                    logger.debug(f"Unhandled message type: {msg_type}")

            except ConnectionClosed:
                break
            except Exception as e:
                logger.error(f"Receive error: {e}")
                break

    async def _send_loop(self):
        """Send queued messages to the registry."""
        while self._running and self._websocket:
            try:
                message = await asyncio.wait_for(
                    self._message_queue.get(),
                    timeout=1.0,
                )
                await self._websocket.send(json.dumps(message))
            except asyncio.TimeoutError:
                continue
            except ConnectionClosed:
                # Re-queue the message
                await self._message_queue.put(message)
                break
            except Exception as e:
                logger.error(f"Send error: {e}")
                break

    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self._running and self._websocket:
            await asyncio.sleep(30)
            await self.send({
                "type": "heartbeat",
                "mcp_id": self.mcp_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    async def send(self, message: Dict[str, Any]):
        """Queue a message for sending."""
        await self._message_queue.put(message)

    async def send_tools_update(self, tools: List[Dict[str, Any]]):
        """Send tools update to registry."""
        self._tools = tools
        await self.send({
            "type": "tools_update",
            "mcp_id": self.mcp_id,
            "tools": tools,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def send_event(self, event_type: str, data: Dict[str, Any]):
        """Send an event to the registry."""
        await self.send({
            "type": "event",
            "mcp_id": self.mcp_id,
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def send_status(self, status: str, details: Optional[Dict[str, Any]] = None):
        """Send status update to registry."""
        await self.send({
            "type": "status_update",
            "mcp_id": self.mcp_id,
            "status": status,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    @property
    def is_connected(self) -> bool:
        return self._state.status == SyncStatus.CONNECTED

    def get_state(self) -> Dict[str, Any]:
        return {
            "status": self._state.status.value,
            "connected_at": self._state.connected_at.isoformat() if self._state.connected_at else None,
            "last_message_at": self._state.last_message_at.isoformat() if self._state.last_message_at else None,
            "reconnect_attempts": self._state.reconnect_attempts,
            "error": self._state.error,
        }


# Factory function for easy integration
def create_registry_sync(
    mcp_id: str,
    mcp_name: str,
    port: int,
    registry_url: Optional[str] = None,
) -> RegistrySync:
    """Create a RegistrySync instance for a native MCP server."""
    import os
    registry_url = registry_url or os.getenv("REGISTRY_URL", "http://localhost:2016")
    return RegistrySync(
        mcp_id=mcp_id,
        mcp_name=mcp_name,
        port=port,
        registry_url=registry_url,
    )
