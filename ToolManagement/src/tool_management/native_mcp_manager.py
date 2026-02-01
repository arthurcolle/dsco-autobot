"""
Native MCP Manager - Orchestrates native MCP server lifecycle.

Handles:
- Starting native MCP servers as subprocesses
- WebSocket synchronization channels
- Health monitoring and auto-restart
- Tool discovery and registration with the central registry
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import httpx
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class NativeMCPStatus(str, Enum):
    """Status of a native MCP server."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    UNHEALTHY = "unhealthy"
    FAILED = "failed"
    STOPPING = "stopping"


@dataclass
class NativeMCPConfig:
    """Configuration for a native MCP server."""
    id: str
    name: str
    module: str
    port: int
    capabilities: List[str]
    env_vars: Dict[str, str] = field(default_factory=dict)
    health_endpoint: str = "/health"
    mcp_endpoint: str = "/mcp"
    tools_endpoint: str = "/tools"
    ws_endpoint: str = "/sse"  # SSE endpoint for now, will add WS
    auto_restart: bool = True
    restart_delay_seconds: float = 5.0
    health_check_interval_seconds: float = 30.0


@dataclass
class NativeMCPState:
    """Runtime state of a native MCP server."""
    config: NativeMCPConfig
    status: NativeMCPStatus = NativeMCPStatus.STOPPED
    process: Optional[subprocess.Popen] = None
    started_at: Optional[datetime] = None
    last_health_check: Optional[datetime] = None
    health_check_failures: int = 0
    tools: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


# Default native MCP configurations
NATIVE_MCPS = {
    "distributed-search-mcp": NativeMCPConfig(
        id="distributed-search-mcp",
        name="Distributed Search MCP",
        module="native.distributed_search_mcp.server",
        port=4001,
        capabilities=["search", "read", "verify", "rerank", "embed", "web_search", "content_extraction"],
        env_vars={"JINA_API_KEY": os.getenv("JINA_API_KEY", "")},
    ),
    "distributed-research-mcp": NativeMCPConfig(
        id="distributed-research-mcp",
        name="Distributed Research MCP",
        module="native.distributed_research_mcp.server",
        port=4002,
        capabilities=["deep_search", "run_task", "extract_data", "create_task_group", "find_all", "parallel_execution", "task_management"],
        env_vars={"PARALLEL_API_KEY": os.getenv("PARALLEL_API_KEY", "")},
    ),
    "distributed-publish-mcp": NativeMCPConfig(
        id="distributed-publish-mcp",
        name="Distributed Publish MCP",
        module="native.distributed_publish_mcp.server",
        port=4003,
        capabilities=["webhooks", "events", "notifications", "syndication", "message_queue", "content_publishing"],
        env_vars={},
    ),
    "distributed-memory-mcp": NativeMCPConfig(
        id="distributed-memory-mcp",
        name="Distributed Memory MCP",
        module="native.distributed_memory_mcp.server",
        port=4004,
        capabilities=[
            "working_memory", "episodic_memory", "semantic_memory", "procedural_memory",
            "focus", "recall", "consolidate", "learn_skill", "query_beliefs", "cognitive_architecture"
        ],
        env_vars={"MEMORY_DATA_DIR": os.getenv("MEMORY_DATA_DIR", "data/memory")},
    ),
}


class NativeMCPSyncChannel:
    """
    WebSocket-based synchronization channel for native MCPs.

    Handles bidirectional communication between registry and native MCPs:
    - Tool discovery and registration
    - Health status updates
    - Event broadcasting
    - Command dispatch
    """

    def __init__(self, manager: "NativeMCPManager"):
        self.manager = manager
        self.connections: Dict[str, WebSocket] = {}  # mcp_id -> WebSocket
        self._subscriptions: Dict[str, Set[str]] = {}  # event_type -> set of mcp_ids
        self._message_handlers: Dict[str, Callable] = {}
        self._register_handlers()

    def _register_handlers(self):
        """Register message handlers."""
        self._message_handlers = {
            "register": self._handle_register,
            "heartbeat": self._handle_heartbeat,
            "tools_update": self._handle_tools_update,
            "event": self._handle_event,
            "status_update": self._handle_status_update,
        }

    async def connect(self, websocket: WebSocket, mcp_id: str):
        """Accept a WebSocket connection from a native MCP."""
        await websocket.accept()
        self.connections[mcp_id] = websocket
        logger.info(f"Native MCP connected via WebSocket: {mcp_id}")

        # Send initial sync
        await self.send_to_mcp(mcp_id, {
            "type": "sync_request",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def disconnect(self, mcp_id: str):
        """Handle disconnection."""
        if mcp_id in self.connections:
            del self.connections[mcp_id]
        for subs in self._subscriptions.values():
            subs.discard(mcp_id)
        logger.info(f"Native MCP disconnected: {mcp_id}")

    async def send_to_mcp(self, mcp_id: str, message: Dict[str, Any]):
        """Send a message to a specific native MCP."""
        ws = self.connections.get(mcp_id)
        if ws:
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.error(f"Failed to send to {mcp_id}: {e}")
                await self.disconnect(mcp_id)

    async def broadcast(self, message: Dict[str, Any], event_type: Optional[str] = None):
        """Broadcast a message to all connected MCPs (or subscribers)."""
        targets = self.connections.keys()
        if event_type and event_type in self._subscriptions:
            targets = self._subscriptions[event_type]

        for mcp_id in list(targets):
            await self.send_to_mcp(mcp_id, message)

    async def handle_message(self, mcp_id: str, message: Dict[str, Any]):
        """Handle an incoming message from a native MCP."""
        msg_type = message.get("type")
        handler = self._message_handlers.get(msg_type)

        if handler:
            await handler(mcp_id, message)
        else:
            logger.warning(f"Unknown message type from {mcp_id}: {msg_type}")

    async def _handle_register(self, mcp_id: str, message: Dict[str, Any]):
        """Handle MCP registration message."""
        tools = message.get("tools", [])
        capabilities = message.get("capabilities", [])

        # Update manager state
        if mcp_id in self.manager.servers:
            state = self.manager.servers[mcp_id]
            state.tools = tools
            state.status = NativeMCPStatus.RUNNING

        logger.info(f"Native MCP registered: {mcp_id} with {len(tools)} tools")

        # Acknowledge
        await self.send_to_mcp(mcp_id, {
            "type": "register_ack",
            "status": "accepted",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def _handle_heartbeat(self, mcp_id: str, message: Dict[str, Any]):
        """Handle heartbeat message."""
        if mcp_id in self.manager.servers:
            state = self.manager.servers[mcp_id]
            state.last_health_check = datetime.now(timezone.utc)
            state.health_check_failures = 0

    async def _handle_tools_update(self, mcp_id: str, message: Dict[str, Any]):
        """Handle tools update message."""
        tools = message.get("tools", [])
        if mcp_id in self.manager.servers:
            self.manager.servers[mcp_id].tools = tools
            logger.info(f"Tools updated for {mcp_id}: {len(tools)} tools")

    async def _handle_event(self, mcp_id: str, message: Dict[str, Any]):
        """Handle event from native MCP."""
        event_type = message.get("event_type")
        event_data = message.get("data", {})

        # Forward to registry event handlers
        await self.manager._dispatch_event(mcp_id, event_type, event_data)

    async def _handle_status_update(self, mcp_id: str, message: Dict[str, Any]):
        """Handle status update from native MCP."""
        status = message.get("status")
        if mcp_id in self.manager.servers and status:
            try:
                self.manager.servers[mcp_id].status = NativeMCPStatus(status)
            except ValueError:
                pass

    def subscribe(self, mcp_id: str, event_types: List[str]):
        """Subscribe an MCP to specific event types."""
        for event_type in event_types:
            if event_type not in self._subscriptions:
                self._subscriptions[event_type] = set()
            self._subscriptions[event_type].add(mcp_id)


class NativeMCPManager:
    """
    Manages lifecycle of native MCP servers.

    Features:
    - Start/stop native MCPs as subprocesses
    - Health monitoring with auto-restart
    - Tool discovery and registration
    - WebSocket synchronization
    """

    def __init__(
        self,
        registry_url: str = "http://localhost:2016",
        project_root: Optional[Path] = None,
    ):
        self.registry_url = registry_url
        self.project_root = project_root or Path(__file__).parent.parent.parent
        self.servers: Dict[str, NativeMCPState] = {}
        self.sync_channel = NativeMCPSyncChannel(self)
        self._health_task: Optional[asyncio.Task] = None
        self._event_handlers: Dict[str, List[Callable]] = {}
        self._client: Optional[httpx.AsyncClient] = None
        self._running = False

    async def start(self):
        """Start the manager and all configured native MCPs."""
        logger.info("Starting Native MCP Manager...")
        self._running = True
        self._client = httpx.AsyncClient(timeout=30.0)

        # Initialize server states
        for mcp_id, config in NATIVE_MCPS.items():
            self.servers[mcp_id] = NativeMCPState(config=config)

        # Start all native MCPs
        for mcp_id in self.servers:
            await self.start_server(mcp_id)

        # Start health monitoring
        self._health_task = asyncio.create_task(self._health_monitor_loop())

        logger.info(f"Native MCP Manager started with {len(self.servers)} servers")

    async def stop(self):
        """Stop the manager and all native MCPs."""
        logger.info("Stopping Native MCP Manager...")
        self._running = False

        # Stop health monitoring
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        # Stop all servers
        for mcp_id in list(self.servers.keys()):
            await self.stop_server(mcp_id)

        # Close HTTP client
        if self._client:
            await self._client.aclose()

        logger.info("Native MCP Manager stopped")

    async def start_server(self, mcp_id: str) -> bool:
        """Start a specific native MCP server."""
        state = self.servers.get(mcp_id)
        if not state:
            logger.error(f"Unknown MCP: {mcp_id}")
            return False

        if state.status in (NativeMCPStatus.RUNNING, NativeMCPStatus.STARTING):
            logger.warning(f"MCP {mcp_id} is already running/starting")
            return True

        config = state.config
        state.status = NativeMCPStatus.STARTING
        state.error = None

        # Build environment
        env = os.environ.copy()
        env["PORT"] = str(config.port)
        env["REGISTRY_URL"] = self.registry_url
        env["PYTHONPATH"] = f"{self.project_root}:{self.project_root}/src:{env.get('PYTHONPATH', '')}"
        env.update(config.env_vars)

        logger.info(f"Starting {config.name} on port {config.port}...")

        try:
            process = subprocess.Popen(
                [sys.executable, "-m", config.module],
                cwd=str(self.project_root),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            state.process = process
            state.started_at = datetime.now(timezone.utc)

            # Wait for health
            healthy = await self._wait_for_healthy(mcp_id, timeout=30.0)
            if healthy:
                state.status = NativeMCPStatus.RUNNING
                logger.info(f"✓ {config.name} started successfully")

                # Discover and register tools
                await self._discover_and_register_tools(mcp_id)
                return True
            else:
                state.status = NativeMCPStatus.FAILED
                state.error = "Health check timeout"
                logger.error(f"✗ {config.name} failed to become healthy")
                return False

        except Exception as e:
            state.status = NativeMCPStatus.FAILED
            state.error = str(e)
            logger.error(f"✗ Failed to start {config.name}: {e}")
            return False

    async def stop_server(self, mcp_id: str) -> bool:
        """Stop a specific native MCP server."""
        state = self.servers.get(mcp_id)
        if not state or not state.process:
            return True

        config = state.config
        state.status = NativeMCPStatus.STOPPING

        logger.info(f"Stopping {config.name}...")

        try:
            state.process.terminate()
            try:
                state.process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                state.process.kill()
                state.process.wait()

            state.process = None
            state.status = NativeMCPStatus.STOPPED
            logger.info(f"✓ {config.name} stopped")
            return True

        except Exception as e:
            logger.error(f"Error stopping {config.name}: {e}")
            return False

    async def restart_server(self, mcp_id: str) -> bool:
        """Restart a specific native MCP server."""
        await self.stop_server(mcp_id)
        await asyncio.sleep(1.0)
        return await self.start_server(mcp_id)

    async def _wait_for_healthy(self, mcp_id: str, timeout: float = 30.0) -> bool:
        """Wait for a server to become healthy."""
        state = self.servers.get(mcp_id)
        if not state:
            return False

        config = state.config
        url = f"http://localhost:{config.port}{config.health_endpoint}"

        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                response = await self._client.get(url, timeout=5.0)
                if response.status_code == 200:
                    return True
            except (httpx.ConnectError, httpx.TimeoutException):
                pass
            await asyncio.sleep(1.0)

        return False

    async def _discover_and_register_tools(self, mcp_id: str):
        """Discover tools from a native MCP and register with central registry."""
        state = self.servers.get(mcp_id)
        if not state:
            return

        config = state.config
        tools_url = f"http://localhost:{config.port}{config.tools_endpoint}"

        try:
            response = await self._client.get(tools_url, timeout=10.0)
            if response.status_code == 200:
                data = response.json()
                state.tools = data.get("tools", [])
                logger.info(f"Discovered {len(state.tools)} tools from {config.name}")

                # Register with central registry's backend system
                await self._register_as_backend(mcp_id)

        except Exception as e:
            logger.error(f"Failed to discover tools from {config.name}: {e}")

    async def _register_as_backend(self, mcp_id: str):
        """Register native MCP as a backend with the task queue."""
        state = self.servers.get(mcp_id)
        if not state:
            return

        config = state.config

        # This will be called by the registry to add the backend
        # The actual registration happens via the queue system
        backend_info = {
            "id": config.id,
            "name": config.name,
            "url": f"http://localhost:{config.port}",
            "health_endpoint": config.health_endpoint,
            "mcp_endpoint": config.mcp_endpoint,
            "capabilities": config.capabilities,
            "tools": [t.get("name") for t in state.tools],
            "max_concurrent": 10,
            "priority": 10,
            "native": True,
        }

        # Notify via sync channel
        await self.sync_channel.broadcast({
            "type": "backend_registered",
            "backend": backend_info,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return backend_info

    async def _health_monitor_loop(self):
        """Periodically check health of all native MCPs."""
        while self._running:
            await asyncio.sleep(30.0)

            for mcp_id, state in self.servers.items():
                if state.status != NativeMCPStatus.RUNNING:
                    continue

                config = state.config
                url = f"http://localhost:{config.port}{config.health_endpoint}"

                try:
                    response = await self._client.get(url, timeout=10.0)
                    if response.status_code == 200:
                        state.last_health_check = datetime.now(timezone.utc)
                        state.health_check_failures = 0
                    else:
                        state.health_check_failures += 1
                except Exception:
                    state.health_check_failures += 1

                # Auto-restart if unhealthy
                if state.health_check_failures >= 3:
                    state.status = NativeMCPStatus.UNHEALTHY
                    logger.warning(f"{config.name} is unhealthy, restarting...")

                    if config.auto_restart:
                        await asyncio.sleep(config.restart_delay_seconds)
                        await self.restart_server(mcp_id)

    async def _dispatch_event(self, source_mcp: str, event_type: str, data: Dict[str, Any]):
        """Dispatch an event from a native MCP to registered handlers."""
        handlers = self._event_handlers.get(event_type, [])
        for handler in handlers:
            try:
                await handler(source_mcp, event_type, data)
            except Exception as e:
                logger.error(f"Event handler error: {e}")

    def on_event(self, event_type: str, handler: Callable):
        """Register an event handler."""
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)

    def get_status(self) -> Dict[str, Any]:
        """Get status of all native MCPs."""
        return {
            "running": self._running,
            "servers": {
                mcp_id: {
                    "id": state.config.id,
                    "name": state.config.name,
                    "port": state.config.port,
                    "status": state.status.value,
                    "started_at": state.started_at.isoformat() if state.started_at else None,
                    "last_health_check": state.last_health_check.isoformat() if state.last_health_check else None,
                    "tools_count": len(state.tools),
                    "capabilities": state.config.capabilities,
                    "error": state.error,
                }
                for mcp_id, state in self.servers.items()
            },
            "sync_channel": {
                "connected": list(self.sync_channel.connections.keys()),
            },
        }

    def get_all_tools(self) -> List[Dict[str, Any]]:
        """Get all tools from all native MCPs."""
        tools = []
        for state in self.servers.values():
            for tool in state.tools:
                tool_copy = tool.copy()
                tool_copy["source_mcp"] = state.config.id
                tool_copy["source_url"] = f"http://localhost:{state.config.port}"
                tools.append(tool_copy)
        return tools


# Singleton instance
_manager: Optional[NativeMCPManager] = None


def get_native_mcp_manager() -> NativeMCPManager:
    """Get the singleton NativeMCPManager instance."""
    global _manager
    if _manager is None:
        _manager = NativeMCPManager()
    return _manager


async def init_native_mcp_manager(registry_url: str = "http://localhost:2016") -> NativeMCPManager:
    """Initialize and start the native MCP manager."""
    global _manager
    _manager = NativeMCPManager(registry_url=registry_url)
    await _manager.start()
    return _manager
