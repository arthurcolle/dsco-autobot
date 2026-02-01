#!/usr/bin/env python3
"""
Distributed Publish MCP - Native MCP Server

First-class MCP server providing:
- Webhook management (create, trigger, list)
- Event publishing and streaming
- Content syndication
- Notification dispatch
- Message queuing

This is a NATIVE MCP server - it implements the MCP protocol directly.

Run with:
    python -m native.distributed_publish_mcp
    # or
    uvicorn native.distributed_publish_mcp:app --port 4003

Environment:
    REGISTRY_URL: Central registry URL (optional)
    PORT: Server port (default: 4003)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# MODELS
# =============================================================================

class MCPProtocolVersion(str, Enum):
    V1 = "2024-11-05"


class ConnectionStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


class WebhookStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"


class EventType(str, Enum):
    WEBHOOK_TRIGGERED = "webhook.triggered"
    CONTENT_PUBLISHED = "content.published"
    NOTIFICATION_SENT = "notification.sent"
    MESSAGE_QUEUED = "message.queued"
    SYSTEM = "system"


@dataclass
class MCPTool:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Optional[Callable] = None


@dataclass
class ConnectionState:
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    connected_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    error_message: Optional[str] = None
    session_id: Optional[str] = None
    protocol_version: str = MCPProtocolVersion.V1.value


@dataclass
class Webhook:
    id: str
    name: str
    url: str
    events: List[str]
    secret: Optional[str] = None
    status: WebhookStatus = WebhookStatus.ACTIVE
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0
    failure_count: int = 0


@dataclass
class Event:
    id: str
    type: EventType
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "native-mcp"


@dataclass
class QueuedMessage:
    id: str
    channel: str
    payload: Dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    delivered: bool = False
    delivery_attempts: int = 0


class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)


class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


# =============================================================================
# NATIVE MCP SERVER
# =============================================================================

class DistributedPublishMCP:
    """
    Native MCP server for content publishing and event distribution.

    This is the third native MCP - it handles webhooks, events, and message
    distribution for the distributed systems platform.
    """

    SERVICE_ID = "distributed-publish-mcp"
    SERVICE_NAME = "Distributed Publish MCP"
    VERSION = "1.0.0"

    def __init__(
        self,
        port: int = 4003,
        registry_url: Optional[str] = None,
    ):
        self.port = port
        self.registry_url = registry_url or os.getenv("REGISTRY_URL")

        self._client: Optional[httpx.AsyncClient] = None
        self._tools: Dict[str, MCPTool] = {}
        self._state = ConnectionState()

        # Native storage
        self._webhooks: Dict[str, Webhook] = {}
        self._events: deque[Event] = deque(maxlen=1000)  # Last 1000 events
        self._message_queue: Dict[str, deque[QueuedMessage]] = {}  # channel -> messages
        self._subscribers: Dict[str, List[asyncio.Queue]] = {}  # channel -> subscriber queues

        # Register native tools
        self._register_tools()

    def _register_tools(self):
        """Register all native MCP tools."""

        # Webhook management
        self._tools["create_webhook"] = MCPTool(
            name="create_webhook",
            description="Create a new webhook endpoint to receive event notifications.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Webhook name"
                    },
                    "url": {
                        "type": "string",
                        "description": "Target URL to receive webhook payloads"
                    },
                    "events": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Event types to subscribe to",
                        "default": ["*"]
                    },
                    "secret": {
                        "type": "string",
                        "description": "Optional secret for payload signing"
                    }
                },
                "required": ["name", "url"]
            },
            handler=self._execute_create_webhook
        )

        self._tools["trigger_webhook"] = MCPTool(
            name="trigger_webhook",
            description="Manually trigger a webhook with custom payload.",
            input_schema={
                "type": "object",
                "properties": {
                    "webhook_id": {
                        "type": "string",
                        "description": "Webhook ID to trigger"
                    },
                    "payload": {
                        "type": "object",
                        "description": "Payload to send"
                    }
                },
                "required": ["webhook_id", "payload"]
            },
            handler=self._execute_trigger_webhook
        )

        self._tools["list_webhooks"] = MCPTool(
            name="list_webhooks",
            description="List all registered webhooks.",
            input_schema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["active", "paused", "failed", "all"],
                        "default": "all"
                    }
                }
            },
            handler=self._execute_list_webhooks
        )

        self._tools["delete_webhook"] = MCPTool(
            name="delete_webhook",
            description="Delete a webhook.",
            input_schema={
                "type": "object",
                "properties": {
                    "webhook_id": {
                        "type": "string",
                        "description": "Webhook ID to delete"
                    }
                },
                "required": ["webhook_id"]
            },
            handler=self._execute_delete_webhook
        )

        # Event publishing
        self._tools["publish_event"] = MCPTool(
            name="publish_event",
            description="Publish an event to all subscribers and matching webhooks.",
            input_schema={
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Event type (e.g., 'content.published')"
                    },
                    "data": {
                        "type": "object",
                        "description": "Event payload data"
                    },
                    "source": {
                        "type": "string",
                        "description": "Event source identifier"
                    }
                },
                "required": ["type", "data"]
            },
            handler=self._execute_publish_event
        )

        self._tools["list_events"] = MCPTool(
            name="list_events",
            description="List recent events.",
            input_schema={
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Filter by event type"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum events to return",
                        "default": 50
                    }
                }
            },
            handler=self._execute_list_events
        )

        # Message queue
        self._tools["queue_message"] = MCPTool(
            name="queue_message",
            description="Queue a message for delivery to a channel.",
            input_schema={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Channel name"
                    },
                    "payload": {
                        "type": "object",
                        "description": "Message payload"
                    }
                },
                "required": ["channel", "payload"]
            },
            handler=self._execute_queue_message
        )

        self._tools["get_queue_messages"] = MCPTool(
            name="get_queue_messages",
            description="Get messages from a queue channel.",
            input_schema={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "description": "Channel name"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 10
                    },
                    "acknowledge": {
                        "type": "boolean",
                        "description": "Mark messages as delivered",
                        "default": False
                    }
                },
                "required": ["channel"]
            },
            handler=self._execute_get_queue_messages
        )

        # Notifications
        self._tools["send_notification"] = MCPTool(
            name="send_notification",
            description="Send a notification to specified targets.",
            input_schema={
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Notification title"
                    },
                    "body": {
                        "type": "string",
                        "description": "Notification body"
                    },
                    "targets": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Target channels or webhook IDs"
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["low", "normal", "high", "urgent"],
                        "default": "normal"
                    }
                },
                "required": ["title", "body"]
            },
            handler=self._execute_send_notification
        )

        # Content syndication
        self._tools["syndicate_content"] = MCPTool(
            name="syndicate_content",
            description="Syndicate content to multiple channels/platforms.",
            input_schema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "object",
                        "description": "Content to syndicate",
                        "properties": {
                            "title": {"type": "string"},
                            "body": {"type": "string"},
                            "url": {"type": "string"},
                            "metadata": {"type": "object"}
                        }
                    },
                    "channels": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Target channels for syndication"
                    }
                },
                "required": ["content", "channels"]
            },
            handler=self._execute_syndicate_content
        )

        # Connectivity test
        self._tools["_connectivity_test"] = MCPTool(
            name="_connectivity_test",
            description="Internal connectivity test.",
            input_schema={
                "type": "object",
                "properties": {
                    "echo": {"type": "string"}
                }
            },
            handler=self._execute_connectivity_test
        )

    async def start(self):
        """Start the MCP server."""
        logger.info(f"Starting {self.SERVICE_NAME}...")

        self._state.status = ConnectionStatus.CONNECTING
        self._state.session_id = str(uuid.uuid4())

        self._client = httpx.AsyncClient(timeout=30.0)

        try:
            result = await self._execute_connectivity_test({"echo": "native-publish-mcp-init"})
            if result.get("success"):
                self._state.status = ConnectionStatus.CONNECTED
                self._state.connected_at = datetime.now(timezone.utc)
                self._state.last_heartbeat = datetime.now(timezone.utc)
                logger.info(f"✓ {self.SERVICE_NAME} connected (session: {self._state.session_id[:8]}...)")
            else:
                raise RuntimeError("Connectivity test failed")
        except Exception as e:
            self._state.status = ConnectionStatus.ERROR
            self._state.error_message = str(e)
            logger.error(f"✗ {self.SERVICE_NAME} failed to connect: {e}")
            raise

        if self.registry_url:
            await self._register_with_registry()

    async def stop(self):
        """Stop the MCP server."""
        logger.info(f"Stopping {self.SERVICE_NAME}...")

        if self._client:
            await self._client.aclose()
            self._client = None

        self._state.status = ConnectionStatus.DISCONNECTED
        self._state.connected_at = None
        logger.info(f"✓ {self.SERVICE_NAME} stopped")

    async def _register_with_registry(self):
        """Register with central registry."""
        if not self.registry_url:
            return

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.registry_url}/api/v1/services",
                    json={
                        "service_id": self.SERVICE_ID,
                        "name": self.SERVICE_NAME,
                        "base_url": f"http://localhost:{self.port}",
                        "service_type": "mcp",
                        "version": self.VERSION,
                        "tool_names": list(self._tools.keys()),
                        "capabilities": {
                            "tool_execution": True,
                            "tool_discovery": True,
                            "webhooks": True,
                            "event_streaming": True,
                            "message_queue": True,
                            "mcp_protocol": True,
                        }
                    },
                    timeout=10.0
                )
                if response.status_code in (200, 201):
                    logger.info(f"✓ Registered with registry at {self.registry_url}")
                else:
                    logger.warning(f"Registry registration returned {response.status_code}")
        except Exception as e:
            logger.warning(f"Could not register with registry: {e}")

    # =========================================================================
    # MCP PROTOCOL HANDLERS
    # =========================================================================

    async def handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "protocolVersion": MCPProtocolVersion.V1.value,
            "capabilities": {"tools": {}},
            "serverInfo": {
                "name": self.SERVICE_NAME,
                "version": self.VERSION,
            }
        }

    async def handle_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        tools = []
        for name, tool in self._tools.items():
            if not name.startswith("_"):
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.input_schema
                })
        return {"tools": tools}

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")

        if not tool.handler:
            raise ValueError(f"Tool {tool_name} has no handler")

        result = await tool.handler(arguments)

        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result) if isinstance(result, dict) else str(result)
                }
            ]
        }

    async def handle_mcp_request(self, request: MCPRequest) -> MCPResponse:
        try:
            handlers = {
                "initialize": self.handle_initialize,
                "tools/list": self.handle_tools_list,
                "tools/call": self.handle_tools_call,
            }

            handler = handlers.get(request.method)
            if not handler:
                return MCPResponse(
                    id=request.id,
                    error={"code": -32601, "message": f"Method not found: {request.method}"}
                )

            result = await handler(request.params or {})
            return MCPResponse(id=request.id, result=result)

        except Exception as e:
            logger.error(f"MCP request error: {e}")
            return MCPResponse(
                id=request.id,
                error={"code": -32000, "message": str(e)}
            )

    # =========================================================================
    # TOOL IMPLEMENTATIONS
    # =========================================================================

    async def _execute_create_webhook(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = args.get("name")
        url = args.get("url")
        if not name or not url:
            raise ValueError("name and url are required")

        webhook = Webhook(
            id=str(uuid.uuid4()),
            name=name,
            url=url,
            events=args.get("events", ["*"]),
            secret=args.get("secret")
        )
        self._webhooks[webhook.id] = webhook

        return {
            "webhook_id": webhook.id,
            "name": webhook.name,
            "url": webhook.url,
            "events": webhook.events,
            "status": webhook.status.value,
            "success": True
        }

    async def _execute_trigger_webhook(self, args: Dict[str, Any]) -> Dict[str, Any]:
        webhook_id = args.get("webhook_id")
        payload = args.get("payload", {})

        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            return {"error": f"Webhook not found: {webhook_id}", "success": False}

        try:
            response = await self._client.post(
                webhook.url,
                json=payload,
                headers={"X-Webhook-ID": webhook.id},
                timeout=10.0
            )

            webhook.last_triggered = datetime.now(timezone.utc)
            webhook.trigger_count += 1

            # Record event
            event = Event(
                id=str(uuid.uuid4()),
                type=EventType.WEBHOOK_TRIGGERED,
                data={
                    "webhook_id": webhook.id,
                    "webhook_name": webhook.name,
                    "status_code": response.status_code
                }
            )
            self._events.append(event)

            return {
                "webhook_id": webhook.id,
                "status_code": response.status_code,
                "triggered_at": webhook.last_triggered.isoformat(),
                "success": response.status_code < 400
            }

        except Exception as e:
            webhook.failure_count += 1
            return {
                "webhook_id": webhook.id,
                "error": str(e),
                "success": False
            }

    async def _execute_list_webhooks(self, args: Dict[str, Any]) -> Dict[str, Any]:
        status_filter = args.get("status", "all")

        webhooks = list(self._webhooks.values())
        if status_filter != "all":
            target_status = WebhookStatus(status_filter)
            webhooks = [w for w in webhooks if w.status == target_status]

        return {
            "webhooks": [
                {
                    "id": w.id,
                    "name": w.name,
                    "url": w.url,
                    "events": w.events,
                    "status": w.status.value,
                    "trigger_count": w.trigger_count,
                    "failure_count": w.failure_count,
                    "created_at": w.created_at.isoformat(),
                    "last_triggered": w.last_triggered.isoformat() if w.last_triggered else None
                }
                for w in webhooks
            ],
            "total": len(webhooks),
            "success": True
        }

    async def _execute_delete_webhook(self, args: Dict[str, Any]) -> Dict[str, Any]:
        webhook_id = args.get("webhook_id")
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            return {"webhook_id": webhook_id, "deleted": True, "success": True}
        return {"webhook_id": webhook_id, "deleted": False, "success": False}

    async def _execute_publish_event(self, args: Dict[str, Any]) -> Dict[str, Any]:
        event_type = args.get("type")
        data = args.get("data", {})
        source = args.get("source", "api")

        event = Event(
            id=str(uuid.uuid4()),
            type=EventType(event_type) if event_type in [e.value for e in EventType] else EventType.SYSTEM,
            data=data,
            source=source
        )
        self._events.append(event)

        # Trigger matching webhooks
        triggered_webhooks = []
        for webhook in self._webhooks.values():
            if webhook.status == WebhookStatus.ACTIVE:
                if "*" in webhook.events or event_type in webhook.events:
                    await self._execute_trigger_webhook({
                        "webhook_id": webhook.id,
                        "payload": {"event": event_type, "data": data}
                    })
                    triggered_webhooks.append(webhook.id)

        # Notify subscribers
        if event_type in self._subscribers:
            for queue in self._subscribers[event_type]:
                await queue.put(event)

        return {
            "event_id": event.id,
            "type": event_type,
            "webhooks_triggered": len(triggered_webhooks),
            "timestamp": event.timestamp.isoformat(),
            "success": True
        }

    async def _execute_list_events(self, args: Dict[str, Any]) -> Dict[str, Any]:
        event_type = args.get("type")
        limit = args.get("limit", 50)

        events = list(self._events)
        if event_type:
            events = [e for e in events if e.type.value == event_type]

        events = events[-limit:]  # Most recent

        return {
            "events": [
                {
                    "id": e.id,
                    "type": e.type.value,
                    "data": e.data,
                    "source": e.source,
                    "timestamp": e.timestamp.isoformat()
                }
                for e in reversed(events)
            ],
            "total": len(events),
            "success": True
        }

    async def _execute_queue_message(self, args: Dict[str, Any]) -> Dict[str, Any]:
        channel = args.get("channel")
        payload = args.get("payload", {})

        if not channel:
            raise ValueError("channel is required")

        message = QueuedMessage(
            id=str(uuid.uuid4()),
            channel=channel,
            payload=payload
        )

        if channel not in self._message_queue:
            self._message_queue[channel] = deque(maxlen=1000)

        self._message_queue[channel].append(message)

        # Record event
        event = Event(
            id=str(uuid.uuid4()),
            type=EventType.MESSAGE_QUEUED,
            data={"channel": channel, "message_id": message.id}
        )
        self._events.append(event)

        return {
            "message_id": message.id,
            "channel": channel,
            "queued_at": message.created_at.isoformat(),
            "queue_length": len(self._message_queue[channel]),
            "success": True
        }

    async def _execute_get_queue_messages(self, args: Dict[str, Any]) -> Dict[str, Any]:
        channel = args.get("channel")
        limit = args.get("limit", 10)
        acknowledge = args.get("acknowledge", False)

        if not channel:
            raise ValueError("channel is required")

        if channel not in self._message_queue:
            return {"channel": channel, "messages": [], "success": True}

        messages = []
        queue = self._message_queue[channel]

        for msg in list(queue)[:limit]:
            if not msg.delivered or not acknowledge:
                messages.append({
                    "id": msg.id,
                    "payload": msg.payload,
                    "created_at": msg.created_at.isoformat(),
                    "delivered": msg.delivered
                })
                if acknowledge:
                    msg.delivered = True

        return {
            "channel": channel,
            "messages": messages,
            "remaining": len(queue) - len(messages),
            "success": True
        }

    async def _execute_send_notification(self, args: Dict[str, Any]) -> Dict[str, Any]:
        title = args.get("title")
        body = args.get("body")
        targets = args.get("targets", [])
        priority = args.get("priority", "normal")

        if not title or not body:
            raise ValueError("title and body are required")

        # Record notification event
        event = Event(
            id=str(uuid.uuid4()),
            type=EventType.NOTIFICATION_SENT,
            data={
                "title": title,
                "body": body,
                "targets": targets,
                "priority": priority
            }
        )
        self._events.append(event)

        # Send to webhook targets
        sent_to = []
        for target in targets:
            if target in self._webhooks:
                await self._execute_trigger_webhook({
                    "webhook_id": target,
                    "payload": {
                        "notification": {
                            "title": title,
                            "body": body,
                            "priority": priority
                        }
                    }
                })
                sent_to.append(target)

        return {
            "notification_id": event.id,
            "title": title,
            "sent_to": sent_to,
            "priority": priority,
            "timestamp": event.timestamp.isoformat(),
            "success": True
        }

    async def _execute_syndicate_content(self, args: Dict[str, Any]) -> Dict[str, Any]:
        content = args.get("content", {})
        channels = args.get("channels", [])

        if not content or not channels:
            raise ValueError("content and channels are required")

        # Record syndication event
        event = Event(
            id=str(uuid.uuid4()),
            type=EventType.CONTENT_PUBLISHED,
            data={
                "content": content,
                "channels": channels
            }
        )
        self._events.append(event)

        # Queue messages to each channel
        queued = []
        for channel in channels:
            result = await self._execute_queue_message({
                "channel": channel,
                "payload": content
            })
            if result.get("success"):
                queued.append(channel)

        return {
            "syndication_id": event.id,
            "content_title": content.get("title", "Untitled"),
            "channels_queued": queued,
            "total_channels": len(channels),
            "timestamp": event.timestamp.isoformat(),
            "success": True
        }

    async def _execute_connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        echo = args.get("echo", "test")

        return {
            "success": True,
            "echo": echo,
            "server_id": self.SERVICE_ID,
            "version": self.VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": self._state.session_id,
            "tools_registered": len(self._tools),
            "webhooks_count": len(self._webhooks),
            "events_count": len(self._events),
            "queue_channels": len(self._message_queue),
        }

    # =========================================================================
    # PROPERTIES
    # =========================================================================

    @property
    def is_connected(self) -> bool:
        return self._state.status == ConnectionStatus.CONNECTED

    @property
    def tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema
            }
            for name, t in self._tools.items()
            if not name.startswith("_")
        ]

    def get_status(self) -> Dict[str, Any]:
        return {
            "service_id": self.SERVICE_ID,
            "service_name": self.SERVICE_NAME,
            "version": self.VERSION,
            "status": self._state.status.value,
            "connected_at": self._state.connected_at.isoformat() if self._state.connected_at else None,
            "session_id": self._state.session_id,
            "tools_count": len([t for t in self._tools if not t.startswith("_")]),
            "webhooks_count": len(self._webhooks),
            "events_count": len(self._events),
            "queue_channels": len(self._message_queue),
        }


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

def create_server(
    port: int = 4003,
    registry_url: Optional[str] = None,
) -> DistributedPublishMCP:
    return DistributedPublishMCP(port=port, registry_url=registry_url)


_server: Optional[DistributedPublishMCP] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _server
    _server = create_server(
        port=int(os.getenv("PORT", "4003")),
        registry_url=os.getenv("REGISTRY_URL")
    )
    await _server.start()

    # Register tools with central registry
    registry_url = os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
    service_url = os.getenv("SERVICE_URL", f"http://localhost:{os.getenv('PORT', '4003')}")

    try:
        from tool_management.client import ToolManagementClient

        client = ToolManagementClient(
            registry_url=registry_url,
            provider_id=DistributedPublishMCP.SERVICE_ID,
            provider_url=service_url,
            namespace="publish",
        )

        for tool in _server.tools:
            client._tools[tool["name"]] = type("RegisteredTool", (), {
                "name": tool["name"],
                "description": tool["description"],
                "input_schema": tool["inputSchema"],
                "handler": lambda **kw: None,
                "output_schema": None,
                "namespace": "publish",
                "tags": ["publish", "webhooks", "events"],
            })()

        await client.register_all()
        logger.info(f"Registered {len(_server.tools)} tools with {registry_url}")

    except ImportError:
        logger.info("Tool management client not available, skipping global registration")
    except Exception as e:
        logger.warning(f"Could not register with central registry: {e}")

    yield
    await _server.stop()


app = FastAPI(
    title="Distributed Publish MCP",
    description="Native MCP server for webhooks, events, and content syndication",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    if _server:
        return _server.get_status()
    return {"status": "initializing"}


@app.get("/health")
async def health():
    if _server and _server.is_connected:
        return {"status": "healthy", "service": DistributedPublishMCP.SERVICE_ID}
    return JSONResponse(status_code=503, content={"status": "unhealthy"})


@app.get("/tools")
async def list_tools():
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return {"tools": _server.tools}


@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    if not _server:
        raise HTTPException(503, "Server not initialized")
    response = await _server.handle_mcp_request(request)
    return response.model_dump(exclude_none=True)


@app.post("/execute")
async def execute_tool(request: ToolCallRequest):
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")

    result = await tool.handler(request.arguments)
    return {"result": result}


@app.get("/webhooks")
async def list_webhooks_endpoint():
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return await _server._execute_list_webhooks({})


@app.get("/events")
async def list_events_endpoint(type: Optional[str] = None, limit: int = 50):
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return await _server._execute_list_events({"type": type, "limit": limit})


@app.get("/sse")
async def sse_endpoint(request: Request):
    if not _server:
        raise HTTPException(503, "Server not initialized")

    async def event_generator() -> AsyncGenerator[str, None]:
        yield f"data: {json.dumps({'type': 'connected', 'session_id': _server._state.session_id})}\n\n"

        while True:
            if await request.is_disconnected():
                break

            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat(), 'webhooks': len(_server._webhooks), 'events': len(_server._events)})}\n\n"
            await asyncio.sleep(30)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "4003"))

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║         DISTRIBUTED PUBLISH MCP - Native Server                  ║
║               Third Native MCP Implementation                    ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-publish-mcp                             ║
║  Port: {port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools:                                                          ║
║  • create_webhook    - Register webhook endpoints                ║
║  • trigger_webhook   - Manually trigger webhooks                 ║
║  • list_webhooks     - List all webhooks                         ║
║  • delete_webhook    - Remove webhooks                           ║
║  • publish_event     - Publish events to subscribers             ║
║  • list_events       - View recent events                        ║
║  • queue_message     - Queue messages for delivery               ║
║  • get_queue_messages- Retrieve queued messages                  ║
║  • send_notification - Send notifications                        ║
║  • syndicate_content - Distribute content to channels            ║
╠══════════════════════════════════════════════════════════════════╣
║  Endpoints:                                                      ║
║  • GET  /            Service info                                ║
║  • GET  /health      Health check                                ║
║  • GET  /tools       List tools                                  ║
║  • POST /mcp         MCP JSON-RPC endpoint                       ║
║  • POST /execute     Direct tool execution                       ║
║  • GET  /webhooks    List webhooks                               ║
║  • GET  /events      List events                                 ║
║  • GET  /sse         Server-Sent Events stream                   ║
╚══════════════════════════════════════════════════════════════════╝

Starting native MCP server...
""")

    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
