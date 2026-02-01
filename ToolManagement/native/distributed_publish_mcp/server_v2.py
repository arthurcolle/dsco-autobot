#!/usr/bin/env python3
"""
Distributed Publish MCP v2 - Migrated to NativeMCPBase.

Provides:
- Webhook management (create, trigger, list)
- Event publishing and streaming
- Content syndication
- Notification dispatch
- Message queuing

Run with:
    python -m native.distributed_publish_mcp.server_v2 --port 4003
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase, CapabilityType

logger = logging.getLogger(__name__)


# =============================================================================
# MODELS
# =============================================================================

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


# =============================================================================
# PUBLISH MCP
# =============================================================================

class DistributedPublishMCPv2(NativeMCPBase):
    """
    Native MCP server for content publishing and event distribution.

    Migrated to use NativeMCPBase for standardized infrastructure.
    """

    SERVICE_ID = "distributed-publish-mcp"
    SERVICE_NAME = "Distributed Publish MCP"
    VERSION = "2.0.0"

    # This MCP provides these capabilities
    CAPABILITIES = [
        CapabilityType.PUBLISH,
        CapabilityType.SUBSCRIBE,
        CapabilityType.NOTIFY,
        "webhook",
        "message_queue",
    ]

    # No dependencies
    REQUIRES = []
    DEPENDS = []

    # Tool definitions
    TOOLS = [
        {
            "name": "create_webhook",
            "description": "Create a new webhook endpoint to receive event notifications.",
            "inputSchema": {
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
            }
        },
        {
            "name": "trigger_webhook",
            "description": "Manually trigger a webhook with custom payload.",
            "inputSchema": {
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
            }
        },
        {
            "name": "list_webhooks",
            "description": "List all registered webhooks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["active", "paused", "failed", "all"],
                        "default": "all"
                    }
                }
            }
        },
        {
            "name": "delete_webhook",
            "description": "Delete a webhook.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "webhook_id": {
                        "type": "string",
                        "description": "Webhook ID to delete"
                    }
                },
                "required": ["webhook_id"]
            }
        },
        {
            "name": "publish_event",
            "description": "Publish an event to all subscribers and matching webhooks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Event type"
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
            }
        },
        {
            "name": "list_events",
            "description": "List recent events.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Filter by event type"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 50
                    }
                }
            }
        },
        {
            "name": "queue_message",
            "description": "Queue a message for delivery to a channel.",
            "inputSchema": {
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
            }
        },
        {
            "name": "get_queue_messages",
            "description": "Get messages from a queue channel.",
            "inputSchema": {
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
            }
        },
        {
            "name": "send_notification",
            "description": "Send a notification to specified targets.",
            "inputSchema": {
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
            }
        },
        {
            "name": "syndicate_content",
            "description": "Syndicate content to multiple channels/platforms.",
            "inputSchema": {
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
            }
        },
        {
            "name": "broadcast",
            "description": "Broadcast a message to all other MCPs via the mesh.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "object",
                        "description": "Message to broadcast"
                    },
                    "target_capability": {
                        "type": "string",
                        "description": "Only broadcast to MCPs with this capability"
                    }
                },
                "required": ["message"]
            }
        },
    ]

    def __init__(self, port: int = 4003, **kwargs):
        super().__init__(port=port, **kwargs)

        # Native storage
        self._webhooks: Dict[str, Webhook] = {}
        self._events: deque[Event] = deque(maxlen=1000)
        self._message_queue: Dict[str, deque[QueuedMessage]] = {}
        self._subscribers: Dict[str, List[asyncio.Queue]] = {}

        # HTTP client for webhook calls
        self._client: Optional[httpx.AsyncClient] = None

        # Lifecycle hooks
        self.on_start(self._on_start)
        self.on_stop(self._on_stop)

    async def _on_start(self):
        """Initialize HTTP client."""
        self._client = httpx.AsyncClient(timeout=30.0)
        logger.info("Publish MCP started with webhook client")

    async def _on_stop(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # TOOL HANDLERS
    # =========================================================================

    async def _tool_create_webhook(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new webhook."""
        name = args.get("name")
        url = args.get("url")
        if not name or not url:
            return self._error("name and url are required")

        webhook = Webhook(
            id=str(uuid.uuid4()),
            name=name,
            url=url,
            events=args.get("events", ["*"]),
            secret=args.get("secret")
        )
        self._webhooks[webhook.id] = webhook

        return self._text_result({
            "webhook_id": webhook.id,
            "name": webhook.name,
            "url": webhook.url,
            "events": webhook.events,
            "status": webhook.status.value,
            "success": True
        })

    async def _tool_trigger_webhook(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Trigger a webhook with payload."""
        webhook_id = args.get("webhook_id")
        payload = args.get("payload", {})

        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            return self._error(f"Webhook not found: {webhook_id}")

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

            return self._text_result({
                "webhook_id": webhook.id,
                "status_code": response.status_code,
                "triggered_at": webhook.last_triggered.isoformat(),
                "success": response.status_code < 400
            })

        except Exception as e:
            webhook.failure_count += 1
            return self._error(str(e))

    async def _tool_list_webhooks(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List all webhooks."""
        status_filter = args.get("status", "all")

        webhooks = list(self._webhooks.values())
        if status_filter != "all":
            target_status = WebhookStatus(status_filter)
            webhooks = [w for w in webhooks if w.status == target_status]

        return self._text_result({
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
        })

    async def _tool_delete_webhook(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Delete a webhook."""
        webhook_id = args.get("webhook_id")
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            return self._text_result({"webhook_id": webhook_id, "deleted": True, "success": True})
        return self._error(f"Webhook not found: {webhook_id}")

    async def _tool_publish_event(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Publish an event."""
        event_type = args.get("type")
        data = args.get("data", {})
        source = args.get("source", "api")

        if not event_type:
            return self._error("type is required")

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
                    await self._tool_trigger_webhook({
                        "webhook_id": webhook.id,
                        "payload": {"event": event_type, "data": data}
                    })
                    triggered_webhooks.append(webhook.id)

        # Notify subscribers
        if event_type in self._subscribers:
            for queue in self._subscribers[event_type]:
                await queue.put(event)

        return self._text_result({
            "event_id": event.id,
            "type": event_type,
            "webhooks_triggered": len(triggered_webhooks),
            "timestamp": event.timestamp.isoformat(),
            "success": True
        })

    async def _tool_list_events(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List recent events."""
        event_type = args.get("type")
        limit = args.get("limit", 50)

        events = list(self._events)
        if event_type:
            events = [e for e in events if e.type.value == event_type]

        events = events[-limit:]

        return self._text_result({
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
        })

    async def _tool_queue_message(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Queue a message for delivery."""
        channel = args.get("channel")
        payload = args.get("payload", {})

        if not channel:
            return self._error("channel is required")

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

        return self._text_result({
            "message_id": message.id,
            "channel": channel,
            "queued_at": message.created_at.isoformat(),
            "queue_length": len(self._message_queue[channel]),
            "success": True
        })

    async def _tool_get_queue_messages(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get messages from a queue."""
        channel = args.get("channel")
        limit = args.get("limit", 10)
        acknowledge = args.get("acknowledge", False)

        if not channel:
            return self._error("channel is required")

        if channel not in self._message_queue:
            return self._text_result({"channel": channel, "messages": [], "success": True})

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

        return self._text_result({
            "channel": channel,
            "messages": messages,
            "remaining": len(queue) - len(messages),
            "success": True
        })

    async def _tool_send_notification(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Send a notification."""
        title = args.get("title")
        body = args.get("body")
        targets = args.get("targets", [])
        priority = args.get("priority", "normal")

        if not title or not body:
            return self._error("title and body are required")

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
                await self._tool_trigger_webhook({
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

        return self._text_result({
            "notification_id": event.id,
            "title": title,
            "sent_to": sent_to,
            "priority": priority,
            "timestamp": event.timestamp.isoformat(),
            "success": True
        })

    async def _tool_syndicate_content(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Syndicate content to multiple channels."""
        content = args.get("content", {})
        channels = args.get("channels", [])

        if not content or not channels:
            return self._error("content and channels are required")

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
            result = await self._tool_queue_message({
                "channel": channel,
                "payload": content
            })
            try:
                result_data = json.loads(result.get("content", [{}])[0].get("text", "{}"))
                if result_data.get("success"):
                    queued.append(channel)
            except Exception:
                pass

        return self._text_result({
            "syndication_id": event.id,
            "content_title": content.get("title", "Untitled"),
            "channels_queued": queued,
            "total_channels": len(channels),
            "timestamp": event.timestamp.isoformat(),
            "success": True
        })

    async def _tool_broadcast(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Broadcast a message to other MCPs via mesh."""
        message = args.get("message", {})
        target_capability = args.get("target_capability")

        if not message:
            return self._error("message is required")

        # Find target MCPs
        await self.mesh.discover()

        endpoints = []
        if target_capability:
            endpoints = self.mesh.find_by_capability(target_capability)
        else:
            endpoints = list(self.mesh._endpoints.values())

        # Broadcast via publish_event if they support it
        broadcast_count = 0
        for endpoint in endpoints:
            if endpoint.has_capability(CapabilityType.SUBSCRIBE):
                try:
                    await self.mesh.call(
                        tool="publish_event",
                        args={
                            "type": "broadcast",
                            "data": message,
                            "source": self.SERVICE_ID
                        },
                        target=endpoint.mcp_id
                    )
                    broadcast_count += 1
                except Exception as e:
                    logger.debug(f"Broadcast to {endpoint.mcp_id} failed: {e}")

        return self._text_result({
            "message": message,
            "broadcast_to": broadcast_count,
            "target_capability": target_capability,
            "success": True
        })

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _text_result(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Format a text result."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(data, indent=2, default=str)
            }]
        }

    def _error(self, message: str) -> Dict[str, Any]:
        """Format an error result."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({"error": message, "success": False})
            }],
            "isError": True
        }


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4003)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║         DISTRIBUTED PUBLISH MCP v2 - NativeMCPBase               ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-publish-mcp                             ║
║  Port: {args.port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Capabilities: publish, subscribe, notify, webhook, message_queue║
╠══════════════════════════════════════════════════════════════════╣
║  Tools: create_webhook, trigger_webhook, list_webhooks,          ║
║         delete_webhook, publish_event, list_events, queue_message║
║         get_queue_messages, send_notification, syndicate_content,║
║         broadcast                                                ║
╠══════════════════════════════════════════════════════════════════╣
║  Built on NativeMCPBase with:                                    ║
║  • Cross-MCP mesh communication                                  ║
║  • Automatic service discovery                                   ║
║  • Mesh-based broadcasting                                       ║
╚══════════════════════════════════════════════════════════════════╝
""")

    server = DistributedPublishMCPv2(port=args.port)
    server.run()
