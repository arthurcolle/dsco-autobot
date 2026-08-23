#!/usr/bin/env python3
"""
Distributed Publish MCP - Native MCP Server

First-class MCP server providing:
- Draft-first Substack authoring and confirmed publication
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
import contextvars
import json
import logging
import os
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union, AsyncGenerator
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from native.base import install_backend_bearer_middleware

try:
    from tool_management.core.credential_envelope import (
        CredentialEnvelopeError,
        open_credential_envelope,
    )
except ImportError:
    CredentialEnvelopeError = ValueError  # type: ignore[assignment]
    open_credential_envelope = None  # type: ignore[assignment]

try:
    from .substack_client import SubstackClient, SubstackDocument, MarkdownToSubstack
except ImportError:
    from substack_client import SubstackClient, SubstackDocument, MarkdownToSubstack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_request_substack_client: contextvars.ContextVar[Optional["SubstackClient"]] = (
    contextvars.ContextVar("request_substack_client", default=None)
)

# Output guard for enforcing size limits and URL capture
try:
    from tool_management.guards.output_guard import guard_mcp_content
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False
    logger.info("Output guard not available - running without output limits")


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
    type: Union[EventType, str]
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

        # Substack integration
        self._substack_client: Optional[SubstackClient] = None
        self._substack_lock = asyncio.Lock()
        self._live_session: Optional[Dict[str, Any]] = None

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

        # Integration feedback tool
        self._tools["integration_feedback"] = MCPTool(
            name="integration_feedback",
            description=(
                "Submit feedback about your experience using a tool on this integration "
                "server (correctness, schema clarity, missing capabilities, errors, etc). "
                "Call this after using a tool if something was confusing, broken, or "
                "worked great — it helps us improve the API for AI agents."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": "Which tool this feedback concerns (use 'general' for service-wide feedback)",
                    },
                    "outcome": {
                        "type": "string",
                        "enum": ["success", "partial", "failure"],
                        "description": "How the tool call(s) being reported on went",
                    },
                    "category": {
                        "type": "string",
                        "enum": [
                            "correctness", "schema_clarity", "performance", "error_handling",
                            "documentation", "missing_capability", "rate_limit", "auth", "other",
                        ],
                        "default": "other",
                    },
                    "rating": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 5,
                        "description": "1 (poor) to 5 (excellent)",
                    },
                    "message": {
                        "type": "string",
                        "description": "Free-text feedback: what happened, what was confusing, what would help",
                    },
                    "suggested_fix": {"type": "string"},
                    "context": {
                        "type": "object",
                        "description": "Optional supporting context, e.g. the arguments used or error text received",
                    },
                    "session_id": {"type": "string"},
                    "trace_id": {"type": "string"},
                },
                "required": ["message"],
            },
            handler=self._execute_integration_feedback
        )

        # =====================================================================
        # SUBSTACK TOOLS — always advertised as a core feature. Calls fail
        # closed with a configuration error until credentials are available.
        # =====================================================================

        self._tools["substack_connection_status"] = MCPTool(
            name="substack_connection_status",
            description="Check Substack feature readiness without exposing credentials.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            handler=self._execute_substack_connection_status,
        )

        self._tools["substack_create_draft"] = MCPTool(
            name="substack_create_draft",
            description="Create a new Substack draft post from markdown content.",
            input_schema={
                "type": "object",
                "properties": {
                    "title": {"type": "string", "minLength": 1, "maxLength": 500, "description": "Post title"},
                    "subtitle": {"type": "string", "description": "Post subtitle"},
                    "body": {"type": "string", "description": "Markdown content for the post body"},
                    "audience": {
                        "type": "string",
                        "enum": ["everyone", "only_paid", "founding"],
                        "default": "everyone",
                        "description": "Who can read this post"
                    }
                },
                "required": ["title"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_create_draft
        )

        self._tools["substack_update_draft"] = MCPTool(
            name="substack_update_draft",
            description="Update an existing Substack draft's title, subtitle, or body content.",
            input_schema={
                "type": "object",
                "properties": {
                    "draft_id": {"type": "integer", "minimum": 1, "description": "Draft ID to update"},
                    "title": {"type": "string", "description": "New title"},
                    "subtitle": {"type": "string", "description": "New subtitle"},
                    "body": {"type": "string", "description": "New markdown content (replaces existing)"}
                },
                "required": ["draft_id"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_update_draft
        )

        self._tools["substack_append_to_draft"] = MCPTool(
            name="substack_append_to_draft",
            description="Append markdown content to an existing Substack draft (for live blogging or incremental writing).",
            input_schema={
                "type": "object",
                "properties": {
                    "draft_id": {"type": "integer", "minimum": 1, "description": "Draft ID"},
                    "content": {"type": "string", "minLength": 1, "description": "Markdown content to append"},
                    "section_title": {"type": "string", "description": "Optional section header"},
                    "add_timestamp": {"type": "boolean", "default": True, "description": "Add timestamp"}
                },
                "required": ["draft_id", "content"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_append_to_draft
        )

        self._tools["substack_add_code_block"] = MCPTool(
            name="substack_add_code_block",
            description="Add a syntax-highlighted code block to a Substack draft.",
            input_schema={
                "type": "object",
                "properties": {
                    "draft_id": {"type": "integer", "minimum": 1},
                    "code": {"type": "string", "minLength": 1, "description": "Code content"},
                    "language": {"type": "string", "description": "Programming language"},
                    "filename": {"type": "string", "description": "Optional filename label"},
                    "caption": {"type": "string", "description": "Optional caption/explanation"}
                },
                "required": ["draft_id", "code"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_add_code_block
        )

        self._tools["substack_add_image"] = MCPTool(
            name="substack_add_image",
            description="Add an image to a Substack draft with optional caption and alt text.",
            input_schema={
                "type": "object",
                "properties": {
                    "draft_id": {"type": "integer", "minimum": 1},
                    "url": {"type": "string", "format": "uri", "description": "Image URL"},
                    "caption": {"type": "string", "description": "Image caption"},
                    "alt": {"type": "string", "description": "Alt text for accessibility"}
                },
                "required": ["draft_id", "url"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_add_image
        )

        self._tools["substack_publish"] = MCPTool(
            name="substack_publish",
            description="Publish a reviewed Substack draft. Requires explicit publication confirmation; subscriber email requires a second confirmation.",
            input_schema={
                "type": "object",
                "properties": {
                    "draft_id": {"type": "integer", "minimum": 1, "description": "Draft ID to publish"},
                    "send_email": {"type": "boolean", "default": False, "description": "Email to subscribers"},
                    "confirm_publish": {"type": "boolean", "description": "Must be true to make the draft public"},
                    "confirm_email": {"type": "boolean", "default": False, "description": "Must be true when send_email is true"}
                },
                "required": ["draft_id", "confirm_publish"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_publish
        )

        self._tools["substack_post_note"] = MCPTool(
            name="substack_post_note",
            description="Post a public short-form Note after explicit confirmation.",
            input_schema={
                "type": "object",
                "properties": {
                    "text": {"type": "string", "minLength": 1, "description": "Note content"},
                    "link_url": {"type": "string", "format": "uri", "description": "Optional link to attach"},
                    "confirm_post": {"type": "boolean", "description": "Must be true to post publicly"}
                },
                "required": ["text", "confirm_post"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_post_note
        )

        self._tools["substack_get_drafts"] = MCPTool(
            name="substack_get_drafts",
            description="List all Substack draft posts.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            handler=self._execute_substack_get_drafts
        )

        self._tools["substack_get_posts"] = MCPTool(
            name="substack_get_posts",
            description="List published Substack posts.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10, "description": "Max posts to return"}
                },
                "additionalProperties": False,
            },
            handler=self._execute_substack_get_posts
        )

        self._tools["substack_upload_image"] = MCPTool(
            name="substack_upload_image",
            description="Upload an image already staged on the integration server to Substack's CDN.",
            input_schema={
                "type": "object",
                "properties": {
                    "image_path": {"type": "string", "minLength": 1, "description": "Absolute server-side image path"}
                },
                "required": ["image_path"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_upload_image,
        )

        self._tools["substack_generate_image"] = MCPTool(
            name="substack_generate_image",
            description="Generate an image through OpenRouter, upload it to Substack, and optionally append it to a draft. Incurs provider charges and requires explicit confirmation.",
            input_schema={
                "type": "object",
                "properties": {
                    "prompt": {"type": "string", "minLength": 1, "description": "Image generation prompt"},
                    "draft_id": {"type": "integer", "minimum": 1, "description": "Optional draft to receive the image"},
                    "caption": {"type": "string", "description": "Optional image caption"},
                    "model": {"type": "string", "default": "google/gemini-2.5-flash-image", "description": "OpenRouter model ID"},
                    "confirm_generation": {"type": "boolean", "description": "Must be true because generation can incur provider charges"},
                },
                "required": ["prompt", "confirm_generation"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_generate_image,
        )

        self._tools["substack_live_blog_start"] = MCPTool(
            name="substack_live_blog_start",
            description="Start a live blogging session on Substack — creates a draft and tracks updates.",
            input_schema={
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "Live blog title"},
                    "subtitle": {"type": "string", "description": "Subtitle"}
                },
                "required": ["title"],
                "additionalProperties": False,
            },
            handler=self._execute_substack_live_blog_start
        )

        self._tools["substack_live_blog_end"] = MCPTool(
            name="substack_live_blog_end",
            description="End the current live blogging session, optionally publishing the post.",
            input_schema={
                "type": "object",
                "properties": {
                    "publish": {"type": "boolean", "default": False, "description": "Publish on end"},
                    "confirm_publish": {"type": "boolean", "default": False, "description": "Must be true when publish is true"}
                },
                "additionalProperties": False,
            },
            handler=self._execute_substack_live_blog_end
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

        # Initialize Substack client if credentials available
        substack_sid = os.getenv("SUBSTACK_SID", "")
        substack_pub = os.getenv("SUBSTACK_PUBLICATION", "")
        if substack_sid and substack_pub:
            try:
                self._substack_client = SubstackClient(
                    substack_sid,
                    substack_pub,
                    rate_limit=float(os.getenv("SUBSTACK_RATE_LIMIT_SECONDS", "0.5")),
                    timeout=float(os.getenv("SUBSTACK_REQUEST_TIMEOUT_SECONDS", "30")),
                )
                if os.getenv("SUBSTACK_VALIDATE_ON_STARTUP", "0") == "1":
                    ok = await asyncio.to_thread(self._substack_client.test_connection)
                    logger.info("Substack startup validation: %s", "ok" if ok else "failed")
                else:
                    logger.info("Substack credentials loaded for %s", self._substack_client.publication)
            except Exception as e:
                logger.warning("Substack init failed: %s", type(e).__name__)
                self._substack_client = None
        else:
            logger.info("No SUBSTACK_SID/SUBSTACK_PUBLICATION — Substack calls will fail closed")

        # Announce to Tool Management
        await self._announce_to_registry()

    async def stop(self):
        """Stop the MCP server."""
        logger.info(f"Stopping {self.SERVICE_NAME}...")

        if self._substack_client:
            await asyncio.to_thread(self._substack_client.close)
            self._substack_client = None

        if self._client:
            await self._client.aclose()
            self._client = None

        self._state.status = ConnectionStatus.DISCONNECTED
        self._state.connected_at = None
        logger.info(f"✓ {self.SERVICE_NAME} stopped")

    async def _announce_to_registry(self):
        """Announce this server and its tools to Tool Management."""
        registry_url = self.registry_url or os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
        service_url = os.getenv("SERVICE_URL", f"http://localhost:{self.port}")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    f"{registry_url}/api/v1/tools/announce",
                    json={
                        "backend_id": self.SERVICE_ID,
                        "name": self.SERVICE_NAME,
                        "url": service_url,
                        "health_endpoint": "/health",
                        "mcp_endpoint": "/mcp",
                        "capabilities": list(self._tools.keys()),
                        "tools": self.tools,
                    },
                    headers=(
                        {"x-registry-token": os.environ["REGISTRY_ANNOUNCE_TOKEN"]}
                        if os.getenv("REGISTRY_ANNOUNCE_TOKEN") else {}
                    ),
                    timeout=15.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Announced {result.get('registered', 0)} tools to {registry_url}")
                else:
                    logger.warning(f"Announce returned {response.status_code}: {response.text[:200]}")
        except Exception as e:
            logger.warning(f"Could not announce to registry: {e}")

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

    async def _invoke_tool(self, tool_name: Any, raw_arguments: Any) -> Any:
        """Invoke one tool with request-scoped credential isolation."""
        arguments = dict(raw_arguments or {})
        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")

        if not tool.handler:
            raise ValueError(f"Tool {tool_name} has no handler")

        # Customer SID values only arrive as authenticated, short-lived,
        # backend-bound envelopes.  ContextVars isolate concurrent tenants;
        # no request credential is ever assigned to the process-global client.
        if "tenant_id" in arguments or "_tenant_credential" in arguments:
            raise ValueError("raw tenant credentials or tenant_id are not accepted")
        envelope = arguments.pop("_credential_envelope", None)
        request_client: Optional[SubstackClient] = None
        context_token = None
        if envelope:
            if not str(tool_name or "").startswith("substack_"):
                raise ValueError("credential envelope is not valid for this tool")
            if open_credential_envelope is None:
                raise ValueError("credential envelope support is unavailable")
            try:
                opened = open_credential_envelope(
                    str(envelope),
                    expected_backend_id=self.SERVICE_ID,
                    expected_provider="substack",
                )
                bundle = json.loads(opened["credential"])
                sid = str(bundle.get("sid") or "").strip()
                publication = str(bundle.get("publication") or "").strip().lower()
                if not sid or not publication:
                    raise ValueError("Substack credential bundle is incomplete")
                request_client = SubstackClient(
                    sid,
                    publication,
                    rate_limit=float(os.getenv("SUBSTACK_RATE_LIMIT_SECONDS", "0.5")),
                    timeout=float(os.getenv("SUBSTACK_REQUEST_TIMEOUT_SECONDS", "30")),
                )
                context_token = _request_substack_client.set(request_client)
            except (CredentialEnvelopeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                raise ValueError(f"credential envelope rejected: {type(exc).__name__}") from exc

        try:
            result = await tool.handler(arguments)
        finally:
            if context_token is not None:
                _request_substack_client.reset(context_token)
            if request_client is not None:
                await asyncio.to_thread(request_client.close)

        return result

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        tool_name = params.get("name")
        result = await self._invoke_tool(tool_name, params.get("arguments", {}))

        mcp_result = {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result) if isinstance(result, dict) else str(result)
                }
            ]
        }

        # Apply output guard: enforce size limits, cap links, capture URLs
        if OUTPUT_GUARD_AVAILABLE:
            mcp_result = guard_mcp_content(
                mcp_result,
                tool_name=tool_name,
                source_context={"server": self.SERVICE_ID},
            )

        return mcp_result

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
        event_type = args.get("type", "system")
        data = args.get("data", {})
        source = args.get("source", "api")

        # Accept known enum values directly; for custom types, store as-is
        try:
            resolved_type: Union[EventType, str] = EventType(event_type) if event_type in [e.value for e in EventType] else event_type
        except (ValueError, KeyError):
            resolved_type = event_type

        event = Event(
            id=str(uuid.uuid4()),
            type=resolved_type,
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
            events = [e for e in events if (e.type.value if isinstance(e.type, EventType) else e.type) == event_type]

        events = events[-limit:]  # Most recent

        return {
            "events": [
                {
                    "id": e.id,
                    "type": e.type.value if isinstance(e.type, EventType) else e.type,
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

        notification_payload = {
            "notification": {
                "title": title,
                "body": body,
                "priority": priority
            }
        }

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

        # When no targets specified, queue to default "notifications" channel
        if not targets:
            await self._execute_queue_message({
                "channel": "notifications",
                "payload": notification_payload
            })
            return {
                "notification_id": event.id,
                "title": title,
                "sent_to": ["notifications"],
                "queued": True,
                "priority": priority,
                "timestamp": event.timestamp.isoformat(),
                "success": True
            }

        # Send to targets
        sent_to = []
        for target in targets:
            if target in self._webhooks:
                # Webhook target
                await self._execute_trigger_webhook({
                    "webhook_id": target,
                    "payload": notification_payload
                })
                sent_to.append(target)
            elif target.startswith("http://") or target.startswith("https://"):
                # Raw URL target — POST notification payload directly
                try:
                    await self._client.post(
                        target,
                        json=notification_payload,
                        timeout=10.0,
                    )
                    sent_to.append(target)
                except Exception as e:
                    logger.warning(f"Failed to send notification to URL {target}: {e}")
            else:
                # Queue channel target
                await self._execute_queue_message({
                    "channel": target,
                    "payload": notification_payload
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

    async def _execute_integration_feedback(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Forward agent feedback about this server's tools to the central registry."""
        message = args.get("message")
        if not message:
            return {
                "content": [{"type": "text", "text": "message is required"}],
                "isError": True,
            }

        payload = {
            "backend_id": self.SERVICE_ID,
            "message": message,
            "tool_name": args.get("tool_name"),
            "outcome": args.get("outcome"),
            "category": args.get("category") or "other",
            "rating": args.get("rating"),
            "suggested_fix": args.get("suggested_fix"),
            "context": args.get("context"),
            "session_id": args.get("session_id"),
            "trace_id": args.get("trace_id"),
        }

        registry_url = self.registry_url or os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{registry_url}/api/v1/integration-feedback",
                    json=payload,
                    timeout=10.0,
                )
            if response.status_code == 200:
                return {"content": [{"type": "text", "text": json.dumps(response.json())}]}
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            fallback_path = Path("data") / "feedback_fallback.jsonl"
            try:
                fallback_path.parent.mkdir(parents=True, exist_ok=True)
                record = dict(payload)
                record["recorded_at"] = datetime.now(timezone.utc).isoformat()
                record["registry_error"] = str(e)
                with open(fallback_path, "a") as f:
                    f.write(json.dumps(record, default=str) + "\n")
            except Exception as write_err:
                logger.warning(f"Failed to write feedback fallback: {write_err}")
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "status": "recorded_locally",
                        "warning": f"registry unreachable ({e}); saved to {fallback_path}",
                    }),
                }]
            }

    # =========================================================================
    # SUBSTACK TOOL IMPLEMENTATIONS
    # =========================================================================

    def _require_substack(self) -> SubstackClient:
        """Return SubstackClient or raise."""
        request_client = _request_substack_client.get()
        if request_client is not None:
            return request_client
        if not self._substack_client:
            sid = os.getenv("SUBSTACK_SID", "")
            publication = os.getenv("SUBSTACK_PUBLICATION", "")
            if sid and publication:
                self._substack_client = SubstackClient(
                    sid,
                    publication,
                    rate_limit=float(os.getenv("SUBSTACK_RATE_LIMIT_SECONDS", "0.5")),
                    timeout=float(os.getenv("SUBSTACK_REQUEST_TIMEOUT_SECONDS", "30")),
                )
        if not self._substack_client:
            raise ValueError("Substack not configured. Set SUBSTACK_SID and SUBSTACK_PUBLICATION env vars.")
        return self._substack_client

    async def _substack_call(self, function: Callable, *args: Any, **kwargs: Any) -> Any:
        """Serialize the unofficial API session and keep blocking I/O off the event loop."""
        async with self._substack_lock:
            return await asyncio.to_thread(function, *args, **kwargs)

    @staticmethod
    def _positive_int(args: Dict[str, Any], field: str) -> int:
        value = args.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{field} must be a positive integer")
        return value

    @staticmethod
    def _require_confirmation(args: Dict[str, Any], field: str, action: str) -> None:
        if args.get(field) is not True:
            raise ValueError(f"{field}=true is required to {action}")

    @staticmethod
    def _http_url(value: Any, field: str) -> str:
        parsed = urlparse(value if isinstance(value, str) else "")
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"{field} must be an absolute HTTP(S) URL")
        return value

    @staticmethod
    def _parse_draft_body(draft_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse body_json or draft_body from a draft response into a ProseMirror doc dict."""
        raw = draft_data.get("body_json") or draft_data.get("draft_body")
        if raw is None or raw == "":
            return {"type": "doc", "content": []}
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not isinstance(raw, dict):
            raise ValueError(f"Unexpected draft body type: {type(raw).__name__}")
        if raw.get("type") != "doc":
            raw["type"] = "doc"
        if not isinstance(raw.get("content"), list):
            raw["content"] = []
        return raw

    async def _execute_substack_connection_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        request_client = _request_substack_client.get()
        effective_client = request_client or self._substack_client
        configured = request_client is not None or bool(
            os.getenv("SUBSTACK_SID") and os.getenv("SUBSTACK_PUBLICATION")
        )
        return {
            "success": True,
            "configured": configured,
            "client_initialized": effective_client is not None,
            "publication": effective_client.publication if effective_client else None,
            "credential_scope": "tenant_request" if request_client is not None else "worker",
            "draft_first": True,
            "public_actions_require_confirmation": True,
        }

    async def _execute_substack_create_draft(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        title = str(args.get("title", "")).strip()
        if not title:
            raise ValueError("title is required")
        subtitle = args.get("subtitle", "")
        body = args.get("body", "")
        audience = args.get("audience", "everyone")

        draft = await self._substack_call(
            sc.create_draft, title=title, subtitle=subtitle, body=body, audience=audience
        )
        return {
            "success": True,
            "draft_id": draft.id,
            "title": draft.title,
            "edit_url": f"https://{sc.publication}/publish/post/{draft.id}"
        }

    async def _execute_substack_update_draft(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        draft_id = self._positive_int(args, "draft_id")
        if not any(args.get(field) is not None for field in ("title", "subtitle", "body")):
            raise ValueError("at least one of title, subtitle, or body is required")
        await self._substack_call(
            sc.update_draft,
            draft_id=draft_id,
            title=args.get("title"),
            subtitle=args.get("subtitle"),
            body=args.get("body"),
        )
        return {"success": True, "draft_id": draft_id, "message": "Draft updated"}

    async def _execute_substack_append_to_draft(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        draft_id = self._positive_int(args, "draft_id")
        content_md = str(args.get("content", ""))
        if not content_md.strip():
            raise ValueError("content is required")
        section_title = args.get("section_title")
        add_timestamp = args.get("add_timestamp", True)

        draft_data = await self._substack_call(sc.get_draft, draft_id)
        body_json = self._parse_draft_body(draft_data)

        new_nodes: List[Dict[str, Any]] = []
        if section_title:
            ts = ""
            if add_timestamp:
                ts = f" ({datetime.now(timezone.utc).strftime('%I:%M %p UTC')})"
            new_nodes.append({
                "type": "heading",
                "attrs": {"level": 3},
                "content": [{"type": "text", "text": f"{section_title}{ts}"}]
            })

        converted = MarkdownToSubstack.convert(content_md)
        new_nodes.extend(converted.get("content", []))
        body_json["content"].extend(new_nodes)
        await self._substack_call(sc.update_draft, draft_id=draft_id, body=body_json)

        if self._live_session and self._live_session.get("draft_id") == draft_id:
            self._live_session["updates"] = self._live_session.get("updates", 0) + 1

        return {
            "success": True,
            "draft_id": draft_id,
            "message": "Content appended",
            "total_sections": len([n for n in body_json["content"] if n.get("type") == "heading"])
        }

    async def _execute_substack_add_code_block(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        draft_id = self._positive_int(args, "draft_id")
        code = str(args.get("code", ""))
        if not code:
            raise ValueError("code is required")
        language = args.get("language", "")
        filename = args.get("filename", "")
        caption = args.get("caption", "")

        draft_data = await self._substack_call(sc.get_draft, draft_id)
        body_json = self._parse_draft_body(draft_data)

        if filename:
            body_json["content"].append({
                "type": "paragraph",
                "content": [{"type": "text", "text": f"File: {filename}", "marks": [{"type": "code"}]}]
            })

        body_json["content"].append({
            "type": "codeBlock",
            "attrs": {"language": language},
            "content": [{"type": "text", "text": code}]
        })

        if caption:
            body_json["content"].append({
                "type": "paragraph",
                "content": [{"type": "text", "text": caption, "marks": [{"type": "em"}]}]
            })

        await self._substack_call(sc.update_draft, draft_id=draft_id, body=body_json)

        if self._live_session and self._live_session.get("draft_id") == draft_id:
            self._live_session["updates"] = self._live_session.get("updates", 0) + 1

        return {"success": True, "draft_id": draft_id, "message": "Code block added"}

    async def _execute_substack_add_image(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        draft_id = self._positive_int(args, "draft_id")
        url = self._http_url(args.get("url"), "url")
        caption = args.get("caption", "")
        alt = args.get("alt", "")

        draft_data = await self._substack_call(sc.get_draft, draft_id)
        body_json = self._parse_draft_body(draft_data)

        image_doc = SubstackDocument()
        image_doc.image(url, alt=alt, caption=caption)
        body_json["content"].extend(image_doc.build()["content"])

        await self._substack_call(sc.update_draft, draft_id=draft_id, body=body_json)

        if self._live_session and self._live_session.get("draft_id") == draft_id:
            self._live_session["updates"] = self._live_session.get("updates", 0) + 1

        return {"success": True, "draft_id": draft_id, "message": "Image added"}

    async def _execute_substack_publish(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        draft_id = self._positive_int(args, "draft_id")
        self._require_confirmation(args, "confirm_publish", "publish a draft")
        send_email = args.get("send_email", False)
        if send_email:
            self._require_confirmation(args, "confirm_email", "email subscribers")

        pub_result = await self._substack_call(sc.publish_draft, draft_id, send_email=send_email)
        return {
            "success": True,
            "url": pub_result.get("canonical_url", ""),
            "email_sent": send_email
        }

    async def _execute_substack_post_note(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        self._require_confirmation(args, "confirm_post", "post a public Note")
        text = str(args.get("text", "")).strip()
        if not text:
            raise ValueError("text is required")
        link_url = args.get("link_url")

        if link_url:
            self._http_url(link_url, "link_url")
            result = await self._substack_call(sc.post_note_with_link, text, link_url)
        else:
            result = await self._substack_call(sc.post_note, text)
        return {"success": True, "note_id": result.get("id"), "message": "Note posted"}

    async def _execute_substack_get_drafts(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        drafts = await self._substack_call(sc.get_drafts)
        return {
            "success": True,
            "drafts": [
                {"id": d.id, "title": d.title or "(Untitled)", "subtitle": d.subtitle}
                for d in drafts
            ]
        }

    async def _execute_substack_get_posts(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        limit = args.get("limit", 10)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 to 100")
        posts = await self._substack_call(sc.get_archive, limit=limit)
        return {
            "success": True,
            "posts": [
                {"id": p.id, "title": p.title, "url": p.canonical_url, "date": p.post_date}
                for p in posts
            ]
        }

    async def _execute_substack_upload_image(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        image_path = str(args.get("image_path", "")).strip()
        if not image_path:
            raise ValueError("image_path is required")
        upload = await self._substack_call(sc.upload_image, image_path)
        return {
            "success": True,
            "url": upload["url"],
            "width": upload.get("width"),
            "height": upload.get("height"),
        }

    async def _execute_substack_generate_image(self, args: Dict[str, Any]) -> Dict[str, Any]:
        import base64
        import re
        import tempfile

        sc = self._require_substack()
        self._require_confirmation(args, "confirm_generation", "generate a billed image")
        prompt = str(args.get("prompt", "")).strip()
        if not prompt:
            raise ValueError("prompt is required")
        api_key = os.getenv("OPENROUTER_API_KEY", "")
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY is not configured")

        model = str(args.get("model", "google/gemini-2.5-flash-image")).strip()
        caption = str(args.get("caption", ""))
        draft_id = args.get("draft_id")
        if draft_id is not None:
            draft_id = self._positive_int(args, "draft_id")

        client = self._client or httpx.AsyncClient(timeout=180.0)
        owns_client = self._client is None
        try:
            response = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}]},
                timeout=180.0,
            )
            if response.status_code != 200:
                raise RuntimeError(f"image provider returned HTTP {response.status_code}")
            max_response_bytes = int(
                os.getenv("OPENROUTER_MAX_IMAGE_RESPONSE_BYTES", str(25 * 1024 * 1024))
            )
            if len(response.content) > max_response_bytes:
                raise ValueError("image provider response exceeded the configured size limit")
            match = re.search(
                rb"data:image/[^;]+;base64,([A-Za-z0-9+/=]{100,})",
                response.content,
            )
            if not match:
                raise RuntimeError("image provider response did not contain image data")
            image_data = base64.b64decode(match.group(1), validate=True)
        finally:
            if owns_client:
                await client.aclose()

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as handle:
            handle.write(image_data)
            temp_path = Path(handle.name)
        try:
            upload = await self._substack_call(sc.upload_image, str(temp_path))
        finally:
            temp_path.unlink(missing_ok=True)

        cdn_url = upload["url"]
        if draft_id is not None:
            draft_data = await self._substack_call(sc.get_draft, draft_id)
            body_json = self._parse_draft_body(draft_data)
            image_doc = SubstackDocument().image(
                cdn_url,
                alt=caption or prompt[:160],
                caption=caption,
            )
            body_json["content"].extend(image_doc.build()["content"])
            await self._substack_call(sc.update_draft, draft_id=draft_id, body=body_json)

        return {
            "success": True,
            "url": cdn_url,
            "draft_id": draft_id,
            "caption": caption,
        }

    async def _execute_substack_live_blog_start(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        title = str(args.get("title", "")).strip()
        if not title:
            raise ValueError("title is required")
        subtitle = args.get("subtitle", "")

        doc = SubstackDocument()
        doc.paragraph(SubstackDocument.bold("Live Blog Started"), f" - {datetime.now(timezone.utc).strftime('%I:%M %p UTC')}")
        doc.horizontal_rule()

        draft = await self._substack_call(sc.create_draft, title=title, subtitle=subtitle, body=doc)

        self._live_session = {
            "draft_id": draft.id,
            "title": title,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "updates": 0,
            "active": True
        }

        return {
            "success": True,
            "session": self._live_session,
            "edit_url": f"https://{sc.publication}/publish/post/{draft.id}"
        }

    async def _execute_substack_live_blog_end(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sc = self._require_substack()
        if not self._live_session:
            return {"success": False, "error": "No active live blog session"}

        draft_id = self._live_session["draft_id"]
        publish = args.get("publish", False)
        if publish:
            self._require_confirmation(args, "confirm_publish", "publish the live blog")

        draft_data = await self._substack_call(sc.get_draft, draft_id)
        body_json = self._parse_draft_body(draft_data)

        closing = SubstackDocument()
        closing.horizontal_rule()
        closing.paragraph(SubstackDocument.bold("Live Blog Ended"), f" - {datetime.now(timezone.utc).strftime('%I:%M %p UTC')}")
        body_json["content"].extend(closing.build()["content"])
        await self._substack_call(sc.update_draft, draft_id=draft_id, body=body_json)

        result: Dict[str, Any] = {"success": True, "session": self._live_session, "published": False}

        if publish:
            pub_result = await self._substack_call(sc.publish_draft, draft_id, send_email=False)
            result["published"] = True
            result["url"] = pub_result.get("canonical_url", "")

        self._live_session = None
        return result

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

    # Background heartbeat: re-announce to registry every 60s
    _heartbeat_task = None

    async def _heartbeat_loop():
        await asyncio.sleep(30)
        while True:
            try:
                await _server._announce_to_registry()
                logger.debug("Heartbeat: re-announced to registry")
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
            await asyncio.sleep(60)

    _heartbeat_task = asyncio.create_task(_heartbeat_loop())

    yield

    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass

    await _server.stop()


app = FastAPI(
    title="Distributed Publish MCP",
    description="Core MCP integration for draft-first Substack publishing, webhooks, events, and content syndication",
    version=DistributedPublishMCP.VERSION,
    lifespan=lifespan
)

_production = os.getenv("ENVIRONMENT", "").lower() in {"prod", "production"}
_cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ORIGINS", "" if _production else "*").split(",")
    if origin.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=bool(_cors_origins and _cors_origins != ["*"]),
    allow_methods=["*"],
    allow_headers=["*"],
)
install_backend_bearer_middleware(app)


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

    # Keep the bespoke endpoint on the identical envelope/context path as MCP.
    try:
        result = await _server._invoke_tool(request.name, request.arguments)
    except ValueError as exc:
        if str(exc).startswith("Unknown tool:"):
            raise HTTPException(404, str(exc)) from exc
        raise HTTPException(422, str(exc)) from exc
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


@app.get("/mcp/sse")
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


@app.post("/mcp/sse")
@app.post("/sse")
async def sse_post_endpoint(request: Request):
    """Handle MCP JSON-RPC POST requests via SSE URL (mcp-remote pattern)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    body = await request.json()
    mcp_request = MCPRequest(**body)
    response = await _server.handle_mcp_request(mcp_request)
    return response.model_dump(exclude_none=True)


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
║  • substack_*       - Draft-first, confirmed Substack publishing ║
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
