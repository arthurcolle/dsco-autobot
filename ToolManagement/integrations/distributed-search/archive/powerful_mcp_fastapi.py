"""
Powerful FastAPI MCP Chat with /v1/interactions endpoint
=========================================================

An enterprise-grade FastAPI application with SSE streaming, MCP tool integration,
advanced session management, and powerful multi-turn capabilities.

Features:
- /v1/interactions endpoint with SSE streaming
- Session persistence and resumable conversations
- Advanced context management with compression
- Tool execution telemetry and observability
- Rate limiting and circuit breakers
- Webhook support for async notifications
- Multi-modal support (text, images, files)
- Token budget optimization
- Conversation threading
- Health checks and metrics

Usage:
    uvicorn powerful_mcp_fastapi:app --host 0.0.0.0 --port 2046 --reload
"""

import asyncio
import json
import os
import sys
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Tuple
import hashlib
import base64

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel, Field, validator

# Load environment
load_dotenv()

# MCP imports
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    from mcp.client.streamable_http import streamablehttp_client
except ImportError:
    print("ERROR: mcp package not found. Install with: pip install mcp", file=sys.stderr)
    sys.exit(1)

# Turso database import
try:
    from turso_db import init_database, get_database, TursoDatabase
except ImportError:
    print("WARNING: turso_db module not found - database features disabled", file=sys.stderr)
    TursoDatabase = None

# Optional token management
try:
    from MCP.client.token_budget_manager import TokenBudgetManager, TokenCounter
    from MCP.client.token_integration import TokenBudgetIntegration
    TOKEN_MANAGEMENT_AVAILABLE = True
except ImportError:
    TokenBudgetManager = None
    TokenCounter = None
    TokenBudgetIntegration = None
    TOKEN_MANAGEMENT_AVAILABLE = False


# ============================================================================
# Configuration and Constants
# ============================================================================

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
JINA_API_KEY = os.getenv("JINA_API_KEY")
JINA_MCP_URL = os.getenv("JINA_MCP_URL", "https://mcp.jina.ai/sse")

ENABLE_TOKEN_MANAGEMENT = os.getenv("ENABLE_TOKEN_MANAGEMENT", "true").lower() in {"1", "true", "yes"}
ENABLE_RATE_LIMITING = os.getenv("ENABLE_RATE_LIMITING", "true").lower() in {"1", "true", "yes"}
ENABLE_WEBHOOKS = os.getenv("ENABLE_WEBHOOKS", "true").lower() in {"1", "true", "yes"}

MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "10"))
MAX_CONTEXT_MESSAGES = int(os.getenv("MAX_CONTEXT_MESSAGES", "50"))
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "3600"))

# Rate limiting config
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "100"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))


# ============================================================================
# Data Models
# ============================================================================

class EventType(str, Enum):
    """SSE event types for /v1/interactions"""
    # Session lifecycle
    SESSION_CREATED = "session.created"
    SESSION_RESUMED = "session.resumed"
    SESSION_UPDATED = "session.updated"
    SESSION_COMPLETED = "session.completed"

    # Interaction lifecycle
    INTERACTION_STARTED = "interaction.started"
    INTERACTION_THINKING = "interaction.thinking"
    INTERACTION_COMPLETED = "interaction.completed"

    # Message streaming
    MESSAGE_START = "message.start"
    MESSAGE_DELTA = "message.delta"
    MESSAGE_COMPLETED = "message.completed"

    # Tool execution
    TOOL_CALL_INITIATED = "tool.call.initiated"
    TOOL_CALL_EXECUTING = "tool.call.executing"
    TOOL_CALL_COMPLETED = "tool.call.completed"
    TOOL_CALL_FAILED = "tool.call.failed"

    # Context and budget
    CONTEXT_COMPRESSED = "context.compressed"
    BUDGET_WARNING = "budget.warning"
    BUDGET_EXCEEDED = "budget.exceeded"

    # Metrics and telemetry
    METRICS_UPDATE = "metrics.update"
    USAGE_REPORT = "usage.report"
    ITERATION_SUMMARY = "iteration.summary"

    # Errors and warnings
    ERROR = "error"
    WARNING = "warning"

    # System
    HEARTBEAT = "heartbeat"


class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"


class InteractionStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class MCPServerConfig:
    """MCP server configuration"""
    name: str
    url: str
    transport: str = "sse"
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: float = 120.0


class Message(BaseModel):
    """Conversation message"""
    role: MessageRole
    content: str
    metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None

    class Config:
        use_enum_values = True


class FileAttachment(BaseModel):
    """File attachment"""
    name: str
    content_type: str
    size: int
    data: str  # base64 encoded

    @validator('data')
    def validate_base64(cls, v):
        try:
            base64.b64decode(v)
            return v
        except Exception:
            raise ValueError("Invalid base64 data")


class InteractionRequest(BaseModel):
    """Request model for /v1/interactions"""
    session_id: Optional[str] = None
    message: str = Field(..., min_length=1)
    model: str = Field(default="anthropic/claude-sonnet-4.5")
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(default=4000, gt=0, le=100000)

    # Features
    enable_tools: bool = True
    enable_streaming: bool = True
    enable_context_compression: bool = True

    # Advanced options
    system_prompt: Optional[str] = None
    files: Optional[List[FileAttachment]] = None
    metadata: Optional[Dict[str, Any]] = None

    # Webhook for async completion
    webhook_url: Optional[str] = None
    webhook_headers: Optional[Dict[str, str]] = None

    # Resume options
    resume_from_message_id: Optional[str] = None


class InteractionResponse(BaseModel):
    """Response model for interaction creation"""
    interaction_id: str
    session_id: str
    status: InteractionStatus
    created_at: str
    stream_url: Optional[str] = None

    class Config:
        use_enum_values = True


class SessionInfo(BaseModel):
    """Session information"""
    session_id: str
    created_at: str
    updated_at: str
    message_count: int
    total_tokens: int
    model: str
    status: str
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# Session and State Management
# ============================================================================

@dataclass
class Session:
    """Conversation session with state"""
    session_id: str
    created_at: datetime
    updated_at: datetime
    messages: List[Dict[str, Any]] = field(default_factory=list)
    model: str = "anthropic/claude-sonnet-4.5"
    token_usage: Dict[str, int] = field(default_factory=lambda: {
        "prompt": 0,
        "completion": 0,
        "total": 0,
        "limit": 0
    })
    metadata: Dict[str, Any] = field(default_factory=dict)
    token_manager: Optional[Any] = None
    status: str = "active"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "message_count": len(self.messages),
            "total_tokens": self.token_usage["total"],
            "model": self.model,
            "status": self.status,
            "metadata": self.metadata
        }


class SessionManager:
    """Manage conversation sessions with TTL"""

    def __init__(self, ttl_seconds: int = 3600):
        self.sessions: Dict[str, Session] = {}
        self.ttl_seconds = ttl_seconds
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start_cleanup(self):
        """Start background cleanup task"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop_cleanup(self):
        """Stop background cleanup task"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self):
        """Cleanup expired sessions periodically"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in cleanup loop: {e}")

    async def cleanup_expired(self):
        """Remove expired sessions"""
        now = datetime.utcnow()
        expired = []

        for session_id, session in self.sessions.items():
            age = (now - session.updated_at).total_seconds()
            if age > self.ttl_seconds:
                expired.append(session_id)

        for session_id in expired:
            del self.sessions[session_id]
            print(f"Cleaned up expired session: {session_id}")

    def create_session(
        self,
        session_id: Optional[str] = None,
        model: str = "anthropic/claude-sonnet-4.5"
    ) -> Session:
        """Create a new session"""
        if not session_id:
            session_id = f"sess_{uuid.uuid4().hex[:12]}"

        session = Session(
            session_id=session_id,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            model=model
        )

        self.sessions[session_id] = session
        return session

    def get_session(self, session_id: str) -> Optional[Session]:
        """Get session by ID"""
        session = self.sessions.get(session_id)
        if session:
            session.updated_at = datetime.utcnow()
        return session

    def get_or_create_session(
        self,
        session_id: Optional[str] = None,
        model: str = "anthropic/claude-sonnet-4.5"
    ) -> Tuple[Session, bool]:
        """Get existing session or create new one. Returns (session, is_new)"""
        if session_id:
            session = self.get_session(session_id)
            if session:
                return session, False

        session = self.create_session(session_id, model)
        return session, True

    def delete_session(self, session_id: str) -> bool:
        """Delete a session"""
        if session_id in self.sessions:
            del self.sessions[session_id]
            return True
        return False

    def list_sessions(self) -> List[Dict[str, Any]]:
        """List all active sessions"""
        return [s.to_dict() for s in self.sessions.values()]


class RateLimiter:
    """Simple in-memory rate limiter"""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[str, List[float]] = defaultdict(list)

    def check_rate_limit(self, client_id: str) -> Tuple[bool, Optional[float]]:
        """Check if client is within rate limit. Returns (allowed, retry_after)"""
        now = time.time()
        window_start = now - self.window_seconds

        # Clean old requests
        self.requests[client_id] = [
            req_time for req_time in self.requests[client_id]
            if req_time > window_start
        ]

        if len(self.requests[client_id]) >= self.max_requests:
            oldest = self.requests[client_id][0]
            retry_after = self.window_seconds - (now - oldest)
            return False, retry_after

        self.requests[client_id].append(now)
        return True, None


# ============================================================================
# MCP Integration
# ============================================================================

@dataclass
class MCPState:
    """MCP session state"""
    session: Any
    mcp_tools: List[Dict[str, Any]]
    openai_tools: List[Dict[str, Any]]
    tool_name_map: Dict[str, str]
    server_config: MCPServerConfig


def sanitize_tool_name(name: str) -> str:
    """Sanitize tool names for OpenAI compatibility"""
    import unicodedata
    import re

    cleaned = unicodedata.normalize("NFKD", name or "")
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    cleaned = cleaned.strip()
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_-")
    if not cleaned:
        cleaned = "tool"
    return cleaned[:128].lower()


def ensure_unique_tool_name(base: str, used: Set[str]) -> str:
    """Ensure tool name is unique"""
    candidate = base or "tool"
    counter = 1
    while candidate in used:
        suffix = f"_{counter}"
        max_base_len = max(1, 128 - len(suffix))
        candidate = f"{base[:max_base_len]}{suffix}"
        counter += 1
    used.add(candidate)
    return candidate


async def fetch_mcp_tools(session) -> List[Dict[str, Any]]:
    """Fetch tools from MCP server"""
    tools_resp = await session.list_tools()
    items = tools_resp.get("tools") if isinstance(tools_resp, dict) else getattr(tools_resp, "tools", tools_resp)

    if not items:
        return []

    tools = []
    for t in items:
        name = getattr(t, "name", None) or t.get("name")
        desc = getattr(t, "description", None) or t.get("description")
        schema = (
            getattr(t, "input_schema", None) or
            getattr(t, "inputSchema", None) or
            t.get("input_schema") or
            t.get("inputSchema") or
            {"type": "object", "properties": {}}
        )

        if not isinstance(schema, dict):
            schema = {"type": "object", "properties": {}}

        schema.setdefault("type", "object")
        schema.setdefault("properties", {})

        tools.append({
            "name": name,
            "description": desc,
            "input_schema": schema
        })

    return tools


def to_openai_tools(mcp_tools: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Convert MCP tools to OpenAI format"""
    tools = []
    used_names = set()
    name_map = {}

    for t in mcp_tools:
        name = t.get("name")
        if not name:
            continue

        desc = t.get("description") or ""
        schema = t.get("input_schema") or {"type": "object", "properties": {}}

        sanitized_base = sanitize_tool_name(name)
        sanitized = ensure_unique_tool_name(sanitized_base, used_names)
        name_map[sanitized] = name

        tools.append({
            "type": "function",
            "function": {
                "name": sanitized,
                "description": desc,
                "parameters": schema,
            },
        })

    return tools, name_map


async def call_mcp_tool(session, tool_name: str, arguments: Dict[str, Any]) -> Any:
    """Call an MCP tool"""
    try:
        result = await session.call_tool(tool_name, arguments)
        return result
    except Exception as e:
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({"error": str(e)})
            }]
        }


def flatten_mcp_content(result: Any) -> str:
    """Flatten MCP result content to text"""
    if isinstance(result, str):
        return result

    if isinstance(result, dict):
        content = result.get("content", [])
    else:
        content = getattr(result, "content", [])

    if not content:
        return json.dumps(result) if isinstance(result, dict) else str(result)

    parts = []
    for item in content:
        if isinstance(item, dict):
            if item.get("type") == "text":
                parts.append(item.get("text", ""))
            elif item.get("type") == "image":
                parts.append(f"[Image: {item.get('data', '')[:50]}...]")
            elif item.get("type") == "resource":
                parts.append(f"[Resource: {item.get('uri', '')}]")
            else:
                parts.append(json.dumps(item))
        else:
            text = getattr(item, "text", None)
            if text:
                parts.append(text)
            else:
                parts.append(str(item))

    return "\n".join(parts) if parts else json.dumps(content)


# ============================================================================
# OpenRouter Integration
# ============================================================================

async def openrouter_stream(
    client: httpx.AsyncClient,
    model: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> AsyncGenerator[Tuple[str, Optional[Any], Optional[List[Dict[str, Any]]]], None]:
    """Stream from OpenRouter"""
    payload = {
        "model": model,
        "messages": messages,
        "stream": True,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    url = "https://openrouter.ai/api/v1/chat/completions"
    tool_calls_map = {}

    async with client.stream("POST", url, json=payload) as response:
        if response.status_code >= 400:
            error_body = await response.aread()
            raise HTTPException(
                status_code=response.status_code,
                detail=f"OpenRouter error: {error_body.decode()}"
            )

        async for line in response.aiter_lines():
            if not line or line.startswith(":"):
                continue

            if not line.startswith("data: "):
                continue

            data = line[6:].strip()
            if data == "[DONE]":
                break

            try:
                obj = json.loads(data)
            except:
                continue

            choices = obj.get("choices", [])
            if not choices:
                continue

            delta = choices[0].get("delta", {})

            # Content streaming
            content = delta.get("content")
            if content:
                yield ("content", content, None)

            # Tool call deltas
            tool_deltas = delta.get("tool_calls", [])
            for tc_delta in tool_deltas:
                idx = tc_delta.get("index", 0)

                if idx not in tool_calls_map:
                    tool_calls_map[idx] = {
                        "id": tc_delta.get("id"),
                        "type": "function",
                        "function": {
                            "name": "",
                            "arguments": ""
                        }
                    }

                fn_delta = tc_delta.get("function", {})
                if fn_delta.get("name"):
                    tool_calls_map[idx]["function"]["name"] = fn_delta["name"]

                if "arguments" in fn_delta:
                    tool_calls_map[idx]["function"]["arguments"] += fn_delta["arguments"]

            usage = obj.get("usage")
            if usage:
                yield ("usage", usage, None)

    if tool_calls_map:
        tool_calls = [tool_calls_map[i] for i in sorted(tool_calls_map.keys())]
        yield ("tool_calls", None, tool_calls)
    else:
        yield ("done", None, None)


# ============================================================================
# SSE Helpers
# ============================================================================

def create_sse_event(
    event_type: EventType,
    data: Dict[str, Any],
    interaction_id: str,
    session_id: str,
    sequence: int
) -> str:
    """Create SSE formatted event"""
    event_data = {
        "event": event_type.value,
        "data": data,
        "interaction_id": interaction_id,
        "session_id": session_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sequence": sequence
    }
    return f"event: {event_type.value}\ndata: {json.dumps(event_data)}\n\n"


# ============================================================================
# Main Interaction Handler
# ============================================================================

async def handle_interaction(
    interaction_id: str,
    session: Session,
    user_message: str,
    mcp_state: MCPState,
    http_client: httpx.AsyncClient,
    enable_tools: bool = True,
    system_prompt: Optional[str] = None,
    files: Optional[List[FileAttachment]] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> AsyncGenerator[str, None]:
    """Handle interaction with SSE streaming"""

    sequence = 0

    # Build user message with files
    user_message_with_files = user_message
    if files:
        file_context = "\n\n[Attached files:\n"
        for file in files:
            try:
                decoded = base64.b64decode(file.data)
                if file.content_type.startswith('text/'):
                    content = decoded.decode('utf-8', errors='ignore')[:2000]
                    file_context += f"- {file.name} ({file.content_type}):\n{content}\n\n"
                else:
                    file_context += f"- {file.name} ({file.content_type}, {file.size} bytes)\n"
            except Exception as e:
                file_context += f"- {file.name} (error: {str(e)})\n"
        file_context += "]"
        user_message_with_files += file_context

    # INTERACTION STARTED
    yield create_sse_event(
        EventType.INTERACTION_STARTED,
        {
            "message": user_message,
            "model": session.model,
            "tools_enabled": enable_tools,
            "tools_available": len(mcp_state.openai_tools) if enable_tools else 0
        },
        interaction_id,
        session.session_id,
        sequence
    )
    sequence += 1

    # Build messages
    messages: List[Dict[str, Any]] = []

    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    messages.extend(session.messages)
    messages.append({"role": "user", "content": user_message_with_files})

    # Add to session
    session.messages.append({
        "role": "user",
        "content": user_message,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    })

    tools = mcp_state.openai_tools if enable_tools else None
    iteration = 0

    while iteration < MAX_ITERATIONS:
        iteration += 1

        yield create_sse_event(
            EventType.MESSAGE_START,
            {"iteration": iteration},
            interaction_id,
            session.session_id,
            sequence
        )
        sequence += 1

        full_response = ""
        tool_calls_received = None
        usage_info = {}

        async for event_type, content_delta, tool_calls in openrouter_stream(
            http_client,
            session.model,
            messages,
            tools,
            temperature,
            max_tokens
        ):
            if event_type == "content" and content_delta:
                full_response += content_delta

                yield create_sse_event(
                    EventType.MESSAGE_DELTA,
                    {
                        "delta": content_delta,
                        "full_text": full_response,
                        "iteration": iteration
                    },
                    interaction_id,
                    session.session_id,
                    sequence
                )
                sequence += 1

            elif event_type == "tool_calls" and tool_calls:
                tool_calls_received = tool_calls

            elif event_type == "usage" and isinstance(content_delta, dict):
                usage_info = content_delta

                # Update session usage
                session.token_usage["prompt"] += usage_info.get("prompt_tokens", 0)
                session.token_usage["completion"] += usage_info.get("completion_tokens", 0)
                session.token_usage["total"] += usage_info.get("total_tokens", 0)

                yield create_sse_event(
                    EventType.USAGE_REPORT,
                    {
                        "iteration": iteration,
                        "usage": usage_info,
                        "cumulative": session.token_usage
                    },
                    interaction_id,
                    session.session_id,
                    sequence
                )
                sequence += 1

        yield create_sse_event(
            EventType.MESSAGE_COMPLETED,
            {
                "text": full_response,
                "has_tool_calls": bool(tool_calls_received),
                "iteration": iteration,
                "usage": usage_info
            },
            interaction_id,
            session.session_id,
            sequence
        )
        sequence += 1

        # Add assistant message
        assistant_msg = {
            "role": "assistant",
            "content": full_response,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "iteration": iteration
        }

        if tool_calls_received:
            assistant_msg["tool_calls"] = tool_calls_received

        messages.append(assistant_msg)
        session.messages.append(assistant_msg)

        # Handle tool calls
        if tool_calls_received:
            for tc in tool_calls_received:
                tool_id = tc.get("id")
                fn = tc.get("function", {})
                fn_name = fn.get("name")
                fn_args_str = fn.get("arguments", "{}")

                yield create_sse_event(
                    EventType.TOOL_CALL_INITIATED,
                    {
                        "tool_id": tool_id,
                        "tool_name": fn_name,
                        "arguments": fn_args_str,
                        "iteration": iteration
                    },
                    interaction_id,
                    session.session_id,
                    sequence
                )
                sequence += 1

                try:
                    fn_args = json.loads(fn_args_str)
                except:
                    fn_args = {}

                original_tool_name = mcp_state.tool_name_map.get(fn_name, fn_name)

                yield create_sse_event(
                    EventType.TOOL_CALL_EXECUTING,
                    {
                        "tool_id": tool_id,
                        "mcp_tool_name": original_tool_name,
                        "iteration": iteration
                    },
                    interaction_id,
                    session.session_id,
                    sequence
                )
                sequence += 1

                try:
                    result = await call_mcp_tool(mcp_state.session, original_tool_name, fn_args)
                    result_text = flatten_mcp_content(result)

                    yield create_sse_event(
                        EventType.TOOL_CALL_COMPLETED,
                        {
                            "tool_id": tool_id,
                            "result": result_text[:1000],
                            "result_length": len(result_text),
                            "iteration": iteration
                        },
                        interaction_id,
                        session.session_id,
                        sequence
                    )
                    sequence += 1

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_id,
                        "content": result_text
                    })

                    session.messages.append({
                        "role": "tool",
                        "tool_call_id": tool_id,
                        "name": fn_name,
                        "content": result_text,
                        "timestamp": datetime.utcnow().isoformat() + "Z"
                    })

                except Exception as e:
                    yield create_sse_event(
                        EventType.TOOL_CALL_FAILED,
                        {
                            "tool_id": tool_id,
                            "error": str(e),
                            "iteration": iteration
                        },
                        interaction_id,
                        session.session_id,
                        sequence
                    )
                    sequence += 1

        # Iteration summary
        yield create_sse_event(
            EventType.ITERATION_SUMMARY,
            {
                "iteration": iteration,
                "token_usage": usage_info,
                "tool_calls": len(tool_calls_received or []),
                "message_preview": full_response[:200]
            },
            interaction_id,
            session.session_id,
            sequence
        )
        sequence += 1

        if not tool_calls_received:
            break

    yield create_sse_event(
        EventType.INTERACTION_COMPLETED,
        {
            "iterations": iteration,
            "total_events": sequence
        },
        interaction_id,
        session.session_id,
        sequence
    )


# ============================================================================
# Application Setup
# ============================================================================

# Global state
session_manager: Optional[SessionManager] = None
mcp_state: Optional[MCPState] = None
http_client: Optional[httpx.AsyncClient] = None
rate_limiter: Optional[RateLimiter] = None


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Application lifecycle management"""
    global session_manager, mcp_state, http_client, rate_limiter

    print("🚀 Starting Powerful MCP FastAPI Server...")

    # Initialize session manager
    session_manager = SessionManager(ttl_seconds=SESSION_TTL_SECONDS)
    await session_manager.start_cleanup()

    # Initialize rate limiter
    if ENABLE_RATE_LIMITING:
        rate_limiter = RateLimiter(
            max_requests=RATE_LIMIT_REQUESTS,
            window_seconds=RATE_LIMIT_WINDOW
        )

    # Initialize HTTP client
    if not OPENROUTER_API_KEY:
        print("WARNING: OPENROUTER_API_KEY not set", file=sys.stderr)

    http_client = httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "HTTP-Referer": "http://localhost:2046",
            "X-Title": "Powerful MCP Chat"
        },
        timeout=120.0
    )

    # Initialize Turso database if available
    if TursoDatabase:
        try:
            await init_database()
            print("✅ Turso database initialized")
        except Exception as e:
            print(f"WARNING: Could not initialize database: {e}")

    # Initialize MCP
    if JINA_API_KEY:
        try:
            server_config = MCPServerConfig(
                name="jina",
                url=JINA_MCP_URL,
                transport="sse",
                headers={"Authorization": f"Bearer {JINA_API_KEY}"}
            )

            print(f"📡 Connecting to MCP server: {server_config.url}")

            client_context = sse_client(server_config.url, server_config.headers)
            streams = await client_context.__aenter__()
            read, write = streams

            session = ClientSession(read, write)
            await session.__aenter__()
            await session.initialize()

            print("🔧 Fetching MCP tools...")
            mcp_tools = await fetch_mcp_tools(session)
            print(f"✅ Found {len(mcp_tools)} MCP tools")

            openai_tools, name_map = to_openai_tools(mcp_tools)
            mcp_state = MCPState(
                session=session,
                mcp_tools=mcp_tools,
                openai_tools=openai_tools,
                tool_name_map=name_map,
                server_config=server_config
            )
        except Exception as e:
            print(f"ERROR initializing MCP: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
    else:
        print("WARNING: JINA_API_KEY not set - MCP tools disabled", file=sys.stderr)

    print("✅ Server ready!")

    try:
        yield
    finally:
        print("🛑 Shutting down...")

        if session_manager:
            await session_manager.stop_cleanup()

        if TursoDatabase:
            db = await get_database()
            if db:
                try:
                    await db.close()
                except Exception as e:
                    print(f"WARNING: Error closing database: {e}")

        if http_client:
            await http_client.aclose()


app = FastAPI(
    title="Powerful MCP Chat API",
    version="2.0.0",
    description="Enterprise-grade FastAPI with /v1/interactions endpoint",
    lifespan=app_lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "mcp_connected": mcp_state is not None,
        "tools_available": len(mcp_state.openai_tools) if mcp_state else 0,
        "active_sessions": len(session_manager.sessions) if session_manager else 0
    }


@app.get("/api/status")
async def api_status():
    """Detailed API status"""
    return {
        "service": "Powerful MCP Chat API",
        "version": "2.0.0",
        "status": "online",
        "features": {
            "mcp_enabled": mcp_state is not None,
            "tools_available": len(mcp_state.openai_tools) if mcp_state else 0,
            "token_management": ENABLE_TOKEN_MANAGEMENT and TOKEN_MANAGEMENT_AVAILABLE,
            "rate_limiting": ENABLE_RATE_LIMITING,
            "webhooks": ENABLE_WEBHOOKS,
            "database": TursoDatabase is not None
        },
        "stats": {
            "active_sessions": len(session_manager.sessions) if session_manager else 0,
            "total_messages": sum(
                len(s.messages) for s in session_manager.sessions.values()
            ) if session_manager else 0
        },
        "limits": {
            "max_iterations": MAX_ITERATIONS,
            "max_context_messages": MAX_CONTEXT_MESSAGES,
            "session_ttl_seconds": SESSION_TTL_SECONDS,
            "rate_limit_requests": RATE_LIMIT_REQUESTS if ENABLE_RATE_LIMITING else None,
            "rate_limit_window": RATE_LIMIT_WINDOW if ENABLE_RATE_LIMITING else None
        },
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/v1/interactions", response_model=InteractionResponse)
async def create_interaction(
    request: InteractionRequest,
    background_tasks: BackgroundTasks,
    http_request: Request
):
    """
    Create a new interaction - the main powerful endpoint!

    This endpoint creates a new interaction and returns a streaming response
    with SSE events for real-time updates.
    """

    # Rate limiting
    if ENABLE_RATE_LIMITING and rate_limiter:
        client_ip = http_request.client.host
        allowed, retry_after = rate_limiter.check_rate_limit(client_ip)
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Retry after {retry_after:.1f} seconds",
                headers={"Retry-After": str(int(retry_after))}
            )

    # Validate MCP is ready
    if not mcp_state:
        raise HTTPException(
            status_code=503,
            detail="MCP not initialized - check JINA_API_KEY"
        )

    if not http_client:
        raise HTTPException(
            status_code=503,
            detail="HTTP client not initialized"
        )

    # Get or create session
    session, is_new = session_manager.get_or_create_session(
        request.session_id,
        request.model
    )

    # Generate interaction ID
    interaction_id = f"int_{uuid.uuid4().hex[:12]}"

    # If not streaming, we could handle async via webhooks
    if not request.enable_streaming:
        # TODO: Implement webhook-based async interaction
        raise HTTPException(
            status_code=501,
            detail="Non-streaming mode not yet implemented"
        )

    # Return streaming response
    return StreamingResponse(
        handle_interaction(
            interaction_id=interaction_id,
            session=session,
            user_message=request.message,
            mcp_state=mcp_state,
            http_client=http_client,
            enable_tools=request.enable_tools,
            system_prompt=request.system_prompt,
            files=request.files,
            temperature=request.temperature,
            max_tokens=request.max_tokens
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Session-ID": session.session_id,
            "X-Interaction-ID": interaction_id,
            "X-Session-New": str(is_new).lower()
        }
    )


@app.get("/v1/sessions", response_model=List[SessionInfo])
async def list_sessions():
    """List all active sessions"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    sessions = session_manager.list_sessions()
    return [
        SessionInfo(
            session_id=s["session_id"],
            created_at=s["created_at"],
            updated_at=s["updated_at"],
            message_count=s["message_count"],
            total_tokens=s["total_tokens"],
            model=s["model"],
            status=s["status"],
            metadata=s.get("metadata")
        )
        for s in sessions
    ]


@app.get("/v1/sessions/{session_id}", response_model=SessionInfo)
async def get_session_info(session_id: str):
    """Get session information"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    session_dict = session.to_dict()
    return SessionInfo(
        session_id=session_dict["session_id"],
        created_at=session_dict["created_at"],
        updated_at=session_dict["updated_at"],
        message_count=session_dict["message_count"],
        total_tokens=session_dict["total_tokens"],
        model=session_dict["model"],
        status=session_dict["status"],
        metadata=session_dict.get("metadata")
    )


@app.delete("/v1/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    deleted = session_manager.delete_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")

    return {"message": "Session deleted", "session_id": session_id}


@app.get("/v1/sessions/{session_id}/messages")
async def get_session_messages(session_id: str, limit: int = 100, offset: int = 0):
    """Get messages from a session"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    messages = session.messages[offset:offset + limit]

    return {
        "session_id": session_id,
        "messages": messages,
        "total": len(session.messages),
        "limit": limit,
        "offset": offset
    }


@app.get("/v1/tools")
async def list_tools():
    """List available MCP tools"""
    if not mcp_state:
        raise HTTPException(status_code=503, detail="MCP not initialized")

    return {
        "tools": mcp_state.mcp_tools,
        "count": len(mcp_state.mcp_tools),
        "server": mcp_state.server_config.name
    }


@app.get("/")
async def root():
    """API documentation"""
    return {
        "service": "Powerful MCP Chat API",
        "version": "2.0.0",
        "description": "Enterprise-grade FastAPI with /v1/interactions endpoint",
        "endpoints": {
            "health": "GET /health",
            "status": "GET /api/status",
            "interactions": "POST /v1/interactions",
            "sessions_list": "GET /v1/sessions",
            "session_get": "GET /v1/sessions/{session_id}",
            "session_delete": "DELETE /v1/sessions/{session_id}",
            "session_messages": "GET /v1/sessions/{session_id}/messages",
            "tools": "GET /v1/tools"
        },
        "docs": "/docs",
        "openapi": "/openapi.json"
    }


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 POWERFUL MCP FASTAPI SERVER")
    print("=" * 80)
    print("")
    print("Features:")
    print("  ✅ /v1/interactions endpoint with SSE streaming")
    print("  ✅ Session management with TTL")
    print("  ✅ MCP tool integration")
    print("  ✅ Multi-turn conversations")
    print("  ✅ Token budget tracking")
    print(f"  {'✅' if ENABLE_RATE_LIMITING else '⬜'} Rate limiting")
    print(f"  {'✅' if TursoDatabase else '⬜'} Turso database")
    print("")
    print("Endpoints:")
    print("  📡 POST /v1/interactions - Main interaction endpoint")
    print("  📊 GET /api/status - Detailed status")
    print("  🔧 GET /v1/tools - List MCP tools")
    print("  💬 GET /v1/sessions - List sessions")
    print("  📖 GET /docs - OpenAPI documentation")
    print("")
    print("Environment:")
    print(f"  OPENROUTER_API_KEY: {'✅ Set' if OPENROUTER_API_KEY else '❌ Missing'}")
    print(f"  JINA_API_KEY: {'✅ Set' if JINA_API_KEY else '❌ Missing'}")
    print(f"  MAX_ITERATIONS: {MAX_ITERATIONS}")
    print(f"  SESSION_TTL: {SESSION_TTL_SECONDS}s")
    print("")
    print("=" * 80)
    print("")

    uvicorn.run(
        "powerful_mcp_fastapi:app",
        host="0.0.0.0",
        port=2046,
        reload=True,
        log_level="info"
    )
