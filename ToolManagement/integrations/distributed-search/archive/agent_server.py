#!/usr/bin/env python3
"""
Agent Server - A powerful /v1/interactions endpoint that wraps entire agent flows
==================================================================================

This server provides a streamlined /v1/interactions endpoint that:
1. Streams tokens for simple text responses ("say ok" -> streaming tokens)
2. Streams complete end-to-end flows for complex multi-tool interactions
3. Intelligently manages context to avoid pollution
4. Provides comprehensive observability with SSE events

Features:
- Smart context management with automatic summarization
- Adaptive streaming based on interaction complexity
- Tool call orchestration with parallel execution support
- Session persistence with Turso integration
- Token budget optimization
- Real-time metrics and telemetry

Usage:
    export OPENROUTER_API_KEY=your_key
    export JINA_API_KEY=your_key

    python agent_server.py

    # Then make requests:
    curl -X POST http://localhost:2046/v1/interactions \
      -H "Content-Type: application/json" \
      -d '{"message": "say ok"}'

    curl -X POST http://localhost:2046/v1/interactions \
      -H "Content-Type: application/json" \
      -d '{"message": "get the weather in 12 cities"}'
"""

import asyncio
import json
import os
import sys
import time
import uuid
import hashlib
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Tuple
import re

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from pydantic import BaseModel, Field

# Load environment
load_dotenv()

# MCP imports
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
except ImportError:
    print("ERROR: mcp package not found. Install with: pip install mcp", file=sys.stderr)
    sys.exit(1)

# Optional Turso database
try:
    from turso_db import init_database, get_database, TursoDatabase
    TURSO_AVAILABLE = True
except ImportError:
    print("INFO: turso_db not available - persistence disabled", file=sys.stderr)
    TursoDatabase = None
    TURSO_AVAILABLE = False

# Tool Retrieval Service
try:
    from tool_retrieval_service import ToolRetrievalService
    TOOL_RETRIEVAL_AVAILABLE = True
except ImportError:
    print("INFO: tool_retrieval_service not available - using all tools", file=sys.stderr)
    ToolRetrievalService = None
    TOOL_RETRIEVAL_AVAILABLE = False


# ============================================================================
# Configuration
# ============================================================================

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
JINA_API_KEY = os.getenv("JINA_API_KEY")
JINA_MCP_URL = os.getenv("JINA_MCP_URL", "https://mcp.jina.ai/sse")

MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "15"))
MAX_CONTEXT_MESSAGES = int(os.getenv("MAX_CONTEXT_MESSAGES", "100"))
CONTEXT_COMPRESSION_THRESHOLD = int(os.getenv("CONTEXT_COMPRESSION_THRESHOLD", "50"))
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "7200"))

# Smart defaults
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "anthropic/claude-sonnet-4.5")
DEFAULT_TEMPERATURE = float(os.getenv("DEFAULT_TEMPERATURE", "0.7"))
DEFAULT_MAX_TOKENS = int(os.getenv("DEFAULT_MAX_TOKENS", "8000"))


# ============================================================================
# Event Types for SSE Streaming
# ============================================================================

class EventType(str, Enum):
    """All possible SSE event types"""
    # Session
    SESSION_START = "session.start"
    SESSION_RESUME = "session.resume"
    SESSION_COMPLETE = "session.complete"

    # Interaction lifecycle
    INTERACTION_START = "interaction.start"
    INTERACTION_ANALYZING = "interaction.analyzing"
    INTERACTION_COMPLETE = "interaction.complete"

    # Message streaming
    MESSAGE_START = "message.start"
    MESSAGE_CHUNK = "message.chunk"
    MESSAGE_COMPLETE = "message.complete"

    # Tool execution
    TOOL_RETRIEVAL = "tool.retrieval"
    TOOL_PLAN = "tool.plan"
    TOOL_EXECUTE_START = "tool.execute.start"
    TOOL_EXECUTE_PROGRESS = "tool.execute.progress"
    TOOL_EXECUTE_COMPLETE = "tool.execute.complete"
    TOOL_EXECUTE_ERROR = "tool.execute.error"
    TOOL_PARALLEL_START = "tool.parallel.start"
    TOOL_PARALLEL_PROGRESS = "tool.parallel.progress"
    TOOL_PARALLEL_COMPLETE = "tool.parallel.complete"

    # Context management
    CONTEXT_UPDATE = "context.update"
    CONTEXT_COMPRESS = "context.compress"
    CONTEXT_SUMMARY = "context.summary"

    # Metrics
    METRICS = "metrics"
    USAGE = "usage"
    ITERATION = "iteration"

    # System
    ERROR = "error"
    WARNING = "warning"
    DEBUG = "debug"


# ============================================================================
# Data Models
# ============================================================================

class InteractionRequest(BaseModel):
    """Request to /v1/interactions endpoint"""
    message: str = Field(..., min_length=1, description="User message")
    session_id: Optional[str] = Field(None, description="Session ID for continuity")

    # Model configuration
    model: str = Field(default=DEFAULT_MODEL)
    temperature: float = Field(default=DEFAULT_TEMPERATURE, ge=0.0, le=2.0)
    max_tokens: int = Field(default=DEFAULT_MAX_TOKENS, gt=0)

    # Behavior
    enable_tools: bool = Field(default=True, description="Enable MCP tool execution")
    enable_context_compression: bool = Field(default=True, description="Compress context when needed")
    enable_parallel_tools: bool = Field(default=True, description="Execute independent tools in parallel")

    # Advanced
    system_prompt: Optional[str] = None
    max_iterations: Optional[int] = Field(default=None, ge=1, le=50)
    metadata: Optional[Dict[str, Any]] = None


class SessionInfo(BaseModel):
    """Session information"""
    session_id: str
    created_at: str
    updated_at: str
    message_count: int
    total_tokens: int
    model: str
    compressed: bool = False


# ============================================================================
# Session Management with Smart Context
# ============================================================================

@dataclass
class ContextMessage:
    """A message in the context with metadata"""
    role: str
    content: str
    timestamp: datetime
    tokens: int = 0
    compressed: bool = False
    tool_calls: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class Session:
    """Agent session with intelligent context management"""
    session_id: str
    created_at: datetime
    updated_at: datetime
    model: str

    # Context storage
    messages: List[ContextMessage] = field(default_factory=list)
    compressed_summary: Optional[str] = None

    # Metrics
    total_tokens: int = 0
    total_interactions: int = 0
    total_tool_calls: int = 0

    # State
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_message(
        self,
        role: str,
        content: str,
        tokens: int = 0,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ContextMessage:
        """Add a message to the context"""
        msg = ContextMessage(
            role=role,
            content=content,
            timestamp=datetime.utcnow(),
            tokens=tokens,
            tool_calls=tool_calls,
            metadata=metadata
        )
        self.messages.append(msg)
        self.total_tokens += tokens
        self.updated_at = datetime.utcnow()
        return msg

    def get_context_for_llm(
        self,
        max_messages: Optional[int] = None,
        system_prompt: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Build context for LLM, optionally compressing old messages"""
        context = []

        # Add system prompt
        if system_prompt:
            context.append({"role": "system", "content": system_prompt})

        # Add compressed summary if exists
        if self.compressed_summary:
            context.append({
                "role": "system",
                "content": f"Previous conversation summary:\n{self.compressed_summary}"
            })

        # Add recent messages
        recent_messages = self.messages
        if max_messages:
            recent_messages = self.messages[-max_messages:]

        for msg in recent_messages:
            if msg.compressed:
                continue  # Skip individually compressed messages

            message_dict = {
                "role": msg.role,
                "content": msg.content
            }

            if msg.tool_calls:
                message_dict["tool_calls"] = msg.tool_calls

            context.append(message_dict)

        return context

    def compress_old_context(self, keep_recent: int = 10) -> str:
        """Compress old messages into a summary"""
        if len(self.messages) <= keep_recent:
            return ""

        to_compress = self.messages[:-keep_recent]

        # Create summary
        summary_parts = []
        for msg in to_compress:
            if msg.role == "user":
                summary_parts.append(f"User: {msg.content[:100]}")
            elif msg.role == "assistant":
                summary_parts.append(f"Assistant: {msg.content[:100]}")
            elif msg.role == "tool" and msg.metadata:
                tool_name = msg.metadata.get("tool_name", "unknown")
                summary_parts.append(f"Tool ({tool_name}): executed")

        compressed = "\n".join(summary_parts)
        self.compressed_summary = compressed

        # Mark messages as compressed
        for msg in to_compress:
            msg.compressed = True

        return compressed

    def to_dict(self) -> Dict[str, Any]:
        """Serialize session info"""
        return {
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "message_count": len(self.messages),
            "total_tokens": self.total_tokens,
            "total_interactions": self.total_interactions,
            "total_tool_calls": self.total_tool_calls,
            "model": self.model,
            "compressed": bool(self.compressed_summary),
            "metadata": self.metadata
        }


class SessionManager:
    """Manage sessions with TTL and cleanup"""

    def __init__(self, ttl_seconds: int = 3600):
        self.sessions: Dict[str, Session] = {}
        self.ttl_seconds = ttl_seconds
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start_cleanup(self):
        """Start background cleanup"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop_cleanup(self):
        """Stop background cleanup"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def _cleanup_loop(self):
        """Periodically clean up expired sessions"""
        while True:
            try:
                await asyncio.sleep(60)
                await self.cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Cleanup error: {e}", file=sys.stderr)

    async def cleanup_expired(self):
        """Remove expired sessions"""
        now = datetime.utcnow()
        expired = [
            sid for sid, sess in self.sessions.items()
            if (now - sess.updated_at).total_seconds() > self.ttl_seconds
        ]

        for sid in expired:
            del self.sessions[sid]
            print(f"Cleaned up expired session: {sid}")

    def get_or_create(
        self,
        session_id: Optional[str] = None,
        model: str = DEFAULT_MODEL
    ) -> Tuple[Session, bool]:
        """Get existing or create new session. Returns (session, is_new)"""
        if session_id and session_id in self.sessions:
            session = self.sessions[session_id]
            session.updated_at = datetime.utcnow()
            return session, False

        # Create new
        if not session_id:
            session_id = f"sess_{uuid.uuid4().hex[:16]}"

        session = Session(
            session_id=session_id,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            model=model
        )

        self.sessions[session_id] = session
        return session, True

    def get(self, session_id: str) -> Optional[Session]:
        """Get session by ID"""
        return self.sessions.get(session_id)

    def delete(self, session_id: str) -> bool:
        """Delete session"""
        if session_id in self.sessions:
            del self.sessions[session_id]
            return True
        return False

    def list_all(self) -> List[Dict[str, Any]]:
        """List all sessions"""
        return [s.to_dict() for s in self.sessions.values()]


# ============================================================================
# MCP Integration
# ============================================================================

@dataclass
class MCPServerRegistration:
    """Single MCP server registration"""
    server_id: str
    name: str
    url: str
    auth_token: Optional[str] = None
    enabled: bool = True
    last_connected: Optional[datetime] = None
    last_error: Optional[str] = None
    tool_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPState:
    """MCP server state"""
    session: Any
    tools: List[Dict[str, Any]]
    openai_tools: List[Dict[str, Any]]
    tool_name_map: Dict[str, str]
    server_id: str = "default"
    server_name: str = "default"


class MCPRegistry:
    """Dynamic registry for multiple MCP servers"""

    def __init__(self):
        self.servers: Dict[str, MCPServerRegistration] = {}
        self.connections: Dict[str, MCPState] = {}
        self._lock = asyncio.Lock()

    def register(
        self,
        url: str,
        name: str,
        auth_token: Optional[str] = None,
        enabled: bool = True,
        server_id: Optional[str] = None
    ) -> MCPServerRegistration:
        """Register a new MCP server"""
        if not server_id:
            server_id = f"mcp_{hashlib.md5(url.encode()).hexdigest()[:8]}"

        reg = MCPServerRegistration(
            server_id=server_id,
            name=name,
            url=url,
            auth_token=auth_token,
            enabled=enabled
        )
        self.servers[server_id] = reg
        return reg

    def unregister(self, server_id: str) -> bool:
        """Unregister an MCP server"""
        if server_id in self.servers:
            del self.servers[server_id]
            if server_id in self.connections:
                del self.connections[server_id]
            return True
        return False

    def get(self, server_id: str) -> Optional[MCPServerRegistration]:
        """Get server registration"""
        return self.servers.get(server_id)

    def list_all(self) -> List[MCPServerRegistration]:
        """List all registered servers"""
        return list(self.servers.values())

    def get_enabled(self) -> List[MCPServerRegistration]:
        """Get all enabled servers"""
        return [s for s in self.servers.values() if s.enabled]

    async def connect(self, server_id: str) -> Optional[MCPState]:
        """Connect to an MCP server"""
        async with self._lock:
            reg = self.servers.get(server_id)
            if not reg or not reg.enabled:
                return None

            try:
                # Build headers
                headers = {}
                if reg.auth_token:
                    headers["Authorization"] = f"Bearer {reg.auth_token}"

                # Connect via SSE
                client_context = sse_client(reg.url, headers)
                streams = await client_context.__aenter__()
                read, write = streams

                session = ClientSession(read, write)
                await session.__aenter__()
                await session.initialize()

                # Fetch tools
                mcp_tools = await fetch_mcp_tools(session)
                openai_tools, name_map = to_openai_tools(mcp_tools)

                # Create state
                state = MCPState(
                    session=session,
                    tools=mcp_tools,
                    openai_tools=openai_tools,
                    tool_name_map=name_map,
                    server_id=server_id,
                    server_name=reg.name
                )

                self.connections[server_id] = state

                # Update registration
                reg.last_connected = datetime.utcnow()
                reg.last_error = None
                reg.tool_count = len(mcp_tools)

                return state

            except Exception as e:
                reg.last_error = str(e)
                print(f"Failed to connect to MCP server {reg.name}: {e}", file=sys.stderr)
                return None

    async def connect_all(self) -> Dict[str, MCPState]:
        """Connect to all enabled servers"""
        tasks = [self.connect(s.server_id) for s in self.get_enabled()]
        await asyncio.gather(*tasks, return_exceptions=True)
        return self.connections

    def get_all_tools(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, str]]:
        """Get merged tools from all connected servers"""
        all_mcp_tools = []
        all_openai_tools = []
        all_name_map = {}

        for state in self.connections.values():
            all_mcp_tools.extend(state.tools)
            all_openai_tools.extend(state.openai_tools)
            all_name_map.update(state.tool_name_map)

        return all_mcp_tools, all_openai_tools, all_name_map

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Call a tool from any connected server"""
        # Find which server has this tool
        for state in self.connections.values():
            # Check if this server has the tool (either in original name or mapped name)
            original_name = state.tool_name_map.get(tool_name, tool_name)
            if original_name in [t["name"] for t in state.tools]:
                return await call_mcp_tool(state.session, original_name, arguments)

        raise ValueError(f"Tool {tool_name} not found in any connected MCP server")


def sanitize_tool_name(name: str) -> str:
    """Sanitize tool name for OpenAI"""
    import unicodedata
    cleaned = unicodedata.normalize("NFKD", name or "")
    cleaned = cleaned.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_-").lower()
    return cleaned[:128] if cleaned else "tool"


def ensure_unique_tool_name(base: str, used: Set[str]) -> str:
    """Ensure unique tool name"""
    candidate = base or "tool"
    counter = 1
    while candidate in used:
        suffix = f"_{counter}"
        max_len = max(1, 128 - len(suffix))
        candidate = f"{base[:max_len]}{suffix}"
        counter += 1
    used.add(candidate)
    return candidate


async def fetch_mcp_tools(session) -> List[Dict[str, Any]]:
    """Fetch tools from MCP server"""
    tools_resp = await session.list_tools()
    items = (
        tools_resp.get("tools") if isinstance(tools_resp, dict)
        else getattr(tools_resp, "tools", tools_resp)
    )

    if not items:
        return []

    tools = []
    for t in items:
        name = getattr(t, "name", None) or t.get("name")
        desc = getattr(t, "description", None) or t.get("description", "")
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
    """Convert MCP tools to OpenAI format with name mapping"""
    tools = []
    used_names: Set[str] = set()
    name_map = {}

    for t in mcp_tools:
        name = t.get("name")
        if not name:
            continue

        desc = t.get("description", "")
        schema = t.get("input_schema", {"type": "object", "properties": {}})

        sanitized_base = sanitize_tool_name(name)
        sanitized = ensure_unique_tool_name(sanitized_base, used_names)
        name_map[sanitized] = name

        tools.append({
            "type": "function",
            "function": {
                "name": sanitized,
                "description": desc,
                "parameters": schema
            }
        })

    return tools, name_map


async def call_mcp_tool(session, tool_name: str, arguments: Dict[str, Any]) -> Any:
    """Call MCP tool and return result"""
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


def flatten_mcp_result(result: Any) -> str:
    """Flatten MCP result to text"""
    if isinstance(result, str):
        return result

    content = (
        result.get("content", []) if isinstance(result, dict)
        else getattr(result, "content", [])
    )

    if not content:
        return json.dumps(result) if isinstance(result, dict) else str(result)

    parts = []
    for item in content:
        if isinstance(item, dict):
            if item.get("type") == "text":
                parts.append(item.get("text", ""))
            elif item.get("type") == "image":
                parts.append(f"[Image: {item.get('data', '')[:50]}...]")
            else:
                parts.append(json.dumps(item))
        else:
            text = getattr(item, "text", None)
            parts.append(text if text else str(item))

    return "\n".join(parts) if parts else json.dumps(content)


# ============================================================================
# OpenRouter Streaming
# ============================================================================

async def stream_from_openrouter(
    client: httpx.AsyncClient,
    model: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000
) -> AsyncGenerator[Tuple[str, Any, Any], None]:
    """Stream from OpenRouter. Yields (event_type, content, extra)"""

    payload = {
        "model": model,
        "messages": messages,
        "stream": True,
        "temperature": temperature,
        "max_tokens": max_tokens
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
            if not line or line.startswith(":") or not line.startswith("data: "):
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
            if "content" in delta and delta["content"]:
                yield ("content", delta["content"], None)

            # Tool calls
            if "tool_calls" in delta:
                for tc_delta in delta["tool_calls"]:
                    idx = tc_delta.get("index", 0)

                    if idx not in tool_calls_map:
                        tool_calls_map[idx] = {
                            "id": tc_delta.get("id"),
                            "type": "function",
                            "function": {"name": "", "arguments": ""}
                        }

                    fn_delta = tc_delta.get("function", {})
                    if "name" in fn_delta:
                        tool_calls_map[idx]["function"]["name"] = fn_delta["name"]
                    if "arguments" in fn_delta:
                        tool_calls_map[idx]["function"]["arguments"] += fn_delta["arguments"]

            # Usage
            if "usage" in obj:
                yield ("usage", obj["usage"], None)

    # Send tool calls if any
    if tool_calls_map:
        tool_calls = [tool_calls_map[i] for i in sorted(tool_calls_map.keys())]
        yield ("tool_calls", None, tool_calls)
    else:
        yield ("done", None, None)


# ============================================================================
# SSE Event Creation
# ============================================================================

def create_sse(
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

async def handle_agent_interaction(
    interaction_id: str,
    session: Session,
    user_message: str,
    mcp_registry: MCPRegistry,
    http_client: httpx.AsyncClient,
    enable_tools: bool = True,
    enable_compression: bool = True,
    enable_parallel: bool = True,
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
    max_iterations: int = MAX_ITERATIONS
) -> AsyncGenerator[str, None]:
    """
    Main agent interaction handler with smart streaming

    This intelligently handles everything from simple "say ok" to complex
    multi-tool interactions, streaming the complete flow without polluting context.
    """

    sequence = 0

    # SESSION START
    is_compressed = bool(session.compressed_summary)
    yield create_sse(
        EventType.SESSION_START,
        {
            "session_new": session.total_interactions == 0,
            "message_count": len(session.messages),
            "compressed": is_compressed,
            "model": session.model
        },
        interaction_id,
        session.session_id,
        sequence
    )
    sequence += 1

    # INTERACTION START
    # Get tools - either via retrieval or load all
    if tool_retrieval_service and enable_tools:
        # Use tool retrieval
        retrieval_result = await tool_retrieval_service.get_tools_for_query(
            query=user_message,
            enable_multi_hop=True,
            max_tools=15
        )

        if retrieval_result:
            # Convert retrieved tools to OpenAI format
            selected_tools = retrieval_result['selected_tools']
            openai_tools = []
            tool_name_map = {}

            for tool_dict in selected_tools:
                schema = tool_dict['schema']
                tool_name = tool_dict['tool_name']

                openai_tools.append({
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "description": schema.get('description', ''),
                        "parameters": schema.get('parameters', {})
                    }
                })
                tool_name_map[tool_name] = tool_name

            # Emit retrieval metrics
            yield create_sse(
                EventType.TOOL_RETRIEVAL,
                retrieval_result['metrics'],
                interaction_id,
                session.session_id,
                sequence
            )
            sequence += 1
        else:
            # Fallback to all tools
            _, openai_tools, tool_name_map = mcp_registry.get_all_tools()
    else:
        # Load all tools (fallback or tools disabled)
        _, openai_tools, tool_name_map = mcp_registry.get_all_tools()

    yield create_sse(
        EventType.INTERACTION_START,
        {
            "message": user_message,
            "tools_enabled": enable_tools,
            "tools_available": len(openai_tools) if enable_tools else 0,
            "max_iterations": max_iterations,
            "mcp_servers": len(mcp_registry.connections)
        },
        interaction_id,
        session.session_id,
        sequence
    )
    sequence += 1

    # Compress context if needed
    if enable_compression and len(session.messages) > CONTEXT_COMPRESSION_THRESHOLD:
        yield create_sse(
            EventType.CONTEXT_COMPRESS,
            {"messages_before": len(session.messages), "compressing": True},
            interaction_id,
            session.session_id,
            sequence
        )
        sequence += 1

        summary = session.compress_old_context(keep_recent=20)

        yield create_sse(
            EventType.CONTEXT_SUMMARY,
            {
                "summary": summary[:500],
                "messages_after": len([m for m in session.messages if not m.compressed])
            },
            interaction_id,
            session.session_id,
            sequence
        )
        sequence += 1

    # Build context
    context = session.get_context_for_llm(
        max_messages=MAX_CONTEXT_MESSAGES,
        system_prompt=system_prompt
    )
    context.append({"role": "user", "content": user_message})

    # Add user message to session
    session.add_message("user", user_message)
    session.total_interactions += 1

    # Main iteration loop
    tools = openai_tools if enable_tools else None
    iteration = 0

    try:
        while iteration < max_iterations:
            iteration += 1

            yield create_sse(
                EventType.ITERATION,
                {"iteration": iteration, "max": max_iterations},
                interaction_id,
                session.session_id,
                sequence
            )
            sequence += 1

            # MESSAGE START
            yield create_sse(
                EventType.MESSAGE_START,
                {"iteration": iteration},
                interaction_id,
                session.session_id,
                sequence
            )
            sequence += 1

            # Stream from OpenRouter
            full_response = ""
            tool_calls_received = None
            usage_info = {}

            try:
                async for event_type, content, extra in stream_from_openrouter(
                    http_client,
                    session.model,
                    context,
                    tools,
                    temperature,
                    max_tokens
                ):
                    if event_type == "content" and content:
                        full_response += content

                        # Stream chunks
                        yield create_sse(
                            EventType.MESSAGE_CHUNK,
                            {"chunk": content, "iteration": iteration},
                            interaction_id,
                            session.session_id,
                            sequence
                        )
                        sequence += 1

                    elif event_type == "tool_calls" and extra:
                        tool_calls_received = extra

                    elif event_type == "usage" and content:
                        usage_info = content

            except Exception as stream_error:
                # Handle streaming errors gracefully
                yield create_sse(
                    EventType.ERROR,
                    {
                        "error": "Streaming error",
                        "message": str(stream_error),
                        "iteration": iteration
                    },
                    interaction_id,
                    session.session_id,
                    sequence
                )
                sequence += 1
                raise  # Re-raise to exit the loop

            # MESSAGE COMPLETE
            yield create_sse(
                EventType.MESSAGE_COMPLETE,
                {
                    "text": full_response,
                    "has_tool_calls": bool(tool_calls_received),
                    "iteration": iteration
                },
                interaction_id,
                session.session_id,
                sequence
            )
            sequence += 1

            # Add assistant message
            assistant_msg = session.add_message(
                "assistant",
                full_response,
                tokens=usage_info.get("completion_tokens", 0),
                tool_calls=tool_calls_received
            )

            context.append({
                "role": "assistant",
                "content": full_response,
                **({"tool_calls": tool_calls_received} if tool_calls_received else {})
            })

            # Handle tool calls
            if tool_calls_received:
                session.total_tool_calls += len(tool_calls_received)

                # Determine execution mode
                execution_mode = "parallel" if enable_parallel and len(tool_calls_received) > 1 else "sequential"

                yield create_sse(
                    EventType.TOOL_PLAN,
                    {
                        "tools": [
                            {
                                "id": tc.get("id"),
                                "name": tc.get("function", {}).get("name")
                            }
                            for tc in tool_calls_received
                        ],
                        "count": len(tool_calls_received),
                        "execution_mode": execution_mode
                    },
                    interaction_id,
                    session.session_id,
                    sequence
                )
                sequence += 1

                if execution_mode == "parallel":
                    # PARALLEL EXECUTION
                    yield create_sse(
                        EventType.TOOL_PARALLEL_START,
                        {
                            "tool_count": len(tool_calls_received),
                            "tool_names": [tc.get("function", {}).get("name") for tc in tool_calls_received]
                        },
                        interaction_id,
                        session.session_id,
                        sequence
                    )
                    sequence += 1

                    # Execute all tools in parallel
                    async def execute_tool(tc, index):
                        tool_id = tc.get("id")
                        fn = tc.get("function", {})
                        fn_name = fn.get("name")
                        fn_args_str = fn.get("arguments", "{}")

                        try:
                            fn_args = json.loads(fn_args_str)
                        except:
                            fn_args = {}

                        original_name = tool_name_map.get(fn_name, fn_name)

                        try:
                            result = await mcp_registry.call_tool(original_name, fn_args)
                            result_text = flatten_mcp_result(result)
                            return {
                                "success": True,
                                "tool_id": tool_id,
                                "tool_name": fn_name,
                                "result": result_text,
                                "index": index
                            }
                        except Exception as e:
                            return {
                                "success": False,
                                "tool_id": tool_id,
                                "tool_name": fn_name,
                                "error": str(e),
                                "index": index
                            }

                    # Start all tool executions
                    for idx, tc in enumerate(tool_calls_received):
                        tool_id = tc.get("id")
                        fn_name = tc.get("function", {}).get("name")
                        yield create_sse(
                            EventType.TOOL_EXECUTE_START,
                            {
                                "tool_id": tool_id,
                                "tool_name": fn_name,
                                "index": idx,
                                "mode": "parallel"
                            },
                            interaction_id,
                            session.session_id,
                            sequence
                        )
                        sequence += 1

                    # Execute in parallel with asyncio.gather
                    results = await asyncio.gather(
                        *[execute_tool(tc, idx) for idx, tc in enumerate(tool_calls_received)],
                        return_exceptions=False
                    )

                    # Process results in order
                    for result in results:
                        if result["success"]:
                            yield create_sse(
                                EventType.TOOL_EXECUTE_COMPLETE,
                                {
                                    "tool_id": result["tool_id"],
                                    "tool_name": result["tool_name"],
                                    "result_preview": result["result"][:300],
                                    "result_length": len(result["result"]),
                                    "mode": "parallel",
                                    "index": result["index"]
                                },
                                interaction_id,
                                session.session_id,
                                sequence
                            )
                            sequence += 1

                            # Add to context
                            context.append({
                                "role": "tool",
                                "tool_call_id": result["tool_id"],
                                "content": result["result"]
                            })

                            # Add to session
                            session.add_message(
                                "tool",
                                result["result"],
                                metadata={"tool_name": result["tool_name"], "tool_id": result["tool_id"]}
                            )
                        else:
                            yield create_sse(
                                EventType.TOOL_EXECUTE_ERROR,
                                {
                                    "tool_id": result["tool_id"],
                                    "tool_name": result["tool_name"],
                                    "error": result["error"],
                                    "mode": "parallel",
                                    "index": result["index"]
                                },
                                interaction_id,
                                session.session_id,
                                sequence
                            )
                            sequence += 1

                    yield create_sse(
                        EventType.TOOL_PARALLEL_COMPLETE,
                        {
                            "tool_count": len(tool_calls_received),
                            "successful": sum(1 for r in results if r["success"]),
                            "failed": sum(1 for r in results if not r["success"])
                        },
                        interaction_id,
                        session.session_id,
                        sequence
                    )
                    sequence += 1

                else:
                    # SEQUENTIAL EXECUTION
                    for idx, tc in enumerate(tool_calls_received):
                        tool_id = tc.get("id")
                        fn = tc.get("function", {})
                        fn_name = fn.get("name")
                        fn_args_str = fn.get("arguments", "{}")

                        yield create_sse(
                            EventType.TOOL_EXECUTE_START,
                            {
                                "tool_id": tool_id,
                                "tool_name": fn_name,
                                "arguments": fn_args_str[:200],
                                "index": idx,
                                "mode": "sequential"
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

                        # Map to original MCP tool name
                        original_name = tool_name_map.get(fn_name, fn_name)

                        try:
                            result = await mcp_registry.call_tool(original_name, fn_args)
                            result_text = flatten_mcp_result(result)

                            yield create_sse(
                                EventType.TOOL_EXECUTE_COMPLETE,
                                {
                                    "tool_id": tool_id,
                                    "tool_name": fn_name,
                                    "result_preview": result_text[:300],
                                    "result_length": len(result_text),
                                    "mode": "sequential",
                                    "index": idx
                                },
                                interaction_id,
                                session.session_id,
                                sequence
                            )
                            sequence += 1

                            # Add to context
                            context.append({
                                "role": "tool",
                                "tool_call_id": tool_id,
                                "content": result_text
                            })

                            # Add to session (but mark for potential compression)
                            session.add_message(
                                "tool",
                                result_text,
                                metadata={"tool_name": fn_name, "tool_id": tool_id}
                            )

                        except Exception as e:
                            yield create_sse(
                                EventType.TOOL_EXECUTE_ERROR,
                                {
                                    "tool_id": tool_id,
                                    "tool_name": fn_name,
                                    "error": str(e),
                                    "mode": "sequential",
                                    "index": idx
                                },
                                interaction_id,
                                session.session_id,
                                sequence
                            )
                            sequence += 1

                # Log tool usage for retrieval system learning
                if tool_retrieval_service and tool_calls_received:
                    tool_names = [tc.get("function", {}).get("name") for tc in tool_calls_received]
                    # Note: We don't have latencies here, could track them in execute_tool
                    await tool_retrieval_service.log_tool_usage(
                        session_id=session.session_id,
                        query=user_message,
                        tool_sequence=tool_names
                    )

                # Continue loop for next iteration
                continue

            # No tool calls - we're done!
            break

        # USAGE REPORT
        if usage_info:
            yield create_sse(
                EventType.USAGE,
                {
                    "iteration_usage": usage_info,
                    "session_total": session.total_tokens,
                    "session_interactions": session.total_interactions,
                    "session_tool_calls": session.total_tool_calls
                },
                interaction_id,
                session.session_id,
                sequence
            )
            sequence += 1

        # INTERACTION COMPLETE
        yield create_sse(
            EventType.INTERACTION_COMPLETE,
            {
                "iterations": iteration,
                "total_events": sequence,
                "final_message": full_response[:200] if full_response else ""
            },
            interaction_id,
            session.session_id,
            sequence
        )

    except Exception as e:
        # Catch any unhandled errors and send as SSE error event
        import traceback
        error_details = {
            "error": type(e).__name__,
            "message": str(e),
            "traceback": traceback.format_exc()[:500]
        }

        yield create_sse(
            EventType.ERROR,
            error_details,
            interaction_id,
            session.session_id,
            sequence
        )

        # Log the full error
        print(f"Error in interaction {interaction_id}: {e}", file=sys.stderr)
        traceback.print_exc()


# ============================================================================
# Application Setup
# ============================================================================

# Global state
session_manager: Optional[SessionManager] = None
mcp_registry: Optional[MCPRegistry] = None
http_client: Optional[httpx.AsyncClient] = None
tool_retrieval_service: Optional['ToolRetrievalService'] = None


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Application lifecycle"""
    global session_manager, mcp_registry, http_client, tool_retrieval_service

    print("🚀 Starting Agent Server...")

    # Initialize session manager
    session_manager = SessionManager(ttl_seconds=SESSION_TTL_SECONDS)
    await session_manager.start_cleanup()

    # Initialize HTTP client
    if not OPENROUTER_API_KEY:
        print("WARNING: OPENROUTER_API_KEY not set", file=sys.stderr)

    http_client = httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "HTTP-Referer": "http://localhost:2046",
            "X-Title": "Agent Server"
        },
        timeout=120.0
    )

    # Initialize Turso
    if TURSO_AVAILABLE:
        try:
            await init_database()
            print("✅ Turso database initialized")
        except Exception as e:
            print(f"WARNING: Turso init failed: {e}")

    # Initialize MCP Registry
    print("📡 Initializing MCP Registry...")
    mcp_registry = MCPRegistry()

    # Register default MCP server if Jina API key is set
    if JINA_API_KEY:
        print(f"📌 Registering Jina MCP: {JINA_MCP_URL}")
        mcp_registry.register(
            url=JINA_MCP_URL,
            name="Jina AI",
            auth_token=JINA_API_KEY,
            server_id="jina"
        )

    # Auto-connect to all registered servers
    if mcp_registry.list_all():
        print("🔌 Connecting to MCP servers...")
        await mcp_registry.connect_all()

        total_tools = sum(s.tool_count for s in mcp_registry.list_all())
        connected = len(mcp_registry.connections)
        total = len(mcp_registry.list_all())

        print(f"✅ Connected to {connected}/{total} MCP servers")
        print(f"🔧 Loaded {total_tools} total tools")

        # Show connected servers
        for state in mcp_registry.connections.values():
            reg = mcp_registry.get(state.server_id)
            print(f"   • {reg.name}: {len(state.tools)} tools")

        # Initialize Tool Retrieval Service
        if TOOL_RETRIEVAL_AVAILABLE:
            print("🧠 Initializing Tool Retrieval Service...")
            try:
                tool_retrieval_service = ToolRetrievalService("tool_index.db")

                # Set up Jina embedding and reranking functions from MCP
                # Find any server that has generate_embeddings and sort_by_relevance
                has_embeddings = False
                has_reranker = False

                for state in mcp_registry.connections.values():
                    tool_names = [t['name'] for t in state.tools]
                    if 'generate_embeddings' in tool_names:
                        has_embeddings = True
                    if 'sort_by_relevance' in tool_names:
                        has_reranker = True

                if has_embeddings and has_reranker:
                    async def embed_func(params):
                        return await mcp_registry.call_tool('generate_embeddings', params)

                    async def rerank_func(params):
                        return await mcp_registry.call_tool('sort_by_relevance', params)

                    tool_retrieval_service.set_jina_functions(embed_func, rerank_func)

                    # Embed all tools from all servers
                    await tool_retrieval_service.initialize(mcp_registry)

                    stats = tool_retrieval_service.get_stats()
                    print(f"✅ Tool Retrieval initialized: {stats['total_tools']} tools embedded")
                else:
                    print(f"⚠️  Missing Jina tools (embeddings: {has_embeddings}, reranker: {has_reranker}) - tool retrieval disabled")
                    tool_retrieval_service = None

            except Exception as e:
                print(f"⚠️  Tool Retrieval init failed: {e}")
                import traceback
                traceback.print_exc()
                tool_retrieval_service = None
    else:
        print("⚠️  No MCP servers registered - tools disabled")
        print("   Add servers via POST /v1/mcp/servers")

    print("✅ Agent Server ready!")

    try:
        yield
    finally:
        print("🛑 Shutting down...")

        if session_manager:
            await session_manager.stop_cleanup()

        if http_client:
            await http_client.aclose()


app = FastAPI(
    title="Agent Server",
    version="1.0.0",
    description="Intelligent /v1/interactions endpoint with smart context management",
    lifespan=app_lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/")
async def root():
    """Serve the Datastar chat UI"""
    html_path = os.path.join(os.path.dirname(__file__), "chat_ui_datastar.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
    else:
        # Fallback to vanilla JS version
        vanilla_path = os.path.join(os.path.dirname(__file__), "chat_ui.html")
        if os.path.exists(vanilla_path):
            return FileResponse(vanilla_path)
        # Final fallback to API info
        return {
            "service": "Agent Server",
            "version": "1.0.0",
            "endpoints": {
                "interactions": "POST /v1/interactions",
                "sessions": "GET /v1/sessions",
                "session": "GET /v1/sessions/{session_id}",
                "health": "GET /health"
            },
            "docs": "/docs"
        }


@app.get("/api")
async def api_info():
    """API information"""
    return {
        "service": "Agent Server",
        "version": "1.0.0",
        "endpoints": {
            "interactions": "POST /v1/interactions",
            "sessions": "GET /v1/sessions",
            "session": "GET /v1/sessions/{session_id}",
            "health": "GET /health"
        },
        "docs": "/docs"
    }


@app.get("/health")
async def health():
    """Health check"""
    mcp_tools, openai_tools, _ = mcp_registry.get_all_tools() if mcp_registry else ([], [], {})
    return {
        "status": "healthy",
        "mcp_servers_registered": len(mcp_registry.list_all()) if mcp_registry else 0,
        "mcp_servers_connected": len(mcp_registry.connections) if mcp_registry else 0,
        "tools_available": len(openai_tools),
        "active_sessions": len(session_manager.sessions) if session_manager else 0,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/v1/interactions")
async def create_interaction(request: InteractionRequest):
    """
    Main /v1/interactions endpoint

    Handles everything from simple text ("say ok") to complex multi-tool flows.
    Streams the complete interaction with intelligent context management.
    """

    # Validate
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")
    if not http_client:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")

    # Get or create session
    session, is_new = session_manager.get_or_create(
        request.session_id,
        request.model
    )

    # Generate interaction ID
    interaction_id = f"int_{uuid.uuid4().hex[:16]}"

    # Stream response
    return StreamingResponse(
        handle_agent_interaction(
            interaction_id=interaction_id,
            session=session,
            user_message=request.message,
            mcp_registry=mcp_registry,
            http_client=http_client,
            enable_tools=request.enable_tools,
            enable_compression=request.enable_context_compression,
            enable_parallel=request.enable_parallel_tools,
            system_prompt=request.system_prompt,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            max_iterations=request.max_iterations or MAX_ITERATIONS
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


@app.get("/v1/sessions")
async def list_sessions():
    """List all active sessions"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    return {
        "sessions": session_manager.list_all(),
        "total": len(session_manager.sessions)
    }


@app.get("/v1/sessions/{session_id}")
async def get_session(session_id: str):
    """Get session details"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    session = session_manager.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    return session.to_dict()


@app.delete("/v1/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session"""
    if not session_manager:
        raise HTTPException(status_code=503, detail="Session manager not initialized")

    deleted = session_manager.delete(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")

    return {"message": "Session deleted", "session_id": session_id}


@app.get("/v1/tools")
async def list_tools():
    """List available MCP tools from all connected servers"""
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")

    mcp_tools, _, _ = mcp_registry.get_all_tools()

    # Group tools by server
    tools_by_server = defaultdict(list)
    for state in mcp_registry.connections.values():
        reg = mcp_registry.get(state.server_id)
        tools_by_server[reg.name] = state.tools

    return {
        "tools": mcp_tools,
        "count": len(mcp_tools),
        "servers": dict(tools_by_server)
    }


# ============================================================================
# MCP Server Management API
# ============================================================================

class MCPServerRequest(BaseModel):
    """Request to register an MCP server"""
    url: str = Field(..., description="MCP server SSE URL")
    name: str = Field(..., description="Human-readable name")
    auth_token: Optional[str] = Field(None, description="Optional Bearer token")
    enabled: bool = Field(default=True, description="Enable immediately")
    auto_connect: bool = Field(default=True, description="Connect immediately after registration")


@app.get("/v1/mcp/servers")
async def list_mcp_servers():
    """List all registered MCP servers"""
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")

    servers = []
    for reg in mcp_registry.list_all():
        connected = reg.server_id in mcp_registry.connections
        servers.append({
            "server_id": reg.server_id,
            "name": reg.name,
            "url": reg.url,
            "enabled": reg.enabled,
            "connected": connected,
            "tool_count": reg.tool_count,
            "last_connected": reg.last_connected.isoformat() if reg.last_connected else None,
            "last_error": reg.last_error,
            "metadata": reg.metadata
        })

    return {
        "servers": servers,
        "total": len(servers),
        "connected": sum(1 for s in servers if s["connected"])
    }


@app.post("/v1/mcp/servers")
async def register_mcp_server(request: MCPServerRequest):
    """Register a new MCP server"""
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")

    # Register the server
    reg = mcp_registry.register(
        url=request.url,
        name=request.name,
        auth_token=request.auth_token,
        enabled=request.enabled
    )

    # Auto-connect if requested
    state = None
    if request.auto_connect and request.enabled:
        state = await mcp_registry.connect(reg.server_id)

    return {
        "server_id": reg.server_id,
        "name": reg.name,
        "url": reg.url,
        "enabled": reg.enabled,
        "connected": state is not None,
        "tool_count": reg.tool_count if state else 0,
        "message": f"MCP server '{reg.name}' registered" + (" and connected" if state else "")
    }


@app.delete("/v1/mcp/servers/{server_id}")
async def unregister_mcp_server(server_id: str):
    """Unregister an MCP server"""
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")

    reg = mcp_registry.get(server_id)
    if not reg:
        raise HTTPException(status_code=404, detail="Server not found")

    mcp_registry.unregister(server_id)

    return {
        "message": f"MCP server '{reg.name}' unregistered",
        "server_id": server_id
    }


@app.post("/v1/mcp/servers/{server_id}/connect")
async def connect_mcp_server(server_id: str):
    """Connect to a registered MCP server"""
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")

    reg = mcp_registry.get(server_id)
    if not reg:
        raise HTTPException(status_code=404, detail="Server not found")

    state = await mcp_registry.connect(server_id)

    if not state:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect: {reg.last_error or 'Unknown error'}"
        )

    return {
        "server_id": server_id,
        "name": reg.name,
        "connected": True,
        "tool_count": len(state.tools),
        "tools": [t["name"] for t in state.tools]
    }


@app.get("/v1/mcp/servers/{server_id}/tools")
async def get_server_tools(server_id: str):
    """Get tools from a specific MCP server"""
    if not mcp_registry:
        raise HTTPException(status_code=503, detail="MCP registry not initialized")

    state = mcp_registry.connections.get(server_id)
    if not state:
        raise HTTPException(status_code=404, detail="Server not connected")

    return {
        "server_id": server_id,
        "server_name": state.server_name,
        "tools": state.tools,
        "count": len(state.tools)
    }


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("🤖 AGENT SERVER - Intelligent /v1/interactions Endpoint")
    print("=" * 80)
    print()
    print("Features:")
    print("  ✅ Smart context management with auto-compression")
    print("  ✅ Adaptive streaming (simple text -> complex flows)")
    print("  ✅ MCP tool integration with parallel execution")
    print("  ✅ Session persistence and resumability")
    print("  ✅ Real-time SSE event streaming")
    print()
    print("Examples:")
    print("  Simple: 'say ok' -> streams tokens")
    print("  Complex: 'get weather in 12 cities' -> streams full flow")
    print()
    print("Endpoints:")
    print("  📡 POST /v1/interactions - Main interaction endpoint")
    print("  💬 GET /v1/sessions - List sessions")
    print("  🔧 GET /v1/tools - List available tools")
    print("  📖 GET /docs - API documentation")
    print()
    print("Environment:")
    print(f"  OPENROUTER_API_KEY: {'✅' if OPENROUTER_API_KEY else '❌'}")
    print(f"  JINA_API_KEY: {'✅' if JINA_API_KEY else '❌'}")
    print(f"  MAX_ITERATIONS: {MAX_ITERATIONS}")
    print(f"  CONTEXT_COMPRESSION: {CONTEXT_COMPRESSION_THRESHOLD} messages")
    print()
    print("=" * 80)
    print()

    uvicorn.run(
        "agent_server:app",
        host="0.0.0.0",
        port=2046,
        reload=True,
        log_level="info"
    )
