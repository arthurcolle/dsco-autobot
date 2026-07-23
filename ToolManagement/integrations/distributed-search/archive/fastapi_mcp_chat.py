"""
FastAPI wrapper for chat-openrouter-mcp with SSE streaming
Wraps the existing MCP client/server functionality into a web API
"""

import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional, AsyncGenerator, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from contextlib import asynccontextmanager
import uuid
import httpx

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from enum import Enum
import uvicorn
import base64
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Optional token budget manager integration
try:
    from MCP.client.token_budget_manager import TokenBudgetManager, TokenCounter
    from MCP.client.token_integration import TokenBudgetIntegration
    TOKEN_MANAGEMENT_AVAILABLE = True
except ImportError:
    TokenBudgetManager = None  # type: ignore
    TokenCounter = None  # type: ignore
    TokenBudgetIntegration = None  # type: ignore
    TOKEN_MANAGEMENT_AVAILABLE = False

ENABLE_TOKEN_MANAGEMENT = os.getenv("ENABLE_TOKEN_MANAGEMENT", "true").strip().lower() in {"1", "true", "yes", "on"}
TOKEN_SUMMARY_MODEL = os.getenv("TOKEN_SUMMARY_MODEL", "anthropic/claude-3-haiku")
TOKEN_DEBUG = os.getenv("TOKEN_DEBUG", "false").strip().lower() in {"1", "true", "yes", "on"}
TOKEN_CIRCUIT_BREAKER = os.getenv("TOKEN_CIRCUIT_BREAKER", "true").strip().lower() in {"1", "true", "yes", "on"}
TOKEN_CIRCUIT_THRESHOLD = int(os.getenv("TOKEN_CIRCUIT_THRESHOLD", "3"))

STREAMING_TOKEN_COUNTER = TokenCounter() if TOKEN_MANAGEMENT_AVAILABLE and TokenCounter else None
MAX_ITERATION_HISTORY = 50

# Import MCP components
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    from mcp.client.streamable_http import streamablehttp_client
    from mcp.client.session_group import ClientSessionGroup, StreamableHttpParameters, SseServerParameters
except ImportError:
    print("ERROR: mcp package not found. Install with: pip install mcp", file=sys.stderr)
    sys.exit(1)

# Import Turso database
try:
    from turso_db import init_database, get_database, TursoDatabase
except ImportError:
    print("ERROR: turso_db module not found", file=sys.stderr)
    sys.exit(1)


# Event types for SSE streaming
class EventType(str, Enum):
    SESSION_START = "session.start"
    SESSION_UPDATE = "session.update"
    SESSION_END = "session.end"

    MESSAGE_START = "message.start"
    MESSAGE_DELTA = "message.delta"
    MESSAGE_END = "message.end"

    TOOL_CALL_START = "tool.call.start"
    TOOL_CALL_ARGS = "tool.call.args"
    TOOL_CALL_EXECUTE = "tool.call.execute"
    TOOL_RESULT = "tool.result"
    TOOL_CALL_END = "tool.call.end"

    THINKING_START = "thinking.start"
    THINKING_DELTA = "thinking.delta"
    THINKING_END = "thinking.end"

    ERROR = "error"
    METADATA = "metadata"
    HEARTBEAT = "heartbeat"
    BUDGET_UPDATE = "budget.update"
    USAGE_UPDATE = "usage.update"
    ITERATION_SUMMARY = "iteration.summary"


@dataclass
class MCPServerConfig:
    """Configuration for an MCP server"""
    name: str
    url: str
    transport: str = "sse"  # "sse" or "streamable_http"
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class MCPSessionState:
    """Holds MCP session and tools"""
    session: Any  # ClientSession or ClientSessionGroup
    mcp_tools: List[Dict[str, Any]]
    openai_tools: List[Dict[str, Any]]
    tool_name_map: Dict[str, str]  # sanitized -> original
    server_configs: List[MCPServerConfig]


class ChatMessage(BaseModel):
    role: str
    content: str
    metadata: Optional[Dict[str, Any]] = None


class FileData(BaseModel):
    name: str
    type: str
    size: int
    data: str  # base64 encoded


class ChatRequest(BaseModel):
    conversation_id: Optional[str] = None
    message: str
    model: str = "anthropic/claude-sonnet-4.5"
    temperature: float = Field(0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(4000, gt=0)
    stream: bool = True
    enable_tools: bool = True
    system_prompt: Optional[str] = None
    files: Optional[List[FileData]] = None


# Global state
ConversationState = Dict[str, Any]
conversations: Dict[str, ConversationState] = {}
uploaded_files: Dict[str, List[Dict[str, Any]]] = {}  # conversation_id -> files
mcp_state: Optional[MCPSessionState] = None
http_client: Optional[httpx.AsyncClient] = None


def create_conversation_state(model: Optional[str]) -> ConversationState:
    """Create a new conversation scaffold"""
    return {
        "messages": [],
        "model": model,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "token_manager": None,
        "token_integration": None,
        "token_usage": {"prompt": 0, "completion": 0, "total": 0, "limit": 0},
        "iteration_summaries": [],
        "last_message_id": None,
        "iterations": 0,
    }


def get_conversation_state(conversation_id: str, model: Optional[str]) -> ConversationState:
    """Fetch or initialize conversation state"""
    if conversation_id not in conversations:
        conversations[conversation_id] = create_conversation_state(model)
    state = conversations[conversation_id]
    if model:
        state["model"] = model
    state.setdefault("messages", [])
    state.setdefault("token_usage", {"prompt": 0, "completion": 0, "total": 0, "limit": 0})
    state.setdefault("iteration_summaries", [])
    state.setdefault("iterations", 0)
    return state


def ensure_conversation_token_manager(state: ConversationState, model: Optional[str]) -> Optional["TokenBudgetManager"]:
    """Ensure a token manager is attached to the conversation state if enabled"""
    if not (ENABLE_TOKEN_MANAGEMENT and TOKEN_MANAGEMENT_AVAILABLE and http_client):
        return None

    manager: Optional[TokenBudgetManager] = state.get("token_manager")
    integration: Optional[TokenBudgetIntegration] = state.get("token_integration")

    if manager is None or integration is None:
        summarize_model = TOKEN_SUMMARY_MODEL or model or "anthropic/claude-3-haiku"
        integration = TokenBudgetIntegration(
            http_client=http_client,  # type: ignore[arg-type]
            summarize_model=summarize_model,
            debug=TOKEN_DEBUG,
        )
        manager = integration.create_manager(
            model=model or "default",
            enable_monitoring=True,
            enable_circuit_breaker=TOKEN_CIRCUIT_BREAKER,
            circuit_breaker_threshold=TOKEN_CIRCUIT_THRESHOLD,
            max_compression_attempts=5,
        )
        state["token_manager"] = manager
        state["token_integration"] = integration
        state.setdefault("token_usage", {})["limit"] = manager.limits.effective_input_limit
    elif model and manager.model != model:
        manager.set_model(model)
        state.setdefault("token_usage", {})["limit"] = manager.limits.effective_input_limit

    return manager


def update_conversation_usage(state: ConversationState, usage_delta: Dict[str, int]) -> Dict[str, int]:
    """Accumulate token usage stats for a conversation"""
    usage = state.setdefault("token_usage", {"prompt": 0, "completion": 0, "total": 0, "limit": 0})
    usage["prompt"] = usage.get("prompt", 0) + usage_delta.get("prompt_tokens", 0)
    usage["completion"] = usage.get("completion", 0) + usage_delta.get("completion_tokens", 0)
    if "total_tokens" in usage_delta:
        usage["total"] = usage.get("total", 0) + usage_delta.get("total_tokens", 0)
    else:
        usage["total"] = usage.get("total", 0) + usage_delta.get("prompt_tokens", 0) + usage_delta.get("completion_tokens", 0)
    return usage


# === MCP Helper Functions (adapted from chat-openrouter-mcp.py) ===

def sanitize_tool_name(name: str) -> str:
    """Sanitize tool names for OpenAI compatibility"""
    import re
    import unicodedata

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

    out = []
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

        out.append({
            "name": name,
            "description": desc,
            "input_schema": schema
        })

    return out


def to_openai_tools(mcp_tools: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Convert MCP tools to OpenAI format, returns (tools, name_map)"""
    tools = []
    used_names = set()
    name_map = {}  # sanitized -> original

    for t in mcp_tools:
        name = t.get("name")
        if not name:
            continue

        desc = t.get("description") or ""
        schema = t.get("input_schema") or {"type": "object", "properties": {}}

        sanitized_base = sanitize_tool_name(name)
        sanitized = ensure_unique_tool_name(sanitized_base, used_names)

        name_map[sanitized] = name

        tool_def = {
            "type": "function",
            "function": {
                "name": sanitized,
                "description": desc,
                "parameters": schema,
            },
        }
        tools.append(tool_def)

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

    # Handle dict response
    if isinstance(result, dict):
        content = result.get("content", [])
    else:
        # Handle object with content attribute
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


# === OpenRouter Integration ===

async def openrouter_stream(
    client: httpx.AsyncClient,
    model: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]] = None,
    temperature: float = 0.7,
    max_tokens: int = 4000,
) -> AsyncGenerator[tuple[str, Optional[Any], Optional[List[Dict[str, Any]]]], None]:
    """
    Stream from OpenRouter, yielding tuples of (event_type, payload, tool_calls)

    event_type:
      - "content": incremental assistant text
      - "tool_calls": aggregated tool call definitions at the end of a turn
      - "usage": token usage metadata emitted by OpenRouter
      - "done": terminal marker when no tool calls are present
    """
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

    # Yield final tool calls if any
    if tool_calls_map:
        tool_calls = [tool_calls_map[i] for i in sorted(tool_calls_map.keys())]
        yield ("tool_calls", None, tool_calls)
    else:
        yield ("done", None, None)


# === SSE Event Helpers ===

def create_sse_event(
    event_type: EventType,
    data: Dict[str, Any],
    conversation_id: str,
    sequence: int
) -> str:
    """Create SSE formatted event"""
    event_data = {
        "event": event_type.value,
        "data": data,
        "conversation_id": conversation_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sequence": sequence
    }
    return f"event: {event_type.value}\ndata: {json.dumps(event_data)}\n\n"


# === Main Streaming Chat Handler ===

async def stream_chat_response(
    conversation_id: str,
    user_message: str,
    model: str,
    temperature: float,
    max_tokens: int,
    enable_tools: bool,
    system_prompt: Optional[str],
    files: Optional[List[FileData]] = None,
) -> AsyncGenerator[str, None]:
    """Main streaming chat handler with MCP tool integration"""

    global mcp_state, http_client

    if not http_client:
        raise HTTPException(status_code=500, detail="HTTP client not initialized")

    sequence = 0
    message_id = f"msg_{uuid.uuid4().hex[:12]}"
    conversation_state = get_conversation_state(conversation_id, model)
    conversation_history = conversation_state["messages"]
    conversation_state["last_message_id"] = message_id

    # Build user message with file context
    user_message_with_files = user_message
    if files and len(files) > 0:
        file_context = "\n\n[Attached files:\n"
        for file in files:
            try:
                decoded_data = base64.b64decode(file.data)
                if file.type.startswith('text/') or file.name.endswith(('.txt', '.md', '.json', '.csv')):
                    content = decoded_data.decode('utf-8', errors='ignore')[:2000]
                    file_context += f"- {file.name} ({file.type}):\n{content}\n\n"
                elif file.type.startswith('image/'):
                    file_context += f"- {file.name} (image, {file.size} bytes)\n"
                else:
                    file_context += f"- {file.name} ({file.type}, {file.size} bytes)\n"
            except Exception as e:
                file_context += f"- {file.name} (error reading file: {str(e)})\n"

        file_context += "]"
        user_message_with_files += file_context

    # SESSION START
    yield create_sse_event(
        EventType.SESSION_START,
        {
            "message_id": message_id,
            "model": model,
            "user_message": user_message,
            "tools_available": len(mcp_state.openai_tools) if mcp_state and enable_tools else 0
        },
        conversation_id,
        sequence
    )
    sequence += 1

    # Build messages for OpenRouter
    messages: List[Dict[str, Any]] = []

    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    messages.extend(conversation_history)

    messages.append({"role": "user", "content": user_message_with_files})
    conversation_history.append({
        "role": "user",
        "content": user_message,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    })

    tools = None
    if enable_tools and mcp_state:
        tools = mcp_state.openai_tools

    max_iterations = 10
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        conversation_state["iterations"] = iteration

        manager = ensure_conversation_token_manager(conversation_state, model)
        budget_metadata: Optional[Dict[str, Any]] = None
        messages_to_send = messages

        if manager:
            try:
                messages_to_send, budget_metadata = await manager.enforce_budget(messages, auto_compress=True)
            except RuntimeError as exc:
                yield create_sse_event(
                    EventType.ERROR,
                    {"message": str(exc), "iteration": iteration},
                    conversation_id,
                    sequence
                )
                sequence += 1
                raise

        budget_payload = None
        if budget_metadata:
            budget_payload = {
                "iteration": iteration,
                "model": manager.model if manager else model,
                "limit": manager.limits.effective_input_limit if manager else None,
                "compressed": budget_metadata.get("compressed"),
                "strategy": budget_metadata.get("strategy"),
                "original_tokens": budget_metadata.get("original_tokens"),
                "final_tokens": budget_metadata.get("final_tokens"),
            }
            yield create_sse_event(
                EventType.BUDGET_UPDATE,
                budget_payload,
                conversation_id,
                sequence
            )
            sequence += 1

        yield create_sse_event(
            EventType.MESSAGE_START,
            {"iteration": iteration, "message_id": message_id, "budget_applied": bool(budget_metadata)},
            conversation_id,
            sequence
        )
        sequence += 1

        full_response = ""
        tool_calls_received = None
        iteration_token_estimate = 0
        usage_info: Dict[str, Any] = {}
        usage_reported = False

        async for event_type, content_delta, tool_calls in openrouter_stream(
            http_client,
            model,
            messages_to_send,
            tools,
            temperature,
            max_tokens
        ):
            if event_type == "content" and content_delta:
                full_response += content_delta
                token_delta = None
                if STREAMING_TOKEN_COUNTER:
                    try:
                        token_delta = max(1, STREAMING_TOKEN_COUNTER.count_tokens(content_delta, model))
                    except Exception:
                        token_delta = max(1, len(content_delta) // 4 or 1)
                else:
                    token_delta = max(1, len(content_delta) // 4 or 1)
                iteration_token_estimate += token_delta

                yield create_sse_event(
                    EventType.MESSAGE_DELTA,
                    {
                        "delta": content_delta,
                        "full_text": full_response,
                        "iteration": iteration,
                        "token_delta_estimate": token_delta
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

            elif event_type == "tool_calls" and tool_calls:
                tool_calls_received = tool_calls

            elif event_type == "usage" and isinstance(content_delta, dict):
                usage_info = content_delta
                usage_reported = True
                cumulative_usage = update_conversation_usage(conversation_state, usage_info)
                yield create_sse_event(
                    EventType.USAGE_UPDATE,
                    {
                        "iteration": iteration,
                        "usage": usage_info,
                        "cumulative": cumulative_usage
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

        if not usage_info:
            prompt_tokens = budget_metadata.get("final_tokens", 0) if budget_metadata else 0
            usage_info = {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": iteration_token_estimate,
                "total_tokens": prompt_tokens + iteration_token_estimate,
            }

        if not usage_reported and usage_info:
            cumulative_usage = update_conversation_usage(conversation_state, usage_info)
            yield create_sse_event(
                EventType.USAGE_UPDATE,
                {
                    "iteration": iteration,
                    "usage": usage_info,
                    "cumulative": cumulative_usage
                },
                conversation_id,
                sequence
            )
            sequence += 1

        yield create_sse_event(
            EventType.MESSAGE_END,
            {
                "full_text": full_response,
                "has_tool_calls": bool(tool_calls_received),
                "iteration": iteration,
                "token_usage": usage_info
            },
            conversation_id,
            sequence
        )
        sequence += 1

        assistant_msg = {
            "role": "assistant",
            "content": full_response,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "iteration": iteration,
            "token_usage": usage_info
        }

        if tool_calls_received:
            assistant_msg["tool_calls"] = tool_calls_received

        messages.append(assistant_msg)
        conversation_history.append(assistant_msg)

        if tool_calls_received:
            for tc in tool_calls_received:
                tool_id = tc.get("id")
                fn = tc.get("function", {})
                fn_name = fn.get("name")
                fn_args_str = fn.get("arguments", "{}")

                yield create_sse_event(
                    EventType.TOOL_CALL_START,
                    {
                        "tool_id": tool_id,
                        "tool_name": fn_name,
                        "arguments_raw": fn_args_str,
                        "iteration": iteration
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

                try:
                    fn_args = json.loads(fn_args_str)
                except Exception:
                    fn_args = {}

                yield create_sse_event(
                    EventType.TOOL_CALL_ARGS,
                    {
                        "tool_id": tool_id,
                        "arguments": fn_args,
                        "iteration": iteration
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

                original_tool_name = mcp_state.tool_name_map.get(fn_name, fn_name)

                yield create_sse_event(
                    EventType.TOOL_CALL_EXECUTE,
                    {
                        "tool_id": tool_id,
                        "mcp_tool_name": original_tool_name,
                        "status": "executing",
                        "iteration": iteration
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

                result = await call_mcp_tool(mcp_state.session, original_tool_name, fn_args)
                result_text = flatten_mcp_content(result)

                yield create_sse_event(
                    EventType.TOOL_RESULT,
                    {
                        "tool_id": tool_id,
                        "result": result_text[:1000],
                        "result_length": len(result_text),
                        "iteration": iteration
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

                yield create_sse_event(
                    EventType.TOOL_CALL_END,
                    {
                        "tool_id": tool_id,
                        "status": "completed",
                        "iteration": iteration
                    },
                    conversation_id,
                    sequence
                )
                sequence += 1

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": result_text
                })
                conversation_history.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "name": fn_name,
                    "content": result_text,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                })

        iteration_summary = {
            "iteration": iteration,
            "token_usage": usage_info,
            "tool_calls": len(tool_calls_received or []),
            "message_preview": (full_response or "").strip()[:220],
            "budget": budget_payload,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        if tool_calls_received:
            iteration_summary["tool_calls_detail"] = [
                {
                    "id": tc.get("id"),
                    "name": tc.get("function", {}).get("name")
                }
                for tc in tool_calls_received
            ]

        conversation_summaries = conversation_state.setdefault("iteration_summaries", [])
        conversation_summaries.append(iteration_summary)
        if len(conversation_summaries) > MAX_ITERATION_HISTORY:
            conversation_state["iteration_summaries"] = conversation_summaries[-MAX_ITERATION_HISTORY:]

        yield create_sse_event(
            EventType.ITERATION_SUMMARY,
            iteration_summary,
            conversation_id,
            sequence
        )
        sequence += 1

        if not tool_calls_received:
            break

    yield create_sse_event(
        EventType.SESSION_END,
        {
            "message_id": message_id,
            "iterations": iteration,
            "total_events": sequence
        },
        conversation_id,
        sequence
    )


# === Application Lifecycle ===

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Manage startup and shutdown resources in a single task"""
    global mcp_state, http_client

    client_context = None
    session_ref: Optional[ClientSession] = None
    client_context_entered = False
    session_entered = False

    await init_database()

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        print("WARNING: OPENROUTER_API_KEY not set", file=sys.stderr)

    http_client = httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": "http://localhost:2046",
            "X-Title": "MCP Chat FastAPI"
        },
        timeout=120.0
    )

    mcp_servers: List[MCPServerConfig] = []

    jina_url = os.getenv("JINA_MCP_URL", "https://mcp.jina.ai/sse")
    jina_api_key = os.getenv("JINA_API_KEY")
    if jina_api_key:
        mcp_servers.append(MCPServerConfig(
            name="jina",
            url=jina_url,
            transport="sse",
            headers={"Authorization": f"Bearer {jina_api_key}"}
        ))

    extra_servers = os.getenv("MCP_SERVER_URLS", "")
    if extra_servers:
        for url in extra_servers.split(","):
            url = url.strip()
            if url:
                mcp_servers.append(MCPServerConfig(
                    name=f"server_{len(mcp_servers)}",
                    url=url,
                    transport="sse"
                ))

    if not mcp_servers:
        print("WARNING: No MCP servers configured. Set JINA_API_KEY or MCP_SERVER_URLS", file=sys.stderr)
    else:
        try:
            server = mcp_servers[0]
            print(f"Connecting to MCP server: {server.name} at {server.url}")

            client_context = sse_client(server.url, server.headers) if server.transport == "sse" else streamablehttp_client(server.url, server.headers)

            streams = await client_context.__aenter__()
            client_context_entered = True
            read, write = streams

            session_ref = ClientSession(read, write)
            await session_ref.__aenter__()
            session_entered = True
            await session_ref.initialize()

            print("Fetching MCP tools...")
            mcp_tools = await fetch_mcp_tools(session_ref)
            print(f"Found {len(mcp_tools)} MCP tools")

            openai_tools, name_map = to_openai_tools(mcp_tools)
            mcp_state = MCPSessionState(
                session=session_ref,
                mcp_tools=mcp_tools,
                openai_tools=openai_tools,
                tool_name_map=name_map,
                server_configs=mcp_servers
            )
            print("MCP initialization complete")
        except Exception as exc:
            print(f"ERROR initializing MCP: {exc}", file=sys.stderr)
            import traceback
            traceback.print_exc()

    try:
        yield
    finally:
        db = await get_database()
        if db:
            try:
                await db.close()
            except Exception as exc:
                print(f"WARNING: Error closing database: {exc}", file=sys.stderr)

        session_to_close = None
        if mcp_state and mcp_state.session:
            session_to_close = mcp_state.session
        elif session_entered and session_ref:
            session_to_close = session_ref
        if session_to_close:
            try:
                await session_to_close.__aexit__(None, None, None)
            except Exception as exc:
                print(f"WARNING: Error closing MCP session: {exc}", file=sys.stderr)

        if client_context and client_context_entered:
            try:
                await client_context.__aexit__(None, None, None)
            except Exception as exc:
                print(f"WARNING: Error closing MCP transport: {exc}", file=sys.stderr)

        if http_client:
            try:
                await http_client.aclose()
            except Exception as exc:
                print(f"WARNING: Error closing HTTP client: {exc}", file=sys.stderr)
            finally:
                http_client = None

        mcp_state = None


app = FastAPI(title="MCP Chat API", version="1.0.0", lifespan=app_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# === Conversation API Endpoints ===

@app.get("/api/conversations")
async def list_conversations(limit: int = 100, offset: int = 0):
    """List all conversations"""
    db = await get_database()
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    conversations = await db.list_conversations(limit=limit, offset=offset)
    return {"conversations": conversations}


@app.get("/api/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Get a specific conversation with all messages"""
    db = await get_database()
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    conversation = await db.get_conversation(conversation_id)
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    messages = await db.get_conversation_messages(conversation_id)
    conversation["messages"] = messages

    return conversation


@app.post("/api/conversations")
async def create_conversation(request: dict):
    """Create a new conversation"""
    db = await get_database()
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    conversation_id = request.get("conversation_id") or str(uuid.uuid4())
    title = request.get("title", "New conversation")
    model = request.get("model")
    metadata = request.get("metadata")

    conversation = await db.create_conversation(
        conversation_id=conversation_id,
        title=title,
        model=model,
        metadata=metadata
    )

    return conversation


@app.put("/api/conversations/{conversation_id}")
async def update_conversation(conversation_id: str, request: dict):
    """Update conversation title"""
    db = await get_database()
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    title = request.get("title")
    if title:
        await db.update_conversation_title(conversation_id, title)

    return {"success": True}


@app.delete("/api/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str):
    """Delete a conversation"""
    db = await get_database()
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    await db.delete_conversation(conversation_id)
    return {"success": True}


@app.post("/api/test-db")
async def test_database():
    """Test database write and read operations"""
    db = await get_database()
    if not db:
        raise HTTPException(status_code=503, detail="Database not available")

    # Create test conversation
    test_id = f"test_{uuid.uuid4()}"
    conversation = await db.create_conversation(
        conversation_id=test_id,
        title="Test Conversation",
        model="test-model",
        metadata={"test": True}
    )

    # Add test messages
    msg1 = await db.add_message(
        conversation_id=test_id,
        role="user",
        content="Hello, this is a test message"
    )

    msg2 = await db.add_message(
        conversation_id=test_id,
        role="assistant",
        content="Hello! I'm a test response",
        tool_calls=[{"name": "test_tool", "args": {}}]
    )

    # Read back
    retrieved_conv = await db.get_conversation(test_id)
    messages = await db.get_conversation_messages(test_id)

    # Clean up
    await db.delete_conversation(test_id)

    return {
        "success": True,
        "created_conversation": conversation,
        "created_messages": [msg1, msg2],
        "retrieved_conversation": retrieved_conv,
        "retrieved_messages": messages
    }


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the polished MCP-style chat UI"""
    # Get tool count if MCP is initialized
    tools_count = len(mcp_state.openai_tools) if mcp_state else 0

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MCP Chat Assistant</title>
  <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.5/bundles/datastar.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;500;600;700&family=Space+Grotesk:wght@500&display=swap');
    :root {{
      color-scheme: light;
      --bg: #f4f5f8;
      --panel-bg: rgba(255, 255, 255, 0.98);
      --panel-border: rgba(15, 23, 42, 0.08);
      --panel-glow: rgba(99, 102, 241, 0.18);
      --text: #0f172a;
      --muted: #475569;
      --accent: #6366f1;
      --accent-strong: #a855f7;
      --shadow: 0 20px 55px rgba(15, 23, 42, 0.12);
      --border-soft: rgba(15, 23, 42, 0.1);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      font-family: "Sora", "Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }}
    a, button {{
      font-family: inherit;
    }}
    .app-shell {{
      min-height: 100vh;
      padding: 1.25rem;
      display: grid;
      grid-template-columns: 260px minmax(520px, 1fr) 300px;
      gap: 1rem;
      max-width: 1440px;
      margin: 0 auto;
      align-items: start;
    }}
    .sidebar, .main-column, .right-column {{
      background: rgba(255, 255, 255, 0.9);
      border-radius: 1.5rem;
      border: 1px solid var(--border-soft);
      box-shadow: var(--shadow);
    }}
    .sidebar {{
      padding: 1.35rem 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 1.25rem;
    }}
    .sidebar-brand {{
      display: flex;
      gap: 0.85rem;
      align-items: center;
    }}
    .brand-mark {{
      width: 48px;
      height: 48px;
      border-radius: 14px;
      background: linear-gradient(135deg, rgba(99, 102, 241, 0.35), rgba(168, 85, 247, 0.35));
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-weight: 700;
      color: white;
      letter-spacing: 0.1em;
    }}
    .sidebar-brand h1 {{
      margin: 0;
      font-size: 1.1rem;
    }}
    .eyebrow {{
      margin: 0 0 0.15rem 0;
      font-size: 0.7rem;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 600;
    }}
    .sidebar-actions {{
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }}
    .sidebar-section {{
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }}
    .section-title {{
      margin: 0;
      font-size: 0.85rem;
      letter-spacing: 0.2em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .sidebar-links {{
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }}
    .link-btn {{
      border: 0;
      background: #f1f5f9;
      padding: 0.65rem 0.9rem;
      border-radius: 0.85rem;
      text-align: left;
      color: var(--text);
      font-weight: 500;
      cursor: pointer;
      transition: background 0.2s ease;
    }}
    .link-btn:hover {{
      background: #e2e8f0;
    }}
    .main-column {{
      padding: 1.5rem;
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }}
    .main-header {{
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
    }}
    .main-header h2 {{
      margin: 0;
      font-size: 1.75rem;
    }}
    .header-actions {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }}
    .chat-card {{
      display: flex;
      flex-direction: column;
      gap: 1.25rem;
      position: relative;
      padding: 1.5rem;
    }}
    .panel {{
      box-shadow: none;
    }}
    .agent-hero {{
      display: flex;
      justify-content: space-between;
      gap: 1rem;
      align-items: center;
      background: rgba(99, 102, 241, 0.05);
      border-radius: 1rem;
      padding: 1rem 1.1rem;
      border: 1px solid rgba(99, 102, 241, 0.15);
    }}
    .hero-copy {{
      max-width: 70%;
    }}
    .hero-copy h2 {{
      margin: 0 0 0.5rem 0;
      font-size: 1.2rem;
    }}
    .hero-copy p {{
      margin: 0;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .hero-actions {{
      display: flex;
      gap: 0.5rem;
    }}
    .messages-panel {{
      background: rgba(244, 245, 248, 0.8);
      border-radius: 1.2rem;
      padding: 1rem 1.25rem;
      border: 1px solid rgba(15, 23, 42, 0.06);
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }}
    .messages-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .messages-header h3 {{
      margin: 0;
      font-size: 1rem;
    }}
    .message-stream {{
      min-height: 240px;
      max-height: 420px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }}
    .messages-list {{
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }}
    .message-item {{
      background: white;
      border-radius: 1.1rem;
      border: 1px solid rgba(15, 23, 42, 0.08);
      padding: 0.85rem 1rem;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }}
    .message-item.user {{
      align-self: flex-end;
      background: #eef2ff;
      border-color: rgba(99, 102, 241, 0.4);
      max-width: 80%;
    }}
    .message-item.assistant {{
      align-self: flex-start;
      max-width: 85%;
    }}
    .message-meta {{
      display: flex;
      justify-content: space-between;
      font-size: 0.75rem;
      color: var(--muted);
    }}
    .message-meta span {{
      font-weight: 600;
      color: var(--text);
    }}
    .message-content {{
      white-space: pre-wrap;
      word-wrap: break-word;
      line-height: 1.6;
    }}
    .message-tokens {{
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
      font-size: 0.75rem;
      color: var(--muted);
    }}
    .token-chip {{
      padding: 0.2rem 0.55rem;
      border-radius: 0.65rem;
      background: rgba(99, 102, 241, 0.08);
      border: 1px solid rgba(99, 102, 241, 0.2);
      color: #3730a3;
      font-weight: 600;
    }}
    .response-gallery {{
      background: #fff;
      border: 1px solid rgba(15, 23, 42, 0.08);
      border-radius: 1.2rem;
      padding: 1rem 1.2rem;
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      box-shadow: 0 10px 35px rgba(15, 23, 42, 0.08);
    }}
    .response-pills {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 0.75rem;
    }}
    .response-pill {{
      border: 1px solid rgba(99, 102, 241, 0.15);
      border-radius: 1rem;
      padding: 0.75rem;
      background: rgba(99, 102, 241, 0.04);
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      min-height: 110px;
    }}
    .response-pill .pill-header {{
      display: flex;
      justify-content: space-between;
      font-size: 0.75rem;
      color: var(--muted);
      font-weight: 600;
    }}
    .response-pill p {{
      margin: 0;
      font-size: 0.85rem;
      line-height: 1.3;
    }}
    .token-progress {{
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }}
    .progress-bar {{
      width: 100%;
      height: 10px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.08);
      overflow: hidden;
    }}
    .progress-fill {{
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(120deg, #6366f1, #a855f7);
      transition: width 0.3s ease;
    }}
    .token-stats {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 0.5rem;
      list-style: none;
      padding: 0;
      margin: 0;
      font-size: 0.8rem;
      color: var(--muted);
    }}
    .token-stats li {{
      display: flex;
      justify-content: space-between;
      border: 1px solid rgba(15, 23, 42, 0.08);
      border-radius: 0.8rem;
      padding: 0.4rem 0.6rem;
      background: #f8fafc;
    }}
    .iteration-feed, .tool-feed {{
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }}
    .iteration-card, .tool-event {{
      border: 1px solid rgba(15, 23, 42, 0.08);
      border-radius: 1rem;
      padding: 0.75rem 0.9rem;
      background: white;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }}
    .iteration-meta {{
      display: flex;
      justify-content: space-between;
      font-size: 0.75rem;
      color: var(--muted);
    }}
    .tool-event strong {{
      color: var(--text);
    }}
    .message-form {{
      display: flex;
      flex-direction: column;
      gap: 0.7rem;
    }}
    .message-form textarea {{
      width: 100%;
      border-radius: 1rem;
      border: 1px solid rgba(15, 23, 42, 0.15);
      padding: 0.9rem 1rem;
      font-family: inherit;
      font-size: 1rem;
      resize: vertical;
      min-height: 120px;
    }}
    .message-form textarea:focus {{
      outline: none;
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(99, 102, 241, 0.25);
    }}
    .form-footer {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.75rem;
      font-size: 0.85rem;
      color: var(--muted);
    }}
    .form-actions {{
      display: flex;
      gap: 0.5rem;
    }}
    .file-chip {{
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.4rem 0.7rem;
      background: rgba(59, 130, 246, 0.1);
      border: 1px solid rgba(59, 130, 246, 0.3);
      border-radius: 0.7rem;
      font-size: 0.85rem;
      color: #1e40af;
    }}
    .file-chip button {{
      border: none;
      background: transparent;
      cursor: pointer;
      padding: 0;
      display: flex;
      align-items: center;
      color: inherit;
    }}
    .right-column {{
      padding: 1.35rem 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }}
    .insight-card {{
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }}
    .panel-heading {{
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .panel-heading h4 {{
      margin: 0;
      font-size: 1rem;
    }}
    .tag {{
      padding: 0.2rem 0.7rem;
      border-radius: 0.8rem;
      border: 1px solid rgba(15, 23, 42, 0.12);
      font-size: 0.75rem;
    }}
    .tag-soft {{
      background: rgba(99, 102, 241, 0.08);
      color: #4338ca;
    }}
    .tag-accent {{
      background: rgba(14, 165, 233, 0.15);
      border-color: rgba(14, 165, 233, 0.4);
      color: #0ea5e9;
    }}
    .tag-xs {{
      font-size: 0.7rem;
      padding: 0.15rem 0.55rem;
    }}
    .status-stack {{
      display: flex;
      flex-direction: column;
      gap: 0.55rem;
    }}
    .status-pill {{
      padding: 0.65rem 0.9rem;
      border-radius: 0.85rem;
      font-weight: 600;
      border: 1px solid rgba(15, 23, 42, 0.08);
      background: #f8fafc;
    }}
    .status-pill.status-idle {{
      color: #0f172a;
    }}
    .tool-badge {{
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.35rem 0.6rem;
      background: rgba(14, 165, 233, 0.1);
      border: 1px solid rgba(14, 165, 233, 0.3);
      border-radius: 0.6rem;
      font-size: 0.8rem;
      color: #0369a1;
      margin-top: 0.25rem;
    }}
    .empty-state {{
      color: var(--muted);
      padding: 1rem;
      border-radius: 1rem;
      border: 1px dashed rgba(15, 23, 42, 0.2);
      background: rgba(15, 23, 42, 0.02);
      text-align: center;
    }}
    .btn {{
      border: none;
      border-radius: 0.9rem;
      padding: 0.6rem 1.25rem;
      font-weight: 600;
      cursor: pointer;
      transition: transform 0.1s ease, box-shadow 0.2s ease;
    }}
    .btn:focus-visible {{
      outline: 2px solid rgba(99, 102, 241, 0.6);
      outline-offset: 2px;
    }}
    .btn-primary {{
      background: linear-gradient(120deg, #6366f1, #a855f7);
      color: white;
      box-shadow: 0 10px 30px rgba(99, 102, 241, 0.25);
    }}
    .btn-secondary {{
      background: white;
      border: 1px solid rgba(15, 23, 42, 0.1);
      color: var(--text);
    }}
    .btn-outline {{
      background: transparent;
      border: 1px dashed rgba(15, 23, 42, 0.5);
      color: var(--text);
    }}
    .btn-ghost {{
      background: rgba(99, 102, 241, 0.08);
      border: 1px solid transparent;
      color: #4338ca;
    }}
    .btn:hover {{
      transform: translateY(-1px);
    }}
    .btn:disabled {{
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }}
    .status-foot {{
      margin: 0;
      font-size: 0.8rem;
      color: var(--muted);
    }}
    .typing-indicator {{
      display: inline-flex;
      gap: 0.3rem;
      align-items: center;
    }}
    .typing-indicator span {{
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--muted);
      animation: typing 1.4s infinite;
    }}
    .typing-indicator span:nth-child(2) {{
      animation-delay: 0.2s;
    }}
    .typing-indicator span:nth-child(3) {{
      animation-delay: 0.4s;
    }}
    @keyframes typing {{
      0%, 60%, 100% {{ transform: translateY(0); opacity: 0.4; }}
      30% {{ transform: translateY(-8px); opacity: 1; }}
    }}
    @media (max-width: 1160px) {{
      .app-shell {{
        grid-template-columns: minmax(240px, 280px) minmax(460px, 1fr);
      }}
      .right-column {{
        grid-column: span 2;
        order: 3;
      }}
    }}
    @media (max-width: 960px) {{
      .app-shell {{
        grid-template-columns: 1fr;
      }}
      .sidebar, .main-column, .right-column {{
        border-radius: 1rem;
      }}
      .sidebar, .right-column {{
        order: 0;
      }}
    }}
  </style>
</head>
<body data-signals='{{"messages":[],"currentMessage":"","conversationId":null,"isLoading":false,"toolsEnabled":true,"model":"anthropic/claude-sonnet-4.5","uploadedFiles":[],"toolsCount":{tools_count},"tokenUsage":{{"prompt":0,"completion":0,"total":0,"limit":0}},"recentResponses":[],"iterationFeed":[],"toolEvents":[],"budgetEvents":[],"activeIteration":null}}'>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="sidebar-brand">
        <span class="brand-mark">MCP</span>
        <div>
          <p class="eyebrow">Model Context Protocol</p>
          <h1>Chat Assistant</h1>
        </div>
      </div>
      <div class="sidebar-actions">
        <button class="btn btn-ghost" type="button" data-on-click="$$messages=[];$$conversationId=null;$$uploadedFiles=[]">+ New chat</button>
        <label class="btn btn-secondary" style="cursor: pointer; text-align: center; display: flex; align-items: center; justify-content: center; gap: 0.5rem;">
          <input type="file" id="fileInput" style="display: none;" multiple>
          📎 Upload files
        </label>
      </div>
      <div class="sidebar-section">
        <p class="section-title">Settings</p>
        <div class="sidebar-links">
          <label class="link-btn" style="cursor: pointer; display: flex; align-items: center; gap: 0.5rem;">
            <input type="checkbox" data-model="toolsEnabled" style="width: 16px; height: 16px;">
            <span>Enable MCP Tools</span>
          </label>
        </div>
      </div>
    </aside>
    <main class="main-column panel">
      <header class="main-header">
        <div>
          <p class="eyebrow">Conversations</p>
          <h2>Mission control</h2>
        </div>
        <div class="header-actions">
          <span class="tag tag-soft">Multi-turn</span>
          <span class="tag tag-accent">Realtime</span>
        </div>
      </header>
      <section class="chat-card">
        <div class="agent-hero">
          <div class="hero-copy">
            <h2>MCP Chat Assistant</h2>
            <p>Powered by OpenRouter with {tools_count} MCP tools available</p>
          </div>
          <div class="hero-actions">
            <span class="tag tag-soft" data-text="$model"></span>
          </div>
        </div>
        <div class="messages-panel">
          <div class="messages-header">
            <div>
              <p class="eyebrow">Live stream</p>
              <h3>Conversation timeline</h3>
            </div>
            <span class="tag tag-soft">Listening</span>
          </div>
          <div id="messages" class="message-stream">
            <div data-show="$messages.length === 0" class="empty-state">No conversation yet. Say something to start.</div>
            <div class="messages-list">
              <template data-for="(msg, idx) in $messages">
                <article data-bind-class="'message-item ' + msg.role" data-bind-key="idx">
                  <div class="message-meta">
                    <span data-text="msg.role === 'user' ? 'You' : 'Assistant'"></span>
                    <time data-text="msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString() : ''"></time>
                  </div>
                  <div data-show="msg.toolCalls && msg.toolCalls.length > 0" style="display: flex; flex-wrap: wrap; gap: 0.4rem;">
                    <template data-for="tool in msg.toolCalls">
                      <span class="tool-badge">🔧 <span data-text="tool.name"></span></span>
                    </template>
                  </div>
                  <div class="message-content" data-text="msg.content"></div>
                  <div class="message-tokens" data-show="msg.iteration || msg.tokenUsage">
                    <span class="token-chip" data-show="msg.iteration" data-text="'Iter ' + msg.iteration"></span>
                    <span class="token-chip" data-show="msg.tokenUsage" data-text="msg.tokenUsage ? 'Completion ' + (msg.tokenUsage.completion_tokens || 0) + ' tok' : ''"></span>
                    <span class="token-chip" data-show="msg.tokenUsage && msg.tokenUsage.prompt_tokens" data-text="'Prompt ' + msg.tokenUsage.prompt_tokens + ' tok'"></span>
                    <span class="token-chip" data-show="msg.tokenUsage && msg.tokenUsage.total_tokens" data-text="'Total ' + msg.tokenUsage.total_tokens + ' tok'"></span>
                  </div>
                </article>
              </template>
            </div>
            <div data-show="$isLoading" class="message-item assistant">
              <div class="typing-indicator">
                <span></span>
                <span></span>
                <span></span>
              </div>
            </div>
          </div>
        </div>
        <div class="response-gallery">
          <div class="messages-header">
            <div>
              <p class="eyebrow">Agent responses</p>
              <h3>Latest output</h3>
            </div>
            <span class="tag tag-soft" data-text="$recentResponses.length + ' stored'"></span>
          </div>
          <div class="response-pills" data-show="$recentResponses.length > 0">
            <template data-for="resp in $recentResponses">
              <article class="response-pill">
                <div class="pill-header">
                  <span data-text="resp.iteration ? 'Iter ' + resp.iteration : 'Response'"></span>
                  <span data-text="resp.tokens ? resp.tokens + ' tok' : ''"></span>
                </div>
                <p data-text="resp.preview"></p>
                <time data-text="resp.timestamp ? new Date(resp.timestamp).toLocaleTimeString() : ''"></time>
              </article>
            </template>
          </div>
          <div data-show="$recentResponses.length === 0" class="empty-state">No assistant responses yet.</div>
        </div>
        <div data-show="$uploadedFiles.length > 0" style="display: flex; flex-wrap: wrap; gap: 0.5rem; padding: 0.5rem 0;">
          <template data-for="(file, idx) in $uploadedFiles">
            <span class="file-chip">
              📄 <span data-text="file.name"></span>
              <button type="button" data-on-click="$$uploadedFiles = $uploadedFiles.filter((f, i) => i !== idx)">✕</button>
            </span>
          </template>
        </div>
        <form id="message-form" class="message-form">
          <textarea name="message" data-model="currentMessage" placeholder="Ask the agent anything..." rows="4"></textarea>
          <div class="form-footer">
            <span id="api-feedback"></span>
            <div class="form-actions">
              <button class="btn btn-primary" type="submit" data-bind-disabled="$isLoading || !$currentMessage.trim()">Send</button>
            </div>
          </div>
        </form>
      </section>
    </main>
    <aside class="right-column">
      <section class="insight-card panel">
        <div class="panel-heading">
          <h4>Status</h4>
          <span class="tag tag-xs">Live</span>
        </div>
        <div class="status-stack">
          <span class="status-pill status-idle" data-text="$isLoading ? 'Processing...' : 'Ready'"></span>
        </div>
        <p class="status-foot">Streaming updates in real-time via SSE.</p>
      </section>
      <section class="insight-card panel">
        <div class="panel-heading">
          <h4>MCP Tools</h4>
          <span class="tag tag-xs" data-text="$toolsCount + ' available'"></span>
        </div>
        <div class="status-stack">
          <span class="status-pill" data-text="$toolsEnabled ? 'Tools enabled' : 'Tools disabled'"></span>
        </div>
      </section>
      <section class="insight-card panel">
        <div class="panel-heading">
          <h4>Token budget</h4>
          <span class="tag tag-xs" data-text="$tokenUsage.limit ? ($tokenUsage.total + ' / ' + $tokenUsage.limit) : ($tokenUsage.total + ' tok')"></span>
        </div>
        <div class="token-progress">
          <div class="progress-bar">
            <div class="progress-fill" data-bind-style="'width:' + ($tokenUsage.limit ? Math.min(100, Math.round(($tokenUsage.total / ($tokenUsage.limit || 1)) * 100)) : Math.min(100, $tokenUsage.total || 5)) + '%'">
            </div>
          </div>
          <ul class="token-stats">
            <li><span>Prompt</span><strong data-text="$tokenUsage.prompt"></strong></li>
            <li><span>Completion</span><strong data-text="$tokenUsage.completion"></strong></li>
            <li><span>Total</span><strong data-text="$tokenUsage.total"></strong></li>
            <li><span>Limit</span><strong data-text="$tokenUsage.limit || '—'"></strong></li>
          </ul>
        </div>
      </section>
      <section class="insight-card panel">
        <div class="panel-heading">
          <h4>Multi-turn execution</h4>
          <span class="tag tag-xs" data-text="$iterationFeed.length + ' events'"></span>
        </div>
        <div class="iteration-feed">
          <template data-for="item in $iterationFeed">
            <article class="iteration-card">
              <div class="iteration-meta">
                <strong data-text="'Iter ' + item.iteration"></strong>
                <span data-text="item.timestamp ? new Date(item.timestamp).toLocaleTimeString() : ''"></span>
              </div>
              <p data-text="item.message_preview"></p>
              <div class="message-tokens" data-show="item.token_usage">
                <span class="token-chip" data-text="'Total ' + ((item.token_usage && (item.token_usage.total_tokens || item.token_usage.completion_tokens)) || 0) + ' tok'"></span>
                <span class="token-chip" data-show="item.token_usage && item.token_usage.prompt_tokens" data-text="'Prompt ' + item.token_usage.prompt_tokens + ' tok'"></span>
              </div>
            </article>
          </template>
          <div class="empty-state" data-show="$iterationFeed.length === 0">No iterations yet.</div>
        </div>
      </section>
      <section class="insight-card panel">
        <div class="panel-heading">
          <h4>Tool activity</h4>
          <span class="tag tag-xs" data-text="$toolEvents.length + ' calls'"></span>
        </div>
        <div class="tool-feed">
          <template data-for="event in $toolEvents">
            <article class="tool-event">
              <div class="iteration-meta">
                <strong data-text="event.name || event.tool_name"></strong>
                <span data-text="event.timestamp ? new Date(event.timestamp).toLocaleTimeString() : ''"></span>
              </div>
              <p data-text="event.summary"></p>
            </article>
          </template>
          <div class="empty-state" data-show="$toolEvents.length === 0">No tool calls yet.</div>
        </div>
      </section>
    </aside>
  </div>

  <script>
    // Helper to get Datastar's store
    const getStore = () => {{
      return document.body._dsPlugins?.store || {{}};
    }};

    const readSignals = () => {{
      const store = getStore();
      return store.value || JSON.parse(document.body.dataset.signals || '{{}}');
    }};

    const writeSignals = (signals) => {{
      const store = getStore();
      if (store.value) {{
        // Use Datastar's store if available
        Object.assign(store.value, signals);
      }} else {{
        // Fallback to dataset
        document.body.dataset.signals = JSON.stringify(signals);
      }}
    }};

    const updateSignals = (updater) => {{
      const signals = readSignals();
      updater(signals);
      writeSignals(signals);
      return signals;
    }};

    const pushRecentResponse = (entry) => {{
      updateSignals((signals) => {{
        const existing = signals.recentResponses || [];
        signals.recentResponses = [entry, ...existing].slice(0, 6);
      }});
    }};

    const pushIterationFeed = (entry) => {{
      updateSignals((signals) => {{
        const existing = signals.iterationFeed || [];
        signals.iterationFeed = [entry, ...existing].slice(0, 12);
      }});
    }};

    const pushToolEvent = (entry) => {{
      updateSignals((signals) => {{
        const existing = signals.toolEvents || [];
        signals.toolEvents = [entry, ...existing].slice(0, 12);
      }});
    }};

    const updateTokenUsage = (usage) => {{
      updateSignals((signals) => {{
        const current = signals.tokenUsage || {{ prompt: 0, completion: 0, total: 0, limit: 0 }};
        signals.tokenUsage = {{
          prompt: usage.prompt ?? current.prompt,
          completion: usage.completion ?? current.completion,
          total: usage.total ?? current.total,
          limit: usage.limit ?? current.limit
        }};
      }});
    }};

    const fileInput = document.getElementById('fileInput');
    const form = document.getElementById('message-form');
    const feedback = document.getElementById('api-feedback');
    const agentList = document.getElementById('agent-list');

    function renderFeedback(text, isError = false) {{
      if (!feedback) return;
      feedback.textContent = text;
      feedback.style.color = isError ? "#dc2626" : "var(--accent)";
    }}

    if (fileInput) {{
      fileInput.addEventListener('change', async (event) => {{
        const files = Array.from(event.target.files || []);
        const uploadedFiles = [];

        for (const file of files) {{
          const reader = new FileReader();
          const base64 = await new Promise((resolve) => {{
            reader.onload = () => resolve(reader.result.split(',')[1]);
            reader.readAsDataURL(file);
          }});

          uploadedFiles.push({{
            name: file.name,
            type: file.type,
            size: file.size,
            data: base64
          }});
        }}

        updateSignals((signals) => {{
          signals.uploadedFiles = [...(signals.uploadedFiles || []), ...uploadedFiles];
        }});

        event.target.value = '';
      }});
    }}

    if (form) {{
      form.addEventListener('submit', async (evt) => {{
        evt.preventDefault();

        const currentState = readSignals();
        const textarea = form.querySelector('textarea[name="message"]');
        const message = (textarea?.value || currentState.currentMessage || '').trim();

        if (!message) {{
          renderFeedback("Enter a message before sending.", true);
          return;
        }}

        const userEntry = {{
          role: 'user',
          content: message,
          timestamp: new Date().toISOString()
        }};

        updateSignals((signals) => {{
          signals.messages.push(userEntry);
          signals.currentMessage = '';
          signals.isLoading = true;
        }});

        if (textarea) textarea.value = '';

        const container = document.getElementById('messages');
        if (container) {{
          setTimeout(() => container.scrollTop = container.scrollHeight, 100);
        }}

        const payload = {{
          message,
          conversation_id: currentState.conversationId,
          model: currentState.model,
          stream: true,
          enable_tools: currentState.toolsEnabled,
          temperature: 0.7,
          max_tokens: 4000
        }};

        if ((currentState.uploadedFiles || []).length > 0) {{
          payload.files = currentState.uploadedFiles;
        }}

        try {{
          const response = await fetch('/chat', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(payload)
          }});

          if (!response.ok) {{
            throw new Error(`HTTP error! status: ${{response.status}}`);
          }}

          const reader = response.body?.getReader?.();
          if (!reader) {{
            throw new Error('Streaming is not supported in this environment.');
          }}

          const decoder = new TextDecoder();
          let assistantMessage = {{
            role: 'assistant',
            content: '',
            toolCalls: [],
            timestamp: new Date().toISOString()
          }};
          let messageInserted = false;
          let buffer = '';  // Buffer for incomplete SSE lines

          const syncAssistantMessage = () => {{
            updateSignals((signals) => {{
              if (!messageInserted) {{
                console.log('[syncAssistant] Inserting new message', assistantMessage);
                signals.messages.push({{ ...assistantMessage }});
                messageInserted = true;
              }} else {{
                console.log('[syncAssistant] Updating message', assistantMessage);
                signals.messages[signals.messages.length - 1] = {{ ...assistantMessage }};
              }}
              console.log('[syncAssistant] Total messages:', signals.messages.length);
            }});
            if (container) {{
              setTimeout(() => container.scrollTop = container.scrollHeight, 0);
            }}
          }};

          const handleEvent = (payloadEvent) => {{
            const eventType = payloadEvent.event;
            const data = payloadEvent.data || {{}};
            console.log('[SSE Event]', eventType, data);

            switch (eventType) {{
              case 'session.start':
                updateSignals((signals) => {{
                  signals.conversationId = payloadEvent.conversation_id;
                }});
                break;
              case 'message.start':
                updateSignals((signals) => {{
                  signals.activeIteration = data.iteration;
                }});
                // Show assistant message immediately with thinking indicator
                if (!messageInserted) {{
                  assistantMessage.content = '';
                  syncAssistantMessage();
                }}
                break;
              case 'message.delta':
                assistantMessage.content += data.delta || '';
                assistantMessage.iteration = data.iteration;
                assistantMessage.tokenEstimate = (assistantMessage.tokenEstimate || 0) + (data.token_delta_estimate || 0);
                syncAssistantMessage();
                break;
              case 'message.end':
                assistantMessage.tokenUsage = data.token_usage || null;
                syncAssistantMessage();
                pushRecentResponse({{
                  iteration: data.iteration,
                  preview: assistantMessage.content.length > 160 ? `${{assistantMessage.content.slice(0, 160)}}…` : assistantMessage.content,
                  tokens: (data.token_usage && (data.token_usage.completion_tokens || data.token_usage.total_tokens)) || assistantMessage.tokenEstimate || 0,
                  timestamp: payloadEvent.timestamp
                }});
                break;
              case 'budget.update':
                updateSignals((signals) => {{
                  signals.budgetEvents = [data, ...(signals.budgetEvents || [])].slice(0, 6);
                  if (data.limit) {{
                    signals.tokenUsage.limit = data.limit;
                  }}
                }});
                break;
              case 'usage.update':
                if (data.cumulative) {{
                  updateTokenUsage({{
                    prompt: data.cumulative.prompt,
                    completion: data.cumulative.completion,
                    total: data.cumulative.total,
                    limit: data.cumulative.limit
                  }});
                }}
                break;
              case 'iteration.summary':
                pushIterationFeed(data);
                break;
              case 'tool.call.start':
                if (!assistantMessage.toolCalls) assistantMessage.toolCalls = [];
                assistantMessage.toolCalls.push({{ name: data.tool_name, id: data.tool_id }});
                syncAssistantMessage();
                pushToolEvent({{
                  name: data.tool_name || 'Tool call',
                  summary: 'Started',
                  timestamp: payloadEvent.timestamp,
                  iteration: data.iteration
                }});
                break;
              case 'tool.call.execute':
                pushToolEvent({{
                  name: data.mcp_tool_name || data.tool_id,
                  summary: 'Executing',
                  timestamp: payloadEvent.timestamp,
                  iteration: data.iteration
                }});
                break;
              case 'tool.result':
                pushToolEvent({{
                  name: data.tool_id,
                  summary: `Result (${{data.result_length || 0}} chars)`,
                  timestamp: payloadEvent.timestamp,
                  iteration: data.iteration
                }});
                break;
              case 'tool.call.end':
                pushToolEvent({{
                  name: data.tool_id,
                  summary: 'Completed',
                  timestamp: payloadEvent.timestamp,
                  iteration: data.iteration
                }});
                break;
              case 'session.end':
                updateSignals((signals) => {{
                  signals.activeIteration = null;
                }});
                break;
              case 'error':
                renderFeedback(data.message || 'Server error', true);
                break;
              default:
                break;
            }}
          }};

          while (true) {{
            const {{ done, value }} = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, {{ stream: true }});
            const lines = buffer.split('\\n');

            // Keep the last incomplete line in buffer
            buffer = lines.pop() || '';

            for (const rawLine of lines) {{
              const line = rawLine.trim();
              if (!line || line.startsWith('event:') || line.startsWith(':')) {{
                continue;
              }}
              if (line.startsWith('data:')) {{
                const payloadStr = line.substring(5).trim();
                if (!payloadStr || payloadStr === '[DONE]') {{
                  continue;
                }}
                try {{
                  const parsed = JSON.parse(payloadStr);
                  handleEvent(parsed);
                }} catch (err) {{
                  console.error('Parse error:', err, line);
                }}
              }}
            }}
          }}

          updateSignals((signals) => {{
            signals.uploadedFiles = [];
          }});
          renderFeedback('Message sent successfully');
        }} catch (error) {{
          console.error('Chat error:', error);
          renderFeedback(error?.message || 'Request failed', true);
          updateSignals((signals) => {{
            signals.messages.push({{
              role: 'assistant',
              content: 'Sorry, there was an error processing your request.',
              timestamp: new Date().toISOString()
            }});
          }});
        }} finally {{
          updateSignals((signals) => {{
            signals.isLoading = false;
          }});
        }}
      }});
    }}

    if (agentList) {{
      agentList.addEventListener('click', (evt) => {{
        const button = evt.target.closest('[data-agent-id]');
        if (!button) {{
          return;
        }}
        const agentId = button.dataset.agentId;
        const params = new URLSearchParams(window.location.search);
        params.set('agent', agentId);
        window.location.search = params.toString();
      }});
    }}
  </script>

</body>
</html>
"""
    return HTMLResponse(content=html_content)


@app.get("/api/status")
async def api_status():
    """API health check"""
    return {
        "status": "online",
        "service": "MCP Chat FastAPI",
        "version": "1.0.0",
        "mcp_connected": mcp_state is not None,
        "tools_available": len(mcp_state.openai_tools) if mcp_state else 0,
        "timestamp": datetime.utcnow().isoformat(),
        "active_conversations": len(conversations),
        "total_messages": sum(len(state.get("messages", [])) for state in conversations.values()),
        "token_management_enabled": bool(ENABLE_TOKEN_MANAGEMENT and TOKEN_MANAGEMENT_AVAILABLE)
    }


@app.get("/tools")
async def list_tools():
    """List available MCP tools"""
    if not mcp_state:
        raise HTTPException(status_code=503, detail="MCP not initialized")

    return {
        "tools": mcp_state.mcp_tools,
        "count": len(mcp_state.mcp_tools)
    }


@app.post("/chat")
async def chat(request: ChatRequest):
    """Chat endpoint with streaming support"""

    if not mcp_state:
        raise HTTPException(status_code=503, detail="MCP not initialized")

    # Get or create conversation
    conversation_id = request.conversation_id or f"conv_{uuid.uuid4().hex[:12]}"
    conversation_state = get_conversation_state(conversation_id, request.model)
    conversation_state["model"] = request.model

    if request.stream:
        return StreamingResponse(
            stream_chat_response(
                conversation_id=conversation_id,
                user_message=request.message,
                model=request.model,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                enable_tools=request.enable_tools,
                system_prompt=request.system_prompt,
                files=request.files
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )
    else:
        # Non-streaming not implemented yet
        raise HTTPException(status_code=501, detail="Non-streaming mode not implemented")


@app.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Get conversation history"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    state = conversations[conversation_id]
    messages = state.get("messages", [])
    return {
        "conversation_id": conversation_id,
        "messages": messages,
        "message_count": len(messages),
        "token_usage": state.get("token_usage"),
        "iterations": state.get("iterations", 0),
        "iteration_summaries": state.get("iteration_summaries", [])
    }


@app.get("/conversations")
async def list_conversations():
    """List all conversations"""
    return {
        "conversations": [
            {
                "id": conv_id,
                "message_count": len(state.get("messages", []))
            }
            for conv_id, state in conversations.items()
        ],
        "total": len(conversations)
    }


@app.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str):
    """Delete a conversation"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    del conversations[conversation_id]
    return {"message": "Conversation deleted"}


if __name__ == "__main__":
    print("🚀 Starting MCP Chat FastAPI Server on port 2046")
    print("📡 Streaming with SSE and MCP tool integration")
    print("🔗 API docs: http://localhost:2046/docs")
    print("")
    print("Environment variables:")
    print("  OPENROUTER_API_KEY - Required for LLM calls")
    print("  JINA_API_KEY - Required for Jina MCP server")
    print("  MCP_SERVER_URLS - Comma-separated additional MCP servers")
    print("")

    uvicorn.run(
        "fastapi_mcp_chat:app",
        host="0.0.0.0",
        port=2046,
        reload=True,
        log_level="info"
    )
