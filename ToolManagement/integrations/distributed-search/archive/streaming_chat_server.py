"""
FastAPI server with comprehensive streaming endpoints for multi-turn chat
Streams all types of events: sessions, messages, updates, tool calls, etc.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, AsyncGenerator, Literal
from enum import Enum
import asyncio
import json
import time
from datetime import datetime
import uuid
import uvicorn


class EventType(str, Enum):
    """All possible streaming event types"""
    SESSION_START = "session.start"
    SESSION_UPDATE = "session.update"
    SESSION_END = "session.end"

    MESSAGE_START = "message.start"
    MESSAGE_DELTA = "message.delta"
    MESSAGE_UPDATE = "message.update"
    MESSAGE_END = "message.end"

    TOOL_CALL_START = "tool.call.start"
    TOOL_CALL_DELTA = "tool.call.delta"
    TOOL_CALL_END = "tool.call.end"
    TOOL_RESULT = "tool.result"

    THINKING_START = "thinking.start"
    THINKING_DELTA = "thinking.delta"
    THINKING_END = "thinking.end"

    ERROR = "error"
    METADATA = "metadata"
    HEARTBEAT = "heartbeat"


app = FastAPI(title="Streaming Chat Server", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store conversation history and metadata
conversations: Dict[str, Dict[str, Any]] = {}


class Message(BaseModel):
    role: str = Field(..., description="Role: user/assistant/system")
    content: str = Field(..., description="Message content")
    timestamp: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ToolCall(BaseModel):
    name: str
    arguments: Dict[str, Any]
    call_id: Optional[str] = None


class ChatRequest(BaseModel):
    conversation_id: Optional[str] = Field(None, description="Conversation ID for multi-turn chat")
    message: str = Field(..., description="User message")
    stream: bool = Field(True, description="Stream the response")
    model: str = Field("default", description="Model to use")
    temperature: float = Field(0.7, ge=0.0, le=2.0)
    max_tokens: int = Field(2000, gt=0)
    enable_tools: bool = Field(False, description="Enable tool calling simulation")
    enable_thinking: bool = Field(False, description="Enable thinking/reasoning display")
    metadata: Optional[Dict[str, Any]] = None


class StreamEvent(BaseModel):
    """Standardized streaming event format"""
    event: EventType
    data: Dict[str, Any]
    timestamp: str
    conversation_id: str
    message_id: Optional[str] = None
    sequence: int


def create_event(
    event_type: EventType,
    conversation_id: str,
    data: Dict[str, Any],
    message_id: Optional[str] = None,
    sequence: int = 0
) -> str:
    """Create a standardized SSE event"""
    event = StreamEvent(
        event=event_type,
        data=data,
        timestamp=datetime.utcnow().isoformat() + "Z",
        conversation_id=conversation_id,
        message_id=message_id,
        sequence=sequence
    )
    return f"event: {event_type.value}\ndata: {event.model_dump_json()}\n\n"


async def simulate_tool_call(
    tool_name: str,
    arguments: Dict[str, Any],
    conversation_id: str,
    message_id: str,
    sequence: int
) -> AsyncGenerator[str, None]:
    """Simulate a tool call with streaming events"""

    call_id = f"call_{uuid.uuid4().hex[:8]}"

    # Tool call start
    yield create_event(
        EventType.TOOL_CALL_START,
        conversation_id,
        {
            "call_id": call_id,
            "tool_name": tool_name,
            "arguments": arguments,
            "status": "starting"
        },
        message_id,
        sequence
    )

    await asyncio.sleep(0.3)

    # Tool call progress (delta)
    yield create_event(
        EventType.TOOL_CALL_DELTA,
        conversation_id,
        {
            "call_id": call_id,
            "status": "executing",
            "progress": 0.5
        },
        message_id,
        sequence + 1
    )

    await asyncio.sleep(0.5)

    # Tool call end
    yield create_event(
        EventType.TOOL_CALL_END,
        conversation_id,
        {
            "call_id": call_id,
            "tool_name": tool_name,
            "status": "completed",
            "duration_ms": 800
        },
        message_id,
        sequence + 2
    )

    # Tool result
    result_data = {
        "search": {"results": ["Result 1", "Result 2", "Result 3"]},
        "calculate": {"answer": 42},
        "weather": {"temp": 72, "condition": "sunny"}
    }.get(tool_name, {"status": "success"})

    yield create_event(
        EventType.TOOL_RESULT,
        conversation_id,
        {
            "call_id": call_id,
            "tool_name": tool_name,
            "result": result_data
        },
        message_id,
        sequence + 3
    )


async def generate_chat_stream(
    conversation_id: str,
    user_message: str,
    conversation_data: Dict[str, Any],
    model: str = "default",
    temperature: float = 0.7,
    max_tokens: int = 2000,
    enable_tools: bool = False,
    enable_thinking: bool = False,
    metadata: Optional[Dict[str, Any]] = None
) -> AsyncGenerator[str, None]:
    """
    Generate comprehensive streaming chat response with all event types
    """

    sequence = 0
    message_id = f"msg_{uuid.uuid4().hex[:12]}"

    # Update conversation metadata
    conversation_data["last_activity"] = datetime.utcnow().isoformat()
    conversation_data["message_count"] = len(conversation_data["messages"]) + 1

    # SESSION START
    yield create_event(
        EventType.SESSION_START,
        conversation_id,
        {
            "message_id": message_id,
            "model": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "user_message": user_message,
            "conversation_length": len(conversation_data["messages"]),
            "metadata": metadata or {}
        },
        message_id,
        sequence
    )
    sequence += 1

    await asyncio.sleep(0.1)

    # Add user message to history
    user_msg = {
        "role": "user",
        "content": user_message,
        "timestamp": datetime.utcnow().isoformat(),
        "message_id": f"msg_user_{uuid.uuid4().hex[:8]}"
    }
    conversation_data["messages"].append(user_msg)

    # SESSION UPDATE - user message added
    yield create_event(
        EventType.SESSION_UPDATE,
        conversation_id,
        {
            "action": "message_added",
            "role": "user",
            "message_count": len(conversation_data["messages"])
        },
        message_id,
        sequence
    )
    sequence += 1

    # THINKING (if enabled)
    if enable_thinking:
        yield create_event(
            EventType.THINKING_START,
            conversation_id,
            {
                "status": "analyzing",
                "context": "Processing user request"
            },
            message_id,
            sequence
        )
        sequence += 1

        thinking_text = "Let me think about this... "
        for i, word in enumerate(thinking_text.split()):
            yield create_event(
                EventType.THINKING_DELTA,
                conversation_id,
                {
                    "delta": word + " ",
                    "index": i
                },
                message_id,
                sequence
            )
            sequence += 1
            await asyncio.sleep(0.1)

        yield create_event(
            EventType.THINKING_END,
            conversation_id,
            {
                "status": "complete",
                "full_text": thinking_text
            },
            message_id,
            sequence
        )
        sequence += 1

    # TOOL CALLS (if enabled)
    if enable_tools and ("search" in user_message.lower() or "weather" in user_message.lower() or "calculate" in user_message.lower()):
        tool_name = "search" if "search" in user_message.lower() else "weather" if "weather" in user_message.lower() else "calculate"

        async for event in simulate_tool_call(
            tool_name,
            {"query": user_message},
            conversation_id,
            message_id,
            sequence
        ):
            yield event
            sequence += 1

        await asyncio.sleep(0.2)

    # MESSAGE START
    yield create_event(
        EventType.MESSAGE_START,
        conversation_id,
        {
            "message_id": message_id,
            "role": "assistant",
            "model": model
        },
        message_id,
        sequence
    )
    sequence += 1

    # Generate response
    base_response = f"I received your message: '{user_message}'. "

    if len(conversation_data["messages"]) > 2:
        base_response += f"This is message #{len(conversation_data['messages'])} in our conversation. "

    if enable_tools:
        base_response += "Tool calling is enabled. "

    if enable_thinking:
        base_response += "Thinking mode is active. "

    base_response += f"Using {model} at temperature {temperature}. "
    base_response += "This is a streaming response with comprehensive event tracking."

    # Stream response word by word with MESSAGE_DELTA events
    words = base_response.split()
    full_response = ""

    for i, word in enumerate(words):
        chunk = f"{word} " if i < len(words) - 1 else word
        full_response += chunk

        yield create_event(
            EventType.MESSAGE_DELTA,
            conversation_id,
            {
                "delta": chunk,
                "full_text": full_response,
                "index": i,
                "total_words": len(words)
            },
            message_id,
            sequence
        )
        sequence += 1

        await asyncio.sleep(0.05)

        # Send periodic MESSAGE_UPDATE events
        if i % 10 == 0 and i > 0:
            yield create_event(
                EventType.MESSAGE_UPDATE,
                conversation_id,
                {
                    "status": "streaming",
                    "progress": i / len(words),
                    "words_generated": i
                },
                message_id,
                sequence
            )
            sequence += 1

    # MESSAGE END
    tokens_used = len(words)
    yield create_event(
        EventType.MESSAGE_END,
        conversation_id,
        {
            "message_id": message_id,
            "full_text": full_response,
            "tokens_used": tokens_used,
            "finish_reason": "complete",
            "model": model
        },
        message_id,
        sequence
    )
    sequence += 1

    # Add assistant message to history
    assistant_msg = {
        "role": "assistant",
        "content": full_response,
        "timestamp": datetime.utcnow().isoformat(),
        "message_id": message_id,
        "tokens": tokens_used
    }
    conversation_data["messages"].append(assistant_msg)

    # SESSION UPDATE - assistant message added
    yield create_event(
        EventType.SESSION_UPDATE,
        conversation_id,
        {
            "action": "message_added",
            "role": "assistant",
            "message_count": len(conversation_data["messages"])
        },
        message_id,
        sequence
    )
    sequence += 1

    # METADATA
    yield create_event(
        EventType.METADATA,
        conversation_id,
        {
            "total_messages": len(conversation_data["messages"]),
            "total_tokens": sum(m.get("tokens", 0) for m in conversation_data["messages"]),
            "duration_ms": sequence * 50,  # Approximate
            "model_info": {
                "name": model,
                "temperature": temperature,
                "max_tokens": max_tokens
            }
        },
        message_id,
        sequence
    )
    sequence += 1

    # SESSION END
    yield create_event(
        EventType.SESSION_END,
        conversation_id,
        {
            "message_id": message_id,
            "status": "completed",
            "total_events": sequence,
            "conversation_id": conversation_id
        },
        message_id,
        sequence
    )


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "Comprehensive Streaming Chat Server",
        "version": "2.0.0",
        "port": 2046,
        "features": [
            "Multi-turn conversations",
            "Server-Sent Events (SSE)",
            "Session events",
            "Message streaming",
            "Tool call simulation",
            "Thinking/reasoning display",
            "Comprehensive metadata"
        ],
        "event_types": [e.value for e in EventType],
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/chat")
async def chat(request: ChatRequest):
    """
    Comprehensive chat endpoint with full event streaming.
    Streams: sessions, messages, updates, tool calls, thinking, metadata, etc.
    """

    # Generate or use provided conversation_id
    conversation_id = request.conversation_id or f"conv_{uuid.uuid4().hex[:12]}"

    # Get or create conversation
    if conversation_id not in conversations:
        conversations[conversation_id] = {
            "id": conversation_id,
            "created_at": datetime.utcnow().isoformat(),
            "messages": [],
            "metadata": request.metadata or {}
        }

    conversation_data = conversations[conversation_id]

    if request.stream:
        # Return streaming response with all events
        return StreamingResponse(
            generate_chat_stream(
                conversation_id=conversation_id,
                user_message=request.message,
                conversation_data=conversation_data,
                model=request.model,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                enable_tools=request.enable_tools,
                enable_thinking=request.enable_thinking,
                metadata=request.metadata
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "Access-Control-Allow-Origin": "*"
            }
        )
    else:
        # Non-streaming response
        message_id = f"msg_{uuid.uuid4().hex[:12]}"

        conversation_data["messages"].append({
            "role": "user",
            "content": request.message,
            "timestamp": datetime.utcnow().isoformat()
        })

        response_text = f"Non-streaming response to: '{request.message}'"

        conversation_data["messages"].append({
            "role": "assistant",
            "content": response_text,
            "timestamp": datetime.utcnow().isoformat(),
            "message_id": message_id
        })

        return {
            "conversation_id": conversation_id,
            "message_id": message_id,
            "message": response_text,
            "role": "assistant",
            "timestamp": datetime.utcnow().isoformat(),
            "stream": False
        }


@app.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str):
    """Get full conversation history"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    return conversations[conversation_id]


@app.get("/conversations")
async def list_conversations():
    """List all active conversations"""
    return {
        "conversations": [
            {
                "id": conv_id,
                "created_at": data.get("created_at"),
                "message_count": len(data.get("messages", [])),
                "last_activity": data.get("last_activity")
            }
            for conv_id, data in conversations.items()
        ],
        "total": len(conversations)
    }


@app.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: str):
    """Delete a conversation"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    del conversations[conversation_id]
    return {"message": "Conversation deleted", "conversation_id": conversation_id}


@app.post("/conversations/{conversation_id}/clear")
async def clear_conversation(conversation_id: str):
    """Clear conversation messages but keep metadata"""
    if conversation_id not in conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conversations[conversation_id]["messages"] = []
    conversations[conversation_id]["last_activity"] = datetime.utcnow().isoformat()

    return {"message": "Conversation cleared", "conversation_id": conversation_id}


@app.get("/event-types")
async def get_event_types():
    """List all available event types"""
    return {
        "event_types": [
            {
                "name": e.value,
                "category": e.value.split(".")[0],
                "description": {
                    "session.start": "Fired when a new chat session begins",
                    "session.update": "Fired when session state changes",
                    "session.end": "Fired when session completes",
                    "message.start": "Fired when assistant starts responding",
                    "message.delta": "Fired for each token/chunk of the message",
                    "message.update": "Fired for periodic message updates",
                    "message.end": "Fired when message is complete",
                    "tool.call.start": "Fired when a tool call begins",
                    "tool.call.delta": "Fired during tool execution",
                    "tool.call.end": "Fired when tool call completes",
                    "tool.result": "Fired with tool execution results",
                    "thinking.start": "Fired when reasoning begins",
                    "thinking.delta": "Fired for reasoning stream",
                    "thinking.end": "Fired when reasoning completes",
                    "error": "Fired on errors",
                    "metadata": "Fired with session metadata",
                    "heartbeat": "Keep-alive event"
                }.get(e.value, "Event type")
            }
            for e in EventType
        ]
    }


if __name__ == "__main__":
    print("🚀 Starting Comprehensive Streaming Chat Server on port 2046")
    print("📡 Streaming all event types: sessions, messages, tools, thinking, metadata")
    print("🔗 Server will be available at: http://localhost:2046")
    print("📖 API docs: http://localhost:2046/docs")
    print("🎯 Event types: http://localhost:2046/event-types")

    uvicorn.run(
        "streaming_chat_server:app",
        host="0.0.0.0",
        port=2046,
        reload=True,
        log_level="info"
    )
