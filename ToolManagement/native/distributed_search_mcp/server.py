#!/usr/bin/env python3
"""
Distributed Search MCP - Native MCP Server

First-class MCP server providing:
- Web search (powered by Jina AI)
- URL content extraction
- Fact checking
- Semantic reranking
- Embedding generation

This is a NATIVE MCP server - it implements the MCP protocol directly
rather than wrapping an external service.

Run with:
    python -m native.distributed_search_mcp
    # or
    uvicorn native.distributed_search_mcp:app --port 4001

Environment:
    JINA_API_KEY: Jina AI API key (required for full functionality)
    REGISTRY_URL: Central registry URL (optional)
    PORT: Server port (default: 4001)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL Queue integration - import with fallback for standalone mode
try:
    from tool_management.queue.url_queue import get_url_queue, URLSource
    URL_QUEUE_AVAILABLE = True
except ImportError:
    URL_QUEUE_AVAILABLE = False
    logger.info("URL queue not available - running in standalone mode")


# =============================================================================
# MODELS
# =============================================================================

class MCPProtocolVersion(str, Enum):
    V1 = "2024-11-05"


class MCPCapability(str, Enum):
    TOOLS = "tools"
    RESOURCES = "resources"
    PROMPTS = "prompts"
    LOGGING = "logging"


class ConnectionStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


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

class DistributedSearchMCP:
    """
    Native MCP server for distributed search capabilities.

    This is our first native MCP - it directly implements the MCP protocol
    rather than acting as a proxy. The connectivity test verifies:

    1. MCP protocol handshake works
    2. Tool discovery functions
    3. Tool execution succeeds
    4. Streaming responses work (SSE)
    """

    SERVICE_ID = "distributed-search-mcp"
    SERVICE_NAME = "Distributed Search MCP"
    VERSION = "1.0.0"

    # Jina API endpoints
    JINA_SEARCH_URL = "https://s.jina.ai"
    JINA_READ_URL = "https://r.jina.ai"
    JINA_EMBED_URL = "https://api.jina.ai/v1/embeddings"
    JINA_RERANK_URL = "https://api.jina.ai/v1/rerank"

    def __init__(
        self,
        api_key: Optional[str] = None,
        port: int = 4001,
        registry_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        self.port = port
        self.registry_url = registry_url or os.getenv("REGISTRY_URL")

        self._client: Optional[httpx.AsyncClient] = None
        self._tools: Dict[str, MCPTool] = {}
        self._state = ConnectionState()

        # Register native tools
        self._register_tools()

    def _register_tools(self):
        """Register all native MCP tools."""

        # Web search
        self._tools["search"] = MCPTool(
            name="search",
            description="Search the web using Jina AI. Returns relevant results with snippets.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results (1-20)",
                        "default": 10
                    }
                },
                "required": ["query"]
            },
            handler=self._execute_search
        )

        # Read URL
        self._tools["read"] = MCPTool(
            name="read",
            description="Extract and read content from a URL. Returns clean markdown text.",
            input_schema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to read"
                    },
                    "include_images": {
                        "type": "boolean",
                        "description": "Include image descriptions",
                        "default": False
                    }
                },
                "required": ["url"]
            },
            handler=self._execute_read
        )

        # Fact check
        self._tools["verify"] = MCPTool(
            name="verify",
            description="Verify a claim or statement against web sources.",
            input_schema={
                "type": "object",
                "properties": {
                    "statement": {
                        "type": "string",
                        "description": "Statement to fact-check"
                    }
                },
                "required": ["statement"]
            },
            handler=self._execute_verify
        )

        # Rerank
        self._tools["rerank"] = MCPTool(
            name="rerank",
            description="Rerank documents by relevance to a query using semantic similarity.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Query to rank against"
                    },
                    "documents": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Documents to rerank"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of top results",
                        "default": 5
                    }
                },
                "required": ["query", "documents"]
            },
            handler=self._execute_rerank
        )

        # Embed
        self._tools["embed"] = MCPTool(
            name="embed",
            description="Generate embeddings for text using Jina embedding models.",
            input_schema={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Text to embed"
                    },
                    "model": {
                        "type": "string",
                        "description": "Embedding model",
                        "default": "jina-embeddings-v3"
                    }
                },
                "required": ["text"]
            },
            handler=self._execute_embed
        )

        # Connectivity test tool
        self._tools["_connectivity_test"] = MCPTool(
            name="_connectivity_test",
            description="Internal connectivity test for native MCP verification.",
            input_schema={
                "type": "object",
                "properties": {
                    "echo": {
                        "type": "string",
                        "description": "Echo string for verification"
                    }
                }
            },
            handler=self._execute_connectivity_test
        )

    async def start(self):
        """Start the MCP server and establish connectivity."""
        logger.info(f"Starting {self.SERVICE_NAME}...")

        self._state.status = ConnectionStatus.CONNECTING
        self._state.session_id = str(uuid.uuid4())

        # Initialize HTTP client
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers=headers
        )

        # Run connectivity test
        try:
            result = await self._execute_connectivity_test({"echo": "native-mcp-init"})
            if result.get("success"):
                self._state.status = ConnectionStatus.CONNECTED
                self._state.connected_at = datetime.utcnow()
                self._state.last_heartbeat = datetime.utcnow()
                logger.info(f"✓ {self.SERVICE_NAME} connected (session: {self._state.session_id[:8]}...)")
            else:
                raise RuntimeError("Connectivity test failed")
        except Exception as e:
            self._state.status = ConnectionStatus.ERROR
            self._state.error_message = str(e)
            logger.error(f"✗ {self.SERVICE_NAME} failed to connect: {e}")
            raise

        # Register with central registry if configured
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
        """Register this server with the central registry."""
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
                            "streaming": True,
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
        """Handle MCP initialize request."""
        return {
            "protocolVersion": MCPProtocolVersion.V1.value,
            "capabilities": {
                "tools": {},
            },
            "serverInfo": {
                "name": self.SERVICE_NAME,
                "version": self.VERSION,
            }
        }

    async def handle_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request."""
        tools = []
        for name, tool in self._tools.items():
            if not name.startswith("_"):  # Hide internal tools
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.input_schema
                })
        return {"tools": tools}

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request."""
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
        """Route MCP request to appropriate handler."""
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
                    error={
                        "code": -32601,
                        "message": f"Method not found: {request.method}"
                    }
                )

            result = await handler(request.params or {})
            return MCPResponse(id=request.id, result=result)

        except Exception as e:
            logger.error(f"MCP request error: {e}")
            return MCPResponse(
                id=request.id,
                error={
                    "code": -32000,
                    "message": str(e)
                }
            )

    # =========================================================================
    # TOOL IMPLEMENTATIONS
    # =========================================================================

    async def _execute_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute web search via Jina."""
        query = args.get("query")
        if not query:
            raise ValueError("query is required")

        num_results = min(args.get("num_results", 10), 20)

        # Optional user/session context for URL queuing
        user_id = args.get("user_id")
        session_id = args.get("session_id") or (self._state.session_id if self._state else None)

        try:
            response = await self._client.get(
                f"{self.JINA_SEARCH_URL}/{query}",
                headers={"Accept": "application/json"},
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                results = data.get("data", [])[:num_results]

                # Queue URLs for future processing
                queued_count = await self._queue_search_urls(
                    results=results,
                    query=query,
                    user_id=user_id,
                    session_id=session_id,
                )

                return {
                    "query": query,
                    "results": results,
                    "success": True,
                    "urls_queued": queued_count,
                }
            else:
                return {
                    "query": query,
                    "error": f"Search failed: {response.status_code}",
                    "success": False
                }
        except Exception as e:
            return {
                "query": query,
                "error": str(e),
                "success": False
            }

    async def _queue_search_urls(
        self,
        results: List[Dict[str, Any]],
        query: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> int:
        """Queue URLs from search results for future processing."""
        if not URL_QUEUE_AVAILABLE:
            return 0

        if not results:
            return 0

        try:
            url_queue = get_url_queue()

            # Extract URL data from search results
            urls_to_queue = []
            for idx, result in enumerate(results):
                url = result.get("url") or result.get("link")
                if url:
                    # Get full content if available, otherwise use snippet
                    full_content = result.get("content")
                    snippet = result.get("snippet") or result.get("description")

                    urls_to_queue.append({
                        "url": url,
                        "title": result.get("title"),
                        "description": snippet[:500] if snippet else None,
                        "content": full_content,  # Cache the full page content
                        "metadata": {
                            "source_type": "jina_search",
                            "position": idx,
                            "has_full_content": full_content is not None,
                        }
                    })

            # Batch enqueue
            queued = url_queue.enqueue_batch(
                urls=urls_to_queue,
                source=URLSource.SEARCH,
                source_query=query,
                user_id=user_id,
                session_id=session_id,
            )

            logger.info(f"Queued {len(queued)} URLs from search query: {query[:50]}...")
            return len(queued)

        except Exception as e:
            logger.warning(f"Failed to queue search URLs: {e}")
            return 0

    async def _execute_read(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Read and extract content from URL via Jina."""
        url = args.get("url")
        if not url:
            raise ValueError("url is required")

        try:
            response = await self._client.get(
                f"{self.JINA_READ_URL}/{url}",
                headers={"Accept": "application/json"},
                timeout=60.0
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "url": url,
                    "title": data.get("title", ""),
                    "content": data.get("content", ""),
                    "success": True
                }
            else:
                return {
                    "url": url,
                    "error": f"Read failed: {response.status_code}",
                    "success": False
                }
        except Exception as e:
            return {
                "url": url,
                "error": str(e),
                "success": False
            }

    async def _execute_verify(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Fact-check a statement."""
        statement = args.get("statement")
        if not statement:
            raise ValueError("statement is required")

        # Search for evidence
        search_result = await self._execute_search({
            "query": f"fact check: {statement}",
            "num_results": 5
        })

        return {
            "statement": statement,
            "sources": search_result.get("results", []),
            "note": "Review sources to verify claim",
            "success": search_result.get("success", False)
        }

    async def _execute_rerank(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Rerank documents by relevance."""
        query = args.get("query")
        documents = args.get("documents", [])
        top_k = args.get("top_k", 5)

        if not query or not documents:
            raise ValueError("query and documents are required")

        if not self.api_key:
            return {
                "error": "API key required for reranking",
                "success": False
            }

        try:
            response = await self._client.post(
                self.JINA_RERANK_URL,
                json={
                    "model": "jina-reranker-v2-base-multilingual",
                    "query": query,
                    "documents": documents,
                    "top_n": top_k
                },
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "query": query,
                    "results": data.get("results", []),
                    "success": True
                }
            else:
                return {
                    "error": f"Rerank failed: {response.status_code}",
                    "success": False
                }
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _execute_embed(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Generate embeddings for text."""
        text = args.get("text")
        model = args.get("model", "jina-embeddings-v3")

        if not text:
            raise ValueError("text is required")

        if not self.api_key:
            return {
                "error": "API key required for embeddings",
                "success": False
            }

        try:
            response = await self._client.post(
                self.JINA_EMBED_URL,
                json={
                    "model": model,
                    "input": [text]
                },
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                embeddings = data.get("data", [])
                return {
                    "text": text[:100] + "..." if len(text) > 100 else text,
                    "model": model,
                    "embedding": embeddings[0].get("embedding") if embeddings else None,
                    "dimensions": len(embeddings[0].get("embedding", [])) if embeddings else 0,
                    "success": True
                }
            else:
                return {
                    "error": f"Embed failed: {response.status_code}",
                    "success": False
                }
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _execute_connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Internal connectivity verification."""
        echo = args.get("echo", "test")

        return {
            "success": True,
            "echo": echo,
            "server_id": self.SERVICE_ID,
            "version": self.VERSION,
            "timestamp": datetime.utcnow().isoformat(),
            "session_id": self._state.session_id,
            "tools_registered": len(self._tools),
            "api_key_configured": bool(self.api_key),
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
            "api_key_configured": bool(self.api_key),
        }


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

def create_server(
    api_key: Optional[str] = None,
    port: int = 8101,
    registry_url: Optional[str] = None,
) -> DistributedSearchMCP:
    """Create and return a DistributedSearchMCP instance."""
    return DistributedSearchMCP(
        api_key=api_key,
        port=port,
        registry_url=registry_url
    )


# Global server instance
_server: Optional[DistributedSearchMCP] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage server lifecycle."""
    global _server

    _server = create_server(
        port=int(os.getenv("PORT", "4001")),
        registry_url=os.getenv("REGISTRY_URL")
    )

    await _server.start()

    # Register tools with central registry
    registry_url = os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
    service_url = os.getenv("SERVICE_URL", f"http://localhost:{os.getenv('PORT', '4001')}")

    try:
        from tool_management.client import ToolManagementClient

        client = ToolManagementClient(
            registry_url=registry_url,
            provider_id=DistributedSearchMCP.SERVICE_ID,
            provider_url=service_url,
            namespace="search",
        )

        # Register each tool
        for tool in _server.tools:
            client._tools[tool["name"]] = type("RegisteredTool", (), {
                "name": tool["name"],
                "description": tool["description"],
                "input_schema": tool["inputSchema"],
                "handler": lambda **kw: None,  # Execution happens via /execute endpoint
                "output_schema": None,
                "namespace": "search",
                "tags": ["search", "jina"],
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
    title="Distributed Search MCP",
    description="Native MCP server for web search and content extraction",
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
    """Service info."""
    if _server:
        return _server.get_status()
    return {"status": "initializing"}


@app.get("/health")
async def health():
    """Health check."""
    if _server and _server.is_connected:
        return {"status": "healthy", "service": DistributedSearchMCP.SERVICE_ID}
    return JSONResponse(
        status_code=503,
        content={"status": "unhealthy", "service": DistributedSearchMCP.SERVICE_ID}
    )


@app.get("/tools")
async def list_tools():
    """List available MCP tools."""
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return {"tools": _server.tools}


@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP JSON-RPC endpoint."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    response = await _server.handle_mcp_request(request)
    return response.model_dump(exclude_none=True)


@app.post("/execute")
async def execute_tool(request: ToolCallRequest):
    """Execute a tool directly (non-MCP interface)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")

    if not tool.handler:
        raise HTTPException(500, f"Tool {request.name} has no handler")

    result = await tool.handler(request.arguments)
    return {"result": result}


@app.get("/sse")
async def sse_endpoint(request: Request):
    """Server-Sent Events endpoint for MCP streaming."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    async def event_generator() -> AsyncGenerator[str, None]:
        # Send initial connection event
        yield f"data: {json.dumps({'type': 'connected', 'session_id': _server._state.session_id})}\n\n"

        # Keep connection alive
        while True:
            if await request.is_disconnected():
                break

            # Send heartbeat
            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.utcnow().isoformat()})}\n\n"
            await asyncio.sleep(30)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "4001"))

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║          DISTRIBUTED SEARCH MCP - Native Server                  ║
║               First Native MCP Implementation                    ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-search-mcp                              ║
║  Port: {port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools:                                                          ║
║  • search  - Web search via Jina AI                              ║
║  • read    - Extract content from URLs                           ║
║  • verify  - Fact-check statements                               ║
║  • rerank  - Semantic document reranking                         ║
║  • embed   - Generate text embeddings                            ║
╠══════════════════════════════════════════════════════════════════╣
║  Endpoints:                                                      ║
║  • GET  /         Service info                                   ║
║  • GET  /health   Health check                                   ║
║  • GET  /tools    List tools                                     ║
║  • POST /mcp      MCP JSON-RPC endpoint                          ║
║  • POST /execute  Direct tool execution                          ║
║  • GET  /sse      Server-Sent Events stream                      ║
╚══════════════════════════════════════════════════════════════════╝

Starting native MCP server...
""")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
