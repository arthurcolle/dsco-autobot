"""
Unified Tool Management API

The single API gateway an agent needs to connect to.

Features:
- MCP server registration and discovery
- GraphQL unified interface
- REST API endpoints
- Semantic search for tools (Jina embeddings)
- Neo4j graph storage for internal tools
- Universal tool invocation (HTTP, SSE, MCP protocol)
- Secret management for API keys
- Session management for dynamic tool loading
- Batch/parallel/DAG tool execution
- Built-in capability servers

This combines:
1. The existing Neo4j-backed tool management for internal/custom tools
2. MCP registry for external MCP servers
3. GraphQL for unified query interface
4. REST API for compatibility
"""

from modal import App, Image, Secret, asgi_app
from fastapi import FastAPI, HTTPException, Depends, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import os
import json
import asyncio
import logging
import httpx

# ============================================================================
# Configuration
# ============================================================================

APP_LABEL = "unified_tool_api"
NEO4J_SECRET_NAME = "NEO4J_CREDENTIALS"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# ============================================================================
# Modal Configuration
# ============================================================================

image = (
    Image.debian_slim(python_version="3.11")
    .pip_install([
        "fastapi",
        "uvicorn",
        "pydantic",
        "neo4j",
        "httpx",
        "strawberry-graphql",
        "numpy",
    ])
)

stub = App(APP_LABEL)

# ============================================================================
# Import our modules (available at runtime)
# ============================================================================

# These will be available when running the actual app
# For development/testing, they're imported directly

try:
    from mcp_registry import (
        MCPRegistry, get_registry,
        MCPServerConfig, MCPServerInfo,
        SecretConfig, ProfileConfig,
        ToolSearchQuery, ToolSearchResult,
        ToolCallRequest, ToolCallResult,
        ExecutionPlan, ExecutionType
    )
    from graphql_schema import schema as graphql_schema, GRAPHQL_SYSTEM_PROMPT, GRAPHQL_RESOURCE
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    logger.warning("MCP registry modules not available")

try:
    from neo4j import AsyncGraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False
    AsyncGraphDatabase = None
    logger.warning("Neo4j driver not installed")

try:
    from strawberry.fastapi import GraphQLRouter
    GRAPHQL_AVAILABLE = True
except ImportError:
    GRAPHQL_AVAILABLE = False
    logger.warning("Strawberry GraphQL not available")

# ============================================================================
# Pydantic Models (for REST API)
# ============================================================================

# --- Neo4j Tools ---
class ToolCreate(BaseModel):
    """Create an internal tool stored in Neo4j"""
    name: str
    description: str
    input_schema: dict = Field(default_factory=dict)
    output_schema: dict = Field(default_factory=dict)
    code: str = ""
    tags: List[str] = Field(default_factory=list)


class ToolResponse(BaseModel):
    """Response for tool CRUD operations"""
    id: str
    name: str
    description: str
    input_schema: dict
    output_schema: dict
    source: str = "neo4j"  # "neo4j" or "mcp"


# --- MCP Server ---
class MCPServerCreate(BaseModel):
    """Create/register an MCP server"""
    name: str
    url: str
    transport: str = "http"
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    openapi_url: Optional[str] = None
    defer_loading: bool = False


class MCPServerResponse(BaseModel):
    """Response for MCP server operations"""
    name: str
    url: str
    transport: str
    description: Optional[str]
    healthy: bool
    tool_count: int
    tags: List[str]


# --- Secrets ---
class SecretCreate(BaseModel):
    """Create a secret"""
    key: str
    value: str
    description: Optional[str] = None
    linked_servers: List[str] = Field(default_factory=list)


class SecretResponse(BaseModel):
    """Secret response (no value)"""
    key: str
    description: Optional[str]
    linked_servers: List[str]


# --- Tool Search ---
class ToolSearchRequest(BaseModel):
    """Search request"""
    query: str
    limit: int = 10
    use_semantic: bool = True
    include_neo4j: bool = True
    include_mcp: bool = True


class ToolSearchResponse(BaseModel):
    """Search result"""
    tool_id: str
    name: str
    description: str
    source: str
    server: Optional[str]
    score: float
    match_reason: str


# --- Tool Execution ---
class ToolExecuteRequest(BaseModel):
    """Execute a tool"""
    tool_id: str
    input_data: Dict[str, Any] = Field(default_factory=dict)


class ToolExecuteResponse(BaseModel):
    """Execution result"""
    tool_id: str
    success: bool
    output_data: Optional[Any] = None
    error: Optional[str] = None
    execution_ms: int = 0


# --- Session ---
class SessionCreate(BaseModel):
    """Create a session"""
    context: str
    max_tools: int = 10
    token_budget: int = 8000


class SessionResponse(BaseModel):
    """Session info"""
    session_id: str
    active_tools: List[str]
    token_budget_used: int
    created_at: str


# ============================================================================
# Neo4j Connection Management
# ============================================================================

class Neo4jConnection:
    def __init__(self):
        if not NEO4J_AVAILABLE:
            self.driver = None
            return

        uri = os.getenv("NEO4J_URI", "bolt://n4j.brainchain.cloud")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password")
        self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))

    async def close(self):
        if self.driver:
            await self.driver.close()

    async def get_session(self):
        if self.driver:
            return self.driver.session()
        return None


_neo4j: Optional[Neo4jConnection] = None


async def get_neo4j():
    global _neo4j
    if _neo4j is None:
        _neo4j = Neo4jConnection()
    return _neo4j


async def get_neo4j_session():
    neo4j = await get_neo4j()
    session = await neo4j.get_session()
    if session is None:
        raise HTTPException(status_code=503, detail="Neo4j not available")
    async with session as s:
        yield s


# ============================================================================
# Embedding Provider
# ============================================================================

class EmbeddingProvider:
    def __init__(self):
        self.api_key = os.getenv("JINA_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.provider = "jina" if os.getenv("JINA_API_KEY") else "openai" if os.getenv("OPENAI_API_KEY") else None
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def embed(self, text: str) -> Optional[List[float]]:
        if not self.provider or not self.api_key:
            return None

        try:
            client = await self._get_client()

            if self.provider == "jina":
                dim = int(os.getenv("EMBEDDING_DIM", "1024"))
                response = await client.post(
                    "https://api.jina.ai/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json={"model": "jina-embeddings-v3", "input": [text], "dimensions": dim}
                )
                response.raise_for_status()
                return response.json()["data"][0]["embedding"]

            elif self.provider == "openai":
                response = await client.post(
                    "https://api.openai.com/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json={"model": "text-embedding-3-small", "input": text}
                )
                response.raise_for_status()
                return response.json()["data"][0]["embedding"]
        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return None


_embedder: Optional[EmbeddingProvider] = None


def get_embedder() -> EmbeddingProvider:
    global _embedder
    if _embedder is None:
        _embedder = EmbeddingProvider()
    return _embedder


# ============================================================================
# Built-in MCP Servers Configuration
# ============================================================================

BUILTIN_SERVERS = [
    {
        "name": "search-rag",
        "url": "http://localhost:8100",
        "description": "Search & RAG with Jina AI embeddings, reranking, classification",
        "tags": ["search", "rag", "embeddings", "jina", "rerank"]
    },
    {
        "name": "document-processing",
        "url": "http://localhost:8101",
        "description": "60 document chunking strategies for RAG, QA, summarization",
        "tags": ["chunking", "documents", "processing", "rag"]
    },
    {
        "name": "knowledge-base",
        "url": "http://localhost:8102",
        "description": "Knowledge base management with ArXiv integration",
        "tags": ["knowledge", "arxiv", "papers", "research"]
    },
    {
        "name": "model-router",
        "url": "http://localhost:8112",
        "description": "Model routing based on context, cost, modality, capabilities",
        "tags": ["routing", "models", "optimization", "cost"]
    }
]


# ============================================================================
# Create FastAPI App
# ============================================================================

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""

    app = FastAPI(
        title="Unified Tool Management API",
        description="The single gateway an agent needs for tool discovery, management, and invocation",
        version="1.0.0"
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ========================================================================
    # GraphQL Integration
    # ========================================================================

    if GRAPHQL_AVAILABLE and MCP_AVAILABLE:
        async def get_graphql_context():
            return {"registry": get_registry()}

        graphql_app = GraphQLRouter(
            graphql_schema,
            context_getter=get_graphql_context,
            graphiql=True
        )
        app.include_router(graphql_app, prefix="/graphql")

        @app.get("/graphql/prompt")
        async def graphql_prompt():
            """System prompt for agents using GraphQL."""
            return JSONResponse(content={
                "prompt": GRAPHQL_SYSTEM_PROMPT,
                "resource": GRAPHQL_RESOURCE,
                "endpoint": "/graphql"
            })

    # ========================================================================
    # Startup Events
    # ========================================================================

    @app.on_event("startup")
    async def startup():
        logger.info("Starting Unified Tool API...")

        # Create Neo4j vector index
        if NEO4J_AVAILABLE:
            try:
                neo4j = await get_neo4j()
                session = await neo4j.get_session()
                if session:
                    async with session as s:
                        dim = int(os.getenv("EMBEDDING_DIM", "1024"))
                        await s.run(f"""
                            CREATE VECTOR INDEX tool_embedding IF NOT EXISTS
                            FOR (t:Tool) ON t.embedding
                            OPTIONS {{indexConfig: {{
                                `vector.dimensions`: {dim},
                                `vector.similarity_function`: 'cosine'
                            }}}}
                        """)
                        logger.info(f"Neo4j vector index ensured ({dim} dims)")
            except Exception as e:
                logger.warning(f"Could not create vector index: {e}")

        # Register built-in servers
        if MCP_AVAILABLE:
            registry = get_registry()
            for server_config in BUILTIN_SERVERS:
                if not registry.get_server(server_config["name"]):
                    config = MCPServerConfig(
                        name=server_config["name"],
                        url=server_config["url"],
                        description=server_config["description"],
                        tags=server_config["tags"],
                        transport="http"
                    )
                    registry.register_server(config)
                    logger.info(f"Registered built-in server: {server_config['name']}")

        logger.info("Unified Tool API started!")

    @app.on_event("shutdown")
    async def shutdown():
        global _neo4j, _embedder
        if _neo4j:
            await _neo4j.close()
        if _embedder and _embedder._client:
            await _embedder._client.aclose()
        logger.info("Unified Tool API stopped")

    # ========================================================================
    # Root & Health Endpoints
    # ========================================================================

    @app.get("/", response_class=HTMLResponse)
    async def root():
        """Simple UI for the registry."""
        return """<!DOCTYPE html>
<html><head><title>Unified Tool API</title>
<style>body{font-family:system-ui;max-width:800px;margin:0 auto;padding:20px}
h1{color:#333}a{color:#0066cc}</style></head>
<body>
<h1>Unified Tool Management API</h1>
<p>The single gateway for tool discovery, management, and invocation.</p>
<h2>Endpoints</h2>
<ul>
<li><a href="/graphql">GraphQL Playground</a> - Unified query interface</li>
<li><a href="/docs">REST API Docs</a> - OpenAPI documentation</li>
<li><a href="/graphql/prompt">Agent Prompt</a> - System prompt for agents</li>
</ul>
<h2>Quick Start</h2>
<pre>
# Search for tools
curl -X POST /unified/search -d '{"query": "web search", "limit": 5}'

# Execute a tool
curl -X POST /unified/execute -d '{"tool_id": "search-rag::web_search", "input_data": {"query": "AI news"}}'

# GraphQL
curl -X POST /graphql -d '{"query": "{ stats { totalServers totalTools } }"}'
</pre>
</body></html>"""

    @app.get("/health")
    async def health():
        return {"status": "healthy", "graphql": GRAPHQL_AVAILABLE, "neo4j": NEO4J_AVAILABLE, "mcp": MCP_AVAILABLE}

    # ========================================================================
    # Unified Tool Endpoints
    # ========================================================================

    @app.get("/unified/stats")
    async def unified_stats():
        """Get unified stats across Neo4j tools and MCP servers."""
        stats = {"neo4j_tools": 0, "mcp_servers": 0, "mcp_tools": 0}

        if NEO4J_AVAILABLE:
            try:
                neo4j = await get_neo4j()
                session = await neo4j.get_session()
                if session:
                    async with session as s:
                        result = await s.run("MATCH (t:Tool) RETURN count(t) as count")
                        record = await result.single()
                        stats["neo4j_tools"] = record["count"] if record else 0
            except Exception:
                pass

        if MCP_AVAILABLE:
            registry = get_registry()
            mcp_stats = registry.get_stats()
            stats["mcp_servers"] = mcp_stats["total_servers"]
            stats["mcp_tools"] = mcp_stats["total_tools"]

        stats["total_tools"] = stats["neo4j_tools"] + stats["mcp_tools"]
        return stats

    @app.post("/unified/search", response_model=List[ToolSearchResponse])
    async def unified_search(request: ToolSearchRequest):
        """
        Search for tools across all sources (Neo4j + MCP).

        Uses semantic search with embeddings when available.
        """
        results = []

        # Search Neo4j tools
        if request.include_neo4j and NEO4J_AVAILABLE:
            try:
                embedder = get_embedder()
                query_embedding = await embedder.embed(request.query)

                neo4j = await get_neo4j()
                session = await neo4j.get_session()

                if session and query_embedding:
                    async with session as s:
                        result = await s.run(
                            """
                            CALL db.index.vector.queryNodes('tool_embedding', $k, $queryEmbedding)
                            YIELD node, score
                            RETURN node.name AS name, node.description AS description, ID(node) AS id, score
                            """,
                            k=request.limit,
                            queryEmbedding=query_embedding
                        )

                        async for record in result:
                            results.append(ToolSearchResponse(
                                tool_id=f"neo4j::{record['id']}",
                                name=record["name"],
                                description=record["description"] or "",
                                source="neo4j",
                                server=None,
                                score=record["score"],
                                match_reason="semantic"
                            ))
            except Exception as e:
                logger.warning(f"Neo4j search failed: {e}")

        # Search MCP tools
        if request.include_mcp and MCP_AVAILABLE:
            try:
                registry = get_registry()
                query = ToolSearchQuery(
                    query=request.query,
                    limit=request.limit,
                    use_semantic=request.use_semantic
                )

                mcp_results = await registry.search_tools(query)

                for r in mcp_results:
                    results.append(ToolSearchResponse(
                        tool_id=r.tool_id,
                        name=r.tool_name,
                        description=r.description,
                        source="mcp",
                        server=r.server_name,
                        score=r.score,
                        match_reason=r.match_reason
                    ))
            except Exception as e:
                logger.warning(f"MCP search failed: {e}")

        # Sort by score and limit
        results.sort(key=lambda x: -x.score)
        return results[:request.limit]

    @app.post("/unified/execute", response_model=ToolExecuteResponse)
    async def unified_execute(request: ToolExecuteRequest):
        """
        Execute a tool by ID.

        Tool ID format:
        - neo4j::{id} - Internal Neo4j tool
        - {server}::{tool_name} - MCP server tool
        """
        from time import time
        start = time()

        # Parse tool_id
        if "::" not in request.tool_id:
            return ToolExecuteResponse(
                tool_id=request.tool_id,
                success=False,
                error="Invalid tool_id format. Expected source::identifier"
            )

        source, identifier = request.tool_id.split("::", 1)

        # Neo4j tool
        if source == "neo4j" and NEO4J_AVAILABLE:
            try:
                neo4j = await get_neo4j()
                session = await neo4j.get_session()

                if session:
                    async with session as s:
                        result = await s.run(
                            """
                            MATCH (t:Tool)-[:HAS_CODE]->(c:ToolCode)
                            WHERE ID(t) = $tool_id
                            RETURN c.code AS code
                            """,
                            tool_id=int(identifier)
                        )
                        record = await result.single()

                        if record and record["code"]:
                            exec_globals = {"input_data": request.input_data, "output_data": {}}
                            exec(record["code"], exec_globals)
                            return ToolExecuteResponse(
                                tool_id=request.tool_id,
                                success=True,
                                output_data=exec_globals["output_data"],
                                execution_ms=int((time() - start) * 1000)
                            )
                        else:
                            return ToolExecuteResponse(
                                tool_id=request.tool_id,
                                success=False,
                                error="Tool not found or has no code",
                                execution_ms=int((time() - start) * 1000)
                            )

            except Exception as e:
                return ToolExecuteResponse(
                    tool_id=request.tool_id,
                    success=False,
                    error=str(e),
                    execution_ms=int((time() - start) * 1000)
                )

        # MCP tool
        elif MCP_AVAILABLE:
            registry = get_registry()
            call = ToolCallRequest(
                server_name=source,
                tool_name=identifier,
                arguments=request.input_data
            )
            result = await registry.execute_tool(call)

            return ToolExecuteResponse(
                tool_id=request.tool_id,
                success=result.success,
                output_data=result.result,
                error=result.error,
                execution_ms=result.execution_ms
            )

        return ToolExecuteResponse(
            tool_id=request.tool_id,
            success=False,
            error="Tool source not available"
        )

    # ========================================================================
    # MCP Server Endpoints
    # ========================================================================

    @app.get("/mcp/servers", response_model=List[MCPServerResponse])
    async def list_mcp_servers():
        """List all registered MCP servers."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        return [
            MCPServerResponse(
                name=s.name,
                url=s.url,
                transport=s.transport,
                description=s.description,
                healthy=s.healthy,
                tool_count=len(s.tools),
                tags=s.tags
            )
            for s in registry.list_servers()
        ]

    @app.post("/mcp/servers", response_model=MCPServerResponse)
    async def add_mcp_server(request: MCPServerCreate):
        """Register a new MCP server."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()

        if registry.get_server(request.name):
            raise HTTPException(status_code=409, detail=f"Server '{request.name}' already exists")

        config = MCPServerConfig(
            name=request.name,
            url=request.url,
            transport=request.transport,
            description=request.description,
            tags=request.tags,
            openapi_url=request.openapi_url,
            defer_loading=request.defer_loading
        )

        server = registry.register_server(config)

        return MCPServerResponse(
            name=server.name,
            url=server.url,
            transport=server.transport,
            description=server.description,
            healthy=server.healthy,
            tool_count=len(server.tools),
            tags=server.tags
        )

    @app.delete("/mcp/servers/{name}")
    async def remove_mcp_server(name: str):
        """Remove an MCP server."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        if not registry.unregister_server(name):
            raise HTTPException(status_code=404, detail=f"Server '{name}' not found")
        return {"status": "removed", "name": name}

    @app.post("/mcp/servers/{name}/sync")
    async def sync_mcp_server(name: str):
        """Sync tools from an MCP server."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        result = await registry.sync_server_tools(name)

        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])

        return result

    # ========================================================================
    # Secret Endpoints
    # ========================================================================

    @app.get("/secrets", response_model=List[SecretResponse])
    async def list_secrets():
        """List secrets (values never exposed)."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        return [
            SecretResponse(
                key=s.key,
                description=s.description,
                linked_servers=s.linked_servers
            )
            for s in registry.list_secrets()
        ]

    @app.post("/secrets", response_model=SecretResponse)
    async def set_secret(request: SecretCreate):
        """Set a secret."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        config = SecretConfig(
            key=request.key,
            value=request.value,
            description=request.description,
            linked_servers=request.linked_servers
        )
        secret = registry.set_secret(config)

        return SecretResponse(
            key=secret.key,
            description=secret.description,
            linked_servers=secret.linked_servers
        )

    @app.delete("/secrets/{key}")
    async def delete_secret(key: str):
        """Delete a secret."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        if not registry.delete_secret(key):
            raise HTTPException(status_code=404, detail=f"Secret '{key}' not found")
        return {"status": "deleted", "key": key}

    # ========================================================================
    # Session Endpoints
    # ========================================================================

    @app.post("/sessions", response_model=SessionResponse)
    async def create_session(request: SessionCreate):
        """Create a session with dynamically loaded tools."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        session = await registry.create_session(
            context=request.context,
            max_tools=request.max_tools,
            token_budget=request.token_budget
        )

        return SessionResponse(
            session_id=session.session_id,
            active_tools=session.active_tools,
            token_budget_used=session.token_budget_used,
            created_at=session.created_at.isoformat()
        )

    @app.get("/sessions/{session_id}", response_model=SessionResponse)
    async def get_session(session_id: str):
        """Get session info."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        session = registry.get_session(session_id)

        if not session:
            raise HTTPException(status_code=404, detail="Session not found")

        return SessionResponse(
            session_id=session_id,
            active_tools=session.active_tools,
            token_budget_used=session.token_budget_used,
            created_at=session.created_at.isoformat()
        )

    @app.delete("/sessions/{session_id}")
    async def close_session(session_id: str):
        """Close a session."""
        if not MCP_AVAILABLE:
            raise HTTPException(status_code=503, detail="MCP registry not available")

        registry = get_registry()
        if not registry.close_session(session_id):
            raise HTTPException(status_code=404, detail="Session not found")
        return {"status": "closed", "session_id": session_id}

    # ========================================================================
    # MCP Protocol Endpoint (for clients that speak MCP)
    # ========================================================================

    @app.post("/mcp")
    async def mcp_jsonrpc(request: Request):
        """
        MCP JSON-RPC endpoint.

        Supports:
        - tools/list - List all available tools
        - tools/call - Execute a tool
        """
        body = await request.json()
        method = body.get("method", "")
        params = body.get("params", {})
        req_id = body.get("id", 1)

        if method == "tools/list":
            tools = []

            # Collect from MCP registry
            if MCP_AVAILABLE:
                registry = get_registry()
                for server in registry.list_servers():
                    for tool in server.tools:
                        tools.append({
                            "name": f"{server.name}::{tool.get('name', '')}",
                            "description": tool.get("description", ""),
                            "inputSchema": tool.get("input_schema", {})
                        })

            # Collect from Neo4j
            if NEO4J_AVAILABLE:
                try:
                    neo4j = await get_neo4j()
                    session = await neo4j.get_session()
                    if session:
                        async with session as s:
                            result = await s.run("""
                                MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
                                RETURN t.name AS name, t.description AS description,
                                       s.input_schema AS input_schema, ID(t) AS id
                            """)
                            async for record in result:
                                tools.append({
                                    "name": f"neo4j::{record['id']}",
                                    "description": record["description"] or "",
                                    "inputSchema": json.loads(record["input_schema"]) if record["input_schema"] else {}
                                })
                except Exception:
                    pass

            return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}}

        elif method == "tools/call":
            tool_name = params.get("name", "")
            arguments = params.get("arguments", {})

            request = ToolExecuteRequest(tool_id=tool_name, input_data=arguments)
            result = await unified_execute(request)

            if result.success:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(result.output_data)}]
                    }
                }
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32000, "message": result.error}
                }

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method '{method}' not found"}
        }

    # ========================================================================
    # Agent Prompt Endpoint
    # ========================================================================

    @app.get("/prompt")
    async def agent_prompt():
        """Get system prompt for agents."""
        stats = {"total_servers": 0, "total_tools": 0}

        if MCP_AVAILABLE:
            registry = get_registry()
            mcp_stats = registry.get_stats()
            stats = mcp_stats

        prompt = f"""
# Unified Tool API

You have access to a unified tool gateway with {stats.get('total_tools', 0)} tools across {stats.get('total_servers', 0)} servers.

## Quick Reference

### Search for tools
```
POST /unified/search
{{"query": "what you need", "limit": 10}}
```

### Execute a tool
```
POST /unified/execute
{{"tool_id": "server::tool_name", "input_data": {{...}}}}
```

### GraphQL (recommended)
```
POST /graphql
{{"query": "{{ searchTools(input: {{query: \\"...\\"}}}}) {{ tool {{ toolId name }}}} }}"}}
```

## Tool ID Format
- `neo4j::{{id}}` - Internal tools
- `{{server}}::{{tool_name}}` - MCP server tools

## Available Servers
Query /mcp/servers to see all registered servers.

## Sessions
Create sessions to dynamically load relevant tools within a token budget:
```
POST /sessions
{{"context": "what you're doing", "max_tools": 10, "token_budget": 8000}}
```
"""
        return {"prompt": prompt, "stats": stats}

    return app


# ============================================================================
# Modal Entry Point
# ============================================================================

@stub.function(
    image=image,
    secrets=[Secret.from_name(NEO4J_SECRET_NAME)],
    timeout=600,
)
@asgi_app()
def app():
    """Modal ASGI app entry point."""
    return create_app()


# ============================================================================
# Local Development
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    application = create_app()
    uvicorn.run(application, host="0.0.0.0", port=PORT)
