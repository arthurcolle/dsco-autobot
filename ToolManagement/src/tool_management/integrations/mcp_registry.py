"""
MCP Registry - Universal MCP Server Registry

The single gateway an agent needs to connect to for tool access.
Supports:
- MCP server registration and discovery
- Semantic tool search with embeddings
- Universal tool invocation (HTTP, SSE, stdio)
- Secret management for API keys
- Session management for dynamic tool loading
- Batch/parallel/DAG tool execution
"""

import os
import re
import json
import asyncio
import hashlib
import logging
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from pydantic import BaseModel, Field

import httpx

logger = logging.getLogger(__name__)


# ============================================================================
# Models
# ============================================================================

class MCPServerConfig(BaseModel):
    """Configuration for registering an MCP server."""
    name: str = Field(..., description="Unique name/identifier for the MCP server")
    url: str = Field(..., description="Base URL of the MCP server (or stdio command)")
    transport: str = Field("http", description="Transport type: http, stdio, sse, mcp")
    description: Optional[str] = Field(None, description="Description of what this server provides")
    openapi_url: Optional[str] = Field(None, description="OpenAPI schema URL if available")
    tags: List[str] = Field(default_factory=list, description="Tags for categorizing the server")
    env_vars: Dict[str, str] = Field(default_factory=dict, description="Environment variables for this server")
    defer_loading: bool = Field(False, description="Defer tool loading until searched (saves tokens)")


class MCPServerInfo(MCPServerConfig):
    """Full info about a registered MCP server."""
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    last_health_check: Optional[datetime] = None
    healthy: bool = True
    tools: List[Dict] = Field(default_factory=list, description="Cached tools from this server")


class SecretConfig(BaseModel):
    """A secret/environment variable."""
    key: str = Field(..., description="Environment variable name")
    value: str = Field(..., description="Secret value")
    description: Optional[str] = Field(None, description="What this secret is for")
    linked_servers: List[str] = Field(default_factory=list, description="MCP servers that use this secret")


class ProfileConfig(BaseModel):
    """A profile is a named collection of servers."""
    name: str = Field(..., description="Profile name")
    description: Optional[str] = Field(None, description="Profile description")
    servers: List[str] = Field(default_factory=list, description="Server names in this profile")


class ToolSearchQuery(BaseModel):
    """Search query for tools across all servers."""
    query: str = Field(..., description="Search term for tool names/descriptions")
    profile: Optional[str] = Field(None, description="Limit to servers in this profile")
    limit: int = Field(10, description="Max results to return")
    use_semantic: bool = Field(True, description="Use semantic search vs keyword")


class ToolSearchResult(BaseModel):
    """A tool found via search."""
    server_name: str
    tool_name: str
    tool_id: str  # server::tool_name
    description: str
    input_schema: Dict
    examples: List[Dict] = Field(default_factory=list)
    score: float = 0.0
    match_reason: str = "keyword"


class ToolCallRequest(BaseModel):
    """Request to call a single tool."""
    server_name: str = Field(..., description="MCP server name")
    tool_name: str = Field(..., description="Tool/function name")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Tool arguments")


class ToolCallResult(BaseModel):
    """Result from calling a tool."""
    success: bool
    server_name: str
    tool_name: str
    tool_id: str
    result: Any = None
    error: Optional[str] = None
    execution_ms: int = 0


class ExecutionType(str, Enum):
    """Execution strategy for tool calls."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    COMPOUND = "compound"


class ExecutionStage(BaseModel):
    """A stage in an execution plan."""
    calls: List[ToolCallRequest]
    execution_type: ExecutionType
    depends_on: List[int] = Field(default_factory=list, description="Stage indices this depends on")


class ExecutionPlan(BaseModel):
    """Intelligent execution plan for multiple tools."""
    stages: List[ExecutionStage]
    estimated_execution_ms: Optional[int] = None
    strategy_reason: Optional[str] = None


class SessionInfo(BaseModel):
    """Active session with loaded tools."""
    session_id: str
    active_tools: List[str]  # tool_ids currently loaded
    token_budget_used: int
    created_at: datetime
    last_query: Optional[str] = None


# ============================================================================
# Jina AI Client for Embeddings/Reranking
# ============================================================================

class JinaClient:
    """Minimal async client for Jina AI embeddings and reranking."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("JINA_API_KEY")
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=60.0)
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def embed_text(
        self,
        texts: List[str],
        model: str = "jina-embeddings-v3",
        task: str = "retrieval.passage",
        dimensions: Optional[int] = None
    ) -> List[List[float]]:
        """Generate embeddings for texts."""
        if not self.api_key:
            return []

        client = await self._get_client()

        payload = {
            "model": model,
            "input": texts,
            "task": task,
            "normalized": True
        }
        if dimensions:
            payload["dimensions"] = dimensions

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        try:
            resp = await client.post(
                "https://api.jina.ai/v1/embeddings",
                json=payload,
                headers=headers
            )
            resp.raise_for_status()
            data = resp.json()
            return [item["embedding"] for item in data["data"]]
        except Exception as e:
            logger.error(f"Jina embed failed: {e}")
            return []

    async def rerank(
        self,
        query: str,
        documents: List[str],
        model: str = "jina-reranker-v2-base-multilingual",
        top_n: Optional[int] = None
    ) -> Dict[str, Any]:
        """Rerank documents by relevance to query."""
        if not self.api_key:
            return {"results": []}

        client = await self._get_client()

        payload = {
            "model": model,
            "query": query,
            "documents": documents
        }
        if top_n:
            payload["top_n"] = top_n

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        try:
            resp = await client.post(
                "https://api.jina.ai/v1/rerank",
                json=payload,
                headers=headers
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Jina rerank failed: {e}")
            return {"results": []}


# ============================================================================
# MCP Registry
# ============================================================================

class MCPRegistry:
    """
    In-memory MCP registry with JSON persistence.

    The registry manages:
    - MCP server registration and health
    - Tool discovery and caching
    - Secret management
    - Server profiles
    - Semantic search with embeddings
    - Sessions for dynamic tool loading
    """

    def __init__(self, data_file: str = "registry_data.json"):
        self.data_file = data_file
        self._servers: Dict[str, MCPServerInfo] = {}
        self._secrets: Dict[str, SecretConfig] = {}
        self._profiles: Dict[str, ProfileConfig] = {}
        self._tool_embeddings: Dict[str, List[float]] = {}  # tool_id -> embedding
        self._sessions: Dict[str, Dict] = {}  # session_id -> session data
        self._jina: Optional[JinaClient] = None
        self._load()

    def _load(self):
        """Load registry state from JSON file."""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r") as f:
                    data = json.load(f)

                for s in data.get("servers", []):
                    s["registered_at"] = datetime.fromisoformat(s["registered_at"])
                    if s.get("last_health_check"):
                        s["last_health_check"] = datetime.fromisoformat(s["last_health_check"])
                    self._servers[s["name"]] = MCPServerInfo(**s)

                for sec in data.get("secrets", []):
                    self._secrets[sec["key"]] = SecretConfig(**sec)

                for p in data.get("profiles", []):
                    self._profiles[p["name"]] = ProfileConfig(**p)

                logger.info(f"Loaded registry: {len(self._servers)} servers, {len(self._secrets)} secrets")
            except Exception as e:
                logger.warning(f"Could not load registry data: {e}")

    def _save(self):
        """Persist registry state to JSON file."""
        data = {
            "servers": [s.model_dump(mode="json") for s in self._servers.values()],
            "secrets": [s.model_dump() for s in self._secrets.values()],
            "profiles": [p.model_dump() for p in self._profiles.values()],
        }
        try:
            with open(self.data_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Could not save registry data: {e}")

    def get_jina(self) -> JinaClient:
        """Get or create Jina client."""
        if self._jina is None:
            self._jina = JinaClient()
        return self._jina

    # --- Server Management ---

    def register_server(self, config: MCPServerConfig) -> MCPServerInfo:
        """Register a new MCP server."""
        info = MCPServerInfo(**config.model_dump())
        self._servers[config.name] = info
        self._save()
        logger.info(f"Registered server: {config.name}")
        return info

    def unregister_server(self, name: str) -> bool:
        """Remove a server from the registry."""
        if name in self._servers:
            del self._servers[name]
            # Clear tool embeddings for this server
            to_remove = [k for k in self._tool_embeddings if k.startswith(f"{name}::")]
            for k in to_remove:
                del self._tool_embeddings[k]
            self._save()
            logger.info(f"Unregistered server: {name}")
            return True
        return False

    def get_server(self, name: str) -> Optional[MCPServerInfo]:
        """Get server by name."""
        return self._servers.get(name)

    def list_servers(self) -> List[MCPServerInfo]:
        """List all registered servers."""
        return list(self._servers.values())

    def update_server(self, name: str, **kwargs) -> Optional[MCPServerInfo]:
        """Update server attributes."""
        if name in self._servers:
            for k, v in kwargs.items():
                if hasattr(self._servers[name], k):
                    setattr(self._servers[name], k, v)
            self._save()
            return self._servers[name]
        return None

    # --- Secret Management ---

    def set_secret(self, config: SecretConfig) -> SecretConfig:
        """Set a secret."""
        self._secrets[config.key] = config
        self._save()
        return config

    def delete_secret(self, key: str) -> bool:
        """Delete a secret."""
        if key in self._secrets:
            del self._secrets[key]
            self._save()
            return True
        return False

    def get_secret(self, key: str) -> Optional[SecretConfig]:
        """Get a secret (including value)."""
        return self._secrets.get(key)

    def list_secrets(self) -> List[SecretConfig]:
        """List all secrets."""
        return list(self._secrets.values())

    def get_secrets_for_server(self, server_name: str) -> Dict[str, str]:
        """Get all secrets linked to a server."""
        result = {}
        for secret in self._secrets.values():
            if server_name in secret.linked_servers:
                result[secret.key] = secret.value
        return result

    # --- Profile Management ---

    def set_profile(self, config: ProfileConfig) -> ProfileConfig:
        """Create or update a profile."""
        self._profiles[config.name] = config
        self._save()
        return config

    def delete_profile(self, name: str) -> bool:
        """Delete a profile."""
        if name in self._profiles:
            del self._profiles[name]
            self._save()
            return True
        return False

    def get_profile(self, name: str) -> Optional[ProfileConfig]:
        """Get a profile."""
        return self._profiles.get(name)

    def list_profiles(self) -> List[ProfileConfig]:
        """List all profiles."""
        return list(self._profiles.values())

    def get_servers_for_profile(self, profile_name: str) -> List[MCPServerInfo]:
        """Get all servers in a profile."""
        profile = self._profiles.get(profile_name)
        if not profile:
            return []
        return [self._servers[n] for n in profile.servers if n in self._servers]

    # --- Tool Sync ---

    async def sync_server_tools(self, name: str) -> Dict[str, Any]:
        """
        Fetch and cache tools from a server.
        Supports both OpenAPI and MCP protocol.
        """
        server = self.get_server(name)
        if not server:
            return {"error": f"Server '{name}' not found"}

        async with httpx.AsyncClient(timeout=10.0) as client:
            tools = []
            protocol = "unknown"

            # Try MCP protocol first
            if server.transport in ("mcp", "sse") or "/mcp" in server.url:
                try:
                    tools = await self._sync_mcp_tools(client, server)
                    protocol = "mcp"
                except Exception:
                    pass

            # Try OpenAPI if MCP didn't work
            if not tools:
                try:
                    tools = await self._sync_openapi_tools(client, server)
                    protocol = "openapi"
                except Exception as e:
                    logger.warning(f"Could not sync tools for {name}: {e}")

            if tools:
                self.update_server(name, tools=tools)
                # Clear cached embeddings
                to_remove = [k for k in self._tool_embeddings if k.startswith(f"{name}::")]
                for k in to_remove:
                    del self._tool_embeddings[k]

            return {
                "synced": len(tools),
                "tools": [t.get("name") for t in tools],
                "protocol": protocol
            }

    async def _sync_mcp_tools(self, client: httpx.AsyncClient, server: MCPServerInfo) -> List[Dict]:
        """Sync tools using MCP JSON-RPC protocol."""
        mcp_url = server.url.rstrip('/') + '/mcp' if not server.url.endswith('/mcp') else server.url

        request_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }

        resp = await client.post(mcp_url, json=request_payload)
        resp.raise_for_status()
        data = resp.json()

        if "result" in data and "tools" in data["result"]:
            mcp_tools = data["result"]["tools"]
            tools = []
            for tool in mcp_tools:
                tools.append({
                    "name": tool.get("name", "unknown"),
                    "description": tool.get("description", ""),
                    "input_schema": tool.get("inputSchema", {}),
                    "path": "/mcp",
                    "method": "post",
                    "mcp_protocol": True
                })
            return tools

        return []

    async def _sync_openapi_tools(self, client: httpx.AsyncClient, server: MCPServerInfo) -> List[Dict]:
        """Sync tools from OpenAPI schema."""
        schema_url = server.openapi_url or f"{server.url}/openapi.json"
        resp = await client.get(schema_url)
        resp.raise_for_status()
        schema = resp.json()

        components = schema.get("components", {})
        tools = []
        paths = schema.get("paths", {})

        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ("get", "post", "put", "delete", "patch"):
                    tool_name = details.get("operationId", f"{method}_{path}")

                    # Resolve input schema
                    raw_input_schema = details.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema", {})
                    input_schema = self._resolve_schema(raw_input_schema, components)

                    tools.append({
                        "name": tool_name,
                        "description": details.get("summary", details.get("description", "")),
                        "input_schema": input_schema,
                        "path": path,
                        "method": method,
                        "mcp_protocol": False
                    })

        return tools

    def _resolve_schema(self, schema: Dict, components: Dict, depth: int = 0) -> Dict:
        """Resolve $ref in OpenAPI schema."""
        if depth > 5:
            return schema

        if "$ref" in schema:
            ref = schema["$ref"]
            if ref.startswith("#/components/schemas/"):
                schema_name = ref.split("/")[-1]
                resolved = components.get("schemas", {}).get(schema_name, {})
                return self._resolve_schema(resolved, components, depth + 1)

        if "properties" in schema:
            resolved_props = {}
            for prop_name, prop_schema in schema["properties"].items():
                resolved_props[prop_name] = self._resolve_schema(prop_schema, components, depth + 1)
            schema = {**schema, "properties": resolved_props}

        return schema

    # --- Tool Search ---

    async def search_tools(self, query: ToolSearchQuery) -> List[ToolSearchResult]:
        """
        Search for tools across all servers.

        Supports:
        - Semantic search with Jina embeddings
        - Keyword fallback
        - Profile filtering
        - Reranking for improved relevance
        """
        # Get candidate tools
        all_tools = []
        servers = self.get_servers_for_profile(query.profile) if query.profile else self.list_servers()

        for server in servers:
            for tool in server.tools:
                tool_id = f"{server.name}::{tool.get('name', '')}"
                text = f"{tool.get('name', '')} {tool.get('description', '')}"
                all_tools.append({
                    "tool_id": tool_id,
                    "server": server,
                    "tool": tool,
                    "text": text
                })

        if not all_tools:
            return []

        results = []

        # Try semantic search
        if query.use_semantic:
            jina = self.get_jina()
            if jina.api_key:
                try:
                    results = await self._semantic_search(query.query, all_tools, query.limit, jina)
                except Exception as e:
                    logger.warning(f"Semantic search failed: {e}")

        # Fall back to keyword search
        if not results:
            results = self._keyword_search(query.query, all_tools, query.limit)

        return results

    async def _semantic_search(
        self,
        query: str,
        tools: List[Dict],
        limit: int,
        jina: JinaClient
    ) -> List[ToolSearchResult]:
        """Semantic search using embeddings and reranking."""
        import numpy as np

        # Embed query
        query_embeddings = await jina.embed_text([query], task="retrieval.query")
        if not query_embeddings:
            return []

        query_vec = np.array(query_embeddings[0])
        query_vec = query_vec / np.linalg.norm(query_vec)

        # Embed tools if not cached
        tools_to_embed = []
        for t in tools:
            if t["tool_id"] not in self._tool_embeddings:
                tools_to_embed.append(t)

        if tools_to_embed:
            texts = [t["text"] for t in tools_to_embed]
            embeddings = await jina.embed_text(texts, task="retrieval.passage")
            for t, emb in zip(tools_to_embed, embeddings):
                self._tool_embeddings[t["tool_id"]] = emb

        # Calculate similarities
        scored = []
        for t in tools:
            if t["tool_id"] in self._tool_embeddings:
                tool_vec = np.array(self._tool_embeddings[t["tool_id"]])
                tool_vec = tool_vec / np.linalg.norm(tool_vec)
                score = float(np.dot(query_vec, tool_vec))
                scored.append((t, score))

        # Sort by score
        scored.sort(key=lambda x: -x[1])

        # Rerank top candidates
        if len(scored) > 3:
            top_texts = [s[0]["text"] for s in scored[:20]]
            reranked = await jina.rerank(query, top_texts, top_n=limit)

            results = []
            for r in reranked.get("results", []):
                idx = r["index"]
                t = scored[idx][0]
                results.append(self._to_search_result(t, r["relevance_score"], "semantic + rerank"))
            return results
        else:
            return [
                self._to_search_result(t, score, "semantic")
                for t, score in scored[:limit]
            ]

    def _keyword_search(
        self,
        query: str,
        tools: List[Dict],
        limit: int
    ) -> List[ToolSearchResult]:
        """Simple keyword search fallback."""
        query_lower = query.lower()
        pattern = re.compile(re.escape(query_lower), re.IGNORECASE)

        scored = []
        for t in tools:
            text_lower = t["text"].lower()
            score = 0.0

            # Name match = higher score
            if pattern.search(t["tool"]["name"]):
                score += 2.0
            # Description match
            if pattern.search(t["tool"].get("description", "")):
                score += 1.0

            if score > 0:
                scored.append((t, score))

        scored.sort(key=lambda x: -x[1])
        return [
            self._to_search_result(t, score, "keyword")
            for t, score in scored[:limit]
        ]

    def _to_search_result(self, t: Dict, score: float, reason: str) -> ToolSearchResult:
        """Convert internal tool dict to ToolSearchResult."""
        tool = t["tool"]
        server = t["server"]
        return ToolSearchResult(
            server_name=server.name,
            tool_name=tool.get("name", ""),
            tool_id=t["tool_id"],
            description=tool.get("description", ""),
            input_schema=tool.get("input_schema", {}),
            score=score,
            match_reason=reason
        )

    # --- Tool Execution ---

    async def execute_tool(self, request: ToolCallRequest) -> ToolCallResult:
        """
        Execute a tool call on an MCP server.

        Handles:
        - Tool lookup and validation
        - Secret injection
        - HTTP/MCP protocol routing
        """
        from time import time
        start = time()

        tool_id = f"{request.server_name}::{request.tool_name}"
        server = self.get_server(request.server_name)

        if not server:
            return ToolCallResult(
                success=False,
                server_name=request.server_name,
                tool_name=request.tool_name,
                tool_id=tool_id,
                error=f"Server '{request.server_name}' not found",
                execution_ms=int((time() - start) * 1000)
            )

        if not server.healthy:
            return ToolCallResult(
                success=False,
                server_name=request.server_name,
                tool_name=request.tool_name,
                tool_id=tool_id,
                error=f"Server '{request.server_name}' is unhealthy",
                execution_ms=int((time() - start) * 1000)
            )

        # Find tool
        tool = next((t for t in server.tools if t.get("name") == request.tool_name), None)
        if not tool:
            return ToolCallResult(
                success=False,
                server_name=request.server_name,
                tool_name=request.tool_name,
                tool_id=tool_id,
                error=f"Tool '{request.tool_name}' not found on server",
                execution_ms=int((time() - start) * 1000)
            )

        # Get secrets for this server
        env_vars = dict(server.env_vars)
        env_vars.update(self.get_secrets_for_server(request.server_name))

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                headers = {"Content-Type": "application/json"}

                # Inject API keys
                for key, value in env_vars.items():
                    if "API_KEY" in key.upper():
                        headers["Authorization"] = f"Bearer {value}"

                # MCP protocol
                if tool.get("mcp_protocol", False):
                    mcp_url = server.url.rstrip('/') + '/mcp'
                    payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {
                            "name": request.tool_name,
                            "arguments": request.arguments
                        }
                    }

                    resp = await client.post(mcp_url, json=payload, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()

                    if "result" in data:
                        return ToolCallResult(
                            success=True,
                            server_name=request.server_name,
                            tool_name=request.tool_name,
                            tool_id=tool_id,
                            result=data["result"],
                            execution_ms=int((time() - start) * 1000)
                        )
                    elif "error" in data:
                        return ToolCallResult(
                            success=False,
                            server_name=request.server_name,
                            tool_name=request.tool_name,
                            tool_id=tool_id,
                            error=f"MCP error: {data['error']}",
                            execution_ms=int((time() - start) * 1000)
                        )

                # REST/OpenAPI protocol
                else:
                    path = tool.get("path", "")
                    method = tool.get("method", "post").lower()
                    url = f"{server.url}{path}"

                    if method == "get":
                        resp = await client.get(url, params=request.arguments, headers=headers)
                    elif method in ("post", "put", "patch"):
                        resp = await client.request(method, url, json=request.arguments, headers=headers)
                    else:
                        resp = await client.delete(url, headers=headers)

                    resp.raise_for_status()
                    result = resp.json() if resp.content else {"status": "success"}

                    return ToolCallResult(
                        success=True,
                        server_name=request.server_name,
                        tool_name=request.tool_name,
                        tool_id=tool_id,
                        result=result,
                        execution_ms=int((time() - start) * 1000)
                    )

        except Exception as e:
            return ToolCallResult(
                success=False,
                server_name=request.server_name,
                tool_name=request.tool_name,
                tool_id=tool_id,
                error=str(e),
                execution_ms=int((time() - start) * 1000)
            )

    async def execute_tools_parallel(self, calls: List[ToolCallRequest]) -> List[ToolCallResult]:
        """Execute multiple tool calls in parallel."""
        tasks = [self.execute_tool(call) for call in calls]
        return await asyncio.gather(*tasks)

    async def execute_tools_sequential(self, calls: List[ToolCallRequest]) -> List[ToolCallResult]:
        """Execute tool calls sequentially."""
        results = []
        for call in calls:
            result = await self.execute_tool(call)
            results.append(result)
        return results

    def create_execution_plan(self, calls: List[ToolCallRequest]) -> ExecutionPlan:
        """
        Analyze tool calls and create an intelligent execution plan.

        Detects patterns in tool names to determine execution strategy.
        """
        if not calls:
            return ExecutionPlan(stages=[], strategy_reason="No tool calls provided")

        if len(calls) == 1:
            return ExecutionPlan(
                stages=[ExecutionStage(calls=calls, execution_type=ExecutionType.SEQUENTIAL)],
                strategy_reason="Single tool call"
            )

        # Keywords for detecting patterns
        sequential_keywords = ["analyze", "process", "summarize", "review", "validate", "check"]
        compound_keywords = ["chain", "pipe", "feed", "cascade", "transform", "map"]

        tool_names = [c.tool_name.lower() for c in calls]

        has_compound = any(kw in name for name in tool_names for kw in compound_keywords)
        has_sequential = any(kw in name for name in tool_names for kw in sequential_keywords)

        if has_compound:
            # Compound: each tool depends on previous
            return ExecutionPlan(
                stages=[
                    ExecutionStage(
                        calls=[call],
                        execution_type=ExecutionType.COMPOUND,
                        depends_on=[i - 1] if i > 0 else []
                    )
                    for i, call in enumerate(calls)
                ],
                strategy_reason=f"Detected compound pattern - creating {len(calls)} dependent stages"
            )
        elif has_sequential or len(calls) > 5:
            return ExecutionPlan(
                stages=[ExecutionStage(calls=calls, execution_type=ExecutionType.SEQUENTIAL)],
                strategy_reason="Sequential execution for dependent or many calls"
            )
        else:
            return ExecutionPlan(
                stages=[ExecutionStage(calls=calls, execution_type=ExecutionType.PARALLEL)],
                strategy_reason=f"All {len(calls)} tools are independent - parallel execution"
            )

    # --- Session Management ---

    async def create_session(
        self,
        context: str,
        max_tools: int = 10,
        token_budget: int = 8000
    ) -> SessionInfo:
        """
        Create a session with dynamically loaded tools based on context.

        Uses semantic search to find relevant tools, respecting token budget.
        """
        # Generate session ID
        session_id = hashlib.sha256(
            f"{context}{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:12]

        # Search for relevant tools
        query = ToolSearchQuery(query=context, limit=max_tools, use_semantic=True)
        results = await self.search_tools(query)

        # Select tools within token budget
        active_tools = []
        token_budget_used = 0

        for result in results:
            # Estimate tokens (rough: 1 token per 4 chars)
            tool_tokens = len(result.description) // 4
            tool_tokens += sum(len(k) + 10 for k in result.input_schema.get("properties", {}).keys())

            if token_budget_used + tool_tokens <= token_budget:
                active_tools.append(result.tool_id)
                token_budget_used += tool_tokens

        # Store session
        self._sessions[session_id] = {
            "active_tools": active_tools,
            "token_budget_used": token_budget_used,
            "created_at": datetime.utcnow().isoformat(),
            "last_query": context
        }

        return SessionInfo(
            session_id=session_id,
            active_tools=active_tools,
            token_budget_used=token_budget_used,
            created_at=datetime.utcnow(),
            last_query=context
        )

    def get_session(self, session_id: str) -> Optional[SessionInfo]:
        """Get session info."""
        session = self._sessions.get(session_id)
        if not session:
            return None

        return SessionInfo(
            session_id=session_id,
            active_tools=session.get("active_tools", []),
            token_budget_used=session.get("token_budget_used", 0),
            created_at=datetime.fromisoformat(session.get("created_at", datetime.utcnow().isoformat())),
            last_query=session.get("last_query")
        )

    def close_session(self, session_id: str) -> bool:
        """Close a session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False

    # --- Stats ---

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        total_tools = sum(len(s.tools) for s in self._servers.values())

        return {
            "total_servers": len(self._servers),
            "total_tools": total_tools,
            "total_secrets": len(self._secrets),
            "total_profiles": len(self._profiles),
            "active_sessions": len(self._sessions),
            "tools_with_embeddings": len(self._tool_embeddings),
            "servers": [
                {
                    "name": s.name,
                    "tools": len(s.tools),
                    "healthy": s.healthy,
                    "tags": s.tags
                }
                for s in self._servers.values()
            ]
        }


# ============================================================================
# Global Registry Instance
# ============================================================================

_registry: Optional[MCPRegistry] = None


def get_registry() -> MCPRegistry:
    """Get the global registry instance."""
    global _registry
    if _registry is None:
        _registry = MCPRegistry()
    return _registry
