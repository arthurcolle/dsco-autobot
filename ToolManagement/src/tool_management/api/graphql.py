"""
GraphQL Schema for MCP Registry

Single endpoint that exposes:
- Tool discovery with semantic search
- Server management
- Secret management (write-only values)
- Session management for dynamic tool loading
- Universal tool invocation

The agent only needs to know about ONE tool: the GraphQL endpoint.
Everything else is discoverable via introspection.
"""

import strawberry
from strawberry.types import Info
from typing import Optional, List, Any
from datetime import datetime
import json
import asyncio

# ============================================================================
# Core Types
# ============================================================================

@strawberry.type
class ToolParameter:
    """A parameter in a tool's input schema"""
    name: str
    type: str
    description: Optional[str] = None
    required: bool = False
    default: Optional[str] = None
    enum_values: Optional[List[str]] = None


@strawberry.type
class ToolSchema:
    """Complete schema for a tool"""
    tool_id: str  # server::tool_name
    server_name: str
    name: str
    description: str
    input_parameters: List[ToolParameter]
    output_description: Optional[str] = None
    tags: List[str] = strawberry.field(default_factory=list)
    relevance_score: Optional[float] = None


@strawberry.type
class ServerInfo:
    """MCP Server information"""
    name: str
    url: str
    transport: str
    description: Optional[str] = None
    healthy: bool = True
    tool_count: int = 0
    tags: List[str] = strawberry.field(default_factory=list)
    last_sync: Optional[str] = None


@strawberry.type
class SecretInfo:
    """Secret metadata (never exposes value)"""
    key: str
    description: Optional[str] = None
    linked_servers: List[str] = strawberry.field(default_factory=list)


@strawberry.type
class SessionInfo:
    """Active session with loaded tools"""
    session_id: str
    active_tools: List[str]
    token_budget_used: int
    created_at: str
    last_query: Optional[str] = None


@strawberry.type
class ToolInvocationResult:
    """Result from invoking a tool"""
    tool_id: str
    success: bool
    result: Optional[str] = None  # JSON-encoded result
    error: Optional[str] = None
    duration_ms: int = 0


@strawberry.type
class SearchResult:
    """Tool search result with relevance"""
    tool: ToolSchema
    score: float
    match_reason: str


@strawberry.type
class RegistryStats:
    """Overall registry statistics"""
    total_servers: int
    total_tools: int
    total_secrets: int
    active_sessions: int
    tools_with_embeddings: int


# ============================================================================
# Input Types
# ============================================================================

@strawberry.input
class ServerInput:
    """Input for registering a server"""
    name: str
    url: str
    transport: str = "http"
    description: Optional[str] = None
    tags: List[str] = strawberry.field(default_factory=list)
    openapi_url: Optional[str] = None
    defer_loading: bool = False


@strawberry.input
class SecretInput:
    """Input for setting a secret"""
    key: str
    value: str
    description: Optional[str] = None
    linked_servers: List[str] = strawberry.field(default_factory=list)


@strawberry.input
class ToolSearchInput:
    """Input for searching tools"""
    query: str
    limit: int = 10
    server_filter: Optional[List[str]] = None
    tag_filter: Optional[List[str]] = None
    use_semantic: bool = True


@strawberry.input
class SessionInput:
    """Input for creating/updating a session"""
    context: str
    max_tools: int = 10
    token_budget: int = 8000


@strawberry.input
class InvokeToolInput:
    """Input for invoking a tool"""
    tool_id: str
    parameters: str  # JSON-encoded parameters
    session_id: Optional[str] = None


# ============================================================================
# Query Resolvers
# ============================================================================

@strawberry.type
class Query:
    """
    GraphQL Query operations for MCP Registry.

    Use introspection to discover all available queries:
    ```graphql
    { __schema { queryType { fields { name description } } } }
    ```
    """

    @strawberry.field(description="Get registry statistics")
    async def stats(self, info: Info) -> RegistryStats:
        registry = info.context["registry"]
        stats = registry.get_stats()

        return RegistryStats(
            total_servers=stats["total_servers"],
            total_tools=stats["total_tools"],
            total_secrets=stats["total_secrets"],
            active_sessions=stats["active_sessions"],
            tools_with_embeddings=stats["tools_with_embeddings"]
        )

    @strawberry.field(description="List all registered servers")
    async def servers(
        self,
        info: Info,
        healthy_only: bool = False
    ) -> List[ServerInfo]:
        registry = info.context["registry"]
        servers = registry.list_servers()

        if healthy_only:
            servers = [s for s in servers if s.healthy]

        return [
            ServerInfo(
                name=s.name,
                url=s.url,
                transport=s.transport,
                description=s.description,
                healthy=s.healthy,
                tool_count=len(s.tools),
                tags=s.tags,
                last_sync=s.last_health_check.isoformat() if s.last_health_check else None
            )
            for s in servers
        ]

    @strawberry.field(description="Get a specific server")
    async def server(self, info: Info, name: str) -> Optional[ServerInfo]:
        registry = info.context["registry"]
        s = registry.get_server(name)
        if not s:
            return None

        return ServerInfo(
            name=s.name,
            url=s.url,
            transport=s.transport,
            description=s.description,
            healthy=s.healthy,
            tool_count=len(s.tools),
            tags=s.tags,
            last_sync=s.last_health_check.isoformat() if s.last_health_check else None
        )

    @strawberry.field(description="List all tools across all servers")
    async def tools(
        self,
        info: Info,
        server: Optional[str] = None,
        limit: int = 100
    ) -> List[ToolSchema]:
        registry = info.context["registry"]

        if server:
            servers = [registry.get_server(server)] if registry.get_server(server) else []
        else:
            servers = registry.list_servers()

        results = []
        for srv in servers:
            if not srv:
                continue
            for tool in srv.tools[:limit]:
                tool_id = f"{srv.name}::{tool.get('name', '')}"

                # Parse input schema into parameters
                input_schema = tool.get("input_schema", {})
                properties = input_schema.get("properties", {})
                required = input_schema.get("required", [])

                params = []
                for param_name, param_def in properties.items():
                    params.append(ToolParameter(
                        name=param_name,
                        type=param_def.get("type", "string"),
                        description=param_def.get("description"),
                        required=param_name in required,
                        default=json.dumps(param_def.get("default")) if "default" in param_def else None,
                        enum_values=param_def.get("enum")
                    ))

                results.append(ToolSchema(
                    tool_id=tool_id,
                    server_name=srv.name,
                    name=tool.get("name", ""),
                    description=tool.get("description", ""),
                    input_parameters=params,
                    tags=srv.tags
                ))

        return results[:limit]

    @strawberry.field(description="Get a specific tool by ID (server::name)")
    async def tool(self, info: Info, tool_id: str) -> Optional[ToolSchema]:
        registry = info.context["registry"]

        if "::" not in tool_id:
            return None

        server_name, tool_name = tool_id.split("::", 1)
        server = registry.get_server(server_name)
        if not server:
            return None

        tool = next((t for t in server.tools if t.get("name") == tool_name), None)
        if not tool:
            return None

        input_schema = tool.get("input_schema", {})
        properties = input_schema.get("properties", {})
        required = input_schema.get("required", [])

        params = []
        for param_name, param_def in properties.items():
            params.append(ToolParameter(
                name=param_name,
                type=param_def.get("type", "string"),
                description=param_def.get("description"),
                required=param_name in required,
                default=json.dumps(param_def.get("default")) if "default" in param_def else None,
                enum_values=param_def.get("enum")
            ))

        return ToolSchema(
            tool_id=tool_id,
            server_name=server_name,
            name=tool.get("name", ""),
            description=tool.get("description", ""),
            input_parameters=params,
            tags=server.tags
        )

    @strawberry.field(description="Search tools using semantic similarity or keywords")
    async def search_tools(
        self,
        info: Info,
        input: ToolSearchInput
    ) -> List[SearchResult]:
        registry = info.context["registry"]
        from mcp_registry import ToolSearchQuery

        # Convert GraphQL input to registry query
        query = ToolSearchQuery(
            query=input.query,
            limit=input.limit,
            profile=None,  # Could extend to support profiles
            use_semantic=input.use_semantic
        )

        results = await registry.search_tools(query)

        search_results = []
        for r in results:
            # Get tool schema
            tool_id = r.tool_id
            server = registry.get_server(r.server_name)
            if not server:
                continue

            tool_data = next((t for t in server.tools if t.get("name") == r.tool_name), None)
            if not tool_data:
                continue

            input_schema = tool_data.get("input_schema", {})
            properties = input_schema.get("properties", {})
            required = input_schema.get("required", [])

            params = []
            for param_name, param_def in properties.items():
                params.append(ToolParameter(
                    name=param_name,
                    type=param_def.get("type", "string"),
                    description=param_def.get("description"),
                    required=param_name in required
                ))

            search_results.append(SearchResult(
                tool=ToolSchema(
                    tool_id=tool_id,
                    server_name=r.server_name,
                    name=r.tool_name,
                    description=r.description,
                    input_parameters=params,
                    relevance_score=r.score,
                    tags=server.tags
                ),
                score=r.score,
                match_reason=r.match_reason
            ))

        return search_results

    @strawberry.field(description="List secrets (values are never exposed)")
    async def secrets(self, info: Info) -> List[SecretInfo]:
        registry = info.context["registry"]
        secrets = registry.list_secrets()

        return [
            SecretInfo(
                key=s.key,
                description=s.description,
                linked_servers=s.linked_servers
            )
            for s in secrets
        ]

    @strawberry.field(description="Get active session info")
    async def session(self, info: Info, session_id: str) -> Optional[SessionInfo]:
        registry = info.context["registry"]
        session = registry.get_session(session_id)
        if not session:
            return None

        return SessionInfo(
            session_id=session_id,
            active_tools=session.active_tools,
            token_budget_used=session.token_budget_used,
            created_at=session.created_at.isoformat(),
            last_query=session.last_query
        )


# ============================================================================
# Mutation Resolvers
# ============================================================================

@strawberry.type
class Mutation:
    """
    GraphQL Mutation operations for MCP Registry.

    Use introspection to discover all available mutations:
    ```graphql
    { __schema { mutationType { fields { name description } } } }
    ```
    """

    @strawberry.mutation(description="Register a new MCP server")
    async def add_server(self, info: Info, input: ServerInput) -> ServerInfo:
        registry = info.context["registry"]
        from mcp_registry import MCPServerConfig

        config = MCPServerConfig(
            name=input.name,
            url=input.url,
            transport=input.transport,
            description=input.description,
            tags=input.tags,
            openapi_url=input.openapi_url,
            defer_loading=input.defer_loading
        )

        server = registry.register_server(config)

        return ServerInfo(
            name=server.name,
            url=server.url,
            transport=server.transport,
            description=server.description,
            healthy=server.healthy,
            tool_count=len(server.tools),
            tags=server.tags
        )

    @strawberry.mutation(description="Remove a server")
    async def remove_server(self, info: Info, name: str) -> bool:
        registry = info.context["registry"]
        return registry.unregister_server(name)

    @strawberry.mutation(description="Sync tools from a server")
    async def sync_server_tools(self, info: Info, name: str) -> ServerInfo:
        registry = info.context["registry"]

        result = await registry.sync_server_tools(name)
        if "error" in result:
            raise ValueError(result["error"])

        server = registry.get_server(name)
        if not server:
            raise ValueError(f"Server {name} not found")

        return ServerInfo(
            name=server.name,
            url=server.url,
            transport=server.transport,
            description=server.description,
            healthy=server.healthy,
            tool_count=len(server.tools),
            tags=server.tags
        )

    @strawberry.mutation(description="Set a secret (value is stored securely)")
    async def set_secret(self, info: Info, input: SecretInput) -> SecretInfo:
        registry = info.context["registry"]
        from mcp_registry import SecretConfig

        config = SecretConfig(
            key=input.key,
            value=input.value,
            description=input.description,
            linked_servers=input.linked_servers
        )

        secret = registry.set_secret(config)

        return SecretInfo(
            key=secret.key,
            description=secret.description,
            linked_servers=secret.linked_servers
        )

    @strawberry.mutation(description="Delete a secret")
    async def delete_secret(self, info: Info, key: str) -> bool:
        registry = info.context["registry"]
        return registry.delete_secret(key)

    @strawberry.mutation(description="Link a secret to servers")
    async def link_secret(
        self,
        info: Info,
        key: str,
        servers: List[str]
    ) -> SecretInfo:
        registry = info.context["registry"]
        from mcp_registry import SecretConfig

        secret = registry.get_secret(key)
        if not secret:
            raise ValueError(f"Secret {key} not found")

        # Update linked servers
        updated = SecretConfig(
            key=secret.key,
            value=secret.value,
            description=secret.description,
            linked_servers=servers
        )
        registry.set_secret(updated)

        return SecretInfo(
            key=updated.key,
            description=updated.description,
            linked_servers=updated.linked_servers
        )

    @strawberry.mutation(description="Create a session with dynamically loaded tools")
    async def create_session(
        self,
        info: Info,
        input: SessionInput
    ) -> SessionInfo:
        registry = info.context["registry"]

        session = await registry.create_session(
            context=input.context,
            max_tools=input.max_tools,
            token_budget=input.token_budget
        )

        return SessionInfo(
            session_id=session.session_id,
            active_tools=session.active_tools,
            token_budget_used=session.token_budget_used,
            created_at=session.created_at.isoformat(),
            last_query=session.last_query
        )

    @strawberry.mutation(description="Close a session")
    async def close_session(self, info: Info, session_id: str) -> bool:
        registry = info.context["registry"]
        return registry.close_session(session_id)

    @strawberry.mutation(description="Invoke a tool with parameters")
    async def invoke_tool(
        self,
        info: Info,
        input: InvokeToolInput
    ) -> ToolInvocationResult:
        registry = info.context["registry"]
        from mcp_registry import ToolCallRequest

        # Parse tool_id
        if "::" not in input.tool_id:
            return ToolInvocationResult(
                tool_id=input.tool_id,
                success=False,
                error="Invalid tool_id format. Expected server::tool_name"
            )

        server_name, tool_name = input.tool_id.split("::", 1)

        # Parse parameters
        try:
            params = json.loads(input.parameters)
        except json.JSONDecodeError as e:
            return ToolInvocationResult(
                tool_id=input.tool_id,
                success=False,
                error=f"Invalid JSON parameters: {e}"
            )

        # Execute
        request = ToolCallRequest(
            server_name=server_name,
            tool_name=tool_name,
            arguments=params
        )

        result = await registry.execute_tool(request)

        return ToolInvocationResult(
            tool_id=input.tool_id,
            success=result.success,
            result=json.dumps(result.result) if result.result else None,
            error=result.error,
            duration_ms=result.execution_ms
        )


# ============================================================================
# Schema Creation
# ============================================================================

schema = strawberry.Schema(query=Query, mutation=Mutation)


# ============================================================================
# Agent System Prompt
# ============================================================================

GRAPHQL_SYSTEM_PROMPT = """
You have access to a unified MCP Registry via a single GraphQL endpoint.

## Quick Start

1. **Discover tools**: Search for tools relevant to your task
   ```graphql
   query { searchTools(input: {query: "web search", limit: 5}) { tool { toolId name description } score } }
   ```

2. **Create a session**: Load relevant tools into a token-efficient session
   ```graphql
   mutation { createSession(input: {context: "I need to search the web and analyze results", maxTools: 10}) { sessionId activeTools tokenBudgetUsed } }
   ```

3. **Invoke tools**: Call tools with parameters
   ```graphql
   mutation { invokeTool(input: {toolId: "search-rag::web_search", parameters: "{\\"query\\": \\"AI news\\"}"}) { success result error } }
   ```

## Key Queries
- `stats` - Registry overview
- `servers` - List MCP servers
- `tools(server: "name")` - List tools
- `searchTools(input: {...})` - Semantic search for tools
- `session(sessionId: "...")` - Session info

## Key Mutations
- `addServer` / `removeServer` - Manage servers
- `syncServerTools(name: "...")` - Fetch tools from server
- `setSecret` / `deleteSecret` - Manage API keys
- `linkSecret(key: "...", servers: [...])` - Link secrets to servers
- `createSession` / `closeSession` - Dynamic tool loading
- `invokeTool` - Execute a tool

## Tips
- Use `searchTools` with semantic search to find relevant tools
- Create sessions to manage token budget automatically
- Tools are identified as `server::toolName`
- Secrets linked to servers are auto-injected on invocation
"""

GRAPHQL_RESOURCE = {
    "uri": "mcp://registry/graphql",
    "name": "MCP Registry GraphQL API",
    "description": "Unified API for tool discovery, management, and invocation",
    "mimeType": "application/graphql",
    "schema_url": "/graphql/schema"
}
