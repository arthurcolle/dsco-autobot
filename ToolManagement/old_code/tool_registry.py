"""
Tool Registry - Neo4j-backed tool management service.

Stores tools, schemas, relationships, and embeddings for semantic search.
"""
from typing import Callable, Type, Dict, Any, List, Optional, AsyncIterator, TypeVar
from pydantic import BaseModel, Field
from functools import wraps
import inspect
import asyncio
import httpx
import logging
import uuid
import json
from datetime import datetime, timezone
from enum import Enum
from contextlib import asynccontextmanager

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import ServiceUnavailable, AuthError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# Models
# =============================================================================

class ToolType(str, Enum):
    ENDPOINT = "endpoint"
    FUNCTION = "function"
    SNIPPET = "snippet"
    TASK = "task"
    STREAM = "stream"


class ActionType(str, Enum):
    HTTP = "http"
    FUNCTION = "function"
    CODE = "code"
    TASK = "task"
    STREAM = "stream"


class Tool(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str = ""
    type: ToolType = ToolType.FUNCTION
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    action: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    version: str = "1.0.0"
    enabled: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    embedding: Optional[List[float]] = None

    class Config:
        use_enum_values = True


class ToolExecution(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tool_id: str
    tool_name: str
    input_data: Dict[str, Any]
    output_data: Optional[Dict[str, Any]] = None
    status: str = "pending"
    error: Optional[str] = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None


class Service(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    url: str
    description: str = ""
    health_endpoint: str = "/health"
    enabled: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# =============================================================================
# Neo4j Repository
# =============================================================================

class Neo4jToolRepository:
    """Neo4j-backed repository for tool storage and retrieval."""

    def __init__(
        self,
        uri: str = "neo4j://127.0.0.1:7687",
        username: str = "neo4j",
        password: str = "password",
        database: str = "neo4j"
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self._driver: Optional[AsyncDriver] = None

    async def connect(self) -> None:
        """Establish connection to Neo4j."""
        try:
            self._driver = AsyncGraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password)
            )
            # Verify connectivity
            async with self._driver.session(database=self.database) as session:
                await session.run("RETURN 1")
            logger.info(f"Connected to Neo4j at {self.uri}")
        except (ServiceUnavailable, AuthError) as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

    async def close(self) -> None:
        """Close the Neo4j connection."""
        if self._driver:
            await self._driver.close()
            logger.info("Closed Neo4j connection")

    async def init_schema(self) -> None:
        """Initialize Neo4j schema with constraints and indexes."""
        async with self._driver.session(database=self.database) as session:
            # Constraints
            await session.run("""
                CREATE CONSTRAINT tool_id IF NOT EXISTS
                FOR (t:Tool) REQUIRE t.id IS UNIQUE
            """)
            await session.run("""
                CREATE CONSTRAINT tool_name IF NOT EXISTS
                FOR (t:Tool) REQUIRE t.name IS UNIQUE
            """)
            await session.run("""
                CREATE CONSTRAINT service_id IF NOT EXISTS
                FOR (s:Service) REQUIRE s.id IS UNIQUE
            """)
            await session.run("""
                CREATE CONSTRAINT execution_id IF NOT EXISTS
                FOR (e:Execution) REQUIRE e.id IS UNIQUE
            """)

            # Indexes for search
            await session.run("""
                CREATE INDEX tool_type IF NOT EXISTS
                FOR (t:Tool) ON (t.type)
            """)
            await session.run("""
                CREATE INDEX tool_enabled IF NOT EXISTS
                FOR (t:Tool) ON (t.enabled)
            """)

            # Full-text index for search
            try:
                await session.run("""
                    CREATE FULLTEXT INDEX tool_search IF NOT EXISTS
                    FOR (t:Tool) ON EACH [t.name, t.description]
                """)
            except Exception:
                pass  # Index may already exist

            # Vector index for embeddings (Neo4j 5.x+)
            try:
                await session.run("""
                    CREATE VECTOR INDEX tool_embedding IF NOT EXISTS
                    FOR (t:Tool) ON (t.embedding)
                    OPTIONS {indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'cosine'
                    }}
                """)
            except Exception as e:
                logger.warning(f"Could not create vector index (may need Neo4j 5.11+): {e}")

            logger.info("Neo4j schema initialized")

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Get a Neo4j session."""
        async with self._driver.session(database=self.database) as session:
            yield session

    # -------------------------------------------------------------------------
    # Tool CRUD
    # -------------------------------------------------------------------------

    async def create_tool(self, tool: Tool) -> Tool:
        """Create a new tool in Neo4j."""
        async with self.session() as session:
            result = await session.run("""
                CREATE (t:Tool {
                    id: $id,
                    name: $name,
                    description: $description,
                    type: $type,
                    input_schema: $input_schema,
                    output_schema: $output_schema,
                    action: $action,
                    metadata: $metadata,
                    tags: $tags,
                    version: $version,
                    enabled: $enabled,
                    created_at: datetime($created_at),
                    updated_at: datetime($updated_at)
                })
                RETURN t
            """, {
                "id": tool.id,
                "name": tool.name,
                "description": tool.description,
                "type": tool.type,
                "input_schema": json.dumps(tool.input_schema),
                "output_schema": json.dumps(tool.output_schema),
                "action": json.dumps(tool.action),
                "metadata": json.dumps(tool.metadata),
                "tags": tool.tags,
                "version": tool.version,
                "enabled": tool.enabled,
                "created_at": tool.created_at.isoformat(),
                "updated_at": tool.updated_at.isoformat()
            })
            record = await result.single()
            logger.info(f"Created tool: {tool.name}")
            return tool

    async def get_tool(self, tool_id: str) -> Optional[Tool]:
        """Get a tool by ID."""
        async with self.session() as session:
            result = await session.run("""
                MATCH (t:Tool {id: $id})
                RETURN t
            """, {"id": tool_id})
            record = await result.single()
            if record:
                return self._node_to_tool(record["t"])
            return None

    async def get_tool_by_name(self, name: str) -> Optional[Tool]:
        """Get a tool by name."""
        async with self.session() as session:
            result = await session.run("""
                MATCH (t:Tool {name: $name})
                RETURN t
            """, {"name": name})
            record = await result.single()
            if record:
                return self._node_to_tool(record["t"])
            return None

    async def list_tools(
        self,
        tool_type: Optional[ToolType] = None,
        enabled_only: bool = True,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Tool]:
        """List tools with optional filters."""
        query_parts = ["MATCH (t:Tool)"]
        params: Dict[str, Any] = {"limit": limit, "offset": offset}

        where_clauses = []
        if enabled_only:
            where_clauses.append("t.enabled = true")
        if tool_type:
            where_clauses.append("t.type = $type")
            params["type"] = tool_type.value if isinstance(tool_type, ToolType) else tool_type
        if tags:
            where_clauses.append("any(tag IN $tags WHERE tag IN t.tags)")
            params["tags"] = tags

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        query_parts.append("RETURN t ORDER BY t.name SKIP $offset LIMIT $limit")

        async with self.session() as session:
            result = await session.run(" ".join(query_parts), params)
            records = await result.data()
            return [self._node_to_tool(r["t"]) for r in records]

    async def update_tool(self, tool_id: str, updates: Dict[str, Any]) -> Optional[Tool]:
        """Update a tool."""
        updates["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Serialize JSON fields
        for key in ["input_schema", "output_schema", "action", "metadata"]:
            if key in updates and isinstance(updates[key], dict):
                updates[key] = json.dumps(updates[key])

        set_clauses = ", ".join([f"t.{k} = ${k}" for k in updates.keys()])

        async with self.session() as session:
            result = await session.run(f"""
                MATCH (t:Tool {{id: $id}})
                SET {set_clauses}
                RETURN t
            """, {"id": tool_id, **updates})
            record = await result.single()
            if record:
                return self._node_to_tool(record["t"])
            return None

    async def delete_tool(self, tool_id: str) -> bool:
        """Delete a tool."""
        async with self.session() as session:
            result = await session.run("""
                MATCH (t:Tool {id: $id})
                DETACH DELETE t
                RETURN count(t) as deleted
            """, {"id": tool_id})
            record = await result.single()
            return record["deleted"] > 0

    async def upsert_tool(self, tool: Tool) -> Tool:
        """Create or update a tool based on name."""
        existing = await self.get_tool_by_name(tool.name)
        if existing:
            tool.id = existing.id
            tool.created_at = existing.created_at
            return await self.update_tool(existing.id, tool.model_dump(exclude={"id", "created_at"}))
        return await self.create_tool(tool)

    # -------------------------------------------------------------------------
    # Search
    # -------------------------------------------------------------------------

    async def search_tools(self, query: str, limit: int = 10) -> List[Tool]:
        """Full-text search for tools."""
        async with self.session() as session:
            result = await session.run("""
                CALL db.index.fulltext.queryNodes('tool_search', $query)
                YIELD node, score
                WHERE node:Tool AND node.enabled = true
                RETURN node, score
                ORDER BY score DESC
                LIMIT $limit
            """, {"query": query, "limit": limit})
            records = await result.data()
            return [self._node_to_tool(r["node"]) for r in records]

    async def vector_search(
        self,
        embedding: List[float],
        limit: int = 10,
        min_score: float = 0.7
    ) -> List[tuple[Tool, float]]:
        """Vector similarity search for tools."""
        async with self.session() as session:
            result = await session.run("""
                CALL db.index.vector.queryNodes('tool_embedding', $limit, $embedding)
                YIELD node, score
                WHERE score >= $min_score AND node.enabled = true
                RETURN node, score
                ORDER BY score DESC
            """, {"embedding": embedding, "limit": limit, "min_score": min_score})
            records = await result.data()
            return [(self._node_to_tool(r["node"]), r["score"]) for r in records]

    async def set_tool_embedding(self, tool_id: str, embedding: List[float]) -> None:
        """Set the embedding vector for a tool."""
        async with self.session() as session:
            await session.run("""
                MATCH (t:Tool {id: $id})
                SET t.embedding = $embedding
            """, {"id": tool_id, "embedding": embedding})

    # -------------------------------------------------------------------------
    # Relationships
    # -------------------------------------------------------------------------

    async def link_tool_to_service(self, tool_id: str, service_id: str) -> None:
        """Create relationship between tool and service."""
        async with self.session() as session:
            await session.run("""
                MATCH (t:Tool {id: $tool_id})
                MATCH (s:Service {id: $service_id})
                MERGE (t)-[:PROVIDED_BY]->(s)
            """, {"tool_id": tool_id, "service_id": service_id})

    async def link_tools(self, from_tool_id: str, to_tool_id: str, relationship: str = "RELATED_TO") -> None:
        """Create relationship between tools."""
        async with self.session() as session:
            await session.run(f"""
                MATCH (t1:Tool {{id: $from_id}})
                MATCH (t2:Tool {{id: $to_id}})
                MERGE (t1)-[:{relationship}]->(t2)
            """, {"from_id": from_tool_id, "to_id": to_tool_id})

    async def get_related_tools(self, tool_id: str, relationship: Optional[str] = None) -> List[Tool]:
        """Get tools related to a given tool."""
        rel_pattern = f"[:{relationship}]" if relationship else "[r]"
        async with self.session() as session:
            result = await session.run(f"""
                MATCH (t:Tool {{id: $id}})-{rel_pattern}-(related:Tool)
                WHERE related.enabled = true
                RETURN DISTINCT related
            """, {"id": tool_id})
            records = await result.data()
            return [self._node_to_tool(r["related"]) for r in records]

    # -------------------------------------------------------------------------
    # Services
    # -------------------------------------------------------------------------

    async def create_service(self, service: Service) -> Service:
        """Create a service."""
        async with self.session() as session:
            await session.run("""
                CREATE (s:Service {
                    id: $id,
                    name: $name,
                    url: $url,
                    description: $description,
                    health_endpoint: $health_endpoint,
                    enabled: $enabled,
                    created_at: datetime($created_at)
                })
            """, {
                "id": service.id,
                "name": service.name,
                "url": service.url,
                "description": service.description,
                "health_endpoint": service.health_endpoint,
                "enabled": service.enabled,
                "created_at": service.created_at.isoformat()
            })
            return service

    async def get_service_tools(self, service_id: str) -> List[Tool]:
        """Get all tools provided by a service."""
        async with self.session() as session:
            result = await session.run("""
                MATCH (t:Tool)-[:PROVIDED_BY]->(s:Service {id: $id})
                WHERE t.enabled = true
                RETURN t
            """, {"id": service_id})
            records = await result.data()
            return [self._node_to_tool(r["t"]) for r in records]

    # -------------------------------------------------------------------------
    # Executions
    # -------------------------------------------------------------------------

    async def log_execution(self, execution: ToolExecution) -> ToolExecution:
        """Log a tool execution."""
        async with self.session() as session:
            await session.run("""
                MATCH (t:Tool {id: $tool_id})
                CREATE (e:Execution {
                    id: $id,
                    tool_id: $tool_id,
                    tool_name: $tool_name,
                    input_data: $input_data,
                    output_data: $output_data,
                    status: $status,
                    error: $error,
                    started_at: datetime($started_at),
                    completed_at: $completed_at,
                    duration_ms: $duration_ms
                })
                CREATE (e)-[:EXECUTED]->(t)
            """, {
                "id": execution.id,
                "tool_id": execution.tool_id,
                "tool_name": execution.tool_name,
                "input_data": json.dumps(execution.input_data),
                "output_data": json.dumps(execution.output_data) if execution.output_data else None,
                "status": execution.status,
                "error": execution.error,
                "started_at": execution.started_at.isoformat(),
                "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
                "duration_ms": execution.duration_ms
            })
            return execution

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _node_to_tool(self, node: Dict[str, Any]) -> Tool:
        """Convert a Neo4j node to a Tool model."""
        data = dict(node)
        # Parse JSON fields
        for key in ["input_schema", "output_schema", "action", "metadata"]:
            if key in data and isinstance(data[key], str):
                data[key] = json.loads(data[key])
        # Handle datetime
        for key in ["created_at", "updated_at"]:
            if key in data and data[key]:
                if hasattr(data[key], 'to_native'):
                    data[key] = data[key].to_native()
        return Tool(**data)


# =============================================================================
# Embedding Service
# =============================================================================

class EmbeddingService:
    """Service for generating embeddings using OpenAI or local models."""

    def __init__(self, api_key: Optional[str] = None, model: str = "text-embedding-3-small"):
        self.api_key = api_key
        self.model = model
        self._client: Optional[httpx.AsyncClient] = None

    async def get_embedding(self, text: str) -> List[float]:
        """Get embedding for text using OpenAI API."""
        if not self.api_key:
            raise ValueError("OpenAI API key required for embeddings")

        if not self._client:
            self._client = httpx.AsyncClient()

        response = await self._client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"input": text, "model": self.model}
        )
        response.raise_for_status()
        return response.json()["data"][0]["embedding"]

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()


# =============================================================================
# Tool Registry (Main Class)
# =============================================================================

class ToolRegistry:
    """
    Neo4j-backed tool registry with decorator support.

    Usage:
        registry = ToolRegistry(
            neo4j_uri="neo4j://127.0.0.1:7687",
            neo4j_password="your-password",
            service_url="http://localhost:8000"
        )

        @registry.function(description="Add two numbers")
        def add(input: AddInput) -> AddOutput:
            return AddOutput(result=input.a + input.b)

        await registry.start()
    """

    def __init__(
        self,
        neo4j_uri: str = "neo4j://127.0.0.1:7687",
        neo4j_username: str = "neo4j",
        neo4j_password: str = "password",
        neo4j_database: str = "neo4j",
        service_name: str = "default",
        service_url: str = "http://localhost:8000",
        openai_api_key: Optional[str] = None
    ):
        self.repo = Neo4jToolRepository(
            uri=neo4j_uri,
            username=neo4j_username,
            password=neo4j_password,
            database=neo4j_database
        )
        self.embedding_service = EmbeddingService(api_key=openai_api_key) if openai_api_key else None
        self.service_name = service_name
        self.service_url = service_url.rstrip("/")
        self._pending_tools: List[Tool] = []
        self._service: Optional[Service] = None

    async def start(self) -> None:
        """Initialize connection and register pending tools."""
        await self.repo.connect()
        await self.repo.init_schema()

        # Create/update service
        self._service = Service(
            name=self.service_name,
            url=self.service_url,
            description=f"Service: {self.service_name}"
        )
        await self.repo.create_service(self._service)

        # Register pending tools
        for tool in self._pending_tools:
            registered = await self.repo.upsert_tool(tool)
            await self.repo.link_tool_to_service(registered.id, self._service.id)

            # Generate embedding if service available
            if self.embedding_service and not tool.embedding:
                try:
                    text = f"{tool.name}: {tool.description}"
                    embedding = await self.embedding_service.get_embedding(text)
                    await self.repo.set_tool_embedding(registered.id, embedding)
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for {tool.name}: {e}")

        logger.info(f"Registered {len(self._pending_tools)} tools")
        self._pending_tools.clear()

    async def stop(self) -> None:
        """Close connections."""
        await self.repo.close()
        if self.embedding_service:
            await self.embedding_service.close()

    # -------------------------------------------------------------------------
    # Decorators
    # -------------------------------------------------------------------------

    def endpoint(self, description: str = "", tags: Optional[List[str]] = None):
        """Decorator for FastAPI endpoints."""
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            http_method = self._infer_http_method(func)
            route_path = self._infer_route_path(func) or f"/{func.__name__}"

            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            response_model = sig.return_annotation

            tool = Tool(
                name=func.__name__,
                description=(description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                type=ToolType.ENDPOINT,
                input_schema=input_model.model_json_schema() if input_model else {},
                output_schema=response_model.model_json_schema() if response_model and isinstance(response_model, type) and issubclass(response_model, BaseModel) else {},
                action={
                    "type": "http",
                    "http": {
                        "method": http_method or "POST",
                        "url": f"{self.service_url}{route_path}",
                        "headers": {"Content-Type": "application/json"}
                    }
                },
                tags=tags or [],
                metadata={"framework": "fastapi"}
            )

            self._pending_tools.append(tool)

            @wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            wrapper.tool = tool  # type: ignore
            return wrapper
        return decorator

    def function(self, description: str = "", name: Optional[str] = None, tags: Optional[List[str]] = None):
        """Decorator for standalone functions."""
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            output_model = sig.return_annotation
            func_name = name or func.__name__

            tool = Tool(
                name=func_name,
                description=(description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                type=ToolType.FUNCTION,
                input_schema=input_model.model_json_schema() if input_model else {},
                output_schema=output_model.model_json_schema() if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {},
                action={
                    "type": "function",
                    "function": {
                        "module": func.__module__,
                        "name": func.__name__,
                        "is_async": asyncio.iscoroutinefunction(func)
                    }
                },
                tags=tags or []
            )

            self._pending_tools.append(tool)

            @wraps(func)
            async def wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                return func(*args, **kwargs)

            wrapper.tool = tool  # type: ignore
            wrapper._original_func = func  # type: ignore
            return wrapper
        return decorator

    def snippet(
        self,
        language: str,
        description: str = "",
        name: Optional[str] = None,
        runtime: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        tags: Optional[List[str]] = None
    ):
        """Decorator for code snippets."""
        def decorator(func: Callable):
            code = inspect.getdoc(func) or ""
            snippet_name = name or func.__name__
            inferred_runtime = runtime or self._infer_runtime(language)

            tool = Tool(
                name=snippet_name,
                description=description or f"{language} snippet: {snippet_name}",
                type=ToolType.SNIPPET,
                input_schema={},
                output_schema={"type": "string"},
                action={
                    "type": "code",
                    "code": {
                        "language": language.lower(),
                        "runtime": inferred_runtime,
                        "source": code.strip(),
                        "dependencies": dependencies or []
                    }
                },
                tags=tags or [language.lower()],
                metadata={"language": language, "runtime": inferred_runtime}
            )

            self._pending_tools.append(tool)

            @wraps(func)
            def wrapper(*args, **kwargs):
                return code.strip()

            wrapper.tool = tool  # type: ignore
            wrapper.source_code = code.strip()  # type: ignore
            return wrapper
        return decorator

    def task(
        self,
        description: str = "",
        name: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: int = 0,
        tags: Optional[List[str]] = None
    ):
        """Decorator for long-running tasks."""
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            output_model = sig.return_annotation
            is_async_gen = inspect.isasyncgenfunction(func)
            is_gen = inspect.isgeneratorfunction(func)
            func_name = name or func.__name__

            tool = Tool(
                name=func_name,
                description=(description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                type=ToolType.TASK,
                input_schema=input_model.model_json_schema() if input_model else {},
                output_schema=output_model.model_json_schema() if output_model and isinstance(output_model, type) and issubclass(output_model, BaseModel) else {},
                action={
                    "type": "task",
                    "task": {
                        "module": func.__module__,
                        "name": func.__name__,
                        "is_async": asyncio.iscoroutinefunction(func) or is_async_gen,
                        "is_generator": is_async_gen or is_gen,
                        "timeout": timeout,
                        "retries": retries
                    }
                },
                tags=tags or ["task"],
                metadata={"timeout": timeout, "retries": retries, "streaming": is_async_gen or is_gen}
            )

            self._pending_tools.append(tool)

            if is_async_gen or is_gen:
                @wraps(func)
                async def gen_wrapper(*args, **kwargs):
                    if is_async_gen:
                        async for item in func(*args, **kwargs):
                            yield item
                    else:
                        for item in func(*args, **kwargs):
                            yield item
                gen_wrapper.tool = tool  # type: ignore
                return gen_wrapper
            else:
                @wraps(func)
                async def wrapper(*args, **kwargs):
                    if asyncio.iscoroutinefunction(func):
                        return await func(*args, **kwargs)
                    return func(*args, **kwargs)
                wrapper.tool = tool  # type: ignore
                return wrapper
        return decorator

    def stream(
        self,
        description: str = "",
        name: Optional[str] = None,
        chunk_type: str = "text",
        tags: Optional[List[str]] = None
    ):
        """Decorator for streaming responses."""
        def decorator(func: Callable):
            sig = inspect.signature(func)
            params = list(sig.parameters.values())

            input_model = None
            for param in params:
                ann = param.annotation
                if ann != inspect.Parameter.empty and isinstance(ann, type) and issubclass(ann, BaseModel):
                    input_model = ann
                    break

            func_name = name or func.__name__
            is_async_gen = inspect.isasyncgenfunction(func)

            content_type_map = {
                "sse": "text/event-stream",
                "json": "application/x-ndjson",
                "text": "text/plain"
            }

            tool = Tool(
                name=func_name,
                description=(description + "\n\n" + (func.__doc__ or "").strip()).strip(),
                type=ToolType.STREAM,
                input_schema=input_model.model_json_schema() if input_model else {},
                output_schema={"type": "string", "format": "stream"},
                action={
                    "type": "stream",
                    "stream": {
                        "module": func.__module__,
                        "name": func.__name__,
                        "chunk_type": chunk_type,
                        "is_async": is_async_gen
                    }
                },
                tags=tags or ["stream"],
                metadata={"chunk_type": chunk_type, "content_type": content_type_map.get(chunk_type, "text/plain")}
            )

            self._pending_tools.append(tool)

            @wraps(func)
            async def wrapper(*args, **kwargs):
                if is_async_gen:
                    async for item in func(*args, **kwargs):
                        yield item
                else:
                    for item in func(*args, **kwargs):
                        yield item

            wrapper.tool = tool  # type: ignore
            return wrapper
        return decorator

    # -------------------------------------------------------------------------
    # API Methods
    # -------------------------------------------------------------------------

    async def get_tools(self, **filters) -> List[Tool]:
        """Get all registered tools."""
        return await self.repo.list_tools(**filters)

    async def get_tool(self, tool_id: str) -> Optional[Tool]:
        """Get a tool by ID."""
        return await self.repo.get_tool(tool_id)

    async def get_tool_by_name(self, name: str) -> Optional[Tool]:
        """Get a tool by name."""
        return await self.repo.get_tool_by_name(name)

    async def search(self, query: str, limit: int = 10) -> List[Tool]:
        """Search tools by text."""
        return await self.repo.search_tools(query, limit)

    async def semantic_search(self, query: str, limit: int = 10) -> List[tuple[Tool, float]]:
        """Search tools by semantic similarity."""
        if not self.embedding_service:
            raise ValueError("Embedding service required for semantic search")
        embedding = await self.embedding_service.get_embedding(query)
        return await self.repo.vector_search(embedding, limit)

    async def execute(self, tool_name: str, input_data: Dict[str, Any]) -> Any:
        """Execute a tool by name."""
        tool = await self.repo.get_tool_by_name(tool_name)
        if not tool:
            raise ValueError(f"Tool not found: {tool_name}")

        execution = ToolExecution(
            tool_id=tool.id,
            tool_name=tool.name,
            input_data=input_data
        )

        start_time = datetime.now(timezone.utc)
        try:
            # Execute based on action type
            action = tool.action
            if action["type"] == "http":
                async with httpx.AsyncClient() as client:
                    response = await client.request(
                        method=action["http"]["method"],
                        url=action["http"]["url"],
                        json=input_data,
                        headers=action["http"].get("headers", {})
                    )
                    response.raise_for_status()
                    result = response.json()
            else:
                raise NotImplementedError(f"Execution type not implemented: {action['type']}")

            execution.output_data = result
            execution.status = "completed"
            execution.completed_at = datetime.now(timezone.utc)
            execution.duration_ms = int((execution.completed_at - start_time).total_seconds() * 1000)

        except Exception as e:
            execution.status = "failed"
            execution.error = str(e)
            execution.completed_at = datetime.now(timezone.utc)
            execution.duration_ms = int((execution.completed_at - start_time).total_seconds() * 1000)
            raise
        finally:
            await self.repo.log_execution(execution)

        return result

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _infer_http_method(self, func: Callable) -> Optional[str]:
        name = func.__name__.lower()
        if name.startswith(('get_', 'list_', 'fetch_', 'retrieve_')):
            return 'GET'
        elif name.startswith(('create_', 'add_', 'post_')):
            return 'POST'
        elif name.startswith(('update_', 'modify_', 'put_')):
            return 'PUT'
        elif name.startswith(('delete_', 'remove_')):
            return 'DELETE'
        elif name.startswith(('patch_', 'partial_')):
            return 'PATCH'
        return None

    def _infer_route_path(self, func: Callable) -> Optional[str]:
        if hasattr(func, '_path'):
            return func._path  # type: ignore
        if hasattr(func, 'path'):
            return func.path  # type: ignore
        return None

    def _infer_runtime(self, language: str) -> str:
        runtimes = {
            'javascript': 'node', 'js': 'node', 'typescript': 'node', 'ts': 'node',
            'python': 'python3', 'py': 'python3',
            'ruby': 'ruby', 'rb': 'ruby',
            'go': 'go', 'golang': 'go',
            'rust': 'cargo', 'rs': 'cargo',
            'sql': 'postgres', 'mysql': 'mysql', 'postgres': 'postgres',
            'bash': 'bash', 'sh': 'sh', 'shell': 'bash',
        }
        return runtimes.get(language.lower(), language.lower())


# =============================================================================
# FastAPI Integration
# =============================================================================

def create_registry_app(registry: ToolRegistry) -> "FastAPI":
    """Create a FastAPI app with registry endpoints."""
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import StreamingResponse

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await registry.start()
        yield
        await registry.stop()

    app = FastAPI(title="Tool Registry", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "healthy", "service": registry.service_name}

    @app.get("/tools", response_model=List[Tool])
    async def list_tools(
        type: Optional[str] = None,
        tags: Optional[str] = None,
        limit: int = 100
    ):
        filters = {"limit": limit}
        if type:
            filters["tool_type"] = type
        if tags:
            filters["tags"] = tags.split(",")
        return await registry.get_tools(**filters)

    @app.get("/tools/{tool_id}", response_model=Tool)
    async def get_tool(tool_id: str):
        tool = await registry.get_tool(tool_id)
        if not tool:
            raise HTTPException(404, f"Tool not found: {tool_id}")
        return tool

    @app.get("/tools/name/{name}", response_model=Tool)
    async def get_tool_by_name(name: str):
        tool = await registry.get_tool_by_name(name)
        if not tool:
            raise HTTPException(404, f"Tool not found: {name}")
        return tool

    @app.get("/search")
    async def search_tools(q: str, limit: int = 10):
        return await registry.search(q, limit)

    @app.post("/execute/{tool_name}")
    async def execute_tool(tool_name: str, input_data: Dict[str, Any]):
        try:
            return await registry.execute(tool_name, input_data)
        except ValueError as e:
            raise HTTPException(404, str(e))
        except Exception as e:
            raise HTTPException(500, str(e))

    return app
