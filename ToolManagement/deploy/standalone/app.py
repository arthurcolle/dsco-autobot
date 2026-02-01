"""
Clean, production-ready FastAPI application for Tool Management Service.

- Health and readiness endpoints
- CORS support, configurable via env
- Tool management routes
- Structured logging
- Uvicorn entrypoint for local runs

Environment variables:
  - APP_NAME: Service title (default: Tool Management API)
  - APP_VERSION: Version string (default: 0.1.0)
  - ENV: Environment name (default: development)
  - DEBUG: Enable debug logs when set to "1"
  - CORS_ORIGINS: Comma-separated list or "*" (default: *)
  - HOST: Bind host for uvicorn (default: 0.0.0.0)
  - PORT: Bind port for uvicorn (default: 8000)
  - RELOAD: Enable uvicorn reload when set to "1"
"""

from __future__ import annotations

import logging
import os
import time
import httpx
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status, WebSocket
from fastapi.middleware.cors import CORSMiddleware


# ---------------------------
# Embedding Provider
# ---------------------------

class EmbeddingProvider:
    """Compute embeddings for tool semantic search."""

    def __init__(self):
        self.api_key = os.getenv("JINA_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.provider = "jina" if os.getenv("JINA_API_KEY") else "openai" if os.getenv("OPENAI_API_KEY") else None
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def embed(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text. Returns None if no provider configured."""
        if not self.provider or not self.api_key:
            return None

        try:
            client = await self._get_client()

            if self.provider == "jina":
                response = await client.post(
                    "https://api.jina.ai/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    json={"model": "jina-embeddings-v4", "input": [text], "normalized": True, "dimensions": 1024}
                )
                response.raise_for_status()
                return response.json()["data"][0]["embedding"]

            elif self.provider == "openai":
                response = await client.post(
                    "https://api.openai.com/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    json={"model": "text-embedding-3-small", "input": text}
                )
                response.raise_for_status()
                return response.json()["data"][0]["embedding"]

        except Exception as e:
            logging.error(f"Embedding failed: {e}")
            return None

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Global embedder instance
_embedder: Optional[EmbeddingProvider] = None

def get_embedder() -> EmbeddingProvider:
    global _embedder
    if _embedder is None:
        _embedder = EmbeddingProvider()
    return _embedder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Set


# ---------------------------
# Configuration and utilities
# ---------------------------


class Settings:
    """Lightweight settings loaded from environment.

    Avoids coupling to a specific Pydantic version.
    """

    def __init__(self) -> None:
        self.title: str = os.getenv("APP_NAME", "Tool Management API")
        self.version: str = os.getenv("APP_VERSION", "0.1.0")
        self.env: str = os.getenv("ENV", "development")
        self.debug: bool = os.getenv("DEBUG", "0") == "1"
        raw_origins = os.getenv("CORS_ORIGINS", "*")
        self.cors_origins: List[str] = (
            [o.strip() for o in raw_origins.split(",") if o.strip()]
            if raw_origins != "*"
            else ["*"]
        )


def get_settings() -> Settings:
    return Settings()


START_TIME = time.time()


def configure_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Tidy overly chatty loggers in prod
    if not debug:
        for noisy in ("uvicorn.error", "uvicorn.access", "asyncio", "httpx"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


# Import MCP and interconnectivity modules
from mcp_server import MCPServer, MCPWebSocketHandler
from interconnectivity import (
    InterconnectivityManager,
    CompositionType,
    EventType,
    Webhook,
    RegistryNode
)
from hateoas import HATEOASBuilder, ContentNegotiation

# ---------------------------
# Lifespan
# ---------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown lifecycle events."""
    logger = logging.getLogger(__name__)
    settings = get_settings()
    configure_logging(settings.debug)

    # Startup
    logger.info(f"Starting {settings.title} v{settings.version} [{settings.env}]")

    # Initialize MCP Server
    app.state.mcp_server = MCPServer(TOOLS_STORAGE)
    app.state.mcp_ws_handler = MCPWebSocketHandler(app.state.mcp_server)

    # Initialize Interconnectivity Manager
    async def tool_executor(tool_id=None, tool_name=None, input_data=None):
        # Simple executor that finds and executes the tool
        if tool_id:
            tool = TOOLS_STORAGE.get(tool_id)
        elif tool_name:
            tool = next((t for t in TOOLS_STORAGE.values() if t.get("name") == tool_name), None)
        else:
            raise ValueError("Either tool_id or tool_name must be provided")

        if not tool:
            raise ValueError(f"Tool not found: {tool_id or tool_name}")

        # In a real implementation, this would execute the actual tool
        # For now, return a simulated result
        return {
            "tool": tool.get("name"),
            "input": input_data,
            "output": {"message": f"Executed {tool.get('name')}"},
            "status": "success"
        }

    app.state.interconnectivity = InterconnectivityManager(tool_executor)
    await app.state.interconnectivity.initialize()

    # Initialize HATEOAS builder
    base_url = os.getenv("BASE_URL", "http://localhost:8000")
    app.state.hateoas = HATEOASBuilder(base_url)

    yield

    # Shutdown
    logger.info("Shutting down...")
    # Clean up any resources here


# ---------------------------
# Pydantic Models
# ---------------------------


class HealthResponse(BaseModel):
    status: str
    uptime: float
    service: str
    version: str
    environment: str


class ToolSchema(BaseModel):
    """Schema for a tool definition."""
    name: str = Field(..., description="Unique name of the tool")
    description: str = Field(..., description="Description of what the tool does")
    input_schema: Dict[str, Any] = Field(default_factory=dict, description="JSON schema for tool inputs")
    output_schema: Dict[str, Any] = Field(default_factory=dict, description="JSON schema for tool outputs")
    service_url: Optional[str] = Field(None, description="Service URL where the tool is hosted")
    endpoint: Optional[str] = Field(None, description="API endpoint for the tool")

    # Organization and ownership
    user_id: Optional[str] = Field(None, description="User who created/owns the tool")
    organization_id: Optional[str] = Field(None, description="Organization that owns the tool")
    toolkit_id: Optional[str] = Field(None, description="Toolkit/collection this tool belongs to")

    # UI and rendering
    has_ui_effects: bool = Field(False, description="Whether tool produces UI components")
    ui_renderer: Optional[str] = Field(None, description="Type of UI renderer (react, vue, html, etc.)")
    ui_schema: Optional[Dict[str, Any]] = Field(None, description="Schema for UI components")

    # Categorization and search
    tags: List[str] = Field(default_factory=list, description="Tags for categorization")
    category: Optional[str] = Field(None, description="Primary category")
    visibility: str = Field("private", description="Visibility: public, private, organization")

    # Advanced metadata
    version: str = Field("1.0.0", description="Tool version")
    dependencies: List[str] = Field(default_factory=list, description="Tool dependencies")
    pricing_tier: Optional[str] = Field(None, description="Pricing tier if applicable")
    usage_count: int = Field(0, description="Number of times tool has been used")
    rating: Optional[float] = Field(None, description="Average user rating")

    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class ToolResponse(ToolSchema):
    """Response model for tool operations."""
    id: str = Field(..., description="Unique identifier for the tool")
    created_at: Optional[str] = Field(None, description="Creation timestamp")
    updated_at: Optional[str] = Field(None, description="Last update timestamp")


class ToolExecutionRequest(BaseModel):
    """Request model for executing a tool."""
    tool_id: Optional[str] = Field(None, description="Tool ID to execute")
    tool_name: Optional[str] = Field(None, description="Tool name to execute")
    input_data: Dict[str, Any] = Field(..., description="Input data for the tool")


class ToolExecutionResponse(BaseModel):
    """Response model for tool execution."""
    tool_id: str
    status: str
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


# ---------------------------
# Application Factory
# ---------------------------


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.title,
        version=settings.version,
        lifespan=lifespan,
        docs_url="/docs" if settings.env != "production" else None,
        redoc_url="/redoc" if settings.env != "production" else None,
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add exception handler
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
        )

    # Add generic exception handler for unexpected errors
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logger = logging.getLogger(__name__)
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    return app


# ---------------------------
# Routes
# ---------------------------


# Create the main app instance
app = create_app()

# Create API router for versioned endpoints
api_router = APIRouter(prefix="/api/v1")

# In-memory storage for demo purposes (replace with database in production)
TOOLS_STORAGE: Dict[str, Dict[str, Any]] = {}
TOOL_ID_COUNTER = 0


@app.get("/", tags=["Root"])
async def root(request: Request):
    """Root endpoint with HATEOAS links to all available resources."""
    return request.app.state.hateoas.build_root_response()


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check(settings: Settings = Depends(get_settings)):
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        uptime=time.time() - START_TIME,
        service=settings.title,
        version=settings.version,
        environment=settings.env,
    )


@app.get("/ready", tags=["System"])
async def readiness_check():
    """Readiness check endpoint."""
    # Add any readiness checks here (e.g., database connectivity)
    return {"ready": True}


@api_router.get("/tools", response_model=List[ToolResponse], tags=["Tools"])
async def list_tools(
    user_id: Optional[str] = None,
    organization_id: Optional[str] = None,
    toolkit_id: Optional[str] = None,
    category: Optional[str] = None,
    has_ui_effects: Optional[bool] = None,
    visibility: Optional[str] = None,
    tags: Optional[str] = None,  # Comma-separated tags
    min_rating: Optional[float] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    List tools with advanced filtering options.

    Query Parameters:
    - user_id: Filter by user who created the tool
    - organization_id: Filter by organization
    - toolkit_id: Filter by toolkit/collection
    - category: Filter by category
    - has_ui_effects: Filter by UI rendering capability
    - visibility: Filter by visibility (public, private, organization)
    - tags: Comma-separated list of tags to filter by
    - min_rating: Minimum rating threshold
    - limit: Maximum number of results
    - offset: Pagination offset
    """
    filtered_tools = []
    tag_list = tags.split(",") if tags else []

    for tool in TOOLS_STORAGE.values():
        # Apply filters
        if user_id and tool.get("user_id") != user_id:
            continue
        if organization_id and tool.get("organization_id") != organization_id:
            continue
        if toolkit_id and tool.get("toolkit_id") != toolkit_id:
            continue
        if category and tool.get("category") != category:
            continue
        if has_ui_effects is not None and tool.get("has_ui_effects") != has_ui_effects:
            continue
        if visibility and tool.get("visibility") != visibility:
            continue
        if tag_list and not any(tag in tool.get("tags", []) for tag in tag_list):
            continue
        if min_rating and tool.get("rating", 0) < min_rating:
            continue

        filtered_tools.append(tool)

    # Apply pagination
    start = offset
    end = offset + limit
    return filtered_tools[start:end]


@api_router.post("/tools", response_model=ToolResponse, status_code=status.HTTP_201_CREATED, tags=["Tools"])
async def create_tool(tool: ToolSchema, request: Request):
    """Register a new tool."""
    global TOOL_ID_COUNTER

    # Check if tool with same name exists
    for existing_tool in TOOLS_STORAGE.values():
        if existing_tool["name"] == tool.name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tool with name '{tool.name}' already exists"
            )

    TOOL_ID_COUNTER += 1
    tool_id = f"tool_{TOOL_ID_COUNTER}"

    tool_data = tool.model_dump()
    tool_data["id"] = tool_id
    tool_data["created_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    tool_data["updated_at"] = tool_data["created_at"]

    # Compute embedding from name + description
    embedder = get_embedder()
    embed_text = f"{tool.name}: {tool.description}"
    embedding = await embedder.embed(embed_text)
    if embedding:
        tool_data["embedding"] = embedding
        logger.info(f"Computed embedding for tool '{tool.name}' ({len(embedding)} dims)")
    else:
        logger.warning(f"No embedding computed for tool '{tool.name}' - no provider configured")

    TOOLS_STORAGE[tool_id] = tool_data

    # Publish event
    if hasattr(request.app.state, 'interconnectivity'):
        await request.app.state.interconnectivity.event_bus.publish(
            EventType.TOOL_CREATED,
            {"tool_id": tool_id, "tool_name": tool.name}
        )

        # Notify MCP clients
        if hasattr(request.app.state, 'mcp_server'):
            await request.app.state.mcp_server.notify_tool_changed(tool_id)

    return ToolResponse(**tool_data)


@api_router.get("/tools/{tool_id}", tags=["Tools"])
async def get_tool(tool_id: str, request: Request):
    """Get a specific tool by ID with HATEOAS links."""
    if tool_id not in TOOLS_STORAGE:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tool with ID '{tool_id}' not found"
        )

    # Check content negotiation
    accept = request.headers.get("accept", "application/hal+json")

    # Build HATEOAS response
    tool_response = request.app.state.hateoas.build_tool_response(TOOLS_STORAGE[tool_id])

    # Convert to requested format if needed
    if "application/vnd.siren+json" in accept:
        from hateoas import ContentNegotiation
        tool_response = ContentNegotiation.convert_to_siren(tool_response)

    return tool_response


@api_router.get("/tools/name/{tool_name}", response_model=ToolResponse, tags=["Tools"])
async def get_tool_by_name(tool_name: str):
    """Get a specific tool by name."""
    for tool_data in TOOLS_STORAGE.values():
        if tool_data["name"] == tool_name:
            return ToolResponse(**tool_data)

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Tool with name '{tool_name}' not found"
    )


@api_router.put("/tools/{tool_id}", response_model=ToolResponse, tags=["Tools"])
async def update_tool(tool_id: str, tool: ToolSchema):
    """Update an existing tool."""
    if tool_id not in TOOLS_STORAGE:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tool with ID '{tool_id}' not found"
        )

    old_tool = TOOLS_STORAGE[tool_id]
    tool_data = tool.model_dump()
    tool_data["id"] = tool_id
    tool_data["created_at"] = old_tool["created_at"]
    tool_data["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    # Recompute embedding if name or description changed
    if tool.name != old_tool.get("name") or tool.description != old_tool.get("description"):
        embedder = get_embedder()
        embed_text = f"{tool.name}: {tool.description}"
        embedding = await embedder.embed(embed_text)
        if embedding:
            tool_data["embedding"] = embedding
            logger.info(f"Recomputed embedding for tool '{tool.name}'")
    else:
        # Preserve existing embedding
        if "embedding" in old_tool:
            tool_data["embedding"] = old_tool["embedding"]

    TOOLS_STORAGE[tool_id] = tool_data

    return ToolResponse(**tool_data)


@api_router.delete("/tools/{tool_id}", tags=["Tools"])
async def delete_tool(tool_id: str):
    """Delete a tool."""
    if tool_id not in TOOLS_STORAGE:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tool with ID '{tool_id}' not found"
        )

    del TOOLS_STORAGE[tool_id]
    return {"message": f"Tool '{tool_id}' deleted successfully"}


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


@api_router.post("/tools/search", tags=["Tools"])
async def search_tools_semantic(prompt: str, limit: int = 10):
    """
    Semantic search for tools using precomputed embeddings.

    Args:
        prompt: Natural language query
        limit: Max results to return
    """
    embedder = get_embedder()

    # Compute query embedding
    query_embedding = await embedder.embed(prompt)
    if not query_embedding:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No embedding provider configured (set JINA_API_KEY or OPENAI_API_KEY)"
        )

    # Find tools with embeddings and compute similarity
    scored_tools = []
    for tool_id, tool_data in TOOLS_STORAGE.items():
        if "embedding" not in tool_data:
            continue

        score = cosine_similarity(query_embedding, tool_data["embedding"])
        scored_tools.append({
            "tool_id": tool_id,
            "name": tool_data["name"],
            "description": tool_data["description"],
            "score": score
        })

    # Sort by score descending
    scored_tools.sort(key=lambda x: x["score"], reverse=True)

    return {
        "query": prompt,
        "results": scored_tools[:limit],
        "total_with_embeddings": len([t for t in TOOLS_STORAGE.values() if "embedding" in t]),
        "total_tools": len(TOOLS_STORAGE)
    }


@api_router.post("/tools/execute", response_model=ToolExecutionResponse, tags=["Execution"])
async def execute_tool(request: ToolExecutionRequest):
    """Execute a tool with given inputs."""
    tool_id = request.tool_id

    # If tool_name is provided instead of tool_id, find the tool by name
    if not tool_id and request.tool_name:
        for tid, tool_data in TOOLS_STORAGE.items():
            if tool_data["name"] == request.tool_name:
                tool_id = tid
                break

    if not tool_id or tool_id not in TOOLS_STORAGE:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tool not found"
        )

    # This is a placeholder for actual tool execution
    # In a real implementation, this would call the tool's service URL/endpoint
    return ToolExecutionResponse(
        tool_id=tool_id,
        status="success",
        output={"message": f"Tool '{TOOLS_STORAGE[tool_id]['name']}' executed successfully", "input": request.input_data}
    )


@api_router.post("/tools/search", response_model=List[ToolResponse], tags=["Tools"])
async def search_tools(
    node_label: Optional[str] = "Tool",
    prompt: Optional[str] = None,
    limit: int = 10
):
    """Search for tools based on criteria."""
    results = []

    for tool_data in TOOLS_STORAGE.values():
        # Simple search implementation - in production, use proper search/filtering
        if prompt and prompt.lower() not in tool_data["name"].lower() and prompt.lower() not in tool_data["description"].lower():
            continue
        results.append(ToolResponse(**tool_data))

        if len(results) >= limit:
            break

    return results


@api_router.get("/tools/by-user/{user_id}", response_model=List[ToolResponse], tags=["Tools"])
async def get_tools_by_user(user_id: str):
    """Get all tools created by a specific user."""
    user_tools = [
        tool for tool in TOOLS_STORAGE.values()
        if tool.get("user_id") == user_id
    ]
    return user_tools


@api_router.get("/tools/by-organization/{organization_id}", response_model=List[ToolResponse], tags=["Tools"])
async def get_tools_by_organization(organization_id: str):
    """Get all tools belonging to a specific organization."""
    org_tools = [
        tool for tool in TOOLS_STORAGE.values()
        if tool.get("organization_id") == organization_id
    ]
    return org_tools


@api_router.get("/tools/by-toolkit/{toolkit_id}", response_model=List[ToolResponse], tags=["Tools"])
async def get_tools_by_toolkit(toolkit_id: str):
    """Get all tools in a specific toolkit/collection."""
    toolkit_tools = [
        tool for tool in TOOLS_STORAGE.values()
        if tool.get("toolkit_id") == toolkit_id
    ]
    return toolkit_tools


@api_router.post("/tools/by-input-schema", response_model=List[ToolResponse], tags=["Tools"])
async def get_tools_by_input_schema(
    schema: Dict[str, Any],
    match_type: str = "exact"  # exact, compatible, partial
):
    """
    Find tools with matching input schemas.

    - exact: Schema must match exactly
    - compatible: Tool can accept the provided schema
    - partial: Schema shares common properties
    """
    matching_tools = []

    for tool in TOOLS_STORAGE.values():
        tool_schema = tool.get("input_schema", {})

        if match_type == "exact":
            if tool_schema == schema:
                matching_tools.append(tool)
        elif match_type == "compatible":
            # Check if provided schema satisfies tool requirements
            if _schema_compatible(schema, tool_schema):
                matching_tools.append(tool)
        elif match_type == "partial":
            # Check for any common properties
            if _schemas_have_common_properties(schema, tool_schema):
                matching_tools.append(tool)

    return matching_tools


@api_router.post("/tools/by-output-schema", response_model=List[ToolResponse], tags=["Tools"])
async def get_tools_by_output_schema(
    schema: Dict[str, Any],
    match_type: str = "exact"  # exact, compatible, partial
):
    """
    Find tools with matching output schemas.

    - exact: Schema must match exactly
    - compatible: Tool produces compatible output
    - partial: Schema shares common properties
    """
    matching_tools = []

    for tool in TOOLS_STORAGE.values():
        tool_schema = tool.get("output_schema", {})

        if match_type == "exact":
            if tool_schema == schema:
                matching_tools.append(tool)
        elif match_type == "compatible":
            # Check if tool output satisfies required schema
            if _schema_compatible(tool_schema, schema):
                matching_tools.append(tool)
        elif match_type == "partial":
            # Check for any common properties
            if _schemas_have_common_properties(schema, tool_schema):
                matching_tools.append(tool)

    return matching_tools


@api_router.get("/tools/with-ui-effects", response_model=List[ToolResponse], tags=["Tools"])
async def get_tools_with_ui_effects(
    ui_renderer: Optional[str] = None
):
    """Get all tools that produce UI effects, optionally filtered by renderer type."""
    ui_tools = []

    for tool in TOOLS_STORAGE.values():
        if tool.get("has_ui_effects"):
            if ui_renderer is None or tool.get("ui_renderer") == ui_renderer:
                ui_tools.append(tool)

    return ui_tools


@api_router.get("/tools/categories", tags=["Tools"])
async def get_tool_categories():
    """Get all available tool categories with counts."""
    categories = {}

    for tool in TOOLS_STORAGE.values():
        category = tool.get("category", "uncategorized")
        categories[category] = categories.get(category, 0) + 1

    return {
        "categories": [
            {"name": cat, "count": count}
            for cat, count in categories.items()
        ]
    }


@api_router.get("/tools/tags", tags=["Tools"])
async def get_tool_tags():
    """Get all available tool tags with usage counts."""
    tags = {}

    for tool in TOOLS_STORAGE.values():
        for tag in tool.get("tags", []):
            tags[tag] = tags.get(tag, 0) + 1

    return {
        "tags": [
            {"name": tag, "count": count}
            for tag, count in sorted(tags.items(), key=lambda x: x[1], reverse=True)
        ]
    }


@api_router.get("/tools/statistics", tags=["Tools"])
async def get_tools_statistics():
    """Get aggregated statistics about all tools."""
    tools = list(TOOLS_STORAGE.values())

    if not tools:
        return {
            "total_tools": 0,
            "by_visibility": {},
            "by_category": {},
            "with_ui_effects": 0,
            "average_rating": 0,
            "most_used": []
        }

    visibility_counts = {}
    category_counts = {}
    ui_count = 0
    total_rating = 0
    rated_count = 0

    for tool in tools:
        # Visibility
        vis = tool.get("visibility", "private")
        visibility_counts[vis] = visibility_counts.get(vis, 0) + 1

        # Category
        cat = tool.get("category", "uncategorized")
        category_counts[cat] = category_counts.get(cat, 0) + 1

        # UI effects
        if tool.get("has_ui_effects"):
            ui_count += 1

        # Rating
        if tool.get("rating"):
            total_rating += tool["rating"]
            rated_count += 1

    # Most used tools
    most_used = sorted(
        tools,
        key=lambda x: x.get("usage_count", 0),
        reverse=True
    )[:10]

    return {
        "total_tools": len(tools),
        "by_visibility": visibility_counts,
        "by_category": category_counts,
        "with_ui_effects": ui_count,
        "average_rating": total_rating / rated_count if rated_count > 0 else 0,
        "most_used": [
            {
                "name": tool["name"],
                "usage_count": tool.get("usage_count", 0)
            }
            for tool in most_used
        ]
    }


# Helper functions for schema matching
def _schema_compatible(provided: Dict, required: Dict) -> bool:
    """Check if provided schema satisfies required schema."""
    required_props = required.get("properties", {})
    provided_props = provided.get("properties", {})

    for prop_name, prop_schema in required_props.items():
        if prop_name not in provided_props:
            # Check if property is required
            if prop_name in required.get("required", []):
                return False

    return True


def _schemas_have_common_properties(schema1: Dict, schema2: Dict) -> bool:
    """Check if two schemas have any common properties."""
    props1 = set(schema1.get("properties", {}).keys())
    props2 = set(schema2.get("properties", {}).keys())
    return bool(props1.intersection(props2))


# Include the API router
app.include_router(api_router)


# ==============================================================================
# MCP (Model Context Protocol) Endpoints
# ==============================================================================

@app.websocket("/mcp")
async def mcp_websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for MCP protocol"""
    await app.state.mcp_ws_handler.handle_websocket(websocket)


@api_router.get("/mcp/info", tags=["MCP"])
async def get_mcp_info():
    """Get MCP server information"""
    return {
        "protocol_version": "DRAFT-2025-v3",
        "server_name": "Tool Management MCP Server",
        "server_version": "1.0.0",
        "capabilities": {
            "tools": {"listChanged": True},
            "resources": {"subscribe": True, "listChanged": True},
            "prompts": {"listChanged": True},
            "experimental": {
                "toolComposition": True,
                "webhooks": True,
                "federation": True
            }
        }
    }


# ==============================================================================
# Tool Composition Endpoints
# ==============================================================================

@api_router.post("/compositions", tags=["Compositions"])
async def create_composition(
    name: str,
    description: str,
    type: CompositionType,
    tools: List[Dict[str, Any]],
    connections: Optional[List[Dict[str, Any]]] = None
):
    """Create a tool composition for chaining or parallel execution"""
    composition = await app.state.interconnectivity.tool_chain.create_composition(
        name=name,
        description=description,
        type=type,
        tools=tools,
        connections=connections
    )
    return composition.model_dump()


@api_router.get("/compositions", tags=["Compositions"])
async def list_compositions():
    """List all tool compositions"""
    compositions = app.state.interconnectivity.tool_chain.compositions
    return [comp.model_dump() for comp in compositions.values()]


@api_router.post("/compositions/{composition_id}/execute", tags=["Compositions"])
async def execute_composition(
    composition_id: str,
    input_data: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None
):
    """Execute a tool composition"""
    try:
        result = await app.state.interconnectivity.tool_chain.execute_composition(
            composition_id=composition_id,
            input_data=input_data,
            context=context
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/compositions/history", tags=["Compositions"])
async def get_composition_history(limit: int = 100):
    """Get composition execution history"""
    history = app.state.interconnectivity.tool_chain.execution_history
    return history[-limit:]


# ==============================================================================
# Webhook Endpoints
# ==============================================================================

@api_router.post("/webhooks", tags=["Webhooks"])
async def register_webhook(webhook: Webhook):
    """Register a webhook for events"""
    webhook_id = await app.state.interconnectivity.event_bus.register_webhook(webhook)
    return {"webhook_id": webhook_id, "status": "registered"}


@api_router.delete("/webhooks/{webhook_id}", tags=["Webhooks"])
async def unregister_webhook(webhook_id: str):
    """Unregister a webhook"""
    await app.state.interconnectivity.event_bus.unregister_webhook(webhook_id)
    return {"webhook_id": webhook_id, "status": "unregistered"}


@api_router.get("/webhooks", tags=["Webhooks"])
async def list_webhooks():
    """List all registered webhooks"""
    webhooks = app.state.interconnectivity.event_bus.webhooks
    return [webhook.model_dump() for webhook in webhooks.values()]


@api_router.get("/webhooks/deliveries", tags=["Webhooks"])
async def get_webhook_deliveries(limit: int = 100):
    """Get webhook delivery history"""
    deliveries = app.state.interconnectivity.event_bus.webhook_deliveries
    return deliveries[-limit:]


@api_router.get("/events", tags=["Events"])
async def get_events(
    event_type: Optional[EventType] = None,
    limit: int = 100
):
    """Get event history"""
    events = app.state.interconnectivity.event_bus.event_history

    if event_type:
        events = [e for e in events if e.type == event_type]

    return [
        {
            "id": e.id,
            "type": e.type.value,
            "timestamp": e.timestamp.isoformat(),
            "data": e.data,
            "metadata": e.metadata
        }
        for e in events[-limit:]
    ]


# ==============================================================================
# Protocol Adapter Endpoints
# ==============================================================================

@api_router.post("/adapters/convert", tags=["Adapters"])
async def convert_tool_format(
    tool: Dict[str, Any],
    from_protocol: str,
    to_protocol: str
):
    """Convert a tool between different protocol formats"""
    # Get adapters
    from_adapter = app.state.interconnectivity.get_adapter(from_protocol)
    to_adapter = app.state.interconnectivity.get_adapter(to_protocol)

    if not from_adapter:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown source protocol: {from_protocol}"
        )

    if not to_adapter:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown target protocol: {to_protocol}"
        )

    # Convert to internal format
    internal_tool = from_adapter.to_internal(tool)

    # Convert to target format
    converted_tool = to_adapter.from_internal(internal_tool)

    return {
        "original": tool,
        "internal": internal_tool,
        "converted": converted_tool,
        "from_protocol": from_protocol,
        "to_protocol": to_protocol
    }


@api_router.get("/adapters", tags=["Adapters"])
async def list_protocol_adapters():
    """List available protocol adapters"""
    adapters = app.state.interconnectivity.adapters.keys()
    return {
        "adapters": list(adapters),
        "supported_protocols": {
            "openai": "OpenAI Function Calling format",
            "anthropic": "Anthropic Tool Use format",
            "langchain": "LangChain Tool format",
            "mcp": "Model Context Protocol format"
        }
    }


# ==============================================================================
# Federation Endpoints
# ==============================================================================

@api_router.post("/federation/nodes", tags=["Federation"])
async def add_federation_node(node: RegistryNode):
    """Add a federated registry node"""
    node_id = await app.state.interconnectivity.federation.add_node(node)
    return {"node_id": node_id, "status": "added"}


@api_router.delete("/federation/nodes/{node_id}", tags=["Federation"])
async def remove_federation_node(node_id: str):
    """Remove a federated registry node"""
    await app.state.interconnectivity.federation.remove_node(node_id)
    return {"node_id": node_id, "status": "removed"}


@api_router.get("/federation/nodes", tags=["Federation"])
async def list_federation_nodes():
    """List all federated registry nodes"""
    nodes = app.state.interconnectivity.federation.nodes
    return [node.model_dump() for node in nodes.values()]


@api_router.post("/federation/sync/{node_id}", tags=["Federation"])
async def sync_federation_node(node_id: str):
    """Manually sync a specific federated node"""
    try:
        await app.state.interconnectivity.federation.sync_node(node_id)
        return {"node_id": node_id, "status": "synced"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api_router.post("/federation/search", tags=["Federation"])
async def search_federated_tools(
    query: str,
    filters: Optional[Dict[str, Any]] = None
):
    """Search tools across all federated registries"""
    results = await app.state.interconnectivity.federation.search_federated(
        query=query,
        filters=filters
    )
    return {"query": query, "results": results, "count": len(results)}


@api_router.post("/federation/execute", tags=["Federation"])
async def execute_federated_tool(
    node_id: str,
    tool_id: str,
    input_data: Dict[str, Any]
):
    """Execute a tool on a federated node"""
    try:
        result = await app.state.interconnectivity.federation.execute_federated_tool(
            node_id=node_id,
            tool_id=tool_id,
            input_data=input_data
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------
# Main entry point
# ---------------------------


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("RELOAD", "0") == "1"

    uvicorn.run(
        "fastapi_app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="debug" if settings.debug else "info",
    )