"""API endpoints and routers."""

from .routes import router
from .service_routes import router as service_router
from .retrieval_routes import router as retrieval_router
from .graphql import Query, Mutation
from .unified import (
    ToolCreate,
    ToolResponse,
    MCPServerCreate,
    MCPServerResponse,
    ToolSearchRequest,
    ToolSearchResponse,
    ToolExecuteRequest,
    ToolExecuteResponse,
)

__all__ = [
    # REST
    "router",
    "service_router",
    "retrieval_router",
    # GraphQL
    "Query",
    "Mutation",
    # Unified API models
    "ToolCreate",
    "ToolResponse",
    "MCPServerCreate",
    "MCPServerResponse",
    "ToolSearchRequest",
    "ToolSearchResponse",
    "ToolExecuteRequest",
    "ToolExecuteResponse",
]
