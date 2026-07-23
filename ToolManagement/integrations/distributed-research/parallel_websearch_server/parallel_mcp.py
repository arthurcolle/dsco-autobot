"""MCP glue helpers for exposing the application as a Parallel Web Search MCP."""

from typing import TYPE_CHECKING, List

from fastapi import FastAPI

if TYPE_CHECKING:
    from fastapi_mcp import FastApiMCP

from parallel_websearch_server.config import settings
from parallel_websearch_server.mcp_extensions import register_mcp_extensions

MCP_TOOL_TAGS: List[str] = [
    "Meta-Pipeline",     # /v1/meta/* - advanced meta-pipeline with KG
    "Search & Extract",  # /v1beta/search, /v1beta/extract
    "Tasks",             # /v1/tasks/runs/*
    "Task Groups",       # /v1beta/tasks/groups/*
    "FindAll",           # /v1beta/findall/*
    "Monitors",          # /v1alpha/monitors/*
    "Storage",           # Retrieve stored tasks/searches
]

MCP_FORWARD_HEADERS: List[str] = [
    "authorization",
    "x-api-key",
    "parallel-beta",
    "x-request-id",
]


def mount_parallel_mcp(app: FastAPI) -> "FastApiMCP":
    """Install the Parallel Web Search MCP onto the provided FastAPI app."""
    from fastapi_mcp import FastApiMCP

    mcp = FastApiMCP(
        app,
        name="Parallel Web Search MCP",
        description=(
            "Parallel Web Search MCP exposing Search, Extract, Task, Task Group, "
            "FindAll, and Monitor operations with SSE streaming support."
        ),
        headers=MCP_FORWARD_HEADERS,
        include_tags=MCP_TOOL_TAGS,
        describe_all_responses=True,
        describe_full_response_schema=True,
    )

    mcp.mount_http(mount_path=settings.mcp_http_path)
    mcp.mount_sse(mount_path=settings.mcp_sse_path)
    register_mcp_extensions(mcp)

    return mcp
