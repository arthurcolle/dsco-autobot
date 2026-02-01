"""
API Routes for Desktop MCP Server.

Exposes desktop/filesystem tools via REST API and MCP endpoint.
"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..mcp.desktop_server import get_desktop_server

router = APIRouter(prefix="/desktop", tags=["desktop"])


# =============================================================================
# Request/Response Models
# =============================================================================

class ToolCallRequest(BaseModel):
    """Request to call a desktop tool."""
    tool: str = Field(..., description="Tool name")
    arguments: Dict[str, Any] = Field(default_factory=dict)


class MCPRequest(BaseModel):
    """MCP JSON-RPC request."""
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[Any] = None


# =============================================================================
# Tool Listing
# =============================================================================

@router.get("/tools")
async def list_desktop_tools():
    """List available desktop tools."""
    server = get_desktop_server()
    return {
        "tools": server.get_tools(),
        "total": len(server.get_tools()),
    }


# =============================================================================
# Tool Execution
# =============================================================================

@router.post("/call")
async def call_tool(request: ToolCallRequest):
    """Call a desktop tool directly."""
    server = get_desktop_server()
    result = await server.call_tool(request.tool, request.arguments)
    return result


@router.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP JSON-RPC endpoint for desktop tools."""
    server = get_desktop_server()

    if request.method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request.id,
            "result": {"tools": server.get_tools()},
        }

    if request.method == "tools/call":
        params = request.params or {}
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        if not tool_name:
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "error": {"code": -32602, "message": "Missing tool name"},
            }

        result = await server.call_tool(tool_name, arguments)

        return {
            "jsonrpc": "2.0",
            "id": request.id,
            "result": {"content": [{"type": "text", "text": str(result)}]},
        }

    return {
        "jsonrpc": "2.0",
        "id": request.id,
        "error": {"code": -32601, "message": f"Unknown method: {request.method}"},
    }


# =============================================================================
# File Operations (Direct REST)
# =============================================================================

@router.get("/files/read")
async def read_file(
    path: str = Query(..., description="File path"),
    max_lines: int = Query(0, description="Max lines (0=all)"),
):
    """Read a file."""
    server = get_desktop_server()
    return await server.call_tool("read_file", {"path": path, "max_lines": max_lines})


@router.post("/files/write")
async def write_file(
    path: str = Query(..., description="File path"),
    content: str = "",
    append: bool = False,
):
    """Write to a file."""
    server = get_desktop_server()
    return await server.call_tool("write_file", {"path": path, "content": content, "append": append})


@router.get("/files/list")
async def list_directory(
    path: str = Query(..., description="Directory path"),
    pattern: Optional[str] = Query(None),
    recursive: bool = Query(False),
):
    """List directory contents."""
    server = get_desktop_server()
    return await server.call_tool("list_directory", {
        "path": path,
        "pattern": pattern,
        "recursive": recursive,
    })


@router.get("/files/search")
async def search_files(
    pattern: str = Query(..., description="Glob pattern"),
    root: Optional[str] = Query(None, description="Root directory"),
    content: Optional[str] = Query(None, description="Content regex"),
):
    """Search for files."""
    server = get_desktop_server()
    return await server.call_tool("search_files", {
        "pattern": pattern,
        "root": root,
        "content_pattern": content,
    })


@router.get("/files/info")
async def get_file_info(path: str = Query(..., description="File path")):
    """Get file metadata."""
    server = get_desktop_server()
    return await server.call_tool("get_file_info", {"path": path})


# =============================================================================
# Command Execution
# =============================================================================

@router.post("/command")
async def run_command(
    command: str,
    cwd: Optional[str] = None,
    timeout: int = 30,
):
    """Execute a shell command."""
    server = get_desktop_server()
    return await server.call_tool("run_command", {
        "command": command,
        "cwd": cwd,
        "timeout": timeout,
    })


# =============================================================================
# Project Management
# =============================================================================

@router.get("/projects")
async def list_projects(
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """List all projects."""
    server = get_desktop_server()
    return await server.call_tool("project_list", {"status_filter": status})


@router.get("/projects/{project_id}")
async def get_project(project_id: str):
    """Get a specific project."""
    server = get_desktop_server()
    return await server.call_tool("project_list", {"project_id": project_id})


class ProjectCreateRequest(BaseModel):
    name: str
    root_path: str
    description: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


@router.post("/projects")
async def create_project(request: ProjectCreateRequest):
    """Create a new project."""
    server = get_desktop_server()
    return await server.call_tool("project_create", {
        "name": request.name,
        "root_path": request.root_path,
        "description": request.description,
        "metadata": request.metadata,
    })


class ProjectUpdateRequest(BaseModel):
    note: Optional[str] = None
    files_accessed: Optional[List[str]] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@router.patch("/projects/{project_id}")
async def update_project(project_id: str, request: ProjectUpdateRequest):
    """Update a project."""
    server = get_desktop_server()
    return await server.call_tool("project_update", {
        "project_id": project_id,
        "note": request.note,
        "files_accessed": request.files_accessed,
        "status": request.status,
        "metadata": request.metadata,
    })


# =============================================================================
# System Info
# =============================================================================

@router.get("/system")
async def get_system_info(include_env: bool = Query(False)):
    """Get system information."""
    server = get_desktop_server()
    return await server.call_tool("system_info", {"include_env": include_env})
