"""
MCP (Model Context Protocol) Server Implementation

This module provides MCP server capabilities, allowing tools to be exposed
via the Model Context Protocol for AI model integration.

Protocol Version: DRAFT-2025-v3
"""

import os
import json
import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import uuid

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
import httpx

logger = logging.getLogger(__name__)

# API Base URL - configurable for deployed environments
# In Modal, we use the public URL; locally, we use localhost
_default_url = "https://tools.distributed.systems" if os.getenv("MODAL_ENVIRONMENT") or os.getenv("DATA_DIR") == "/data" else "http://localhost:2016"
API_BASE_URL = os.getenv("API_BASE_URL", _default_url)

# Shared HTTP client for connection pooling
_http_client: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    """Get or create shared HTTP client with connection pooling."""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )
    return _http_client


async def close_http_client():
    """Close the shared HTTP client."""
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None

# MCP Protocol Constants
LATEST_PROTOCOL_VERSION = "2024-11-05"
JSONRPC_VERSION = "2.0"

# JSON-RPC Error Codes
class ErrorCode(Enum):
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    SERVER_ERROR = -32000


# MCP Types
class MCPRequest(BaseModel):
    """MCP JSON-RPC Request"""
    jsonrpc: str = JSONRPC_VERSION
    id: Union[str, int]
    method: str
    params: Optional[Dict[str, Any]] = None


class MCPNotification(BaseModel):
    """MCP JSON-RPC Notification"""
    jsonrpc: str = JSONRPC_VERSION
    method: str
    params: Optional[Dict[str, Any]] = None


class MCPResponse(BaseModel):
    """MCP JSON-RPC Response"""
    jsonrpc: str = JSONRPC_VERSION
    id: Union[str, int]
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None


class MCPError(BaseModel):
    """MCP Error object"""
    code: int
    message: str
    data: Optional[Any] = None


# MCP Server Capabilities
@dataclass
class ServerCapabilities:
    """Server capabilities for MCP"""
    tools: Dict[str, Any] = field(default_factory=lambda: {"listChanged": True})
    resources: Dict[str, Any] = field(default_factory=lambda: {
        "subscribe": True,
        "listChanged": True
    })
    prompts: Dict[str, Any] = field(default_factory=lambda: {"listChanged": True})
    logging: Dict[str, Any] = field(default_factory=dict)
    completions: Dict[str, Any] = field(default_factory=dict)
    experimental: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ClientCapabilities:
    """Client capabilities for MCP"""
    roots: Optional[Dict[str, Any]] = None
    sampling: Optional[Dict[str, Any]] = None
    elicitation: Optional[Dict[str, Any]] = None
    experimental: Dict[str, Any] = field(default_factory=dict)


class MCPToolAdapter:
    """Adapter to convert internal tools to MCP format"""

    @staticmethod
    def to_mcp_tool(tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert internal tool representation to MCP Tool format"""
        return {
            "name": tool.get("name"),
            "description": tool.get("description"),
            "inputSchema": {
                "type": "object",
                "properties": tool.get("input_schema", {}).get("properties", {}),
                "required": tool.get("input_schema", {}).get("required", [])
            },
            "outputSchema": {
                "type": "object",
                "properties": tool.get("output_schema", {}).get("properties", {}),
                "required": tool.get("output_schema", {}).get("required", [])
            } if tool.get("output_schema") else None,
            "annotations": {
                "title": tool.get("name"),
                "category": tool.get("category"),
                "tags": tool.get("tags", []),
                "version": tool.get("version", "1.0.0"),
                "author": tool.get("user_id"),
                "organization": tool.get("organization_id")
            },
            "_meta": {
                "toolkit_id": tool.get("toolkit_id"),
                "has_ui_effects": tool.get("has_ui_effects"),
                "ui_renderer": tool.get("ui_renderer"),
                "visibility": tool.get("visibility"),
                "rating": tool.get("rating"),
                "usage_count": tool.get("usage_count")
            }
        }

    @staticmethod
    def from_mcp_tool(mcp_tool: Dict[str, Any]) -> Dict[str, Any]:
        """Convert MCP Tool format to internal representation"""
        annotations = mcp_tool.get("annotations", {})
        meta = mcp_tool.get("_meta", {})

        return {
            "name": mcp_tool.get("name"),
            "description": mcp_tool.get("description"),
            "input_schema": {
                "type": "object",
                "properties": mcp_tool.get("inputSchema", {}).get("properties", {}),
                "required": mcp_tool.get("inputSchema", {}).get("required", [])
            },
            "output_schema": {
                "type": "object",
                "properties": mcp_tool.get("outputSchema", {}).get("properties", {}),
                "required": mcp_tool.get("outputSchema", {}).get("required", [])
            } if mcp_tool.get("outputSchema") else {},
            "category": annotations.get("category"),
            "tags": annotations.get("tags", []),
            "version": annotations.get("version", "1.0.0"),
            "user_id": annotations.get("author"),
            "organization_id": annotations.get("organization"),
            "toolkit_id": meta.get("toolkit_id"),
            "has_ui_effects": meta.get("has_ui_effects", False),
            "ui_renderer": meta.get("ui_renderer"),
            "visibility": meta.get("visibility", "private"),
            "rating": meta.get("rating"),
            "usage_count": meta.get("usage_count", 0)
        }


class MCPResourceAdapter:
    """Adapter for MCP Resources"""

    @staticmethod
    def create_resource(
        uri: str,
        name: str,
        description: Optional[str] = None,
        mime_type: str = "application/json",
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create an MCP Resource"""
        return {
            "uri": uri,
            "name": name,
            "description": description,
            "mimeType": mime_type,
            "_meta": metadata or {}
        }

    @staticmethod
    def create_resource_contents(
        uri: str,
        content: Any,
        mime_type: str = "application/json"
    ) -> Dict[str, Any]:
        """Create MCP ResourceContents"""
        if mime_type == "application/json":
            return {
                "uri": uri,
                "mimeType": mime_type,
                "text": json.dumps(content) if not isinstance(content, str) else content
            }
        else:
            return {
                "uri": uri,
                "mimeType": mime_type,
                "text": str(content)
            }


class MCPServer:
    """MCP Server implementation"""

    def __init__(self, tool_storage: Dict[str, Any]):
        self.tool_storage = tool_storage
        self.server_info = {
            "name": "Tool Management MCP Server",
            "version": "1.0.0",
            "protocolVersion": LATEST_PROTOCOL_VERSION,
            "capabilities": ServerCapabilities()
        }
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.subscriptions: Dict[str, List[str]] = {}
        self.tool_adapter = MCPToolAdapter()
        self.resource_adapter = MCPResourceAdapter()

    async def handle_request(self, request: MCPRequest, session_id: str) -> MCPResponse:
        """Handle incoming MCP request"""
        try:
            method_handler = getattr(self, f"_handle_{request.method.replace('/', '_')}", None)
            if not method_handler:
                return self._error_response(
                    request.id,
                    ErrorCode.METHOD_NOT_FOUND.value,
                    f"Method '{request.method}' not found"
                )

            result = await method_handler(request.params or {}, session_id)
            return MCPResponse(id=request.id, result=result)

        except Exception as e:
            logger.error(f"Error handling request {request.method}: {e}")
            return self._error_response(
                request.id,
                ErrorCode.INTERNAL_ERROR.value,
                str(e)
            )

    async def handle_notification(self, notification: MCPNotification, session_id: str):
        """Handle incoming MCP notification"""
        try:
            method_handler = getattr(self, f"_handle_{notification.method.replace('/', '_')}", None)
            if method_handler:
                await method_handler(notification.params or {}, session_id)
        except Exception as e:
            logger.error(f"Error handling notification {notification.method}: {e}")

    def _error_response(self, request_id: Union[str, int], code: int, message: str, data: Any = None) -> MCPResponse:
        """Create an error response"""
        return MCPResponse(
            id=request_id,
            error={
                "code": code,
                "message": message,
                "data": data
            }
        )

    # MCP Protocol Handlers
    async def _handle_initialize(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Handle initialize request"""
        client_info = params.get("clientInfo", {})
        client_capabilities = params.get("capabilities", {})

        # Store session info
        self.sessions[session_id] = {
            "client_info": client_info,
            "capabilities": ClientCapabilities(**client_capabilities),
            "initialized": False
        }

        return {
            "serverInfo": {
                "id": "mcpsrv_tool_registry",
                "name": self.server_info["name"],
                "version": self.server_info["version"]
            },
            "protocolVersion": self.server_info["protocolVersion"],
            "capabilities": {
                "tools": {"listChanged": True},
                "resources": {"subscribe": True, "listChanged": True},
                "prompts": {"listChanged": True},
                "logging": {},
                "completions": {}
            }
        }

    async def _handle_notifications_initialized(self, params: Dict[str, Any], session_id: str):
        """Handle initialized notification"""
        if session_id in self.sessions:
            self.sessions[session_id]["initialized"] = True
            logger.info(f"Session {session_id} initialized")

    async def _handle_tools_list(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """
        Handle tools/list request - returns ONLY meta-tools for discovery & composition.

        CRITICAL: We have 100s of tools from multiple MCP servers. Returning all would
        cause context overflow and crash. Instead, return meta-tools that enable:
        - Lazy discovery via semantic search
        - Tool composition by reference
        - On-demand tool loading
        """
        cursor = params.get("cursor")

        # Define meta-tools for lazy discovery and composition
        meta_tools = [
            {
                "name": "search_tools",
                "description": "Semantically search for tools by description, use case, or capability. Returns matching tools with relevance scores. Use this to discover which tools are available before using them.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query describing what you want to do (e.g., 'search the web', 'extract entities', 'send notifications')"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of tools to return",
                            "default": 5
                        },
                        "min_score": {
                            "type": "number",
                            "description": "Minimum relevance score (0-1)",
                            "default": 0.5
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_tool",
                "description": "Get detailed information about a specific tool by name, including its full schema, parameters, and usage examples.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tool_name": {
                            "type": "string",
                            "description": "Exact name of the tool to retrieve"
                        }
                    },
                    "required": ["tool_name"]
                }
            },
            {
                "name": "execute_tool",
                "description": "Execute a specific tool by name with provided arguments. Use search_tools first to find the right tool.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tool_name": {
                            "type": "string",
                            "description": "Name of the tool to execute"
                        },
                        "arguments": {
                            "type": "object",
                            "description": "Arguments to pass to the tool"
                        }
                    },
                    "required": ["tool_name", "arguments"]
                }
            },
            {
                "name": "compose_workflow",
                "description": "Compose multiple tools into a workflow (sequential, parallel, map-reduce, etc.). Tools are referenced by name and executed lazily.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "workflow_type": {
                            "type": "string",
                            "enum": ["sequential", "parallel", "map", "reduce", "map_reduce", "segment_process", "pipe"],
                            "description": "Type of workflow composition"
                        },
                        "tools": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of tool names to compose"
                        },
                        "arguments": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "Arguments for each tool in the workflow"
                        },
                        "inputs": {
                            "type": "object",
                            "description": "Initial inputs to the workflow"
                        }
                    },
                    "required": ["workflow_type", "tools", "inputs"]
                }
            },
            {
                "name": "list_tool_categories",
                "description": "List available tool categories and counts. Use this to understand the tool landscape before searching.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "get_registry_stats",
                "description": "Get statistics about the tool registry: total tools, categories, MCP servers, recent activity.",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "list_tools_paginated",
                "description": "List all tools with pagination. Returns tool details in pages to avoid context overflow. Use offset/limit to navigate pages.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "offset": {
                            "type": "integer",
                            "description": "Starting index (0-based)",
                            "default": 0
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of tools per page",
                            "default": 20
                        },
                        "category": {
                            "type": "string",
                            "description": "Filter by category (optional)"
                        }
                    }
                }
            },
            {
                "name": "list_tool_names",
                "description": "Get a lightweight list of all tool names, optionally filtered by category. Returns just names without full schemas.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "description": "Filter by category (optional)"
                        }
                    }
                }
            }
        ]

        # Handle pagination (though we only have a few meta-tools)
        page_size = 100
        start = 0
        if cursor:
            try:
                start = int(cursor)
            except:
                start = 0

        end = min(start + page_size, len(meta_tools))
        tools_page = meta_tools[start:end]

        result = {"tools": tools_page}

        if end < len(meta_tools):
            result["nextCursor"] = str(end)

        return result

    async def _handle_tools_call(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Handle tools/call request - routes meta-tools to appropriate endpoints"""
        import httpx
        import json as json_module

        tool_name = params.get("name")
        tool_args = params.get("arguments", {})

        # Route meta-tools to appropriate endpoints
        if tool_name == "search_tools":
            # Search tools from persistent store
            try:
                client = get_http_client()
                query = tool_args.get("query", "")
                limit = tool_args.get("limit", 20)
                # Use the tools list endpoint and filter client-side
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/?limit={limit}",
                    timeout=10.0
                )

                if response.status_code == 200:
                    data = response.json()
                    tools = data.get("tools", [])
                    # Filter by query if provided
                    if query:
                        query_lower = query.lower()
                        tools = [t for t in tools if query_lower in t.get("name", "").lower() or query_lower in t.get("description", "").lower()]
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({"tools": tools, "count": len(tools)}, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Tool search failed with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"search_tools failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "get_tool":
            # Get specific tool details - accept "name" or "tool_name"
            try:
                target_name = tool_args.get("name") or tool_args.get("tool_name")
                client = get_http_client()
                # Use the list endpoint and filter to find the tool
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/?limit=100",
                    timeout=10.0
                )

                if response.status_code == 200:
                    data = response.json()
                    tools = data.get("tools", [])
                    # Find matching tool by name or tool_id
                    matching = [t for t in tools if t.get("name") == target_name or t.get("tool_id") == target_name]
                    if matching:
                        return {
                            "content": [
                                {
                                    "type": "text",
                                    "text": json_module.dumps(matching[0], indent=2)
                                }
                            ],
                            "isError": False
                        }
                    else:
                        return {
                            "content": [
                                {
                                    "type": "text",
                                    "text": json_module.dumps({
                                        "error": f"Tool not found: {target_name}",
                                        "available_tools": [t.get("name") for t in tools[:10]]
                                    }, indent=2)
                                }
                            ],
                            "isError": True
                        }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Failed to get tool with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"get_tool failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "execute_tool":
            # Execute actual tool via persistent tools API
            try:
                # Accept either "tool" or "tool_name" parameter
                target_tool = tool_args.get("tool") or tool_args.get("tool_name")
                client = get_http_client()
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/tools/execute",
                    json={
                        "tool": target_tool,
                        "arguments": tool_args.get("arguments", {})
                    },
                    timeout=60.0
                )

                if response.status_code == 200:
                    result = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Tool execution failed with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"execute_tool failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "compose_workflow":
            # Execute workflow composition
            try:
                client = get_http_client()
                response = await client.post(
                    f"{API_BASE_URL}/compose/compose-simple",
                    json={
                        "workflow_type": tool_args.get("workflow_type"),
                        "tools": tool_args.get("tools", []),
                        "arguments": tool_args.get("arguments", []),
                        "inputs": tool_args.get("inputs", {})
                    },
                    timeout=60.0
                )

                if response.status_code == 200:
                    result = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Workflow composition failed with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"compose_workflow failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "list_tool_categories":
            # List available categories with detailed breakdown
            try:
                client = get_http_client()
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/admin/tool-categories",
                    timeout=10.0
                )

                if response.status_code == 200:
                    result = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Failed to list categories with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"list_tool_categories failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "get_registry_stats":
            # Get registry statistics
            try:
                client = get_http_client()
                response = await client.get(
                    f"{API_BASE_URL}/health",
                    timeout=5.0
                )

                if response.status_code == 200:
                    stats = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(stats, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Failed to get stats with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"get_registry_stats failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "list_tools_paginated":
            # List tools with pagination
            try:
                client = get_http_client()
                offset = tool_args.get("offset", 0)
                limit = tool_args.get("limit", 20)
                category = tool_args.get("category")

                params = {"offset": offset, "limit": limit}
                if category:
                    params["category"] = category

                response = await client.get(
                    f"{API_BASE_URL}/api/v1/admin/list-all-tools",
                    params=params,
                    timeout=10.0
                )

                if response.status_code == 200:
                    result = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Failed to list tools with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"list_tools_paginated failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "list_tool_names":
            # Get lightweight list of tool names
            try:
                client = get_http_client()
                params = {}
                if tool_args.get("category"):
                    params["category"] = tool_args["category"]

                response = await client.get(
                    f"{API_BASE_URL}/api/v1/admin/list-tool-names",
                    params=params,
                    timeout=10.0
                )

                if response.status_code == 200:
                    result = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Failed to list tool names with status {response.status_code}",
                                    "details": response.text
                                }, indent=2)
                            }
                        ],
                        "isError": True
                    }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": f"list_tool_names failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        # Unknown meta-tool
        return {
            "content": [
                {
                    "type": "text",
                    "text": json_module.dumps({
                        "error": f"Unknown meta-tool: {tool_name}",
                        "available_meta_tools": [
                            "search_tools",
                            "get_tool",
                            "execute_tool",
                            "compose_workflow",
                            "list_tool_categories",
                            "get_registry_stats"
                        ]
                    }, indent=2)
                }
            ],
            "isError": True
        }

    async def _handle_resources_list(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Handle resources/list request"""
        # Create resources from tools that have UI effects or data outputs
        resources = []

        for tool in self.tool_storage.values():
            if tool.get("has_ui_effects"):
                resource = self.resource_adapter.create_resource(
                    uri=f"tool://{tool['id']}/ui",
                    name=f"{tool['name']} UI",
                    description=f"UI output from {tool['name']}",
                    mime_type="text/html",
                    metadata={"tool_id": tool["id"], "type": "ui"}
                )
                resources.append(resource)

            # Add tool configuration as a resource
            resource = self.resource_adapter.create_resource(
                uri=f"tool://{tool['id']}/config",
                name=f"{tool['name']} Configuration",
                description=f"Configuration for {tool['name']}",
                mime_type="application/json",
                metadata={"tool_id": tool["id"], "type": "config"}
            )
            resources.append(resource)

        return {"resources": resources}

    async def _handle_resources_read(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Handle resources/read request"""
        uri = params.get("uri")

        if not uri or not uri.startswith("tool://"):
            raise ValueError(f"Invalid resource URI: {uri}")

        # Parse URI to get tool ID and resource type
        parts = uri.replace("tool://", "").split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid resource URI format: {uri}")

        tool_id = parts[0]
        resource_type = parts[1]

        # Find the tool
        tool = self.tool_storage.get(tool_id)
        if not tool:
            raise ValueError(f"Tool '{tool_id}' not found")

        # Return appropriate content based on resource type
        if resource_type == "config":
            content = self.resource_adapter.create_resource_contents(
                uri=uri,
                content=tool,
                mime_type="application/json"
            )
        elif resource_type == "ui":
            content = self.resource_adapter.create_resource_contents(
                uri=uri,
                content=f"<html><body><h1>{tool['name']} UI</h1></body></html>",
                mime_type="text/html"
            )
        else:
            raise ValueError(f"Unknown resource type: {resource_type}")

        return {"contents": [content]}

    async def _handle_resources_subscribe(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Handle resources/subscribe request"""
        uri = params.get("uri")

        if uri not in self.subscriptions:
            self.subscriptions[uri] = []

        if session_id not in self.subscriptions[uri]:
            self.subscriptions[uri].append(session_id)

        return {}

    async def _handle_resources_unsubscribe(self, params: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Handle resources/unsubscribe request"""
        uri = params.get("uri")

        if uri in self.subscriptions and session_id in self.subscriptions[uri]:
            self.subscriptions[uri].remove(session_id)

        return {}

    async def notify_tool_changed(self, tool_id: str):
        """Notify clients about tool changes"""
        notification = MCPNotification(
            method="notifications/tools/listChanged",
            params={}
        )

        # Send to all connected sessions
        for session_id in self.sessions:
            # In real implementation, would send via WebSocket
            logger.info(f"Would send tool change notification to session {session_id}")

    async def notify_resource_updated(self, uri: str, contents: Any):
        """Notify subscribed clients about resource updates"""
        if uri in self.subscriptions:
            notification = MCPNotification(
                method="notifications/resources/updated",
                params={
                    "uri": uri,
                    "contents": contents
                }
            )

            for session_id in self.subscriptions[uri]:
                # In real implementation, would send via WebSocket
                logger.info(f"Would send resource update to session {session_id}")


class MCPWebSocketHandler:
    """WebSocket handler for MCP protocol"""

    def __init__(self, mcp_server: MCPServer):
        self.mcp_server = mcp_server
        self.connections: Dict[str, WebSocket] = {}

    async def handle_websocket(self, websocket: WebSocket):
        """Handle WebSocket connection for MCP"""
        await websocket.accept()
        session_id = str(uuid.uuid4())
        self.connections[session_id] = websocket

        try:
            while True:
                # Receive message
                data = await websocket.receive_text()
                message = json.loads(data)

                # Determine message type
                if "id" in message:
                    # It's a request
                    request = MCPRequest(**message)
                    response = await self.mcp_server.handle_request(request, session_id)
                    await websocket.send_text(response.model_dump_json())
                else:
                    # It's a notification
                    notification = MCPNotification(**message)
                    await self.mcp_server.handle_notification(notification, session_id)

        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected for session {session_id}")
        except Exception as e:
            logger.error(f"WebSocket error for session {session_id}: {e}")
        finally:
            if session_id in self.connections:
                del self.connections[session_id]
            if session_id in self.mcp_server.sessions:
                del self.mcp_server.sessions[session_id]

    async def send_notification(self, session_id: str, notification: MCPNotification):
        """Send notification to specific session"""
        if session_id in self.connections:
            websocket = self.connections[session_id]
            await websocket.send_text(notification.model_dump_json())

    async def broadcast_notification(self, notification: MCPNotification):
        """Broadcast notification to all sessions"""
        for session_id in self.connections:
            await self.send_notification(session_id, notification)