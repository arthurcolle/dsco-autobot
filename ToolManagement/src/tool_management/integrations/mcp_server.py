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

# Output guard for enforcing size limits and URL capture
try:
    from tool_management.guards.output_guard import guard_mcp_content, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False
    logger.info("Output guard not available")

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
            timeout=httpx.Timeout(None, connect=15.0),  # No read/write timeout
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

            # Apply output guard on tools/call results (enforce size, cap links, capture URLs)
            if request.method == "tools/call" and OUTPUT_GUARD_AVAILABLE and isinstance(result, dict):
                tool_name = (request.params or {}).get("name", "")
                result = guard_mcp_content(
                    result,
                    tool_name=tool_name,
                    source_context={"server": "mcp-meta", "session": session_id},
                )

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
                            "default": 0.3
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
                        },
                        "execution_mode": {
                            "type": "string",
                            "enum": ["auto", "sync", "stream", "job"],
                            "description": "Execution path selector. auto uses latency-aware sync with timeout escalation.",
                            "default": "auto"
                        },
                        "stream": {
                            "type": "boolean",
                            "description": "Request streaming mode. MCP returns a detached/job handle for stream requests.",
                            "default": False
                        },
                        "early_stop": {
                            "type": "object",
                            "description": "Advisory early-stop policy used for streaming calls."
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "description": "Tool execution timeout in milliseconds",
                            "default": 30000
                        },
                        "sync_timeout_ms": {
                            "type": "integer",
                            "description": "Auto-mode sync attempt budget before escalation to job",
                            "default": 5000
                        },
                        "idempotency_key": {
                            "type": "string",
                            "description": "Idempotency key for write operations to prevent duplicate side effects"
                        },
                        "priority": {
                            "type": "string",
                            "description": "Queue priority: p0|p1|p2|p3 (or critical|high|normal|background)",
                            "default": "p1"
                        },
                        "dedupe_window_seconds": {
                            "type": "integer",
                            "description": "Deduplicate identical requests in this window",
                            "default": 60
                        },
                        "dedupe_key": {
                            "type": "string",
                            "description": "Optional explicit dedupe key"
                        },
                        "max_output_chars": {
                            "type": "integer",
                            "description": "Maximum output size in characters (default 32000). Outputs exceeding this are truncated.",
                            "default": 32000
                        },
                        "max_links": {
                            "type": "integer",
                            "description": "Maximum number of URLs/links to include in output (default 50). Extra URLs are still captured for background processing.",
                            "default": 50
                        }
                    },
                    "required": ["tool_name", "arguments"]
                }
            },
            {
                "name": "batch_execute",
                "description": "Execute multiple independent tool calls in one request. Supports parallel or sequential execution and returns results in input order.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "calls": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "tool_name": {"type": "string"},
                                    "tool": {"type": "string"},
                                    "arguments": {"type": "object"},
                                    "execution_mode": {
                                        "type": "string",
                                        "enum": ["auto", "sync", "stream", "job"],
                                        "default": "auto"
                                    },
                                    "idempotency_key": {"type": "string"},
                                    "priority": {"type": "string", "default": "p1"},
                                    "dedupe_window_seconds": {"type": "integer", "default": 60},
                                    "dedupe_key": {"type": "string"}
                                }
                            },
                            "description": "Tool calls to execute"
                        },
                        "execution": {
                            "type": "string",
                            "enum": ["parallel", "sequential"],
                            "default": "parallel"
                        }
                    },
                    "required": ["calls"]
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
            },
            {
                "name": "batch_get_tools",
                "description": "Fetch multiple tools by name in a single call. Returns full schemas or compact signatures. Use this after search_tools to get details for several tools at once.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "names": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of exact tool names to fetch"
                        },
                        "compact": {
                            "type": "boolean",
                            "description": "If true, return compact signatures instead of full JSON schemas",
                            "default": True
                        }
                    },
                    "required": ["names"]
                }
            },
            {
                "name": "query_tools",
                "description": "Advanced tool query with text search, sorting, tag filtering, and compact mode. More powerful than search_tools for structured filtering.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "search": {
                            "type": "string",
                            "description": "Text search across tool names and descriptions"
                        },
                        "backend_id": {
                            "type": "string",
                            "description": "Filter by backend/server ID"
                        },
                        "category": {
                            "type": "string",
                            "description": "Filter by category"
                        },
                        "tags": {
                            "type": "string",
                            "description": "Comma-separated tag list (e.g., 'web,api,search')"
                        },
                        "sort_by": {
                            "type": "string",
                            "enum": ["name", "call_count", "updated_at", "avg_latency_ms", "error_count"],
                            "description": "Sort results by this field",
                            "default": "name"
                        },
                        "sort_order": {
                            "type": "string",
                            "enum": ["asc", "desc"],
                            "default": "asc"
                        },
                        "limit": {
                            "type": "integer",
                            "default": 20
                        },
                        "offset": {
                            "type": "integer",
                            "default": 0
                        }
                    }
                }
            },
            {
                "name": "submit_job",
                "description": "Submit a long-running tool as a background job. Returns a job_id immediately that you can poll with get_job_status. Use this for tools that may take >30s (e.g., deep_search).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tool_name": {
                            "type": "string",
                            "description": "Tool to execute (e.g., 'deep_search', 'backend:tool')"
                        },
                        "arguments": {
                            "type": "object",
                            "description": "Arguments to pass to the tool"
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Max execution time in seconds",
                            "default": 300
                        },
                        "priority": {
                            "type": "string",
                            "description": "Queue priority tier",
                            "default": "p1"
                        },
                        "dedupe_window_seconds": {
                            "type": "integer",
                            "description": "Deduplicate identical requests in this window",
                            "default": 60
                        },
                        "dedupe_key": {
                            "type": "string",
                            "description": "Optional explicit dedupe key"
                        }
                    },
                    "required": ["tool_name", "arguments"]
                }
            },
            {
                "name": "get_job_status",
                "description": "Poll the status and results of a previously submitted job. Returns status, progress percentage, partial results, and final result when complete.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "string",
                            "description": "The job_id returned by submit_job"
                        }
                    },
                    "required": ["job_id"]
                }
            },
            {
                "name": "cancel_job",
                "description": "Cancel a running job by its job_id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "job_id": {
                            "type": "string",
                            "description": "The job_id to cancel"
                        }
                    },
                    "required": ["job_id"]
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
            # Semantic search via multi-faceted embeddings (Jina v3)
            try:
                client = get_http_client()
                query = tool_args.get("query", "")
                limit = tool_args.get("limit", 10)
                min_score = tool_args.get("min_score", 0.3)
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/discover/search",
                    json={"query": query, "limit": limit},
                    timeout=15.0
                )

                if response.status_code == 200:
                    data = response.json()
                    tools = data.get("results", [])
                    # Filter by min_score
                    if min_score and min_score > 0:
                        tools = [t for t in tools if t.get("score", 0) >= min_score]

                    # Enrich with compact signatures via batch-get
                    tool_names = [t.get("name") for t in tools if t.get("name")]
                    if tool_names:
                        try:
                            sig_resp = await client.post(
                                f"{API_BASE_URL}/api/v1/tools/batch-get",
                                json={"names": tool_names, "compact": True},
                                timeout=10.0
                            )
                            if sig_resp.status_code == 200:
                                sig_data = sig_resp.json()
                                sig_map = {s["name"]: s.get("signature", "") for s in sig_data.get("tools", [])}
                                for t in tools:
                                    t["signature"] = sig_map.get(t.get("name"), "")
                        except Exception:
                            pass  # signatures are optional enrichment

                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "tools": tools,
                                    "count": len(tools),
                                    "query": query,
                                    "search_type": "semantic_embedding",
                                }, indent=2)
                            }
                        ],
                        "isError": False
                    }
                else:
                    # Fallback: keyword search against DuckDB tools list (paginated)
                    fallback = await client.get(
                        f"{API_BASE_URL}/api/v1/tools/",
                        params={"limit": 500, "offset": 0},
                        timeout=10.0
                    )
                    if fallback.status_code == 200:
                        fb_data = fallback.json()
                        all_tools = fb_data.get("tools", [])
                        if query:
                            q = query.lower()
                            words = q.split()
                            scored = []
                            for t in all_tools:
                                text = f"{t.get('name','')} {t.get('description','')}".lower()
                                score = sum(1 for w in words if w in text)
                                if score > 0:
                                    scored.append({**t, "score": round(score / len(words), 2)})
                            scored.sort(key=lambda x: x["score"], reverse=True)
                            all_tools = scored[:limit]
                        else:
                            all_tools = all_tools[:limit]
                        return {
                            "content": [
                                {
                                    "type": "text",
                                    "text": json_module.dumps({
                                        "tools": all_tools,
                                        "count": len(all_tools),
                                        "total": fb_data.get("total", len(all_tools)),
                                        "query": query,
                                        "search_type": "keyword_fallback",
                                    }, indent=2)
                                }
                            ],
                            "isError": False
                        }
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "error": f"Tool search failed with status {response.status_code}",
                                    "details": response.text[:200]
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
            # Get specific tool details from DuckDB store via direct lookup
            try:
                target_name = tool_args.get("name") or tool_args.get("tool_name")
                client = get_http_client()

                # Fetch full tool data
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/{target_name}",
                    timeout=10.0
                )

                if response.status_code == 200:
                    tool_data = response.json()
                    # Enrich with compact signature
                    try:
                        schema_resp = await client.get(
                            f"{API_BASE_URL}/api/v1/tools/schema/{target_name}",
                            timeout=5.0
                        )
                        if schema_resp.status_code == 200:
                            schema_data = schema_resp.json()
                            tool_data["signature"] = schema_data.get("signature")
                            tool_data["parameters_summary"] = schema_data.get("parameters")
                    except Exception:
                        pass  # signature is optional enrichment
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(tool_data, indent=2)
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
                                    "hint": "Use search_tools to find available tools by description"
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
            # Execution-mode aware dispatch through the streaming-first execute API.
            try:
                target_tool = tool_args.get("tool") or tool_args.get("tool_name")
                arguments = tool_args.get("arguments", {})
                execution_mode = tool_args.get("execution_mode", "auto")
                stream_requested = bool(tool_args.get("stream", False))
                timeout_ms = int(tool_args.get("timeout_ms", 30000))
                sync_timeout_ms = int(tool_args.get("sync_timeout_ms", 5000))
                idempotency_key = tool_args.get("idempotency_key")
                priority = tool_args.get("priority", "p1")
                dedupe_window_seconds = int(tool_args.get("dedupe_window_seconds", 60))
                dedupe_key = tool_args.get("dedupe_key")
                early_stop = tool_args.get("early_stop")

                # Per-call output limits forwarded to job executor
                if tool_args.get("max_output_chars"):
                    arguments["_max_output_chars"] = tool_args["max_output_chars"]
                if tool_args.get("max_links"):
                    arguments["_max_links"] = tool_args["max_links"]

                # MCP call returns JSON, not live SSE. For stream requests, run as detached job.
                effective_mode = execution_mode
                if stream_requested or execution_mode == "stream":
                    effective_mode = "job"

                client = get_http_client()
                exec_response = await client.post(
                    f"{API_BASE_URL}/api/v1/execute",
                    json={
                        "tool_name": target_tool,
                        "arguments": arguments,
                        "execution_mode": effective_mode,
                        "stream": False,
                        "timeout_ms": timeout_ms,
                        "sync_timeout_ms": sync_timeout_ms,
                        "idempotency_key": idempotency_key,
                        "priority": priority,
                        "dedupe_window_seconds": dedupe_window_seconds,
                        "dedupe_key": dedupe_key,
                        "early_stop": early_stop,
                    },
                )

                if exec_response.status_code == 200:
                    exec_data = exec_response.json()
                    status = exec_data.get("status")
                    job_id = exec_data.get("job_id") or exec_data.get("call_id")

                    if status in {"submitted", "running_background"} and job_id:
                        mode_text = "stream-detached" if (stream_requested or execution_mode == "stream") else "job"
                        return {
                            "content": [
                                {
                                    "type": "text",
                                    "text": json_module.dumps({
                                        "status": status,
                                        "execution_mode": mode_text,
                                        "job_id": job_id,
                                        "message": (
                                            f"Tool '{target_tool}' is running as job '{job_id}'. "
                                            f"Call get_job_status with job_id='{job_id}' to retrieve results."
                                        ),
                                        "poll_url": exec_data.get("poll_url"),
                                        "stream_url": exec_data.get("stream_url"),
                                        "deduplicated": exec_data.get("deduplicated", False),
                                        "priority": exec_data.get("priority"),
                                    }, indent=2)
                                }
                            ],
                            "isError": False
                        }

                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({
                                    "status": status or "completed",
                                    "execution_mode": execution_mode,
                                    "tool": target_tool,
                                    "result": exec_data.get("result"),
                                    "latency_ms": exec_data.get("latency_ms"),
                                    "idempotent_replay": exec_data.get("idempotent_replay", False),
                                    "deduplicated": exec_data.get("deduplicated", False),
                                    "priority": exec_data.get("priority"),
                                }, indent=2)
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
                                    "error": {
                                        "code": "EXECUTE_FAILED",
                                        "message": f"execute_tool failed with HTTP {exec_response.status_code}",
                                        "category": "transient",
                                        "retryable": True,
                                        "details": exec_response.text[:500],
                                    }
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

        elif tool_name == "batch_execute":
            # Execute multiple tools in one request; results are returned in input order.
            try:
                client = get_http_client()
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/execute/batch",
                    json={
                        "calls": tool_args.get("calls", []),
                        "execution": tool_args.get("execution", "parallel"),
                    },
                    timeout=35.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2),
                            }
                        ],
                        "isError": False,
                    }
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": {
                                    "code": "BATCH_EXECUTE_FAILED",
                                    "message": f"batch_execute failed with status {response.status_code}",
                                    "category": "transient",
                                    "retryable": True,
                                    "details": response.text[:300],
                                }
                            }, indent=2)
                        }
                    ],
                    "isError": True,
                }
            except Exception as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": json_module.dumps({
                                "error": {
                                    "code": "BATCH_EXECUTE_FAILED",
                                    "message": f"batch_execute failed: {str(e)}",
                                    "category": "transient",
                                    "retryable": True,
                                }
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "compose_workflow":
            # Execute workflow composition — no timeout
            try:
                client = get_http_client()
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/compose/compose-simple",
                    json={
                        "workflow_type": tool_args.get("workflow_type"),
                        "tools": tool_args.get("tools", []),
                        "arguments": tool_args.get("arguments", []),
                        "inputs": tool_args.get("inputs", {})
                    },
                )

                if response.status_code == 200:
                    result = response.json()
                    is_error = result.get("status") == "failed" or "error" in result
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps(result, indent=2)
                            }
                        ],
                        "isError": is_error
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
            # List backends and their tools from DuckDB store
            try:
                client = get_http_client()
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/backends",
                    timeout=10.0
                )

                if response.status_code == 200:
                    backends = response.json()
                    categories = {}
                    for b in backends:
                        bid = b.get("backend_id", "unknown")
                        categories[bid] = {
                            "name": b.get("name"),
                            "tool_count": b.get("tool_count", 0),
                            "healthy": b.get("is_healthy", False),
                            "url": b.get("url"),
                        }
                    result = {
                        "backends": categories,
                        "total_backends": len(backends),
                        "total_tools": sum(b.get("tool_count", 0) for b in backends),
                    }
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
            # Get registry statistics from DuckDB store
            try:
                client = get_http_client()
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/stats/overview",
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
            # List tools with pagination from DuckDB store
            try:
                client = get_http_client()
                offset = tool_args.get("offset", 0)
                limit = tool_args.get("limit", 20)
                category = tool_args.get("category")

                params = {"limit": limit, "offset": offset}
                if category:
                    params["category"] = category

                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/",
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
            # Get lightweight list of tool names from DuckDB store
            try:
                client = get_http_client()
                params = {"limit": 500, "offset": 0}
                if tool_args.get("category"):
                    params["category"] = tool_args["category"]

                response = await client.get(
                    f"{API_BASE_URL}/api/v1/tools/",
                    params=params,
                    timeout=10.0
                )

                if response.status_code == 200:
                    data = response.json()
                    tools = data.get("tools", [])
                    total = data.get("total", len(tools))
                    # Group by backend
                    by_backend = {}
                    for t in tools:
                        bid = t.get("backend", "unknown")
                        by_backend.setdefault(bid, []).append(t.get("name"))
                    result = {
                        "tool_names": [t.get("name") for t in tools],
                        "count": len(tools),
                        "total": total,
                        "by_backend": by_backend,
                    }
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

        elif tool_name == "batch_get_tools":
            # Fetch multiple tools by name in one call
            try:
                client = get_http_client()
                names = tool_args.get("names", [])
                compact = tool_args.get("compact", True)
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/tools/batch-get",
                    json={"names": names, "compact": compact},
                    timeout=15.0
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
                                    "error": f"batch_get_tools failed with status {response.status_code}",
                                    "details": response.text[:200]
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
                                "error": f"batch_get_tools failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "query_tools":
            # Advanced query with text search, sorting, tag filtering
            try:
                client = get_http_client()
                params = {}
                for key in ("search", "backend_id", "category", "tags", "sort_by", "sort_order"):
                    if tool_args.get(key):
                        params[key] = tool_args[key]
                params["limit"] = tool_args.get("limit", 20)
                params["offset"] = tool_args.get("offset", 0)
                params["compact"] = True  # Always compact for MCP to save context

                response = await client.post(
                    f"{API_BASE_URL}/api/v1/tools/query",
                    params=params,
                    timeout=15.0
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
                                    "error": f"query_tools failed with status {response.status_code}",
                                    "details": response.text[:200]
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
                                "error": f"query_tools failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "submit_job":
            # Submit a long-running tool as a background job
            try:
                client = get_http_client()
                target_tool = tool_args.get("tool_name") or tool_args.get("tool")
                arguments = tool_args.get("arguments", {})
                timeout = tool_args.get("timeout", 300)
                priority = tool_args.get("priority", "p1")
                dedupe_window_seconds = int(tool_args.get("dedupe_window_seconds", 60))
                dedupe_key = tool_args.get("dedupe_key")
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/jobs/submit",
                    json={
                        "tool": target_tool,
                        "arguments": arguments,
                        "timeout": timeout,
                        "priority": priority,
                        "dedupe_window_seconds": dedupe_window_seconds,
                        "dedupe_key": dedupe_key,
                    },
                    timeout=10.0,
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
                                    "error": f"submit_job failed with status {response.status_code}",
                                    "details": response.text[:300]
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
                                "error": f"submit_job failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "get_job_status":
            # Poll the status of a submitted job
            try:
                client = get_http_client()
                job_id = tool_args.get("job_id")
                if not job_id:
                    return {
                        "content": [{"type": "text", "text": json_module.dumps({"error": "job_id is required"})}],
                        "isError": True,
                    }
                response = await client.get(
                    f"{API_BASE_URL}/api/v1/jobs/{job_id}",
                    timeout=10.0,
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
                elif response.status_code == 404:
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json_module.dumps({"error": f"Job not found: {job_id}"}, indent=2)
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
                                    "error": f"get_job_status failed with status {response.status_code}",
                                    "details": response.text[:300]
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
                                "error": f"get_job_status failed: {str(e)}"
                            }, indent=2)
                        }
                    ],
                    "isError": True
                }

        elif tool_name == "cancel_job":
            # Cancel a running job
            try:
                client = get_http_client()
                job_id = tool_args.get("job_id")
                if not job_id:
                    return {
                        "content": [{"type": "text", "text": json_module.dumps({"error": "job_id is required"})}],
                        "isError": True,
                    }
                response = await client.post(
                    f"{API_BASE_URL}/api/v1/jobs/{job_id}/cancel",
                    timeout=10.0,
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
                                    "error": f"cancel_job failed with status {response.status_code}",
                                    "details": response.text[:300]
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
                                "error": f"cancel_job failed: {str(e)}"
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
                            "batch_execute",
                            "compose_workflow",
                            "list_tool_categories",
                            "get_registry_stats",
                            "list_tools_paginated",
                            "list_tool_names",
                            "batch_get_tools",
                            "query_tools",
                            "submit_job",
                            "get_job_status",
                            "cancel_job"
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
