#!/usr/bin/env python3
"""
Registry Wrapper MCP - MCP interface to the Tool Registry

Provides MCP tools to interact with the central tool registry on port 2016.
Allows retrieving tools, searching, and executing tools through the registry.

Run with:
    python -m native.registry_wrapper_mcp.server --port 4005
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase, CapabilityType

logger = logging.getLogger(__name__)


class RegistryWrapperMCP(NativeMCPBase):
    """
    MCP wrapper for the central tool registry.

    Provides tools to query and interact with the tool registry at port 2016.
    """

    SERVICE_ID = "registry-wrapper-mcp"
    SERVICE_NAME = "Registry Wrapper MCP"
    VERSION = "1.0.0"

    # Capabilities
    CAPABILITIES = [
        "query",
        "registry",
        "tool_discovery",
    ]

    REQUIRES = []
    DEPENDS = []

    # Tool definitions
    TOOLS = [
        {
            "name": "list_tools",
            "description": "List all tools available in the registry with optional filtering by category or tags.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Filter by category"
                    },
                    "tags": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Filter by tags"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of tools to return",
                        "default": 100
                    }
                }
            }
        },
        {
            "name": "get_tool",
            "description": "Get detailed information about a specific tool by name.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Tool name"
                    }
                },
                "required": ["name"]
            }
        },
        {
            "name": "search_tools",
            "description": "Search for tools by query string across names and descriptions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results",
                        "default": 20
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "execute_tool",
            "description": "Execute a tool from the registry with given arguments.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": "Name of the tool to execute"
                    },
                    "arguments": {
                        "type": "object",
                        "description": "Tool arguments"
                    }
                },
                "required": ["tool_name"]
            }
        },
        {
            "name": "list_native_mcps",
            "description": "List all native MCP servers registered with the registry.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "include_tools": {
                        "type": "boolean",
                        "description": "Include tool details for each MCP",
                        "default": False
                    }
                }
            }
        },
        {
            "name": "get_registry_stats",
            "description": "Get statistics about the tool registry.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "list_services",
            "description": "List all registered services in the service registry.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "capability": {
                        "type": "string",
                        "description": "Filter by capability"
                    }
                }
            }
        }
    ]

    def __init__(
        self,
        port: int = 4005,
        registry_url: Optional[str] = None,
        **kwargs
    ):
        super().__init__(port=port, **kwargs)

        self.registry_url = registry_url or os.getenv("REGISTRY_URL", "http://localhost:2016")
        self._client: Optional[httpx.AsyncClient] = None

        # Lifecycle hooks
        self.on_start(self._on_start)
        self.on_stop(self._on_stop)

    async def _on_start(self):
        """Initialize HTTP client."""
        self._client = httpx.AsyncClient(timeout=30.0)
        logger.info(f"Registry wrapper connected to {self.registry_url}")

    async def _on_stop(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # TOOL HANDLERS
    # =========================================================================

    async def _tool_list_tools(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List all tools in the registry."""
        try:
            params = {}
            if args.get("category"):
                params["category"] = args["category"]
            if args.get("tags"):
                params["tags"] = ",".join(args["tags"])

            response = await self._client.get(
                f"{self.registry_url}/api/v1/tools",
                params=params
            )

            if response.status_code == 200:
                data = response.json()
                # API returns a list directly, not {"tools": [...]}
                if isinstance(data, list):
                    tools = data
                else:
                    tools = data.get("tools", [])

                limit = args.get("limit", 100)
                if len(tools) > limit:
                    tools = tools[:limit]

                return self._text_result({
                    "tools": tools,
                    "count": len(tools),
                    "total": len(tools),
                    "registry_url": self.registry_url,
                    "success": True
                })
            else:
                return self._error(f"Registry request failed: {response.status_code}")

        except Exception as e:
            return self._error(str(e))

    async def _tool_get_tool(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get a specific tool by name."""
        tool_name = args.get("name")
        if not tool_name:
            return self._error("name is required")

        try:
            response = await self._client.get(
                f"{self.registry_url}/api/v1/tools/{tool_name}"
            )

            if response.status_code == 200:
                tool = response.json()
                return self._text_result({
                    "tool": tool,
                    "success": True
                })
            elif response.status_code == 404:
                return self._error(f"Tool not found: {tool_name}")
            else:
                return self._error(f"Registry request failed: {response.status_code}")

        except Exception as e:
            return self._error(str(e))

    async def _tool_search_tools(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Search for tools."""
        query = args.get("query")
        if not query:
            return self._error("query is required")

        limit = args.get("limit", 20)

        try:
            response = await self._client.get(
                f"{self.registry_url}/api/v1/tools/search",
                params={"q": query, "limit": limit}
            )

            if response.status_code == 200:
                data = response.json()
                return self._text_result({
                    "query": query,
                    "results": data.get("results", []),
                    "count": len(data.get("results", [])),
                    "success": True
                })
            else:
                return self._error(f"Search failed: {response.status_code}")

        except Exception as e:
            return self._error(str(e))

    async def _tool_execute_tool(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool through the registry."""
        tool_name = args.get("tool_name")
        if not tool_name:
            return self._error("tool_name is required")

        arguments = args.get("arguments", {})

        try:
            response = await self._client.post(
                f"{self.registry_url}/api/v1/tools/{tool_name}/execute",
                json={"arguments": arguments}
            )

            if response.status_code == 200:
                result = response.json()
                return self._text_result({
                    "tool_name": tool_name,
                    "result": result,
                    "success": True
                })
            else:
                error_detail = response.text[:200]
                return self._error(f"Tool execution failed ({response.status_code}): {error_detail}")

        except Exception as e:
            return self._error(str(e))

    async def _tool_list_native_mcps(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List native MCP servers."""
        include_tools = args.get("include_tools", False)

        try:
            response = await self._client.get(
                f"{self.registry_url}/api/v1/native"
            )

            if response.status_code == 200:
                data = response.json()
                # The API returns {"servers": {...}} not {"mcps": [...]}
                servers = data.get("servers", {})

                # Convert dict to list
                mcps = list(servers.values())

                if not include_tools:
                    # Strip out tool details to reduce payload
                    mcps = [
                        {k: v for k, v in mcp.items() if k != "tools"}
                        for mcp in mcps
                    ]

                return self._text_result({
                    "mcps": mcps,
                    "count": len(mcps),
                    "success": True
                })
            else:
                return self._error(f"Failed to list MCPs: {response.status_code}")

        except Exception as e:
            return self._error(str(e))

    async def _tool_get_registry_stats(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get registry statistics."""
        try:
            # Get tools count
            tools_response = await self._client.get(
                f"{self.registry_url}/api/v1/tools"
            )

            # Get native MCPs
            mcps_response = await self._client.get(
                f"{self.registry_url}/api/v1/native"
            )

            # Get health
            health_response = await self._client.get(
                f"{self.registry_url}/health"
            )

            stats = {
                "registry_url": self.registry_url,
                "healthy": health_response.status_code == 200,
                "total_tools": 0,
                "total_mcps": 0,
            }

            if tools_response.status_code == 200:
                tools_data = tools_response.json()
                # API returns a list directly
                if isinstance(tools_data, list):
                    stats["total_tools"] = len(tools_data)
                else:
                    stats["total_tools"] = len(tools_data.get("tools", []))

            if mcps_response.status_code == 200:
                mcps_data = mcps_response.json()
                # The API returns {"servers": {...}, ...} not {"mcps": [...]}
                servers = mcps_data.get("servers", {})
                stats["total_mcps"] = len(servers)

            if health_response.status_code == 200:
                health_data = health_response.json()
                stats["health"] = health_data

            return self._text_result({
                "stats": stats,
                "success": True
            })

        except Exception as e:
            return self._error(str(e))

    async def _tool_list_services(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List services from the service registry."""
        try:
            params = {}
            if args.get("capability"):
                params["capability"] = args["capability"]

            response = await self._client.get(
                f"{self.registry_url}/api/v1/services",
                params=params
            )

            if response.status_code == 200:
                data = response.json()
                return self._text_result({
                    "services": data.get("services", []),
                    "count": len(data.get("services", [])),
                    "success": True
                })
            else:
                return self._error(f"Failed to list services: {response.status_code}")

        except Exception as e:
            return self._error(str(e))

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _text_result(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Format a text result."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(data, indent=2, default=str)
            }]
        }

    def _error(self, message: str) -> Dict[str, Any]:
        """Format an error result."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({"error": message, "success": False})
            }],
            "isError": True
        }


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4005)
    parser.add_argument("--registry-url", type=str, default="http://localhost:2016")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║              REGISTRY WRAPPER MCP - Tool Registry Access         ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: registry-wrapper-mcp                                ║
║  Port: {args.port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
║  Registry: {args.registry_url:<52} ║
╠══════════════════════════════════════════════════════════════════╣
║  Capabilities: query, registry, tool_discovery                   ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools:                                                          ║
║  • list_tools        - List all tools in registry               ║
║  • get_tool          - Get tool details                         ║
║  • search_tools      - Search for tools                         ║
║  • execute_tool      - Execute a tool                           ║
║  • list_native_mcps  - List native MCP servers                  ║
║  • get_registry_stats - Get registry statistics                 ║
║  • list_services     - List registered services                 ║
╠══════════════════════════════════════════════════════════════════╣
║  Built on NativeMCPBase with cross-MCP mesh communication       ║
╚══════════════════════════════════════════════════════════════════╝
""")

    server = RegistryWrapperMCP(port=args.port, registry_url=args.registry_url)
    server.run()
