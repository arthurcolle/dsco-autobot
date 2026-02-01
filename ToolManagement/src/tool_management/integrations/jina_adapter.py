"""
Jina AI MCP Adapter - Integration with Jina's MCP server.

Jina provides:
- Web search
- Content extraction (read URLs)
- Fact checking
- Grounding/reranking

Endpoint: https://mcp.jina.ai/sse
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from .mcp_adapter import MCPAdapter, MCPServerConfig, MCPTransport, MCPTool

logger = logging.getLogger(__name__)

# Jina's known tools (as of late 2024)
JINA_TOOLS = [
    {
        "name": "web_search",
        "description": "Search the web using Jina AI's search API. Returns relevant results with excerpts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query"
                },
                "num_results": {
                    "type": "integer",
                    "description": "Number of results to return",
                    "default": 5
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "read_url",
        "description": "Read and extract content from a URL. Returns clean, readable text.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL to read"
                },
                "include_images": {
                    "type": "boolean",
                    "description": "Whether to include image descriptions",
                    "default": False
                }
            },
            "required": ["url"]
        }
    },
    {
        "name": "fact_check",
        "description": "Fact-check a statement using web sources.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "statement": {
                    "type": "string",
                    "description": "The statement to fact-check"
                }
            },
            "required": ["statement"]
        }
    },
    {
        "name": "rerank",
        "description": "Rerank a list of documents based on relevance to a query.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The query to rank against"
                },
                "documents": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of documents to rerank"
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of top results to return",
                    "default": 5
                }
            },
            "required": ["query", "documents"]
        }
    },
    {
        "name": "embed",
        "description": "Generate embeddings for text using Jina's embedding models.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Text to embed"
                },
                "model": {
                    "type": "string",
                    "description": "Embedding model to use",
                    "default": "jina-embeddings-v2-base-en"
                }
            },
            "required": ["text"]
        }
    }
]


class JinaAdapter(MCPAdapter):
    """
    Adapter for Jina AI's MCP server.

    Usage:
        adapter = JinaAdapter(api_key="your-jina-key")
        await adapter.connect()
        tools = adapter.get_discovered_tools()
        result = await adapter.execute_tool("web_search", {"query": "python async"})
    """

    DEFAULT_URL = "https://mcp.jina.ai"
    SSE_URL = "https://mcp.jina.ai/sse"

    def __init__(
        self,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        tools_registry=None,
        **kwargs
    ):
        api_key = api_key or os.getenv("JINA_API_KEY")

        config = MCPServerConfig(
            name="jina-ai",
            url=url or self.DEFAULT_URL,
            transport=MCPTransport.SSE,
            api_key=api_key,
            api_key_header="Authorization",
            api_key_prefix="Bearer",
            tags=["search", "extraction", "embeddings", "jina"],
            category="search",
            metadata={"provider": "Jina AI", "sse_url": self.SSE_URL},
            **kwargs
        )

        super().__init__(config, tools_registry)

    async def _discover_tools(self):
        """Discover Jina tools - use known tools + try dynamic discovery."""
        # Start with known tools
        for tool_data in JINA_TOOLS:
            tool = MCPTool(
                name=tool_data["name"],
                description=tool_data["description"],
                input_schema=tool_data["inputSchema"],
                server_name=self.name,
                server_url=self.config.url,
            )
            self._discovered_tools[tool.name] = tool

        # Try to discover additional tools via MCP protocol
        try:
            response = await self._client.post(
                f"{self.config.url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "tools/list",
                    "id": 1
                },
                headers=self._headers,
            )

            if response.status_code == 200:
                data = response.json()
                if "result" in data and "tools" in data["result"]:
                    for tool_data in data["result"]["tools"]:
                        if tool_data["name"] not in self._discovered_tools:
                            tool = MCPTool(
                                name=tool_data["name"],
                                description=tool_data.get("description", ""),
                                input_schema=tool_data.get("inputSchema", {}),
                                server_name=self.name,
                                server_url=self.config.url,
                            )
                            self._discovered_tools[tool.name] = tool

        except Exception as e:
            logger.debug(f"Dynamic tool discovery failed (using known tools): {e}")

    async def execute_tool(self, tool_name: str, inputs: Dict[str, Any]) -> Any:
        """Execute a Jina tool via MCP protocol."""
        if not self._connected:
            raise RuntimeError("Not connected to Jina MCP server")

        mcp_tool = self._discovered_tools.get(tool_name)
        if not mcp_tool:
            raise ValueError(f"Tool not found: {tool_name}")

        # Execute via MCP JSON-RPC
        try:
            response = await self._client.post(
                f"{self.config.url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": inputs
                    },
                    "id": 1
                },
                headers=self._headers,
                timeout=60.0,  # Longer timeout for search/extraction
            )

            if response.status_code == 200:
                data = response.json()
                if "error" in data:
                    raise RuntimeError(f"MCP error: {data['error']}")
                return data.get("result", data)
            else:
                raise RuntimeError(f"Request failed: {response.status_code}")

        except httpx.TimeoutException:
            raise RuntimeError(f"Timeout executing {tool_name}")
        except Exception as e:
            logger.error(f"Failed to execute Jina tool {tool_name}: {e}")
            raise

    async def web_search(self, query: str, num_results: int = 5) -> Dict[str, Any]:
        """Convenience method for web search."""
        return await self.execute_tool("web_search", {
            "query": query,
            "num_results": num_results
        })

    async def read_url(self, url: str, include_images: bool = False) -> Dict[str, Any]:
        """Convenience method for reading URLs."""
        return await self.execute_tool("read_url", {
            "url": url,
            "include_images": include_images
        })

    async def fact_check(self, statement: str) -> Dict[str, Any]:
        """Convenience method for fact checking."""
        return await self.execute_tool("fact_check", {
            "statement": statement
        })

    async def embed(self, text: str, model: str = "jina-embeddings-v2-base-en") -> Dict[str, Any]:
        """Convenience method for generating embeddings."""
        return await self.execute_tool("embed", {
            "text": text,
            "model": model
        })
