"""
Jina AI MCP Adapter - Integration with Jina's MCP server.

Jina provides:
- Web search (search_web) and parallel/arxiv/ssrn/image variants
- Content extraction (read_url)
- Relevance reranking (sort_by_relevance)
- Embeddings via the REST API (api.jina.ai) — not exposed as an MCP tool

Endpoint: https://mcp.jina.ai/v1 (streamable HTTP; responses are SSE-framed)

Legacy tool names remain callable for backward compatibility:
    web_search -> search_web
    rerank     -> sort_by_relevance (top_k -> top_n)
    fact_check -> emulated with search_web (g.jina.ai grounding is retired)
    embed      -> REST call to api.jina.ai/v1/embeddings
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

# Aliases: legacy name -> current Jina MCP tool name
TOOL_ALIASES = {
    "web_search": "search_web",
    "rerank": "sort_by_relevance",
}

# Known tools (fallback if dynamic discovery fails). Schemas mirror
# mcp.jina.ai/v1 server 1.4.x.
JINA_TOOLS = [
    {
        "name": "search_web",
        "description": "Search the web for current information, news, articles, and websites.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query"},
                "num": {"type": "integer", "description": "Number of results", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "read_url",
        "description": "Extract and convert web page content to clean, readable markdown.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The URL to read"},
                "withAllLinks": {"type": "boolean", "default": False},
                "withAllImages": {"type": "boolean", "default": False},
            },
            "required": ["url"],
        },
    },
    {
        "name": "sort_by_relevance",
        "description": "Rerank a list of documents by relevance to a query.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "documents": {"type": "array", "items": {"type": "string"}},
                "top_n": {"type": "integer", "default": 5},
            },
            "required": ["query", "documents"],
        },
    },
    {
        "name": "search_arxiv",
        "description": "Search academic papers and preprints on arXiv.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "num": {"type": "integer", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_images",
        "description": "Search for images across the web.",
        "inputSchema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    },
    # Legacy-compat surface (kept so existing services/tool maps keep working)
    {
        "name": "web_search",
        "description": "Legacy alias of search_web.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "num_results": {"type": "integer", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "rerank",
        "description": "Legacy alias of sort_by_relevance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "documents": {"type": "array", "items": {"type": "string"}},
                "top_k": {"type": "integer", "default": 5},
            },
            "required": ["query", "documents"],
        },
    },
    {
        "name": "fact_check",
        "description": "Fact-check a statement using web search evidence (grounding API retired).",
        "inputSchema": {
            "type": "object",
            "properties": {"statement": {"type": "string"}},
            "required": ["statement"],
        },
    },
    {
        "name": "embed",
        "description": "Generate embeddings for text using Jina's embedding models (REST).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "model": {"type": "string", "default": "jina-embeddings-v3"},
            },
            "required": ["text"],
        },
    },
]


def _parse_rpc_body(text: str) -> Dict[str, Any]:
    """Parse a JSON-RPC response that may be plain JSON or SSE-framed."""
    body = text.strip()
    if body.startswith("{"):
        return json.loads(body)
    # SSE frame: use the last "data:" line (single response per request)
    last_data = None
    for line in body.splitlines():
        if line.startswith("data:"):
            last_data = line[len("data:"):].strip()
    if last_data:
        return json.loads(last_data)
    raise ValueError(f"Unrecognized MCP response body: {body[:200]}")


class JinaAdapter(MCPAdapter):
    """
    Adapter for Jina AI's MCP server.

    Usage:
        adapter = JinaAdapter(api_key="your-jina-key")
        await adapter.connect()
        tools = adapter.get_discovered_tools()
        result = await adapter.execute_tool("search_web", {"query": "python async"})
    """

    DEFAULT_URL = "https://mcp.jina.ai"
    MCP_PATH = "/v1"
    SSE_URL = "https://mcp.jina.ai/sse"
    EMBEDDINGS_URL = "https://api.jina.ai/v1/embeddings"

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

    @property
    def _rpc_headers(self) -> Dict[str, str]:
        headers = dict(self._headers or {})
        # Streamable-HTTP MCP requires accepting both content types
        headers["Accept"] = "application/json, text/event-stream"
        return headers

    async def _rpc(self, method: str, params: Optional[Dict[str, Any]] = None,
                   timeout: float = 60.0) -> Dict[str, Any]:
        """Send one JSON-RPC request to the MCP endpoint and parse the reply."""
        payload: Dict[str, Any] = {"jsonrpc": "2.0", "method": method, "id": 1}
        if params is not None:
            payload["params"] = params
        response = await self._client.post(
            f"{self.config.url}{self.MCP_PATH}",
            json=payload,
            headers=self._rpc_headers,
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Request failed: {response.status_code}")
        data = _parse_rpc_body(response.text)
        if "error" in data:
            raise RuntimeError(f"MCP error: {data['error']}")
        return data.get("result", data)

    async def _discover_tools(self):
        """Discover Jina tools - dynamic discovery with known-tools fallback."""
        for tool_data in JINA_TOOLS:
            tool = MCPTool(
                name=tool_data["name"],
                description=tool_data["description"],
                input_schema=tool_data["inputSchema"],
                server_name=self.name,
                server_url=self.config.url,
            )
            self._discovered_tools[tool.name] = tool

        try:
            result = await self._rpc("tools/list", timeout=15.0)
            for tool_data in result.get("tools", []):
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
        """Execute a Jina tool via MCP protocol (with legacy-name handling)."""
        if not self._connected:
            raise RuntimeError("Not connected to Jina MCP server")

        if tool_name not in self._discovered_tools:
            raise ValueError(f"Tool not found: {tool_name}")

        # Locally-implemented tools
        if tool_name == "embed":
            return await self._embed_rest(inputs)
        if tool_name == "fact_check":
            # g.jina.ai grounding is retired; gather evidence via web search
            statement = inputs.get("statement", "")
            inputs = {"query": statement, "num": 5}
            tool_name = "search_web"

        # Legacy aliases and argument shims
        if tool_name == "web_search":
            inputs = dict(inputs)
            if "num_results" in inputs:
                inputs["num"] = inputs.pop("num_results")
        if tool_name == "rerank":
            inputs = dict(inputs)
            if "top_k" in inputs:
                inputs["top_n"] = inputs.pop("top_k")
        tool_name = TOOL_ALIASES.get(tool_name, tool_name)

        try:
            return await self._rpc(
                "tools/call",
                {"name": tool_name, "arguments": inputs},
                timeout=60.0,
            )
        except httpx.TimeoutException:
            raise RuntimeError(f"Timeout executing {tool_name}")
        except Exception as e:
            logger.error(f"Failed to execute Jina tool {tool_name}: {e}")
            raise

    async def _embed_rest(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Embeddings are REST-only on Jina; call api.jina.ai directly."""
        text = inputs.get("text", "")
        model = inputs.get("model") or "jina-embeddings-v3"
        response = await self._client.post(
            self.EMBEDDINGS_URL,
            json={"model": model, "input": [text] if isinstance(text, str) else text},
            headers=self._headers,
            timeout=30.0,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Embeddings request failed: {response.status_code} {response.text[:200]}")
        return response.json()

    async def web_search(self, query: str, num_results: int = 5) -> Dict[str, Any]:
        """Convenience method for web search."""
        return await self.execute_tool("search_web", {
            "query": query,
            "num": num_results
        })

    async def read_url(self, url: str, include_images: bool = False) -> Dict[str, Any]:
        """Convenience method for reading URLs."""
        inputs: Dict[str, Any] = {"url": url}
        if include_images:
            inputs["withAllImages"] = True
        return await self.execute_tool("read_url", inputs)

    async def fact_check(self, statement: str) -> Dict[str, Any]:
        """Convenience method for fact checking."""
        return await self.execute_tool("fact_check", {
            "statement": statement
        })

    async def embed(self, text: str, model: str = "jina-embeddings-v3") -> Dict[str, Any]:
        """Convenience method for generating embeddings."""
        return await self.execute_tool("embed", {
            "text": text,
            "model": model
        })
