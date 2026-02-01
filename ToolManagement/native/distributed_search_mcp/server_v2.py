#!/usr/bin/env python3
"""
Distributed Search MCP v2 - Migrated to NativeMCPBase.

Provides:
- Web search (powered by Jina AI)
- URL content extraction
- Fact checking
- Semantic reranking
- Embedding generation

Run with:
    python -m native.distributed_search_mcp.server_v2 --port 4001
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase, CapabilityType

logger = logging.getLogger(__name__)


class DistributedSearchMCPv2(NativeMCPBase):
    """
    Native MCP server for distributed search capabilities.

    Migrated to use NativeMCPBase for standardized infrastructure.
    """

    SERVICE_ID = "distributed-search-mcp"
    SERVICE_NAME = "Distributed Search MCP"
    VERSION = "2.0.0"

    # This MCP provides these capabilities
    CAPABILITIES = [
        CapabilityType.SEARCH,
        CapabilityType.EMBED,
        CapabilityType.RERANK,
        CapabilityType.READ,
    ]

    # No dependencies
    REQUIRES = []
    DEPENDS = []

    # Tool definitions
    TOOLS = [
        {
            "name": "search",
            "description": "Search the web using Jina AI. Returns relevant results with snippets.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query"
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Number of results (1-20)",
                        "default": 10
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": "read",
            "description": "Extract and read content from a URL. Returns clean markdown text.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to read"
                    },
                    "include_images": {
                        "type": "boolean",
                        "description": "Include image descriptions",
                        "default": False
                    }
                },
                "required": ["url"]
            }
        },
        {
            "name": "verify",
            "description": "Verify a claim or statement against web sources.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "statement": {
                        "type": "string",
                        "description": "Statement to fact-check"
                    }
                },
                "required": ["statement"]
            }
        },
        {
            "name": "rerank",
            "description": "Rerank documents by relevance to a query using semantic similarity.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Query to rank against"
                    },
                    "documents": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Documents to rerank"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of top results",
                        "default": 5
                    }
                },
                "required": ["query", "documents"]
            }
        },
        {
            "name": "embed",
            "description": "Generate embeddings for text using Jina embedding models.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Single text to embed (use 'texts' for batch)"
                    },
                    "texts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Multiple texts to embed"
                    },
                    "model": {
                        "type": "string",
                        "description": "Embedding model",
                        "default": "jina-embeddings-v3"
                    }
                }
            }
        },
    ]

    # Jina API endpoints
    JINA_SEARCH_URL = "https://s.jina.ai"
    JINA_READ_URL = "https://r.jina.ai"
    JINA_EMBED_URL = "https://api.jina.ai/v1/embeddings"
    JINA_RERANK_URL = "https://api.jina.ai/v1/rerank"

    def __init__(
        self,
        port: int = 4001,
        api_key: Optional[str] = None,
        **kwargs
    ):
        super().__init__(port=port, **kwargs)
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        self._client: Optional[httpx.AsyncClient] = None

        # Register lifecycle hooks
        self.on_start(self._on_start)
        self.on_stop(self._on_stop)

    async def _on_start(self):
        """Initialize HTTP client on startup."""
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers=headers
        )
        logger.info(f"Jina API client initialized (key configured: {bool(self.api_key)})")

    async def _on_stop(self):
        """Close HTTP client on shutdown."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # TOOL HANDLERS - Named _tool_{tool_name}
    # =========================================================================

    async def _tool_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute web search via Jina."""
        query = args.get("query")
        if not query:
            return self._error("query is required")

        num_results = min(args.get("num_results", 10), 20)

        try:
            response = await self._client.get(
                f"{self.JINA_SEARCH_URL}/{query}",
                headers={"Accept": "application/json"},
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                return self._text_result({
                    "query": query,
                    "results": data.get("data", [])[:num_results],
                    "success": True
                })
            else:
                return self._text_result({
                    "query": query,
                    "error": f"Search failed: {response.status_code}",
                    "success": False
                })
        except Exception as e:
            return self._error(str(e))

    async def _tool_read(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Read and extract content from URL via Jina."""
        url = args.get("url")
        if not url:
            return self._error("url is required")

        try:
            response = await self._client.get(
                f"{self.JINA_READ_URL}/{url}",
                headers={"Accept": "application/json"},
                timeout=60.0
            )

            if response.status_code == 200:
                data = response.json()
                return self._text_result({
                    "url": url,
                    "title": data.get("title", ""),
                    "content": data.get("content", ""),
                    "success": True
                })
            else:
                return self._text_result({
                    "url": url,
                    "error": f"Read failed: {response.status_code}",
                    "success": False
                })
        except Exception as e:
            return self._error(str(e))

    async def _tool_verify(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Fact-check a statement."""
        statement = args.get("statement")
        if not statement:
            return self._error("statement is required")

        # Search for evidence
        search_result = await self._tool_search({
            "query": f"fact check: {statement}",
            "num_results": 5
        })

        try:
            # Parse search results from the content
            content = search_result.get("content", [{}])[0].get("text", "{}")
            data = json.loads(content)

            return self._text_result({
                "statement": statement,
                "sources": data.get("results", []),
                "note": "Review sources to verify claim",
                "success": data.get("success", False)
            })
        except Exception:
            return self._text_result({
                "statement": statement,
                "sources": [],
                "note": "Could not gather sources",
                "success": False
            })

    async def _tool_rerank(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Rerank documents by relevance."""
        query = args.get("query")
        documents = args.get("documents", [])
        top_k = args.get("top_k", 5)

        if not query or not documents:
            return self._error("query and documents are required")

        if not self.api_key:
            return self._error("API key required for reranking")

        try:
            response = await self._client.post(
                self.JINA_RERANK_URL,
                json={
                    "model": "jina-reranker-v2-base-multilingual",
                    "query": query,
                    "documents": documents,
                    "top_n": top_k
                },
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                return self._text_result({
                    "query": query,
                    "results": data.get("results", []),
                    "success": True
                })
            else:
                return self._error(f"Rerank failed: {response.status_code}")
        except Exception as e:
            return self._error(str(e))

    async def _tool_embed(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Generate embeddings for text."""
        # Support both single text and batch
        text = args.get("text")
        texts = args.get("texts", [])
        model = args.get("model", "jina-embeddings-v3")

        if text:
            texts = [text]

        if not texts:
            return self._error("text or texts is required")

        if not self.api_key:
            return self._error("API key required for embeddings")

        try:
            response = await self._client.post(
                self.JINA_EMBED_URL,
                json={
                    "model": model,
                    "input": texts
                },
                timeout=30.0
            )

            if response.status_code == 200:
                data = response.json()
                embedding_data = data.get("data", [])
                embeddings = [e.get("embedding") for e in embedding_data]

                return self._text_result({
                    "texts": [t[:100] + "..." if len(t) > 100 else t for t in texts],
                    "model": model,
                    "embeddings": embeddings,
                    "dimensions": len(embeddings[0]) if embeddings else 0,
                    "success": True
                })
            else:
                return self._error(f"Embed failed: {response.status_code}")
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
    parser.add_argument("--port", type=int, default=4001)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║          DISTRIBUTED SEARCH MCP v2 - NativeMCPBase               ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-search-mcp                              ║
║  Port: {args.port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Capabilities: search, embed, rerank, read                       ║
║  Tools: search, read, verify, rerank, embed                      ║
╠══════════════════════════════════════════════════════════════════╣
║  Built on NativeMCPBase with:                                    ║
║  • Cross-MCP mesh communication                                  ║
║  • Automatic service discovery                                   ║
║  • Circuit breaker pattern                                       ║
║  • Result caching                                                ║
╚══════════════════════════════════════════════════════════════════╝
""")

    server = DistributedSearchMCPv2(port=args.port)
    server.run()
