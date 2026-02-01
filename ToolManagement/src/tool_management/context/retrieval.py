"""
Tool Retrieval with GraphSub Integration.

Uses GraphSub's HNSW vector index for semantic tool retrieval,
avoiding the need to dump all schemas into context.
"""

from __future__ import annotations

import json
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Set
import httpx

logger = logging.getLogger(__name__)


class RetrievalStrategy(Enum):
    """How to retrieve tools for context."""
    EXACT = "exact"          # Exact name match
    SEMANTIC = "semantic"    # Vector similarity search
    GRAPH = "graph"          # Graph traversal (related tools)
    HYBRID = "hybrid"        # Combine semantic + graph
    CLUSTER = "cluster"      # Tool cluster/group


@dataclass
class RetrievalResult:
    """Result from tool retrieval."""
    tool_name: str
    backend_id: str
    score: float
    schema: Dict[str, Any]
    reason: str  # Why this tool was selected
    related_tools: List[str] = field(default_factory=list)


@dataclass
class GraphSubConfig:
    """GraphSub connection configuration."""
    rest_url: str = "http://localhost:54545"
    mcp_url: str = "http://localhost:7879"
    shard_id: int = 0
    embedding_model: str = "jina-embeddings-v3"
    embedding_dim: int = 1024

    # Tool node type (extend GraphSub's type system)
    tool_type_id: int = 100
    tool_capability_type_id: int = 101
    tool_category_type_id: int = 102


class ToolRetriever:
    """
    Intelligent tool retrieval using GraphSub.

    Stores tool schemas as graph nodes with:
    - Semantic embeddings for similarity search
    - Edges to capabilities, categories, related tools
    - Usage-based ranking

    Falls back to local ToolContextManager when GraphSub unavailable.
    """

    def __init__(
        self,
        config: Optional[GraphSubConfig] = None,
        jina_api_key: Optional[str] = None,
        local_context_manager: Optional[Any] = None,
    ):
        self.config = config or GraphSubConfig()
        self.jina_api_key = jina_api_key
        self._client: Optional[httpx.AsyncClient] = None
        self._local_ctx = local_context_manager

        # Local cache of tool -> node mappings
        self._tool_nodes: Dict[str, int] = {}  # key -> node_id
        self._initialized = False

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def initialize(self):
        """Initialize GraphSub connection and schema."""
        if self._initialized:
            return

        client = await self._get_client()

        # Check GraphSub health
        try:
            resp = await client.get(f"{self.config.rest_url}/health")
            if resp.status_code != 200:
                logger.warning(f"GraphSub not healthy: {resp.status_code}")
                return
            logger.info("Connected to GraphSub")
        except Exception as e:
            logger.warning(f"GraphSub not available: {e}")
            return

        self._initialized = True

    async def index_tool(
        self,
        backend_id: str,
        name: str,
        description: str,
        input_schema: Dict[str, Any],
        category: Optional[str] = None,
        capabilities: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[int]:
        """
        Index a tool in GraphSub for semantic retrieval.

        Creates:
        - Tool node with embedded description
        - Edges to capability nodes
        - Edges to category nodes
        """
        if not self._initialized:
            await self.initialize()
            if not self._initialized:
                return None

        key = f"{backend_id}:{name}"
        client = await self._get_client()

        # Create tool node
        tool_data = {
            "type_id": self.config.tool_type_id,
            "name": key,
            "payload": json.dumps({
                "backend_id": backend_id,
                "tool_name": name,
                "description": description,
                "input_schema": input_schema,
                "category": category,
                "capabilities": capabilities or [],
                "tags": tags or [],
                "indexed_at": datetime.now(timezone.utc).isoformat(),
            })
        }

        try:
            resp = await client.post(
                f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes",
                json=tool_data
            )

            if resp.status_code in (200, 201):
                result = resp.json()
                node_id = result.get("id") or result.get("node_id")
                self._tool_nodes[key] = node_id

                # Add embedding for semantic search
                await self._add_embedding(node_id, f"{name}: {description}")

                # Create capability edges
                if capabilities:
                    for cap in capabilities:
                        await self._link_capability(node_id, cap)

                # Create category edge
                if category:
                    await self._link_category(node_id, category)

                logger.debug(f"Indexed tool {key} as node {node_id}")
                return node_id
            else:
                logger.warning(f"Failed to index tool {key}: {resp.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error indexing tool {key}: {e}")
            return None

    async def _add_embedding(self, node_id: int, text: str):
        """Add semantic embedding to node."""
        client = await self._get_client()

        # Get embedding from Jina (or use local model)
        embedding = await self._get_embedding(text)
        if not embedding:
            return

        try:
            await client.post(
                f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/vectors/embeddings",
                json={
                    "node_id": node_id,
                    "embedding": embedding,
                }
            )
        except Exception as e:
            logger.debug(f"Could not add embedding: {e}")

    async def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Get embedding vector for text."""
        if not self.jina_api_key:
            # Generate deterministic pseudo-embedding for testing
            return self._pseudo_embedding(text)

        client = await self._get_client()

        try:
            resp = await client.post(
                "https://api.jina.ai/v1/embeddings",
                headers={"Authorization": f"Bearer {self.jina_api_key}"},
                json={
                    "model": self.config.embedding_model,
                    "input": [text],
                }
            )

            if resp.status_code == 200:
                data = resp.json()
                return data["data"][0]["embedding"]
        except Exception as e:
            logger.debug(f"Could not get embedding: {e}")

        return self._pseudo_embedding(text)

    def _pseudo_embedding(self, text: str) -> List[float]:
        """Generate deterministic pseudo-embedding for testing."""
        # Use hash to create reproducible vector
        h = hashlib.sha256(text.encode()).digest()
        import struct
        # Create 1024-dim vector from hash (cyclic)
        values = []
        for i in range(self.config.embedding_dim):
            idx = i % 32
            val = struct.unpack('b', h[idx:idx+1])[0] / 128.0
            values.append(val)
        return values

    async def _link_capability(self, tool_node_id: int, capability: str):
        """Link tool to capability node."""
        client = await self._get_client()

        # Get or create capability node
        cap_key = f"capability:{capability}"
        cap_node_id = self._tool_nodes.get(cap_key)

        if not cap_node_id:
            try:
                resp = await client.post(
                    f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes",
                    json={
                        "type_id": self.config.tool_capability_type_id,
                        "name": cap_key,
                    }
                )
                if resp.status_code in (200, 201):
                    result = resp.json()
                    cap_node_id = result.get("id") or result.get("node_id")
                    self._tool_nodes[cap_key] = cap_node_id
            except Exception:
                return

        if cap_node_id:
            # Create edge: tool -[has_capability]-> capability
            try:
                await client.post(
                    f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/edges",
                    json={
                        "source": tool_node_id,
                        "target": cap_node_id,
                        "relation_type": 26,  # enables
                    }
                )
            except Exception:
                pass

    async def _link_category(self, tool_node_id: int, category: str):
        """Link tool to category node."""
        client = await self._get_client()

        cat_key = f"category:{category}"
        cat_node_id = self._tool_nodes.get(cat_key)

        if not cat_node_id:
            try:
                resp = await client.post(
                    f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes",
                    json={
                        "type_id": self.config.tool_category_type_id,
                        "name": cat_key,
                    }
                )
                if resp.status_code in (200, 201):
                    result = resp.json()
                    cat_node_id = result.get("id") or result.get("node_id")
                    self._tool_nodes[cat_key] = cat_node_id
            except Exception:
                return

        if cat_node_id:
            try:
                await client.post(
                    f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/edges",
                    json={
                        "source": tool_node_id,
                        "target": cat_node_id,
                        "relation_type": 20,  # part_of
                    }
                )
            except Exception:
                pass

    def _get_local_ctx(self):
        """Get local context manager, lazy initialize if needed."""
        if self._local_ctx is None:
            from .tool_context import get_tool_context_manager
            self._local_ctx = get_tool_context_manager()
        return self._local_ctx

    def _local_fallback(
        self,
        query: str,
        limit: int,
        backend_filter: Optional[str] = None,
    ) -> List[RetrievalResult]:
        """Fallback to local context manager when GraphSub unavailable."""
        ctx = self._get_local_ctx()
        local_results = ctx.search_tools(
            query=query,
            backend_id=backend_filter,
            limit=limit,
        )
        return [
            RetrievalResult(
                tool_name=s.name,
                backend_id=s.backend_id,
                score=1.0,
                schema={
                    "description": s.description,
                    "input_schema": s.input_schema,
                    "category": s.group,
                    "tags": s.tags,
                },
                reason="local fallback",
            )
            for s in local_results
        ]

    def _local_exact_lookup(
        self,
        name: str,
        backend_id: Optional[str] = None,
    ) -> Optional[RetrievalResult]:
        """Exact tool lookup from local context manager."""
        ctx = self._get_local_ctx()
        key = f"{backend_id}:{name}" if backend_id else None

        # Try exact match first
        if key and key in ctx._schemas:
            s = ctx._schemas[key]
            return RetrievalResult(
                tool_name=s.name,
                backend_id=s.backend_id,
                score=1.0,
                schema={
                    "description": s.description,
                    "input_schema": s.input_schema,
                    "category": s.group,
                    "tags": s.tags,
                },
                reason="exact local match",
            )

        # Search by name across backends
        for schema_key, s in ctx._schemas.items():
            if s.name == name:
                if backend_id is None or s.backend_id == backend_id:
                    return RetrievalResult(
                        tool_name=s.name,
                        backend_id=s.backend_id,
                        score=1.0,
                        schema={
                            "description": s.description,
                            "input_schema": s.input_schema,
                            "category": s.group,
                            "tags": s.tags,
                        },
                        reason="exact local match",
                    )
        return None

    async def retrieve(
        self,
        query: str,
        strategy: RetrievalStrategy = RetrievalStrategy.HYBRID,
        limit: int = 10,
        backend_filter: Optional[str] = None,
        category_filter: Optional[str] = None,
        min_score: float = 0.0,
    ) -> List[RetrievalResult]:
        """
        Retrieve relevant tools for a query.

        Args:
            query: Natural language query describing needed capability
            strategy: Retrieval strategy to use
            limit: Max tools to return
            backend_filter: Only return tools from this backend
            category_filter: Only return tools in this category
            min_score: Minimum similarity score

        Returns:
            List of matching tools with scores and schemas
        """
        if not self._initialized:
            await self.initialize()

        results = []

        # Try GraphSub first if initialized
        if self._initialized:
            if strategy == RetrievalStrategy.EXACT:
                results = await self._retrieve_exact(query, limit)
            elif strategy == RetrievalStrategy.SEMANTIC:
                results = await self._retrieve_semantic(query, limit, min_score)
            elif strategy == RetrievalStrategy.GRAPH:
                results = await self._retrieve_graph(query, limit)
            elif strategy == RetrievalStrategy.HYBRID:
                # Combine semantic and graph
                semantic = await self._retrieve_semantic(query, limit * 2, min_score)
                graph = await self._retrieve_graph(query, limit)

                # Merge and deduplicate
                seen: Set[str] = set()
                for r in semantic + graph:
                    key = f"{r.backend_id}:{r.tool_name}"
                    if key not in seen:
                        results.append(r)
                        seen.add(key)
                    if len(results) >= limit:
                        break
            elif strategy == RetrievalStrategy.CLUSTER:
                results = await self._retrieve_cluster(query, limit)

        # Fallback to local context manager if no results
        if not results:
            logger.debug(f"GraphSub returned no results, falling back to local context")
            results = self._local_fallback(query, limit, backend_filter)

        # Apply filters
        if backend_filter:
            results = [r for r in results if r.backend_id == backend_filter]
        if category_filter:
            results = [r for r in results if r.schema.get("category") == category_filter]

        return results[:limit]

    async def _retrieve_exact(self, query: str, limit: int) -> List[RetrievalResult]:
        """Exact/pattern matching retrieval."""
        if not self._initialized:
            return []

        client = await self._get_client()
        results = []

        try:
            # Search by name pattern
            resp = await client.get(
                f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes",
                params={"pattern": f"*{query}*", "limit": limit}
            )

            if resp.status_code == 200:
                nodes = resp.json()
                for node in nodes:
                    if node.get("type_id") == self.config.tool_type_id:
                        payload = json.loads(node.get("payload", "{}"))
                        results.append(RetrievalResult(
                            tool_name=payload.get("tool_name", node.get("name", "")),
                            backend_id=payload.get("backend_id", ""),
                            score=1.0,
                            schema=payload,
                            reason="exact match",
                        ))
        except Exception as e:
            logger.debug(f"Exact retrieval error: {e}")

        return results

    async def _retrieve_semantic(
        self,
        query: str,
        limit: int,
        min_score: float = 0.0,
    ) -> List[RetrievalResult]:
        """Vector similarity retrieval using HNSW index."""
        if not self._initialized:
            return []

        client = await self._get_client()
        results = []

        # Get query embedding
        query_embedding = await self._get_embedding(query)
        if not query_embedding:
            return []

        try:
            resp = await client.post(
                f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/vectors/search",
                json={
                    "query": query_embedding,
                    "k": limit,
                    "ef": max(limit * 2, 50),
                }
            )

            if resp.status_code == 200:
                matches = resp.json()
                for match in matches:
                    if match.get("score", 0) < min_score:
                        continue

                    # Get full node data
                    node_id = match.get("node_id")
                    node_resp = await client.get(
                        f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes/{node_id}"
                    )

                    if node_resp.status_code == 200:
                        node = node_resp.json()
                        if node.get("type_id") == self.config.tool_type_id:
                            payload = json.loads(node.get("payload", "{}"))
                            results.append(RetrievalResult(
                                tool_name=payload.get("tool_name", ""),
                                backend_id=payload.get("backend_id", ""),
                                score=match.get("score", 0),
                                schema=payload,
                                reason=f"semantic similarity: {match.get('score', 0):.3f}",
                            ))
        except Exception as e:
            logger.debug(f"Semantic retrieval error: {e}")

        return results

    async def _retrieve_graph(self, query: str, limit: int) -> List[RetrievalResult]:
        """Graph traversal retrieval - find related tools."""
        if not self._initialized:
            return []

        client = await self._get_client()
        results = []

        # First find any matching tool
        exact = await self._retrieve_exact(query, 1)
        if not exact:
            return []

        seed_key = f"{exact[0].backend_id}:{exact[0].tool_name}"
        seed_node_id = self._tool_nodes.get(seed_key)

        if not seed_node_id:
            return []

        try:
            # Get related tools via graph traversal
            resp = await client.get(
                f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes/{seed_node_id}/edges",
                params={"direction": "both", "depth": 2}
            )

            if resp.status_code == 200:
                edges = resp.json()
                related_nodes: Set[int] = set()

                for edge in edges:
                    related_nodes.add(edge.get("source"))
                    related_nodes.add(edge.get("target"))

                related_nodes.discard(seed_node_id)

                for node_id in list(related_nodes)[:limit]:
                    node_resp = await client.get(
                        f"{self.config.rest_url}/api/v1/shards/{self.config.shard_id}/nodes/{node_id}"
                    )

                    if node_resp.status_code == 200:
                        node = node_resp.json()
                        if node.get("type_id") == self.config.tool_type_id:
                            payload = json.loads(node.get("payload", "{}"))
                            results.append(RetrievalResult(
                                tool_name=payload.get("tool_name", ""),
                                backend_id=payload.get("backend_id", ""),
                                score=0.8,  # Related tool score
                                schema=payload,
                                reason=f"related to {exact[0].tool_name}",
                                related_tools=[exact[0].tool_name],
                            ))
        except Exception as e:
            logger.debug(f"Graph retrieval error: {e}")

        return results

    async def _retrieve_cluster(self, query: str, limit: int) -> List[RetrievalResult]:
        """Retrieve all tools in same cluster/category."""
        # First find category from query
        exact = await self._retrieve_exact(query, 1)
        if not exact:
            return await self._retrieve_semantic(query, limit, 0)

        category = exact[0].schema.get("category")
        if not category:
            return exact

        # Get all tools in category
        return await self.retrieve(
            query,
            strategy=RetrievalStrategy.EXACT,
            limit=limit,
            category_filter=category,
        )

    async def get_tool_context(
        self,
        tools: List[str],
        token_budget: int = 4000,
        include_related: bool = True,
    ) -> Dict[str, Any]:
        """
        Build optimized context for specific tools.

        Returns tool schemas that fit within token budget,
        prioritizing requested tools then related ones.
        """
        context_tools = []
        tokens_used = 0

        for tool_key in tools:
            if ":" in tool_key:
                backend_id, name = tool_key.split(":", 1)
            else:
                backend_id, name = None, tool_key

            # Try exact local lookup first (most reliable)
            result = self._local_exact_lookup(name, backend_id)

            # Fallback to semantic retrieval
            if not result:
                results = await self.retrieve(
                    name,
                    strategy=RetrievalStrategy.EXACT,
                    limit=1,
                    backend_filter=backend_id,
                )
                result = results[0] if results else None

            if result:
                schema = result.schema
                schema_tokens = len(json.dumps(schema)) // 4

                if tokens_used + schema_tokens <= token_budget:
                    context_tools.append({
                        "name": result.tool_name,
                        "backend": result.backend_id,
                        "description": schema.get("description", ""),
                        "inputSchema": schema.get("input_schema", {}),
                    })
                    tokens_used += schema_tokens

        # Track already-loaded tools to avoid duplicates
        loaded_keys = {f"{t['backend']}:{t['name']}" for t in context_tools}

        # Add related tools if budget allows and GraphSub is available
        if include_related and tokens_used < token_budget * 0.8 and self._initialized:
            for tool in context_tools[:3]:  # Check relations for top 3
                related = await self.retrieve(
                    tool["name"],
                    strategy=RetrievalStrategy.GRAPH,
                    limit=3,
                )

                for r in related:
                    key = f"{r.backend_id}:{r.tool_name}"
                    if key in loaded_keys:
                        continue  # Skip duplicates

                    schema_tokens = len(json.dumps(r.schema)) // 4
                    if tokens_used + schema_tokens <= token_budget:
                        context_tools.append({
                            "name": r.tool_name,
                            "backend": r.backend_id,
                            "description": r.schema.get("description", ""),
                            "inputSchema": r.schema.get("input_schema", {}),
                            "_related_to": tool["name"],
                        })
                        tokens_used += schema_tokens
                        loaded_keys.add(key)

        return {
            "tools": context_tools,
            "tokens_used": tokens_used,
            "token_budget": token_budget,
        }

    async def index_from_openapi(
        self,
        backend_id: str,
        openapi_spec: Dict[str, Any],
        category: Optional[str] = None,
    ) -> int:
        """Index tools from OpenAPI specification."""
        paths = openapi_spec.get("paths", {})
        count = 0

        for path, methods in paths.items():
            for method, spec in methods.items():
                if method not in ("get", "post", "put", "delete", "patch"):
                    continue

                name = spec.get("operationId", f"{method}_{path}")
                description = spec.get("summary", "") or spec.get("description", "")

                # Extract input schema from parameters and requestBody
                input_schema = {"type": "object", "properties": {}}

                for param in spec.get("parameters", []):
                    input_schema["properties"][param["name"]] = {
                        "type": param.get("schema", {}).get("type", "string"),
                        "description": param.get("description", ""),
                    }

                req_body = spec.get("requestBody", {})
                if req_body:
                    content = req_body.get("content", {})
                    json_schema = content.get("application/json", {}).get("schema", {})
                    if json_schema:
                        input_schema = json_schema

                # Determine capabilities from tags
                capabilities = spec.get("tags", [])

                result = await self.index_tool(
                    backend_id=backend_id,
                    name=name,
                    description=description,
                    input_schema=input_schema,
                    category=category or (capabilities[0] if capabilities else None),
                    capabilities=capabilities,
                    tags=capabilities,
                )

                if result:
                    count += 1

        logger.info(f"Indexed {count} tools from OpenAPI for {backend_id}")
        return count


# Convenience function
_retriever: Optional[ToolRetriever] = None

def get_tool_retriever(
    config: Optional[GraphSubConfig] = None,
    jina_api_key: Optional[str] = None,
) -> ToolRetriever:
    """Get or create singleton ToolRetriever."""
    global _retriever
    if _retriever is None:
        _retriever = ToolRetriever(config=config, jina_api_key=jina_api_key)
    return _retriever
