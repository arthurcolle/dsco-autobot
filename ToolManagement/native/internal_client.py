"""
Internal MCP Client - Cross-MCP Communication Layer.

Provides a unified interface for native MCPs to call tools on other MCPs,
with intelligent routing through the registry or direct calls.

Features:
- Service discovery by capability
- Caching for idempotent operations (embeddings, etc.)
- Direct calls for low-latency when target is known and healthy
- Fallback to registry gateway for resilience
- Circuit breaker for failing endpoints
- Metrics emission for observability

Usage:
    client = InternalMCPClient(
        registry_url="http://localhost:2016",
        source_mcp_id="distributed-memory-mcp",
    )

    # Call by capability (service discovery)
    result = await client.call_tool(
        tool_name="embed",
        arguments={"texts": ["hello world"]},
        capability="embed",
        cache_ttl=3600,  # Cache embeddings for 1 hour
    )

    # Call specific MCP directly
    result = await client.call_tool(
        tool_name="search",
        arguments={"query": "test"},
        target_mcp="distributed-search-mcp",
    )
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from collections import OrderedDict

import httpx

logger = logging.getLogger(__name__)


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================

class CircuitState(str, Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if recovered


@dataclass
class CircuitBreaker:
    """Simple circuit breaker for endpoint health."""
    failure_threshold: int = 3
    recovery_timeout: float = 30.0

    failures: int = 0
    last_failure: float = 0.0
    state: CircuitState = CircuitState.CLOSED

    def record_success(self):
        """Record a successful call."""
        self.failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self):
        """Record a failed call."""
        self.failures += 1
        self.last_failure = time.time()

        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker opened after {self.failures} failures")

    def can_proceed(self) -> bool:
        """Check if a call can proceed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            # Check if recovery timeout has passed
            if time.time() - self.last_failure > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True
            return False

        # HALF_OPEN - allow one test call
        return True


# =============================================================================
# ENDPOINT REGISTRY
# =============================================================================

@dataclass
class MCPEndpoint:
    """Represents a known MCP endpoint."""
    mcp_id: str
    name: str
    url: str
    capabilities: List[str]
    tools: List[str] = field(default_factory=list)
    healthy: bool = True
    last_seen: float = field(default_factory=time.time)
    circuit: CircuitBreaker = field(default_factory=CircuitBreaker)

    def has_capability(self, capability: str) -> bool:
        return capability in self.capabilities

    def has_tool(self, tool_name: str) -> bool:
        return tool_name in self.tools


# =============================================================================
# CACHE
# =============================================================================

class LRUCache:
    """Simple LRU cache with TTL support."""

    def __init__(self, max_size: int = 1000, default_ttl: float = 3600.0):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, Tuple[Any, float]] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        """Get a value, returning None if expired or missing."""
        if key not in self._cache:
            return None

        value, expires_at = self._cache[key]
        if time.time() > expires_at:
            del self._cache[key]
            return None

        # Move to end (most recently used)
        self._cache.move_to_end(key)
        return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None):
        """Set a value with optional TTL."""
        ttl = ttl or self.default_ttl
        expires_at = time.time() + ttl

        # Evict if at capacity
        while len(self._cache) >= self.max_size:
            self._cache.popitem(last=False)

        self._cache[key] = (value, expires_at)

    def invalidate(self, key: str):
        """Remove a key from cache."""
        self._cache.pop(key, None)

    def clear(self):
        """Clear all cached values."""
        self._cache.clear()

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        now = time.time()
        valid = sum(1 for _, (_, exp) in self._cache.items() if exp > now)
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "valid_entries": valid,
        }


# =============================================================================
# INTERNAL MCP CLIENT
# =============================================================================

class InternalMCPClient:
    """
    Client for native MCPs to call tools on other MCPs.

    Provides:
    - Service discovery by capability
    - Intelligent routing (direct vs gateway)
    - Caching for idempotent operations
    - Circuit breaker for failing endpoints
    - Automatic retries with backoff
    """

    def __init__(
        self,
        registry_url: str = "http://localhost:2016",
        source_mcp_id: str = "unknown",
        cache_max_size: int = 1000,
        default_timeout: float = 30.0,
    ):
        self.registry_url = registry_url.rstrip("/")
        self.source_mcp_id = source_mcp_id
        self.default_timeout = default_timeout

        # Endpoint registry
        self._endpoints: Dict[str, MCPEndpoint] = {}
        self._capability_index: Dict[str, List[str]] = {}  # capability -> [mcp_ids]
        self._tool_index: Dict[str, List[str]] = {}  # tool_name -> [mcp_ids]

        # Cache for results
        self._cache = LRUCache(max_size=cache_max_size)

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        # Discovery state
        self._last_discovery: float = 0.0
        self._discovery_interval: float = 60.0  # Re-discover every minute

        # Metrics
        self._metrics = {
            "calls_total": 0,
            "calls_direct": 0,
            "calls_gateway": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0,
        }

    async def _ensure_client(self):
        """Ensure HTTP client is initialized."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.default_timeout)

    async def close(self):
        """Close the client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # SERVICE DISCOVERY
    # =========================================================================

    async def discover(self, force: bool = False) -> Dict[str, MCPEndpoint]:
        """
        Discover available MCPs from the registry.

        Returns a dict of mcp_id -> MCPEndpoint.
        """
        await self._ensure_client()

        # Check if we should refresh
        now = time.time()
        if not force and (now - self._last_discovery) < self._discovery_interval:
            return self._endpoints

        try:
            # Query registry for native MCPs
            response = await self._client.get(f"{self.registry_url}/api/v1/native")
            if response.status_code != 200:
                logger.warning(f"Discovery failed: {response.status_code}")
                return self._endpoints

            data = response.json()

            # Update endpoints
            new_endpoints = {}
            new_capability_index: Dict[str, List[str]] = {}
            new_tool_index: Dict[str, List[str]] = {}

            for mcp_id, info in data.get("servers", {}).items():
                # Skip self
                if mcp_id == self.source_mcp_id:
                    continue

                # Skip non-running
                if info.get("status") != "running":
                    continue

                endpoint = MCPEndpoint(
                    mcp_id=mcp_id,
                    name=info.get("name", mcp_id),
                    url=f"http://localhost:{info.get('port', 0)}",
                    capabilities=info.get("capabilities", []),
                    healthy=True,
                )

                # Preserve circuit breaker state from existing endpoint
                if mcp_id in self._endpoints:
                    endpoint.circuit = self._endpoints[mcp_id].circuit
                    endpoint.tools = self._endpoints[mcp_id].tools

                new_endpoints[mcp_id] = endpoint

                # Index by capability
                for cap in endpoint.capabilities:
                    if cap not in new_capability_index:
                        new_capability_index[cap] = []
                    new_capability_index[cap].append(mcp_id)

            # Discover tools for each endpoint
            for mcp_id, endpoint in new_endpoints.items():
                if not endpoint.tools:
                    try:
                        tools_response = await self._client.get(
                            f"{endpoint.url}/tools",
                            timeout=5.0,
                        )
                        if tools_response.status_code == 200:
                            tools_data = tools_response.json()
                            endpoint.tools = [t.get("name") for t in tools_data.get("tools", [])]

                            # Index by tool
                            for tool_name in endpoint.tools:
                                if tool_name not in new_tool_index:
                                    new_tool_index[tool_name] = []
                                new_tool_index[tool_name].append(mcp_id)
                    except Exception as e:
                        logger.debug(f"Failed to get tools from {mcp_id}: {e}")

            self._endpoints = new_endpoints
            self._capability_index = new_capability_index
            self._tool_index = new_tool_index
            self._last_discovery = now

            logger.debug(f"Discovered {len(self._endpoints)} MCPs with {len(self._tool_index)} unique tools")

        except Exception as e:
            logger.error(f"Discovery error: {e}")

        return self._endpoints

    async def find_by_capability(self, capability: str) -> List[MCPEndpoint]:
        """Find MCPs that have a specific capability."""
        await self.discover()
        mcp_ids = self._capability_index.get(capability, [])
        return [self._endpoints[mid] for mid in mcp_ids if mid in self._endpoints]

    async def find_by_tool(self, tool_name: str) -> List[MCPEndpoint]:
        """Find MCPs that have a specific tool."""
        await self.discover()
        mcp_ids = self._tool_index.get(tool_name, [])
        return [self._endpoints[mid] for mid in mcp_ids if mid in self._endpoints]

    # =========================================================================
    # CALL TOOL
    # =========================================================================

    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        target_mcp: Optional[str] = None,
        capability: Optional[str] = None,
        prefer_direct: bool = True,
        cache_key: Optional[str] = None,
        cache_ttl: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Call a tool on another MCP.

        Args:
            tool_name: Name of the tool to call
            arguments: Tool arguments
            target_mcp: Specific MCP to call (optional)
            capability: Find MCP by capability (optional)
            prefer_direct: Try direct call before gateway (default True)
            cache_key: Cache key for result (optional)
            cache_ttl: Cache TTL in seconds (optional)
            timeout: Request timeout (optional)

        Returns:
            Tool result dict with "content" and optional "isError"
        """
        await self._ensure_client()
        self._metrics["calls_total"] += 1

        # Check cache first
        if cache_key:
            cached = self._cache.get(cache_key)
            if cached is not None:
                self._metrics["cache_hits"] += 1
                logger.debug(f"Cache hit for {cache_key}")
                return cached
            self._metrics["cache_misses"] += 1

        # Resolve target endpoint
        endpoint = await self._resolve_endpoint(tool_name, target_mcp, capability)
        if not endpoint:
            return {
                "content": [{"type": "text", "text": f"No endpoint found for tool: {tool_name}"}],
                "isError": True,
            }

        # Try direct call first if enabled
        result = None
        if prefer_direct and endpoint.circuit.can_proceed():
            try:
                result = await self._direct_call(endpoint, tool_name, arguments, timeout)
                endpoint.circuit.record_success()
                self._metrics["calls_direct"] += 1
            except Exception as e:
                logger.debug(f"Direct call failed, falling back to gateway: {e}")
                endpoint.circuit.record_failure()

        # Fall back to gateway
        if result is None:
            try:
                result = await self._gateway_call(tool_name, arguments, endpoint.mcp_id, timeout)
                self._metrics["calls_gateway"] += 1
            except Exception as e:
                self._metrics["errors"] += 1
                return {
                    "content": [{"type": "text", "text": f"Call failed: {e}"}],
                    "isError": True,
                }

        # Cache result if successful
        if cache_key and not result.get("isError"):
            self._cache.set(cache_key, result, ttl=cache_ttl)

        return result

    async def _resolve_endpoint(
        self,
        tool_name: str,
        target_mcp: Optional[str],
        capability: Optional[str],
    ) -> Optional[MCPEndpoint]:
        """Resolve which endpoint to call."""
        await self.discover()

        # If target specified, use it
        if target_mcp:
            return self._endpoints.get(target_mcp)

        # Find by tool name
        endpoints = await self.find_by_tool(tool_name)
        if endpoints:
            # Return healthiest one
            healthy = [e for e in endpoints if e.circuit.can_proceed()]
            return healthy[0] if healthy else endpoints[0]

        # Find by capability
        if capability:
            endpoints = await self.find_by_capability(capability)
            if endpoints:
                healthy = [e for e in endpoints if e.circuit.can_proceed()]
                return healthy[0] if healthy else endpoints[0]

        return None

    async def _direct_call(
        self,
        endpoint: MCPEndpoint,
        tool_name: str,
        arguments: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Make a direct MCP call to an endpoint."""
        mcp_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
        }

        response = await self._client.post(
            f"{endpoint.url}/mcp",
            json=mcp_request,
            timeout=timeout or self.default_timeout,
            headers={"X-Source-MCP": self.source_mcp_id},
        )

        if response.status_code != 200:
            raise Exception(f"MCP call failed: {response.status_code}")

        data = response.json()

        if "error" in data:
            raise Exception(data["error"].get("message", "Unknown error"))

        return data.get("result", {})

    async def _gateway_call(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        target_mcp: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Route call through registry gateway."""
        # Use the gateway execute endpoint
        request_body = {
            "tool_name": tool_name,
            "arguments": arguments,
        }

        if target_mcp:
            request_body["backend_id"] = target_mcp

        response = await self._client.post(
            f"{self.registry_url}/api/v1/gateway/execute",
            json=request_body,
            timeout=timeout or self.default_timeout,
            headers={"X-Source-MCP": self.source_mcp_id},
        )

        if response.status_code != 200:
            raise Exception(f"Gateway call failed: {response.status_code}")

        data = response.json()
        return data.get("result", data)

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    async def get_embedding(
        self,
        text: str,
        cache: bool = True,
    ) -> Optional[List[float]]:
        """
        Get embedding vector for text using Search MCP.

        This is a convenience method that handles caching and
        calls the embed tool on distributed-search-mcp.
        """
        # Generate cache key from text hash
        cache_key = None
        if cache:
            text_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
            cache_key = f"embed:{text_hash}"

        try:
            result = await self.call_tool(
                tool_name="embed",
                arguments={"text": text},
                capability="embed",
                cache_key=cache_key,
                cache_ttl=86400,  # Cache embeddings for 24 hours
            )

            if result.get("isError"):
                return None

            # Parse result - handle both MCP format and direct format
            content = result.get("content", [])
            if content and isinstance(content, list):
                text_content = content[0].get("text", "{}")
                data = json.loads(text_content)
                # Search MCP embed returns {"embedding": [...]} (singular)
                embedding = data.get("embedding")
                if embedding is not None:
                    return embedding
                # Fallback: try plural form
                embeddings = data.get("embeddings", [])
                return embeddings[0] if embeddings else None

            return None

        except Exception as e:
            logger.error(f"Failed to get embedding: {e}")
            return None

    async def get_embeddings_batch(
        self,
        texts: List[str],
        cache: bool = True,
    ) -> List[Optional[List[float]]]:
        """
        Get embeddings for multiple texts, with per-text caching.
        """
        results: List[Optional[List[float]]] = []
        uncached_indices: List[int] = []
        uncached_texts: List[str] = []

        # Check cache for each text
        for i, text in enumerate(texts):
            if cache:
                text_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
                cached = self._cache.get(f"embed:{text_hash}")
                if cached is not None:
                    self._metrics["cache_hits"] += 1
                    results.append(cached)
                    continue

            self._metrics["cache_misses"] += 1
            results.append(None)
            uncached_indices.append(i)
            uncached_texts.append(text)

        # Call embed for each uncached text (embed tool accepts single text only)
        if uncached_texts:
            try:
                for j, text in enumerate(uncached_texts):
                    embedding = await self.get_embedding(text, cache=False)
                    if embedding is not None:
                        idx = uncached_indices[j]
                        results[idx] = embedding

                        # Cache each embedding
                        if cache:
                            text_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
                            self._cache.set(f"embed:{text_hash}", embedding, ttl=86400)

            except Exception as e:
                logger.error(f"Batch embedding failed: {e}")

        return results

    async def search_web(
        self,
        query: str,
        num_results: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search the web using Search MCP.
        """
        try:
            result = await self.call_tool(
                tool_name="search",
                arguments={"query": query, "num_results": num_results},
                capability="search",
            )

            if result.get("isError"):
                return []

            content = result.get("content", [])
            if content and isinstance(content, list):
                text_content = content[0].get("text", "{}")
                data = json.loads(text_content)
                return data.get("results", [])

            return []

        except Exception as e:
            logger.error(f"Web search failed: {e}")
            return []

    # =========================================================================
    # METRICS
    # =========================================================================

    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        return {
            **self._metrics,
            "endpoints_known": len(self._endpoints),
            "capabilities_indexed": len(self._capability_index),
            "tools_indexed": len(self._tool_index),
            "cache": self._cache.stats(),
        }

    def reset_metrics(self):
        """Reset metrics counters."""
        self._metrics = {
            "calls_total": 0,
            "calls_direct": 0,
            "calls_gateway": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "errors": 0,
        }


# =============================================================================
# FACTORY
# =============================================================================

# Global client instances per source MCP
_clients: Dict[str, InternalMCPClient] = {}


def get_internal_client(
    source_mcp_id: str,
    registry_url: Optional[str] = None,
) -> InternalMCPClient:
    """
    Get or create an InternalMCPClient for a source MCP.

    Uses a singleton pattern per source_mcp_id.
    """
    if source_mcp_id not in _clients:
        _clients[source_mcp_id] = InternalMCPClient(
            registry_url=registry_url or os.getenv("REGISTRY_URL", "http://localhost:2016"),
            source_mcp_id=source_mcp_id,
        )
    return _clients[source_mcp_id]


async def close_all_clients():
    """Close all client instances."""
    for client in _clients.values():
        await client.close()
    _clients.clear()
