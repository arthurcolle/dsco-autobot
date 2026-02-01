"""
Native MCP Base - Foundation for all native MCP servers.

Provides:
- Standard MCP protocol implementation
- Built-in cross-MCP communication
- Capability declaration and discovery
- Lifecycle management
- Metrics and observability
- Health monitoring

All native MCPs should inherit from NativeMCPBase.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import time
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Generic

import httpx
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
import uvicorn

logger = logging.getLogger(__name__)

T = TypeVar('T')


# =============================================================================
# CORE TYPES
# =============================================================================

class CapabilityType(str, Enum):
    """Standard capability types across MCPs."""
    # Search & Retrieval
    SEARCH = "search"
    EMBED = "embed"
    RERANK = "rerank"
    READ = "read"

    # Memory & Storage
    MEMORY = "memory"
    CACHE = "cache"
    PERSIST = "persist"

    # Execution
    EXECUTE = "execute"
    TASK = "task"
    PIPELINE = "pipeline"

    # Intelligence
    REASON = "reason"
    PLAN = "plan"
    LEARN = "learn"

    # Communication
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"
    NOTIFY = "notify"

    # Observation
    METRICS = "metrics"
    TRACE = "trace"
    LOG = "log"


@dataclass
class Capability:
    """A capability that an MCP provides or requires."""
    type: str  # CapabilityType or custom string
    version: str = "1.0"
    config: Dict[str, Any] = field(default_factory=dict)

    def matches(self, other: "Capability") -> bool:
        """Check if this capability satisfies a requirement."""
        if self.type != other.type:
            return False
        # Version compatibility (simple major version check)
        self_major = int(self.version.split(".")[0])
        other_major = int(other.version.split(".")[0])
        return self_major >= other_major


@dataclass
class MCPEndpoint:
    """A discovered MCP endpoint."""
    mcp_id: str
    name: str
    url: str
    port: int
    capabilities: List[Capability]
    tools: List[str] = field(default_factory=list)
    healthy: bool = True
    last_seen: float = field(default_factory=time.time)
    latency_ms: float = 0.0

    def has_capability(self, cap_type: str) -> bool:
        return any(c.type == cap_type for c in self.capabilities)

    def get_capability(self, cap_type: str) -> Optional[Capability]:
        for c in self.capabilities:
            if c.type == cap_type:
                return c
        return None


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker for endpoint health."""
    failure_threshold: int = 3
    recovery_timeout: float = 30.0
    failures: int = 0
    last_failure: float = 0.0
    state: CircuitState = CircuitState.CLOSED

    def record_success(self):
        self.failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self):
        self.failures += 1
        self.last_failure = time.time()
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN

    def can_proceed(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True  # HALF_OPEN


# =============================================================================
# CACHING
# =============================================================================

class ResultCache:
    """LRU cache with TTL for cross-MCP call results."""

    def __init__(self, max_size: int = 1000, default_ttl: float = 3600.0):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def _make_key(self, tool: str, args: Dict[str, Any]) -> str:
        """Create cache key from tool and arguments."""
        arg_str = json.dumps(args, sort_keys=True, default=str)
        return hashlib.sha256(f"{tool}:{arg_str}".encode()).hexdigest()[:24]

    def get(self, tool: str, args: Dict[str, Any]) -> Optional[Any]:
        key = self._make_key(tool, args)
        if key not in self._cache:
            self._misses += 1
            return None

        value, expires_at = self._cache[key]
        if time.time() > expires_at:
            del self._cache[key]
            self._misses += 1
            return None

        self._cache.move_to_end(key)
        self._hits += 1
        return value

    def set(self, tool: str, args: Dict[str, Any], value: Any, ttl: Optional[float] = None):
        key = self._make_key(tool, args)
        ttl = ttl or self.default_ttl

        while len(self._cache) >= self.max_size:
            self._cache.popitem(last=False)

        self._cache[key] = (value, time.time() + ttl)

    def invalidate(self, tool: str, args: Dict[str, Any]):
        key = self._make_key(tool, args)
        self._cache.pop(key, None)

    def clear(self):
        self._cache.clear()

    def stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / max(1, total),
        }


# =============================================================================
# CROSS-MCP COMMUNICATION
# =============================================================================

class MCPMesh:
    """
    Service mesh for cross-MCP communication.

    Provides:
    - Service discovery via registry
    - Intelligent routing
    - Caching
    - Circuit breaking
    - Metrics
    """

    def __init__(
        self,
        source_mcp_id: str,
        registry_url: str = "http://localhost:2016",
    ):
        self.source_mcp_id = source_mcp_id
        self.registry_url = registry_url.rstrip("/")

        # Discovered endpoints
        self._endpoints: Dict[str, MCPEndpoint] = {}
        self._by_capability: Dict[str, List[str]] = {}
        self._by_tool: Dict[str, List[str]] = {}
        self._circuits: Dict[str, CircuitBreaker] = {}

        # Cache
        self._cache = ResultCache(max_size=2000)

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        # Discovery state
        self._last_discovery: float = 0.0
        self._discovery_interval: float = 30.0

        # Metrics
        self._metrics = {
            "calls": 0,
            "direct_calls": 0,
            "gateway_calls": 0,
            "cache_hits": 0,
            "errors": 0,
            "total_latency_ms": 0.0,
        }

    async def start(self):
        """Initialize the mesh."""
        self._client = httpx.AsyncClient(timeout=30.0)
        await self.discover()

    async def stop(self):
        """Shutdown the mesh."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def discover(self, force: bool = False) -> Dict[str, MCPEndpoint]:
        """Discover available MCPs from registry."""
        if not self._client:
            return {}

        now = time.time()
        if not force and (now - self._last_discovery) < self._discovery_interval:
            return self._endpoints

        try:
            response = await self._client.get(f"{self.registry_url}/api/v1/native")
            if response.status_code != 200:
                return self._endpoints

            data = response.json()
            new_endpoints = {}
            new_by_cap: Dict[str, List[str]] = {}
            new_by_tool: Dict[str, List[str]] = {}

            for mcp_id, info in data.get("servers", {}).items():
                if mcp_id == self.source_mcp_id:
                    continue
                if info.get("status") != "running":
                    continue

                # Parse capabilities
                caps = [
                    Capability(type=c) if isinstance(c, str) else Capability(**c)
                    for c in info.get("capabilities", [])
                ]

                endpoint = MCPEndpoint(
                    mcp_id=mcp_id,
                    name=info.get("name", mcp_id),
                    url=f"http://localhost:{info.get('port', 0)}",
                    port=info.get("port", 0),
                    capabilities=caps,
                )

                # Preserve circuit breaker state
                if mcp_id in self._circuits:
                    pass  # Keep existing circuit
                else:
                    self._circuits[mcp_id] = CircuitBreaker()

                new_endpoints[mcp_id] = endpoint

                # Index by capability
                for cap in caps:
                    if cap.type not in new_by_cap:
                        new_by_cap[cap.type] = []
                    new_by_cap[cap.type].append(mcp_id)

            # Discover tools for each endpoint
            for mcp_id, endpoint in new_endpoints.items():
                try:
                    tools_resp = await self._client.get(f"{endpoint.url}/tools", timeout=5.0)
                    if tools_resp.status_code == 200:
                        tools_data = tools_resp.json()
                        endpoint.tools = [t.get("name") for t in tools_data.get("tools", [])]
                        for tool_name in endpoint.tools:
                            if tool_name not in new_by_tool:
                                new_by_tool[tool_name] = []
                            new_by_tool[tool_name].append(mcp_id)
                except Exception:
                    pass

            self._endpoints = new_endpoints
            self._by_capability = new_by_cap
            self._by_tool = new_by_tool
            self._last_discovery = now

        except Exception as e:
            logger.error(f"Discovery error: {e}")

        return self._endpoints

    def find_by_capability(self, cap_type: str) -> List[MCPEndpoint]:
        """Find healthy MCPs with a capability."""
        mcp_ids = self._by_capability.get(cap_type, [])
        return [
            self._endpoints[mid] for mid in mcp_ids
            if mid in self._endpoints and self._circuits.get(mid, CircuitBreaker()).can_proceed()
        ]

    def find_by_tool(self, tool_name: str) -> List[MCPEndpoint]:
        """Find healthy MCPs with a tool."""
        mcp_ids = self._by_tool.get(tool_name, [])
        return [
            self._endpoints[mid] for mid in mcp_ids
            if mid in self._endpoints and self._circuits.get(mid, CircuitBreaker()).can_proceed()
        ]

    async def call(
        self,
        tool: str,
        args: Dict[str, Any],
        target: Optional[str] = None,
        capability: Optional[str] = None,
        cache_ttl: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Call a tool on another MCP.

        Resolution order:
        1. Specific target MCP
        2. MCP with the tool
        3. MCP with the capability
        """
        self._metrics["calls"] += 1
        start_time = time.time()

        # Check cache
        if cache_ttl is not None or cache_ttl != 0:
            cached = self._cache.get(tool, args)
            if cached is not None:
                self._metrics["cache_hits"] += 1
                return cached

        # Resolve endpoint
        endpoint = await self._resolve_endpoint(tool, target, capability)
        if not endpoint:
            self._metrics["errors"] += 1
            return {"error": f"No endpoint found for tool: {tool}", "isError": True}

        # Try direct call
        circuit = self._circuits.get(endpoint.mcp_id, CircuitBreaker())
        result = None

        if circuit.can_proceed():
            try:
                result = await self._direct_call(endpoint, tool, args, timeout)
                circuit.record_success()
                self._metrics["direct_calls"] += 1
            except Exception as e:
                logger.debug(f"Direct call to {endpoint.mcp_id} failed: {e}")
                circuit.record_failure()

        # Fallback to gateway
        if result is None:
            try:
                result = await self._gateway_call(tool, args, endpoint.mcp_id, timeout)
                self._metrics["gateway_calls"] += 1
            except Exception as e:
                self._metrics["errors"] += 1
                return {"error": str(e), "isError": True}

        # Cache result
        if cache_ttl and not result.get("isError"):
            self._cache.set(tool, args, result, ttl=cache_ttl)

        # Record latency
        latency = (time.time() - start_time) * 1000
        self._metrics["total_latency_ms"] += latency

        return result

    async def _resolve_endpoint(
        self,
        tool: str,
        target: Optional[str],
        capability: Optional[str],
    ) -> Optional[MCPEndpoint]:
        """Resolve which endpoint to call."""
        await self.discover()

        if target:
            return self._endpoints.get(target)

        endpoints = self.find_by_tool(tool)
        if endpoints:
            return endpoints[0]

        if capability:
            endpoints = self.find_by_capability(capability)
            if endpoints:
                return endpoints[0]

        return None

    async def _direct_call(
        self,
        endpoint: MCPEndpoint,
        tool: str,
        args: Dict[str, Any],
        timeout: Optional[float],
    ) -> Dict[str, Any]:
        """Direct MCP call to endpoint."""
        request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": args},
        }

        response = await self._client.post(
            f"{endpoint.url}/mcp",
            json=request,
            timeout=timeout or 30.0,
        )

        if response.status_code != 200:
            raise Exception(f"MCP call failed: {response.status_code}")

        data = response.json()
        if "error" in data:
            raise Exception(data["error"].get("message", "Unknown error"))

        return data.get("result", {})

    async def _gateway_call(
        self,
        tool: str,
        args: Dict[str, Any],
        target: Optional[str],
        timeout: Optional[float],
    ) -> Dict[str, Any]:
        """Route call through registry gateway."""
        request = {
            "tool": tool,
            "inputs": args,
        }
        if target:
            request["options"] = {"backend_id": target}

        response = await self._client.post(
            f"{self.registry_url}/api/v1/gateway/execute",
            json=request,
            timeout=timeout or 30.0,
        )

        if response.status_code != 200:
            raise Exception(f"Gateway call failed: {response.status_code}")

        data = response.json()
        return data.get("result", data)

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    async def embed(self, text: str, cache: bool = True) -> Optional[List[float]]:
        """Get embedding vector for text."""
        cache_ttl = 86400.0 if cache else None  # 24 hours

        result = await self.call(
            tool="embed",
            args={"texts": [text]},
            capability="embed",
            cache_ttl=cache_ttl,
        )

        if result.get("isError"):
            return None

        # Parse result
        content = result.get("content", [])
        if content and isinstance(content, list):
            text_content = content[0].get("text", "{}")
            try:
                data = json.loads(text_content)
                embeddings = data.get("embeddings", [])
                return embeddings[0] if embeddings else None
            except json.JSONDecodeError:
                pass

        return None

    async def embed_batch(self, texts: List[str], cache: bool = True) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts with per-text caching."""
        results: List[Optional[List[float]]] = [None] * len(texts)
        uncached_indices: List[int] = []
        uncached_texts: List[str] = []

        # Check cache
        if cache:
            for i, text in enumerate(texts):
                cached = self._cache.get("embed", {"texts": [text]})
                if cached is not None:
                    try:
                        content = cached.get("content", [])
                        if content:
                            data = json.loads(content[0].get("text", "{}"))
                            embeddings = data.get("embeddings", [])
                            if embeddings:
                                results[i] = embeddings[0]
                                continue
                    except Exception:
                        pass
                uncached_indices.append(i)
                uncached_texts.append(text)
        else:
            uncached_indices = list(range(len(texts)))
            uncached_texts = texts

        # Batch call for uncached
        if uncached_texts:
            result = await self.call(
                tool="embed",
                args={"texts": uncached_texts},
                capability="embed",
            )

            if not result.get("isError"):
                try:
                    content = result.get("content", [])
                    if content:
                        data = json.loads(content[0].get("text", "{}"))
                        embeddings = data.get("embeddings", [])

                        for j, emb in enumerate(embeddings):
                            idx = uncached_indices[j]
                            results[idx] = emb

                            # Cache individual
                            if cache:
                                self._cache.set(
                                    "embed",
                                    {"texts": [uncached_texts[j]]},
                                    {"content": [{"text": json.dumps({"embeddings": [emb]})}]},
                                    ttl=86400.0,
                                )
                except Exception:
                    pass

        return results

    async def search(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """Search the web."""
        result = await self.call(
            tool="search",
            args={"query": query, "num_results": num_results},
            capability="search",
        )

        if result.get("isError"):
            return []

        try:
            content = result.get("content", [])
            if content:
                data = json.loads(content[0].get("text", "{}"))
                return data.get("results", [])
        except Exception:
            pass

        return []

    def get_metrics(self) -> Dict[str, Any]:
        """Get mesh metrics."""
        calls = self._metrics["calls"]
        return {
            **self._metrics,
            "avg_latency_ms": self._metrics["total_latency_ms"] / max(1, calls),
            "endpoints": len(self._endpoints),
            "capabilities": list(self._by_capability.keys()),
            "cache": self._cache.stats(),
        }


# =============================================================================
# VECTOR MATH
# =============================================================================

def cosine_similarity(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two vectors."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def euclidean_distance(a: List[float], b: List[float]) -> float:
    """Compute Euclidean distance between two vectors."""
    if not a or not b or len(a) != len(b):
        return float('inf')
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


# =============================================================================
# BASE MCP SERVER
# =============================================================================

class NativeMCPBase(ABC):
    """
    Base class for all native MCP servers.

    Provides:
    - Standard MCP protocol implementation
    - Built-in cross-MCP mesh
    - Lifecycle management
    - Health monitoring
    - Metrics

    Subclasses must implement:
    - SERVICE_ID: Unique identifier
    - SERVICE_NAME: Human-readable name
    - VERSION: Semantic version
    - CAPABILITIES: List of capabilities provided
    - TOOLS: List of tool definitions
    - Tool handler methods
    """

    # Subclasses must define these
    SERVICE_ID: str = "base-mcp"
    SERVICE_NAME: str = "Base MCP"
    VERSION: str = "1.0.0"
    MCP_VERSION: str = "2024-11-05"
    CAPABILITIES: List[str] = []
    REQUIRES: List[str] = []  # Soft dependencies
    DEPENDS: List[str] = []   # Hard dependencies
    TOOLS: List[Dict[str, Any]] = []

    def __init__(
        self,
        port: int,
        data_dir: Optional[str] = None,
        registry_url: Optional[str] = None,
    ):
        self.port = port
        self.data_dir = Path(data_dir) if data_dir else Path(f"data/{self.SERVICE_ID}")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self._registry_url = registry_url or os.getenv("REGISTRY_URL", "http://localhost:2016")

        # Cross-MCP mesh
        self._mesh: Optional[MCPMesh] = None

        # Capability availability
        self._available_capabilities: Set[str] = set()

        # FastAPI app
        self.app = self._create_app()

        # Lifecycle hooks
        self._on_start_hooks: List[Callable] = []
        self._on_stop_hooks: List[Callable] = []
        self._on_capability_available_hooks: Dict[str, List[Callable]] = {}

    @property
    def mesh(self) -> MCPMesh:
        """Get the MCP mesh (lazy initialization)."""
        if self._mesh is None:
            self._mesh = MCPMesh(
                source_mcp_id=self.SERVICE_ID,
                registry_url=self._registry_url,
            )
        return self._mesh

    def on_start(self, hook: Callable):
        """Register a startup hook."""
        self._on_start_hooks.append(hook)

    def on_stop(self, hook: Callable):
        """Register a shutdown hook."""
        self._on_stop_hooks.append(hook)

    def on_capability_available(self, capability: str, hook: Callable):
        """Register a hook for when a capability becomes available."""
        if capability not in self._on_capability_available_hooks:
            self._on_capability_available_hooks[capability] = []
        self._on_capability_available_hooks[capability].append(hook)

    async def _check_capabilities(self):
        """Check which required capabilities are available."""
        await self.mesh.discover()

        for cap in self.REQUIRES + self.DEPENDS:
            was_available = cap in self._available_capabilities
            is_available = len(self.mesh.find_by_capability(cap)) > 0

            if is_available and not was_available:
                self._available_capabilities.add(cap)
                logger.info(f"Capability now available: {cap}")

                # Fire hooks
                for hook in self._on_capability_available_hooks.get(cap, []):
                    try:
                        if asyncio.iscoroutinefunction(hook):
                            await hook()
                        else:
                            hook()
                    except Exception as e:
                        logger.error(f"Capability hook error: {e}")

            elif not is_available and was_available:
                self._available_capabilities.discard(cap)
                logger.warning(f"Capability no longer available: {cap}")

    def has_capability(self, cap: str) -> bool:
        """Check if a capability is available."""
        return cap in self._available_capabilities

    def _create_app(self) -> FastAPI:
        """Create the FastAPI application."""
        app = FastAPI(
            title=self.SERVICE_NAME,
            description=f"Native MCP Server - {self.SERVICE_NAME}",
            version=self.VERSION,
        )

        @app.on_event("startup")
        async def startup():
            await self.mesh.start()
            await self._check_capabilities()

            # Check hard dependencies
            for dep in self.DEPENDS:
                if dep not in self._available_capabilities:
                    logger.error(f"Hard dependency not available: {dep}")

            # Fire startup hooks
            for hook in self._on_start_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    logger.error(f"Startup hook error: {e}")

            logger.info(f"{self.SERVICE_NAME} started on port {self.port}")

        @app.on_event("shutdown")
        async def shutdown():
            # Fire shutdown hooks
            for hook in self._on_stop_hooks:
                try:
                    if asyncio.iscoroutinefunction(hook):
                        await hook()
                    else:
                        hook()
                except Exception as e:
                    logger.error(f"Shutdown hook error: {e}")

            await self.mesh.stop()
            logger.info(f"{self.SERVICE_NAME} stopped")

        @app.get("/")
        async def root():
            return {
                "service": self.SERVICE_NAME,
                "id": self.SERVICE_ID,
                "version": self.VERSION,
                "mcp_version": self.MCP_VERSION,
                "status": "running",
                "port": self.port,
                "capabilities": self.CAPABILITIES,
                "requires": self.REQUIRES,
                "available_capabilities": list(self._available_capabilities),
            }

        @app.get("/health")
        async def health():
            # Check hard dependencies
            deps_ok = all(d in self._available_capabilities for d in self.DEPENDS)
            return {
                "status": "healthy" if deps_ok else "degraded",
                "service": self.SERVICE_ID,
                "dependencies_met": deps_ok,
            }

        @app.get("/tools")
        async def list_tools():
            return {"tools": self.TOOLS}

        @app.get("/capabilities")
        async def list_capabilities():
            return {
                "provides": self.CAPABILITIES,
                "requires": self.REQUIRES,
                "depends": self.DEPENDS,
                "available": list(self._available_capabilities),
            }

        @app.get("/mesh")
        async def mesh_status():
            return self.mesh.get_metrics()

        @app.post("/mcp")
        async def mcp_handler(request: Request):
            body = await request.json()
            return await self._handle_mcp_request(body)

        return app

    async def _handle_mcp_request(self, body: Dict[str, Any]) -> JSONResponse:
        """Handle MCP JSON-RPC requests."""
        method = body.get("method", "")
        params = body.get("params", {})
        request_id = body.get("id")

        try:
            if method == "initialize":
                result = {
                    "protocolVersion": self.MCP_VERSION,
                    "serverInfo": {
                        "name": self.SERVICE_NAME,
                        "version": self.VERSION,
                    },
                    "capabilities": {
                        "tools": {"listChanged": True},
                    },
                }
            elif method == "tools/list":
                result = {"tools": self.TOOLS}
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await self._execute_tool(tool_name, arguments)
            else:
                return JSONResponse({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Unknown method: {method}"},
                })

            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": result,
            })

        except Exception as e:
            logger.error(f"MCP error: {e}")
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(e)},
            })

    async def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool by name."""
        # Look for method named _tool_{tool_name}
        handler_name = f"_tool_{tool_name}"
        handler = getattr(self, handler_name, None)

        if handler is None:
            return {
                "content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}],
                "isError": True,
            }

        return await handler(arguments)

    def run(self):
        """Run the server."""
        uvicorn.run(self.app, host="0.0.0.0", port=self.port)

    # =========================================================================
    # STANDARD TOOLS (always available)
    # =========================================================================

    async def _tool__connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Test connectivity and show mesh status."""
        await self._check_capabilities()

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "service": self.SERVICE_ID,
                    "status": "connected",
                    "version": self.VERSION,
                    "capabilities": {
                        "provides": self.CAPABILITIES,
                        "requires": self.REQUIRES,
                        "available": list(self._available_capabilities),
                    },
                    "mesh": self.mesh.get_metrics(),
                }, indent=2)
            }]
        }

    async def _tool__mesh_call(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Make a cross-MCP call through the mesh."""
        tool = args.get("tool")
        tool_args = args.get("arguments", {})
        target = args.get("target")
        capability = args.get("capability")
        cache = args.get("cache", True)

        if not tool:
            return {
                "content": [{"type": "text", "text": "Missing required 'tool' argument"}],
                "isError": True,
            }

        result = await self.mesh.call(
            tool=tool,
            args=tool_args,
            target=target,
            capability=capability,
            cache_ttl=3600.0 if cache else None,
        )

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(result, indent=2, default=str)
            }]
        }


# =============================================================================
# EMBEDDING MIXIN
# =============================================================================

class EmbeddingMixin:
    """
    Mixin that adds embedding capabilities to an MCP.

    Requires the mesh to have access to an MCP with 'embed' capability.
    """

    _embedding_cache: Dict[str, List[float]] = {}

    async def get_embedding(self, text: str, cache: bool = True) -> Optional[List[float]]:
        """Get embedding for text."""
        if not hasattr(self, 'mesh'):
            return None

        # Check local cache
        if cache:
            cache_key = hashlib.sha256(text.encode()).hexdigest()[:16]
            if cache_key in self._embedding_cache:
                return self._embedding_cache[cache_key]

        result = await self.mesh.embed(text, cache=cache)

        if result and cache:
            self._embedding_cache[cache_key] = result

        return result

    async def get_embeddings(self, texts: List[str], cache: bool = True) -> List[Optional[List[float]]]:
        """Get embeddings for multiple texts."""
        if not hasattr(self, 'mesh'):
            return [None] * len(texts)

        return await self.mesh.embed_batch(texts, cache=cache)

    def compute_similarity(self, a: List[float], b: List[float]) -> float:
        """Compute cosine similarity between embeddings."""
        return cosine_similarity(a, b)

    async def find_similar(
        self,
        query_embedding: List[float],
        candidates: List[Tuple[str, List[float]]],
        top_k: int = 10,
        min_similarity: float = 0.0,
    ) -> List[Tuple[str, float]]:
        """Find most similar items from candidates."""
        scored = []
        for item_id, embedding in candidates:
            sim = cosine_similarity(query_embedding, embedding)
            if sim >= min_similarity:
                scored.append((item_id, sim))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Core types
    "CapabilityType",
    "Capability",
    "MCPEndpoint",
    "CircuitBreaker",
    "CircuitState",

    # Communication
    "MCPMesh",
    "ResultCache",

    # Base class
    "NativeMCPBase",

    # Mixins
    "EmbeddingMixin",

    # Utilities
    "cosine_similarity",
    "euclidean_distance",
]
