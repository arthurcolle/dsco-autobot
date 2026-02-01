"""
Registry Client - Client for central tool registry.

Allows services to:
- Register their tools
- Discover tools from other services
- Route execution requests

The registry abstracts vendor details - clients only see tool IDs.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# =============================================================================
# MODELS
# =============================================================================

class RegisteredTool(BaseModel):
    """Tool as stored in the registry."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str
    input_schema: Dict[str, Any]

    # Service info (abstracted - no vendor details)
    service_id: str
    service_url: str

    # Classification
    category: Optional[str] = None
    tags: List[str] = []

    # Metadata
    version: str = "1.0.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Status
    enabled: bool = True
    healthy: bool = True
    last_health_check: Optional[datetime] = None


class ServiceRegistration(BaseModel):
    """Service registration in the registry."""
    service_id: str
    service_name: str
    service_url: str
    description: str = ""

    # Tools provided
    tools: List[str] = []  # Tool IDs

    # Health
    healthy: bool = True
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)

    # Metadata
    version: str = "1.0.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ExecuteRequest(BaseModel):
    """Request to execute a tool via registry."""
    tool_id: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None
    timeout_ms: int = 30000


class ExecuteResponse(BaseModel):
    """Response from tool execution via registry."""
    request_id: str
    tool_id: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float
    service_id: str  # Which service handled it


# =============================================================================
# REGISTRY CLIENT
# =============================================================================

class RegistryClient:
    """
    Client for interacting with the central tool registry.

    The registry is the single source of truth for:
    - What tools exist
    - Which service provides each tool
    - How to route execution requests

    Clients never see vendor details - they see tool IDs and service IDs.

    Usage:
        client = RegistryClient("http://registry:8000")

        # Discover tools
        tools = await client.list_tools(category="search")

        # Execute a tool
        result = await client.execute("tool-123", {"query": "AI"})

        # Register a service's tools
        await client.register_service(
            service_id="search-service",
            service_url="http://search:8001",
            tools=[tool1, tool2],
        )
    """

    def __init__(
        self,
        registry_url: str,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.registry_url = registry_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

        # Local cache
        self._tool_cache: Dict[str, RegisteredTool] = {}
        self._cache_ttl = timedelta(minutes=5)
        self._cache_updated: Optional[datetime] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            headers = {}
            if self.api_key:
                headers["X-API-Key"] = self.api_key
            self._client = httpx.AsyncClient(
                base_url=self.registry_url,
                headers=headers,
                timeout=self.timeout,
            )
        return self._client

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # TOOL DISCOVERY
    # =========================================================================

    async def list_tools(
        self,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        search: Optional[str] = None,
        refresh: bool = False,
    ) -> List[RegisteredTool]:
        """
        List available tools from registry.

        Args:
            category: Filter by category
            tags: Filter by tags (any match)
            search: Search in name/description
            refresh: Force refresh from registry
        """
        # Check cache
        if not refresh and self._cache_valid():
            tools = list(self._tool_cache.values())
            return self._filter_tools(tools, category, tags, search)

        # Fetch from registry
        client = await self._get_client()
        params = {}
        if category:
            params["category"] = category
        if tags:
            params["tags"] = ",".join(tags)
        if search:
            params["search"] = search

        try:
            response = await client.get("/tools", params=params)
            response.raise_for_status()
            data = response.json()

            tools = [RegisteredTool(**t) for t in data.get("tools", data)]

            # Update cache
            self._tool_cache = {t.id: t for t in tools}
            self._cache_updated = datetime.utcnow()

            return tools

        except httpx.HTTPError as e:
            logger.error(f"Failed to list tools: {e}")
            # Return cached data if available
            if self._tool_cache:
                return self._filter_tools(
                    list(self._tool_cache.values()),
                    category, tags, search
                )
            raise

    async def get_tool(self, tool_id: str) -> Optional[RegisteredTool]:
        """Get a specific tool by ID."""
        # Check cache first
        if tool_id in self._tool_cache:
            return self._tool_cache[tool_id]

        client = await self._get_client()
        try:
            response = await client.get(f"/tools/{tool_id}")
            if response.status_code == 404:
                return None
            response.raise_for_status()

            tool = RegisteredTool(**response.json())
            self._tool_cache[tool.id] = tool
            return tool

        except httpx.HTTPError as e:
            logger.error(f"Failed to get tool {tool_id}: {e}")
            raise

    async def search_tools(self, query: str) -> List[RegisteredTool]:
        """Search tools by name or description."""
        return await self.list_tools(search=query)

    # =========================================================================
    # TOOL EXECUTION
    # =========================================================================

    async def execute(
        self,
        tool_id: str,
        inputs: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        timeout_ms: int = 30000,
    ) -> ExecuteResponse:
        """
        Execute a tool via the registry.

        The registry routes to the appropriate service.
        Clients don't need to know which service handles it.
        """
        client = await self._get_client()

        request = ExecuteRequest(
            tool_id=tool_id,
            inputs=inputs,
            idempotency_key=idempotency_key,
            timeout_ms=timeout_ms,
        )

        try:
            response = await client.post(
                "/execute",
                json=request.model_dump(),
                timeout=timeout_ms / 1000 + 5,  # Add buffer
            )
            response.raise_for_status()
            return ExecuteResponse(**response.json())

        except httpx.HTTPError as e:
            logger.error(f"Failed to execute tool {tool_id}: {e}")
            raise

    async def execute_by_name(
        self,
        tool_name: str,
        inputs: Dict[str, Any],
        service_id: Optional[str] = None,
    ) -> ExecuteResponse:
        """
        Execute a tool by name.

        If multiple services provide the same tool name,
        specify service_id to disambiguate.
        """
        # Find tool ID
        tools = await self.list_tools()
        matches = [t for t in tools if t.name == tool_name]

        if service_id:
            matches = [t for t in matches if t.service_id == service_id]

        if not matches:
            raise ValueError(f"Tool not found: {tool_name}")

        if len(matches) > 1:
            services = [t.service_id for t in matches]
            raise ValueError(
                f"Multiple services provide '{tool_name}': {services}. "
                "Specify service_id to disambiguate."
            )

        return await self.execute(matches[0].id, inputs)

    # =========================================================================
    # SERVICE REGISTRATION
    # =========================================================================

    async def register_service(
        self,
        service_id: str,
        service_name: str,
        service_url: str,
        tools: List[Dict[str, Any]],
        description: str = "",
    ) -> ServiceRegistration:
        """
        Register a service and its tools with the registry.

        Args:
            service_id: Unique service identifier
            service_name: Human-readable name
            service_url: URL to reach the service
            tools: List of tool definitions
            description: Service description
        """
        client = await self._get_client()

        payload = {
            "service_id": service_id,
            "service_name": service_name,
            "service_url": service_url,
            "description": description,
            "tools": tools,
        }

        try:
            response = await client.post("/services", json=payload)
            response.raise_for_status()
            return ServiceRegistration(**response.json())

        except httpx.HTTPError as e:
            logger.error(f"Failed to register service: {e}")
            raise

    async def register_tool(
        self,
        name: str,
        description: str,
        input_schema: Dict[str, Any],
        service_id: str,
        service_url: str,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> RegisteredTool:
        """Register a single tool with the registry."""
        client = await self._get_client()

        payload = {
            "name": name,
            "description": description,
            "input_schema": input_schema,
            "service_id": service_id,
            "service_url": service_url,
            "category": category,
            "tags": tags or [],
        }

        try:
            response = await client.post("/tools", json=payload)
            response.raise_for_status()

            tool = RegisteredTool(**response.json())
            self._tool_cache[tool.id] = tool
            return tool

        except httpx.HTTPError as e:
            logger.error(f"Failed to register tool: {e}")
            raise

    async def unregister_service(self, service_id: str):
        """Unregister a service and all its tools."""
        client = await self._get_client()

        try:
            response = await client.delete(f"/services/{service_id}")
            response.raise_for_status()

            # Clear cache entries for this service
            self._tool_cache = {
                k: v for k, v in self._tool_cache.items()
                if v.service_id != service_id
            }

        except httpx.HTTPError as e:
            logger.error(f"Failed to unregister service: {e}")
            raise

    async def heartbeat(self, service_id: str) -> bool:
        """Send heartbeat to keep service registration alive."""
        client = await self._get_client()

        try:
            response = await client.post(f"/services/{service_id}/heartbeat")
            return response.status_code == 200
        except httpx.HTTPError:
            return False

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _cache_valid(self) -> bool:
        """Check if local cache is still valid."""
        if not self._cache_updated:
            return False
        return datetime.utcnow() - self._cache_updated < self._cache_ttl

    def _filter_tools(
        self,
        tools: List[RegisteredTool],
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        search: Optional[str] = None,
    ) -> List[RegisteredTool]:
        """Filter tools locally."""
        result = tools

        if category:
            result = [t for t in result if t.category == category]

        if tags:
            result = [
                t for t in result
                if any(tag in t.tags for tag in tags)
            ]

        if search:
            search_lower = search.lower()
            result = [
                t for t in result
                if search_lower in t.name.lower()
                or search_lower in t.description.lower()
            ]

        return result


# =============================================================================
# IN-MEMORY REGISTRY (for development/testing)
# =============================================================================

class InMemoryRegistry:
    """
    In-memory implementation of the registry for development.

    In production, this would be a separate service with persistent storage.
    """

    def __init__(self):
        self.tools: Dict[str, RegisteredTool] = {}
        self.services: Dict[str, ServiceRegistration] = {}
        self._service_clients: Dict[str, str] = {}  # service_id -> url

    def register_tool(self, tool: RegisteredTool):
        """Register a tool."""
        self.tools[tool.id] = tool
        self._service_clients[tool.service_id] = tool.service_url

    def register_service(self, service: ServiceRegistration):
        """Register a service."""
        self.services[service.service_id] = service
        self._service_clients[service.service_id] = service.service_url

    def get_tool(self, tool_id: str) -> Optional[RegisteredTool]:
        """Get a tool by ID."""
        return self.tools.get(tool_id)

    def list_tools(
        self,
        category: Optional[str] = None,
        service_id: Optional[str] = None,
    ) -> List[RegisteredTool]:
        """List tools with optional filters."""
        tools = list(self.tools.values())

        if category:
            tools = [t for t in tools if t.category == category]
        if service_id:
            tools = [t for t in tools if t.service_id == service_id]

        return tools

    async def execute(
        self,
        tool_id: str,
        inputs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a tool by routing to the appropriate service."""
        tool = self.tools.get(tool_id)
        if not tool:
            raise ValueError(f"Tool not found: {tool_id}")

        service_url = self._service_clients.get(tool.service_id)
        if not service_url:
            raise ValueError(f"Service not found: {tool.service_id}")

        # Route to service
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{service_url}/execute",
                json={
                    "tool": tool.name,
                    "inputs": inputs,
                },
            )
            response.raise_for_status()
            return response.json()
