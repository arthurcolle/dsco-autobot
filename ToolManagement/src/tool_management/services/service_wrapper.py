"""
Service Wrapper - White-label wrapper for vendor integrations.

Provides:
- Vendor abstraction (clients never see Jina/Parallel)
- Durable request/response storage
- Usage tracking for billing
- API key validation
- Rate limiting hooks
- Central registry integration
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .durable_store import DurableStore, ToolRequest, ToolResponse, RequestStatus
from ..integrations.mcp_adapter import MCPAdapter

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class ServiceConfig:
    """Configuration for a white-label service."""
    # Identity
    service_id: str
    service_name: str
    description: str = ""

    # Branding (what clients see)
    display_name: str = ""
    version: str = "1.0.0"

    # Storage
    db_path: str = "./data/requests.db"

    # Registry
    registry_url: Optional[str] = None
    register_on_startup: bool = True

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Auth (hooks for your auth system)
    require_api_key: bool = True
    api_key_header: str = "X-API-Key"

    # Rate limiting (hooks)
    rate_limit_requests: int = 1000  # per minute
    rate_limit_tokens: int = 100000  # per minute

    # Billing
    track_usage: bool = True
    cost_per_request_cents: int = 0
    cost_per_1k_tokens_cents: int = 0

    # Vendor (internal only - never exposed)
    _vendor_name: str = ""
    _vendor_adapter_class: Optional[Type[MCPAdapter]] = None


# =============================================================================
# REQUEST/RESPONSE MODELS (Client-facing - no vendor details)
# =============================================================================

class ExecuteRequest(BaseModel):
    """Client request to execute a tool."""
    tool: str = Field(..., description="Tool name to execute")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Tool inputs")
    idempotency_key: Optional[str] = Field(None, description="Idempotency key for deduplication")
    metadata: Optional[Dict[str, Any]] = None


class ExecuteResponse(BaseModel):
    """Response from tool execution (no vendor details)."""
    request_id: str
    tool: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float
    usage: Optional[Dict[str, int]] = None


class ToolInfo(BaseModel):
    """Tool information (sanitized - no vendor details)."""
    name: str
    description: str
    input_schema: Dict[str, Any]
    category: Optional[str] = None
    tags: List[str] = []


class ServiceInfo(BaseModel):
    """Service information (what clients see)."""
    service_id: str
    name: str
    description: str
    version: str
    tools: List[ToolInfo]
    endpoints: Dict[str, str]


# =============================================================================
# SERVICE WRAPPER
# =============================================================================

class ServiceWrapper:
    """
    White-label wrapper for a vendor integration.

    Hides all vendor details from clients. They see:
    - Your service name/branding
    - Your tool names (can be renamed)
    - Your API endpoints
    - No vendor URLs, IDs, or details

    Usage:
        from tool_management.integrations import JinaAdapter
        from tool_management.services import ServiceWrapper, ServiceConfig

        config = ServiceConfig(
            service_id="search-service",
            service_name="Search API",
            display_name="Acme Search",
            port=8001,
        )

        wrapper = ServiceWrapper(config, JinaAdapter())
        app = wrapper.create_app()
        uvicorn.run(app, port=8001)
    """

    def __init__(
        self,
        config: ServiceConfig,
        adapter: MCPAdapter,
        tool_name_map: Optional[Dict[str, str]] = None,
    ):
        self.config = config
        self.adapter = adapter
        self.tool_name_map = tool_name_map or {}  # vendor_name -> our_name

        self._store: Optional[DurableStore] = None
        self._app: Optional[FastAPI] = None
        self._tool_cache: Dict[str, ToolInfo] = {}

    @property
    def service_id(self) -> str:
        return self.config.service_id

    def _map_tool_name(self, vendor_name: str) -> str:
        """Map vendor tool name to our name."""
        return self.tool_name_map.get(vendor_name, vendor_name)

    def _reverse_map_tool_name(self, our_name: str) -> str:
        """Map our tool name back to vendor name."""
        reverse_map = {v: k for k, v in self.tool_name_map.items()}
        return reverse_map.get(our_name, our_name)

    async def _validate_api_key(self, api_key: str) -> Dict[str, Any]:
        """
        Validate API key and return client info.
        Override this with your auth system.
        """
        # Default: accept any non-empty key (replace with real auth)
        if not api_key:
            raise HTTPException(401, "API key required")

        # Return client info (integrate with your auth/billing system)
        return {
            "client_id": hashlib.sha256(api_key.encode()).hexdigest()[:16],
            "organization_id": None,
            "rate_limit": self.config.rate_limit_requests,
        }

    async def _track_usage(
        self,
        request: ToolRequest,
        response: ToolResponse,
        client_info: Dict[str, Any],
    ):
        """
        Track usage for billing.
        Override this to integrate with your billing system.
        """
        # Calculate cost
        cost = self.config.cost_per_request_cents
        if self.config.cost_per_1k_tokens_cents > 0:
            total_tokens = response.tokens_input + response.tokens_output
            cost += (total_tokens / 1000) * self.config.cost_per_1k_tokens_cents

        request.cost_cents = int(cost)
        await self._store.update_request(request)

        logger.debug(f"Usage tracked: {request.id} - {cost} cents")

    def create_app(self) -> FastAPI:
        """Create FastAPI application."""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info(f"Starting {self.config.service_name}...")

            # Initialize store
            self._store = DurableStore(self.config.db_path)
            await self._store.initialize()

            # Connect adapter
            await self.adapter.connect()

            # Build tool cache
            for tool in self.adapter.get_discovered_tools():
                our_name = self._map_tool_name(tool.name)
                self._tool_cache[our_name] = ToolInfo(
                    name=our_name,
                    description=tool.description,
                    input_schema=tool.input_schema,
                    category=self.adapter.config.category,
                    tags=self.adapter.config.tags,
                )

            # Register with central registry
            if self.config.registry_url and self.config.register_on_startup:
                await self._register_with_registry()

            logger.info(f"{self.config.service_name} ready with {len(self._tool_cache)} tools")

            yield

            # Shutdown
            logger.info(f"Shutting down {self.config.service_name}...")
            await self.adapter.disconnect()
            await self._store.close()

        app = FastAPI(
            title=self.config.display_name or self.config.service_name,
            description=self.config.description,
            version=self.config.version,
            lifespan=lifespan,
        )

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        self._app = app
        self._add_routes(app)
        return app

    def _add_routes(self, app: FastAPI):
        """Add API routes."""

        # Dependency for API key validation
        async def get_client(
            x_api_key: Optional[str] = Header(None, alias=self.config.api_key_header)
        ) -> Dict[str, Any]:
            if self.config.require_api_key:
                if not x_api_key:
                    raise HTTPException(401, "API key required")
                return await self._validate_api_key(x_api_key)
            return {"client_id": "anonymous"}

        @app.get("/")
        async def root():
            return ServiceInfo(
                service_id=self.config.service_id,
                name=self.config.display_name or self.config.service_name,
                description=self.config.description,
                version=self.config.version,
                tools=list(self._tool_cache.values()),
                endpoints={
                    "tools": "/tools",
                    "execute": "/execute",
                    "requests": "/requests",
                    "usage": "/usage",
                }
            )

        @app.get("/health")
        async def health():
            return {
                "status": "healthy",
                "service_id": self.config.service_id,
                "connected": self.adapter._connected,
                "tool_count": len(self._tool_cache),
            }

        @app.get("/tools", response_model=List[ToolInfo])
        async def list_tools(client: Dict = Depends(get_client)):
            return list(self._tool_cache.values())

        @app.get("/tools/{tool_name}", response_model=ToolInfo)
        async def get_tool(tool_name: str, client: Dict = Depends(get_client)):
            if tool_name not in self._tool_cache:
                raise HTTPException(404, f"Tool not found: {tool_name}")
            return self._tool_cache[tool_name]

        @app.post("/execute", response_model=ExecuteResponse)
        async def execute(
            body: ExecuteRequest,
            client: Dict = Depends(get_client),
            x_api_key: Optional[str] = Header(None, alias=self.config.api_key_header),
        ):
            tool_name = body.tool

            if tool_name not in self._tool_cache:
                raise HTTPException(404, f"Tool not found: {tool_name}")

            # Check idempotency
            if body.idempotency_key:
                existing = await self._store.get_by_idempotency_key(
                    body.idempotency_key,
                    client["client_id"]
                )
                if existing:
                    response = await self._store.get_response(existing.id)
                    if response:
                        return ExecuteResponse(
                            request_id=existing.id,
                            tool=tool_name,
                            status=response.status.value,
                            result=response.result,
                            error=response.error,
                            execution_time_ms=response.execution_time_ms,
                            usage={
                                "tokens_input": response.tokens_input,
                                "tokens_output": response.tokens_output,
                            } if self.config.track_usage else None,
                        )

            # Create request record
            request = ToolRequest(
                idempotency_key=body.idempotency_key,
                tool_name=tool_name,
                tool_id=f"{self.config.service_id}:{tool_name}",
                service_id=self.config.service_id,
                inputs=body.inputs,
                client_id=client["client_id"],
                organization_id=client.get("organization_id"),
                api_key_id=hashlib.sha256(x_api_key.encode()).hexdigest()[:8] if x_api_key else None,
                status=RequestStatus.PENDING,
                _vendor=self.adapter.name,  # Internal only
            )
            await self._store.save_request(request)

            # Execute
            start_time = datetime.utcnow()
            request.status = RequestStatus.RUNNING
            request.started_at = start_time
            await self._store.update_request(request)

            try:
                # Map to vendor tool name
                vendor_tool = self._reverse_map_tool_name(tool_name)
                result = await self.adapter.execute_tool(vendor_tool, body.inputs)

                elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000

                # Create response
                response = ToolResponse(
                    request_id=request.id,
                    status=RequestStatus.COMPLETED,
                    result=result,
                    execution_time_ms=elapsed,
                )
                await self._store.save_response(response)

                # Update request
                request.status = RequestStatus.COMPLETED
                request.completed_at = datetime.utcnow()
                await self._store.update_request(request)

                # Track usage
                if self.config.track_usage:
                    await self._track_usage(request, response, client)

                return ExecuteResponse(
                    request_id=request.id,
                    tool=tool_name,
                    status="completed",
                    result=result,
                    execution_time_ms=elapsed,
                    usage={
                        "tokens_input": response.tokens_input,
                        "tokens_output": response.tokens_output,
                    } if self.config.track_usage else None,
                )

            except Exception as e:
                elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000
                logger.error(f"Tool execution failed: {e}")

                response = ToolResponse(
                    request_id=request.id,
                    status=RequestStatus.FAILED,
                    error=str(e),
                    execution_time_ms=elapsed,
                )
                await self._store.save_response(response)

                request.status = RequestStatus.FAILED
                request.completed_at = datetime.utcnow()
                await self._store.update_request(request)

                return ExecuteResponse(
                    request_id=request.id,
                    tool=tool_name,
                    status="failed",
                    error=str(e),
                    execution_time_ms=elapsed,
                )

        @app.get("/requests/{request_id}")
        async def get_request(request_id: str, client: Dict = Depends(get_client)):
            request = await self._store.get_request(request_id)
            if not request:
                raise HTTPException(404, "Request not found")

            # Verify ownership
            if request.client_id != client["client_id"]:
                raise HTTPException(403, "Access denied")

            response = await self._store.get_response(request_id)

            return {
                "request_id": request.id,
                "tool": request.tool_name,
                "inputs": request.inputs,
                "status": request.status.value,
                "created_at": request.created_at.isoformat(),
                "completed_at": request.completed_at.isoformat() if request.completed_at else None,
                "result": response.result if response else None,
                "error": response.error if response else None,
            }

        @app.get("/requests")
        async def list_requests(
            client: Dict = Depends(get_client),
            tool: Optional[str] = None,
            status: Optional[str] = None,
            limit: int = 50,
        ):
            status_enum = RequestStatus(status) if status else None

            requests = await self._store.get_requests(
                client_id=client["client_id"],
                tool_name=tool,
                status=status_enum,
                limit=limit,
            )

            return {
                "requests": [
                    {
                        "request_id": r.id,
                        "tool": r.tool_name,
                        "status": r.status.value,
                        "created_at": r.created_at.isoformat(),
                    }
                    for r in requests
                ]
            }

        @app.get("/usage")
        async def get_usage(
            client: Dict = Depends(get_client),
            since: Optional[str] = None,
        ):
            since_dt = datetime.fromisoformat(since) if since else None

            summary = await self._store.get_usage_summary(
                client_id=client["client_id"],
                since=since_dt,
            )

            return summary

    async def _register_with_registry(self):
        """Register service and tools with central registry."""
        if not self.config.registry_url:
            return

        try:
            import httpx
            import socket

            # Determine service URL
            host = self.config.host
            if host == "0.0.0.0":
                host = os.getenv("SERVICE_HOST", socket.gethostname())
            service_url = f"http://{host}:{self.config.port}"

            async with httpx.AsyncClient(timeout=10.0) as client:
                # First register the service itself
                service_payload = {
                    "service_id": self.config.service_id,  # Human-readable ID for heartbeats
                    "name": self.config.service_name,
                    "description": self.config.description,
                    "version": self.config.version,
                    "base_url": service_url,
                    "service_type": "mcp",
                    "tool_names": list(self._tool_cache.keys()),
                    "capabilities": {
                        "tools": list(self._tool_cache.keys()),
                        "features": ["execute", "list_tools"],
                    },
                    "health_check": {
                        "endpoint": "/health",
                        "interval": 30,
                    },
                    "metadata": {
                        "vendor": self.config._vendor_name,
                    },
                }

                try:
                    resp = await client.post(
                        f"{self.config.registry_url}/api/v1/services",
                        json=service_payload,
                    )
                    if resp.status_code in (200, 201):
                        logger.info(f"Registered service {self.config.service_id} with registry")
                    elif resp.status_code == 409:
                        # Already exists - update
                        await client.patch(
                            f"{self.config.registry_url}/api/v1/services/{self.config.service_id}",
                            json={"tool_names": list(self._tool_cache.keys())},
                        )
                        logger.info(f"Updated service {self.config.service_id} in registry")
                except Exception as e:
                    logger.warning(f"Service registration failed: {e}")

                # Register each tool
                for tool in self._tool_cache.values():
                    tool_payload = {
                        "name": f"{self.config.service_id}::{tool.name}",
                        "description": tool.description,
                        "version": self.config.version,
                        "inputs": [
                            {
                                "name": k,
                                "type": v.get("type", "string"),
                                "description": v.get("description", ""),
                                "required": k in tool.input_schema.get("required", []),
                            }
                            for k, v in tool.input_schema.get("properties", {}).items()
                        ] if isinstance(tool.input_schema, dict) else [],
                        "tags": tool.tags or [],
                        "category": tool.category,
                        "service_url": service_url,
                        "endpoint": "/execute",
                        "metadata": {
                            "service_id": self.config.service_id,
                            "original_name": tool.name,
                        },
                    }

                    try:
                        resp = await client.post(
                            f"{self.config.registry_url}/api/v1/tools",
                            json=tool_payload,
                        )
                        if resp.status_code in (200, 201, 409):
                            logger.debug(f"Registered tool {tool.name}")
                    except Exception as e:
                        logger.warning(f"Tool registration failed for {tool.name}: {e}")

                logger.info(f"Registered {len(self._tool_cache)} tools with registry at {self.config.registry_url}")

                # Start heartbeat task
                asyncio.create_task(self._heartbeat_loop())

        except Exception as e:
            logger.warning(f"Failed to register with registry: {e}")

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to registry."""
        import httpx

        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds

                if not self.config.registry_url:
                    break

                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.post(
                        f"{self.config.registry_url}/api/v1/services/{self.config.service_id}/heartbeat",
                    )
                    if resp.status_code == 200:
                        logger.debug(f"Heartbeat sent for {self.config.service_id}")
                    elif resp.status_code == 404:
                        # Service not found - re-register
                        logger.warning("Service not found in registry, re-registering...")
                        await self._register_with_registry()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
