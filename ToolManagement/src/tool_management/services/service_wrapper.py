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

from .billing_hooks import AgentContext, BillingManager
from .durable_store import DurableStore, ToolRequest, ToolResponse, RequestStatus
from ..core.rate_limit import QuotaManager, get_quota_manager
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
    estimated_tokens_input: int = 0
    estimated_tokens_output: int = 0
    reserve_threshold_cents: int = 100
    enforce_economic_relationship: bool = True
    require_organization_id: bool = False
    enforce_quota: bool = True

    # Vendor (internal only - never exposed)
    _vendor_name: str = ""
    _vendor_adapter_class: Optional[Type[MCPAdapter]] = None


@dataclass
class EconomicPolicy:
    """Execution admission and pricing policy."""

    max_cost_per_request_cents: Optional[int] = None
    require_positive_balance: bool = True
    allow_dry_run_admission: bool = True
    high_cost_threshold_cents: int = 500
    metadata: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# REQUEST/RESPONSE MODELS (Client-facing - no vendor details)
# =============================================================================

class ExecuteRequest(BaseModel):
    """Client request to execute a tool."""
    tool: str = Field(..., description="Tool name to execute")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Tool inputs")
    idempotency_key: Optional[str] = Field(None, description="Idempotency key for deduplication")
    estimated_tokens_input: Optional[int] = Field(default=None, ge=0)
    estimated_tokens_output: Optional[int] = Field(default=None, ge=0)
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
    economics: Dict[str, Any] = Field(default_factory=dict)


class CostEstimateRequest(BaseModel):
    tool: str = Field(..., description="Tool name to estimate")
    inputs: Dict[str, Any] = Field(default_factory=dict)
    estimated_tokens_input: Optional[int] = Field(default=None, ge=0)
    estimated_tokens_output: Optional[int] = Field(default=None, ge=0)


class CostEstimateResponse(BaseModel):
    tool: str
    estimated_cost_cents: int
    components: Dict[str, Any] = Field(default_factory=dict)
    policy: Dict[str, Any] = Field(default_factory=dict)
    admissible: bool = True
    reasons: List[str] = Field(default_factory=list)


class AdmissionCheckRequest(BaseModel):
    tool: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    estimated_tokens_input: Optional[int] = Field(default=None, ge=0)
    estimated_tokens_output: Optional[int] = Field(default=None, ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AdmissionCheckResponse(BaseModel):
    admitted: bool
    tool: str
    estimated_cost_cents: int
    reasons: List[str] = Field(default_factory=list)
    quota: Dict[str, Any] = Field(default_factory=dict)
    policy: Dict[str, Any] = Field(default_factory=dict)
    balance: Dict[str, Any] = Field(default_factory=dict)


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
        billing_manager: Optional[BillingManager] = None,
        quota_manager: Optional[QuotaManager] = None,
        economic_policy: Optional[EconomicPolicy] = None,
    ):
        self.config = config
        self.adapter = adapter
        self.tool_name_map = tool_name_map or {}  # vendor_name -> our_name
        self.billing_manager = billing_manager or BillingManager()
        self.quota_manager = quota_manager or get_quota_manager()
        self.economic_policy = economic_policy or EconomicPolicy()

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

    def _policy_dict(self) -> Dict[str, Any]:
        return {
            "max_cost_per_request_cents": self.economic_policy.max_cost_per_request_cents,
            "require_positive_balance": self.economic_policy.require_positive_balance,
            "allow_dry_run_admission": self.economic_policy.allow_dry_run_admission,
            "high_cost_threshold_cents": self.economic_policy.high_cost_threshold_cents,
            "reserve_threshold_cents": int(self.config.reserve_threshold_cents),
            "enforce_quota": bool(self.config.enforce_quota),
            "enforce_economic_relationship": bool(self.config.enforce_economic_relationship),
            "require_organization_id": bool(self.config.require_organization_id),
            "metadata": dict(self.economic_policy.metadata),
        }

    def _calculate_cost(
        self,
        *,
        tokens_input: int = 0,
        tokens_output: int = 0,
    ) -> int:
        cost = int(self.config.cost_per_request_cents)
        token_cost = 0.0
        if self.config.cost_per_1k_tokens_cents > 0:
            total_tokens = max(0, int(tokens_input)) + max(0, int(tokens_output))
            token_cost = (total_tokens / 1000.0) * float(self.config.cost_per_1k_tokens_cents)
            cost += int(round(token_cost))
        return max(0, int(cost))

    def _estimate_cost(self, body: ExecuteRequest | CostEstimateRequest | AdmissionCheckRequest) -> int:
        metadata = getattr(body, "metadata", None)
        metadata_tokens_in = None
        metadata_tokens_out = None
        if isinstance(metadata, dict):
            if isinstance(metadata.get("estimated_tokens_input"), int):
                metadata_tokens_in = int(metadata["estimated_tokens_input"])
            if isinstance(metadata.get("estimated_tokens_output"), int):
                metadata_tokens_out = int(metadata["estimated_tokens_output"])

        estimated_tokens_input = int(
            getattr(body, "estimated_tokens_input", None)
            or metadata_tokens_in
            or self.config.estimated_tokens_input
            or 0
        )
        estimated_tokens_output = int(
            getattr(body, "estimated_tokens_output", None)
            or metadata_tokens_out
            or self.config.estimated_tokens_output
            or 0
        )
        return self._calculate_cost(
            tokens_input=estimated_tokens_input,
            tokens_output=estimated_tokens_output,
        )

    def _get_balance_snapshot(self, client_id: str, organization_id: Optional[str]) -> Dict[str, Any]:
        hook = self.billing_manager.billing_hook
        balance = None
        if hasattr(hook, "get_balance"):
            try:
                lookup_id = organization_id or client_id
                balance = int(getattr(hook, "get_balance")(lookup_id))
            except Exception:
                balance = None
        return {
            "known_balance_cents": balance,
            "billing_hook": type(hook).__name__,
        }

    async def _dry_run_admission(
        self,
        *,
        tool_name: str,
        client: Dict[str, Any],
        estimated_cost_cents: int,
    ) -> AdmissionCheckResponse:
        reasons: List[str] = []
        admitted = True
        policy = self._policy_dict()

        if self.config.require_organization_id and not client.get("organization_id"):
            admitted = False
            reasons.append("organization_required")

        max_cost = self.economic_policy.max_cost_per_request_cents
        if max_cost is not None and estimated_cost_cents > int(max_cost):
            admitted = False
            reasons.append("max_request_cost_exceeded")

        if self.economic_policy.require_positive_balance and estimated_cost_cents > 0:
            has_balance = await self.billing_manager.billing_hook.check_balance(
                client_id=client.get("client_id") or "unknown",
                organization_id=client.get("organization_id"),
                required_cents=max(1, estimated_cost_cents),
            )
            if not has_balance:
                admitted = False
                reasons.append("insufficient_balance")

        quota_snapshot: Dict[str, Any] = {}
        if self.config.enforce_quota:
            quota = self.quota_manager.get_quota(client.get("client_id") or "anonymous")
            usage = self.quota_manager.get_usage(client.get("client_id") or "anonymous")
            quota_snapshot = {
                "quota": quota.to_dict(),
                "usage": usage,
            }

        balance_snapshot = self._get_balance_snapshot(
            client_id=client.get("client_id") or "unknown",
            organization_id=client.get("organization_id"),
        )
        return AdmissionCheckResponse(
            admitted=admitted,
            tool=tool_name,
            estimated_cost_cents=estimated_cost_cents,
            reasons=reasons,
            quota=quota_snapshot,
            policy=policy,
            balance=balance_snapshot,
        )

    async def _admit_execution(
        self,
        *,
        request: ToolRequest,
        client: Dict[str, Any],
        tool_name: str,
        estimated_cost_cents: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        client_id = client.get("client_id") or "anonymous"
        economics: Dict[str, Any] = {
            "admitted": False,
            "estimated_cost_cents": estimated_cost_cents,
            "policy": self._policy_dict(),
        }

        if self.config.require_organization_id and not client.get("organization_id"):
            raise HTTPException(
                status_code=402,
                detail="Organization membership is required for this service",
            )

        max_cost = self.economic_policy.max_cost_per_request_cents
        if max_cost is not None and estimated_cost_cents > int(max_cost):
            raise HTTPException(
                status_code=402,
                detail=f"Estimated request cost {estimated_cost_cents} exceeds max {max_cost}",
            )

        quota_result = None
        if self.config.enforce_quota:
            quota_result = self.quota_manager.check_and_consume(client_id=client_id, tool_name=tool_name)
            if not quota_result.allowed:
                raise HTTPException(
                    status_code=429,
                    detail={
                        "code": "RATE_LIMITED",
                        "reason": quota_result.reason,
                        "retry_after_seconds": quota_result.retry_after,
                        "remaining": quota_result.remaining,
                        "limit": quota_result.limit,
                    },
                )
            economics["quota"] = {
                "remaining": quota_result.remaining,
                "limit": quota_result.limit,
                "reason": quota_result.reason or "allowed",
            }

        agent_context = None
        if metadata and isinstance(metadata, dict) and metadata.get("agent_id"):
            agent_context = AgentContext(
                agent_id=str(metadata.get("agent_id")),
                agent_name=metadata.get("agent_name"),
                organization_id=client.get("organization_id"),
                session_id=metadata.get("session_id"),
                parent_task_id=metadata.get("parent_task_id"),
            )

        if self.config.enforce_economic_relationship:
            try:
                allowed = await self.billing_manager.pre_execute(
                    request=request,
                    estimated_cost_cents=estimated_cost_cents,
                    agent_context=agent_context,
                )
            except Exception as exc:
                if self.config.enforce_quota:
                    self.quota_manager.release(client_id)
                raise HTTPException(status_code=503, detail=f"Billing system unavailable: {exc}") from exc
            if not allowed:
                if self.config.enforce_quota:
                    self.quota_manager.release(client_id)
                raise HTTPException(
                    status_code=402,
                    detail="Insufficient balance for requested operation",
                )

        economics["admitted"] = True
        economics["balance"] = self._get_balance_snapshot(client_id, client.get("organization_id"))
        return {
            "economics": economics,
            "agent_context": agent_context,
            "quota_client_id": client_id,
        }

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
            "economic_tier": "standard",
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
        cost = self._calculate_cost(
            tokens_input=response.tokens_input,
            tokens_output=response.tokens_output,
        )
        request.cost_cents = cost
        await self._store.update_request(request)

        logger.debug(f"Usage tracked: {request.id} - {cost} cents")

    async def _record_economic_event(
        self,
        *,
        event_type: str,
        request_id: Optional[str],
        client_id: Optional[str],
        organization_id: Optional[str],
        amount_cents: int = 0,
        payload: Optional[Dict[str, Any]] = None,
    ):
        if not self._store:
            return
        try:
            await self._store.append_economic_event(
                event_type=event_type,
                request_id=request_id,
                client_id=client_id,
                organization_id=organization_id,
                amount_cents=int(amount_cents),
                payload=payload or {},
            )
        except Exception as exc:
            logger.debug(f"Failed to persist economic event {event_type}: {exc}")

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
            return {
                "client_id": "anonymous",
                "organization_id": None,
                "rate_limit": self.config.rate_limit_requests,
                "economic_tier": "anonymous",
            }

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
                    "economics_policy": "/economics/policy",
                    "economics_status": "/economics/status",
                    "economics_estimate": "/economics/estimate",
                    "economics_admission_check": "/economics/admission-check",
                    "economics_events": "/economics/events",
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

        @app.get("/economics/policy")
        async def get_economic_policy(client: Dict = Depends(get_client)):
            client_id = client.get("client_id") or "anonymous"
            quota = self.quota_manager.get_quota(client_id)
            return {
                "service_id": self.config.service_id,
                "client_id": client_id,
                "organization_id": client.get("organization_id"),
                "policy": self._policy_dict(),
                "pricing": {
                    "cost_per_request_cents": int(self.config.cost_per_request_cents),
                    "cost_per_1k_tokens_cents": int(self.config.cost_per_1k_tokens_cents),
                    "estimated_tokens_input": int(self.config.estimated_tokens_input),
                    "estimated_tokens_output": int(self.config.estimated_tokens_output),
                },
                "quota": quota.to_dict(),
            }

        @app.get("/economics/status")
        async def get_economic_status(client: Dict = Depends(get_client)):
            client_id = client.get("client_id") or "anonymous"
            quota = self.quota_manager.get_quota(client_id)
            usage = self.quota_manager.get_usage(client_id)
            balance = self._get_balance_snapshot(client_id, client.get("organization_id"))
            return {
                "service_id": self.config.service_id,
                "client_id": client_id,
                "organization_id": client.get("organization_id"),
                "economic_tier": client.get("economic_tier"),
                "policy": self._policy_dict(),
                "quota": quota.to_dict(),
                "usage": usage,
                "balance": balance,
            }

        @app.post("/economics/estimate", response_model=CostEstimateResponse)
        async def estimate_cost(
            body: CostEstimateRequest,
            client: Dict = Depends(get_client),
        ):
            if body.tool not in self._tool_cache:
                raise HTTPException(404, f"Tool not found: {body.tool}")
            estimated_cost = self._estimate_cost(body)
            reasons: List[str] = []
            admissible = True
            max_cost = self.economic_policy.max_cost_per_request_cents
            if max_cost is not None and estimated_cost > int(max_cost):
                admissible = False
                reasons.append("max_request_cost_exceeded")
            if self.config.require_organization_id and not client.get("organization_id"):
                admissible = False
                reasons.append("organization_required")

            return CostEstimateResponse(
                tool=body.tool,
                estimated_cost_cents=estimated_cost,
                components={
                    "base_request_cost_cents": int(self.config.cost_per_request_cents),
                    "cost_per_1k_tokens_cents": int(self.config.cost_per_1k_tokens_cents),
                    "estimated_tokens_input": int(
                        body.estimated_tokens_input
                        if body.estimated_tokens_input is not None
                        else self.config.estimated_tokens_input
                    ),
                    "estimated_tokens_output": int(
                        body.estimated_tokens_output
                        if body.estimated_tokens_output is not None
                        else self.config.estimated_tokens_output
                    ),
                },
                policy=self._policy_dict(),
                admissible=admissible,
                reasons=reasons,
            )

        @app.post("/economics/admission-check", response_model=AdmissionCheckResponse)
        async def admission_check(
            body: AdmissionCheckRequest,
            client: Dict = Depends(get_client),
        ):
            if body.tool not in self._tool_cache:
                raise HTTPException(404, f"Tool not found: {body.tool}")
            if not self.economic_policy.allow_dry_run_admission:
                raise HTTPException(403, "Dry-run admission checks are disabled")

            estimated_cost = self._estimate_cost(body)
            return await self._dry_run_admission(
                tool_name=body.tool,
                client=client,
                estimated_cost_cents=estimated_cost,
            )

        @app.post("/execute", response_model=ExecuteResponse)
        async def execute(
            body: ExecuteRequest,
            client: Dict = Depends(get_client),
            x_api_key: Optional[str] = Header(None, alias=self.config.api_key_header),
        ):
            tool_name = body.tool
            estimated_cost_cents = self._estimate_cost(body)
            economics_context: Dict[str, Any] = {
                "estimated_cost_cents": estimated_cost_cents,
                "policy": self._policy_dict(),
            }

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
                        await self._record_economic_event(
                            event_type="idempotent_replay",
                            request_id=existing.id,
                            client_id=client.get("client_id"),
                            organization_id=client.get("organization_id"),
                            amount_cents=int(existing.cost_cents or 0),
                            payload={
                                "tool": tool_name,
                                "idempotency_key": body.idempotency_key,
                            },
                        )
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
                            economics={
                                **economics_context,
                                "idempotent_replay": True,
                                "admitted": True,
                                "charged_cost_cents": int(existing.cost_cents or 0),
                                "balance": self._get_balance_snapshot(
                                    client_id=client.get("client_id") or "unknown",
                                    organization_id=client.get("organization_id"),
                                ),
                            },
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

            admission = await self._admit_execution(
                request=request,
                client=client,
                tool_name=tool_name,
                estimated_cost_cents=estimated_cost_cents,
                metadata=body.metadata,
            )
            economics_context.update(admission.get("economics", {}))
            agent_context = admission.get("agent_context")
            quota_client_id = str(admission.get("quota_client_id") or "")
            await self._record_economic_event(
                event_type="admitted",
                request_id=request.id,
                client_id=client.get("client_id"),
                organization_id=client.get("organization_id"),
                amount_cents=estimated_cost_cents,
                payload={
                    "tool": tool_name,
                    "quota_client_id": quota_client_id,
                },
            )

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
                else:
                    request.cost_cents = self._calculate_cost(
                        tokens_input=response.tokens_input,
                        tokens_output=response.tokens_output,
                    )
                    await self._store.update_request(request)

                if self.config.enforce_economic_relationship:
                    await self.billing_manager.post_execute(
                        request=request,
                        response=response,
                        agent_context=agent_context,
                    )

                if self.config.enforce_quota:
                    self.quota_manager.record_usage(
                        client_id=client["client_id"],
                        tokens=(response.tokens_input + response.tokens_output),
                        cost=float(request.cost_cents),
                    )

                economics_context["charged_cost_cents"] = int(request.cost_cents or 0)
                economics_context["balance"] = self._get_balance_snapshot(
                    client_id=client.get("client_id") or "unknown",
                    organization_id=client.get("organization_id"),
                )
                await self._record_economic_event(
                    event_type="charged",
                    request_id=request.id,
                    client_id=client.get("client_id"),
                    organization_id=client.get("organization_id"),
                    amount_cents=int(request.cost_cents or 0),
                    payload={
                        "tool": tool_name,
                        "execution_time_ms": elapsed,
                        "tokens_input": response.tokens_input,
                        "tokens_output": response.tokens_output,
                    },
                )

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
                    economics=economics_context,
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
                request.cost_cents = 0
                await self._store.update_request(request)

                if self.config.enforce_economic_relationship:
                    try:
                        await self.billing_manager.post_execute(
                            request=request,
                            response=response,
                            agent_context=agent_context,
                        )
                    except Exception as billing_exc:
                        logger.warning(f"Billing post-execution cleanup failed: {billing_exc}")

                economics_context["charged_cost_cents"] = int(request.cost_cents or 0)
                economics_context["balance"] = self._get_balance_snapshot(
                    client_id=client.get("client_id") or "unknown",
                    organization_id=client.get("organization_id"),
                )
                await self._record_economic_event(
                    event_type="execution_failed",
                    request_id=request.id,
                    client_id=client.get("client_id"),
                    organization_id=client.get("organization_id"),
                    amount_cents=0,
                    payload={
                        "tool": tool_name,
                        "error": str(e),
                        "execution_time_ms": elapsed,
                    },
                )

                return ExecuteResponse(
                    request_id=request.id,
                    tool=tool_name,
                    status="failed",
                    error=str(e),
                    execution_time_ms=elapsed,
                    economics=economics_context,
                )
            finally:
                if self.config.enforce_quota and quota_client_id:
                    self.quota_manager.release(quota_client_id)

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

        @app.get("/economics/events")
        async def get_economic_events(
            client: Dict = Depends(get_client),
            event_type: Optional[str] = None,
            since: Optional[str] = None,
            limit: int = 100,
        ):
            since_dt = datetime.fromisoformat(since) if since else None
            rows = await self._store.get_economic_events(
                client_id=client.get("client_id"),
                organization_id=client.get("organization_id"),
                event_type=event_type,
                since=since_dt,
                limit=limit,
            )
            return {
                "client_id": client.get("client_id"),
                "organization_id": client.get("organization_id"),
                "count": len(rows),
                "events": rows,
            }

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
