"""
Clean Tools API - Robust tool execution with persistent storage.

This replaces the brittle batch endpoint with a properly designed API.
"""

import os
import asyncio
import hashlib
import json
import logging
import re
import secrets
import threading
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query, Request
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
import httpx

from ..persistence import get_tool_store, RegisteredTool, get_execution_store
from ..core.circuit_breaker import get_backend_circuit_breaker

# ML-based smart recommender (opt-in via env var)
_USE_ML_SCORING = os.getenv("USE_ML_SCORING", "").lower() in ("1", "true", "yes")
_smart_recommender = None

def _get_smart_recommender():
    global _smart_recommender
    if _smart_recommender is None:
        try:
            from ..ml.integration import get_smart_recommender
            _smart_recommender = get_smart_recommender()
        except Exception:
            pass
    return _smart_recommender

# Output guard for enforcing size limits and URL capture
try:
    from ..guards.output_guard import apply_output_guard, extract_urls, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/tools", tags=["tools"])

# These are transport/control-plane fields, never tool arguments.  In
# particular, accepting them inside a JSON object lets a caller impersonate a
# tenant or smuggle credentials into a backend call.
_RESERVED_ARGUMENT_KEYS = frozenset({
    "tenant_id", "_tenant_credential", "_pooled_ok", "_tenant_id",
    "_trace_id", "_session_id", "_parent_execution_id", "_timeout",
    "_convergence_key", "_fanout_budget", "_group_reducer",
    "_group_reducer_config",
})

# Shared HTTP client
_client: Optional[httpx.AsyncClient] = None
# backend_id -> (last accepted announce time, sha256 of announced tool surface)
_announce_last_seen: Dict[str, tuple] = {}
_WRITE_TOOL_NAMES = {
    name.strip()
    for name in os.getenv(
        "IDEMPOTENT_WRITE_TOOLS",
        "store_belief,publish_event,substack_create_draft",
    ).split(",")
    if name.strip()
}


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(None, connect=15.0),  # No read/write timeout
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )
    return _client


def _backend_headers(backend_id: str, tenant_id: Optional[str] = None) -> Dict[str, str]:
    """Resolve a backend bearer only from server-side environment secrets."""
    env_suffix = re.sub(r"[^A-Z0-9]+", "_", backend_id.upper()).strip("_")
    token = os.getenv(f"BACKEND_{env_suffix}_BEARER_TOKEN")
    if not token:
        token = os.getenv("BACKEND_BEARER_TOKEN")
    if not token and backend_id in {"distributed-email", "distributed-email-mcp"}:
        token = (
            os.getenv("TOOLMANAGEMENT_EMAIL_BEARER_TOKEN")
            or os.getenv("EMAIL_MCP_BEARER_TOKEN")
        )
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    if backend_id == "distributed-email-mcp" or backend_id == "distributed-email":
        if tenant_id:
            headers["X-DSCO-Tenant-ID"] = tenant_id
    return headers


# ==================== Request/Response Models ====================

class ToolCall(BaseModel):
    """A single tool call request."""

    model_config = ConfigDict(extra="forbid")

    tool: str = Field(..., description="Tool name or 'backend:tool' format")
    arguments: Dict[str, Any] = Field(default_factory=dict)
    timeout: float = Field(default=60.0, description="Timeout in seconds")
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Idempotency key for write tools (prevents duplicate side effects)",
    )
    trace_id: Optional[str] = None
    session_id: Optional[str] = None
    parent_execution_id: Optional[str] = None
    # Never part of the wire model. The route binds it from the validated bearer
    # identity after Pydantic has rejected caller-supplied extra fields.
    _tenant_id: Optional[str] = PrivateAttr(default=None)

    @property
    def tenant_id(self) -> Optional[str]:
        return self._tenant_id

    def bind_tenant(self, tenant_id: Optional[str]) -> None:
        self._tenant_id = tenant_id


class BatchRequest(BaseModel):
    """Batch tool execution request."""

    model_config = ConfigDict(extra="forbid")
    calls: List[ToolCall]
    execution: str = Field(
        default="parallel",
        description="Batch mode: parallel or sequential",
    )
    parallel: Optional[bool] = Field(
        default=None,
        description="Deprecated compatibility flag. If set, overrides execution.",
    )


class ToolResult(BaseModel):
    """Result from a tool call."""
    tool: str
    backend: Optional[str] = None
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    latency_ms: float = 0
    idempotent_replay: bool = False


class BatchResponse(BaseModel):
    """Batch execution response."""
    results: List[ToolResult]
    total: int
    successful: int
    failed: int


class RegisterToolRequest(BaseModel):
    """Request to register a tool."""
    name: str
    backend_id: str
    backend_url: str
    description: str = ""
    input_schema: Dict = Field(default_factory=dict)
    output_schema: Dict = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    category: str = "general"


class RegisterBackendRequest(BaseModel):
    """Request to register a backend."""
    backend_id: str
    name: str
    url: str
    health_endpoint: str = "/health"
    mcp_endpoint: str = "/mcp"
    capabilities: List[str] = Field(default_factory=list)


class AnnounceRequest(BaseModel):
    """Push-announce request: backend announces itself + its tools on startup."""
    backend_id: str
    name: str
    url: str
    health_endpoint: str = "/health"
    mcp_endpoint: str = "/mcp"
    capabilities: List[str] = Field(default_factory=list)
    tools: List[Dict[str, Any]] = Field(default_factory=list)


class DependencyUpsertRequest(BaseModel):
    source_tool: str = Field(..., description="Source tool name or backend:tool")
    target_tool: str = Field(..., description="Dependent tool name or backend:tool")
    dependency_type: str = Field(default="explicit", description="explicit|inferred|runtime")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    call_count: int = Field(default=1, ge=1, le=1_000_000)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InferDependenciesRequest(BaseModel):
    hours: int = Field(default=24, ge=1, le=720)
    limit: int = Field(default=100, ge=1, le=500)
    min_support: int = Field(default=2, ge=1, le=1000)
    persist: bool = True


class ToolRecommendationRequest(BaseModel):
    query: Optional[str] = Field(default=None, description="Natural-language objective")
    intent: Optional[str] = Field(default=None, description="query|store|publish|analyze|manage")
    backend_id: Optional[str] = None
    category: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    max_latency_ms: Optional[float] = Field(default=None, ge=0.0)
    require_streaming: bool = False
    include_dependencies: bool = True
    limit: int = Field(default=8, ge=1, le=50)
    trace_id: Optional[str] = None
    session_id: Optional[str] = None


class RecommendationFeedbackRequest(BaseModel):
    recommendation_id: str = Field(..., description="Recommendation request identifier")
    context: Dict[str, Any] = Field(default_factory=dict)
    recommendation: Dict[str, Any] = Field(default_factory=dict)
    chosen_tool: Optional[str] = None
    accepted: bool = False
    outcome_status: Optional[str] = Field(default=None, description="completed|failed|cancelled|timeout")
    latency_ms: Optional[float] = Field(default=None, ge=0.0)
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    trace_id: Optional[str] = None
    session_id: Optional[str] = None


def assert_safe_tool_arguments(arguments: Dict[str, Any]) -> None:
    """Reject control-plane fields at every nesting level before execution."""
    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key)
                if key_text in _RESERVED_ARGUMENT_KEYS:
                    raise HTTPException(
                        status_code=422,
                        detail={
                            "error": "reserved_tool_argument",
                            "field": f"{path}.{key_text}" if path else key_text,
                        },
                    )
                walk(child, f"{path}.{key_text}" if path else key_text)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    walk(arguments, "arguments")


def resolve_execution_identity(
    request: Request, tool_names: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Authenticate an execution caller and classify customer vs service.

    A token that looks like a customer key is never allowed to fall through to
    operator behavior. This avoids turning a stale/revoked customer key into a
    tenantless execution.

    When ``tool_names`` is provided, non-entitled customers may still qualify
    for the free discovery tier (allowlisted read-only tools, daily quota).
    Callers that omit it (jobs, calendar) keep the strict subscription gate.
    """
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"error": "execution_auth_required"})
    token = auth_header[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail={"error": "execution_auth_required"})

    from .user_routes import resolve_user_from_request
    from ..oauth import get_token_identity, require_bearer

    user = resolve_user_from_request(request)
    if token.startswith("sk-user-"):
        if not user:
            raise HTTPException(status_code=401, detail={"error": "customer_identity_invalid"})
    elif user:
        # User-bound OAuth identities are customer identities as well.
        pass
    elif require_bearer(request) is None:
        raise HTTPException(status_code=401, detail={"error": "operator_or_service_identity_invalid"})

    if user:
        from ..persistence.billing_store import get_billing_store

        try:
            entitled = get_billing_store().has_active_entitlement(user["user_id"])
        except Exception as exc:
            logger.warning("Customer entitlement lookup failed: %s", exc)
            raise HTTPException(
                status_code=503,
                detail={"error": "customer_entitlement_unavailable"},
            ) from exc
        if not entitled:
            # Free discovery tier: non-entitled users may still call a small
            # allowlist of read-only discovery/search tools under a daily
            # quota, so an agent that just found us can evaluate the gateway
            # before hitting the paywall. Everything else still 402s.
            if tool_names and _trial_allows(user["user_id"], tool_names):
                return {
                    "kind": "customer_trial",
                    "tenant_id": user["user_id"],
                    "user": user,
                }
            raise HTTPException(
                status_code=402,
                detail={
                    "error": "customer_subscription_required",
                    "checkout": "/api/v1/me/billing/checkout-session",
                    "trial": {
                        "free_tools": sorted(TRIAL_TOOL_ALLOWLIST),
                        "free_calls_per_day": TRIAL_CALLS_PER_DAY,
                        "remaining_today": _trial_remaining(user["user_id"]),
                    },
                },
            )
        return {"kind": "customer", "tenant_id": user["user_id"], "user": user}

    identity = get_token_identity(token) or {}
    return {"kind": "operator_service", "tenant_id": None, "identity": identity}


# ==================== Free discovery tier ====================

# Read-only discovery/search tools an unentitled user may sample. Bare names
# match any backend namespacing (resolve happens later); keep this list to
# side-effect-free tools only.
TRIAL_TOOL_ALLOWLIST = {
    t.strip()
    for t in os.getenv(
        "TRIAL_TOOL_ALLOWLIST",
        "search_tools,list_tool_names,get_tool,query_tools,batch_get_tools,"
        "list_tool_categories,list_tools_paginated,search,embed,rerank,read,"
        "get_search_result,deepsearch,multi_search,segment,classify",
    ).split(",")
    if t.strip()
}
TRIAL_CALLS_PER_DAY = int(os.getenv("TRIAL_CALLS_PER_DAY", "25"))

# Per-process daily counters {user_id: (yyyymmdd, count)}. Soft limit: multi-
# container deployments each hold their own counter, so the effective global
# quota is N_containers * TRIAL_CALLS_PER_DAY — acceptable for a trial gate.
_trial_counts: Dict[str, Any] = {}
_trial_lock = threading.Lock()


def _trial_day() -> str:
    return time.strftime("%Y%m%d", time.gmtime())


def _trial_remaining(user_id: str) -> int:
    with _trial_lock:
        day, count = _trial_counts.get(user_id, (None, 0))
        if day != _trial_day():
            return TRIAL_CALLS_PER_DAY
        return max(0, TRIAL_CALLS_PER_DAY - count)


def _trial_allows(user_id: str, tool_names: List[str]) -> bool:
    """True if every requested tool is trial-eligible and quota covers them."""
    for name in tool_names:
        # Accept both bare and backend-qualified ("backend:tool") forms.
        bare = name.rsplit(":", 1)[-1]
        if bare not in TRIAL_TOOL_ALLOWLIST:
            return False
    n = len(tool_names)
    today = _trial_day()
    with _trial_lock:
        day, count = _trial_counts.get(user_id, (today, 0))
        if day != today:
            day, count = today, 0
        if count + n > TRIAL_CALLS_PER_DAY:
            return False
        _trial_counts[user_id] = (day, count + n)
    return True


def _bind_customer_identity(request: Request, calls: List[ToolCall]) -> Dict[str, Any]:
    """Validate caller, reject reserved arguments, and bind the server tenant."""
    identity = resolve_execution_identity(request, [c.tool for c in calls])
    for call in calls:
        assert_safe_tool_arguments(call.arguments)
        call.bind_tenant(identity["tenant_id"])
    return identity


# ==================== Tool Execution ====================

async def execute_tool(call: ToolCall) -> ToolResult:
    """Execute a single tool call."""
    store = get_tool_store()
    breaker = get_backend_circuit_breaker()
    start = time.time()
    backend_id: Optional[str] = None
    resolved_tool_id: Optional[str] = None

    try:
        # Resolve the tool to backend
        backend_id, backend_url, tool_name = store.resolve_tool(call.tool)
        resolved_tool_id = f"{backend_id}:{tool_name}"

        # Fast-fail: if the live service index knows this backend is currently
        # unhealthy, skip the proxy hop instead of blocking on a doomed call.
        # Fail-open — unknown/stale backends are still attempted (see
        # service_index.is_healthy), so this only trims *known-dead* hops.
        try:
            from ..services.service_index import is_healthy

            if not is_healthy(backend_id):
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=f"BACKEND_UNAVAILABLE: {backend_id} failed its latest health probe",
                    latency_ms=(time.time() - start) * 1000,
                )
        except Exception:  # noqa: BLE001 — never let the index break execution
            pass

        # Some backends front a single operator-owned data plane. They are not
        # public managed add-ons until a tenant-specific instance exists.
        if call.tenant_id and backend_id in {"distributed-email", "distributed-email-mcp"}:
            configured_tenants = (
                os.getenv("DISTRIBUTED_EMAIL_ALLOWED_TENANTS", "")
                or os.getenv("EMAIL_ALLOWED_TENANT_IDS", "")
            )
            allowed_tenants = {
                value.strip()
                for value in configured_tenants.split(",")
                if value.strip()
            }
            if call.tenant_id not in allowed_tenants:
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=(
                        "ACCESS_DENIED: Distributed Email is private while tenant-isolated "
                        "mailbox provisioning is being completed."
                    ),
                    latency_ms=(time.time() - start) * 1000,
                )
        allowed, circuit_state = breaker.allow_request(backend_id)
        if not allowed:
            retry_after = circuit_state.get("retry_after_seconds")
            retry_suffix = f" retry_after={retry_after}s" if retry_after is not None else ""
            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=False,
                error=(
                    f"CIRCUIT_OPEN: backend={backend_id} is temporarily unavailable "
                    f"(status={circuit_state.get('status')}).{retry_suffix}"
                ),
                latency_ms=(time.time() - start) * 1000,
            )
        tool_id = f"{backend_id}:{tool_name}"
        request_hash = None
        use_idempotency = bool(call.idempotency_key) and _is_write_tool(call.tool, tool_name)

        if use_idempotency:
            request_hash = _hash_arguments(call.arguments)
            try:
                existing = store.get_idempotent_response(
                    idempotency_key=call.idempotency_key,
                    tool_id=tool_id,
                    request_hash=request_hash,
                )
            except ValueError as exc:
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=str(exc),
                    latency_ms=(time.time() - start) * 1000,
                )
            if existing is not None:
                payload = existing.get("response") or {}
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=bool(existing.get("success", True)),
                    result=payload.get("result"),
                    error=payload.get("error"),
                    latency_ms=(time.time() - start) * 1000,
                    idempotent_replay=True,
                )

        # Execute via MCP protocol
        outbound_arguments = dict(call.arguments)
        if call.trace_id:
            outbound_arguments.setdefault("_trace_id", call.trace_id)
        if call.session_id:
            outbound_arguments.setdefault("_session_id", call.session_id)
        if call.parent_execution_id:
            outbound_arguments.setdefault("_parent_execution_id", call.parent_execution_id)

        # Per-user credential injection: resolve the caller's own credential
        # for this tool's provider and pass it to the backend. Users with a
        # paid managed add-on instead get pooled platform credentials.
        auth_provider: Optional[str] = None
        auth_connected = False
        addon_active = False
        try:
            from ..integrations.provider_catalog import provider_for_tool

            auth_provider = provider_for_tool(tool_name)
            if call.tenant_id:
                if auth_provider:
                    from ..services.tenant_credentials import (
                        _default_store as _tenant_store,
                    )

                    credential = _tenant_store().get_secret(call.tenant_id, auth_provider)
                    if credential:
                        if backend_id != "integration-fabric":
                            return ToolResult(
                                tool=call.tool,
                                backend=backend_id,
                                success=False,
                                error="CREDENTIAL_ENVELOPE_UNSUPPORTED: backend cannot receive tenant BYOK credentials safely",
                                latency_ms=(time.time() - start) * 1000,
                            )
                        from ..core.credential_envelope import seal_credential_envelope

                        outbound_arguments["_credential_envelope"] = seal_credential_envelope(
                            credential,
                            tenant_id=call.tenant_id,
                            provider=auth_provider,
                            backend_id=backend_id,
                        )
                        auth_connected = True
                    else:
                        from ..persistence.billing_store import get_billing_store

                        addon_active = auth_provider in get_billing_store().active_addon_providers(
                            call.tenant_id
                        )
                        if addon_active:
                            outbound_arguments.setdefault("_pooled_ok", True)
        except Exception as e:
            logger.debug(f"Credential injection skipped for {tool_name}: {e}")

        client = get_client()
        response = await client.post(
            f"{backend_url}/mcp",
            json={
                "jsonrpc": "2.0",
                "id": str(uuid4()),
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": outbound_arguments
                }
            },
            timeout=call.timeout,
            headers=_backend_headers(backend_id, call.tenant_id),
        )

        latency_ms = (time.time() - start) * 1000

        if response.status_code == 200:
            data = response.json()

            if "error" in data:
                err_obj = data.get("error")
                err_code = err_obj.get("code") if isinstance(err_obj, dict) else None
                err_message = err_obj.get("message") if isinstance(err_obj, dict) else str(err_obj)
                store.record_call(
                    f"{backend_id}:{tool_name}",
                    latency_ms,
                    False,
                    execution_mode="sync",
                    trace_id=call.trace_id,
                    session_id=call.session_id,
                    error_code=err_code,
                    error_message=err_message,
                )
                breaker.record_failure(backend_id, error=str(data.get("error")))
                return ToolResult(
                    tool=call.tool,
                    backend=backend_id,
                    success=False,
                    error=str(data["error"]),
                    latency_ms=latency_ms
                )

            store.record_call(
                f"{backend_id}:{tool_name}",
                latency_ms,
                True,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
            )
            breaker.record_success(backend_id)

            # Metered billing: attribute the execution to the authenticated user.
            if call.tenant_id:
                try:
                    from ..persistence.billing_store import get_billing_store

                    get_billing_store().record_usage(
                        call.tenant_id, "tool_execution", ref=tool_name
                    )
                except Exception as e:
                    logger.debug(f"Usage metering skipped: {e}")

            result_data = data.get("result")

            # Interactive-auth hint: if the backend served sandbox data for a
            # provider the caller hasn't connected, attach a connect popup URL.
            if auth_provider and not auth_connected and isinstance(result_data, dict):
                try:
                    payload = result_data
                    content = result_data.get("content")
                    if isinstance(content, list) and content and isinstance(content[0], dict):
                        text = content[0].get("text")
                        if isinstance(text, str):
                            try:
                                payload = json.loads(text)
                            except (json.JSONDecodeError, ValueError):
                                payload = {}
                    if isinstance(payload, dict) and payload.get("mode") == "sandbox":
                        from ..integrations.provider_catalog import PROVIDERS
                        from .user_routes import connect_url_for

                        meta = PROVIDERS.get(auth_provider, {})
                        display = meta.get("display_name", auth_provider)
                        addon_cents = meta.get("addon_monthly_cents")
                        auth_obj = {
                            "status": "auth_required",
                            "provider": auth_provider,
                            "display_name": display,
                            "logo_url": meta.get("logo_url"),
                            "connect_url": connect_url_for(auth_provider),
                            "detail": (
                                f"Sandbox result. Connect your own {display} credentials "
                                f"at the connect_url for live results."
                            ),
                        }
                        if addon_active:
                            auth_obj["status"] = "addon_active"
                            auth_obj["detail"] = (
                                f"Included in your {display} add-on; this provider has no "
                                f"live adapter yet, so results are simulated."
                            )
                        elif addon_cents is not None:
                            auth_obj["addon"] = {
                                "monthly_cents": addon_cents,
                                "enable": f"POST /api/v1/me/addons/{auth_provider}",
                                "detail": (
                                    f"Or enable the managed {display} add-on for "
                                    f"${addon_cents / 100:.2f}/mo — no API key needed."
                                ),
                            }
                        result_data["auth"] = auth_obj
                except Exception as e:
                    logger.debug(f"Auth-hint annotation skipped for {tool_name}: {e}")

            # Apply output guard: enforce size limits, capture URLs
            if OUTPUT_GUARD_AVAILABLE and result_data is not None:
                result_data = apply_output_guard(
                    result_data,
                    tool_name=f"{backend_id}:{tool_name}",
                    source_context={"backend": backend_id, "via": "tools_api"},
                )

            if use_idempotency and call.idempotency_key and request_hash:
                store.save_idempotent_response(
                    idempotency_key=call.idempotency_key,
                    tool_id=tool_id,
                    request_hash=request_hash,
                    response={"result": result_data, "error": None},
                    success=True,
                )

            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=True,
                result=result_data,
                latency_ms=latency_ms
            )
        else:
            store.record_call(
                f"{backend_id}:{tool_name}",
                latency_ms,
                False,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
                error_code=f"HTTP_{response.status_code}",
                error_message=response.text[:500],
            )
            breaker.record_failure(backend_id, error=f"http {response.status_code}")
            return ToolResult(
                tool=call.tool,
                backend=backend_id,
                success=False,
                error=f"HTTP {response.status_code}: {response.text[:200]}",
                latency_ms=latency_ms
            )

    except ValueError:
        # Tool/backend not found in MCP store — try the DuckDB registry
        try:
            from ..registry import get_registry
            registry = get_registry()
            reg_tool = registry.get_by_name(call.tool)
            if reg_tool is None:
                return ToolResult(
                    tool=call.tool,
                    success=False,
                    error=f"Tool not found: {call.tool}",
                    latency_ms=(time.time() - start) * 1000,
                )
            action = getattr(reg_tool.metadata, "action", None) if reg_tool.metadata else None
            if action:
                from .routes import _execute_with_action
                result_data = await _execute_with_action(action, call.arguments)
            else:
                result_data = await reg_tool.execute(**call.arguments)
            latency_ms = (time.time() - start) * 1000
            return ToolResult(
                tool=call.tool,
                success=True,
                result=result_data,
                latency_ms=latency_ms,
            )
        except Exception as fallback_err:
            return ToolResult(
                tool=call.tool,
                success=False,
                error=str(fallback_err),
                latency_ms=(time.time() - start) * 1000,
            )
    except httpx.TimeoutException:
        if backend_id:
            breaker.record_failure(backend_id, error=f"timeout after {call.timeout}s")
        if resolved_tool_id:
            store.record_call(
                resolved_tool_id,
                (time.time() - start) * 1000,
                False,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
                error_code="TIMEOUT",
                error_message=f"Timeout after {call.timeout}s",
            )
        return ToolResult(
            tool=call.tool,
            success=False,
            error=f"Timeout after {call.timeout}s",
            latency_ms=(time.time() - start) * 1000
        )
    except Exception as e:
        if backend_id:
            breaker.record_failure(backend_id, error=str(e))
        if resolved_tool_id:
            store.record_call(
                resolved_tool_id,
                (time.time() - start) * 1000,
                False,
                execution_mode="sync",
                trace_id=call.trace_id,
                session_id=call.session_id,
                error_code="EXCEPTION",
                error_message=str(e),
            )
        logger.exception(f"Tool execution error: {call.tool}")
        return ToolResult(
            tool=call.tool,
            success=False,
            error=str(e),
            latency_ms=(time.time() - start) * 1000
        )


_INTENT_HINTS: Dict[str, List[str]] = {
    "query": ["search", "read", "list", "query", "find", "lookup", "retrieve"],
    "store": ["store", "save", "record", "memory", "belief", "insert", "upsert"],
    "publish": ["publish", "event", "draft", "substack", "notify", "webhook"],
    "analyze": ["analyze", "synthesize", "deep", "summary", "compare", "embed"],
    "manage": ["register", "sync", "list", "get", "update", "admin", "control"],
}


def _tokenize(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return [token.strip().lower() for token in text.replace("/", " ").replace("_", " ").split() if token.strip()]


def _text_relevance_score(query: Optional[str], candidate_text: str) -> float:
    query_tokens = _tokenize(query)
    if not query_tokens:
        return 0.0
    lowered = candidate_text.lower()
    matches = sum(1 for token in query_tokens if token in lowered)
    return float(matches) / max(1, len(query_tokens))


def _intent_score(intent: Optional[str], candidate_text: str) -> float:
    if not intent:
        return 0.0
    normalized = intent.strip().lower()
    keywords = _INTENT_HINTS.get(normalized, [normalized])
    lowered = candidate_text.lower()
    hits = sum(1 for keyword in keywords if keyword in lowered)
    if not keywords:
        return 0.0
    return float(hits) / float(len(keywords))


def _execution_mode_hint(avg_latency_ms: float, require_streaming: bool) -> str:
    if require_streaming:
        return "stream"
    if avg_latency_ms <= 0:
        return "auto"
    if avg_latency_ms < 2000:
        return "sync"
    if avg_latency_ms <= 30000:
        return "stream"
    return "job"


# ==================== API Endpoints ====================

@router.post("/execute", response_model=ToolResult)
async def execute_single_tool(call: ToolCall, request: Request):
    """
    Execute a single tool.

    Tool can be specified as:
    - "tool_name" - will look up backend automatically
    - "backend_id:tool_name" - explicit backend

    Authenticated users (sk-user-* bearer) get their own integration
    credentials injected automatically.
    """
    _bind_customer_identity(request, [call])
    return await execute_tool(call)


@router.post("/batch", response_model=BatchResponse)
async def execute_batch(batch: BatchRequest, request: Request):
    """
    Execute multiple tools in batch.

    Tools can be specified as:
    - "tool_name" - will look up backend automatically
    - "backend_id:tool_name" - explicit backend

    Set parallel=false for sequential execution.
    """
    _bind_customer_identity(request, batch.calls)
    execution_mode = batch.execution.lower() if batch.execution else "parallel"
    if batch.parallel is not None:
        execution_mode = "parallel" if batch.parallel else "sequential"
    if execution_mode not in {"parallel", "sequential"}:
        raise HTTPException(status_code=400, detail="execution must be 'parallel' or 'sequential'")

    if execution_mode == "parallel":
        results = await asyncio.gather(*[execute_tool(call) for call in batch.calls])
    else:
        results = []
        for call in batch.calls:
            result = await execute_tool(call)
            results.append(result)

    successful = sum(1 for r in results if r.success)
    return BatchResponse(
        results=results,
        total=len(results),
        successful=successful,
        failed=len(results) - successful
    )


@router.post("/register")
async def register_tool(request: RegisterToolRequest):
    """Register a new tool."""
    store = get_tool_store()

    tool = RegisteredTool(
        tool_id=f"{request.backend_id}:{request.name}",
        name=request.name,
        backend_id=request.backend_id,
        backend_url=request.backend_url,
        description=request.description,
        input_schema=request.input_schema,
        output_schema=request.output_schema,
        tags=request.tags,
        category=request.category
    )

    return store.register_tool(tool)


@router.get("/debug/test")
async def debug_test():
    """Debug endpoint to test persistence."""
    import os
    data_dir = os.getenv("DATA_DIR", "data")
    try:
        store = get_tool_store()
        return {
            "data_dir": data_dir,
            "db_path": str(store.db_path),
            "store_initialized": store._initialized,
        }
    except Exception as e:
        return {"error": str(e), "data_dir": data_dir}


@router.post("/backends/register")
async def register_backend(request: RegisterBackendRequest):
    """Register a new backend."""
    try:
        store = get_tool_store()
        return store.register_backend(
            backend_id=request.backend_id,
            name=request.name,
            url=request.url,
            health_endpoint=request.health_endpoint,
            mcp_endpoint=request.mcp_endpoint,
            capabilities=request.capabilities
        )
    except Exception as e:
        logger.exception(f"Failed to register backend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/announce")
async def announce_backend(
    request: AnnounceRequest,
    background_tasks: BackgroundTasks,
    http_request: Request,
):
    """
    Push-announce: a backend registers itself and its tools in one call.

    1. Registers the backend in DuckDB (synchronous)
    2. Registers each tool in DuckDB (synchronous)
    3. Queues independently gated embedding generation and graph projection
    """
    expected_token = (
        os.getenv("REGISTRY_ANNOUNCE_TOKEN", "")
        or os.getenv("TOOLMANAGEMENT_ANNOUNCE_TOKEN", "")
    )
    if expected_token:
        supplied_token = http_request.headers.get("x-registry-token", "")
        if not supplied_token:
            authorization = http_request.headers.get("authorization", "")
            if authorization.lower().startswith("bearer "):
                supplied_token = authorization[7:]
        if not secrets.compare_digest(supplied_token, expected_token):
            raise HTTPException(status_code=401, detail="Valid backend announce token required")
    elif os.getenv("REQUIRE_BEARER_AUTH", "0") == "1":
        # A production registry must never silently fall back to anonymous
        # backend mutation if its deployment secret was omitted.
        raise HTTPException(status_code=503, detail="Backend announcement is not configured")

    min_interval = int(os.getenv("ANNOUNCE_MIN_INTERVAL_SECONDS", "300"))
    now = time.time()
    last_seen, last_hash = _announce_last_seen.get(request.backend_id, (0.0, ""))

    # Fingerprint the announced surface so a CHANGED tool list always lands
    # immediately - the throttle only drops unchanged heartbeat announces.
    tools_hash = hashlib.sha256(json.dumps(
        sorted((t.get("name", ""), t.get("description", "")) for t in request.tools),
        default=str,
    ).encode()).hexdigest()

    if (
        min_interval > 0
        and (now - last_seen) < min_interval
        and tools_hash == last_hash
    ):
        return {
            "backend_id": request.backend_id,
            "registered": 0,
            "embedding_sync": "skipped_recent_announce",
            "graph_projection": "skipped_recent_announce",
            "min_interval_seconds": min_interval,
        }

    _announce_last_seen[request.backend_id] = (now, tools_hash)

    # Run blocking DuckDB writes in a worker thread so auth/MCP routes stay responsive.
    def _persist_announce() -> int:
        store = get_tool_store()
        store.register_backend(
            backend_id=request.backend_id,
            name=request.name,
            url=request.url,
            health_endpoint=request.health_endpoint,
            mcp_endpoint=request.mcp_endpoint,
            capabilities=request.capabilities,
        )

        registered_count = 0
        backend_category = request.backend_id.replace("distributed-", "").replace("-mcp", "").strip() or "general"
        for tool_data in request.tools:
            tags = tool_data.get("tags", [])
            if not tags:
                schema = tool_data.get("inputSchema", {})
                props = schema.get("properties", {})
                tags = list(props.keys())[:5]
            tool = RegisteredTool(
                tool_id=f"{request.backend_id}:{tool_data['name']}",
                name=tool_data["name"],
                backend_id=request.backend_id,
                backend_url=request.url,
                description=tool_data.get("description", ""),
                input_schema=tool_data.get("inputSchema", {}),
                tags=tags,
                category=tool_data.get("category", backend_category),
            )
            store.register_tool(tool)
            registered_count += 1

        # Remove tools this backend no longer announces (renames/deletions)
        try:
            announced_ids = [f"{request.backend_id}:{t['name']}" for t in request.tools]
            if announced_ids:
                store.prune_backend_tools(request.backend_id, announced_ids)
        except Exception as prune_err:  # noqa: BLE001
            logger.warning(f"Announce prune failed for {request.backend_id}: {prune_err}")

        store.update_backend_health(request.backend_id, True)
        return registered_count

    registered = await asyncio.to_thread(_persist_announce)

    # Embedding generation and GraphSub projection deliberately have separate
    # gates. A registry can keep its announce path lightweight while still
    # maintaining a current graph-native tool catalogue.
    #
    # Enrich the raw announced tools with the provider/category/tags the
    # persistence layer computed. The semantic embedders turn these fields
    # into embedded text, so the provider/backend label must be present here
    # or provider-name queries (e.g. discover_tools("cloudmail")) can never
    # match anything.
    enriched_tools = _enrich_announced_tools(request)
    embed_sync_enabled = _announce_embedding_sync_enabled()
    graph_projection_enabled = _announce_graph_projection_enabled()
    if embed_sync_enabled or graph_projection_enabled:
        background_tasks.add_task(_embed_and_graph_sync, request.backend_id, enriched_tools, request.url)
    embed_sync_status = "queued" if embed_sync_enabled else "disabled"
    graph_projection_status = "queued" if graph_projection_enabled else "disabled"

    return {
        "backend_id": request.backend_id,
        "registered": registered,
        "embedding_sync": embed_sync_status,
        "graph_projection": graph_projection_status,
    }


def _enabled_env(name: str, default: str = "0") -> bool:
    """Parse deployment feature flags consistently."""
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _enrich_announced_tools(request) -> List[Dict[str, Any]]:
    """Inject provider/category/tags into each announced tool for embedding.

    The persistence layer derives ``backend_category`` and per-tool ``tags`` at
    announce time, but previously handed the embedder the raw ``request.tools``
    so that metadata never reached the vector index. Semantic retrieval matches
    against embedded *text*, and the provider/backend label was only ever a
    filter column — so provider-name queries matched nothing. This builds
    enriched dicts (shallow copies; request.tools is left untouched) carrying
    the backend label, human source, category, and tags so they enter the
    embedded text.
    """
    backend_category = (
        request.backend_id.replace("distributed-", "").replace("-mcp", "").strip()
        or "general"
    )
    enriched: List[Dict[str, Any]] = []
    for tool_data in request.tools:
        tags = tool_data.get("tags", [])
        if not tags:
            schema = tool_data.get("inputSchema", {})
            props = schema.get("properties", {})
            tags = list(props.keys())[:5]

        merged_annotations = dict(tool_data.get("annotations") or {})
        merged_annotations["backend"] = request.backend_id

        enriched_tool = dict(tool_data)
        enriched_tool["category"] = tool_data.get("category", backend_category)
        enriched_tool["tags"] = tags
        enriched_tool["annotations"] = merged_annotations
        enriched_tool["source"] = request.name
        enriched.append(enriched_tool)
    return enriched


def _announce_embedding_sync_enabled() -> bool:
    return _enabled_env("ENABLE_ANNOUNCE_EMBED_SYNC", "1")


def _announce_graph_projection_enabled() -> bool:
    # Preserve existing full-sync behaviour for deployments that have not yet
    # opted into the explicit projection gate. Production sets this separately
    # so it can project announcements without triggering embedding work.
    return _enabled_env(
        "ENABLE_ANNOUNCE_GRAPH_PROJECTION",
        os.getenv("ENABLE_ANNOUNCE_EMBED_SYNC", "1"),
    )


async def _sync_announced_tool_embeddings(backend_id: str, tools: List[Dict]):
    """Generate embeddings for an announced backend's tool surface.

    Writes to the SAME SemanticToolStore that the semantic serving path reads
    (semantic_mcp_routes), so announce-write and semantic-read share one vector
    index. Previously this wrote only to the separate AdvancedToolIndex, which
    the search path never reads — that fragmentation was the write/read store
    split behind provider-name queries returning no matches.
    """
    try:
        from ..embeddings.semantic_tool_store import get_tool_store
        store = get_tool_store()
        await store.index_tools_batch(tools, backend_id, force=True)
        logger.info(f"Embedded {len(tools)} tools for {backend_id} into semantic tool store")
    except Exception as e:
        logger.warning(f"Embedding failed for {backend_id}: {e}")


async def _project_announced_tools_to_graph(
    backend_id: str,
    tools: List[Dict],
    backend_url: str,
    *,
    raise_errors: bool = False,
) -> int:
    """Project announced tool metadata to the configured graph substrate."""
    # GraphSub is the default substrate. Neo4j is available only with an
    # explicit GRAPH_SUBSTRATE=neo4j_compat selection.
    try:
        from ..services.graph_client import get_graph_substrate_client
        graph = get_graph_substrate_client()
        if not graph:
            if raise_errors:
                raise RuntimeError("graph substrate is not configured")
            return 0

        stmts = []
        for t in tools:
            stmts.append({
                "query": "MERGE (tool:Tool {tool_id: $tid}) "
                         "SET tool.name=$name, tool.description=$desc, "
                         "tool.backend_id=$bid, tool.backend_url=$burl, "
                         "tool.updated_at=datetime()",
                "parameters": {
                    "tid": f"{backend_id}:{t['name']}", "name": t["name"],
                    "desc": t.get("description", ""), "bid": backend_id, "burl": backend_url
                }
            })
        await graph.execute_transaction(stmts)
        logger.info(f"Graph substrate synced {len(tools)} tools for {backend_id}")
        return len(tools)
    except Exception as e:
        if raise_errors:
            logger.warning(
                "Graph substrate reconciliation failed for %s (%s)",
                backend_id,
                type(e).__name__,
            )
            raise
        logger.warning(f"Graph substrate sync failed for {backend_id}: {e}")
        return 0


async def _embed_and_graph_sync(backend_id: str, tools: List[Dict], backend_url: str):
    """Run only the announce background jobs enabled for this deployment."""
    if _announce_embedding_sync_enabled():
        await _sync_announced_tool_embeddings(backend_id, tools)
    if _announce_graph_projection_enabled():
        await _project_announced_tools_to_graph(backend_id, tools, backend_url)


@router.get("/backends")
async def list_backends(
    hours: int = Query(default=24, ge=1, le=720),
):
    """List all registered backends with rich latency/error and circuit-breaker metadata."""
    store = get_tool_store()
    breaker = get_backend_circuit_breaker()
    metrics_by_backend = store.get_backend_latency_metrics(hours=hours)
    circuits = breaker.get_all_states()

    enriched: List[Dict[str, Any]] = []
    for backend in store.list_backends():
        backend_id = backend.get("backend_id")
        enriched.append(
            {
                **backend,
                "metrics": metrics_by_backend.get(
                    backend_id,
                    {
                        "window_hours": hours,
                        "calls": 0,
                        "successes": 0,
                        "failures": 0,
                        "error_rate": 0.0,
                        "avg_latency_ms": 0.0,
                        "p50_latency_ms": 0.0,
                        "p95_latency_ms": 0.0,
                        "p99_latency_ms": 0.0,
                        "last_call_at": None,
                    },
                ),
                "circuit_breaker": circuits.get(backend_id, {"status": "closed"}),
            }
        )
    return enriched


@router.get("/backends/{backend_id}")
async def get_backend(
    backend_id: str,
    hours: int = Query(default=24, ge=1, le=720),
):
    """Get one backend enriched with latency/error metrics and circuit-breaker state."""
    store = get_tool_store()
    backend = store.get_backend(backend_id)
    if not backend:
        raise HTTPException(status_code=404, detail=f"Backend not found: {backend_id}")
    breaker = get_backend_circuit_breaker()
    return {
        **backend,
        "metrics": store.get_backend_latency_metrics(hours=hours).get(
            backend_id,
            {
                "window_hours": hours,
                "calls": 0,
                "successes": 0,
                "failures": 0,
                "error_rate": 0.0,
                "avg_latency_ms": 0.0,
                "p50_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "p99_latency_ms": 0.0,
                "last_call_at": None,
            },
        ),
        "circuit_breaker": breaker.get_state(backend_id),
    }


@router.get("/backends/{backend_id}/tools")
async def list_backend_tools(backend_id: str):
    """List all tools for a backend."""
    store = get_tool_store()
    tools = store.list_tools_by_backend(backend_id)
    return {"backend_id": backend_id, "tools": [t.name for t in tools], "count": len(tools)}


@router.get("/")
async def list_tools(backend_id: str = None, category: str = None, limit: int = 100, offset: int = 0):
    """List registered tools with pagination."""
    store = get_tool_store()
    tools = store.list_tools(backend_id=backend_id, category=category, limit=limit, offset=offset)
    total = store.count_tools(backend_id=backend_id, category=category)
    return {
        "tools": [{"tool_id": t.tool_id, "name": t.name, "backend": t.backend_id, "description": t.description} for t in tools],
        "count": len(tools),
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(tools) < total,
    }


@router.get("/stats/overview")
async def get_stats():
    """Get tool store statistics."""
    store = get_tool_store()
    return store.get_stats()


@router.get("/stats/categories")
async def get_categories():
    """Get distinct tool categories with counts."""
    store = get_tool_store()
    return {"categories": store.get_distinct_categories()}


@router.get("/stats/popular")
async def get_popular(limit: int = 10):
    """Get most-used tools by call count."""
    store = get_tool_store()
    return {"tools": store.get_popular_tools(limit=limit)}


@router.get("/stats/backends")
async def get_backend_metrics(
    hours: int = Query(default=24, ge=1, le=720),
):
    """Get backend-level latency/error metrics and circuit-breaker state."""
    store = get_tool_store()
    breaker = get_backend_circuit_breaker()
    metrics = store.get_backend_latency_metrics(hours=hours)
    states = breaker.get_all_states()
    enriched = {
        backend_id: {
            **payload,
            "circuit_breaker": states.get(backend_id, {"status": "closed"}),
        }
        for backend_id, payload in metrics.items()
    }
    return {
        "window_hours": hours,
        "count": len(enriched),
        "backends": enriched,
    }


@router.get("/analytics/overview")
async def get_analytics_overview(
    hours: int = Query(default=24, ge=1, le=720),
    bucket_minutes: int = Query(default=60, ge=1, le=1440),
    chain_limit: int = Query(default=20, ge=1, le=200),
):
    """Get a rich cross-surface analytics snapshot for MCP and general clients."""
    store = get_tool_store()
    execution_store = get_execution_store()

    response: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat(),
        "window_hours": hours,
        "bucket_minutes": bucket_minutes,
        "registry": store.get_stats(),
        "backend_metrics": store.get_backend_latency_metrics(hours=hours),
        "usage_timeseries": store.get_tool_usage_timeseries(
            hours=hours,
            bucket_minutes=bucket_minutes,
            limit=5000,
        ),
        "failure_modes": store.get_failure_modes(hours=hours, limit=50),
        "dependencies": store.list_tool_dependencies(limit=100),
    }

    try:
        response["execution_analytics"] = execution_store.get_usage_stats(hours=hours)
        response["workflow_analytics"] = execution_store.get_workflow_analytics(hours=hours)
        response["chain_analytics"] = execution_store.get_workflow_chain_analytics(
            hours=hours,
            limit=chain_limit,
            min_chain_length=2,
        )
        response["recommendation_feedback"] = execution_store.get_recommendation_feedback_analytics(hours=hours)
    except Exception as exc:
        response["execution_analytics_error"] = str(exc)

    return response


@router.post("/recommendations")
async def recommend_tools(request: ToolRecommendationRequest):
    """
    Recommend tools with transparent scoring for MCP and general clients.

    Ranking combines intent relevance, text relevance, reliability, usage, and latency.
    """
    store = get_tool_store()
    candidate_limit = max(50, min(500, request.limit * 20))
    query_text = (request.query or "").strip()
    intent = (request.intent or "").strip().lower() or None
    tags_filter = [tag for tag in request.tags if str(tag).strip()]

    candidates, _total = store.query_tools(
        search=query_text or None,
        backend_id=request.backend_id,
        category=request.category,
        tags=tags_filter or None,
        sort_by="call_count",
        sort_order="desc",
        limit=candidate_limit,
        offset=0,
    )

    scored_rows: List[Dict[str, Any]] = []
    for row in candidates:
        name = str(row.get("name") or "")
        description = str(row.get("description") or "")
        tool_id = str(row.get("tool_id") or "")
        call_count = int(row.get("call_count") or 0)
        error_count = int(row.get("error_count") or 0)
        avg_latency_ms = float(row.get("avg_latency_ms") or 0.0)
        error_rate = (float(error_count) / call_count) if call_count else 0.0
        reliability = 1.0 - error_rate

        searchable_text = f"{name} {description} {tool_id}"
        score_components = {
            "text_relevance": _text_relevance_score(query_text, searchable_text),
            "intent_relevance": _intent_score(intent, searchable_text),
            "reliability": max(0.0, min(1.0, reliability)),
            "usage_strength": min(1.0, call_count / 100.0),
            "latency_fit": 1.0 if avg_latency_ms <= 0 else max(0.0, min(1.0, 1.0 - (avg_latency_ms / 30000.0))),
        }

        if request.max_latency_ms is not None and avg_latency_ms > 0 and avg_latency_ms > request.max_latency_ms:
            score_components["latency_fit"] *= 0.25

        if request.require_streaming and avg_latency_ms > 0:
            if avg_latency_ms < 2000:
                score_components["latency_fit"] *= 0.8
            elif avg_latency_ms <= 30000:
                score_components["latency_fit"] = min(1.0, score_components["latency_fit"] + 0.2)

        weighted_score = (
            (0.30 * score_components["text_relevance"]) +
            (0.22 * score_components["intent_relevance"]) +
            (0.18 * score_components["reliability"]) +
            (0.15 * score_components["usage_strength"]) +
            (0.15 * score_components["latency_fit"])
        )
        weighted_score = max(0.0, min(1.0, weighted_score))

        reason_bits: List[str] = []
        if score_components["text_relevance"] >= 0.5:
            reason_bits.append("query_match")
        if score_components["intent_relevance"] >= 0.3:
            reason_bits.append("intent_match")
        if reliability >= 0.9 and call_count > 0:
            reason_bits.append("high_reliability")
        if avg_latency_ms > 0 and avg_latency_ms < 2000:
            reason_bits.append("low_latency")
        if call_count >= 10:
            reason_bits.append("proven_usage")
        if request.require_streaming:
            reason_bits.append("streaming_requested")

        scored_rows.append(
            {
                "tool_id": tool_id,
                "name": name,
                "backend_id": row.get("backend_id"),
                "category": row.get("category"),
                "description": description,
                "call_count": call_count,
                "error_count": error_count,
                "error_rate": error_rate,
                "avg_latency_ms": avg_latency_ms,
                "score": weighted_score,
                "confidence": min(1.0, 0.35 + (weighted_score * 0.6) + min(0.05, call_count / 1000.0)),
                "score_components": score_components,
                "recommended_execution_mode": _execution_mode_hint(avg_latency_ms, request.require_streaming),
                "reasons": reason_bits,
            }
        )

    # ML scoring overlay: when enabled, use SmartRecommender to rescore
    scoring_source = "heuristic"
    if _USE_ML_SCORING and query_text:
        recommender = _get_smart_recommender()
        if recommender is not None:
            try:
                execution_store = get_execution_store()
                ml_candidates = [
                    {"name": r["name"], "description": r.get("description", "")}
                    for r in scored_rows
                ]
                ml_results = await recommender.recommend(
                    task=query_text,
                    candidates=ml_candidates,
                    intent=intent,
                    top_k=len(scored_rows),
                    execution_store=execution_store,
                )
                # Build score lookup from ML results
                ml_score_map = {r.name: r for r in ml_results}
                for row in scored_rows:
                    ml_tool = ml_score_map.get(row["name"])
                    if ml_tool:
                        row["score"] = ml_tool.score
                        row["confidence"] = ml_tool.confidence
                        row["score_components"]["ml_score"] = ml_tool.score
                        row["score_components"]["ml_source"] = ml_tool.source
                        if ml_tool.historical_success_rate is not None:
                            row["score_components"]["historical_success_rate"] = ml_tool.historical_success_rate
                        if ml_tool.observation_count > 0:
                            row["score_components"]["observation_count"] = ml_tool.observation_count
                scoring_source = ml_results[0].source if ml_results else "heuristic"
            except Exception as ml_err:
                logger.warning(f"ML scoring failed, using heuristic: {ml_err}")

    scored_rows.sort(key=lambda item: (item["score"], item["call_count"]), reverse=True)
    selected = scored_rows[: request.limit]

    dependency_index: Dict[str, List[Dict[str, Any]]] = {}
    if request.include_dependencies and selected:
        dependencies = store.list_tool_dependencies(limit=2000)
        for edge in dependencies:
            source_id = edge.get("source_tool_id")
            if not source_id:
                continue
            dependency_index.setdefault(source_id, []).append(edge)

    recommendation_id = f"rec_{uuid4().hex}"
    recommendations: List[Dict[str, Any]] = []
    for idx, row in enumerate(selected, start=1):
        tool_dependencies = dependency_index.get(row["tool_id"], [])[:10]
        recommendations.append(
            {
                "rank": idx,
                **row,
                "dependencies": tool_dependencies,
            }
        )

    return {
        "recommendation_id": recommendation_id,
        "generated_at": datetime.utcnow().isoformat(),
        "query": request.query,
        "intent": intent,
        "scoring_source": scoring_source,
        "filters": {
            "backend_id": request.backend_id,
            "category": request.category,
            "tags": tags_filter,
            "max_latency_ms": request.max_latency_ms,
            "require_streaming": request.require_streaming,
        },
        "count": len(recommendations),
        "recommendations": recommendations,
    }


@router.post("/recommendations/feedback")
async def record_recommendation_feedback(request: RecommendationFeedbackRequest):
    """Persist recommendation outcome feedback for improving future routing."""
    execution_store = get_execution_store()
    record = execution_store.append_recommendation_feedback(
        recommendation_id=request.recommendation_id,
        context=request.context,
        recommendation=request.recommendation,
        chosen_tool=request.chosen_tool,
        accepted=request.accepted,
        outcome_status=request.outcome_status,
        latency_ms=request.latency_ms,
        error=request.error,
        metadata=request.metadata,
        trace_id=request.trace_id,
        session_id=request.session_id,
    )
    return {"status": "recorded", "feedback": record}


@router.get("/recommendations/analytics")
async def get_recommendation_feedback_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    limit: int = Query(default=25, ge=1, le=200),
):
    """Get recommendation acceptance/rejection analytics from durable feedback data."""
    execution_store = get_execution_store()
    return execution_store.get_recommendation_feedback_analytics(hours=hours, limit=limit)


@router.post("/query")
async def query_tools(
    search: Optional[str] = None,
    backend_id: Optional[str] = None,
    category: Optional[str] = None,
    tags: Optional[str] = None,
    sort_by: str = "name",
    sort_order: str = "asc",
    limit: int = 50,
    offset: int = 0,
    compact: bool = False,
):
    """
    Advanced tool query with text search, sorting, tag filtering, and compact mode.

    - search: Text search across name and description
    - tags: Comma-separated tag list (e.g., "web,api,search")
    - sort_by: name, call_count, updated_at, avg_latency_ms, error_count
    - compact: If true, omit full schemas — return only name, description, backend, signature
    """
    store = get_tool_store()
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    columns = [
        "tool_id",
        "name",
        "backend_id",
        "description",
        "call_count",
        "avg_latency_ms",
        "error_count",
        "category",
    ] if compact else None

    # Offload DuckDB query to a worker thread to avoid blocking the event loop.
    def _run_query():
        return store.query_tools(
            search=search,
            backend_id=backend_id,
            category=category,
            tags=tag_list,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
            columns=columns,
        )

    tools, total = await asyncio.to_thread(_run_query)

    return {
        "tools": tools,
        "count": len(tools),
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": offset + len(tools) < total,
        "compact": compact,
    }


class BatchGetRequest(BaseModel):
    """Request to fetch multiple tools by name."""
    names: List[str] = Field(..., description="List of tool names to fetch")
    compact: bool = Field(default=False, description="Return compact summaries instead of full schemas")


@router.post("/batch-get")
async def batch_get_tools(request: BatchGetRequest):
    """
    Fetch multiple tools by name in one call.
    With compact=true, returns signature summaries instead of full JSON schemas.
    """
    store = get_tool_store()

    not_found = []
    if request.compact:
        results = []
        for name in request.names:
            tool = store.get_tool_by_name(name)
            if tool:
                summary = store.summarize_tool_schema(tool.tool_id)
                if summary:
                    results.append(summary)
                    continue
            not_found.append(name)
        return {"tools": results, "count": len(results), "not_found": not_found, "format": "compact"}
    else:
        tools = store.get_tools_batch(request.names)
        found_names = {t.get("name") or t.get("tool_id", "").split(":", 1)[-1] for t in tools} if isinstance(tools, list) else set()
        not_found = [n for n in request.names if n not in found_names]
        return {"tools": tools, "count": len(tools), "not_found": not_found, "format": "full"}


@router.get("/metrics/{tool_name}")
async def get_tool_metrics(
    tool_name: str,
    hours: int = Query(default=24, ge=1, le=720),
):
    """Get durable latency/error metrics for one tool."""
    store = get_tool_store()
    metrics = store.get_tool_latency_metrics(tool_name, hours=hours)
    if not metrics:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    return metrics


@router.get("/analytics/usage")
async def get_tool_usage_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    bucket_minutes: int = Query(default=60, ge=1, le=1440),
    tool_name: Optional[str] = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=10000),
):
    """Get bucketed usage and latency trends from durable call history."""
    store = get_tool_store()
    return store.get_tool_usage_timeseries(
        hours=hours,
        bucket_minutes=bucket_minutes,
        tool_ref=tool_name,
        limit=limit,
    )


@router.get("/analytics/failures")
async def get_tool_failure_analytics(
    hours: int = Query(default=24, ge=1, le=720),
    tool_name: Optional[str] = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
):
    """Get top failure modes from durable call history."""
    store = get_tool_store()
    return store.get_failure_modes(hours=hours, tool_ref=tool_name, limit=limit)


@router.post("/dependencies")
async def upsert_tool_dependency(request: DependencyUpsertRequest):
    """Create/update a dependency edge between two tools."""
    store = get_tool_store()
    try:
        return store.upsert_tool_dependency(
            source_tool_ref=request.source_tool,
            target_tool_ref=request.target_tool,
            dependency_type=request.dependency_type,
            confidence=request.confidence,
            call_count=request.call_count,
            metadata=request.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/dependencies")
async def list_tool_dependencies(
    tool_name: Optional[str] = Query(default=None),
    dependency_type: Optional[str] = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """List durable dependency edges for tools."""
    store = get_tool_store()
    dependencies = store.list_tool_dependencies(
        tool_ref=tool_name,
        dependency_type=dependency_type,
        min_confidence=min_confidence,
        limit=limit,
    )
    return {
        "count": len(dependencies),
        "dependencies": dependencies,
    }


@router.post("/dependencies/infer")
async def infer_tool_dependencies(request: InferDependenciesRequest):
    """
    Infer dependency edges from persisted workflow chains and optionally persist them.
    """
    store = get_tool_store()
    execution_store = get_execution_store()
    chain_data = execution_store.get_workflow_chain_analytics(
        hours=request.hours,
        limit=request.limit,
        min_chain_length=2,
    )
    inferred_map: Dict[str, Dict[str, Any]] = {}

    for chain in chain_data.get("chains", []):
        steps = chain.get("steps") or []
        runs = int(chain.get("runs") or 0)
        completed = int(chain.get("completed") or 0)
        failed = int(chain.get("failed") or 0)
        if len(steps) < 2:
            continue
        for idx in range(len(steps) - 1):
            source_name = str(steps[idx] or "").strip()
            target_name = str(steps[idx + 1] or "").strip()
            if not source_name or not target_name:
                continue
            source_tool_id = store.resolve_tool_id(source_name)
            target_tool_id = store.resolve_tool_id(target_name)
            if not source_tool_id or not target_tool_id:
                continue
            edge_key = f"{source_tool_id}->{target_tool_id}"
            bucket = inferred_map.setdefault(
                edge_key,
                {
                    "source_tool_id": source_tool_id,
                    "source_name": source_name,
                    "target_tool_id": target_tool_id,
                    "target_name": target_name,
                    "support": 0,
                    "completed": 0,
                    "failed": 0,
                },
            )
            bucket["support"] += runs
            bucket["completed"] += completed
            bucket["failed"] += failed

    inferred_edges: List[Dict[str, Any]] = []
    persisted = 0
    for edge in inferred_map.values():
        support = int(edge["support"])
        if support < request.min_support:
            continue
        confidence = float(edge["completed"]) / support if support else 0.0
        edge_record = {
            "source_tool_id": edge["source_tool_id"],
            "source_name": edge["source_name"],
            "target_tool_id": edge["target_tool_id"],
            "target_name": edge["target_name"],
            "dependency_type": "inferred",
            "confidence": confidence,
            "support": support,
            "completed": int(edge["completed"]),
            "failed": int(edge["failed"]),
            "window_hours": request.hours,
        }
        inferred_edges.append(edge_record)

        if request.persist:
            try:
                store.upsert_tool_dependency(
                    source_tool_ref=edge["source_tool_id"],
                    target_tool_ref=edge["target_tool_id"],
                    dependency_type="inferred",
                    confidence=confidence,
                    call_count=support,
                    metadata={
                        "window_hours": request.hours,
                        "completed": int(edge["completed"]),
                        "failed": int(edge["failed"]),
                    },
                )
                persisted += 1
            except Exception as exc:
                logger.warning(f"Failed to persist inferred dependency {edge['source_tool_id']}->{edge['target_tool_id']}: {exc}")

    inferred_edges.sort(key=lambda item: (-int(item["support"]), -float(item["confidence"])))
    return {
        "window_hours": request.hours,
        "min_support": request.min_support,
        "count": len(inferred_edges),
        "persisted": persisted,
        "dependencies": inferred_edges,
    }


@router.get("/export/schema-bundle")
async def export_schema_bundle(
    limit: int = Query(default=1000, ge=1, le=10000),
):
    """Export registry schemas as a JSON Schema bundle keyed by tool_id."""
    store = get_tool_store()
    tools = store.list_tools(limit=limit, offset=0)
    bundle: Dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "generated_at": datetime.utcnow().isoformat(),
        "count": len(tools),
        "tools": {},
    }
    for tool in tools:
        bundle["tools"][tool.tool_id] = {
            "name": tool.name,
            "backend_id": tool.backend_id,
            "description": tool.description,
            "input_schema": tool.input_schema or {"type": "object", "properties": {}},
            "output_schema": tool.output_schema or {"type": "object", "properties": {}},
            "tags": tool.tags or [],
            "category": tool.category,
            "version": tool.version,
        }
    return bundle


@router.get("/export/mcp-tools")
async def export_mcp_tools(
    limit: int = Query(default=1000, ge=1, le=10000),
):
    """Export durable registry entries in MCP tool-list format."""
    store = get_tool_store()
    tools = store.list_tools(limit=limit, offset=0)
    result: List[Dict[str, Any]] = []
    for tool in tools:
        result.append(
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema or {"type": "object", "properties": {}},
                "outputSchema": tool.output_schema or None,
                "annotations": {
                    "tool_id": tool.tool_id,
                    "backend": tool.backend_id,
                    "category": tool.category,
                    "tags": tool.tags or [],
                    "version": tool.version,
                    "avg_latency_ms": float(tool.avg_latency_ms or 0.0),
                    "error_rate": float(tool.error_count or 0) / max(1, int(tool.call_count or 0)),
                },
            }
        )
    return {
        "generated_at": datetime.utcnow().isoformat(),
        "count": len(result),
        "tools": result,
    }


@router.get("/export/openapi")
async def export_openapi_tool_catalog(
    limit: int = Query(default=500, ge=1, le=5000),
):
    """
    Export an OpenAPI-compatible catalog for general REST clients.

    The exported schema focuses on dynamic execution endpoints while embedding
    per-tool argument schemas under components for client-side validation.
    """
    store = get_tool_store()
    tools = store.list_tools(limit=limit, offset=0)

    tool_components: Dict[str, Any] = {}
    tool_index: List[Dict[str, Any]] = []
    for tool in tools:
        component_name = tool.tool_id.replace(":", "__").replace("-", "_")
        tool_components[component_name] = {
            "type": "object",
            "properties": tool.input_schema.get("properties", {}) if isinstance(tool.input_schema, dict) else {},
            "required": tool.input_schema.get("required", []) if isinstance(tool.input_schema, dict) else [],
            "additionalProperties": True,
            "description": tool.description or f"Arguments for {tool.tool_id}",
        }
        tool_index.append(
            {
                "tool_id": tool.tool_id,
                "name": tool.name,
                "backend_id": tool.backend_id,
                "component": f"#/components/schemas/{component_name}",
            }
        )

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "DSCO Tool Management API - Tool Catalog",
            "version": "1.0.0",
            "description": "Dynamic execution API with durable tool registry metadata.",
        },
        "servers": [{"url": "/"}],
        "paths": {
            "/api/v1/execute": {
                "post": {
                    "summary": "Execute a tool (sync/stream/job/auto)",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "tool_name": {"type": "string"},
                                        "arguments": {"type": "object"},
                                        "execution_mode": {
                                            "type": "string",
                                            "enum": ["auto", "sync", "stream", "job"],
                                            "default": "auto",
                                        },
                                        "stream": {"type": "boolean", "default": False},
                                        "timeout_ms": {"type": "integer", "default": 30000},
                                        "sync_timeout_ms": {"type": "integer", "default": 5000},
                                        "idempotency_key": {"type": "string"},
                                    },
                                    "required": ["tool_name", "arguments"],
                                    "additionalProperties": False,
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Execution result or job handle",
                            "content": {"application/json": {"schema": {"type": "object"}}},
                        }
                    },
                }
            },
            "/api/v1/tools/execute": {
                "post": {
                    "summary": "Execute a single tool through tools API",
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {
                        "200": {
                            "description": "Tool result",
                            "content": {"application/json": {"schema": {"type": "object"}}},
                        }
                    },
                }
            },
        },
        "components": {
            "schemas": tool_components,
        },
        "x-tool-catalog": {
            "generated_at": datetime.utcnow().isoformat(),
            "count": len(tool_index),
            "tools": tool_index,
        },
    }


@router.get("/names")
async def list_all_tool_names():
    """All tool names (in-process + durable backends), deduped."""
    from ..catalog_view import merged_names
    return {"names": merged_names()}


@router.get("/tags")
async def list_all_tags():
    """All unique tags (in-process + durable backends)."""
    from ..catalog_view import merged_tags
    return {"tags": merged_tags()}


@router.get("/categories")
async def list_all_categories():
    """All categories (in-process + durable backends)."""
    from ..catalog_view import merged_categories
    return {"categories": merged_categories()}


@router.get("/mcp")
async def list_all_mcp_tools():
    """All tools in MCP tool-list format (in-process + durable backends)."""
    from ..catalog_view import merged_mcp_tools
    return {"tools": merged_mcp_tools()}


@router.get("/schema/{tool_name}")
async def get_tool_schema_summary(tool_name: str):
    """
    Get a compact schema summary for a tool: parameter names, types, required flags.
    Much smaller than full JSON schema — ideal for context-limited AI agents.
    """
    store = get_tool_store()
    tool = store.get_tool_by_name(tool_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    summary = store.summarize_tool_schema(tool.tool_id)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Could not summarize: {tool_name}")
    return summary


@router.get("/{tool_name}")
async def get_tool(tool_name: str):
    """Get tool details by name from the durable store or the in-process registry."""
    store = get_tool_store()
    try:
        tool = store.get_tool_by_name(tool_name)
    except Exception as e:
        logger.warning(f"Tool store lookup failed for {tool_name}: {e}")
        tool = None
    if not tool:
        # Built-in/registry tools are not announced into the durable store
        from ..registry import get_registry as get_local_registry

        local_tool = get_local_registry().get_by_name(tool_name)
        if local_tool:
            return {
                "tool_id": local_tool.id,
                "name": local_tool.name,
                "backend": "local-registry",
                "backend_url": None,
                "description": local_tool.description,
                "input_schema": {
                    "type": "object",
                    "properties": {
                        inp.name: {
                            "type": inp.type,
                            "description": inp.description,
                        }
                        for inp in (local_tool.inputs or [])
                    },
                    "required": [
                        inp.name for inp in (local_tool.inputs or []) if inp.required
                    ],
                },
                "tags": local_tool.tags,
                "category": local_tool.category,
                "call_count": None,
                "avg_latency_ms": None,
                "error_count": None,
                "error_rate": None,
                "latency_metrics_24h": {},
                "dependencies": [],
            }
        raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
    try:
        window_metrics = store.get_tool_latency_metrics(tool.tool_id, hours=24) or {}
        dependencies = store.list_tool_dependencies(tool_ref=tool.tool_id, limit=20)
    except Exception as e:
        logger.warning(f"Tool metrics lookup failed for {tool_name}: {e}")
        window_metrics = {}
        dependencies = []
    return {
        "tool_id": tool.tool_id,
        "name": tool.name,
        "backend": tool.backend_id,
        "backend_url": tool.backend_url,
        "description": tool.description,
        "input_schema": tool.input_schema,
        "tags": tool.tags,
        "category": tool.category,
        "call_count": tool.call_count,
        "avg_latency_ms": tool.avg_latency_ms,
        "error_count": tool.error_count,
        "error_rate": tool.error_count / max(1, tool.call_count),
        "latency_metrics_24h": window_metrics,
        "dependencies": dependencies,
    }


@router.post("/sync/{backend_id}")
async def sync_backend_tools(backend_id: str):
    """
    Sync tools from a backend via MCP protocol.

    Fetches tools/list from the backend and registers them.
    """
    store = get_tool_store()
    backend = store.get_backend(backend_id)

    if not backend:
        raise HTTPException(status_code=404, detail=f"Backend not found: {backend_id}")

    try:
        client = get_client()
        url = f"{backend['url']}/mcp"
        # Streamable-HTTP MCP requires: Authorization bearer, an initialize
        # handshake to obtain a session id, and an Accept header covering
        # text/event-stream. Bare tools/list without these returns 400/401/406
        # on strict servers (e.g. cloudmail).
        headers = _backend_headers(backend_id)
        headers.setdefault("Content-Type", "application/json")
        headers["Accept"] = "application/json, text/event-stream"

        def _parse(body: str) -> dict:
            # MCP may answer as SSE ("data: {...}") or plain JSON.
            text = body.strip()
            if text.startswith("data:") or "\ndata:" in text:
                for line in text.splitlines():
                    line = line.strip()
                    if line.startswith("data:"):
                        payload = line[5:].strip()
                        if payload:
                            try:
                                return json.loads(payload)
                            except Exception:
                                continue
                return {}
            return json.loads(text)

        session_id: Optional[str] = None
        try:
            init_resp = await client.post(
                url,
                json={
                    "jsonrpc": "2.0",
                    "id": "init",
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-03-26",
                        "capabilities": {},
                        "clientInfo": {"name": "tool-registry-sync", "version": "1.0"},
                    },
                },
                headers=headers,
                timeout=15.0,
            )
            session_id = init_resp.headers.get("mcp-session-id")
        except Exception:
            session_id = None  # fall through to a session-less attempt

        list_headers = dict(headers)
        if session_id:
            list_headers["Mcp-Session-Id"] = session_id

        response = await client.post(
            url,
            json={
                "jsonrpc": "2.0",
                "id": "sync",
                "method": "tools/list",
                "params": {}
            },
            headers=list_headers,
            timeout=15.0
        )

        if response.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Backend returned {response.status_code}")

        data = _parse(response.text)
        tools = data.get("result", {}).get("tools", [])

        registered = 0
        backend_category = backend_id.replace("distributed-", "").replace("-mcp", "").strip() or "general"
        for tool_data in tools:
            tags = tool_data.get("tags", [])
            if not tags:
                schema = tool_data.get("inputSchema", {})
                props = schema.get("properties", {})
                tags = list(props.keys())[:5]
            tool = RegisteredTool(
                tool_id=f"{backend_id}:{tool_data['name']}",
                name=tool_data["name"],
                backend_id=backend_id,
                backend_url=backend["url"],
                description=tool_data.get("description", ""),
                input_schema=tool_data.get("inputSchema", {}),
                tags=tags,
                category=tool_data.get("category", backend_category),
            )
            store.register_tool(tool)
            registered += 1

        store.update_backend_health(backend_id, True)
        return {"backend_id": backend_id, "synced": registered, "tools": [t["name"] for t in tools]}

    except httpx.TimeoutException:
        store.update_backend_health(backend_id, False)
        raise HTTPException(status_code=504, detail="Backend timeout")
    except Exception as e:
        store.update_backend_health(backend_id, False)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync-all")
async def sync_all_backends():
    """Sync tools from all registered backends."""
    store = get_tool_store()
    backends = store.list_backends()

    results = []
    for backend in backends:
        try:
            result = await sync_backend_tools(backend["backend_id"])
            results.append({"backend": backend["backend_id"], "status": "success", "synced": result["synced"]})
        except Exception as e:
            results.append({"backend": backend["backend_id"], "status": "error", "error": str(e)})

    total_synced = sum(r.get("synced", 0) for r in results if r["status"] == "success")
    return {"backends": results, "total_synced": total_synced}


def _is_write_tool(tool_ref: str, resolved_name: str) -> bool:
    if resolved_name in _WRITE_TOOL_NAMES:
        return True
    if tool_ref in _WRITE_TOOL_NAMES:
        return True
    if ":" in tool_ref and tool_ref.split(":", 1)[-1] in _WRITE_TOOL_NAMES:
        return True
    return False


def _hash_arguments(arguments: Dict[str, Any]) -> str:
    stable_arguments = {
        k: v
        for k, v in arguments.items()
        if k not in {"_trace_id", "_session_id", "_parent_execution_id"}
    }
    stable_payload = json.dumps(stable_arguments, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()
