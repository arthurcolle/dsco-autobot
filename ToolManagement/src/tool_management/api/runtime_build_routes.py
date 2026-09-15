"""Authenticated RuntimeSpec build control-plane API.

This API is intentionally an approval gate, not a remote compiler.  It stores
only a validated public RuntimeSpec; credentials, leases, signing keys, source
checkouts, and binary artifacts are never accepted through this surface.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from ..core.rate_limit import get_quota_manager
from ..persistence import get_runtime_build_store
from .tools_api import resolve_execution_identity

router = APIRouter(prefix="/api/v1/runtime-builds", tags=["runtime-builds"])
_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}\Z")
_FORBIDDEN_KEY = re.compile(r"(?:credential|secret|token|password|api[_-]?key|private[_-]?key|authorization)", re.I)
_MAX_SPEC_BYTES = 64 * 1024


class EntityBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    organization_id: str = Field(min_length=36, max_length=36)
    deployment_id: str = Field(min_length=36, max_length=36)
    capsule_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class RuntimeBuildCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    runtime_spec: dict[str, Any] = Field(..., description="Strict distributed.systems/v1alpha1 RuntimeSpec. Credentials are forbidden.")
    entity_binding: EntityBinding | None = Field(
        default=None,
        description="Immutable signed EntityCapsule identity; contains no credentials.",
    )


def _factory_path() -> Path:
    configured = os.getenv("DSCO_CLOUD_BUILD_FACTORY_PATH")
    if configured:
        return Path(configured)
    return Path("/opt/dsco-cli/scripts/cloud_build_factory.py")


@lru_cache(maxsize=1)
def _factory() -> Any:
    path = _factory_path()
    if not path.is_file():
        raise RuntimeError("runtime_spec_validator_unavailable")
    spec = importlib.util.spec_from_file_location("dsco_cloud_build_factory", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("runtime_spec_validator_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _reject_secretish(value: Any) -> None:
    """Fail before persistence if a client tries to make RuntimeSpec a vault."""
    if isinstance(value, dict):
        for key, child in value.items():
            # These two names are mandatory policy assertions in the closed
            # schema, and their allowed values explicitly say no secret is
            # present. All other secret-like fields are rejected before disk.
            policy_assertion = (
                (key == "credentialMaterial" and child == "excluded")
                or (key == "secretIsolation" and child == "account-vault-reference-only")
            )
            if _FORBIDDEN_KEY.search(str(key)) and not policy_assertion:
                raise HTTPException(status_code=422, detail={"error": "runtime_spec_credentials_forbidden"})
            _reject_secretish(child)
    elif isinstance(value, list):
        for child in value:
            _reject_secretish(child)


def _validate_runtime_spec(runtime_spec: dict[str, Any]) -> tuple[dict[str, Any], str]:
    try:
        encoded = json.dumps(runtime_spec, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, UnicodeEncodeError) as exc:
        raise HTTPException(status_code=422, detail={"error": "runtime_spec_not_canonical_json"}) from exc
    if len(encoded) > _MAX_SPEC_BYTES:
        raise HTTPException(status_code=413, detail={"error": "runtime_spec_too_large"})
    _reject_secretish(runtime_spec)
    try:
        validated, _ceilings, digest = _factory().validate_runtime_spec(runtime_spec)
    except HTTPException:
        raise
    except Exception as exc:
        # Schema errors are intentionally generic: do not echo client payloads.
        message = str(exc)
        if message == "runtime_spec_validator_unavailable":
            raise HTTPException(status_code=503, detail={"error": message}) from exc
        raise HTTPException(status_code=422, detail={"error": "invalid_runtime_spec"}) from exc
    return validated, digest


def _build_blocker() -> str:
    """Remote issuance is disabled unless a separately deployed private worker exists."""
    if os.getenv("RUNTIME_BUILD_EXECUTOR", "").strip() != "private-modal-v1":
        return "private_build_worker_unavailable"
    if os.getenv("RUNTIME_BUILD_SIGNER_SECRET_BOUND", "") != "1":
        return "offline_signer_unavailable"
    if os.getenv("RUNTIME_BUILD_CLEAN_CHECKOUT", "") != "1":
        return "clean_pinned_checkout_unavailable"
    # This service intentionally has no execution credential.  A future private
    # worker may atomically change this row to building/built after it has both
    # the offline signer and a clean pinned source checkout.
    return "private_build_worker_not_attested"


def _public(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": row["request_id"],
        "runtime_spec_sha256": row["runtime_spec_sha256"],
        "status": row["status"],
        "approval": row["approval_state"],
        "blocker_code": row.get("blocker_code"),
        "artifact": row.get("artifact"),
        "entity_binding": row.get("entity_binding"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "approved_at": row.get("approved_at"),
        "local_customer_build": {
            "available": row["approval_state"] == "approved",
            "requires": ["a clean customer-owned dsco-cli checkout", "an issuer-provided activation lease", "customer-controlled release signing material"],
            "factory": "scripts/cloud_build_factory.py",
            "note": "No downloadable binary exists until a private signer and clean pinned checkout attest a build.",
        },
    }


def _customer_identity(request: Request) -> dict[str, Any]:
    identity = resolve_execution_identity(request)
    if identity.get("kind") != "customer" or not identity.get("tenant_id"):
        raise HTTPException(status_code=403, detail={"error": "customer_runtime_build_identity_required"})
    return identity


@router.post("", status_code=202)
async def create_runtime_build(
    body: RuntimeBuildCreate,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    """Create a tenant-owned RuntimeSpec request. Customer approval is required."""
    identity = _customer_identity(request)
    if not idempotency_key or not _IDEMPOTENCY_KEY.fullmatch(idempotency_key):
        raise HTTPException(status_code=400, detail={"error": "idempotency_key_required"})
    validated, digest = _validate_runtime_spec(body.runtime_spec)
    entity_binding = body.entity_binding.model_dump() if body.entity_binding else None
    payload_sha256 = hashlib.sha256(
        json.dumps(
            {"runtime_spec": validated, "entity_binding": entity_binding},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    quota = get_quota_manager().check_and_consume(identity["tenant_id"], "runtime_build_request")
    if not quota.allowed:
        raise HTTPException(status_code=429, detail={"error": "runtime_build_quota_exceeded", "retry_after": quota.retry_after})
    try:
        row, replay = get_runtime_build_store().create_or_get(
            tenant_id=identity["tenant_id"], idempotency_key=idempotency_key,
            payload_sha256=payload_sha256, runtime_spec=validated,
            runtime_spec_sha256=digest, blocker_code=_build_blocker(),
            entity_binding=entity_binding,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc
    finally:
        get_quota_manager().release(identity["tenant_id"])
    if not replay:
        from ..persistence.billing_store import get_billing_store
        get_billing_store().record_usage(identity["tenant_id"], "runtime_build_request", ref=digest)
    response = _public(row)
    response["idempotent_replay"] = replay
    response["approval_url"] = f"/api/v1/runtime-builds/{row['request_id']}/approve"
    response["status_url"] = f"/api/v1/runtime-builds/{row['request_id']}"
    return response


@router.post("/{request_id}/approve")
async def approve_runtime_build(request_id: str, request: Request):
    """Record explicit customer approval; fail closed if private issuance is absent."""
    identity = _customer_identity(request)
    row = get_runtime_build_store().approve(request_id, identity["tenant_id"], _build_blocker())
    if not row:
        raise HTTPException(status_code=404, detail={"error": "runtime_build_not_found"})
    return _public(row)


@router.get("/{request_id}")
async def get_runtime_build(request_id: str, request: Request):
    identity = _customer_identity(request)
    row = get_runtime_build_store().get(request_id, identity["tenant_id"])
    if not row:
        raise HTTPException(status_code=404, detail={"error": "runtime_build_not_found"})
    return _public(row)


@router.get("")
async def list_runtime_builds(request: Request, limit: int = 50):
    identity = _customer_identity(request)
    if not 1 <= limit <= 100:
        raise HTTPException(status_code=422, detail={"error": "invalid_limit"})
    rows = get_runtime_build_store().list(identity["tenant_id"], limit)
    return {"requests": [_public(row) for row in rows], "count": len(rows)}
