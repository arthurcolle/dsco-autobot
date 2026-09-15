"""Consent-first Substack SID connection and autonomous notification webhooks."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from ..core.streaming import validate_callback_url
from ..oauth import TOKEN_SECRET
from ..services.publish_authorizations import (
    get_publish_authorization_store,
    public_record,
)
from ..services.tenant_credentials import _default_store as get_tenant_store
from .user_routes import resolve_user_from_request


router = APIRouter(prefix="/api/v1/publish", tags=["distributed-publish"])

_REQUEST_ID_RE = re.compile(r"^pubauth_[0-9a-f]{32}$")
_HOST_RE = re.compile(r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def _iso(timestamp: Optional[float]) -> Optional[str]:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_publication(value: str) -> str:
    raw = str(value or "").strip()
    parsed = urlsplit(raw if "://" in raw else f"https://{raw}")
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("enter a publication hostname, without a path or query")
    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or not _HOST_RE.fullmatch(host):
        raise ValueError("enter a valid public publication hostname")
    return host


def _require_user(request: Request) -> Dict[str, Any]:
    user = resolve_user_from_request(request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail={"error": "user_identity_required", "account_url": "/account"},
        )
    return user


def _base_url(request: Request) -> str:
    configured = os.getenv("API_BASE_URL", "").strip()
    return configured.rstrip("/") if configured else str(request.base_url).rstrip("/")


def _authorization_url(request: Request, request_id: str, token: str) -> str:
    # The token lives in the URL fragment: browsers never send it in GET logs or
    # Referer headers. The authorization page removes it from history immediately.
    return f"{_base_url(request)}/publish/authorize/{request_id}#token={token}"


def _webhook_master() -> bytes:
    configured = os.getenv("PUBLISH_AUTH_WEBHOOK_SIGNING_KEY", "").strip()
    if configured:
        return configured.encode("utf-8")
    if os.getenv("DSCO_DEPLOYMENT_MODE", "").strip().lower() == "production":
        raise RuntimeError("PUBLISH_AUTH_WEBHOOK_SIGNING_KEY is required in production")
    return TOKEN_SECRET.encode("utf-8")


def _webhook_secret(record: Dict[str, Any]) -> str:
    material = f"v1\0{record['tenant_id']}\0{record['request_id']}".encode("utf-8")
    return hmac.new(_webhook_master(), material, hashlib.sha256).hexdigest()


def _event_payload(record: Dict[str, Any], event: str, authorization_url: Optional[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema": "dsco.publish.authorization.v1",
        "event": event,
        "request_id": record["request_id"],
        "publication": record["publication"],
        "label": record.get("label"),
        "status": record["status"],
        "created_at": _iso(record["created_at"]),
        "expires_at": _iso(record["expires_at"]),
        "completed_at": _iso(record.get("completed_at")),
    }
    if authorization_url is not None:
        payload["authorization_url"] = authorization_url
    return payload


def _formatted_public_record(record: Dict[str, Any]) -> Dict[str, Any]:
    response = public_record(record)
    for field in ("created_at", "expires_at", "completed_at"):
        response[field] = _iso(response[field])
    last_attempt = response["webhook"].get("last_attempt_at")
    if last_attempt is not None:
        response["webhook"]["last_attempt_at"] = _iso(last_attempt)
    return response


async def deliver_authorization_webhook(
    request_id: str,
    event: str,
    authorization_url: Optional[str] = None,
) -> None:
    """Deliver a signed notification with bounded retry and fresh SSRF checks."""
    store = get_publish_authorization_store()
    record = store.get(request_id)
    if not record or not record.get("callback_url"):
        return

    payload = _event_payload(record, event, authorization_url)
    canonical_body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    timestamp = str(int(time.time()))
    signature = hmac.new(
        _webhook_secret(record).encode("utf-8"),
        timestamp.encode("ascii") + b"." + canonical_body,
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "DSCO-Publish-Authorization/1.0",
        "X-DSCO-Webhook-Event": event,
        "X-DSCO-Webhook-Id": f"wh_{uuid4().hex}",
        "X-DSCO-Webhook-Timestamp": timestamp,
        "X-DSCO-Webhook-Signature": f"sha256={signature}",
    }
    attempts = max(1, min(6, int(os.getenv("PUBLISH_AUTH_WEBHOOK_ATTEMPTS", "4"))))
    backoff = max(0.0, float(os.getenv("PUBLISH_AUTH_WEBHOOK_BACKOFF_SECONDS", "1")))
    timeout = max(1.0, min(30.0, float(os.getenv("PUBLISH_AUTH_WEBHOOK_TIMEOUT_SECONDS", "6"))))

    for attempt in range(1, attempts + 1):
        status_code: Optional[int] = None
        try:
            # Validate every attempt so a queued request cannot silently pivot
            # to a private address through a later DNS answer.
            safe_url = await asyncio.to_thread(validate_callback_url, record["callback_url"])
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                response = await client.post(safe_url, content=canonical_body, headers=headers)
            status_code = int(response.status_code)
            succeeded = 200 <= status_code < 300
            store.record_webhook_delivery(
                request_id,
                event=event,
                status_code=status_code,
                success=succeeded,
                error=None if succeeded else f"HTTP {status_code}",
            )
            if succeeded:
                return
        except Exception as exc:
            store.record_webhook_delivery(
                request_id,
                event=event,
                status_code=status_code,
                success=False,
                error=type(exc).__name__,
            )
        if attempt < attempts and backoff:
            await asyncio.sleep(backoff * (2 ** (attempt - 1)))


class AuthorizationCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    publication: str = Field(min_length=3, max_length=253)
    label: Optional[str] = Field(default=None, max_length=120)
    notification_webhook_url: Optional[str] = Field(default=None, max_length=2048)
    ttl_seconds: int = Field(default=900, ge=300, le=3600)

    @field_validator("publication")
    @classmethod
    def validate_publication(cls, value: str) -> str:
        return _normalize_publication(value)

    @field_validator("label")
    @classmethod
    def clean_label(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        if any(ord(char) < 32 for char in cleaned):
            raise ValueError("label cannot contain control characters")
        return cleaned or None


class AuthorizationCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_token: SecretStr = Field(min_length=32, max_length=512)
    sid: SecretStr = Field(min_length=16, max_length=4096)
    authorize_connection: Literal[True]


@router.post("/authorizations", status_code=status.HTTP_201_CREATED)
async def create_authorization(
    body: AuthorizationCreateRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    user = _require_user(request)
    callback_url = body.notification_webhook_url
    if callback_url:
        try:
            callback_url = await asyncio.to_thread(validate_callback_url, callback_url)
            # Fail now if production webhook signing is not configured.
            _webhook_master()
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(
                status_code=422,
                detail={"error": "unsafe_or_unconfigured_webhook"},
            ) from exc

    token = secrets.token_urlsafe(32)
    request_id = f"pubauth_{uuid4().hex}"
    expires_at = time.time() + body.ttl_seconds
    store = get_publish_authorization_store()
    record = store.create(
        request_id=request_id,
        tenant_id=user["user_id"],
        publication=body.publication,
        label=body.label,
        callback_url=callback_url,
        token_hash=store.hash_token(token),
        expires_at=expires_at,
    )
    authorization_url = _authorization_url(request, request_id, token)
    if callback_url:
        background_tasks.add_task(
            deliver_authorization_webhook,
            request_id,
            "substack.authorization.requested",
            authorization_url,
        )

    response = _formatted_public_record(record)
    response["authorization_url"] = authorization_url
    response["webhook_secret"] = _webhook_secret(record) if callback_url else None
    response["credential_handling"] = "SID accepted once, encrypted at rest, never included in webhooks"
    return response


@router.get("/authorizations/{request_id}")
async def get_authorization(request_id: str, request: Request):
    if not _REQUEST_ID_RE.fullmatch(request_id):
        raise HTTPException(status_code=404, detail={"error": "authorization_not_found"})
    user = _require_user(request)
    record = get_publish_authorization_store().get(request_id, user["user_id"])
    if not record:
        raise HTTPException(status_code=404, detail={"error": "authorization_not_found"})
    return _formatted_public_record(record)


@router.post("/authorizations/{request_id}/complete")
async def complete_authorization(
    request_id: str,
    body: AuthorizationCompleteRequest,
    background_tasks: BackgroundTasks,
):
    if not _REQUEST_ID_RE.fullmatch(request_id):
        raise HTTPException(status_code=403, detail={"error": "authorization_invalid"})
    token = body.authorization_token.get_secret_value()
    sid = body.sid.get_secret_value().strip()
    if not sid or any(char.isspace() or ord(char) < 32 for char in sid):
        raise HTTPException(status_code=422, detail={"error": "sid_format_invalid"})

    store = get_publish_authorization_store()
    try:
        record = store.claim(request_id, token)
    except TimeoutError as exc:
        raise HTTPException(status_code=410, detail={"error": "authorization_expired"}) from exc
    except KeyError as exc:
        raise HTTPException(status_code=403, detail={"error": "authorization_invalid"}) from exc
    except RuntimeError as exc:
        state = str(exc)
        if state == "authorized":
            raise HTTPException(status_code=409, detail={"error": "authorization_already_used"}) from exc
        raise HTTPException(status_code=409, detail={"error": "authorization_not_pending"}) from exc

    try:
        tenant_store = get_tenant_store()
        previous_secret = tenant_store.get_secret(record["tenant_id"], "substack")
        previous_metadata = tenant_store.get_metadata(record["tenant_id"], "substack")
    except Exception as exc:
        store.release_claim(request_id)
        raise HTTPException(
            status_code=503,
            detail={"error": "credential_vault_unavailable"},
        ) from exc
    try:
        saved = tenant_store.set_secret(
            record["tenant_id"],
            "substack",
            sid,
            label=record["publication"],
        )
        if not saved:
            raise RuntimeError("credential vault rejected the connection")
        record = store.authorize(request_id)
    except Exception as exc:
        # The authorization DB and credential vault are distinct durable
        # stores. Compensate if the state transition fails after a vault write
        # so a pre-existing connection is never silently lost or replaced.
        try:
            if previous_secret is None:
                tenant_store.delete_secret(record["tenant_id"], "substack")
            else:
                tenant_store.set_secret(
                    record["tenant_id"],
                    "substack",
                    previous_secret,
                    label=getattr(previous_metadata, "label", None),
                )
        except Exception:
            # The original exception remains the useful client boundary. A
            # later request can replace/revoke the tenant credential safely.
            pass
        store.release_claim(request_id)
        raise HTTPException(
            status_code=503,
            detail={"error": "credential_vault_unavailable"},
        ) from exc

    if record.get("callback_url"):
        background_tasks.add_task(
            deliver_authorization_webhook,
            request_id,
            "substack.authorization.authorized",
            None,
        )
    return {
        "connected": True,
        "provider": "substack",
        "publication": record["publication"],
        "request_id": request_id,
        "status": "authorized",
        "credential": "stored_encrypted",
    }


@router.post("/authorizations/{request_id}/cancel")
async def cancel_authorization(
    request_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    user = _require_user(request)
    record = get_publish_authorization_store().cancel(request_id, user["user_id"])
    if not record:
        raise HTTPException(status_code=404, detail={"error": "pending_authorization_not_found"})
    if record.get("callback_url"):
        background_tasks.add_task(
            deliver_authorization_webhook,
            request_id,
            "substack.authorization.cancelled",
            None,
        )
    return {"request_id": request_id, "status": "cancelled"}


@router.get("/connection")
async def connection_status(request: Request):
    user = _require_user(request)
    tenant_store = get_tenant_store()
    connected = tenant_store.has_secret(user["user_id"], "substack")
    metadata = tenant_store.get_metadata(user["user_id"], "substack") if connected else None
    latest = get_publish_authorization_store().latest_for_tenant(user["user_id"])
    return {
        "provider": "substack",
        "connected": connected,
        "publication": getattr(metadata, "label", None) if metadata else None,
        "latest_authorization": _formatted_public_record(latest) if latest else None,
    }


@router.delete("/connection")
async def revoke_connection(request: Request):
    user = _require_user(request)
    tenant_store = get_tenant_store()
    removed = tenant_store.delete_secret(user["user_id"], "substack")
    cancelled = get_publish_authorization_store().cancel_pending_for_tenant(user["user_id"])
    if not removed and not cancelled:
        raise HTTPException(status_code=404, detail={"error": "substack_connection_not_found"})
    return {
        "provider": "substack",
        "disconnected": bool(removed),
        "pending_authorizations_cancelled": cancelled,
    }
