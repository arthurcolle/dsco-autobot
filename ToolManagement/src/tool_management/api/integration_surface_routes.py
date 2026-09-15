"""Declarative presentation surfaces for every Tool Management integration.

The surface API is intentionally a projection layer, not another executor.
Realtime updates and webhook delivery remain owned by the tenant-bound Jobs
API; this module only describes that handoff and projects an existing job into
a small, stable widget document.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..core.streaming import EventType, StreamingExecution, validate_callback_url
from .job_routes import _job_execution
from .tools_api import resolve_execution_identity


router = APIRouter(
    prefix="/api/v1/integration-surfaces",
    tags=["integration-surfaces"],
)

SURFACE_SCHEMA = "distributed.integration-surface/v1"
WIDGET_SCHEMA = "distributed.realtime-widget/v1"
SUPPORTED_CALLBACK_EVENTS = ("progress", "complete", "error", "cancelled")
_INTEGRATION_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}$")
_SENSITIVE_KEY_RE = re.compile(
    r"(?:authorization|cookie|password|passwd|secret|token|api[_-]?key|private[_-]?key)",
    re.IGNORECASE,
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{6,}")
_INLINE_SECRET_RE = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|authorization)"
    r"\s*[:=]\s*['\"]?[^,\s}\"']+"
)


class CallbackEvent(str, Enum):
    PROGRESS = "progress"
    COMPLETE = "complete"
    ERROR = "error"
    CANCELLED = "cancelled"
    ALL = "all"


class WidgetPreferences(BaseModel):
    """Bounded presentation hints; clients still own their visual skin."""

    model_config = ConfigDict(extra="forbid")

    title: Optional[str] = Field(default=None, min_length=1, max_length=120)
    density: str = Field(default="comfortable", pattern=r"^(compact|comfortable)$")
    poll_interval_ms: int = Field(default=3000, ge=1000, le=60000)
    max_partial_items: int = Field(default=4, ge=0, le=12)


class WebhookHandoff(BaseModel):
    """Per-job callback fields passed unchanged to the existing Jobs API."""

    model_config = ConfigDict(extra="forbid")

    callback_url: str = Field(min_length=12, max_length=2048)
    events: List[CallbackEvent] = Field(
        default_factory=lambda: [CallbackEvent.COMPLETE, CallbackEvent.ERROR],
        min_length=1,
        max_length=5,
    )

    @field_validator("events")
    @classmethod
    def _events_are_unique(cls, events: List[CallbackEvent]) -> List[CallbackEvent]:
        values = [event.value for event in events]
        if len(values) != len(set(values)):
            raise ValueError("webhook events must be unique")
        if CallbackEvent.ALL in events and len(events) != 1:
            raise ValueError("'all' cannot be combined with specific webhook events")
        return events


class IntegrationSurfaceRequest(BaseModel):
    """Describe how one integration should appear without registering state."""

    model_config = ConfigDict(extra="forbid")

    integration_ref: str = Field(
        min_length=1,
        max_length=160,
        description="Tool name or backend:tool reference used by /jobs/submit",
    )
    display_name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    description: Optional[str] = Field(default=None, max_length=500)
    widget: WidgetPreferences = Field(default_factory=WidgetPreferences)
    webhook: Optional[WebhookHandoff] = None

    @field_validator("integration_ref")
    @classmethod
    def _validate_integration_ref(cls, value: str) -> str:
        normalized = value.strip()
        if not _INTEGRATION_REF_RE.fullmatch(normalized):
            raise ValueError("integration_ref contains unsupported characters")
        return normalized

    @field_validator("display_name", "description")
    @classmethod
    def _strip_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


def _display_name(integration_ref: str) -> str:
    leaf = integration_ref.rsplit(":", 1)[-1]
    words = leaf.replace("_", " ").replace("-", " ").split()
    return " ".join(word[:1].upper() + word[1:] for word in words) or "Integration"


def _surface_id(request: IntegrationSurfaceRequest) -> str:
    """Keep identity stable when a customer changes a per-job callback URL."""
    identity = {
        "integration_ref": request.integration_ref,
        "display_name": request.display_name,
        "widget": request.widget.model_dump(mode="json"),
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:20]
    return f"surface_{digest}"


def _callback_events(webhook: Optional[WebhookHandoff]) -> List[str]:
    if webhook is None:
        return []
    if webhook.events == [CallbackEvent.ALL]:
        return list(SUPPORTED_CALLBACK_EVENTS)
    return [event.value for event in webhook.events]


def build_surface_manifest(request: IntegrationSurfaceRequest) -> Dict[str, Any]:
    """Build the transport-neutral contract consumed by web and native clients."""
    title = (
        request.widget.title
        or request.display_name
        or _display_name(request.integration_ref)
    )
    callback_events = _callback_events(request.webhook)
    submit_body: Dict[str, Any] = {
        "tool": request.integration_ref,
        "arguments": {},
    }
    if request.webhook is not None:
        submit_body.update(
            {
                "callback_url": request.webhook.callback_url,
                "callback_events": callback_events,
            }
        )

    return {
        "schema": SURFACE_SCHEMA,
        "surface_id": _surface_id(request),
        "integration": {
            "ref": request.integration_ref,
            "display_name": request.display_name or _display_name(request.integration_ref),
            "description": request.description,
        },
        "widget": {
            "schema": WIDGET_SCHEMA,
            "title": title,
            "density": request.widget.density,
            "projection": {
                "method": "GET",
                "path_template": "/api/v1/integration-surfaces/jobs/{job_id}/widget",
                "max_partial_items": request.widget.max_partial_items,
            },
            "refresh": {
                "strategy": "sse_with_poll_fallback",
                "poll_interval_ms": request.widget.poll_interval_ms,
            },
        },
        "realtime": {
            "owner": "jobs",
            "transport": "server_sent_events",
            "stream_path_template": "/api/v1/jobs/{job_id}/stream",
            "resume": {
                "cursor_query": "since_event_id",
                "cursor_field": "latest_event_id",
                "history_query": "include_history",
            },
        },
        "webhook": {
            "owner": "jobs",
            "registration": "per_job_submission",
            "configured": request.webhook is not None,
            "supported_events": list(SUPPORTED_CALLBACK_EVENTS),
            "callback_events": callback_events,
            "delivery_created": False,
            "note": "Delivery begins only when this handoff is submitted through /api/v1/jobs/submit.",
        },
        "execution_handoff": {
            "method": "POST",
            "path": "/api/v1/jobs/submit",
            "body": submit_body,
        },
    }


def _safe_text(value: Any, max_length: int = 500) -> str:
    text = _BEARER_RE.sub("Bearer [redacted]", str(value or ""))
    text = _INLINE_SECRET_RE.sub(lambda match: f"{match.group(1)}=[redacted]", text)
    if len(text) <= max_length:
        return text
    return f"{text[: max(0, max_length - 1)]}\u2026"


def _bounded_preview(
    value: Any,
    *,
    depth: int = 0,
    max_depth: int = 3,
    max_items: int = 8,
) -> Any:
    """Return a small secret-aware JSON projection of arbitrary tool output."""
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return _safe_text(value)
    if depth >= max_depth:
        return "[truncated]"
    if isinstance(value, dict):
        projected: Dict[str, Any] = {}
        for index, (key, child) in enumerate(value.items()):
            if index >= max_items:
                projected["_truncated"] = True
                break
            key_text = _safe_text(key, 80)
            projected[key_text] = (
                "[redacted]"
                if _SENSITIVE_KEY_RE.search(key_text)
                else _bounded_preview(
                    child,
                    depth=depth + 1,
                    max_depth=max_depth,
                    max_items=max_items,
                )
            )
        return projected
    if isinstance(value, (list, tuple)):
        projected = [
            _bounded_preview(
                child,
                depth=depth + 1,
                max_depth=max_depth,
                max_items=max_items,
            )
            for child in value[:max_items]
        ]
        if len(value) > max_items:
            projected.append("[truncated]")
        return projected
    return _safe_text(value)


def _latest_event(execution: StreamingExecution, event_type: EventType) -> Any:
    for event in reversed(execution.events[-250:]):
        if event.type == event_type:
            return event
    return None


def project_execution_widget(
    execution: StreamingExecution,
    *,
    max_partial_items: int = 4,
) -> Dict[str, Any]:
    """Project a job into the stable, bounded widget schema."""
    max_partial_items = max(0, min(int(max_partial_items), 12))
    recent_events = execution.events[-250:]
    partials = [event for event in recent_events if event.type == EventType.PARTIAL]
    if max_partial_items:
        partials = partials[-max_partial_items:]
    else:
        partials = []

    latest_progress = _latest_event(execution, EventType.PROGRESS)
    completed = _latest_event(execution, EventType.COMPLETED)
    failed = _latest_event(execution, EventType.ERROR)
    latest_event = recent_events[-1] if recent_events else None
    status = str(execution.status or "pending").lower()
    state = status if status in {"queued", "running", "completed", "error", "cancelled"} else "pending"
    progress = max(0.0, min(100.0, float(execution.progress or 0.0)))
    latest_event_id = max(
        (int(event.event_id) for event in recent_events if event.event_id is not None),
        default=None,
    )
    stage = None
    message = None
    if latest_progress is not None:
        if isinstance(latest_progress.data, dict):
            stage_value = latest_progress.data.get("stage")
            if stage_value is not None:
                stage = _safe_text(stage_value, 120)
        if latest_progress.message:
            message = _safe_text(latest_progress.message, 240)
            stage = stage or message
    if state == "completed":
        message = "Completed"
    elif state == "error":
        message = "Needs attention"
    elif state == "cancelled":
        message = "Cancelled"
    elif state == "queued" and not message:
        message = "Queued"

    result = completed.data if completed is not None else None
    error = failed.error if failed is not None else None
    if error is None and state == "error":
        error = execution.control_state.get("error")
    callback_events = execution.control_state.get("callback_events", [])
    if not isinstance(callback_events, list):
        callback_events = []

    actions: List[Dict[str, str]] = [
        {
            "id": "refresh",
            "method": "GET",
            "href": (
                "/api/v1/integration-surfaces/jobs/"
                f"{execution.execution_id}/widget"
            ),
        },
        {
            "id": "stream",
            "method": "GET",
            "href": f"/api/v1/jobs/{execution.execution_id}/stream",
        },
    ]
    if state in {"pending", "queued", "running"}:
        actions.append(
            {
                "id": "cancel",
                "method": "POST",
                "href": f"/api/v1/jobs/{execution.execution_id}/cancel",
            }
        )

    last_webhook_event = execution.control_state.get("last_webhook_event")
    if last_webhook_event not in {*SUPPORTED_CALLBACK_EVENTS, "all", None}:
        last_webhook_event = None
    last_webhook_success = execution.control_state.get("last_webhook_success")
    if not isinstance(last_webhook_success, bool):
        last_webhook_success = None
    try:
        webhook_attempts = int(
            execution.control_state.get("webhook_attempts", 0) or 0
        )
    except (TypeError, ValueError, OverflowError):
        webhook_attempts = 0

    return {
        "schema": WIDGET_SCHEMA,
        "widget_id": f"job:{execution.execution_id}",
        "job_id": execution.execution_id,
        "integration_ref": execution.tool_name,
        "state": state,
        "progress": progress,
        "stage": stage,
        "message": message,
        "partial_items": [_bounded_preview(event.data) for event in partials],
        "result": _bounded_preview(result),
        "error": _safe_text(error, 500) if error else None,
        "latest_event_id": latest_event_id,
        "updated_at": latest_event.timestamp if latest_event is not None else (
            execution.completed_at or execution.started_at or execution.queued_at
        ),
        "realtime": {
            "transport": "server_sent_events",
            "stream_url": f"/api/v1/jobs/{execution.execution_id}/stream",
            "resume_cursor": latest_event_id,
        },
        "webhook": {
            "configured": bool(execution.control_state.get("callback_url")),
            "events": [
                event
                for event in callback_events[:5]
                if event in {*SUPPORTED_CALLBACK_EVENTS, "all"}
            ],
            "last_event": last_webhook_event,
            "last_success": last_webhook_success,
            "attempts": min(max(webhook_attempts, 0), 1_000_000),
        },
        "actions": actions,
    }


@router.post("/manifests")
async def create_surface_manifest(
    surface_request: IntegrationSurfaceRequest,
    request: Request,
) -> Dict[str, Any]:
    """Create a pure manifest; no job, subscription, or delivery is started."""
    resolve_execution_identity(request)
    if surface_request.webhook is not None:
        try:
            await asyncio.to_thread(
                validate_callback_url,
                surface_request.webhook.callback_url,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={"error": "unsafe_callback_url"},
            ) from exc
    return build_surface_manifest(surface_request)


@router.get("/jobs/{job_id}/widget")
async def get_job_widget(
    job_id: str,
    request: Request,
    response: Response,
    max_partial_items: int = Query(default=4, ge=0, le=12),
) -> Dict[str, Any]:
    """Return the caller-owned job as a bounded realtime widget document."""
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Vary"] = "Authorization"
    try:
        from ..core.streaming import get_streaming_executor

        executor = get_streaming_executor()
    except ValueError as exc:
        raise HTTPException(503, "Streaming executor not initialized") from exc

    execution = _job_execution(executor, job_id, resolve_execution_identity(request))
    if execution is None:
        raise HTTPException(404, f"Job not found: {job_id}")
    return project_execution_widget(execution, max_partial_items=max_partial_items)
