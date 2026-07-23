"""Webhook event models."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class WebhookEventType(str, Enum):
    """Webhook event types."""

    # FindAll events
    FINDALL_CANDIDATE_GENERATED = "findall.candidate.generated"
    FINDALL_CANDIDATE_MATCHED = "findall.candidate.matched"
    FINDALL_CANDIDATE_UNMATCHED = "findall.candidate.unmatched"
    FINDALL_CANDIDATE_ENRICHED = "findall.candidate.enriched"
    FINDALL_RUN_COMPLETED = "findall.run.completed"
    FINDALL_RUN_CANCELLED = "findall.run.cancelled"
    FINDALL_RUN_FAILED = "findall.run.failed"

    # Task events
    TASK_RUN_COMPLETED = "task.run.completed"
    TASK_RUN_FAILED = "task.run.failed"
    TASK_RUN_CANCELLED = "task.run.cancelled"


class FindAllCandidate(BaseModel):
    """FindAll candidate data."""

    candidate_id: str
    name: str
    url: Optional[str] = None
    description: Optional[str] = None
    match_status: str  # generated, matched, unmatched, enriched
    output: Optional[Dict[str, Any]] = None
    basis: Optional[List[Dict[str, Any]]] = None  # Citations and reasoning


class FindAllRunStatus(BaseModel):
    """FindAll run status."""

    status: str  # running, completed, cancelled, failed
    is_active: bool
    metrics: Dict[str, int] = Field(default_factory=dict)
    termination_reason: Optional[str] = None


class FindAllRun(BaseModel):
    """FindAll run data."""

    findall_id: str
    status: FindAllRunStatus
    processor: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    modified_at: datetime


class TaskRun(BaseModel):
    """Task run data."""

    run_id: str
    status: str  # completed, failed, cancelled
    processor: str
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class FindAllCandidateEvent(BaseModel):
    """FindAll candidate webhook event."""

    type: WebhookEventType
    timestamp: datetime
    data: FindAllCandidate


class FindAllRunEvent(BaseModel):
    """FindAll run webhook event."""

    type: WebhookEventType
    timestamp: datetime
    data: FindAllRun


class TaskRunEvent(BaseModel):
    """Task run webhook event."""

    type: WebhookEventType
    timestamp: datetime
    data: TaskRun


# Union of all event types
WebhookEvent = Union[FindAllCandidateEvent, FindAllRunEvent, TaskRunEvent]


class WebhookDelivery(BaseModel):
    """Webhook delivery attempt record."""

    event_id: str
    event_type: WebhookEventType
    target_url: str
    attempt_number: int
    status_code: Optional[int] = None
    success: bool
    error_message: Optional[str] = None
    delivered_at: datetime
    response_time_ms: Optional[float] = None


class WebhookSubscription(BaseModel):
    """Webhook subscription configuration."""

    subscription_id: str
    url: str
    event_types: List[WebhookEventType]
    secret: Optional[str] = None  # HMAC secret
    active: bool = True
    created_at: datetime
    retry_config: Optional[Dict[str, Any]] = None


class WebhookConfig(BaseModel):
    """Webhook configuration for API requests."""

    url: str = Field(..., description="Webhook endpoint URL")
    event_types: List[WebhookEventType] = Field(
        ...,
        description="Event types to subscribe to"
    )
    secret: Optional[str] = Field(
        None,
        description="Secret for HMAC signature verification (recommended)"
    )
