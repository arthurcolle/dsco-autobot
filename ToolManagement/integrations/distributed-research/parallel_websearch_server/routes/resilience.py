"""API routes for resilience monitoring and management.

Provides endpoints to:
- View circuit breaker status
- Check system health
- View anomaly detection results
- Manage dead letter queue
- View distributed traces
- Monitor webhook deliveries
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from parallel_websearch_server.resilience import (
    CircuitBreaker,
    get_circuit_breaker,
    get_anomaly_detector,
    get_dead_letter_queue,
    get_distributed_tracer,
    get_health_scorer,
    get_webhook_notifier,
    get_request_deduplicator,
    WebhookConfig,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1/resilience", tags=["Resilience"])


# =============================================================================
# Response Models
# =============================================================================

class CircuitBreakerStatus(BaseModel):
    """Circuit breaker status response."""
    name: str
    state: str
    metrics: Dict[str, Any]
    config: Dict[str, Any]
    consecutive_failures: int
    consecutive_successes: int


class HealthStatus(BaseModel):
    """System health status response."""
    status: str
    overall_score: float
    lowest_score: float
    services: Dict[str, Any]
    circuit_breakers: Dict[str, Any]


class AnomalyResponse(BaseModel):
    """Anomaly detection response."""
    anomaly_id: str
    type: str
    metric: str
    value: float
    expected_range: List[float]
    severity: str
    detected_at: str
    context: Dict[str, Any]


class DeadLetterEntry(BaseModel):
    """Dead letter queue entry."""
    entry_id: str
    operation: str
    failure_count: int
    status: str
    first_failure_at: str
    last_failure_at: str
    last_error: Optional[Dict[str, Any]]
    metadata: Dict[str, Any]


class TraceSpan(BaseModel):
    """Distributed trace span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    service_name: str
    start_time: str
    end_time: Optional[str]
    duration_ms: float
    tags: Dict[str, Any]
    logs: List[Dict[str, Any]]


class WebhookRegistration(BaseModel):
    """Webhook registration request."""
    url: str
    events: List[str] = Field(default=["*"])
    secret: Optional[str] = None
    headers: Dict[str, str] = Field(default_factory=dict)


# =============================================================================
# Circuit Breaker Endpoints
# =============================================================================

@router.get("/circuit-breakers", response_model=Dict[str, CircuitBreakerStatus])
async def list_circuit_breakers() -> Dict[str, CircuitBreakerStatus]:
    """List all circuit breakers and their status."""
    return CircuitBreaker.get_all_statuses()


@router.get("/circuit-breakers/{name}", response_model=CircuitBreakerStatus)
async def get_circuit_breaker_status(name: str) -> CircuitBreakerStatus:
    """Get status of a specific circuit breaker."""
    try:
        cb = get_circuit_breaker(name)
        return cb.get_status()
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Circuit breaker '{name}' not found")


@router.post("/circuit-breakers/{name}/reset")
async def reset_circuit_breaker(name: str) -> Dict[str, Any]:
    """Manually reset a circuit breaker to closed state."""
    if CircuitBreaker.reset(name):
        return {"success": True, "message": f"Circuit breaker '{name}' reset to CLOSED"}
    raise HTTPException(status_code=404, detail=f"Circuit breaker '{name}' not found")


# =============================================================================
# Health Endpoints
# =============================================================================

@router.get("/health", response_model=HealthStatus)
async def get_system_health() -> HealthStatus:
    """Get overall system health status."""
    scorer = get_health_scorer()
    return scorer.get_system_health()


@router.get("/health/{service}")
async def get_service_health(service: str) -> Dict[str, Any]:
    """Get health status for a specific service."""
    scorer = get_health_scorer()
    health = scorer.get_health(service)
    if not health:
        raise HTTPException(status_code=404, detail=f"Service '{service}' not found")
    return health


# =============================================================================
# Anomaly Detection Endpoints
# =============================================================================

@router.get("/anomalies", response_model=List[AnomalyResponse])
async def list_anomalies(
    limit: int = Query(default=50, le=100),
) -> List[AnomalyResponse]:
    """List recently detected anomalies."""
    detector = get_anomaly_detector()
    return detector.get_recent_anomalies(limit)


@router.get("/anomalies/stats")
async def get_anomaly_stats() -> Dict[str, Any]:
    """Get anomaly detection statistics."""
    detector = get_anomaly_detector()
    return detector.get_all_stats()


@router.get("/anomalies/endpoint/{endpoint:path}")
async def get_endpoint_stats(endpoint: str) -> Dict[str, Any]:
    """Get statistics for a specific endpoint."""
    detector = get_anomaly_detector()
    stats = detector.get_endpoint_stats(f"/{endpoint}")
    if not stats:
        raise HTTPException(status_code=404, detail=f"Endpoint '/{endpoint}' not found")
    return stats


# =============================================================================
# Dead Letter Queue Endpoints
# =============================================================================

@router.get("/dlq", response_model=List[DeadLetterEntry])
async def list_dead_letters(
    status: Optional[str] = Query(default=None),
    operation: Optional[str] = Query(default=None),
    limit: int = Query(default=50, le=100),
) -> List[DeadLetterEntry]:
    """List dead letter queue entries."""
    dlq = get_dead_letter_queue()
    return dlq.list_entries(status=status, operation=operation, limit=limit)


@router.get("/dlq/stats")
async def get_dlq_stats() -> Dict[str, Any]:
    """Get dead letter queue statistics."""
    dlq = get_dead_letter_queue()
    return dlq.get_stats()


@router.get("/dlq/{entry_id}")
async def get_dead_letter(entry_id: str) -> Dict[str, Any]:
    """Get full details of a dead letter entry."""
    dlq = get_dead_letter_queue()
    entry = dlq.get_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"DLQ entry '{entry_id}' not found")
    return entry


@router.post("/dlq/{entry_id}/retry")
async def retry_dead_letter(entry_id: str) -> Dict[str, Any]:
    """Retry a dead letter entry."""
    dlq = get_dead_letter_queue()
    success = await dlq.retry_entry(entry_id)
    return {
        "success": success,
        "entry_id": entry_id,
        "message": "Retry successful" if success else "Retry failed",
    }


@router.post("/dlq/{entry_id}/discard")
async def discard_dead_letter(
    entry_id: str,
    reason: str = Query(..., description="Reason for discarding"),
) -> Dict[str, Any]:
    """Discard a dead letter entry."""
    dlq = get_dead_letter_queue()
    if dlq.discard_entry(entry_id, reason):
        return {"success": True, "entry_id": entry_id, "reason": reason}
    raise HTTPException(status_code=404, detail=f"DLQ entry '{entry_id}' not found")


# =============================================================================
# Distributed Tracing Endpoints
# =============================================================================

@router.get("/traces/recent", response_model=List[TraceSpan])
async def list_recent_traces(
    limit: int = Query(default=50, le=200),
) -> List[TraceSpan]:
    """List recent trace spans."""
    tracer = get_distributed_tracer()
    return tracer.get_recent_spans(limit)


@router.get("/traces/{trace_id}", response_model=List[TraceSpan])
async def get_trace(trace_id: str) -> List[TraceSpan]:
    """Get all spans for a specific trace."""
    tracer = get_distributed_tracer()
    spans = tracer.get_trace(trace_id)
    if not spans:
        raise HTTPException(status_code=404, detail=f"Trace '{trace_id}' not found")
    return spans


# =============================================================================
# Webhook Endpoints
# =============================================================================

@router.post("/webhooks")
async def register_webhook(config: WebhookRegistration) -> Dict[str, Any]:
    """Register a new webhook endpoint."""
    notifier = get_webhook_notifier()
    webhook_id = notifier.register_webhook(WebhookConfig(
        url=config.url,
        events=config.events,
        secret=config.secret,
        headers=config.headers,
    ))
    return {
        "webhook_id": webhook_id,
        "url": config.url,
        "events": config.events,
    }


@router.delete("/webhooks/{webhook_id}")
async def unregister_webhook(webhook_id: str) -> Dict[str, Any]:
    """Unregister a webhook endpoint."""
    notifier = get_webhook_notifier()
    if notifier.unregister_webhook(webhook_id):
        return {"success": True, "webhook_id": webhook_id}
    raise HTTPException(status_code=404, detail=f"Webhook '{webhook_id}' not found")


@router.get("/webhooks/stats")
async def get_webhook_stats() -> Dict[str, Any]:
    """Get webhook delivery statistics."""
    notifier = get_webhook_notifier()
    return notifier.get_stats()


@router.get("/webhooks/history")
async def get_webhook_history(
    limit: int = Query(default=50, le=200),
) -> List[Dict[str, Any]]:
    """Get webhook delivery history."""
    notifier = get_webhook_notifier()
    return notifier.get_delivery_history(limit)


# =============================================================================
# Deduplication Endpoints
# =============================================================================

@router.get("/deduplication/stats")
async def get_deduplication_stats() -> Dict[str, Any]:
    """Get request deduplication statistics."""
    dedup = get_request_deduplicator()
    return dedup.get_stats()


# =============================================================================
# Combined Status Endpoint
# =============================================================================

@router.get("/status")
async def get_resilience_status() -> Dict[str, Any]:
    """Get complete resilience system status."""
    return {
        "health": get_health_scorer().get_system_health(),
        "circuit_breakers": CircuitBreaker.get_all_statuses(),
        "anomaly_detection": get_anomaly_detector().get_all_stats(),
        "dead_letter_queue": get_dead_letter_queue().get_stats(),
        "deduplication": get_request_deduplicator().get_stats(),
        "webhooks": get_webhook_notifier().get_stats(),
    }
