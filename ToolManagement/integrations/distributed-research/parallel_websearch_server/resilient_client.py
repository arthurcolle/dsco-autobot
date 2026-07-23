"""Resilient HTTP client with circuit breaker, tracing, and anomaly detection.

Wraps httpx.AsyncClient with advanced resilience features:
- Circuit breaker protection
- Automatic retries with exponential backoff and jitter
- Request deduplication
- Distributed tracing
- Anomaly detection
- Health scoring
- Webhook notifications on failures

Usage:
    client = ResilientClient(base_url="https://api.parallel.ai")

    # Simple request
    response = await client.get("/v1/tasks/runs/123")

    # With idempotency key
    response = await client.post(
        "/v1/tasks/runs",
        json={"input": "..."},
        idempotency_key="req_abc123",
    )

    # Check health
    health = client.get_health()
"""

import asyncio
import hashlib
import json
import logging
import random
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import httpx

from parallel_websearch_server.resilience import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    DistributedTracer,
    RequestDeduplicator,
    AnomalyDetector,
    DeadLetterQueue,
    HealthScorer,
    WebhookNotifier,
    get_circuit_breaker,
    get_distributed_tracer,
    get_anomaly_detector,
    get_health_scorer,
    get_dead_letter_queue,
    get_webhook_notifier,
    get_request_deduplicator,
)

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retry_on_status: tuple = (502, 503, 504, 429),
        retry_on_exception: tuple = (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.ConnectTimeout,
        ),
    ):
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retry_on_status = retry_on_status
        self.retry_on_exception = retry_on_exception

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and optional jitter."""
        delay = self.base_delay_seconds * (self.exponential_base ** attempt)
        delay = min(delay, self.max_delay_seconds)

        if self.jitter:
            # Add random jitter (±25%)
            jitter_range = delay * 0.25
            delay = delay + random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    def should_retry(
        self,
        attempt: int,
        status_code: Optional[int] = None,
        exception: Optional[Exception] = None,
    ) -> bool:
        """Determine if request should be retried."""
        if attempt >= self.max_retries:
            return False

        if status_code and status_code in self.retry_on_status:
            return True

        if exception and isinstance(exception, self.retry_on_exception):
            return True

        return False


class ResilientClient:
    """HTTP client with comprehensive resilience features.

    Integrates:
    - Circuit breaker for upstream protection
    - Retries with exponential backoff and jitter
    - Request deduplication via idempotency keys
    - Distributed tracing with context propagation
    - Anomaly detection for response patterns
    - Health scoring for degradation detection
    - Dead letter queue for failed operations
    - Webhook notifications for important events
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        enable_tracing: bool = True,
        enable_anomaly_detection: bool = True,
        enable_deduplication: bool = True,
        service_name: str = "upstream-api",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.service_name = service_name

        # HTTP client
        self._client: Optional[httpx.AsyncClient] = None

        # Resilience components
        self._circuit_breaker = get_circuit_breaker(
            service_name,
            circuit_breaker_config or CircuitBreakerConfig(
                failure_threshold=5,
                success_threshold=3,
                timeout_seconds=30.0,
                slow_call_duration_seconds=10.0,
            )
        )
        self._retry_config = retry_config or RetryConfig()
        self._deduplicator = get_request_deduplicator() if enable_deduplication else None
        self._tracer = get_distributed_tracer() if enable_tracing else None
        self._anomaly_detector = get_anomaly_detector() if enable_anomaly_detection else None
        self._health_scorer = get_health_scorer()
        self._dlq = get_dead_letter_queue()
        self._webhook_notifier = get_webhook_notifier()

        # Metrics
        self._request_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._retry_count = 0

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _generate_request_id(self) -> str:
        """Generate a unique request ID."""
        return f"req_{uuid.uuid4().hex[:16]}"

    def _compute_request_hash(
        self,
        method: str,
        url: str,
        data: Optional[Any] = None,
    ) -> str:
        """Compute a hash for deduplication."""
        content = f"{method}:{url}"
        if data:
            content += f":{json.dumps(data, sort_keys=True, default=str)}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    async def request(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict] = None,
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        idempotency_key: Optional[str] = None,
        trace_context: Optional[Any] = None,
        skip_circuit_breaker: bool = False,
        skip_retry: bool = False,
    ) -> httpx.Response:
        """Make a resilient HTTP request.

        Args:
            method: HTTP method
            url: Request URL (relative to base_url)
            json: JSON body
            data: Form data
            headers: Request headers
            params: Query parameters
            idempotency_key: Key for deduplication
            trace_context: Parent trace context
            skip_circuit_breaker: Bypass circuit breaker
            skip_retry: Bypass retry logic

        Returns:
            httpx.Response

        Raises:
            CircuitBreakerError: If circuit is open
            httpx.HTTPError: If request fails after retries
        """
        self._request_count += 1
        request_id = self._generate_request_id()
        full_url = f"{self.base_url}{url}"
        start_time = time.time()

        # Prepare headers
        request_headers = dict(headers or {})
        request_headers["X-Request-ID"] = request_id

        # Setup tracing
        span = None
        if self._tracer:
            parent = trace_context or self._tracer.extract_context(request_headers)
            span = self._tracer.span(parent, f"{method} {url}")
            trace_headers = self._tracer.inject_headers(span.__enter__())
            request_headers.update(trace_headers)

        # Check deduplication
        if idempotency_key and self._deduplicator:
            request_data = {"method": method, "url": url, "json": json, "data": data}
            existing = await self._deduplicator.check(idempotency_key, request_data)

            if existing and not existing.is_pending:
                # Return cached result
                if existing.status == "completed":
                    logger.debug(f"Deduplication hit for {idempotency_key}")
                    # We can't return the actual response, but we can indicate duplicate
                    raise DuplicateRequestError(idempotency_key, existing.result)
                elif existing.status == "failed":
                    raise DuplicateRequestError(idempotency_key, None, existing.error)

            elif existing and existing.is_pending:
                # Wait for in-flight request to complete
                logger.debug(f"Waiting for in-flight request {idempotency_key}")
                completed = await self._deduplicator.wait_for_completion(
                    idempotency_key, timeout_seconds=self.timeout
                )
                if completed:
                    if completed.status == "completed":
                        raise DuplicateRequestError(idempotency_key, completed.result)
                    elif completed.status == "failed":
                        raise DuplicateRequestError(idempotency_key, None, completed.error)

        # Execute with circuit breaker and retry
        last_exception: Optional[Exception] = None
        last_status_code: Optional[int] = None
        attempt = 0

        while True:
            try:
                # Check circuit breaker
                if not skip_circuit_breaker:
                    async with self._circuit_breaker:
                        response = await self._execute_request(
                            method, url, request_headers, json, data, params
                        )
                else:
                    response = await self._execute_request(
                        method, url, request_headers, json, data, params
                    )

                duration_ms = (time.time() - start_time) * 1000
                last_status_code = response.status_code

                # Record metrics
                self._health_scorer.record_success(self.service_name, duration_ms)

                if self._anomaly_detector:
                    anomalies = self._anomaly_detector.record_request(
                        endpoint=url,
                        duration_ms=duration_ms,
                        status_code=response.status_code,
                        request_size=len(json.dumps(json) if json else ""),
                        response_size=len(response.content),
                    )
                    for anomaly in anomalies:
                        logger.warning(f"Anomaly detected: {anomaly.anomaly_type} on {url}")

                # Check if we should retry on status code
                if (
                    not skip_retry
                    and self._retry_config.should_retry(attempt, status_code=response.status_code)
                ):
                    self._retry_count += 1
                    delay = self._retry_config.calculate_delay(attempt)
                    logger.warning(
                        f"Retrying request to {url} after {response.status_code} "
                        f"(attempt {attempt + 1}, delay {delay:.2f}s)"
                    )
                    await asyncio.sleep(delay)
                    attempt += 1
                    continue

                # Success
                self._success_count += 1

                if idempotency_key and self._deduplicator:
                    await self._deduplicator.complete(idempotency_key, {
                        "status_code": response.status_code,
                        "headers": dict(response.headers),
                    })

                if span:
                    span.__exit__(None, None, None)
                    span.add_tag("http.status_code", response.status_code)

                return response

            except CircuitBreakerError:
                self._failure_count += 1
                self._health_scorer.record_failure(
                    self.service_name, "circuit_open", time.time() - start_time
                )

                if idempotency_key and self._deduplicator:
                    await self._deduplicator.fail(idempotency_key, "circuit_open")

                if span:
                    span.__exit__(CircuitBreakerError, None, None)

                # Notify via webhook
                await self._webhook_notifier.notify("circuit.rejected", {
                    "service": self.service_name,
                    "url": url,
                    "request_id": request_id,
                })

                raise

            except Exception as e:
                last_exception = e
                duration_ms = (time.time() - start_time) * 1000

                # Check if we should retry
                if (
                    not skip_retry
                    and self._retry_config.should_retry(attempt, exception=e)
                ):
                    self._retry_count += 1
                    delay = self._retry_config.calculate_delay(attempt)
                    logger.warning(
                        f"Retrying request to {url} after error: {e} "
                        f"(attempt {attempt + 1}, delay {delay:.2f}s)"
                    )
                    await asyncio.sleep(delay)
                    attempt += 1
                    continue

                # Final failure
                self._failure_count += 1
                self._health_scorer.record_failure(self.service_name, str(e), duration_ms)

                if idempotency_key and self._deduplicator:
                    await self._deduplicator.fail(idempotency_key, str(e))

                if span:
                    span.__exit__(type(e), e, None)

                # Add to dead letter queue if this was a write operation
                if method.upper() in ("POST", "PUT", "PATCH", "DELETE"):
                    self._dlq.add(
                        operation=f"http_{method.lower()}",
                        payload={
                            "url": full_url,
                            "method": method,
                            "json": json,
                            "headers": {k: v for k, v in request_headers.items()
                                       if k.lower() not in ("authorization", "x-api-key")},
                        },
                        error_history=[{
                            "attempt": i + 1,
                            "error": str(e) if i == attempt else "retry",
                            "timestamp": time.time(),
                        } for i in range(attempt + 1)],
                        metadata={
                            "request_id": request_id,
                            "service": self.service_name,
                        }
                    )

                # Notify via webhook
                await self._webhook_notifier.notify("request.failed", {
                    "service": self.service_name,
                    "url": url,
                    "method": method,
                    "error": str(e),
                    "request_id": request_id,
                    "attempts": attempt + 1,
                })

                raise

    async def _execute_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        json_data: Optional[Dict],
        data: Optional[Any],
        params: Optional[Dict[str, str]],
    ) -> httpx.Response:
        """Execute the actual HTTP request."""
        client = await self._get_client()

        return await client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_data,
            data=data,
            params=params,
        )

    # Convenience methods

    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Make a GET request."""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> httpx.Response:
        """Make a POST request."""
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs) -> httpx.Response:
        """Make a PUT request."""
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs) -> httpx.Response:
        """Make a PATCH request."""
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs) -> httpx.Response:
        """Make a DELETE request."""
        return await self.request("DELETE", url, **kwargs)

    # Health and metrics

    def get_health(self) -> Dict[str, Any]:
        """Get client health status."""
        return {
            "service": self.service_name,
            "base_url": self.base_url,
            "health": self._health_scorer.get_health(self.service_name),
            "circuit_breaker": self._circuit_breaker.get_status(),
            "metrics": {
                "total_requests": self._request_count,
                "successful_requests": self._success_count,
                "failed_requests": self._failure_count,
                "retries": self._retry_count,
                "success_rate": round(
                    self._success_count / max(self._request_count, 1) * 100, 1
                ),
            },
        }

    def get_anomalies(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent anomalies."""
        if self._anomaly_detector:
            return self._anomaly_detector.get_recent_anomalies(limit)
        return []

    def reset_circuit_breaker(self) -> bool:
        """Manually reset the circuit breaker."""
        return CircuitBreaker.reset(self.service_name)


class DuplicateRequestError(Exception):
    """Raised when a duplicate request is detected."""

    def __init__(
        self,
        idempotency_key: str,
        cached_result: Optional[Any] = None,
        error: Optional[str] = None,
    ):
        self.idempotency_key = idempotency_key
        self.cached_result = cached_result
        self.error = error
        super().__init__(
            f"Duplicate request with idempotency key: {idempotency_key}"
        )


# =============================================================================
# Factory Functions
# =============================================================================

_resilient_clients: Dict[str, ResilientClient] = {}


def get_resilient_client(
    base_url: str,
    service_name: Optional[str] = None,
    **kwargs,
) -> ResilientClient:
    """Get or create a resilient client for a base URL.

    Args:
        base_url: Base URL for the client
        service_name: Name for circuit breaker and health tracking
        **kwargs: Additional arguments for ResilientClient

    Returns:
        ResilientClient instance (reused for same base_url)
    """
    key = base_url.rstrip("/")

    if key not in _resilient_clients:
        name = service_name or key.split("//")[-1].split("/")[0]
        _resilient_clients[key] = ResilientClient(
            base_url=base_url,
            service_name=name,
            **kwargs,
        )

    return _resilient_clients[key]


async def close_all_clients() -> None:
    """Close all resilient clients."""
    for client in _resilient_clients.values():
        await client.close()
    _resilient_clients.clear()
