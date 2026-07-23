"""Advanced resilience mechanisms for robust request handling.

This module provides:
- Circuit Breaker: Prevent cascading failures when upstream is unhealthy
- Request Deduplication: Prevent duplicate submissions with idempotency keys
- Distributed Tracing: Full trace context propagation across services
- Anomaly Detection: Detect unusual patterns in request/response behavior
- Dead Letter Queue: Handle permanently failed operations
- Request Replay: Replay failed requests from audit log
- Health Scoring: Track upstream health over time with degradation detection
- Webhook Notifications: Send notifications on important events

Usage:
    from parallel_websearch_server.resilience import (
        CircuitBreaker, RequestDeduplicator, DistributedTracer,
        AnomalyDetector, DeadLetterQueue, HealthScorer
    )
"""

import asyncio
import hashlib
import json
import logging
import random
import statistics
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple, TypeVar, Union
import threading

logger = logging.getLogger(__name__)

T = TypeVar('T')


# =============================================================================
# Circuit Breaker
# =============================================================================

class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation, requests pass through
    OPEN = "open"          # Failing, requests are rejected
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 3          # Successes in half-open before closing
    timeout_seconds: float = 30.0       # Time before trying half-open
    half_open_max_calls: int = 3        # Max concurrent calls in half-open
    excluded_exceptions: Tuple = ()     # Exceptions that don't count as failures

    # Advanced settings
    failure_rate_threshold: float = 0.5  # Open if failure rate exceeds this
    minimum_calls: int = 10              # Minimum calls before rate calculation
    slow_call_duration_seconds: float = 5.0  # Calls slower than this are "slow"
    slow_call_rate_threshold: float = 0.8    # Open if slow rate exceeds this


@dataclass
class CircuitBreakerMetrics:
    """Metrics tracked by the circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    slow_calls: int = 0

    # Timing
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    last_state_change_time: Optional[float] = None

    # Rolling window for rate calculation
    recent_results: Deque = field(default_factory=lambda: deque(maxlen=100))
    recent_durations: Deque = field(default_factory=lambda: deque(maxlen=100))

    def record_success(self, duration: float) -> None:
        self.total_calls += 1
        self.successful_calls += 1
        self.last_success_time = time.time()
        self.recent_results.append(True)
        self.recent_durations.append(duration)

    def record_failure(self, duration: float) -> None:
        self.total_calls += 1
        self.failed_calls += 1
        self.last_failure_time = time.time()
        self.recent_results.append(False)
        self.recent_durations.append(duration)

    def record_slow(self, duration: float) -> None:
        self.slow_calls += 1
        self.recent_durations.append(duration)

    def record_rejected(self) -> None:
        self.rejected_calls += 1

    @property
    def failure_rate(self) -> float:
        if not self.recent_results:
            return 0.0
        failures = sum(1 for r in self.recent_results if not r)
        return failures / len(self.recent_results)

    @property
    def slow_call_rate(self) -> float:
        if not self.recent_durations:
            return 0.0
        # This needs threshold from config, use 5.0 as default
        slow = sum(1 for d in self.recent_durations if d > 5.0)
        return slow / len(self.recent_durations)

    @property
    def avg_duration(self) -> float:
        if not self.recent_durations:
            return 0.0
        return statistics.mean(self.recent_durations)

    @property
    def p99_duration(self) -> float:
        if len(self.recent_durations) < 10:
            return self.avg_duration
        sorted_durations = sorted(self.recent_durations)
        idx = int(len(sorted_durations) * 0.99)
        return sorted_durations[min(idx, len(sorted_durations) - 1)]


class CircuitBreakerError(Exception):
    """Raised when circuit is open and request is rejected."""
    def __init__(self, circuit_name: str, state: CircuitState, retry_after: float):
        self.circuit_name = circuit_name
        self.state = state
        self.retry_after = retry_after
        super().__init__(
            f"Circuit '{circuit_name}' is {state.value}. Retry after {retry_after:.1f}s"
        )


class CircuitBreaker:
    """Circuit breaker implementation with advanced features.

    Prevents cascading failures by stopping requests to failing services.
    Automatically recovers when the service becomes healthy again.

    Example:
        breaker = CircuitBreaker("upstream-api")

        async def call_upstream():
            async with breaker:
                return await http_client.get("/api/endpoint")

        try:
            result = await call_upstream()
        except CircuitBreakerError as e:
            # Circuit is open, use fallback
            result = get_cached_result()
    """

    _instances: Dict[str, 'CircuitBreaker'] = {}
    _lock = threading.Lock()

    def __new__(cls, name: str, config: Optional[CircuitBreakerConfig] = None):
        """Ensure single instance per name (singleton per circuit)."""
        with cls._lock:
            if name not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[name] = instance
            return cls._instances[name]

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self._lock = asyncio.Lock()
        self._half_open_calls = 0
        self._consecutive_failures = 0
        self._consecutive_successes = 0

        logger.info(f"Circuit breaker '{name}' initialized in CLOSED state")

    async def __aenter__(self):
        """Context manager entry - check if call is allowed."""
        await self._before_call()
        self._call_start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - record result."""
        duration = time.time() - self._call_start_time

        if exc_type is None:
            await self._on_success(duration)
        elif self._is_failure_exception(exc_type):
            await self._on_failure(duration)

        return False  # Don't suppress exceptions

    def _is_failure_exception(self, exc_type) -> bool:
        """Check if exception should count as a failure."""
        if self.config.excluded_exceptions:
            return not issubclass(exc_type, self.config.excluded_exceptions)
        return True

    async def _before_call(self) -> None:
        """Check if call is allowed based on circuit state."""
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return

            if self.state == CircuitState.OPEN:
                # Check if we should transition to half-open
                time_since_failure = time.time() - (self.metrics.last_failure_time or 0)
                if time_since_failure >= self.config.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
                else:
                    self.metrics.record_rejected()
                    retry_after = self.config.timeout_seconds - time_since_failure
                    raise CircuitBreakerError(self.name, self.state, retry_after)

            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.config.half_open_max_calls:
                    self.metrics.record_rejected()
                    raise CircuitBreakerError(self.name, self.state, 1.0)
                self._half_open_calls += 1

    async def _on_success(self, duration: float) -> None:
        """Handle successful call."""
        async with self._lock:
            self.metrics.record_success(duration)

            if duration > self.config.slow_call_duration_seconds:
                self.metrics.record_slow(duration)

            self._consecutive_failures = 0
            self._consecutive_successes += 1

            if self.state == CircuitState.HALF_OPEN:
                self._half_open_calls -= 1
                if self._consecutive_successes >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    async def _on_failure(self, duration: float) -> None:
        """Handle failed call."""
        async with self._lock:
            self.metrics.record_failure(duration)
            self._consecutive_successes = 0
            self._consecutive_failures += 1

            if self.state == CircuitState.HALF_OPEN:
                self._half_open_calls -= 1
                self._transition_to(CircuitState.OPEN)
            elif self.state == CircuitState.CLOSED:
                # Check if we should open the circuit
                should_open = (
                    self._consecutive_failures >= self.config.failure_threshold or
                    (
                        len(self.metrics.recent_results) >= self.config.minimum_calls and
                        self.metrics.failure_rate >= self.config.failure_rate_threshold
                    )
                )
                if should_open:
                    self._transition_to(CircuitState.OPEN)

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        old_state = self.state
        self.state = new_state
        self.metrics.last_state_change_time = time.time()

        if new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            self._consecutive_successes = 0
        elif new_state == CircuitState.CLOSED:
            self._consecutive_failures = 0
            self._consecutive_successes = 0

        logger.warning(
            f"Circuit '{self.name}' transitioned from {old_state.value} to {new_state.value}"
        )

    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status."""
        return {
            "name": self.name,
            "state": self.state.value,
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "successful_calls": self.metrics.successful_calls,
                "failed_calls": self.metrics.failed_calls,
                "rejected_calls": self.metrics.rejected_calls,
                "slow_calls": self.metrics.slow_calls,
                "failure_rate": round(self.metrics.failure_rate, 3),
                "avg_duration_ms": round(self.metrics.avg_duration * 1000, 1),
                "p99_duration_ms": round(self.metrics.p99_duration * 1000, 1),
            },
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "timeout_seconds": self.config.timeout_seconds,
            },
            "consecutive_failures": self._consecutive_failures,
            "consecutive_successes": self._consecutive_successes,
        }

    @classmethod
    def get_all_statuses(cls) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers."""
        return {name: cb.get_status() for name, cb in cls._instances.items()}

    @classmethod
    def reset(cls, name: str) -> bool:
        """Manually reset a circuit breaker to closed state."""
        if name in cls._instances:
            cb = cls._instances[name]
            cb.state = CircuitState.CLOSED
            cb._consecutive_failures = 0
            cb._consecutive_successes = 0
            logger.info(f"Circuit '{name}' manually reset to CLOSED")
            return True
        return False


# =============================================================================
# Request Deduplication
# =============================================================================

@dataclass
class DeduplicationEntry:
    """Entry in the deduplication cache."""
    idempotency_key: str
    request_hash: str
    result: Optional[Any] = None
    error: Optional[str] = None
    status: str = "pending"  # pending, completed, failed
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    ttl_seconds: float = 3600.0  # 1 hour default

    @property
    def is_expired(self) -> bool:
        return time.time() > self.created_at + self.ttl_seconds

    @property
    def is_pending(self) -> bool:
        return self.status == "pending"


class RequestDeduplicator:
    """Prevent duplicate request processing using idempotency keys.

    Ensures that identical requests are only processed once, even if
    submitted multiple times (e.g., due to retries or network issues).

    Example:
        dedup = RequestDeduplicator()

        async def create_task(request, idempotency_key: str):
            # Check for duplicate
            existing = await dedup.check(idempotency_key, request)
            if existing:
                return existing.result  # Return cached result

            # Process the request
            try:
                result = await actually_create_task(request)
                await dedup.complete(idempotency_key, result)
                return result
            except Exception as e:
                await dedup.fail(idempotency_key, str(e))
                raise
    """

    def __init__(
        self,
        default_ttl_seconds: float = 3600.0,
        max_entries: int = 10000,
        cleanup_interval_seconds: float = 300.0,
    ):
        self.default_ttl = default_ttl_seconds
        self.max_entries = max_entries
        self.cleanup_interval = cleanup_interval_seconds
        self._cache: Dict[str, DeduplicationEntry] = {}
        self._lock = asyncio.Lock()
        self._last_cleanup = time.time()

    def _compute_request_hash(self, request: Any) -> str:
        """Compute a hash of the request for comparison."""
        if isinstance(request, dict):
            serialized = json.dumps(request, sort_keys=True, default=str)
        else:
            serialized = str(request)
        return hashlib.sha256(serialized.encode()).hexdigest()[:32]

    async def check(
        self,
        idempotency_key: str,
        request: Any,
        ttl_seconds: Optional[float] = None,
    ) -> Optional[DeduplicationEntry]:
        """Check if a request is a duplicate.

        Returns:
            DeduplicationEntry if duplicate found (may be pending or complete)
            None if this is a new request
        """
        await self._maybe_cleanup()

        async with self._lock:
            entry = self._cache.get(idempotency_key)

            if entry is None:
                # New request - create pending entry
                request_hash = self._compute_request_hash(request)
                entry = DeduplicationEntry(
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    ttl_seconds=ttl_seconds or self.default_ttl,
                )
                self._cache[idempotency_key] = entry
                return None

            if entry.is_expired:
                # Expired entry - treat as new
                request_hash = self._compute_request_hash(request)
                entry = DeduplicationEntry(
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    ttl_seconds=ttl_seconds or self.default_ttl,
                )
                self._cache[idempotency_key] = entry
                return None

            # Check request hash matches
            request_hash = self._compute_request_hash(request)
            if entry.request_hash != request_hash:
                logger.warning(
                    f"Idempotency key '{idempotency_key}' reused with different request"
                )
                # Could raise an error here, but we'll just log and return

            return entry

    async def complete(
        self,
        idempotency_key: str,
        result: Any,
    ) -> None:
        """Mark a request as successfully completed."""
        async with self._lock:
            entry = self._cache.get(idempotency_key)
            if entry:
                entry.status = "completed"
                entry.result = result
                entry.completed_at = time.time()

    async def fail(
        self,
        idempotency_key: str,
        error: str,
    ) -> None:
        """Mark a request as failed."""
        async with self._lock:
            entry = self._cache.get(idempotency_key)
            if entry:
                entry.status = "failed"
                entry.error = error
                entry.completed_at = time.time()

    async def wait_for_completion(
        self,
        idempotency_key: str,
        timeout_seconds: float = 30.0,
        poll_interval: float = 0.5,
    ) -> Optional[DeduplicationEntry]:
        """Wait for a pending request to complete.

        Useful when a duplicate request comes in while the original is processing.
        """
        start = time.time()
        while time.time() - start < timeout_seconds:
            async with self._lock:
                entry = self._cache.get(idempotency_key)
                if entry and not entry.is_pending:
                    return entry
            await asyncio.sleep(poll_interval)
        return None

    async def _maybe_cleanup(self) -> None:
        """Periodically clean up expired entries."""
        now = time.time()
        if now - self._last_cleanup < self.cleanup_interval:
            return

        async with self._lock:
            self._last_cleanup = now

            # Remove expired entries
            expired = [k for k, v in self._cache.items() if v.is_expired]
            for key in expired:
                del self._cache[key]

            # If still over limit, remove oldest
            if len(self._cache) > self.max_entries:
                sorted_entries = sorted(
                    self._cache.items(),
                    key=lambda x: x[1].created_at
                )
                to_remove = len(self._cache) - self.max_entries
                for key, _ in sorted_entries[:to_remove]:
                    del self._cache[key]

            if expired:
                logger.debug(f"Deduplication cleanup: removed {len(expired)} expired entries")

    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics."""
        pending = sum(1 for e in self._cache.values() if e.is_pending)
        completed = sum(1 for e in self._cache.values() if e.status == "completed")
        failed = sum(1 for e in self._cache.values() if e.status == "failed")

        return {
            "total_entries": len(self._cache),
            "pending": pending,
            "completed": completed,
            "failed": failed,
            "max_entries": self.max_entries,
            "default_ttl_seconds": self.default_ttl,
        }


# =============================================================================
# Distributed Tracing
# =============================================================================

@dataclass
class TraceContext:
    """W3C Trace Context compatible trace context."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    sampled: bool = True
    baggage: Dict[str, str] = field(default_factory=dict)

    # Timing
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    # Metadata
    operation_name: str = ""
    service_name: str = "parallel-websearch-server"
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def generate_id(cls, length: int = 32) -> str:
        """Generate a random hex ID."""
        return uuid.uuid4().hex[:length]

    @classmethod
    def new_trace(cls, operation_name: str = "") -> 'TraceContext':
        """Create a new trace context."""
        return cls(
            trace_id=cls.generate_id(32),
            span_id=cls.generate_id(16),
            operation_name=operation_name,
        )

    def create_child_span(self, operation_name: str) -> 'TraceContext':
        """Create a child span within this trace."""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=self.generate_id(16),
            parent_span_id=self.span_id,
            sampled=self.sampled,
            baggage=self.baggage.copy(),
            operation_name=operation_name,
            service_name=self.service_name,
        )

    def to_traceparent(self) -> str:
        """Convert to W3C traceparent header format."""
        flags = "01" if self.sampled else "00"
        return f"00-{self.trace_id}-{self.span_id}-{flags}"

    @classmethod
    def from_traceparent(cls, traceparent: str, operation_name: str = "") -> 'TraceContext':
        """Parse W3C traceparent header."""
        try:
            parts = traceparent.split('-')
            if len(parts) >= 4:
                return cls(
                    trace_id=parts[1],
                    span_id=cls.generate_id(16),  # New span
                    parent_span_id=parts[2],
                    sampled=parts[3] == "01",
                    operation_name=operation_name,
                )
        except Exception:
            pass
        return cls.new_trace(operation_name)

    def add_tag(self, key: str, value: Any) -> None:
        """Add a tag to the span."""
        self.tags[key] = value

    def add_log(self, event: str, **fields) -> None:
        """Add a log entry to the span."""
        self.logs.append({
            "timestamp": time.time(),
            "event": event,
            **fields
        })

    def finish(self) -> None:
        """Mark the span as finished."""
        self.end_time = time.time()

    @property
    def duration_ms(self) -> float:
        """Get span duration in milliseconds."""
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/transmission."""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "service_name": self.service_name,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "sampled": self.sampled,
            "tags": self.tags,
            "logs": self.logs,
            "baggage": self.baggage,
        }


class DistributedTracer:
    """Distributed tracing with span management and storage.

    Provides OpenTelemetry-compatible tracing for request flows across services.

    Example:
        tracer = DistributedTracer()

        async def handle_request(request):
            # Extract or create trace context
            ctx = tracer.extract_context(request.headers)

            with tracer.span(ctx, "handle_request") as span:
                span.add_tag("http.method", request.method)

                # Create child span for downstream call
                with tracer.span(span, "call_upstream") as child:
                    result = await call_upstream()
                    child.add_tag("upstream.status", result.status)

                return response
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(
        self,
        service_name: str = "parallel-websearch-server",
        sample_rate: float = 1.0,
        max_spans: int = 10000,
    ):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        self.service_name = service_name
        self.sample_rate = sample_rate
        self.max_spans = max_spans

        self._spans: Dict[str, TraceContext] = {}
        self._completed_spans: Deque[TraceContext] = deque(maxlen=max_spans)
        self._lock = asyncio.Lock()

        # Context var for current span (thread-local style)
        self._current_span: Dict[int, TraceContext] = {}

    def _should_sample(self) -> bool:
        """Determine if this trace should be sampled."""
        return random.random() < self.sample_rate

    def extract_context(
        self,
        headers: Dict[str, str],
        operation_name: str = "",
    ) -> TraceContext:
        """Extract trace context from request headers."""
        traceparent = headers.get("traceparent") or headers.get("Traceparent")

        if traceparent:
            ctx = TraceContext.from_traceparent(traceparent, operation_name)
        else:
            ctx = TraceContext.new_trace(operation_name)
            ctx.sampled = self._should_sample()

        ctx.service_name = self.service_name

        # Extract baggage
        tracestate = headers.get("tracestate") or headers.get("Tracestate")
        if tracestate:
            ctx.baggage["tracestate"] = tracestate

        return ctx

    def inject_headers(self, ctx: TraceContext) -> Dict[str, str]:
        """Create headers for propagating trace context."""
        headers = {
            "traceparent": ctx.to_traceparent(),
        }
        if ctx.baggage.get("tracestate"):
            headers["tracestate"] = ctx.baggage["tracestate"]
        return headers

    class SpanContextManager:
        """Context manager for spans."""
        def __init__(self, tracer: 'DistributedTracer', span: TraceContext):
            self.tracer = tracer
            self.span = span

        def __enter__(self) -> TraceContext:
            return self.span

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                self.span.add_tag("error", True)
                self.span.add_tag("error.type", exc_type.__name__)
                self.span.add_tag("error.message", str(exc_val))
            self.span.finish()
            asyncio.create_task(self.tracer._store_span(self.span))
            return False

        async def __aenter__(self) -> TraceContext:
            return self.span

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            if exc_type:
                self.span.add_tag("error", True)
                self.span.add_tag("error.type", exc_type.__name__)
                self.span.add_tag("error.message", str(exc_val))
            self.span.finish()
            await self.tracer._store_span(self.span)
            return False

    def span(
        self,
        parent: Union[TraceContext, None],
        operation_name: str,
    ) -> SpanContextManager:
        """Create a new span, optionally as child of parent."""
        if parent:
            ctx = parent.create_child_span(operation_name)
        else:
            ctx = TraceContext.new_trace(operation_name)
            ctx.sampled = self._should_sample()

        ctx.service_name = self.service_name
        return self.SpanContextManager(self, ctx)

    async def _store_span(self, span: TraceContext) -> None:
        """Store a completed span."""
        if not span.sampled:
            return

        async with self._lock:
            self._completed_spans.append(span)

            # Also try to persist to database
            try:
                from parallel_websearch_server.local_store import get_local_store
                store = get_local_store()
                store.save_event(
                    event_id=f"span_{span.span_id}",
                    event_type="trace.span",
                    resource_id=span.trace_id,
                    resource_type="trace",
                    data=span.to_dict(),
                )
            except Exception as e:
                logger.debug(f"Could not persist span to database: {e}")

    def get_trace(self, trace_id: str) -> List[Dict[str, Any]]:
        """Get all spans for a trace ID."""
        spans = [
            s.to_dict() for s in self._completed_spans
            if s.trace_id == trace_id
        ]
        return sorted(spans, key=lambda x: x["start_time"])

    def get_recent_spans(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent completed spans."""
        spans = list(self._completed_spans)[-limit:]
        return [s.to_dict() for s in spans]


# =============================================================================
# Anomaly Detection
# =============================================================================

@dataclass
class MetricWindow:
    """Rolling window for metric statistics."""
    values: Deque = field(default_factory=lambda: deque(maxlen=1000))
    timestamps: Deque = field(default_factory=lambda: deque(maxlen=1000))

    def add(self, value: float) -> None:
        self.values.append(value)
        self.timestamps.append(time.time())

    def get_recent(self, seconds: float) -> List[float]:
        """Get values from the last N seconds."""
        cutoff = time.time() - seconds
        return [
            v for v, t in zip(self.values, self.timestamps)
            if t >= cutoff
        ]

    @property
    def mean(self) -> float:
        if not self.values:
            return 0.0
        return statistics.mean(self.values)

    @property
    def std_dev(self) -> float:
        if len(self.values) < 2:
            return 0.0
        return statistics.stdev(self.values)

    @property
    def median(self) -> float:
        if not self.values:
            return 0.0
        return statistics.median(self.values)


@dataclass
class AnomalyEvent:
    """Detected anomaly event."""
    anomaly_id: str
    anomaly_type: str
    metric_name: str
    current_value: float
    expected_range: Tuple[float, float]
    severity: str  # low, medium, high, critical
    detected_at: float
    context: Dict[str, Any] = field(default_factory=dict)


class AnomalyDetector:
    """Detect anomalies in request/response patterns.

    Uses statistical methods to detect unusual behavior:
    - Response time anomalies (too slow or suspiciously fast)
    - Error rate spikes
    - Traffic volume changes
    - Payload size anomalies

    Example:
        detector = AnomalyDetector()

        # Record normal operation
        detector.record_request(
            endpoint="/api/tasks",
            duration_ms=150,
            status_code=200,
            request_size=1024,
            response_size=2048,
        )

        # Check for anomalies
        anomalies = detector.detect_anomalies()
        for anomaly in anomalies:
            logger.warning(f"Anomaly detected: {anomaly}")
    """

    def __init__(
        self,
        window_size: int = 1000,
        z_score_threshold: float = 3.0,  # Standard deviations for anomaly
        min_samples: int = 30,  # Minimum samples before detection
    ):
        self.window_size = window_size
        self.z_score_threshold = z_score_threshold
        self.min_samples = min_samples

        # Metrics by endpoint
        self._duration_metrics: Dict[str, MetricWindow] = {}
        self._error_rates: Dict[str, MetricWindow] = {}
        self._request_sizes: Dict[str, MetricWindow] = {}
        self._response_sizes: Dict[str, MetricWindow] = {}
        self._request_counts: Dict[str, MetricWindow] = {}

        # Global metrics
        self._global_duration = MetricWindow()
        self._global_error_rate = MetricWindow()

        # Detected anomalies
        self._recent_anomalies: Deque[AnomalyEvent] = deque(maxlen=100)
        self._lock = threading.Lock()

    def record_request(
        self,
        endpoint: str,
        duration_ms: float,
        status_code: int,
        request_size: int = 0,
        response_size: int = 0,
    ) -> List[AnomalyEvent]:
        """Record a request and check for immediate anomalies."""
        anomalies = []
        is_error = status_code >= 400

        with self._lock:
            # Initialize metrics for new endpoints
            if endpoint not in self._duration_metrics:
                self._duration_metrics[endpoint] = MetricWindow()
                self._error_rates[endpoint] = MetricWindow()
                self._request_sizes[endpoint] = MetricWindow()
                self._response_sizes[endpoint] = MetricWindow()
                self._request_counts[endpoint] = MetricWindow()

            # Check for anomalies before recording (compare to history)
            if len(self._duration_metrics[endpoint].values) >= self.min_samples:
                anomalies.extend(self._check_duration_anomaly(endpoint, duration_ms))
                anomalies.extend(self._check_size_anomaly(endpoint, request_size, response_size))

            # Record metrics
            self._duration_metrics[endpoint].add(duration_ms)
            self._error_rates[endpoint].add(1.0 if is_error else 0.0)
            self._request_sizes[endpoint].add(request_size)
            self._response_sizes[endpoint].add(response_size)
            self._request_counts[endpoint].add(1)

            self._global_duration.add(duration_ms)
            self._global_error_rate.add(1.0 if is_error else 0.0)

            # Store anomalies
            for anomaly in anomalies:
                self._recent_anomalies.append(anomaly)

        return anomalies

    def _check_duration_anomaly(
        self,
        endpoint: str,
        duration_ms: float,
    ) -> List[AnomalyEvent]:
        """Check if duration is anomalous."""
        anomalies = []
        metrics = self._duration_metrics[endpoint]

        if metrics.std_dev == 0:
            return anomalies

        z_score = abs(duration_ms - metrics.mean) / metrics.std_dev

        if z_score > self.z_score_threshold:
            severity = "low"
            if z_score > self.z_score_threshold * 2:
                severity = "medium"
            if z_score > self.z_score_threshold * 3:
                severity = "high"

            expected_low = metrics.mean - (self.z_score_threshold * metrics.std_dev)
            expected_high = metrics.mean + (self.z_score_threshold * metrics.std_dev)

            anomalies.append(AnomalyEvent(
                anomaly_id=f"dur_{uuid.uuid4().hex[:8]}",
                anomaly_type="duration_anomaly",
                metric_name=f"{endpoint}.duration_ms",
                current_value=duration_ms,
                expected_range=(max(0, expected_low), expected_high),
                severity=severity,
                detected_at=time.time(),
                context={
                    "endpoint": endpoint,
                    "z_score": round(z_score, 2),
                    "mean": round(metrics.mean, 2),
                    "std_dev": round(metrics.std_dev, 2),
                }
            ))

        return anomalies

    def _check_size_anomaly(
        self,
        endpoint: str,
        request_size: int,
        response_size: int,
    ) -> List[AnomalyEvent]:
        """Check if request/response sizes are anomalous."""
        anomalies = []

        for size, metric_window, size_type in [
            (request_size, self._request_sizes[endpoint], "request"),
            (response_size, self._response_sizes[endpoint], "response"),
        ]:
            if len(metric_window.values) < self.min_samples or metric_window.std_dev == 0:
                continue

            z_score = abs(size - metric_window.mean) / metric_window.std_dev

            if z_score > self.z_score_threshold:
                severity = "low" if z_score < self.z_score_threshold * 2 else "medium"

                anomalies.append(AnomalyEvent(
                    anomaly_id=f"size_{uuid.uuid4().hex[:8]}",
                    anomaly_type=f"{size_type}_size_anomaly",
                    metric_name=f"{endpoint}.{size_type}_size",
                    current_value=size,
                    expected_range=(
                        max(0, metric_window.mean - self.z_score_threshold * metric_window.std_dev),
                        metric_window.mean + self.z_score_threshold * metric_window.std_dev
                    ),
                    severity=severity,
                    detected_at=time.time(),
                    context={
                        "endpoint": endpoint,
                        "z_score": round(z_score, 2),
                    }
                ))

        return anomalies

    def detect_error_rate_spike(
        self,
        window_seconds: float = 60.0,
        threshold_multiplier: float = 3.0,
    ) -> List[AnomalyEvent]:
        """Detect sudden spikes in error rate."""
        anomalies = []

        with self._lock:
            for endpoint, metrics in self._error_rates.items():
                recent = metrics.get_recent(window_seconds)
                if len(recent) < 10:
                    continue

                current_error_rate = sum(recent) / len(recent)
                historical_error_rate = metrics.mean

                if historical_error_rate > 0 and current_error_rate > historical_error_rate * threshold_multiplier:
                    anomalies.append(AnomalyEvent(
                        anomaly_id=f"err_{uuid.uuid4().hex[:8]}",
                        anomaly_type="error_rate_spike",
                        metric_name=f"{endpoint}.error_rate",
                        current_value=current_error_rate,
                        expected_range=(0, historical_error_rate * threshold_multiplier),
                        severity="high",
                        detected_at=time.time(),
                        context={
                            "endpoint": endpoint,
                            "historical_rate": round(historical_error_rate, 4),
                            "window_seconds": window_seconds,
                        }
                    ))

        return anomalies

    def get_recent_anomalies(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent detected anomalies."""
        with self._lock:
            anomalies = list(self._recent_anomalies)[-limit:]

        return [
            {
                "anomaly_id": a.anomaly_id,
                "type": a.anomaly_type,
                "metric": a.metric_name,
                "value": a.current_value,
                "expected_range": a.expected_range,
                "severity": a.severity,
                "detected_at": datetime.fromtimestamp(a.detected_at).isoformat(),
                "context": a.context,
            }
            for a in anomalies
        ]

    def get_endpoint_stats(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific endpoint."""
        with self._lock:
            if endpoint not in self._duration_metrics:
                return None

            duration = self._duration_metrics[endpoint]
            errors = self._error_rates[endpoint]

            return {
                "endpoint": endpoint,
                "duration_ms": {
                    "mean": round(duration.mean, 2),
                    "median": round(duration.median, 2),
                    "std_dev": round(duration.std_dev, 2),
                    "samples": len(duration.values),
                },
                "error_rate": round(errors.mean, 4) if errors.values else 0,
                "request_count": len(self._request_counts.get(endpoint, MetricWindow()).values),
            }

    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all endpoints."""
        with self._lock:
            endpoints = list(self._duration_metrics.keys())

        return {
            "endpoints": {ep: self.get_endpoint_stats(ep) for ep in endpoints},
            "global": {
                "duration_mean_ms": round(self._global_duration.mean, 2),
                "error_rate": round(self._global_error_rate.mean, 4),
                "total_samples": len(self._global_duration.values),
            },
            "anomalies": {
                "recent_count": len(self._recent_anomalies),
            }
        }


# =============================================================================
# Dead Letter Queue
# =============================================================================

@dataclass
class DeadLetterEntry:
    """Entry in the dead letter queue."""
    entry_id: str
    original_operation: str
    payload: Dict[str, Any]
    error_history: List[Dict[str, Any]]
    first_failure_at: float
    last_failure_at: float
    failure_count: int
    status: str = "dead"  # dead, reprocessing, resolved, discarded
    resolution: Optional[str] = None
    resolved_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DeadLetterQueue:
    """Handle permanently failed operations.

    When operations fail beyond the retry limit, they're moved to the dead
    letter queue for manual review and possible reprocessing.

    Example:
        dlq = DeadLetterQueue()

        # Record a dead letter
        dlq.add(
            operation="save_task_run",
            payload={"run_id": "trun_123", ...},
            error_history=[
                {"attempt": 1, "error": "Connection refused"},
                {"attempt": 2, "error": "Timeout"},
                {"attempt": 3, "error": "Connection refused"},
            ]
        )

        # Review dead letters
        entries = dlq.list_entries(status="dead")

        # Retry a dead letter
        success = await dlq.retry_entry(entry_id)

        # Discard a dead letter
        dlq.discard_entry(entry_id, reason="Duplicate request")
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        self._entries: Dict[str, DeadLetterEntry] = {}
        self._lock = asyncio.Lock()

        # Callbacks for notifications
        self._on_add_callbacks: List[Callable[[DeadLetterEntry], None]] = []

    def add(
        self,
        operation: str,
        payload: Dict[str, Any],
        error_history: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add an entry to the dead letter queue."""
        entry_id = f"dlq_{uuid.uuid4().hex[:12]}"
        now = time.time()

        entry = DeadLetterEntry(
            entry_id=entry_id,
            original_operation=operation,
            payload=payload,
            error_history=error_history,
            first_failure_at=error_history[0].get("timestamp", now) if error_history else now,
            last_failure_at=now,
            failure_count=len(error_history),
            metadata=metadata or {},
        )

        self._entries[entry_id] = entry

        logger.error(
            f"Dead letter queue: Added {entry_id} for operation '{operation}' "
            f"after {len(error_history)} failures"
        )

        # Persist to database
        self._persist_entry(entry)

        # Notify callbacks
        for callback in self._on_add_callbacks:
            try:
                callback(entry)
            except Exception as e:
                logger.error(f"DLQ callback error: {e}")

        return entry_id

    def _persist_entry(self, entry: DeadLetterEntry) -> None:
        """Persist entry to database."""
        try:
            from parallel_websearch_server.local_store import get_local_store
            store = get_local_store()
            store.save_event(
                event_id=entry.entry_id,
                event_type="dead_letter",
                resource_id=entry.original_operation,
                resource_type="dlq",
                data={
                    "operation": entry.original_operation,
                    "payload": entry.payload,
                    "error_history": entry.error_history,
                    "failure_count": entry.failure_count,
                    "status": entry.status,
                    "first_failure_at": entry.first_failure_at,
                    "last_failure_at": entry.last_failure_at,
                    "metadata": entry.metadata,
                }
            )
        except Exception as e:
            logger.error(f"Failed to persist DLQ entry: {e}")

    def list_entries(
        self,
        status: Optional[str] = None,
        operation: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List dead letter entries."""
        entries = list(self._entries.values())

        if status:
            entries = [e for e in entries if e.status == status]
        if operation:
            entries = [e for e in entries if e.original_operation == operation]

        entries = sorted(entries, key=lambda e: e.last_failure_at, reverse=True)[:limit]

        return [
            {
                "entry_id": e.entry_id,
                "operation": e.original_operation,
                "failure_count": e.failure_count,
                "status": e.status,
                "first_failure_at": datetime.fromtimestamp(e.first_failure_at).isoformat(),
                "last_failure_at": datetime.fromtimestamp(e.last_failure_at).isoformat(),
                "last_error": e.error_history[-1] if e.error_history else None,
                "metadata": e.metadata,
            }
            for e in entries
        ]

    def get_entry(self, entry_id: str) -> Optional[Dict[str, Any]]:
        """Get full details of a dead letter entry."""
        entry = self._entries.get(entry_id)
        if not entry:
            return None

        return {
            "entry_id": entry.entry_id,
            "operation": entry.original_operation,
            "payload": entry.payload,
            "error_history": entry.error_history,
            "failure_count": entry.failure_count,
            "status": entry.status,
            "resolution": entry.resolution,
            "resolved_at": datetime.fromtimestamp(entry.resolved_at).isoformat() if entry.resolved_at else None,
            "first_failure_at": datetime.fromtimestamp(entry.first_failure_at).isoformat(),
            "last_failure_at": datetime.fromtimestamp(entry.last_failure_at).isoformat(),
            "metadata": entry.metadata,
        }

    async def retry_entry(self, entry_id: str) -> bool:
        """Attempt to retry a dead letter entry."""
        entry = self._entries.get(entry_id)
        if not entry:
            return False

        if entry.status != "dead":
            logger.warning(f"Cannot retry DLQ entry {entry_id}: status is {entry.status}")
            return False

        entry.status = "reprocessing"

        try:
            from parallel_websearch_server.local_store import get_local_store
            store = get_local_store()

            # Attempt the operation
            if entry.original_operation == "save_task_run":
                store.save_task_run(**entry.payload)
            elif entry.original_operation == "save_search_request":
                store.save_search_request(**entry.payload)
            elif entry.original_operation == "save_extract_request":
                store.save_extract_request(**entry.payload)
            elif entry.original_operation == "save_task_group":
                store.save_task_group(**entry.payload)
            else:
                logger.error(f"Unknown DLQ operation: {entry.original_operation}")
                entry.status = "dead"
                return False

            # Success!
            entry.status = "resolved"
            entry.resolution = "retry_success"
            entry.resolved_at = time.time()

            logger.info(f"DLQ entry {entry_id} successfully retried")
            self._persist_entry(entry)
            return True

        except Exception as e:
            entry.status = "dead"
            entry.error_history.append({
                "attempt": entry.failure_count + 1,
                "error": str(e),
                "timestamp": time.time(),
                "source": "manual_retry",
            })
            entry.failure_count += 1
            entry.last_failure_at = time.time()

            logger.error(f"DLQ retry failed for {entry_id}: {e}")
            self._persist_entry(entry)
            return False

    def discard_entry(self, entry_id: str, reason: str) -> bool:
        """Discard a dead letter entry (mark as intentionally not processed)."""
        entry = self._entries.get(entry_id)
        if not entry:
            return False

        entry.status = "discarded"
        entry.resolution = reason
        entry.resolved_at = time.time()

        logger.info(f"DLQ entry {entry_id} discarded: {reason}")
        self._persist_entry(entry)
        return True

    def on_add(self, callback: Callable[[DeadLetterEntry], None]) -> None:
        """Register a callback for when entries are added."""
        self._on_add_callbacks.append(callback)

    def get_stats(self) -> Dict[str, Any]:
        """Get dead letter queue statistics."""
        by_status = {}
        by_operation = {}

        for entry in self._entries.values():
            by_status[entry.status] = by_status.get(entry.status, 0) + 1
            by_operation[entry.original_operation] = by_operation.get(entry.original_operation, 0) + 1

        return {
            "total_entries": len(self._entries),
            "by_status": by_status,
            "by_operation": by_operation,
            "oldest_entry": min(
                (e.first_failure_at for e in self._entries.values()),
                default=None
            ),
        }


# =============================================================================
# Health Scoring
# =============================================================================

@dataclass
class HealthCheckResult:
    """Result of a health check."""
    check_name: str
    healthy: bool
    latency_ms: float
    message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)


class HealthScorer:
    """Track upstream service health with degradation detection.

    Maintains a health score (0-100) based on:
    - Success rate of recent requests
    - Response latency
    - Error types and frequencies
    - Circuit breaker states

    Example:
        scorer = HealthScorer()

        # Record request outcomes
        scorer.record_success("api.parallel.ai", latency_ms=150)
        scorer.record_failure("api.parallel.ai", error="Timeout")

        # Get health status
        health = scorer.get_health("api.parallel.ai")
        # {"score": 85, "status": "degraded", "details": {...}}

        # Get overall system health
        system_health = scorer.get_system_health()
    """

    def __init__(
        self,
        window_size: int = 100,
        degraded_threshold: float = 80.0,
        unhealthy_threshold: float = 50.0,
    ):
        self.window_size = window_size
        self.degraded_threshold = degraded_threshold
        self.unhealthy_threshold = unhealthy_threshold

        # Per-service metrics
        self._success_rates: Dict[str, Deque] = {}  # bool values
        self._latencies: Dict[str, Deque] = {}  # float values in ms
        self._errors: Dict[str, Deque] = {}  # error strings
        self._health_checks: Dict[str, Deque] = {}  # HealthCheckResult

        # Baseline metrics (learned from history)
        self._baseline_latency: Dict[str, float] = {}

        self._lock = threading.Lock()

    def _ensure_service(self, service: str) -> None:
        """Ensure metrics exist for a service."""
        if service not in self._success_rates:
            self._success_rates[service] = deque(maxlen=self.window_size)
            self._latencies[service] = deque(maxlen=self.window_size)
            self._errors[service] = deque(maxlen=self.window_size)
            self._health_checks[service] = deque(maxlen=20)

    def record_success(self, service: str, latency_ms: float) -> None:
        """Record a successful request."""
        with self._lock:
            self._ensure_service(service)
            self._success_rates[service].append(True)
            self._latencies[service].append(latency_ms)

            # Update baseline (exponential moving average)
            if service in self._baseline_latency:
                alpha = 0.1
                self._baseline_latency[service] = (
                    alpha * latency_ms + (1 - alpha) * self._baseline_latency[service]
                )
            else:
                self._baseline_latency[service] = latency_ms

    def record_failure(self, service: str, error: str, latency_ms: float = 0) -> None:
        """Record a failed request."""
        with self._lock:
            self._ensure_service(service)
            self._success_rates[service].append(False)
            if latency_ms > 0:
                self._latencies[service].append(latency_ms)
            self._errors[service].append({
                "error": error,
                "timestamp": time.time(),
            })

    def record_health_check(self, result: HealthCheckResult) -> None:
        """Record a health check result."""
        with self._lock:
            self._ensure_service(result.check_name)
            self._health_checks[result.check_name].append(result)

    def calculate_score(self, service: str) -> float:
        """Calculate health score for a service (0-100)."""
        with self._lock:
            self._ensure_service(service)

            success_rate = self._success_rates[service]
            latencies = self._latencies[service]

            if not success_rate:
                return 100.0  # No data = assume healthy

            # Base score from success rate (0-70 points)
            successes = sum(1 for s in success_rate if s)
            success_score = (successes / len(success_rate)) * 70

            # Latency score (0-20 points)
            latency_score = 20.0
            if latencies and service in self._baseline_latency:
                avg_latency = statistics.mean(latencies)
                baseline = self._baseline_latency[service]

                if avg_latency > baseline * 3:
                    latency_score = 0
                elif avg_latency > baseline * 2:
                    latency_score = 10
                elif avg_latency > baseline * 1.5:
                    latency_score = 15

            # Consistency score (0-10 points) - based on latency variance
            consistency_score = 10.0
            if len(latencies) > 5:
                cv = statistics.stdev(latencies) / (statistics.mean(latencies) or 1)
                if cv > 1.0:
                    consistency_score = 0
                elif cv > 0.5:
                    consistency_score = 5

            return min(100, max(0, success_score + latency_score + consistency_score))

    def get_health(self, service: str) -> Dict[str, Any]:
        """Get health status for a service."""
        score = self.calculate_score(service)

        if score >= self.degraded_threshold:
            status = "healthy"
        elif score >= self.unhealthy_threshold:
            status = "degraded"
        else:
            status = "unhealthy"

        with self._lock:
            self._ensure_service(service)

            recent_errors = [
                e for e in self._errors[service]
                if time.time() - e["timestamp"] < 300  # Last 5 minutes
            ]

            latencies = list(self._latencies[service])

        return {
            "service": service,
            "score": round(score, 1),
            "status": status,
            "success_rate": round(
                sum(1 for s in self._success_rates.get(service, []) if s) /
                max(len(self._success_rates.get(service, [])), 1) * 100,
                1
            ),
            "avg_latency_ms": round(statistics.mean(latencies), 1) if latencies else 0,
            "recent_errors": len(recent_errors),
            "baseline_latency_ms": round(self._baseline_latency.get(service, 0), 1),
        }

    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health."""
        services = {}
        scores = []

        with self._lock:
            service_names = list(self._success_rates.keys())

        for service in service_names:
            health = self.get_health(service)
            services[service] = health
            scores.append(health["score"])

        avg_score = statistics.mean(scores) if scores else 100
        min_score = min(scores) if scores else 100

        if min_score >= self.degraded_threshold:
            status = "healthy"
        elif min_score >= self.unhealthy_threshold:
            status = "degraded"
        else:
            status = "unhealthy"

        return {
            "status": status,
            "overall_score": round(avg_score, 1),
            "lowest_score": round(min_score, 1),
            "services": services,
            "circuit_breakers": CircuitBreaker.get_all_statuses(),
        }


# =============================================================================
# Webhook Notifications
# =============================================================================

@dataclass
class WebhookConfig:
    """Configuration for a webhook endpoint."""
    url: str
    events: List[str]  # Event types to notify
    secret: Optional[str] = None  # For signing payloads
    retry_count: int = 3
    timeout_seconds: float = 10.0
    enabled: bool = True
    headers: Dict[str, str] = field(default_factory=dict)


class WebhookNotifier:
    """Send webhook notifications for important events.

    Example:
        notifier = WebhookNotifier()

        # Register a webhook
        notifier.register_webhook(WebhookConfig(
            url="https://hooks.example.com/parallel",
            events=["task.completed", "task.failed", "circuit.opened"],
        ))

        # Send notification
        await notifier.notify("task.completed", {
            "run_id": "trun_123",
            "status": "completed",
            "output": {...}
        })
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        self._webhooks: Dict[str, WebhookConfig] = {}
        self._delivery_history: Deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    def register_webhook(self, config: WebhookConfig) -> str:
        """Register a webhook endpoint."""
        webhook_id = f"wh_{uuid.uuid4().hex[:8]}"
        self._webhooks[webhook_id] = config
        logger.info(f"Registered webhook {webhook_id} for events: {config.events}")
        return webhook_id

    def unregister_webhook(self, webhook_id: str) -> bool:
        """Unregister a webhook endpoint."""
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            return True
        return False

    def _sign_payload(self, payload: str, secret: str) -> str:
        """Create HMAC signature for payload."""
        import hmac
        return hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()

    async def notify(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Send notifications to all registered webhooks."""
        import httpx

        results = []

        for webhook_id, config in self._webhooks.items():
            if not config.enabled:
                continue
            if event_type not in config.events and "*" not in config.events:
                continue

            delivery_result = {
                "webhook_id": webhook_id,
                "event_type": event_type,
                "success": False,
                "attempts": 0,
                "timestamp": time.time(),
            }

            notification = {
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "payload": payload,
            }

            body = json.dumps(notification)
            headers = {
                "Content-Type": "application/json",
                **config.headers,
            }

            if config.secret:
                headers["X-Webhook-Signature"] = self._sign_payload(body, config.secret)

            for attempt in range(config.retry_count):
                delivery_result["attempts"] = attempt + 1

                try:
                    async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
                        response = await client.post(
                            config.url,
                            content=body,
                            headers=headers,
                        )

                        delivery_result["status_code"] = response.status_code
                        delivery_result["success"] = 200 <= response.status_code < 300

                        if delivery_result["success"]:
                            break

                except Exception as e:
                    delivery_result["error"] = str(e)

                if attempt < config.retry_count - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

            results.append(delivery_result)
            self._delivery_history.append(delivery_result)

            if not delivery_result["success"]:
                logger.warning(
                    f"Webhook delivery failed for {webhook_id}: "
                    f"{delivery_result.get('error', delivery_result.get('status_code'))}"
                )

        return results

    def get_delivery_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent delivery history."""
        return list(self._delivery_history)[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get webhook statistics."""
        history = list(self._delivery_history)

        success = sum(1 for h in history if h["success"])
        failed = len(history) - success

        return {
            "registered_webhooks": len(self._webhooks),
            "enabled_webhooks": sum(1 for w in self._webhooks.values() if w.enabled),
            "deliveries": {
                "total": len(history),
                "success": success,
                "failed": failed,
                "success_rate": round(success / max(len(history), 1) * 100, 1),
            }
        }


# =============================================================================
# Global Instances
# =============================================================================

def get_circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
    """Get or create a circuit breaker by name."""
    return CircuitBreaker(name, config)


def get_request_deduplicator() -> RequestDeduplicator:
    """Get the global request deduplicator."""
    if not hasattr(get_request_deduplicator, "_instance"):
        get_request_deduplicator._instance = RequestDeduplicator()
    return get_request_deduplicator._instance


def get_distributed_tracer() -> DistributedTracer:
    """Get the global distributed tracer."""
    return DistributedTracer()


def get_anomaly_detector() -> AnomalyDetector:
    """Get the global anomaly detector."""
    if not hasattr(get_anomaly_detector, "_instance"):
        get_anomaly_detector._instance = AnomalyDetector()
    return get_anomaly_detector._instance


def get_dead_letter_queue() -> DeadLetterQueue:
    """Get the global dead letter queue."""
    return DeadLetterQueue()


def get_health_scorer() -> HealthScorer:
    """Get the global health scorer."""
    if not hasattr(get_health_scorer, "_instance"):
        get_health_scorer._instance = HealthScorer()
    return get_health_scorer._instance


def get_webhook_notifier() -> WebhookNotifier:
    """Get the global webhook notifier."""
    return WebhookNotifier()
