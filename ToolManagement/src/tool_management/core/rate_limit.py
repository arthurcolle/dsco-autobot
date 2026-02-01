"""
Rate Limiting and Quota Management.

Provides:
- Token bucket rate limiting
- Per-client quotas
- Per-tool limits
- Sliding window counters
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import threading

logger = logging.getLogger(__name__)


@dataclass
class TokenBucket:
    """Token bucket for rate limiting."""
    capacity: float
    fill_rate: float  # tokens per second
    tokens: float = field(default=0)
    last_update: float = field(default_factory=time.time)

    def __post_init__(self):
        if self.tokens == 0:
            self.tokens = self.capacity

    def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens.

        Returns:
            True if tokens were consumed, False if rate limited
        """
        now = time.time()
        elapsed = now - self.last_update
        self.last_update = now

        # Add tokens based on elapsed time
        self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    @property
    def available_tokens(self) -> float:
        """Get currently available tokens."""
        now = time.time()
        elapsed = now - self.last_update
        return min(self.capacity, self.tokens + elapsed * self.fill_rate)

    @property
    def wait_time(self) -> float:
        """Time to wait for 1 token to be available."""
        if self.available_tokens >= 1:
            return 0
        return (1 - self.available_tokens) / self.fill_rate


@dataclass
class SlidingWindow:
    """Sliding window counter for rate limiting."""
    window_size: int  # seconds
    max_requests: int
    requests: list = field(default_factory=list)

    def record(self) -> bool:
        """
        Record a request.

        Returns:
            True if allowed, False if rate limited
        """
        now = time.time()
        cutoff = now - self.window_size

        # Remove old requests
        self.requests = [t for t in self.requests if t > cutoff]

        if len(self.requests) >= self.max_requests:
            return False

        self.requests.append(now)
        return True

    @property
    def current_count(self) -> int:
        """Get current request count in window."""
        now = time.time()
        cutoff = now - self.window_size
        return sum(1 for t in self.requests if t > cutoff)

    @property
    def remaining(self) -> int:
        """Get remaining requests in window."""
        return max(0, self.max_requests - self.current_count)


@dataclass
class ClientQuota:
    """Quota settings for a client."""
    client_id: str

    # Rate limits
    requests_per_second: int = 10
    requests_per_minute: int = 100
    requests_per_hour: int = 1000
    requests_per_day: int = 10000

    # Concurrent limits
    max_concurrent: int = 10

    # Per-tool limits
    tool_limits: Dict[str, int] = field(default_factory=dict)  # tool -> per minute

    # Usage tracking
    total_requests: int = 0
    total_tokens: int = 0

    # Billing
    monthly_limit: Optional[int] = None
    current_month_usage: int = 0

    # State
    is_blocked: bool = False
    blocked_until: Optional[datetime] = None

    def to_dict(self) -> Dict:
        return {
            "client_id": self.client_id,
            "requests_per_second": self.requests_per_second,
            "requests_per_minute": self.requests_per_minute,
            "requests_per_hour": self.requests_per_hour,
            "requests_per_day": self.requests_per_day,
            "max_concurrent": self.max_concurrent,
            "total_requests": self.total_requests,
            "is_blocked": self.is_blocked,
        }


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    reason: str = ""
    retry_after: float = 0  # seconds
    remaining: int = 0
    limit: int = 0
    reset_at: Optional[datetime] = None


class RateLimiter:
    """
    Multi-level rate limiter.

    Enforces:
    - Per-second token bucket
    - Per-minute sliding window
    - Per-hour/day counters
    - Concurrent request limits
    """

    def __init__(self):
        self._buckets: Dict[str, TokenBucket] = {}
        self._windows: Dict[str, Dict[str, SlidingWindow]] = defaultdict(dict)
        self._concurrent: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

    def check(
        self,
        client_id: str,
        quota: ClientQuota,
        tool_name: str = None,
    ) -> RateLimitResult:
        """
        Check if a request should be allowed.

        Args:
            client_id: Client identifier
            quota: Client's quota settings
            tool_name: Optional tool name for per-tool limits

        Returns:
            RateLimitResult with allowed status and metadata
        """
        with self._lock:
            # Check if blocked
            if quota.is_blocked:
                if quota.blocked_until and datetime.utcnow() < quota.blocked_until:
                    return RateLimitResult(
                        allowed=False,
                        reason="client_blocked",
                        retry_after=(quota.blocked_until - datetime.utcnow()).total_seconds(),
                    )
                quota.is_blocked = False

            # Check concurrent limit
            if self._concurrent[client_id] >= quota.max_concurrent:
                return RateLimitResult(
                    allowed=False,
                    reason="concurrent_limit",
                    remaining=0,
                    limit=quota.max_concurrent,
                )

            # Check per-second bucket
            bucket_key = f"{client_id}:second"
            if bucket_key not in self._buckets:
                self._buckets[bucket_key] = TokenBucket(
                    capacity=quota.requests_per_second,
                    fill_rate=quota.requests_per_second,
                )

            bucket = self._buckets[bucket_key]
            if not bucket.consume():
                return RateLimitResult(
                    allowed=False,
                    reason="rate_limit_second",
                    retry_after=bucket.wait_time,
                    remaining=int(bucket.available_tokens),
                    limit=quota.requests_per_second,
                )

            # Check per-minute window
            if "minute" not in self._windows[client_id]:
                self._windows[client_id]["minute"] = SlidingWindow(
                    window_size=60,
                    max_requests=quota.requests_per_minute,
                )

            minute_window = self._windows[client_id]["minute"]
            if not minute_window.record():
                return RateLimitResult(
                    allowed=False,
                    reason="rate_limit_minute",
                    retry_after=60,
                    remaining=minute_window.remaining,
                    limit=quota.requests_per_minute,
                )

            # Check per-hour window
            if "hour" not in self._windows[client_id]:
                self._windows[client_id]["hour"] = SlidingWindow(
                    window_size=3600,
                    max_requests=quota.requests_per_hour,
                )

            hour_window = self._windows[client_id]["hour"]
            if not hour_window.record():
                return RateLimitResult(
                    allowed=False,
                    reason="rate_limit_hour",
                    retry_after=3600,
                    remaining=hour_window.remaining,
                    limit=quota.requests_per_hour,
                )

            # Check per-tool limit if specified
            if tool_name and tool_name in quota.tool_limits:
                tool_key = f"{tool_name}:minute"
                if tool_key not in self._windows[client_id]:
                    self._windows[client_id][tool_key] = SlidingWindow(
                        window_size=60,
                        max_requests=quota.tool_limits[tool_name],
                    )

                tool_window = self._windows[client_id][tool_key]
                if not tool_window.record():
                    return RateLimitResult(
                        allowed=False,
                        reason=f"tool_limit:{tool_name}",
                        retry_after=60,
                        remaining=tool_window.remaining,
                        limit=quota.tool_limits[tool_name],
                    )

            # Increment concurrent counter
            self._concurrent[client_id] += 1

            return RateLimitResult(
                allowed=True,
                remaining=minute_window.remaining,
                limit=quota.requests_per_minute,
            )

    def release(self, client_id: str):
        """Release a concurrent slot after request completes."""
        with self._lock:
            self._concurrent[client_id] = max(0, self._concurrent[client_id] - 1)

    def get_usage(self, client_id: str) -> Dict:
        """Get current usage for a client."""
        with self._lock:
            windows = self._windows.get(client_id, {})
            return {
                "concurrent": self._concurrent.get(client_id, 0),
                "minute": windows.get("minute", SlidingWindow(60, 100)).current_count,
                "hour": windows.get("hour", SlidingWindow(3600, 1000)).current_count,
            }


class QuotaManager:
    """
    Manages client quotas and usage tracking.
    """

    def __init__(self):
        self._quotas: Dict[str, ClientQuota] = {}
        self._rate_limiter = RateLimiter()
        self._lock = threading.RLock()

        # Default quota for unknown clients
        self._default_quota = ClientQuota(
            client_id="default",
            requests_per_second=5,
            requests_per_minute=50,
            requests_per_hour=500,
            requests_per_day=5000,
            max_concurrent=5,
        )

    def set_quota(self, client_id: str, quota: ClientQuota):
        """Set quota for a client."""
        with self._lock:
            quota.client_id = client_id
            self._quotas[client_id] = quota

    def get_quota(self, client_id: str) -> ClientQuota:
        """Get quota for a client (or default)."""
        return self._quotas.get(client_id, self._default_quota)

    def check_and_consume(
        self,
        client_id: str,
        tool_name: str = None,
    ) -> RateLimitResult:
        """
        Check rate limits and consume quota.

        Args:
            client_id: Client identifier
            tool_name: Optional tool name

        Returns:
            RateLimitResult
        """
        quota = self.get_quota(client_id)
        return self._rate_limiter.check(client_id, quota, tool_name)

    def release(self, client_id: str):
        """Release concurrent slot."""
        self._rate_limiter.release(client_id)

    def record_usage(
        self,
        client_id: str,
        tokens: int = 0,
        cost: float = 0,
    ):
        """Record usage for billing."""
        with self._lock:
            if client_id in self._quotas:
                self._quotas[client_id].total_requests += 1
                self._quotas[client_id].total_tokens += tokens
                self._quotas[client_id].current_month_usage += 1

    def block_client(self, client_id: str, duration_seconds: int, reason: str = ""):
        """Temporarily block a client."""
        with self._lock:
            quota = self.get_quota(client_id)
            quota.is_blocked = True
            quota.blocked_until = datetime.utcnow() + timedelta(seconds=duration_seconds)
            logger.warning(f"Blocked client {client_id} for {duration_seconds}s: {reason}")

    def get_all_quotas(self) -> Dict[str, Dict]:
        """Get all client quotas."""
        return {cid: q.to_dict() for cid, q in self._quotas.items()}

    def get_stats(self) -> Dict:
        """Get quota manager statistics."""
        return {
            "total_clients": len(self._quotas),
            "blocked_clients": sum(1 for q in self._quotas.values() if q.is_blocked),
        }


# Singleton instance
_quota_manager: Optional[QuotaManager] = None


def get_quota_manager() -> QuotaManager:
    """Get or create the global quota manager."""
    global _quota_manager
    if _quota_manager is None:
        _quota_manager = QuotaManager()
    return _quota_manager
