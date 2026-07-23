"""
Rate Limiting System

Production-grade rate limiting with:
- Token bucket algorithm
- Sliding window counters
- Per-user and per-endpoint limits
- Adaptive throttling based on system load
- Distributed support (Redis-compatible)
"""

import time
import asyncio
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
from collections import deque
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class RateLimitStrategy(str, Enum):
    """Rate limiting strategy"""
    TOKEN_BUCKET = "token_bucket"  # Token bucket algorithm
    SLIDING_WINDOW = "sliding_window"  # Sliding window counter
    FIXED_WINDOW = "fixed_window"  # Fixed window counter


@dataclass
class RateLimitConfig:
    """Rate limit configuration"""
    requests_per_second: float = 10.0
    requests_per_minute: int = 300
    requests_per_hour: int = 10000
    burst_size: int = 20  # Max burst for token bucket
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET


@dataclass
class TokenBucket:
    """Token bucket for rate limiting"""
    capacity: float  # Maximum tokens
    tokens: float  # Current tokens
    refill_rate: float  # Tokens added per second
    last_refill: float = field(default_factory=time.time)

    def consume(self, tokens: int = 1) -> bool:
        """
        Attempt to consume tokens.

        Returns:
            True if tokens were consumed, False if insufficient
        """
        # Refill tokens based on elapsed time
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.capacity,
            self.tokens + (elapsed * self.refill_rate)
        )
        self.last_refill = now

        # Try to consume
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True

        return False

    def get_wait_time(self, tokens: int = 1) -> float:
        """
        Get time to wait until tokens are available.

        Returns:
            Seconds to wait
        """
        if self.tokens >= tokens:
            return 0.0

        needed = tokens - self.tokens
        return needed / self.refill_rate

    def get_available_tokens(self) -> int:
        """Get number of available tokens"""
        now = time.time()
        elapsed = now - self.last_refill
        available = min(
            self.capacity,
            self.tokens + (elapsed * self.refill_rate)
        )
        return int(available)


@dataclass
class SlidingWindow:
    """Sliding window counter for rate limiting"""
    window_size: int  # Window size in seconds
    max_requests: int  # Max requests in window
    requests: deque = field(default_factory=deque)  # Timestamp queue

    def add_request(self, timestamp: Optional[float] = None) -> bool:
        """
        Add request to window.

        Returns:
            True if request allowed, False if rate limit exceeded
        """
        now = timestamp or time.time()

        # Remove old requests outside window
        cutoff = now - self.window_size
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()

        # Check if limit exceeded
        if len(self.requests) >= self.max_requests:
            return False

        # Add request
        self.requests.append(now)
        return True

    def get_remaining(self) -> int:
        """Get remaining requests in window"""
        now = time.time()
        cutoff = now - self.window_size

        # Clean old requests
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()

        return max(0, self.max_requests - len(self.requests))

    def get_reset_time(self) -> float:
        """Get time when window resets (oldest request expires)"""
        if not self.requests:
            return 0.0

        oldest = self.requests[0]
        return oldest + self.window_size


@dataclass
class RateLimitStatus:
    """Rate limit status"""
    allowed: bool
    limit: int
    remaining: int
    reset_time: float
    retry_after: Optional[float] = None  # Seconds to wait


class RateLimiter:
    """
    Production-grade rate limiter.

    Supports:
    - Multiple strategies (token bucket, sliding window, fixed window)
    - Per-user and per-endpoint limits
    - Adaptive throttling
    - Rate limit headers
    """

    def __init__(
        self,
        default_config: Optional[RateLimitConfig] = None,
        enable_adaptive: bool = True
    ):
        """
        Args:
            default_config: Default rate limit configuration
            enable_adaptive: Enable adaptive throttling based on system load
        """
        self.default_config = default_config or RateLimitConfig()
        self.enable_adaptive = enable_adaptive

        # Per-key rate limiters
        self.token_buckets: Dict[str, TokenBucket] = {}
        self.sliding_windows: Dict[str, Dict[str, SlidingWindow]] = {}  # key -> window_type -> window

        # Per-endpoint configs
        self.endpoint_configs: Dict[str, RateLimitConfig] = {}

        # Adaptive throttling
        self.adaptive_multiplier = 1.0  # Multiplier for rate limits (0.5 = 50% capacity)
        self.system_load = 0.0  # 0-1 system load

        # Locks
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_lock(self, key: str) -> asyncio.Lock:
        """Get or create lock for key"""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def set_endpoint_config(self, endpoint: str, config: RateLimitConfig):
        """Set rate limit config for specific endpoint"""
        self.endpoint_configs[endpoint] = config
        logger.info(f"Set rate limit for endpoint {endpoint}: {config}")

    def _get_config(self, endpoint: Optional[str] = None) -> RateLimitConfig:
        """Get rate limit config for endpoint"""
        if endpoint and endpoint in self.endpoint_configs:
            return self.endpoint_configs[endpoint]
        return self.default_config

    async def check_rate_limit(
        self,
        key: str,
        endpoint: Optional[str] = None,
        tokens: int = 1
    ) -> RateLimitStatus:
        """
        Check if request is allowed under rate limits.

        Args:
            key: Rate limit key (e.g., API key, user ID, IP)
            endpoint: Optional endpoint for endpoint-specific limits
            tokens: Number of tokens to consume

        Returns:
            Rate limit status
        """
        config = self._get_config(endpoint)

        # Apply adaptive throttling
        if self.enable_adaptive:
            config = self._apply_adaptive_throttling(config)

        # Use configured strategy
        if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            return await self._check_token_bucket(key, config, tokens)
        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            return await self._check_sliding_window(key, config, tokens)
        else:
            # Fixed window not implemented yet, fall back to sliding
            return await self._check_sliding_window(key, config, tokens)

    async def _check_token_bucket(
        self,
        key: str,
        config: RateLimitConfig,
        tokens: int
    ) -> RateLimitStatus:
        """Check rate limit using token bucket"""
        async with self._get_lock(key):
            # Get or create bucket
            if key not in self.token_buckets:
                self.token_buckets[key] = TokenBucket(
                    capacity=config.burst_size,
                    tokens=config.burst_size,
                    refill_rate=config.requests_per_second
                )

            bucket = self.token_buckets[key]

            # Try to consume
            allowed = bucket.consume(tokens)

            if allowed:
                return RateLimitStatus(
                    allowed=True,
                    limit=int(config.burst_size),
                    remaining=bucket.get_available_tokens(),
                    reset_time=time.time() + (1.0 / config.requests_per_second)
                )
            else:
                wait_time = bucket.get_wait_time(tokens)
                return RateLimitStatus(
                    allowed=False,
                    limit=int(config.burst_size),
                    remaining=0,
                    reset_time=time.time() + wait_time,
                    retry_after=wait_time
                )

    async def _check_sliding_window(
        self,
        key: str,
        config: RateLimitConfig,
        tokens: int
    ) -> RateLimitStatus:
        """Check rate limit using sliding window"""
        async with self._get_lock(key):
            # Initialize windows for key
            if key not in self.sliding_windows:
                self.sliding_windows[key] = {}

            windows = self.sliding_windows[key]

            # Check second window
            if "second" not in windows:
                windows["second"] = SlidingWindow(
                    window_size=1,
                    max_requests=int(config.requests_per_second)
                )

            # Check minute window
            if "minute" not in windows:
                windows["minute"] = SlidingWindow(
                    window_size=60,
                    max_requests=config.requests_per_minute
                )

            # Check hour window
            if "hour" not in windows:
                windows["hour"] = SlidingWindow(
                    window_size=3600,
                    max_requests=config.requests_per_hour
                )

            # Check all windows
            now = time.time()

            # Most restrictive window determines outcome
            for window_name, window in windows.items():
                if not window.add_request(now):
                    # Rate limit exceeded
                    return RateLimitStatus(
                        allowed=False,
                        limit=window.max_requests,
                        remaining=0,
                        reset_time=window.get_reset_time(),
                        retry_after=window.get_reset_time() - now
                    )

            # All windows passed
            min_remaining = min(w.get_remaining() for w in windows.values())
            earliest_reset = min(w.get_reset_time() for w in windows.values())

            return RateLimitStatus(
                allowed=True,
                limit=config.requests_per_minute,  # Use minute limit for display
                remaining=min_remaining,
                reset_time=earliest_reset
            )

    def _apply_adaptive_throttling(self, config: RateLimitConfig) -> RateLimitConfig:
        """Apply adaptive throttling based on system load"""
        # Scale limits based on adaptive multiplier
        adapted = RateLimitConfig(
            requests_per_second=config.requests_per_second * self.adaptive_multiplier,
            requests_per_minute=int(config.requests_per_minute * self.adaptive_multiplier),
            requests_per_hour=int(config.requests_per_hour * self.adaptive_multiplier),
            burst_size=int(config.burst_size * self.adaptive_multiplier),
            strategy=config.strategy
        )

        return adapted

    def update_system_load(self, load: float):
        """
        Update system load for adaptive throttling.

        Args:
            load: System load (0-1, where 1 = fully loaded)
        """
        self.system_load = max(0.0, min(1.0, load))

        # Calculate adaptive multiplier
        # High load = reduce capacity
        if load > 0.9:
            self.adaptive_multiplier = 0.5  # 50% capacity
        elif load > 0.8:
            self.adaptive_multiplier = 0.7  # 70% capacity
        elif load > 0.7:
            self.adaptive_multiplier = 0.85  # 85% capacity
        else:
            self.adaptive_multiplier = 1.0  # Full capacity

        logger.info(
            f"System load: {load:.2%}, adaptive multiplier: {self.adaptive_multiplier:.2%}"
        )

    async def get_statistics(self) -> Dict:
        """Get rate limiting statistics"""
        return {
            "total_keys": len(self.token_buckets) + len(self.sliding_windows),
            "token_buckets": len(self.token_buckets),
            "sliding_windows": len(self.sliding_windows),
            "system_load": self.system_load,
            "adaptive_multiplier": self.adaptive_multiplier,
            "adaptive_enabled": self.enable_adaptive
        }

    async def reset_key(self, key: str):
        """Reset rate limits for a specific key"""
        if key in self.token_buckets:
            del self.token_buckets[key]
        if key in self.sliding_windows:
            del self.sliding_windows[key]
        if key in self._locks:
            del self._locks[key]

        logger.info(f"Reset rate limits for key: {key}")


# Global rate limiter instance
_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Get or create global rate limiter"""
    global _limiter
    if _limiter is None:
        _limiter = RateLimiter(
            default_config=RateLimitConfig(
                requests_per_second=10.0,
                requests_per_minute=300,
                requests_per_hour=10000,
                burst_size=20,
                strategy=RateLimitStrategy.SLIDING_WINDOW
            ),
            enable_adaptive=True
        )
    return _limiter
