"""
Tool Result Caching Layer.

Caches tool execution results with:
- TTL-based expiration
- LRU eviction
- Cache key normalization
- Selective caching based on tool properties
"""

from __future__ import annotations

import hashlib
import json
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Callable
from datetime import datetime
from collections import OrderedDict
import threading

logger = logging.getLogger(__name__)


class CachePolicy(str, Enum):
    """Caching policy for tools."""
    ALWAYS = "always"          # Always cache
    NEVER = "never"            # Never cache
    IDEMPOTENT = "idempotent"  # Cache only idempotent operations
    TTL = "ttl"                # Cache with TTL
    STALE_WHILE_REVALIDATE = "swr"  # Serve stale, revalidate in background


@dataclass
class CacheEntry:
    """A single cache entry."""
    key: str
    value: Any
    created_at: float
    expires_at: float
    hit_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    @property
    def age_seconds(self) -> float:
        return time.time() - self.created_at

    @property
    def ttl_remaining(self) -> float:
        return max(0, self.expires_at - time.time())


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    current_size: int = 0
    max_size: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "expirations": self.expirations,
            "hit_rate": f"{self.hit_rate:.2%}",
            "current_size": self.current_size,
            "max_size": self.max_size,
        }


class ToolCache:
    """
    LRU cache for tool execution results.

    Features:
    - Configurable max size and TTL
    - Per-tool cache policies
    - Automatic eviction
    - Thread-safe operations
    """

    def __init__(
        self,
        max_size: int = 10000,
        default_ttl: int = 3600,  # 1 hour
        cleanup_interval: int = 60,
    ):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cleanup_interval = cleanup_interval

        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats(max_size=max_size)

        # Per-tool settings
        self._tool_policies: Dict[str, CachePolicy] = {}
        self._tool_ttls: Dict[str, int] = {}

        # Background cleanup
        self._last_cleanup = time.time()

    def configure_tool(
        self,
        tool_name: str,
        policy: CachePolicy = CachePolicy.TTL,
        ttl: int = None,
    ):
        """Configure caching for a specific tool."""
        self._tool_policies[tool_name] = policy
        if ttl is not None:
            self._tool_ttls[tool_name] = ttl

    def _make_key(self, tool_name: str, inputs: Dict[str, Any]) -> str:
        """Generate a cache key from tool name and inputs."""
        # Normalize inputs for consistent hashing
        normalized = json.dumps(inputs, sort_keys=True, default=str)
        content = f"{tool_name}:{normalized}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def _should_cache(self, tool_name: str) -> bool:
        """Check if results for a tool should be cached."""
        policy = self._tool_policies.get(tool_name, CachePolicy.TTL)
        return policy != CachePolicy.NEVER

    def _get_ttl(self, tool_name: str) -> int:
        """Get TTL for a tool."""
        return self._tool_ttls.get(tool_name, self.default_ttl)

    def get(
        self,
        tool_name: str,
        inputs: Dict[str, Any],
    ) -> Optional[Any]:
        """
        Get a cached result.

        Args:
            tool_name: Name of the tool
            inputs: Tool inputs

        Returns:
            Cached result or None if not found/expired
        """
        if not self._should_cache(tool_name):
            self._stats.misses += 1
            return None

        key = self._make_key(tool_name, inputs)

        with self._lock:
            self._maybe_cleanup()

            if key not in self._cache:
                self._stats.misses += 1
                return None

            entry = self._cache[key]

            if entry.is_expired:
                del self._cache[key]
                self._stats.expirations += 1
                self._stats.misses += 1
                self._stats.current_size = len(self._cache)
                return None

            # Update access time and move to end (LRU)
            entry.hit_count += 1
            entry.last_accessed = time.time()
            self._cache.move_to_end(key)

            self._stats.hits += 1
            logger.debug(f"Cache hit for {tool_name} (hits={entry.hit_count})")
            return entry.value

    def set(
        self,
        tool_name: str,
        inputs: Dict[str, Any],
        result: Any,
        ttl: int = None,
        metadata: Dict[str, Any] = None,
    ):
        """
        Cache a tool result.

        Args:
            tool_name: Name of the tool
            inputs: Tool inputs
            result: Result to cache
            ttl: Override TTL for this entry
            metadata: Additional metadata to store
        """
        if not self._should_cache(tool_name):
            return

        key = self._make_key(tool_name, inputs)
        effective_ttl = ttl if ttl is not None else self._get_ttl(tool_name)

        with self._lock:
            self._maybe_cleanup()

            # Evict if at capacity
            while len(self._cache) >= self.max_size:
                self._evict_one()

            now = time.time()
            entry = CacheEntry(
                key=key,
                value=result,
                created_at=now,
                expires_at=now + effective_ttl,
                metadata=metadata or {},
            )

            self._cache[key] = entry
            self._stats.current_size = len(self._cache)
            logger.debug(f"Cached result for {tool_name} (ttl={effective_ttl}s)")

    def invalidate(self, tool_name: str, inputs: Dict[str, Any] = None):
        """
        Invalidate cached results.

        Args:
            tool_name: Name of the tool
            inputs: Specific inputs to invalidate (None = all for this tool)
        """
        with self._lock:
            if inputs is not None:
                key = self._make_key(tool_name, inputs)
                if key in self._cache:
                    del self._cache[key]
                    self._stats.current_size = len(self._cache)
            else:
                # Invalidate all entries for this tool
                prefix = tool_name + ":"
                keys_to_delete = [
                    k for k, v in self._cache.items()
                    if v.metadata.get("tool_name") == tool_name
                ]
                for key in keys_to_delete:
                    del self._cache[key]
                self._stats.current_size = len(self._cache)

    def clear(self):
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            self._stats.current_size = 0

    def _evict_one(self):
        """Evict the least recently used entry."""
        if self._cache:
            self._cache.popitem(last=False)
            self._stats.evictions += 1

    def _maybe_cleanup(self):
        """Run periodic cleanup of expired entries."""
        now = time.time()
        if now - self._last_cleanup < self.cleanup_interval:
            return

        self._last_cleanup = now
        expired_keys = [
            k for k, v in self._cache.items()
            if v.is_expired
        ]
        for key in expired_keys:
            del self._cache[key]
            self._stats.expirations += 1

        self._stats.current_size = len(self._cache)
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self._stats.to_dict()

    def get_entries(self, limit: int = 100) -> list:
        """Get recent cache entries for debugging."""
        with self._lock:
            entries = []
            for key, entry in list(self._cache.items())[-limit:]:
                entries.append({
                    "key": key[:16] + "...",
                    "age_seconds": round(entry.age_seconds, 1),
                    "ttl_remaining": round(entry.ttl_remaining, 1),
                    "hit_count": entry.hit_count,
                })
            return entries


# Decorator for caching tool results
def cached(ttl: int = 3600, policy: CachePolicy = CachePolicy.TTL):
    """
    Decorator to cache tool results.

    Usage:
        @cached(ttl=600)
        async def my_tool(inputs):
            ...
    """
    def decorator(func: Callable):
        cache = ToolCache(default_ttl=ttl)

        async def wrapper(inputs: Dict[str, Any], **kwargs):
            tool_name = func.__name__

            # Check cache
            cached_result = cache.get(tool_name, inputs)
            if cached_result is not None:
                return cached_result

            # Execute and cache
            result = await func(inputs, **kwargs)
            cache.set(tool_name, inputs, result)
            return result

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper._cache = cache
        return wrapper

    return decorator


# Singleton instance
_cache: Optional[ToolCache] = None


def get_cache() -> ToolCache:
    """Get or create the global cache instance."""
    global _cache
    if _cache is None:
        _cache = ToolCache()
    return _cache
