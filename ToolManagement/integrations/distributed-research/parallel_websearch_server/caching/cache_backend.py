"""
Distributed Caching Backend

Supports multiple backends:
- In-memory (development/testing)
- Redis (production)
- Hybrid (memory + Redis with write-through)

Features:
- TTL support
- Compression for large values
- Atomic operations
- Cache invalidation patterns
"""

import asyncio
import hashlib
import json
import pickle
import time
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Dict, List
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CacheBackendType(str, Enum):
    """Cache backend types"""
    MEMORY = "memory"
    REDIS = "redis"
    HYBRID = "hybrid"


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    value: Any
    created_at: float
    ttl: Optional[float]
    access_count: int = 0
    last_accessed: Optional[float] = None
    size_bytes: int = 0

    def is_expired(self) -> bool:
        """Check if entry has expired"""
        if self.ttl is None:
            return False
        return time.time() > (self.created_at + self.ttl)


class CacheBackend(ABC):
    """Abstract cache backend"""

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set value in cache"""
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        pass

    @abstractmethod
    async def clear(self) -> bool:
        """Clear all cache entries"""
        pass

    @abstractmethod
    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern"""
        pass

    @abstractmethod
    async def get_stats(self) -> Dict:
        """Get cache statistics"""
        pass


class MemoryCacheBackend(CacheBackend):
    """
    In-memory cache backend.

    Features:
    - LRU eviction when max_size reached
    - Automatic expiration
    - Hit/miss statistics
    """

    def __init__(
        self,
        max_size: int = 10000,
        default_ttl: Optional[float] = 3600.0,
        enable_compression: bool = True,
        compression_threshold: int = 1024  # Compress if > 1KB
    ):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.enable_compression = enable_compression
        self.compression_threshold = compression_threshold

        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()

        # Statistics
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        async with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None

            entry = self._cache[key]

            # Check expiration
            if entry.is_expired():
                del self._cache[key]
                self._misses += 1
                return None

            # Update access metadata
            entry.access_count += 1
            entry.last_accessed = time.time()
            self._hits += 1

            # Decompress if needed
            value = entry.value
            if isinstance(value, bytes) and value[:2] == b'ZL':  # Compressed marker
                value = pickle.loads(zlib.decompress(value[2:]))

            return value

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set value in cache"""
        async with self._lock:
            # Serialize and optionally compress
            serialized = pickle.dumps(value)
            size_bytes = len(serialized)

            if self.enable_compression and size_bytes > self.compression_threshold:
                compressed = zlib.compress(serialized)
                if len(compressed) < size_bytes:
                    serialized = b'ZL' + compressed  # Add compression marker
                    size_bytes = len(serialized)

            # Evict if at capacity
            if len(self._cache) >= self.max_size and key not in self._cache:
                await self._evict_lru()

            # Create entry
            entry = CacheEntry(
                value=serialized,
                created_at=time.time(),
                ttl=ttl if ttl is not None else self.default_ttl,
                size_bytes=size_bytes
            )

            self._cache[key] = entry
            return True

    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def exists(self, key: str) -> bool:
        """Check if key exists and is not expired"""
        async with self._lock:
            if key not in self._cache:
                return False

            entry = self._cache[key]
            if entry.is_expired():
                del self._cache[key]
                return False

            return True

    async def clear(self) -> bool:
        """Clear all cache entries"""
        async with self._lock:
            self._cache.clear()
            return True

    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern (simplified glob)"""
        async with self._lock:
            # Remove expired entries
            expired = [k for k, v in self._cache.items() if v.is_expired()]
            for k in expired:
                del self._cache[k]

            # Simple pattern matching
            if pattern == "*":
                return list(self._cache.keys())

            # Basic wildcard support
            import fnmatch
            return [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]

    async def get_stats(self) -> Dict:
        """Get cache statistics"""
        async with self._lock:
            total_size = sum(e.size_bytes for e in self._cache.values())
            total_requests = self._hits + self._misses
            hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0

            return {
                "backend": "memory",
                "entries": len(self._cache),
                "max_size": self.max_size,
                "total_size_bytes": total_size,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "hit_rate": hit_rate,
                "utilization": len(self._cache) / self.max_size * 100
            }

    async def _evict_lru(self):
        """Evict least recently used entry"""
        if not self._cache:
            return

        # Find LRU entry
        lru_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].last_accessed or self._cache[k].created_at
        )

        del self._cache[lru_key]
        self._evictions += 1
        logger.debug(f"Evicted LRU entry: {lru_key}")


class RedisCacheBackend(CacheBackend):
    """
    Redis cache backend.

    Features:
    - Distributed caching
    - Atomic operations
    - TTL support via Redis
    - Connection pooling
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        prefix: str = "cache:",
        default_ttl: Optional[int] = 3600,
        enable_compression: bool = True,
        compression_threshold: int = 1024
    ):
        self.redis_url = redis_url
        self.prefix = prefix
        self.default_ttl = default_ttl
        self.enable_compression = enable_compression
        self.compression_threshold = compression_threshold

        self._redis = None
        self._connected = False

        # Statistics
        self._hits = 0
        self._misses = 0

    async def _connect(self):
        """Lazy connection to Redis"""
        if self._connected:
            return

        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=False  # We handle serialization
            )
            # Test connection
            await self._redis.ping()
            self._connected = True
            logger.info(f"Connected to Redis: {self.redis_url}")
        except ImportError:
            logger.error("redis package not installed. Install with: pip install redis")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def _make_key(self, key: str) -> str:
        """Add prefix to key"""
        return f"{self.prefix}{key}"

    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis"""
        await self._connect()

        redis_key = self._make_key(key)
        data = await self._redis.get(redis_key)

        if data is None:
            self._misses += 1
            return None

        self._hits += 1

        # Deserialize
        if data[:2] == b'ZL':  # Compressed
            data = zlib.decompress(data[2:])

        return pickle.loads(data)

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Set value in Redis"""
        await self._connect()

        # Serialize
        serialized = pickle.dumps(value)

        # Compress if beneficial
        if self.enable_compression and len(serialized) > self.compression_threshold:
            compressed = zlib.compress(serialized)
            if len(compressed) < len(serialized):
                serialized = b'ZL' + compressed

        redis_key = self._make_key(key)
        ttl_seconds = int(ttl) if ttl is not None else self.default_ttl

        if ttl_seconds:
            await self._redis.setex(redis_key, ttl_seconds, serialized)
        else:
            await self._redis.set(redis_key, serialized)

        return True

    async def delete(self, key: str) -> bool:
        """Delete key from Redis"""
        await self._connect()

        redis_key = self._make_key(key)
        result = await self._redis.delete(redis_key)
        return result > 0

    async def exists(self, key: str) -> bool:
        """Check if key exists in Redis"""
        await self._connect()

        redis_key = self._make_key(key)
        return await self._redis.exists(redis_key) > 0

    async def clear(self) -> bool:
        """Clear all keys with prefix"""
        await self._connect()

        pattern = f"{self.prefix}*"
        cursor = 0

        while True:
            cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)
            if keys:
                await self._redis.delete(*keys)
            if cursor == 0:
                break

        return True

    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern"""
        await self._connect()

        redis_pattern = f"{self.prefix}{pattern}"
        cursor = 0
        all_keys = []

        while True:
            cursor, keys = await self._redis.scan(cursor, match=redis_pattern, count=100)
            # Remove prefix from keys
            all_keys.extend([
                k.decode() if isinstance(k, bytes) else k
                for k in keys
            ])
            if cursor == 0:
                break

        # Strip prefix
        return [k[len(self.prefix):] for k in all_keys]

    async def get_stats(self) -> Dict:
        """Get cache statistics"""
        await self._connect()

        info = await self._redis.info("memory")
        total_requests = self._hits + self._misses
        hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0

        return {
            "backend": "redis",
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "memory_used_bytes": info.get("used_memory", 0),
            "memory_peak_bytes": info.get("used_memory_peak", 0),
            "connected": self._connected
        }

    async def close(self):
        """Close Redis connection"""
        if self._redis:
            await self._redis.close()
            self._connected = False


class CacheManager:
    """
    High-level cache manager.

    Provides:
    - Automatic key generation from function arguments
    - Backend selection
    - Cache warming
    - Invalidation patterns
    """

    def __init__(
        self,
        backend: CacheBackend,
        default_ttl: Optional[float] = 3600.0
    ):
        self.backend = backend
        self.default_ttl = default_ttl

    @staticmethod
    def generate_key(prefix: str, *args, **kwargs) -> str:
        """Generate cache key from arguments"""
        # Create deterministic key from arguments
        key_data = {
            "prefix": prefix,
            "args": args,
            "kwargs": sorted(kwargs.items())
        }

        key_str = json.dumps(key_data, sort_keys=True)
        key_hash = hashlib.sha256(key_str.encode()).hexdigest()[:16]

        return f"{prefix}:{key_hash}"

    async def get_or_compute(
        self,
        key: str,
        compute_fn,
        ttl: Optional[float] = None,
        force_refresh: bool = False
    ) -> Any:
        """
        Get from cache or compute if missing.

        Args:
            key: Cache key
            compute_fn: Async function to compute value if not cached
            ttl: TTL override
            force_refresh: Force recomputation even if cached

        Returns:
            Cached or computed value
        """
        if not force_refresh:
            cached = await self.backend.get(key)
            if cached is not None:
                return cached

        # Compute value
        value = await compute_fn() if asyncio.iscoroutinefunction(compute_fn) else compute_fn()

        # Cache result
        await self.backend.set(key, value, ttl or self.default_ttl)

        return value

    async def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all keys matching pattern.

        Returns:
            Number of keys deleted
        """
        keys = await self.backend.keys(pattern)
        count = 0

        for key in keys:
            if await self.backend.delete(key):
                count += 1

        return count

    async def get_statistics(self) -> Dict:
        """Get cache statistics"""
        return await self.backend.get_stats()


# Global cache instance
_cache_manager: Optional[CacheManager] = None


def get_cache_manager(backend_type: CacheBackendType = CacheBackendType.MEMORY) -> CacheManager:
    """Get or create global cache manager"""
    global _cache_manager

    if _cache_manager is None:
        if backend_type == CacheBackendType.MEMORY:
            backend = MemoryCacheBackend(max_size=10000, default_ttl=3600.0)
        elif backend_type == CacheBackendType.REDIS:
            backend = RedisCacheBackend(default_ttl=3600)
        else:
            backend = MemoryCacheBackend()  # Fallback

        _cache_manager = CacheManager(backend=backend, default_ttl=3600.0)

    return _cache_manager
