"""
Cache Management API Routes

Provides endpoints for managing distributed cache.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
import logging

from parallel_websearch_server.caching import get_cache_manager, CacheBackendType

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1beta/cache", tags=["cache"])


# Request/Response Models

class CacheGetRequest(BaseModel):
    """Request to get cache value"""
    key: str = Field(..., description="Cache key")


class CacheSetRequest(BaseModel):
    """Request to set cache value"""
    key: str = Field(..., description="Cache key")
    value: str = Field(..., description="Value to cache (JSON string)")
    ttl: Optional[float] = Field(None, description="TTL in seconds")


class CacheDeleteRequest(BaseModel):
    """Request to delete cache key"""
    key: str = Field(..., description="Cache key to delete")


class CacheInvalidateRequest(BaseModel):
    """Request to invalidate cache pattern"""
    pattern: str = Field(..., description="Key pattern (supports wildcards)")


# Endpoints

@router.get("/statistics")
async def get_cache_statistics():
    """
    Get cache statistics.

    Returns:
        Cache performance metrics
    """
    cache_manager = get_cache_manager()
    stats = await cache_manager.get_statistics()

    return stats


@router.post("/get")
async def get_cache_value(request: CacheGetRequest):
    """
    Get value from cache.

    Args:
        request: Cache get request

    Returns:
        Cached value or null if not found
    """
    cache_manager = get_cache_manager()
    value = await cache_manager.backend.get(request.key)

    if value is None:
        return {
            "key": request.key,
            "found": False,
            "value": None
        }

    return {
        "key": request.key,
        "found": True,
        "value": value
    }


@router.post("/set")
async def set_cache_value(request: CacheSetRequest):
    """
    Set value in cache.

    Args:
        request: Cache set request

    Returns:
        Confirmation
    """
    cache_manager = get_cache_manager()

    # Parse JSON value
    import json
    try:
        value = json.loads(request.value)
    except json.JSONDecodeError:
        # Use as string if not valid JSON
        value = request.value

    success = await cache_manager.backend.set(
        request.key,
        value,
        request.ttl
    )

    return {
        "key": request.key,
        "set": success,
        "ttl": request.ttl
    }


@router.post("/delete")
async def delete_cache_value(request: CacheDeleteRequest):
    """
    Delete value from cache.

    Args:
        request: Cache delete request

    Returns:
        Confirmation
    """
    cache_manager = get_cache_manager()
    deleted = await cache_manager.backend.delete(request.key)

    return {
        "key": request.key,
        "deleted": deleted
    }


@router.post("/invalidate")
async def invalidate_cache_pattern(request: CacheInvalidateRequest):
    """
    Invalidate all keys matching pattern.

    Supports wildcards:
    - `*` matches any number of characters
    - `?` matches single character

    Args:
        request: Cache invalidate request

    Returns:
        Number of keys deleted
    """
    cache_manager = get_cache_manager()
    count = await cache_manager.invalidate_pattern(request.pattern)

    return {
        "pattern": request.pattern,
        "keys_deleted": count
    }


@router.post("/clear")
async def clear_cache():
    """
    Clear entire cache.

    WARNING: This will delete all cached data.

    Returns:
        Confirmation
    """
    cache_manager = get_cache_manager()
    await cache_manager.backend.clear()

    return {
        "cleared": True
    }


@router.get("/keys")
async def list_cache_keys(pattern: str = "*", limit: int = 100):
    """
    List cache keys matching pattern.

    Args:
        pattern: Key pattern (supports wildcards)
        limit: Maximum number of keys to return

    Returns:
        List of matching keys
    """
    cache_manager = get_cache_manager()
    keys = await cache_manager.backend.keys(pattern)

    # Apply limit
    keys = keys[:limit]

    return {
        "pattern": pattern,
        "count": len(keys),
        "keys": keys,
        "truncated": len(keys) >= limit
    }


@router.get("/exists/{key}")
async def check_cache_exists(key: str):
    """
    Check if key exists in cache.

    Args:
        key: Cache key to check

    Returns:
        Whether key exists
    """
    cache_manager = get_cache_manager()
    exists = await cache_manager.backend.exists(key)

    return {
        "key": key,
        "exists": exists
    }


@router.get("/health")
async def cache_health():
    """
    Cache system health check.

    Returns:
        Health status and statistics
    """
    cache_manager = get_cache_manager()
    stats = await cache_manager.get_statistics()

    return {
        "status": "healthy",
        "backend": stats.get("backend", "unknown"),
        "statistics": stats
    }
