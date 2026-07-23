"""
Rate Limiting Management API Routes

Endpoints for managing rate limits and viewing statistics.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import logging

from parallel_websearch_server.rate_limiting import (
    get_rate_limiter,
    RateLimitConfig,
    RateLimitStrategy
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1beta/rate-limits", tags=["rate-limits"])


# Request/Response Models

class SetRateLimitRequest(BaseModel):
    """Request to set rate limit"""
    endpoint: str = Field(..., description="Endpoint pattern (e.g., 'POST /v1/tasks')")
    requests_per_second: float = Field(10.0, ge=0.1, description="Requests per second")
    requests_per_minute: int = Field(300, ge=1, description="Requests per minute")
    requests_per_hour: int = Field(10000, ge=1, description="Requests per hour")
    burst_size: int = Field(20, ge=1, description="Burst size for token bucket")
    strategy: RateLimitStrategy = Field(RateLimitStrategy.SLIDING_WINDOW, description="Rate limit strategy")


class CheckRateLimitRequest(BaseModel):
    """Request to check rate limit"""
    key: str = Field(..., description="Rate limit key (API key, user ID, IP)")
    endpoint: Optional[str] = Field(None, description="Optional endpoint")
    tokens: int = Field(1, ge=1, description="Number of tokens to consume")


class UpdateSystemLoadRequest(BaseModel):
    """Request to update system load"""
    load: float = Field(..., ge=0.0, le=1.0, description="System load (0-1)")


# Endpoints

@router.get("/statistics")
async def get_rate_limit_statistics():
    """
    Get rate limiting statistics.

    Returns:
        Overall rate limiting metrics
    """
    limiter = get_rate_limiter()
    stats = await limiter.get_statistics()
    return stats


@router.post("/check")
async def check_rate_limit(request: CheckRateLimitRequest):
    """
    Check if request would be rate limited.

    Does not actually consume tokens, just checks status.

    Args:
        request: Rate limit check parameters

    Returns:
        Rate limit status
    """
    limiter = get_rate_limiter()
    status = await limiter.check_rate_limit(
        key=request.key,
        endpoint=request.endpoint,
        tokens=request.tokens
    )

    return {
        "allowed": status.allowed,
        "limit": status.limit,
        "remaining": status.remaining,
        "reset_time": status.reset_time,
        "retry_after": status.retry_after
    }


@router.post("/endpoint")
async def set_endpoint_rate_limit(request: SetRateLimitRequest):
    """
    Set rate limit for specific endpoint.

    Use this to configure custom rate limits for different endpoints.

    Args:
        request: Rate limit configuration

    Returns:
        Confirmation
    """
    limiter = get_rate_limiter()

    config = RateLimitConfig(
        requests_per_second=request.requests_per_second,
        requests_per_minute=request.requests_per_minute,
        requests_per_hour=request.requests_per_hour,
        burst_size=request.burst_size,
        strategy=request.strategy
    )

    limiter.set_endpoint_config(request.endpoint, config)

    return {
        "endpoint": request.endpoint,
        "config": {
            "requests_per_second": config.requests_per_second,
            "requests_per_minute": config.requests_per_minute,
            "requests_per_hour": config.requests_per_hour,
            "burst_size": config.burst_size,
            "strategy": config.strategy
        },
        "updated": True
    }


@router.delete("/key/{key}")
async def reset_rate_limit_for_key(key: str):
    """
    Reset rate limits for a specific key.

    Use this to clear rate limit state for a user/API key/IP.

    Args:
        key: Rate limit key to reset

    Returns:
        Confirmation
    """
    limiter = get_rate_limiter()
    await limiter.reset_key(key)

    return {
        "key": key,
        "reset": True
    }


@router.post("/system-load")
async def update_system_load(request: UpdateSystemLoadRequest):
    """
    Update system load for adaptive throttling.

    When system load is high, rate limits are automatically reduced.

    Load levels:
    - 0.0-0.7: Normal operation (100% capacity)
    - 0.7-0.8: Light throttling (85% capacity)
    - 0.8-0.9: Moderate throttling (70% capacity)
    - 0.9-1.0: Heavy throttling (50% capacity)

    Args:
        request: System load update

    Returns:
        Current adaptive multiplier
    """
    limiter = get_rate_limiter()
    limiter.update_system_load(request.load)

    stats = await limiter.get_statistics()

    return {
        "system_load": stats["system_load"],
        "adaptive_multiplier": stats["adaptive_multiplier"],
        "effective_capacity": f"{stats['adaptive_multiplier'] * 100:.0f}%"
    }


@router.get("/health")
async def rate_limit_health():
    """
    Health check for rate limiting system.

    Returns:
        Health status
    """
    limiter = get_rate_limiter()
    stats = await limiter.get_statistics()

    return {
        "status": "healthy",
        "adaptive_enabled": stats["adaptive_enabled"],
        "system_load": stats["system_load"],
        "adaptive_multiplier": stats["adaptive_multiplier"]
    }
