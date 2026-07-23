"""
Rate Limiting Middleware for FastAPI

Integrates rate limiting into request processing pipeline.
"""

from fastapi import Request, Response, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Callable
import logging

from parallel_websearch_server.rate_limiting import get_rate_limiter

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware.

    Enforces rate limits on requests and adds rate limit headers.
    """

    def __init__(
        self,
        app,
        enabled: bool = True,
        key_extractor: Callable[[Request], str] = None
    ):
        """
        Args:
            app: FastAPI app
            enabled: Enable rate limiting
            key_extractor: Function to extract rate limit key from request
                          Default: uses API key or client IP
        """
        super().__init__(app)
        self.enabled = enabled
        self.key_extractor = key_extractor or self._default_key_extractor

    def _default_key_extractor(self, request: Request) -> str:
        """
        Default rate limit key extraction.

        Priority:
        1. API key (from header or query param)
        2. Client IP address
        """
        # Try API key from header
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"api_key:{api_key}"

        # Try API key from query param
        api_key = request.query_params.get("api_key")
        if api_key:
            return f"api_key:{api_key}"

        # Fall back to IP address
        client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"

    async def dispatch(self, request: Request, call_next):
        """Process request with rate limiting"""
        # Skip if disabled
        if not self.enabled:
            return await call_next(request)

        # Skip health check and docs
        if request.url.path in ["/health", "/docs", "/redoc", "/openapi.json"]:
            return await call_next(request)

        # Extract rate limit key
        key = self.key_extractor(request)
        endpoint = f"{request.method} {request.url.path}"

        # Check rate limit
        limiter = get_rate_limiter()
        status_obj = await limiter.check_rate_limit(key, endpoint)

        # Add rate limit headers
        headers = {
            "X-RateLimit-Limit": str(status_obj.limit),
            "X-RateLimit-Remaining": str(status_obj.remaining),
            "X-RateLimit-Reset": str(int(status_obj.reset_time)),
        }

        if not status_obj.allowed:
            # Rate limit exceeded
            if status_obj.retry_after:
                headers["Retry-After"] = str(int(status_obj.retry_after))

            logger.warning(
                f"Rate limit exceeded for key {key} on endpoint {endpoint}"
            )

            return Response(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content=f'{{"error": "Rate limit exceeded", "retry_after": {status_obj.retry_after}}}',
                media_type="application/json",
                headers=headers
            )

        # Process request
        response = await call_next(request)

        # Add rate limit headers to response
        for header_name, header_value in headers.items():
            response.headers[header_name] = header_value

        return response
