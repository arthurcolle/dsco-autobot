"""
Centralized HTTP Client Pool Manager.

Provides shared HTTP client pools with proper connection management,
lifecycle control, and metrics for different use cases.
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional
from contextlib import asynccontextmanager

import httpx

logger = logging.getLogger(__name__)


@dataclass
class PoolConfig:
    """Configuration for an HTTP connection pool."""
    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_expiry: float = 30.0
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    write_timeout: float = 30.0
    pool_timeout: float = 10.0

    def to_httpx_limits(self) -> httpx.Limits:
        """Convert to httpx Limits object."""
        return httpx.Limits(
            max_connections=self.max_connections,
            max_keepalive_connections=self.max_keepalive_connections,
            keepalive_expiry=self.keepalive_expiry
        )

    def to_httpx_timeout(self) -> httpx.Timeout:
        """Convert to httpx Timeout object."""
        return httpx.Timeout(
            connect=self.connect_timeout,
            read=self.read_timeout,
            write=self.write_timeout,
            pool=self.pool_timeout
        )


class HTTPClientPool:
    """
    Centralized HTTP client pool manager.

    Manages multiple named pools (default, background, mcp) with different
    connection limits and timeout configurations. Ensures proper lifecycle
    management and connection reuse.

    Usage:
        pool = HTTPClientPool()
        await pool.startup()

        async with pool.get_client('default') as client:
            response = await client.get('https://example.com')

        await pool.shutdown()
    """

    def __init__(self):
        self._pools: Dict[str, httpx.AsyncClient] = {}
        self._pool_configs: Dict[str, PoolConfig] = {}
        self._started = False
        self._lock = asyncio.Lock()

        # Initialize pool configurations
        self._init_pool_configs()

    def _init_pool_configs(self):
        """Initialize pool configurations from environment or defaults."""

        # Default pool: General purpose requests
        self._pool_configs['default'] = PoolConfig(
            max_connections=int(os.getenv('HTTP_POOL_MAX_CONNECTIONS', '100')),
            max_keepalive_connections=int(os.getenv('HTTP_POOL_MAX_KEEPALIVE', '20')),
            keepalive_expiry=float(os.getenv('HTTP_POOL_KEEPALIVE_EXPIRY', '30.0')),
            connect_timeout=float(os.getenv('HTTP_CONNECT_TIMEOUT', '10.0')),
            read_timeout=float(os.getenv('HTTP_READ_TIMEOUT', '30.0')),
            write_timeout=float(os.getenv('HTTP_WRITE_TIMEOUT', '30.0')),
            pool_timeout=float(os.getenv('HTTP_POOL_TIMEOUT', '10.0'))
        )

        # Background pool: Long-running tasks
        self._pool_configs['background'] = PoolConfig(
            max_connections=int(os.getenv('BACKGROUND_POOL_MAX_CONNECTIONS', '50')),
            max_keepalive_connections=int(os.getenv('BACKGROUND_POOL_MAX_KEEPALIVE', '10')),
            keepalive_expiry=float(os.getenv('BACKGROUND_POOL_KEEPALIVE_EXPIRY', '300.0')),
            connect_timeout=float(os.getenv('HTTP_CONNECT_TIMEOUT', '10.0')),
            read_timeout=float(os.getenv('BACKGROUND_READ_TIMEOUT', '3600.0')),  # 1 hour
            write_timeout=float(os.getenv('HTTP_WRITE_TIMEOUT', '30.0')),
            pool_timeout=float(os.getenv('HTTP_POOL_TIMEOUT', '10.0'))
        )

        # MCP pool: Multiple MCP server requests
        self._pool_configs['mcp'] = PoolConfig(
            max_connections=int(os.getenv('MCP_POOL_MAX_CONNECTIONS', '200')),
            max_keepalive_connections=int(os.getenv('MCP_POOL_MAX_KEEPALIVE', '40')),
            keepalive_expiry=float(os.getenv('MCP_POOL_KEEPALIVE_EXPIRY', '60.0')),
            connect_timeout=float(os.getenv('HTTP_CONNECT_TIMEOUT', '10.0')),
            read_timeout=float(os.getenv('MCP_READ_TIMEOUT', '300.0')),  # 5 minutes
            write_timeout=float(os.getenv('HTTP_WRITE_TIMEOUT', '30.0')),
            pool_timeout=float(os.getenv('HTTP_POOL_TIMEOUT', '10.0'))
        )

    async def startup(self):
        """Initialize all HTTP client pools."""
        async with self._lock:
            if self._started:
                logger.warning("HTTP client pool already started")
                return

            for pool_name, config in self._pool_configs.items():
                logger.info(f"Initializing {pool_name} HTTP pool: "
                           f"max_conn={config.max_connections}, "
                           f"keepalive={config.max_keepalive_connections}, "
                           f"read_timeout={config.read_timeout}s")

                self._pools[pool_name] = httpx.AsyncClient(
                    limits=config.to_httpx_limits(),
                    timeout=config.to_httpx_timeout(),
                    follow_redirects=True
                )

            self._started = True
            logger.info(f"HTTP client pool started with {len(self._pools)} pools")

    async def shutdown(self):
        """Close all HTTP client pools gracefully."""
        async with self._lock:
            if not self._started:
                logger.warning("HTTP client pool not started")
                return

            logger.info("Shutting down HTTP client pools...")

            for pool_name, client in self._pools.items():
                try:
                    await client.aclose()
                    logger.info(f"Closed {pool_name} pool")
                except Exception as e:
                    logger.error(f"Error closing {pool_name} pool: {e}")

            self._pools.clear()
            self._started = False
            logger.info("HTTP client pool shutdown complete")

    @asynccontextmanager
    async def get_client(self, pool_name: str = 'default'):
        """
        Get a client from a specific pool.

        Args:
            pool_name: Pool name ('default', 'background', 'mcp')

        Yields:
            httpx.AsyncClient: Client from the specified pool

        Raises:
            ValueError: If pool not initialized or invalid pool name
        """
        if not self._started:
            raise ValueError("HTTP client pool not started. Call startup() first.")

        if pool_name not in self._pools:
            raise ValueError(f"Invalid pool name: {pool_name}. "
                           f"Available pools: {list(self._pools.keys())}")

        # Yield the shared client (no context manager needed since it's persistent)
        yield self._pools[pool_name]

    def get_client_sync(self, pool_name: str = 'default') -> httpx.AsyncClient:
        """
        Get a client from a pool synchronously (without context manager).

        Use this when you need to pass the client to other functions.
        The client should NOT be closed - it's managed by the pool.

        Args:
            pool_name: Pool name ('default', 'background', 'mcp')

        Returns:
            httpx.AsyncClient: Client from the specified pool

        Raises:
            ValueError: If pool not initialized or invalid pool name
        """
        if not self._started:
            raise ValueError("HTTP client pool not started. Call startup() first.")

        if pool_name not in self._pools:
            raise ValueError(f"Invalid pool name: {pool_name}. "
                           f"Available pools: {list(self._pools.keys())}")

        return self._pools[pool_name]

    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get metrics for all pools.

        Returns:
            Dict mapping pool name to metrics:
            {
                'default': {
                    'max_connections': 100,
                    'max_keepalive': 20,
                    'is_closed': False
                },
                ...
            }
        """
        metrics = {}

        for pool_name, client in self._pools.items():
            config = self._pool_configs[pool_name]
            metrics[pool_name] = {
                'max_connections': config.max_connections,
                'max_keepalive_connections': config.max_keepalive_connections,
                'keepalive_expiry': config.keepalive_expiry,
                'connect_timeout': config.connect_timeout,
                'read_timeout': config.read_timeout,
                'is_closed': client.is_closed
            }

        return metrics


# Global instance
_http_pool: Optional[HTTPClientPool] = None


def get_http_pool() -> HTTPClientPool:
    """Get or create the global HTTP client pool instance."""
    global _http_pool
    if _http_pool is None:
        _http_pool = HTTPClientPool()
    return _http_pool
