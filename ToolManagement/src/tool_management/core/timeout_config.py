"""
Timeout Configuration Management.

Provides hierarchical timeout configuration for different execution contexts
(synchronous, asynchronous, background jobs) with inheritance and validation.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class TimeoutConfig:
    """
    Hierarchical timeout configuration.

    Supports cascading timeouts at different levels:
    - HTTP level: connect, read, write, pool
    - Application level: task execution, background jobs
    """

    # HTTP-level timeouts (seconds)
    connect: float = 10.0
    """Timeout for establishing connection"""

    read: float = 30.0
    """Timeout for reading response"""

    write: float = 30.0
    """Timeout for writing request"""

    pool: float = 10.0
    """Timeout for acquiring connection from pool"""

    # Application-level timeouts (seconds)
    task_timeout: float = 30.0
    """Default timeout for synchronous task execution"""

    background_task_timeout: float = 3600.0
    """Default timeout for background/async jobs (1 hour)"""

    max_timeout: float = 7200.0
    """Maximum allowed timeout (2 hours)"""

    @classmethod
    def from_env(cls) -> "TimeoutConfig":
        """Create configuration from environment variables."""
        return cls(
            connect=float(os.getenv('HTTP_CONNECT_TIMEOUT', '10.0')),
            read=float(os.getenv('HTTP_READ_TIMEOUT', '30.0')),
            write=float(os.getenv('HTTP_WRITE_TIMEOUT', '30.0')),
            pool=float(os.getenv('HTTP_POOL_TIMEOUT', '10.0')),
            task_timeout=float(os.getenv('TASK_DEFAULT_TIMEOUT', '30.0')),
            background_task_timeout=float(os.getenv('TASK_BACKGROUND_TIMEOUT', '3600.0')),
            max_timeout=float(os.getenv('TASK_MAX_TIMEOUT', '7200.0'))
        )

    def for_background_job(self) -> "TimeoutConfig":
        """
        Create timeout configuration for background jobs.

        Returns a new config with extended read timeout for long-running operations.
        """
        return TimeoutConfig(
            connect=self.connect,
            read=self.background_task_timeout,
            write=self.write,
            pool=self.pool,
            task_timeout=self.background_task_timeout,
            background_task_timeout=self.background_task_timeout,
            max_timeout=self.max_timeout
        )

    def for_sync_execution(self) -> "TimeoutConfig":
        """
        Create timeout configuration for synchronous execution.

        Returns a new config with standard timeouts for quick operations.
        """
        return TimeoutConfig(
            connect=self.connect,
            read=self.task_timeout,
            write=self.write,
            pool=self.pool,
            task_timeout=self.task_timeout,
            background_task_timeout=self.background_task_timeout,
            max_timeout=self.max_timeout
        )

    def with_timeout(self, timeout_seconds: float) -> "TimeoutConfig":
        """
        Create timeout configuration with custom timeout.

        Args:
            timeout_seconds: Custom timeout value

        Returns:
            New TimeoutConfig with updated read and task timeouts

        Raises:
            ValueError: If timeout exceeds max_timeout
        """
        if timeout_seconds > self.max_timeout:
            raise ValueError(
                f"Timeout {timeout_seconds}s exceeds maximum {self.max_timeout}s"
            )

        return TimeoutConfig(
            connect=self.connect,
            read=timeout_seconds,
            write=self.write,
            pool=self.pool,
            task_timeout=timeout_seconds,
            background_task_timeout=self.background_task_timeout,
            max_timeout=self.max_timeout
        )

    def with_buffer(self, buffer_seconds: float = 5.0) -> "TimeoutConfig":
        """
        Create timeout configuration with reduced timeout (buffer for upstream).

        Useful for creating child timeouts that complete before parent timeout.

        Args:
            buffer_seconds: Time to subtract from read/task timeout

        Returns:
            New TimeoutConfig with reduced timeouts
        """
        new_read = max(10.0, self.read - buffer_seconds)
        new_task = max(10.0, self.task_timeout - buffer_seconds)

        return TimeoutConfig(
            connect=self.connect,
            read=new_read,
            write=self.write,
            pool=self.pool,
            task_timeout=new_task,
            background_task_timeout=self.background_task_timeout,
            max_timeout=self.max_timeout
        )

    def validate(self) -> bool:
        """
        Validate timeout configuration.

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If any timeout is invalid
        """
        if self.connect <= 0:
            raise ValueError(f"Connect timeout must be positive: {self.connect}")
        if self.read <= 0:
            raise ValueError(f"Read timeout must be positive: {self.read}")
        if self.write <= 0:
            raise ValueError(f"Write timeout must be positive: {self.write}")
        if self.pool <= 0:
            raise ValueError(f"Pool timeout must be positive: {self.pool}")
        if self.task_timeout <= 0:
            raise ValueError(f"Task timeout must be positive: {self.task_timeout}")
        if self.background_task_timeout <= 0:
            raise ValueError(f"Background timeout must be positive: {self.background_task_timeout}")
        if self.task_timeout > self.max_timeout:
            raise ValueError(f"Task timeout {self.task_timeout} exceeds max {self.max_timeout}")

        return True


# Global default configuration
_default_config: Optional[TimeoutConfig] = None


def get_default_timeout_config() -> TimeoutConfig:
    """Get or create the default timeout configuration."""
    global _default_config
    if _default_config is None:
        _default_config = TimeoutConfig.from_env()
        _default_config.validate()
    return _default_config


def set_default_timeout_config(config: TimeoutConfig):
    """Set the global default timeout configuration."""
    global _default_config
    config.validate()
    _default_config = config
