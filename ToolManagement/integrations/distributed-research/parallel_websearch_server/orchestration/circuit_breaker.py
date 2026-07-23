"""Circuit breaker pattern implementation for resilient API calls."""

import logging
import time
from enum import Enum
from threading import Lock
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failures detected, blocking calls
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""

    def __init__(self, message: str, next_attempt_at: float):
        super().__init__(message)
        self.next_attempt_at = next_attempt_at


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    Prevents cascading failures by failing fast when a service is degraded.

    States:
    - CLOSED: Normal operation, all calls allowed
    - OPEN: Too many failures, all calls blocked
    - HALF_OPEN: Testing recovery, limited calls allowed
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_max_calls: int = 3,
        name: str = "circuit_breaker"
    ):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            half_open_max_calls: Test calls allowed in half-open state
            name: Identifier for logging
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.name = name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = Lock()

        logger.info(
            f"Circuit breaker '{name}' initialized: "
            f"threshold={failure_threshold}, timeout={recovery_timeout}s"
        )

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            return self._state

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (blocking calls)."""
        return self.state == CircuitState.OPEN

    def call(self, func: Callable[[], T]) -> T:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to execute

        Returns:
            Result from function

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: Any exception from the function
        """
        with self._lock:
            # Check if we should transition from OPEN to HALF_OPEN
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    logger.info(f"Circuit breaker '{self.name}' transitioning to HALF_OPEN")
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                else:
                    next_attempt = self._last_failure_time + self.recovery_timeout
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' is OPEN. "
                        f"Next attempt at {time.strftime('%H:%M:%S', time.localtime(next_attempt))}",
                        next_attempt
                    )

            # In HALF_OPEN, limit calls
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls >= self.half_open_max_calls:
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' in HALF_OPEN, max test calls reached",
                        time.time() + 1
                    )
                self._half_open_calls += 1

        # Execute the function
        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.recovery_timeout

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._success_count += 1

            if self._state == CircuitState.HALF_OPEN:
                logger.info(
                    f"Circuit breaker '{self.name}' HALF_OPEN success "
                    f"({self._half_open_calls}/{self.half_open_max_calls})"
                )
                # If all test calls succeed, close the circuit
                if self._half_open_calls >= self.half_open_max_calls:
                    logger.info(f"Circuit breaker '{self.name}' closing (recovery successful)")
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._half_open_calls = 0

            # In CLOSED state, reset failure count on success
            if self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in HALF_OPEN returns to OPEN
                logger.warning(
                    f"Circuit breaker '{self.name}' HALF_OPEN test failed, reopening"
                )
                self._state = CircuitState.OPEN
                self._half_open_calls = 0

            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    logger.error(
                        f"Circuit breaker '{self.name}' OPENING "
                        f"(failures: {self._failure_count}/{self.failure_threshold})"
                    )
                    self._state = CircuitState.OPEN
                else:
                    logger.warning(
                        f"Circuit breaker '{self.name}' failure "
                        f"({self._failure_count}/{self.failure_threshold})"
                    )

    def reset(self) -> None:
        """Manually reset circuit breaker to CLOSED state."""
        with self._lock:
            logger.info(f"Circuit breaker '{self.name}' manually reset")
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
            self._last_failure_time = None

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "failure_threshold": self.failure_threshold,
                "last_failure_time": self._last_failure_time,
                "recovery_timeout": self.recovery_timeout,
                "half_open_calls": self._half_open_calls if self._state == CircuitState.HALF_OPEN else None
            }
