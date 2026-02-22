"""
Backend circuit breaker for transient upstream failures.

Implements:
- Closed -> Open transition after repeated failures in a time window
- Open -> Half-open cooldown
- Single probe in half-open before closing again on success
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple


class BackendCircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        failure_window_seconds: int = 60,
        open_cooldown_seconds: int = 30,
    ):
        self.failure_threshold = max(1, int(failure_threshold))
        self.failure_window_seconds = max(1, int(failure_window_seconds))
        self.open_cooldown_seconds = max(1, int(open_cooldown_seconds))
        self._lock = threading.Lock()
        self._state: Dict[str, Dict[str, Any]] = {}

    def _backend_state(self, backend_id: str) -> Dict[str, Any]:
        state = self._state.get(backend_id)
        if state is None:
            state = {
                "status": "closed",  # closed|open|half_open
                "failures": [],
                "opened_at": None,
                "half_open_probe_inflight": False,
                "last_error": None,
                "last_failure_at": None,
                "last_success_at": None,
            }
            self._state[backend_id] = state
        return state

    def _prune_failures(self, failures: List[float], now: float):
        cutoff = now - self.failure_window_seconds
        while failures and failures[0] < cutoff:
            failures.pop(0)

    def allow_request(self, backend_id: str) -> Tuple[bool, Dict[str, Any]]:
        now = time.time()
        with self._lock:
            state = self._backend_state(backend_id)
            failures = state["failures"]
            self._prune_failures(failures, now)

            if state["status"] == "open":
                opened_at = float(state.get("opened_at") or 0.0)
                if (now - opened_at) < self.open_cooldown_seconds:
                    return False, self.get_state(backend_id)
                state["status"] = "half_open"
                state["half_open_probe_inflight"] = False

            if state["status"] == "half_open":
                if state.get("half_open_probe_inflight", False):
                    return False, self.get_state(backend_id)
                state["half_open_probe_inflight"] = True
                return True, self.get_state(backend_id)

            return True, self.get_state(backend_id)

    def record_success(self, backend_id: str):
        now = time.time()
        with self._lock:
            state = self._backend_state(backend_id)
            state["status"] = "closed"
            state["failures"] = []
            state["opened_at"] = None
            state["half_open_probe_inflight"] = False
            state["last_success_at"] = now

    def record_failure(self, backend_id: str, error: Optional[str] = None):
        now = time.time()
        with self._lock:
            state = self._backend_state(backend_id)
            failures = state["failures"]
            failures.append(now)
            self._prune_failures(failures, now)
            state["last_failure_at"] = now
            state["last_error"] = error

            should_open = state["status"] == "half_open" or len(failures) >= self.failure_threshold
            if should_open:
                state["status"] = "open"
                state["opened_at"] = now
                state["half_open_probe_inflight"] = False
            else:
                state["status"] = "closed"

    def get_state(self, backend_id: str) -> Dict[str, Any]:
        now = time.time()
        state = self._backend_state(backend_id)
        failures = list(state.get("failures", []))
        self._prune_failures(failures, now)
        opened_at = state.get("opened_at")
        retry_after_seconds = None
        if state.get("status") == "open" and opened_at:
            retry_after_seconds = max(0, int(self.open_cooldown_seconds - (now - float(opened_at))))
        return {
            "backend_id": backend_id,
            "status": state.get("status", "closed"),
            "failure_count_window": len(failures),
            "failure_threshold": self.failure_threshold,
            "failure_window_seconds": self.failure_window_seconds,
            "open_cooldown_seconds": self.open_cooldown_seconds,
            "retry_after_seconds": retry_after_seconds,
            "last_error": state.get("last_error"),
            "last_failure_at": state.get("last_failure_at"),
            "last_success_at": state.get("last_success_at"),
        }

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            backend_ids = list(self._state.keys())
        return {backend_id: self.get_state(backend_id) for backend_id in backend_ids}


_breaker: Optional[BackendCircuitBreaker] = None


def get_backend_circuit_breaker() -> BackendCircuitBreaker:
    global _breaker
    if _breaker is None:
        _breaker = BackendCircuitBreaker(
            failure_threshold=int(os.getenv("BACKEND_CIRCUIT_FAILURE_THRESHOLD", "5")),
            failure_window_seconds=int(os.getenv("BACKEND_CIRCUIT_WINDOW_SECONDS", "60")),
            open_cooldown_seconds=int(os.getenv("BACKEND_CIRCUIT_COOLDOWN_SECONDS", "30")),
        )
    return _breaker
