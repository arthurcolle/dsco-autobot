from tool_management.core.circuit_breaker import BackendCircuitBreaker


def test_circuit_breaker_opens_after_threshold(monkeypatch):
    clock = {"now": 1000.0}
    monkeypatch.setattr("tool_management.core.circuit_breaker.time.time", lambda: clock["now"])

    breaker = BackendCircuitBreaker(
        failure_threshold=2,
        failure_window_seconds=60,
        open_cooldown_seconds=10,
    )

    allowed, _ = breaker.allow_request("backend-a")
    assert allowed is True

    breaker.record_failure("backend-a", "timeout")
    allowed, _ = breaker.allow_request("backend-a")
    assert allowed is True

    breaker.record_failure("backend-a", "http 500")
    allowed, state = breaker.allow_request("backend-a")
    assert allowed is False
    assert state["status"] == "open"
    assert state["retry_after_seconds"] == 10


def test_circuit_breaker_half_open_probe_and_reopen(monkeypatch):
    clock = {"now": 2000.0}
    monkeypatch.setattr("tool_management.core.circuit_breaker.time.time", lambda: clock["now"])

    breaker = BackendCircuitBreaker(
        failure_threshold=1,
        failure_window_seconds=60,
        open_cooldown_seconds=5,
    )

    breaker.record_failure("backend-b", "timeout")

    allowed, state = breaker.allow_request("backend-b")
    assert allowed is False
    assert state["status"] == "open"

    clock["now"] += 6
    allowed, state = breaker.allow_request("backend-b")
    assert allowed is True
    assert state["status"] == "half_open"

    # Only one probe call is allowed in half-open state.
    allowed, _ = breaker.allow_request("backend-b")
    assert allowed is False

    breaker.record_failure("backend-b", "probe failed")
    allowed, state = breaker.allow_request("backend-b")
    assert allowed is False
    assert state["status"] == "open"


def test_circuit_breaker_success_closes(monkeypatch):
    clock = {"now": 3000.0}
    monkeypatch.setattr("tool_management.core.circuit_breaker.time.time", lambda: clock["now"])

    breaker = BackendCircuitBreaker(
        failure_threshold=1,
        failure_window_seconds=60,
        open_cooldown_seconds=5,
    )

    breaker.record_failure("backend-c", "timeout")
    clock["now"] += 6

    allowed, _ = breaker.allow_request("backend-c")
    assert allowed is True

    breaker.record_success("backend-c")
    state = breaker.get_state("backend-c")
    assert state["status"] == "closed"
    assert state["failure_count_window"] == 0
    assert state["last_success_at"] == clock["now"]
