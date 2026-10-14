"""Regression tests for MCP failure identity and accepted-stream replay safety.

Use the actual streaming executor with an in-memory HTTP transport. The
isolated module package keeps these checks runnable on a partial source
checkout without loading unrelated production services or credentials.
"""

import ast
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import importlib
from types import ModuleType, SimpleNamespace
from typing import Any, Dict

import httpx
import pytest


SOURCE = Path(__file__).parents[1] / "src" / "tool_management"
for name, path in {
    "_streaming_contract": SOURCE,
    "_streaming_contract.core": SOURCE / "core",
    "_streaming_contract.auth": SOURCE / "auth",
}.items():
    package = ModuleType(name)
    package.__path__ = [str(path)]
    sys.modules[name] = package
MODULE_NAME = "_streaming_contract.core.streaming"
spec = importlib.util.spec_from_file_location(MODULE_NAME, SOURCE / "core" / "streaming.py")
streaming = importlib.util.module_from_spec(spec)
sys.modules[MODULE_NAME] = streaming
spec.loader.exec_module(streaming)
REAL_CLIENT = httpx.AsyncClient


@pytest.fixture
def backend_executor(monkeypatch):
    clients = []

    def make(handler, *, replicas=False, idle_timeout=1, default_timeout=300):
        calls = []
        records = []
        breaker_records = []

        class Store:
            def resolve_tool(self, _name):
                return "example", "https://primary.example", "read"

            def record_call(self, *args, **kwargs):
                records.append((args, kwargs))

        class Breaker:
            def allow_request(self, _backend):
                return True, {}

            def record_success(self, _backend):
                breaker_records.append("success")

            def record_failure(self, _backend, **_kwargs):
                breaker_records.append("failure")

        async def dispatch(request):
            calls.append((request.method, str(request.url), json.loads(request.content) if request.content else None))
            if request.url.path == "/health":
                return httpx.Response(200, json={"status": "ok"})
            return await handler(request)

        def client(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(dispatch)
            instance = REAL_CLIENT(*args, **kwargs)
            clients.append(instance)
            return instance

        modules = {
            "_streaming_contract.api.tools_api": {"_backend_headers": lambda *_args: {}},
            "_streaming_contract.persistence": {"get_tool_store": lambda: Store()},
            "_streaming_contract.core.circuit_breaker": {"get_backend_circuit_breaker": lambda: Breaker()},
        }
        for name, attrs in modules.items():
            module = ModuleType(name)
            module.__dict__.update(attrs)
            monkeypatch.setitem(sys.modules, name, module)
        monkeypatch.setattr(httpx, "AsyncClient", client)
        monkeypatch.setenv("BACKEND_STREAM_IDLE_TIMEOUT_SECONDS", str(idle_timeout))
        monkeypatch.setenv("BACKEND_EXECUTION_TIMEOUT_SECONDS", str(default_timeout))
        monkeypatch.setenv("ENABLE_BACKEND_STREAMING_PATH", "1")
        if replicas:
            monkeypatch.setenv("BACKEND_REPLICA_URLS_EXAMPLE", "https://replica.example")
        else:
            monkeypatch.delenv("BACKEND_REPLICA_URLS_EXAMPLE", raising=False)
        executor = streaming.create_tool_executor_for_jobs()

        async def authenticated_execute(*args, **kwargs):
            kwargs.setdefault("execution_identity", {"kind": "operator_service", "tenant_id": None, "identity": {"sub": "operator-1"}})
            return await executor(*args, **kwargs)

        return authenticated_execute, calls, records, breaker_records

    yield make
    # MockTransport owns no sockets; explicitly close instances for consistency.
    for client in clients:
        asyncio.run(client.aclose())


def execution_calls(calls):
    return [call for call in calls if call[1].endswith(("/execute/stream", "/mcp"))]


@pytest.mark.asyncio
async def test_mcp_tool_error_preserves_cause_without_retrying_replica(backend_executor):
    cause = {"isError": True, "content": [{"type": "text", "text": "Provider credentials expired"}]}

    async def handler(request):
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        return httpx.Response(200, json={"jsonrpc": "2.0", "result": cause})

    execute, calls, records, breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error == {
        "code": "TOOL_EXECUTION_FAILED",
        "message": "Provider credentials expired",
        "category": "provider",
        "retryable": False,
        "backend": "example",
        "cause": cause,
    }
    assert len(execution_calls(calls)) == 2
    assert records[-1][1]["error_code"] == "TOOL_EXECUTION_FAILED"
    assert breaker == ["success"]


@pytest.mark.asyncio
async def test_sse_tool_error_is_definitive_and_preserves_cause(backend_executor):
    cause = {"code": "UPSTREAM_AUTH", "message": "Provider authentication failed"}

    async def handler(_request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=f"event: error\ndata: {json.dumps({'error': cause})}\n\n")

    execute, calls, records, breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["code"] == "TOOL_EXECUTION_FAILED"
    assert raised.value.error["cause"] == cause
    assert raised.value.error["message"] == cause["message"]
    assert len(execution_calls(calls)) == 1
    assert records[-1][1]["error_code"] == "TOOL_EXECUTION_FAILED"
    assert breaker == ["success"]


@pytest.mark.asyncio
@pytest.mark.parametrize("body", ["", 'event: chunk\ndata: {"data": "partial"}\n\n'])
async def test_truncated_stream_never_reports_success_or_replays(backend_executor, body):
    async def handler(_request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=body)

    execute, calls, records, breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["code"] == "BACKEND_STREAM_UNCERTAIN"
    assert raised.value.error["cause"]["reason"] == "missing_terminal_frame"
    assert raised.value.error["retryable"] is False
    assert len(execution_calls(calls)) == 1
    assert records[-1][1]["error_code"] == "BACKEND_STREAM_UNCERTAIN"
    assert breaker == ["failure"]


@pytest.mark.asyncio
async def test_accepted_non_sse_response_never_replays(backend_executor):
    async def handler(_request):
        return httpx.Response(200, json={"status": "accepted"})

    execute, calls, _records, _breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["cause"]["reason"] == "unexpected_content_type"
    assert len(execution_calls(calls)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["idle", "disconnect"])
async def test_accepted_stream_stall_or_disconnect_never_replays(backend_executor, failure):
    class BrokenStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'event: meta\ndata: {"call_id": "accepted-call"}\n\n'
            if failure == "idle":
                await asyncio.Event().wait()
            raise httpx.ReadError("Connection closed")

    async def handler(request):
        if request.url.path.endswith("/control"):
            return httpx.Response(200, json={"status": "stop_requested"})
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=BrokenStream())

    execute, calls, records, _breaker = backend_executor(handler, replicas=True, idle_timeout=0.005)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["code"] == "BACKEND_STREAM_UNCERTAIN"
    assert raised.value.error["cause"]["backend_call_id"] == "accepted-call"
    assert raised.value.error["cause"]["reason"] == ("idle_timeout" if failure == "idle" else "ReadError")
    assert len(execution_calls(calls)) == 1
    assert records[-1][1]["error_code"] == "BACKEND_STREAM_UNCERTAIN"


@pytest.mark.asyncio
async def test_stream_done_preserves_success(backend_executor):
    result = {"isError": False, "content": [{"type": "text", "text": "weather"}]}

    async def handler(_request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=f"event: done\ndata: {json.dumps({'result': result})}\n\n")

    execute, calls, records, breaker = backend_executor(handler)
    assert await execute("example:read", {}) == result
    assert len(execution_calls(calls)) == 1
    assert records[-1][0][2] is True
    assert breaker == ["success"]


@pytest.mark.asyncio
async def test_stream_done_mcp_error_is_not_reported_as_success(backend_executor):
    result = {"isError": True, "content": [{"type": "text", "text": "Invalid provider request"}]}

    async def handler(_request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=f"event: done\ndata: {json.dumps({'result': result})}\n\n")

    execute, calls, records, breaker = backend_executor(handler)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["code"] == "TOOL_EXECUTION_FAILED"
    assert len(execution_calls(calls)) == 1
    assert records[-1][0][2] is False
    assert breaker == ["success"]


@pytest.mark.asyncio
async def test_protocol_error_preserves_jsonrpc_error(backend_executor):
    cause = {"code": -32602, "message": "Invalid tool arguments"}

    async def handler(request):
        if request.url.path == "/execute/stream":
            return httpx.Response(405)
        return httpx.Response(200, json={"jsonrpc": "2.0", "error": cause})

    execute, calls, _records, breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["code"] == "MCP_PROTOCOL_ERROR"
    assert raised.value.error["cause"] == cause
    assert len(execution_calls(calls)) == 2
    assert breaker == ["success"]


@pytest.mark.asyncio
async def test_error_metadata_survives_execution_events(monkeypatch):
    monkeypatch.setenv("ENABLE_DURABLE_EXECUTIONS", "0")
    error = streaming.BackendExecutionError("Provider unavailable", backend="example", cause={"code": "AUTH_FAILED"})

    async def fail(*_args, **_kwargs):
        raise error

    executor = streaming.StreamingExecutor(fail)
    execution = streaming.StreamingExecution(tool_name="example:read")
    await executor._run_execution(execution, {})
    terminal = execution.events[-1]
    assert execution.status == "error"
    assert terminal.type == streaming.EventType.ERROR
    assert terminal.data == error.error
    assert terminal.to_dict()["data"]["cause"] == {"code": "AUTH_FAILED"}


def test_execution_route_error_preserves_backend_details():
    # Exercise the actual production adapter without importing every unrelated
    # route in this partial checkout.
    tree = ast.parse((SOURCE / "api" / "execution_routes.py").read_text())
    tree.body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in {"_structured_error", "_terminal_execution_error"}]
    namespace = {"Any": Any, "Dict": Dict, "Optional": __import__("typing").Optional}
    exec(compile(tree, "execution_routes.py", "exec"), namespace)
    details = {"code": "TOOL_EXECUTION_FAILED", "message": "Expired token", "cause": {"provider_code": "AUTH"}, "retryable": False}
    event = SimpleNamespace(data=details, error="Expired token", message=None)
    assert namespace["_terminal_execution_error"](event, "BACKEND_ERROR") == details


def test_collected_response_recovers_error_details_from_retained_event():
    tree = ast.parse((SOURCE / "api" / "execution_routes.py").read_text())
    tree.body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in {"_structured_error", "_terminal_execution_error", "_collected_response_from_execution"}]
    namespace = {
        "Any": Any,
        "Dict": Dict,
        "Optional": __import__("typing").Optional,
        "ExecuteResponse": SimpleNamespace,
        "EventType": streaming.EventType,
        "_TERMINAL_EVENT_TYPES": {streaming.EventType.COMPLETED, streaming.EventType.ERROR, streaming.EventType.CANCELLED},
        "_execution_latency_ms": lambda _execution: 10,
        "_execution_reuse_flags": lambda _execution: {},
    }
    exec(compile(tree, "execution_routes.py", "exec"), namespace)
    details = {"code": "TOOL_EXECUTION_FAILED", "message": "Expired token", "cause": {"provider_code": "AUTH"}, "retryable": False}
    event = streaming.ToolEvent(type=streaming.EventType.ERROR, execution_id="ex-1", data=details, error="Expired token")
    execution = SimpleNamespace(status="error", execution_id="ex-1", events=[event])
    response = namespace["_collected_response_from_execution"](
        tool_name="example:read",
        execution=execution,
        terminal_event=None,
        trace_id=None,
        session_id=None,
        parent_execution_id=None,
        policy_decision=None,
        estimated_cost_cents=None,
    )
    assert response.status == "error"
    assert response.error == details


@pytest.mark.asyncio
async def test_timeout_hint_bounds_stalled_mcp_and_cancels_inflight_call(backend_executor):
    cancelled = False

    async def handler(request):
        nonlocal cancelled
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        try:
            await asyncio.Event().wait()
        finally:
            cancelled = True

    execute, calls, records, breaker = backend_executor(handler, replicas=True)
    inputs = {"_timeout": 0.01, "query": "weather"}
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await asyncio.wait_for(execute("example:read", inputs), timeout=0.25)
    assert raised.value.error["code"] == "BACKEND_TIMEOUT"
    assert raised.value.error["cause"] == {"reason": "execution_deadline", "timeout_seconds": 0.01}
    assert raised.value.error["retryable"] is False
    assert cancelled
    assert len(execution_calls(calls)) == 2
    assert records[-1][1]["error_code"] == "BACKEND_TIMEOUT"
    assert records[-1][0][2] is False
    assert breaker == ["failure"]
    assert inputs == {"_timeout": 0.01, "query": "weather"}
    assert execution_calls(calls)[-1][2]["params"]["arguments"] == {"query": "weather"}


@pytest.mark.asyncio
async def test_total_deadline_bounds_stream_that_keeps_sending_heartbeats(backend_executor):
    class HeartbeatStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                yield b'event: heartbeat\ndata: {}\n\n'
                await asyncio.sleep(0.001)

    async def handler(_request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=HeartbeatStream())

    execute, calls, records, _breaker = backend_executor(handler, replicas=True, idle_timeout=1)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await asyncio.wait_for(execute("example:read", {"_timeout": 0.01}), timeout=0.25)
    assert raised.value.error["code"] == "BACKEND_TIMEOUT"
    assert len(execution_calls(calls)) == 1
    assert records[-1][1]["error_code"] == "BACKEND_TIMEOUT"


@pytest.mark.asyncio
async def test_configured_default_deadline_bounds_calls_without_hint(backend_executor):
    async def handler(request):
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        await asyncio.Event().wait()

    execute, _calls, _records, _breaker = backend_executor(handler, default_timeout=0.01)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await asyncio.wait_for(execute("example:read", {}), timeout=0.25)
    assert raised.value.error["cause"]["timeout_seconds"] == 0.01


@pytest.mark.asyncio
async def test_http_stream_and_mcp_timeouts_are_finite_and_follow_hint(backend_executor):
    budgets = []

    async def handler(request):
        budgets.append(request.extensions["timeout"])
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        return httpx.Response(200, json={"result": {"isError": False, "content": []}})

    execute, _calls, _records, _breaker = backend_executor(handler)
    await execute("example:read", {"_timeout": 1.25})
    assert len(budgets) == 2
    assert all(budget == {"connect": 1.25, "read": 1.25, "write": 1.25, "pool": 1.25} for budget in budgets)


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), "invalid"])
async def test_invalid_deadlines_fail_before_backend_execution(backend_executor, timeout):
    async def handler(_request):
        pytest.fail("Invalid deadline must not execute a provider call")

    execute, calls, _records, _breaker = backend_executor(handler)
    with pytest.raises(ValueError):
        await execute("example:read", {"_timeout": timeout})
    assert calls == []


@pytest.mark.asyncio
async def test_external_cancellation_remains_cancellation(backend_executor):
    entered = asyncio.Event()

    async def handler(request):
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        entered.set()
        await asyncio.Event().wait()

    execute, _calls, records, breaker = backend_executor(handler)
    task = asyncio.create_task(execute("example:read", {"_timeout": 1}))
    await asyncio.wait_for(entered.wait(), timeout=0.25)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert records == []
    assert breaker == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["read_error", "timeout", "http_500", "invalid_json"])
async def test_ambiguous_mcp_failure_never_replays_on_replica(backend_executor, failure):
    async def handler(request):
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        if failure == "read_error":
            raise httpx.ReadError("Reply connection lost")
        if failure == "timeout":
            raise httpx.ReadTimeout("Reply not received")
        if failure == "http_500":
            return httpx.Response(500, text="Execution failed")
        return httpx.Response(200, text="incomplete response")

    execute, calls, records, _breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError) as raised:
        await execute("example:read", {})
    assert raised.value.error["code"] == "BACKEND_EXECUTION_UNCERTAIN"
    assert raised.value.error["retryable"] is False
    assert len(execution_calls(calls)) == 2
    assert records[-1][1]["error_code"] == "BACKEND_EXECUTION_UNCERTAIN"


@pytest.mark.asyncio
async def test_stream_header_read_error_never_replays_on_replica(backend_executor):
    async def handler(_request):
        raise httpx.ReadError("Headers not received")

    execute, calls, _records, _breaker = backend_executor(handler, replicas=True)
    with pytest.raises(streaming.BackendExecutionError):
        await execute("example:read", {})
    assert len(execution_calls(calls)) == 1


@pytest.mark.asyncio
async def test_connect_failure_can_fail_over_before_submission(backend_executor):
    async def handler(request):
        if request.url.host == "primary.example":
            raise httpx.ConnectError("Could not connect")
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text='event: done\ndata: {"result": {"value": "ok"}}\n\n')

    execute, calls, _records, _breaker = backend_executor(handler, replicas=True)
    assert await execute("example:read", {}) == {"value": "ok"}
    assert len(execution_calls(calls)) == 2


@pytest.mark.asyncio
async def test_signed_context_is_created_for_backend_from_private_identity(backend_executor, monkeypatch):
    monkeypatch.setenv("SIGNED_CONTEXT_BACKENDS", "example")
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "test-only-signing-key")

    async def handler(request):
        if request.url.path == "/execute/stream":
            return httpx.Response(404)
        return httpx.Response(200, json={"result": {"value": "ok"}})

    execute, calls, _records, _breaker = backend_executor(handler)
    await execute("example:read", {})
    context = execution_calls(calls)[-1][2]["params"]["arguments"]["_dsco_context"]
    service_context = importlib.import_module("_streaming_contract.auth.service_context")
    claims = service_context.verify_service_context(context, "test-only-signing-key", audience="example")
    assert claims["kind"] == "operator_service"
    assert claims["principal_id"] == "operator-1"
    assert claims["account_id"] is None


@pytest.mark.asyncio
async def test_missing_identity_and_forged_context_fail_before_backend_call(backend_executor):
    async def handler(_request):
        pytest.fail("An unauthenticated/forged context must not invoke a backend")

    execute, calls, _records, _breaker = backend_executor(handler)
    with pytest.raises(Exception, match="EXECUTION_IDENTITY_REQUIRED"):
        await execute("example:read", {}, execution_identity=None)
    with pytest.raises(Exception, match="EXECUTION_CONTEXT_FORBIDDEN"):
        await execute("example:read", {"_dsco_context": "caller-forged"})
    assert calls == []


@pytest.mark.asyncio
async def test_executor_body_typeerror_does_not_repeat_side_effect(monkeypatch):
    monkeypatch.setenv("ENABLE_DURABLE_EXECUTIONS", "0")
    calls = []

    async def execute(*args, **kwargs):
        calls.append((args, kwargs))
        raise TypeError("unexpected keyword argument from provider body")

    executor = streaming.StreamingExecutor(execute)
    execution = streaming.StreamingExecution(tool_name="example:write")
    await executor._run_execution(execution, {})
    assert execution.status == "error"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_legacy_executor_signature_is_negotiated_before_call(monkeypatch):
    monkeypatch.setenv("ENABLE_DURABLE_EXECUTIONS", "0")
    calls = []

    async def execute(name, inputs, progress_callback=None):
        calls.append((name, inputs))
        return {"value": "ok"}

    executor = streaming.StreamingExecutor(execute)
    execution = streaming.StreamingExecution(tool_name="example:read")
    await executor._run_execution(execution, {})
    assert execution.status == "completed"
    assert len(calls) == 1
    authenticated = streaming.StreamingExecution(tool_name="example:read", _execution_identity={"kind": "customer", "tenant_id": "account-1"})
    await executor._run_execution(authenticated, {})
    assert authenticated.status == "error"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_queued_identity_is_minimal_durable_and_hidden_from_control_state(monkeypatch):
    class Store:
        def upsert_execution(self, **kwargs):
            self.row = kwargs

        def append_event(self, **_kwargs):
            return 1

        def get_events(self, _id):
            return []

    captured = []

    async def execute(*_args, **kwargs):
        captured.append(kwargs["execution_identity"])
        return {"value": "ok"}

    store = Store()
    executor = streaming.StreamingExecutor(execute, execution_store=store)
    monkeypatch.setattr(executor, "_ensure_scheduler", lambda: None)
    identity = {"kind": "customer", "tenant_id": "account-1", "user": {"principal_id": "principal-1", "identity_issuer": "issuer-1", "email": "not-persisted@example.com", "bearer": "not-persisted"}}
    execution = await executor.execute("example:read", {}, execution_identity=identity)
    assert execution.tenant_id == "account-1"
    assert execution._execution_identity == {"kind": "customer", "tenant_id": "account-1", "identity": {"principal_id": "principal-1", "issuer": "issuer-1"}}
    assert "_execution_identity" not in execution.control_state
    assert "not-persisted" not in json.dumps(store.row)
    hydrated = executor._hydrate_execution(execution.execution_id, row=store.row, tenant_id="account-1")
    assert hydrated._execution_identity == execution._execution_identity
    assert "_execution_identity" not in hydrated.control_state
    await executor._run_execution(hydrated, {})
    assert captured == [execution._execution_identity]
    with pytest.raises(ValueError, match="cannot be modified"):
        executor.update_control_state(execution.execution_id, {"_execution_identity": {"kind": "operator_service"}})


@pytest.mark.asyncio
async def test_customer_jobs_use_canonical_tenant_policy_without_direct_backend(backend_executor, monkeypatch):
    async def handler(_request):
        pytest.fail("Customer jobs must not bypass canonical tenant policy")

    execute, calls, _records, _breaker = backend_executor(handler)
    api = sys.modules["_streaming_contract.api.tools_api"]
    captured = []

    class ToolCall(SimpleNamespace):
        def bind_identity(self, identity):
            self.identity = identity
            self.tenant_id = identity["tenant_id"]

    async def canonical(call):
        captured.append(call)
        return SimpleNamespace(success=False, backend="distributed-email", error="ACCESS_DENIED: private mailbox", result=None)

    monkeypatch.setattr(api, "ToolCall", ToolCall, raising=False)
    monkeypatch.setattr(api, "assert_safe_tool_arguments", lambda _args: None, raising=False)
    monkeypatch.setattr(api, "execute_tool", canonical, raising=False)
    identity = {"kind": "customer", "tenant_id": "account-1", "user": {"principal_id": "principal-1"}}
    with pytest.raises(streaming.BackendExecutionError, match="ACCESS_DENIED"):
        await execute("distributed-email:read", {"query": "private", "_trace_id": "trace-1", "_session_id": "session-1"}, execution_identity=identity)
    assert calls == []
    assert len(captured) == 1
    assert captured[0].tenant_id == "account-1"
    assert captured[0].identity["kind"] == "customer"
    assert captured[0].arguments == {"query": "private"}
    assert captured[0].trace_id == "trace-1"


def execution_identity_route_namespace():
    from fastapi import HTTPException, Request
    from pydantic import BaseModel, ConfigDict, PrivateAttr
    from typing import List, Optional

    names = {"_AuthenticatedExecutionRequest", "_bind_execution_request_identity", "_submit_job", "_execute_sync", "control_call"}
    tree = ast.parse((SOURCE / "api" / "execution_routes.py").read_text())
    tree.body = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    for node in tree.body:
        node.decorator_list = []
    delegated = importlib.import_module("_streaming_contract.core.delegated_execution")
    namespace = {
        "Any": Any, "Dict": Dict, "Optional": Optional, "List": List,
        "BaseModel": BaseModel, "ConfigDict": ConfigDict, "PrivateAttr": PrivateAttr,
        "HTTPException": HTTPException, "Request": Request, "ToolResult": SimpleNamespace,
        "ControlRequest": SimpleNamespace,
        "validated_identity_snapshot": delegated.validated_identity_snapshot,
    }
    exec(compile(tree, "execution_routes.py", "exec"), namespace)
    return namespace


def test_route_identity_rejects_wire_injection_and_missing_auth():
    from fastapi import HTTPException
    from pydantic import ValidationError

    namespace = execution_identity_route_namespace()
    Model = namespace["_AuthenticatedExecutionRequest"]
    with pytest.raises(ValidationError):
        Model.model_validate({"_execution_identity": {"kind": "operator_service"}})
    with pytest.raises(HTTPException) as raised:
        namespace["_bind_execution_request_identity"](Model(), None, ["example:read"])
    assert raised.value.status_code == 401


@pytest.mark.asyncio
async def test_route_binding_preserves_customer_identity_in_sync_and_job_submission():
    namespace = execution_identity_route_namespace()
    identity = {"kind": "customer", "tenant_id": "account-1", "user": {"principal_id": "principal-1", "identity_issuer": "issuer-1", "bearer": "not-forwarded"}}
    auth_calls = []

    def resolver(request, tool_names):
        auth_calls.append((request, tool_names))
        return identity

    namespace["resolve_execution_identity"] = resolver
    request = namespace["_AuthenticatedExecutionRequest"]()
    raw_request = object()
    bound = namespace["_bind_execution_request_identity"](request, raw_request, ["example:read"])
    assert bound["kind"] == "customer"
    assert bound["tenant_id"] == "account-1"
    assert bound["identity"] == {"principal_id": "principal-1", "issuer": "issuer-1"}
    assert len(auth_calls) == 1
    assert namespace["_bind_execution_request_identity"](request, None, ["example:read"]) == bound
    assert len(auth_calls) == 1

    submissions = []

    class Executor:
        async def execute(self, **kwargs):
            submissions.append(kwargs)
            return SimpleNamespace(execution_id="job-1")

    namespace["get_streaming_executor"] = lambda: Executor()
    await namespace["_submit_job"]("example:read", {"query": "weather"}, 1000, execution_identity=bound)
    assert submissions[0]["tenant_id"] == "account-1"
    assert submissions[0]["execution_identity"] == bound
    assert "_execution_identity" not in submissions[0]["inputs"]

    sync_calls = []

    class ToolCall(SimpleNamespace):
        def bind_identity(self, value):
            self.identity = value
            self.tenant_id = value["tenant_id"]

    async def canonical(call):
        sync_calls.append(call)
        return SimpleNamespace(success=True)

    namespace["ToolCall"] = ToolCall
    namespace["execute_single_tool"] = canonical
    await namespace["_execute_sync"]("example:read", {}, 1000, None, execution_identity=bound)
    assert sync_calls[0].tenant_id == "account-1"
    assert sync_calls[0].identity == bound


@pytest.mark.asyncio
async def test_control_call_authenticates_and_filters_customer_tenant():
    from fastapi import HTTPException

    namespace = execution_identity_route_namespace()
    namespace["resolve_execution_identity"] = lambda _request: {"kind": "customer", "tenant_id": "account-1"}
    lookups = []

    class Executor:
        def get_execution(self, job_id, **kwargs):
            lookups.append((job_id, kwargs))
            return None

    namespace["get_streaming_executor"] = lambda: Executor()
    with pytest.raises(HTTPException) as raised:
        await namespace["control_call"]("other-account-job", SimpleNamespace(action="stop"), object())
    assert raised.value.status_code == 404
    assert lookups == [("other-account-job", {"tenant_id": "account-1"})]
