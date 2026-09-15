import asyncio

import httpx
import pytest

from tool_management.core import streaming


class _Response:
    def __init__(self, payload=None, *, content_type="application/json"):
        self.status_code = 200
        self._payload = payload or {"result": {"ok": True}}
        self.text = ""
        self.headers = {"content-type": content_type}

    def json(self):
        return self._payload

    async def aread(self):
        return self.text.encode()


class _Store:
    def __init__(self, backends):
        self.backends = backends
        self.calls = []

    def resolve_tool(self, tool_name):
        return self.backends[tool_name]

    def record_call(self, *args, **kwargs):
        self.calls.append((args, kwargs))


class _Breaker:
    def allow_request(self, _backend_id):
        return True, {}

    def record_success(self, _backend_id):
        pass

    def record_failure(self, _backend_id, error=None):
        raise AssertionError(error)


class _Client:
    def __init__(self, *, stream_response=None):
        self.get_calls = []
        self.post_calls = []
        self.stream_calls = []
        self.stream_response = stream_response

    async def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        return _Response()

    async def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        return _Response()

    def stream(self, method, url, **kwargs):
        self.stream_calls.append((method, url, kwargs))
        return self.stream_response


@pytest.fixture
def transport(monkeypatch):
    store = _Store({
        "shared:tool": ("shared-backend", "https://shared.example", "tool"),
        "image:tool": ("distributed-image-mcp", "https://image.example", "tool"),
    })
    client = _Client()
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setattr("tool_management.persistence.get_tool_store", lambda: store)
    monkeypatch.setattr(
        "tool_management.core.circuit_breaker.get_backend_circuit_breaker",
        lambda: _Breaker(),
    )
    monkeypatch.setenv("ENABLE_BACKEND_STREAMING_PATH", "0")
    return store, client, streaming.create_tool_executor_for_jobs()


@pytest.mark.asyncio
async def test_job_mcp_uses_per_backend_or_shared_server_bearer(transport, monkeypatch):
    _store, client, executor = transport
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "shared-server-token")
    monkeypatch.setenv(
        "BACKEND_DISTRIBUTED_IMAGE_MCP_BEARER_TOKEN",
        "image-server-token",
    )

    await executor(
        "shared:tool",
        {"Authorization": "caller-token", "query": "shared"},
    )
    await executor(
        "image:tool",
        {"authorization": "caller-token", "query": "image"},
    )

    mcp_calls = [call for call in client.post_calls if call[0].endswith("/mcp")]
    assert [call[1]["headers"] for call in mcp_calls] == [
        {"Authorization": "Bearer shared-server-token"},
        {"Authorization": "Bearer image-server-token"},
    ]
    assert all("caller-token" not in call[1]["headers"].values() for call in mcp_calls)


class _StreamResponse(_Response):
    def __init__(self, cancel_event):
        super().__init__(content_type="text/event-stream")
        self.cancel_event = cancel_event

    def aiter_lines(self):
        cancel_event = self.cancel_event

        async def lines():
            yield "event: meta"
            yield 'data: {"call_id":"backend-call-1"}'
            yield ""
            cancel_event.set()
            yield "event: heartbeat"

        return lines()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


@pytest.mark.asyncio
async def test_backend_sse_and_cancel_use_server_bearer(monkeypatch):
    cancel_event = asyncio.Event()
    response = _StreamResponse(cancel_event)
    client = _Client(stream_response=response)
    store = _Store({
        "image:stream": (
            "distributed-image-mcp",
            "https://image.example",
            "stream_tool",
        ),
    })
    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kwargs: client)
    monkeypatch.setattr("tool_management.persistence.get_tool_store", lambda: store)
    monkeypatch.setattr(
        "tool_management.core.circuit_breaker.get_backend_circuit_breaker",
        lambda: _Breaker(),
    )
    monkeypatch.setenv("ENABLE_BACKEND_STREAMING_PATH", "1")
    monkeypatch.setenv("BACKEND_DISTRIBUTED_IMAGE_MCP_BEARER_TOKEN", "image-server-token")

    executor = streaming.create_tool_executor_for_jobs()
    with pytest.raises(asyncio.CancelledError):
        await executor(
            "image:stream",
            {"Authorization": "caller-token"},
            cancel_event=cancel_event,
            tenant_id="tenant-1",
        )

    assert client.stream_calls[0][2]["headers"] == {
        "Authorization": "Bearer image-server-token",
        "Accept": "text/event-stream",
    }
    control_calls = [call for call in client.post_calls if call[0].endswith("/control")]
    assert control_calls
    assert control_calls[0][1]["headers"] == {
        "Authorization": "Bearer image-server-token",
    }
