import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import mcp_routes
from tool_management.integrations.mcp_server import MCPResponse


class _FakeMCPServer:
    def __init__(self):
        self.sessions = {}
        self.calls = []

    async def handle_request(self, request, session_id):
        self.calls.append((request.method, request.params, session_id))
        if request.method == "tools/list":
            return MCPResponse(
                id=request.id,
                result={
                    "tools": [
                        {"name": "search_tools"},
                        {"name": "execute_tool"},
                        {"name": "get_analytics_overview"},
                        {"name": "record_workflow_gate_decision"},
                    ],
                    "nextCursor": "4",
                },
            )
        return MCPResponse(id=request.id, result={"ok": True})

    async def handle_notification(self, notification, session_id):
        self.calls.append((notification.method, notification.params, session_id))


def _client(monkeypatch, *, oauth_identity=None, api_key_user=None):
    fake = _FakeMCPServer()
    monkeypatch.setattr(mcp_routes, "require_bearer", lambda _request: "customer-token")
    monkeypatch.setattr(
        mcp_routes,
        "get_token_identity",
        lambda _token: oauth_identity or {},
    )
    monkeypatch.setattr(mcp_routes, "get_mcp_server", lambda: fake)

    from tool_management.api import user_routes

    monkeypatch.setattr(
        user_routes,
        "resolve_user_from_request",
        lambda _request: api_key_user,
    )
    app = FastAPI()
    app.include_router(mcp_routes.router)
    return TestClient(app), fake


def _rpc(client, method, params=None):
    return client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": "test", "method": method, "params": params or {}},
    )


def test_initialized_notification_returns_empty_202(monkeypatch):
    client, fake = _client(monkeypatch)
    response = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    assert response.status_code == 202
    assert response.content == b""
    assert fake.calls[0][0] == "notifications/initialized"


def test_customer_oauth_sees_only_tenant_safe_meta_tools(monkeypatch):
    client, fake = _client(
        monkeypatch,
        oauth_identity={"user_id": "user-1", "client_id": "claude"},
    )

    response = _rpc(client, "tools/list")

    assert response.status_code == 200
    result = response.json()["result"]
    assert [tool["name"] for tool in result["tools"]] == ["search_tools", "execute_tool"]
    assert "nextCursor" not in result
    assert fake.calls[0][2].startswith("customer:")


def test_customer_oauth_cannot_call_control_plane_meta_tools(monkeypatch):
    client, fake = _client(
        monkeypatch,
        oauth_identity={"user_id": "user-1", "client_id": "claude"},
    )

    response = _rpc(
        client,
        "tools/call",
        {"name": "get_analytics_overview", "arguments": {}},
    )

    assert response.status_code == 200
    payload = json.loads(response.json()["result"]["content"][0]["text"])
    assert response.json()["result"]["isError"] is True
    assert payload["error"] == "customer_mcp_tool_denied"
    assert fake.calls == []


def test_customer_execute_tool_uses_tenant_aware_interceptor(monkeypatch):
    client, fake = _client(
        monkeypatch,
        oauth_identity={"user_id": "user-1", "client_id": "claude"},
    )
    seen = {}

    async def _intercept(request, http_request):
        seen["name"] = (request.params or {}).get("name")
        seen["authorization"] = http_request.headers.get("authorization")
        return MCPResponse(
            id=request.id,
            result={"content": [{"type": "text", "text": '{"success":true}'}]},
        )

    monkeypatch.setattr(mcp_routes, "_handle_customer_execution", _intercept)

    response = _rpc(
        client,
        "tools/call",
        {
            "name": "execute_tool",
            "arguments": {"tool": "distributed-search-mcp:search", "arguments": {"query": "test"}},
        },
    )

    assert response.status_code == 200
    assert seen["name"] == "execute_tool"
    assert fake.calls == []


def test_sk_user_api_key_is_treated_as_customer_identity(monkeypatch):
    client, _fake = _client(
        monkeypatch,
        oauth_identity={},
        api_key_user={"user_id": "user-key-1"},
    )

    response = _rpc(
        client,
        "tools/call",
        {"name": "get_analytics_overview", "arguments": {}},
    )

    payload = json.loads(response.json()["result"]["content"][0]["text"])
    assert payload["error"] == "customer_mcp_tool_denied"
