"""Focused contracts for the public product and tenant-aware execution path."""

import re

import pytest
from pydantic import ValidationError
from starlette.requests import Request

from tool_management.api import tools_api
from tool_management.api.tools_api import ToolCall
from tool_management.web.landing import render_landing


def _request(token: str = "sk-user-test") -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/api/v1/tools/execute",
            "raw_path": b"/api/v1/tools/execute",
            "query_string": b"",
            "headers": [(b"authorization", f"Bearer {token}".encode())],
            "client": ("127.0.0.1", 1234),
            "server": ("tools.distributed.systems", 443),
        }
    )


def test_landing_states_the_router_contract():
    page = render_landing(tool_count=2524, backend_count=10, healthy_backends=10)
    assert "We maintain a tool router here" in page
    assert "meta-router" in page
    assert "predates MCP" in page
    assert "Tool Management client" in page
    assert "<code>tool_management.client</code>" in page
    # Integration path is concrete, not marketing.
    assert "register_service()" in page
    assert "/api/v1/tools/search" in page
    assert "/api/v1/tools/execute" in page
    # Endpoints listed for developers.
    assert '<a href="/health">' in page
    assert '<a href="/docs">' in page
    assert '<a href="/api/v1/status">' in page
    # No consumer-butler remnants.
    assert "butler" not in page.lower()
    assert "data-skin" not in page
    assert "checkout-session" not in page


def test_tenant_id_is_not_accepted_from_customer_json():
    with pytest.raises(ValidationError):
        ToolCall.model_validate(
            {"tool": "search", "arguments": {}, "tenant_id": "victim-user"}
        )


def test_customer_identity_is_bound_to_every_call(monkeypatch):
    from tool_management.api import user_routes

    monkeypatch.setattr(
        tools_api,
        "resolve_execution_identity",
        lambda request, tool_names=None: {"kind": "customer", "tenant_id": "real-user"},
    )
    calls = [ToolCall(tool="one"), ToolCall(tool="two")]

    user = tools_api._bind_customer_identity(_request(), calls)

    assert user["tenant_id"] == "real-user"
    assert [call.tenant_id for call in calls] == ["real-user", "real-user"]


def test_email_backend_headers_are_server_side_and_tenant_scoped(monkeypatch):
    monkeypatch.setenv("TOOLMANAGEMENT_EMAIL_BEARER_TOKEN", "service-secret")

    headers = tools_api._backend_headers("distributed-email-mcp", "owner-user")

    assert headers == {
        "Authorization": "Bearer service-secret",
        "X-DSCO-Tenant-ID": "owner-user",
    }
