"""Focused contracts for the public product and tenant-aware execution path."""

import re
import time
import asyncio

import pytest
from pydantic import ValidationError
from starlette.requests import Request

from tool_management.api import tools_api
from tool_management.api.tools_api import ToolCall
from tool_management.auth.unified import AuthClaims
from tool_management.auth.service_context import verify_service_context
from tool_management.web.landing import render_landing


def _request(token: str = "sk-user-test", account_id: str | None = None) -> Request:
    headers = [(b"authorization", f"Bearer {token}".encode())]
    if account_id:
        headers.append((b"x-dsco-account-id", account_id.encode()))
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/api/v1/tools/execute",
            "raw_path": b"/api/v1/tools/execute",
            "query_string": b"",
            "headers": headers,
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


def test_signed_context_is_reserved_from_customer_arguments():
    with pytest.raises(Exception) as exc_info:
        tools_api.assert_safe_tool_arguments({"_dsco_context": {"claims": {}}})
    assert getattr(exc_info.value, "status_code", None) == 422


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


def test_canonical_principal_binds_authorized_account_not_subject(monkeypatch):
    from tool_management.auth import unified
    from tool_management.persistence import billing_store

    claims = AuthClaims(
        subject="principal-1",
        client_id="tools-client",
        email="person@example.test",
        scopes=frozenset({"tools"}),
        audience="tools",
        issuer=unified.CANONICAL_ISSUER,
        expires_at=time.time() + 300,
        issued_at=time.time(),
        token_format="zitadel_jwt",
        audiences=frozenset({"tools"}),
        account_ids=frozenset({"account-a", "account-b"}),
    )
    monkeypatch.setattr(unified, "verify_token", lambda _token: claims)
    monkeypatch.setattr(
        billing_store,
        "get_billing_store",
        lambda: type("Billing", (), {"has_active_entitlement": lambda self, account: account == "account-b"})(),
    )

    identity = tools_api.resolve_execution_identity(
        _request("canonical.jwt.token", account_id="account-b"), ["search_tools"]
    )

    assert identity["tenant_id"] == "account-b"
    assert identity["tenant_id"] != claims.subject
    assert identity["user"]["principal_id"] == "principal-1"


def test_canonical_principal_cannot_select_foreign_account(monkeypatch):
    from tool_management.auth import unified

    claims = AuthClaims(
        subject="principal-1",
        client_id="tools-client",
        email=None,
        scopes=frozenset({"tools"}),
        audience="tools",
        issuer=unified.CANONICAL_ISSUER,
        expires_at=time.time() + 300,
        issued_at=time.time(),
        token_format="zitadel_jwt",
        audiences=frozenset({"tools"}),
        account_ids=frozenset({"account-a"}),
    )
    monkeypatch.setattr(unified, "verify_token", lambda _token: claims)

    with pytest.raises(Exception) as exc_info:
        tools_api.resolve_execution_identity(
            _request("canonical.jwt.token", account_id="victim-account"), ["search_tools"]
        )
    assert getattr(exc_info.value, "status_code", None) == 403
    assert exc_info.value.detail == {"error": "account_not_authorized"}


def test_email_backend_headers_are_server_side_and_tenant_scoped(monkeypatch):
    monkeypatch.setenv("TOOLMANAGEMENT_EMAIL_BEARER_TOKEN", "service-secret")

    headers = tools_api._backend_headers("distributed-email-mcp", "owner-user")

    assert headers == {
        "Authorization": "Bearer service-secret",
        "X-DSCO-Tenant-ID": "owner-user",
    }


def test_search_worker_receives_signed_minimum_identity_context(monkeypatch):
    captured = {}

    class Store:
        def resolve_tool(self, _tool):
            return "distributed-search-mcp", "https://search.example", "answer"

        def record_call(self, *args, **kwargs):
            return None

    class Breaker:
        def allow_request(self, _backend):
            return True, {"status": "closed"}

        def record_success(self, _backend):
            return None

        def record_failure(self, *_args, **_kwargs):
            return None

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"result": {"content": [{"type": "text", "text": "{}"}]}}

    class Client:
        async def post(self, url, **kwargs):
            captured.update(url=url, **kwargs)
            return Response()

    from tool_management.services import service_index
    from tool_management.persistence import billing_store

    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "backend-secret")
    monkeypatch.setattr(tools_api, "get_tool_store", lambda: Store())
    monkeypatch.setattr(tools_api, "get_backend_circuit_breaker", lambda: Breaker())
    monkeypatch.setattr(tools_api, "get_client", lambda: Client())
    monkeypatch.setattr(service_index, "is_healthy", lambda _backend: True)
    monkeypatch.setattr(
        billing_store,
        "get_billing_store",
        lambda: type("Billing", (), {"record_usage": lambda *args, **kwargs: None})(),
    )

    call = ToolCall(tool="distributed-search-mcp:answer", trace_id="trace-1")
    call.bind_identity({
        "kind": "customer",
        "tenant_id": "account-a",
        "user": {
            "principal_id": "principal-a",
            "identity_issuer": "https://auth.distributed.systems",
        },
    })
    result = asyncio.run(tools_api.execute_tool(call))

    assert result.success is True
    arguments = captured["json"]["params"]["arguments"]
    context = verify_service_context(
        arguments["_dsco_context"],
        "backend-secret",
        audience="distributed-search-mcp",
    )
    assert context["account_id"] == "account-a"
    assert context["principal_id"] == "principal-a"
    assert context["iss"] == "https://auth.distributed.systems"
    assert context["trace_id"] == "trace-1"
    assert "authorization" not in str(arguments).lower()
