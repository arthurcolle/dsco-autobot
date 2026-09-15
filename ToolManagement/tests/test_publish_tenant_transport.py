"""End-to-end trust-boundary checks for tenant Substack credential handoff."""

import base64
import json

import pytest

from native.distributed_publish_mcp import server as publish_server
from tool_management.api import tools_api
from tool_management.api.tools_api import ToolCall
from tool_management.core.credential_envelope import (
    open_credential_envelope,
    seal_credential_envelope,
)
from tool_management.integrations.provider_catalog import provider_for_tool
from tool_management.services.tenant_credentials import InMemoryTenantCredentialStore


@pytest.mark.asyncio
async def test_registry_seals_tenant_sid_and_publication_for_publish_worker(monkeypatch):
    key = base64.urlsafe_b64encode(b"p" * 32).decode()
    monkeypatch.setenv("BYOK_ENVELOPE_KEY_DISTRIBUTED_PUBLISH_MCP", key)
    tenant_store = InMemoryTenantCredentialStore()
    tenant_store.set_secret(
        "tenant-a",
        "substack",
        "tenant-a-secret-session",
        label="writer.substack.com",
    )

    class ToolStore:
        def resolve_tool(self, tool):
            return (
                "distributed-publish-mcp",
                "https://publish.example.test",
                "substack_connection_status",
            )

        def record_call(self, *args, **kwargs):
            return None

    class Breaker:
        def allow_request(self, backend_id):
            return True, {"status": "closed"}

        def record_success(self, backend_id):
            return None

        def record_failure(self, *args, **kwargs):
            return None

    captured = {}

    async def fake_mcp_call(url, backend_id, tool_name, arguments, timeout, tenant_id=None):
        captured.update(
            backend_id=backend_id,
            tool_name=tool_name,
            arguments=arguments,
            tenant_id=tenant_id,
        )
        return 200, {"result": {"content": [{"type": "text", "text": "{}"}]}}

    from tool_management.services import service_index, tenant_credentials
    from tool_management.persistence import billing_store

    monkeypatch.setattr(tools_api, "get_tool_store", lambda: ToolStore())
    monkeypatch.setattr(tools_api, "get_backend_circuit_breaker", lambda: Breaker())
    monkeypatch.setattr(tools_api, "_mcp_tools_call", fake_mcp_call)
    monkeypatch.setattr(service_index, "is_healthy", lambda backend_id: True)
    monkeypatch.setattr(tenant_credentials, "_default_store", lambda: tenant_store)
    monkeypatch.setattr(
        billing_store,
        "get_billing_store",
        lambda: type("Billing", (), {"record_usage": lambda *a, **k: None})(),
    )

    call = ToolCall(tool="distributed-publish-mcp:substack_connection_status")
    call.bind_tenant("tenant-a")
    result = await tools_api.execute_tool(call)

    assert result.success is True
    assert provider_for_tool("substack_connection_status") == "substack"
    assert captured["backend_id"] == "distributed-publish-mcp"
    assert captured["tenant_id"] == "tenant-a"
    envelope = captured["arguments"]["_credential_envelope"]
    assert "tenant-a-secret-session" not in envelope
    opened = open_credential_envelope(
        envelope,
        expected_backend_id="distributed-publish-mcp",
        expected_provider="substack",
    )
    assert opened["tenant_id"] == "tenant-a"
    assert json.loads(opened["credential"]) == {
        "sid": "tenant-a-secret-session",
        "publication": "writer.substack.com",
    }


@pytest.mark.asyncio
async def test_registry_fails_closed_before_worker_when_tenant_has_no_sid(monkeypatch):
    class ToolStore:
        def resolve_tool(self, tool):
            return (
                "distributed-publish-mcp",
                "https://publish.example.test",
                "substack_create_draft",
            )

    class Breaker:
        def allow_request(self, backend_id):
            return True, {"status": "closed"}

    called = False

    async def should_not_call(*args, **kwargs):
        nonlocal called
        called = True
        return 200, {}

    from tool_management.services import service_index, tenant_credentials

    monkeypatch.setattr(tools_api, "get_tool_store", lambda: ToolStore())
    monkeypatch.setattr(tools_api, "get_backend_circuit_breaker", lambda: Breaker())
    monkeypatch.setattr(tools_api, "_mcp_tools_call", should_not_call)
    monkeypatch.setattr(service_index, "is_healthy", lambda backend_id: True)
    monkeypatch.setattr(
        tenant_credentials,
        "_default_store",
        lambda: InMemoryTenantCredentialStore(),
    )

    call = ToolCall(
        tool="distributed-publish-mcp:substack_create_draft",
        arguments={"title": "Should never leave the registry"},
    )
    call.bind_tenant("tenant-without-credential")
    result = await tools_api.execute_tool(call)

    assert result.success is False
    assert result.error == "SUBSTACK_AUTHORIZATION_REQUIRED: connect at /publish"
    assert called is False


@pytest.mark.asyncio
async def test_worker_uses_context_isolated_client_then_destroys_it(monkeypatch):
    key = base64.urlsafe_b64encode(b"w" * 32).decode()
    monkeypatch.setenv("BYOK_ENVELOPE_KEY_DISTRIBUTED_PUBLISH_MCP", key)
    monkeypatch.delenv("SUBSTACK_SID", raising=False)
    monkeypatch.delenv("SUBSTACK_PUBLICATION", raising=False)
    instances = []

    class RequestClient:
        def __init__(self, sid, publication, **kwargs):
            self.sid = sid
            self.publication = publication
            self.closed = False
            instances.append(self)

        def close(self):
            self.closed = True

    monkeypatch.setattr(publish_server, "SubstackClient", RequestClient)
    server = publish_server.DistributedPublishMCP(port=0)
    envelope = seal_credential_envelope(
        json.dumps(
            {"sid": "tenant-request-session", "publication": "writer.substack.com"},
            separators=(",", ":"),
        ),
        tenant_id="tenant-a",
        provider="substack",
        backend_id="distributed-publish-mcp",
    )

    result = await server._invoke_tool(
        "substack_connection_status",
        {"_credential_envelope": envelope},
    )

    assert result["configured"] is True
    assert result["publication"] == "writer.substack.com"
    assert result["credential_scope"] == "tenant_request"
    assert "tenant-request-session" not in json.dumps(result)
    assert instances[0].closed is True
    assert server._substack_client is None
    assert publish_server._request_substack_client.get() is None

    after = await server._execute_substack_connection_status({})
    assert after["configured"] is False
    assert after["publication"] is None


@pytest.mark.asyncio
async def test_worker_rejects_raw_tenant_selectors(monkeypatch):
    server = publish_server.DistributedPublishMCP(port=0)
    with pytest.raises(ValueError, match="raw tenant credentials"):
        await server._invoke_tool(
            "substack_connection_status",
            {"tenant_id": "victim", "_tenant_credential": "stolen"},
        )
