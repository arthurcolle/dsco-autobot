"""Regression coverage for Cloud BYOK trust-boundary hardening."""

import base64
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import job_routes, tools_api
from tool_management.core.credential_envelope import (
    CredentialEnvelopeError,
    open_credential_envelope,
    seal_credential_envelope,
)
from tool_management.core.streaming import validate_callback_url
from tool_management.persistence.execution_store import ExecutionStore
from tool_management.persistence.user_store import UserStore
from native.base import backend_bearer_is_valid, install_backend_bearer_middleware


@pytest.mark.parametrize("key", [
    "tenant_id", "_tenant_credential", "_pooled_ok", "_trace_id", "_session_id",
])
def test_reserved_control_fields_are_rejected_even_when_nested(key):
    with pytest.raises(Exception) as exc:
        tools_api.assert_safe_tool_arguments({"safe": {"items": [{key: "attacker"}]}})
    assert exc.value.status_code == 422


def test_user_api_key_is_hashed_and_never_returned_after_issue(tmp_path):
    store = UserStore(db_path=str(tmp_path / "users.duckdb"))
    issued = store.create_user("owner@example.com", "long-enough-password")
    raw_key = issued["api_key"]

    assert store.get_by_api_key(raw_key)["user_id"] == issued["user_id"]
    assert "api_key" not in store.get_by_id(issued["user_id"])
    with store._connect() as con:
        stored = con.execute("SELECT api_key, api_key_hash FROM users").fetchone()
    assert raw_key not in stored
    assert stored[0] == stored[1]


def test_execution_store_tenant_filters_and_dedupe_scope(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "executions.db"))
    common = dict(
        tool_name="integration-fabric:github_list", client_id="tenant-a",
        trace_id=None, session_id=None, parent_execution_id=None, status="queued",
        progress=0, queue_priority=1, dedupe_key="tenant:tenant-a:same",
        queued_at=100.0, started_at=None, completed_at=None,
    )
    store.upsert_execution(execution_id="a", tenant_id="tenant-a", **common)
    store.upsert_execution(
        execution_id="b", tenant_id="tenant-b", **{**common, "client_id": "tenant-b", "dedupe_key": "tenant:tenant-b:same"}
    )
    assert store.get_execution("a", tenant_id="tenant-a")["execution_id"] == "a"
    assert store.get_execution("a", tenant_id="tenant-b") is None
    assert [row["execution_id"] for row in store.list_executions(tenant_id="tenant-a")] == ["a"]
    assert store.find_recent_by_dedupe_key("tenant:tenant-a:same", 0, tenant_id="tenant-a")["execution_id"] == "a"


def test_callback_validator_rejects_loopback_and_non_https():
    with pytest.raises(ValueError):
        validate_callback_url("http://example.com/hook")
    with pytest.raises(ValueError):
        validate_callback_url("https://127.0.0.1/hook")


def test_credential_envelope_is_backend_bound_and_requires_shared_key(monkeypatch):
    key = base64.urlsafe_b64encode(b"a" * 32).decode()
    monkeypatch.setenv("BYOK_ENVELOPE_KEY_INTEGRATION_FABRIC", key)
    envelope = seal_credential_envelope(
        "secret-value", tenant_id="tenant-a", provider="github", backend_id="integration-fabric"
    )
    opened = open_credential_envelope(
        envelope, expected_backend_id="integration-fabric", expected_provider="github"
    )
    assert opened == {"credential": "secret-value", "tenant_id": "tenant-a"}
    with pytest.raises(CredentialEnvelopeError):
        open_credential_envelope(
            envelope, expected_backend_id="integration-fabric", expected_provider="slack"
        )
    monkeypatch.delenv("BYOK_ENVELOPE_KEY_INTEGRATION_FABRIC")
    with pytest.raises(CredentialEnvelopeError):
        seal_credential_envelope(
            "secret-value", tenant_id="tenant-a", provider="github", backend_id="integration-fabric"
        )


def test_native_mcp_bearer_auth_fails_closed_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.delenv("BACKEND_BEARER_TOKEN", raising=False)
    assert backend_bearer_is_valid("Bearer anything") == (True, False)
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "shared-secret")
    assert backend_bearer_is_valid("Bearer wrong") == (True, False)
    assert backend_bearer_is_valid("Bearer shared-secret") == (True, True)


def test_native_mcp_middleware_protects_bespoke_execution_routes(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("NATIVE_MCP_REQUIRE_BEARER", "1")
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "shared-secret")
    app = FastAPI()
    install_backend_bearer_middleware(app)

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    @app.post("/execute")
    async def execute():
        return {"executed": True}

    client = TestClient(app)
    assert client.get("/health").status_code == 200
    assert client.post("/execute").status_code == 401
    assert client.post(
        "/execute", headers={"Authorization": "Bearer shared-secret"}
    ).json() == {"executed": True}


def test_customer_job_reads_are_tenant_scoped(monkeypatch):
    own = SimpleNamespace(execution_id="own", tenant_id="tenant-a")
    foreign = SimpleNamespace(execution_id="foreign", tenant_id="tenant-b")

    class Executor:
        def get_execution(self, job_id, tenant_id=None):
            ex = {"own": own, "foreign": foreign}.get(job_id)
            return ex if ex and (tenant_id is None or ex.tenant_id == tenant_id) else None

    monkeypatch.setattr(job_routes, "get_streaming_executor", lambda: Executor())
    monkeypatch.setattr(
        job_routes,
        "resolve_execution_identity",
        lambda _request: {"kind": "customer", "tenant_id": "tenant-a"},
    )
    app = FastAPI()

    @app.get("/probe/{job_id}")
    async def probe(job_id: str):
        return {"found": job_routes._job_execution(Executor(), job_id, {"kind": "customer", "tenant_id": "tenant-a"}) is not None}

    client = TestClient(app)
    assert client.get("/probe/own").json() == {"found": True}
    assert client.get("/probe/foreign").json() == {"found": False}
