"""Security and lifecycle contracts for the Substack consent flow."""

import asyncio
import hashlib
import hmac
import json
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import publish_authorization_routes as publish
from tool_management.services.publish_authorizations import get_publish_authorization_store
from tool_management.services.tenant_credentials import InMemoryTenantCredentialStore


@pytest.fixture()
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("PUBLISH_AUTHORIZATION_DB_PATH", str(tmp_path / "publish-auth.db"))
    monkeypatch.setenv("PUBLISH_AUTH_WEBHOOK_SIGNING_KEY", "test-webhook-root-key")
    monkeypatch.setenv("API_BASE_URL", "https://tools.example.test")
    tenant_store = InMemoryTenantCredentialStore()
    current = {"user_id": "tenant-a"}

    monkeypatch.setattr(
        publish,
        "resolve_user_from_request",
        lambda request: {"user_id": current["user_id"], "email": "owner@example.test"},
    )
    monkeypatch.setattr(publish, "get_tenant_store", lambda: tenant_store)

    app = FastAPI()
    app.include_router(publish.router)
    return TestClient(app), tenant_store, current


def _create(client, **overrides):
    body = {"publication": "writer.substack.com"}
    body.update(overrides)
    response = client.post("/api/v1/publish/authorizations", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def _token_from_url(url):
    return parse_qs(urlsplit(url).fragment)["token"][0]


def test_one_time_consent_stores_sid_only_in_encrypted_vault_boundary(setup):
    client, tenant_store, _ = setup
    created = _create(client)
    request_id = created["request_id"]
    token = _token_from_url(created["authorization_url"])
    sid = "substack-session-value-very-secret-123"

    assert urlsplit(created["authorization_url"]).query == ""
    assert "#token=" in created["authorization_url"]
    assert sid not in json.dumps(created)

    stored_request = get_publish_authorization_store().get(request_id)
    assert stored_request["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    assert token not in json.dumps(stored_request)

    completed = client.post(
        f"/api/v1/publish/authorizations/{request_id}/complete",
        json={
            "authorization_token": token,
            "sid": sid,
            "authorize_connection": True,
        },
    )

    assert completed.status_code == 200
    assert completed.json() == {
        "connected": True,
        "provider": "substack",
        "publication": "writer.substack.com",
        "request_id": request_id,
        "status": "authorized",
        "credential": "stored_encrypted",
    }
    assert sid not in completed.text
    assert tenant_store.get_secret("tenant-a", "substack") == sid
    assert tenant_store.get_metadata("tenant-a", "substack").label == "writer.substack.com"

    replay = client.post(
        f"/api/v1/publish/authorizations/{request_id}/complete",
        json={
            "authorization_token": token,
            "sid": sid,
            "authorize_connection": True,
        },
    )
    assert replay.status_code == 409
    assert replay.json()["detail"]["error"] == "authorization_already_used"


def test_wrong_token_cannot_claim_or_damage_pending_request(setup):
    client, tenant_store, _ = setup
    created = _create(client)
    request_id = created["request_id"]

    denied = client.post(
        f"/api/v1/publish/authorizations/{request_id}/complete",
        json={
            "authorization_token": "x" * 43,
            "sid": "not-the-real-session-value",
            "authorize_connection": True,
        },
    )
    assert denied.status_code == 403
    assert tenant_store.has_secret("tenant-a", "substack") is False
    assert get_publish_authorization_store().get(request_id)["status"] == "pending"


def test_authorization_metadata_is_tenant_isolated_and_revoke_removes_sid(setup):
    client, tenant_store, current = setup
    created = _create(client)
    request_id = created["request_id"]

    current["user_id"] = "tenant-b"
    assert client.get(f"/api/v1/publish/authorizations/{request_id}").status_code == 404
    assert client.post(f"/api/v1/publish/authorizations/{request_id}/cancel").status_code == 404

    current["user_id"] = "tenant-a"
    token = _token_from_url(created["authorization_url"])
    completed = client.post(
        f"/api/v1/publish/authorizations/{request_id}/complete",
        json={
            "authorization_token": token,
            "sid": "session-value-owned-by-tenant-a",
            "authorize_connection": True,
        },
    )
    assert completed.status_code == 200
    assert tenant_store.has_secret("tenant-a", "substack") is True

    revoked = client.delete("/api/v1/publish/connection")
    assert revoked.status_code == 200
    assert revoked.json()["disconnected"] is True
    assert tenant_store.has_secret("tenant-a", "substack") is False


def test_callback_request_queues_notification_and_returns_one_time_signing_secret(setup, monkeypatch):
    client, _, _ = setup
    scheduled = []

    async def capture_schedule(*args):
        scheduled.append(args)

    monkeypatch.setattr(publish, "validate_callback_url", lambda url: url)
    monkeypatch.setattr(publish, "deliver_authorization_webhook", capture_schedule)
    created = _create(
        client,
        notification_webhook_url="https://hooks.example.test/publish",
    )

    assert scheduled == [
        (
            created["request_id"],
            "substack.authorization.requested",
            created["authorization_url"],
        )
    ]
    webhook_secret = created["webhook_secret"]
    assert len(webhook_secret) == 64


def test_actual_webhook_payload_and_hmac_do_not_expose_credential(setup, monkeypatch):
    client, _, _ = setup
    monkeypatch.setattr(publish, "validate_callback_url", lambda url: url)

    async def no_background(*args):
        return None

    original_delivery = publish.deliver_authorization_webhook
    monkeypatch.setattr(publish, "deliver_authorization_webhook", no_background)
    created = _create(
        client,
        notification_webhook_url="https://hooks.example.test/publish",
    )
    monkeypatch.setattr(publish, "deliver_authorization_webhook", original_delivery)

    deliveries = []

    monkeypatch.setenv("PUBLISH_AUTH_WEBHOOK_BACKOFF_SECONDS", "0")
    statuses = iter((500, 204))

    class FakeResponse:
        def __init__(self):
            self.status_code = next(statuses)

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, **kwargs):
            deliveries.append((url, kwargs))
            return FakeResponse()

    monkeypatch.setattr(publish.httpx, "AsyncClient", FakeClient)
    asyncio.run(
        original_delivery(
            created["request_id"],
            "substack.authorization.requested",
            created["authorization_url"],
        )
    )

    assert len(deliveries) == 2
    url, call = deliveries[-1]
    assert url == "https://hooks.example.test/publish"
    body = call["content"]
    payload = json.loads(body)
    assert payload["event"] == "substack.authorization.requested"
    assert payload["authorization_url"] == created["authorization_url"]
    assert "sid" not in body.decode().lower()
    assert "tenant" not in body.decode().lower()

    timestamp = call["headers"]["X-DSCO-Webhook-Timestamp"]
    expected = hmac.new(
        created["webhook_secret"].encode(),
        timestamp.encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    assert call["headers"]["X-DSCO-Webhook-Signature"] == f"sha256={expected}"


def test_unsafe_callback_is_rejected_before_request_is_created(setup, monkeypatch):
    client, _, _ = setup

    def unsafe(url):
        raise ValueError("private address")

    monkeypatch.setattr(publish, "validate_callback_url", unsafe)
    response = client.post(
        "/api/v1/publish/authorizations",
        json={
            "publication": "writer.substack.com",
            "notification_webhook_url": "https://127.0.0.1/capture",
        },
    )
    assert response.status_code == 422
    assert response.json()["detail"]["error"] == "unsafe_or_unconfigured_webhook"


def test_generic_connection_api_cannot_bypass_substack_consent(monkeypatch):
    from tool_management.api import user_routes

    monkeypatch.setattr(
        user_routes,
        "_require_user",
        lambda request: {"user_id": "tenant-a"},
    )
    app = FastAPI()
    app.include_router(user_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/me/connections",
        json={"provider": "substack", "credential": "bypass-session-value"},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "substack_consent_required"
    assert response.json()["detail"]["connect_url"].endswith("/publish")
