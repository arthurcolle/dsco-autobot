import hashlib
import hmac
import json
import time

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from tool_management.api import billing_routes
from tool_management.persistence.billing_store import BillingStore


def _request(*, body: bytes = b"", headers=None, path: str = "/") -> Request:
    raw_headers = [
        (str(key).lower().encode("latin-1"), str(value).encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": path,
            "raw_path": path.encode("ascii"),
            "query_string": b"",
            "headers": raw_headers,
            "client": ("127.0.0.1", 1234),
            "server": ("tools.distributed.systems", 443),
        },
        receive,
    )


def _sign(payload: bytes, secret: str, timestamp: int) -> str:
    signature = hmac.new(
        secret.encode("utf-8"),
        str(timestamp).encode("ascii") + b"." + payload,
        hashlib.sha256,
    ).hexdigest()
    return f"t={timestamp},v1={signature}"


@pytest.fixture
def store(tmp_path):
    return BillingStore(str(tmp_path / "billing.duckdb"))


def test_entitlement_requires_verified_status_and_future_period(store):
    store.upsert_subscription(
        "user-1",
        provider="stripe",
        provider_subscription_id="sub_1",
        status="active",
        access_until=None,
    )
    assert store.has_paid_entitlement("user-1", now=100) is False

    store.upsert_subscription("user-1", access_until=200)
    assert store.has_paid_entitlement("user-1", now=100) is True
    assert store.has_paid_entitlement("user-1", now=201) is False

    store.upsert_subscription("user-1", status="past_due")
    assert store.has_paid_entitlement("user-1", now=100) is False


def test_dev_subscription_is_honored_only_when_explicitly_allowed(store):
    store.upsert_subscription(
        "dev-user",
        provider="dev",
        provider_subscription_id="dev_sub",
        status="active",
        access_until=None,
    )
    assert store.has_paid_entitlement("dev-user", allow_dev=False) is False
    assert store.has_paid_entitlement("dev-user", allow_dev=True) is True


def test_dev_entitlement_does_not_erase_stripe_customer(monkeypatch, store):
    store.upsert_customer(
        "mixed-user",
        provider="stripe",
        provider_customer_id="cus_keep_me",
        dev_mode=False,
    )
    monkeypatch.setenv("BILLING_DEV_MODE", "1")
    billing_routes._ensure_dev_entitlement("mixed-user", store)
    customer = store.get_customer("mixed-user")
    assert customer["provider"] == "stripe"
    assert customer["provider_customer_id"] == "cus_keep_me"


def test_verified_events_are_durable_idempotent_and_order_safe(store):
    checkout = {
        "id": "evt_checkout",
        "type": "checkout.session.completed",
        "created": 200,
        "livemode": False,
        "data": {
            "object": {
                "id": "cs_1",
                "object": "checkout.session",
                "mode": "subscription",
                "customer": "cus_1",
                "subscription": "sub_1",
                "payment_status": "paid",
                "client_reference_id": "user-1",
                "metadata": {"user_id": "user-1", "price_id": "price_server"},
            }
        },
    }
    first = store.apply_stripe_event(checkout, payload_hash="hash-1")
    assert first["processed"] is True
    assert first["applied"] is True
    assert store.get_subscription("user-1")["status"] == "active"
    # Checkout alone has no period boundary, so it cannot grant durable access.
    assert store.has_paid_entitlement("user-1", now=250) is False

    # This event was created first but delivered later. It must extend access
    # without rolling back the newer Checkout-derived status.
    paid = {
        "id": "evt_paid",
        "type": "invoice.paid",
        "created": 199,
        "livemode": False,
        "data": {
            "object": {
                "id": "in_1",
                "object": "invoice",
                "customer": "cus_1",
                "subscription": "sub_1",
                "lines": {
                    "data": [
                        {
                            "price": {"id": "price_server"},
                            "period": {"end": 500},
                        }
                    ]
                },
            }
        },
    }
    applied = store.apply_stripe_event(paid, payload_hash="hash-2")
    assert applied["stale"] is True
    assert applied["applied"] is True
    assert store.has_paid_entitlement("user-1", now=250) is True

    duplicate = store.apply_stripe_event(paid, payload_hash="hash-2")
    assert duplicate == {
        "processed": False,
        "duplicate": True,
        "event_id": "evt_paid",
    }

    canceled = {
        "id": "evt_cancel",
        "type": "customer.subscription.deleted",
        "created": 300,
        "livemode": False,
        "data": {
            "object": {
                "id": "sub_1",
                "object": "subscription",
                "customer": "cus_1",
                "status": "canceled",
                "items": {
                    "data": [
                        {"price": {"id": "price_server"}, "current_period_end": 500}
                    ]
                },
            }
        },
    }
    store.apply_stripe_event(canceled, payload_hash="hash-3")
    assert store.get_subscription("user-1")["status"] == "canceled"
    assert store.has_paid_entitlement("user-1", now=250) is False


def test_unowned_out_of_order_invoice_remains_retryable(store):
    invoice = {
        "id": "evt_invoice_first",
        "type": "invoice.paid",
        "created": 100,
        "livemode": False,
        "data": {
            "object": {
                "id": "in_first",
                "object": "invoice",
                "customer": "cus_first",
                "subscription": "sub_first",
                "lines": {"data": [{"period": {"end": 500}}]},
            }
        },
    }
    with pytest.raises(ValueError, match="owner"):
        store.apply_stripe_event(invoice)

    checkout = {
        "id": "evt_checkout_second",
        "type": "checkout.session.completed",
        "created": 101,
        "livemode": False,
        "data": {
            "object": {
                "id": "cs_second",
                "object": "checkout.session",
                "mode": "subscription",
                "customer": "cus_first",
                "subscription": "sub_first",
                "payment_status": "paid",
                "client_reference_id": "user-first",
                "metadata": {"user_id": "user-first", "price_id": "price_server"},
            }
        },
    }
    store.apply_stripe_event(checkout)
    replay = store.apply_stripe_event(invoice)
    assert replay["applied"] is True
    assert store.has_paid_entitlement("user-first", now=200) is True


def test_signature_verification_uses_raw_body_and_timestamp():
    payload = b'{"id":"evt_1","data":{"raw":"spacing matters"}}'
    secret = "whsec_test"
    header = _sign(payload, secret, 1_000)
    billing_routes._verify_stripe_signature(payload, header, secret, now=1_100)

    with pytest.raises(ValueError, match="signature"):
        billing_routes._verify_stripe_signature(
            payload + b" ", header, secret, now=1_100
        )
    with pytest.raises(ValueError, match="timestamp"):
        billing_routes._verify_stripe_signature(
            payload, header, secret, now=2_000
        )


@pytest.mark.asyncio
async def test_webhook_verifies_then_deduplicates(monkeypatch, store):
    monkeypatch.setattr(billing_routes, "get_billing_store", lambda: store)
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_test")
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_fake")
    event = {
        "id": "evt_webhook",
        "type": "checkout.session.completed",
        "created": int(time.time()),
        "livemode": False,
        "data": {
            "object": {
                "id": "cs_webhook",
                "object": "checkout.session",
                "mode": "subscription",
                "customer": "cus_webhook",
                "subscription": "sub_webhook",
                "payment_status": "paid",
                "client_reference_id": "user-webhook",
                "metadata": {"user_id": "user-webhook", "price_id": "price_server"},
            }
        },
    }
    payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
    signature = _sign(payload, "whsec_test", event["created"])

    result = await billing_routes.stripe_webhook(
        _request(body=payload, headers={"stripe-signature": signature})
    )
    assert result["received"] is True
    assert result["applied"] is True

    repeated = await billing_routes.stripe_webhook(
        _request(body=payload, headers={"stripe-signature": signature})
    )
    assert repeated["duplicate"] is True

    with pytest.raises(HTTPException) as rejected:
        await billing_routes.stripe_webhook(
            _request(body=payload, headers={"stripe-signature": "t=1,v1=bad"})
        )
    assert rejected.value.status_code == 400


@pytest.mark.asyncio
async def test_checkout_uses_only_server_price_and_stable_idempotency(
    monkeypatch, store
):
    monkeypatch.setattr(billing_routes, "get_billing_store", lambda: store)
    monkeypatch.setattr(
        billing_routes,
        "_require_user",
        lambda request: {
            "user_id": "user-checkout",
            "email": "buyer@example.com",
            "display_name": "Buyer",
        },
    )
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_server")
    monkeypatch.setenv("API_BASE_URL", "https://tools.distributed.systems")
    store.upsert_customer(
        "user-checkout",
        provider="stripe",
        provider_customer_id="cus_checkout",
        dev_mode=False,
    )
    calls = []

    async def fake_stripe(method, path, data=None, *, idempotency_key=None):
        calls.append((method, path, data, idempotency_key))
        return {"id": "cs_checkout", "url": "https://checkout.stripe.test/session"}

    monkeypatch.setattr(billing_routes, "_stripe", fake_stripe)
    request = _request(headers={"idempotency-key": "browser-attempt-1"})
    response = await billing_routes.create_checkout_session(request)

    assert response["checkout_url"].startswith("https://checkout.stripe.test/")
    _, path, data, key = calls[-1]
    assert path == "/checkout/sessions"
    assert data["mode"] == "subscription"
    assert data["line_items[0][price]"] == "price_server"
    assert data["subscription_data[metadata][user_id]"] == "user-checkout"
    assert "browser-attempt-1" not in key
    assert len(key) <= 255


@pytest.mark.asyncio
async def test_portal_uses_authenticated_users_stored_customer(monkeypatch, store):
    monkeypatch.setattr(billing_routes, "get_billing_store", lambda: store)
    monkeypatch.setattr(
        billing_routes,
        "_require_user",
        lambda request: {"user_id": "user-portal", "email": "buyer@example.com"},
    )
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("API_BASE_URL", "https://tools.distributed.systems")
    store.upsert_customer(
        "user-portal",
        provider="stripe",
        provider_customer_id="cus_portal",
        dev_mode=False,
    )
    captured = {}

    async def fake_stripe(method, path, data=None, *, idempotency_key=None):
        captured.update(path=path, data=data, key=idempotency_key)
        return {"url": "https://billing.stripe.test/portal"}

    monkeypatch.setattr(billing_routes, "_stripe", fake_stripe)
    response = await billing_routes.create_portal_session(_request())
    assert response["portal_url"] == "https://billing.stripe.test/portal"
    assert captured["path"] == "/billing_portal/sessions"
    assert captured["data"] == {
        "customer": "cus_portal",
        "return_url": "https://tools.distributed.systems/account",
    }


@pytest.mark.asyncio
async def test_unconfigured_billing_fails_closed_unless_dev_mode(monkeypatch, store):
    monkeypatch.setattr(billing_routes, "get_billing_store", lambda: store)
    monkeypatch.setattr(
        billing_routes,
        "_require_user",
        lambda request: {"user_id": "user-dev", "email": "dev@example.com"},
    )
    monkeypatch.delenv("STRIPE_SECRET_KEY", raising=False)
    monkeypatch.delenv("BILLING_DEV_MODE", raising=False)

    with pytest.raises(HTTPException) as missing:
        await billing_routes.add_card(_request())
    assert missing.value.status_code == 503
    assert store.has_paid_entitlement("user-dev", allow_dev=True) is False

    monkeypatch.setenv("BILLING_DEV_MODE", "true")
    response = await billing_routes.add_card(_request())
    assert response["dev_mode"] is True
    assert store.has_paid_entitlement("user-dev", allow_dev=True) is True
    assert store.has_active_entitlement("user-dev") is True

    monkeypatch.delenv("BILLING_DEV_MODE")
    assert store.has_paid_entitlement("user-dev", allow_dev=False) is False
    assert store.has_active_entitlement("user-dev") is False


@pytest.mark.asyncio
async def test_stripe_requests_pin_version_and_idempotency(monkeypatch):
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("STRIPE_API_VERSION", "2026-07-29.dahlia")
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"id": "cus_1"}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, method, url, **kwargs):
            captured.update(method=method, url=url, **kwargs)
            return FakeResponse()

    monkeypatch.setattr(
        billing_routes.httpx,
        "AsyncClient",
        lambda timeout: FakeClient(),
    )
    await billing_routes._stripe(
        "POST", "/customers", {"name": "Test"}, idempotency_key="operation-1"
    )
    assert captured["headers"] == {
        "Stripe-Version": "2026-07-29.dahlia",
        "Idempotency-Key": "operation-1",
    }
