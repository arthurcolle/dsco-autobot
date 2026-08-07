"""All agentic-payment rails top up the same prepaid balance, idempotently."""
import os
import tempfile

import pytest

os.environ["DATA_DIR"] = tempfile.mkdtemp()
os.environ["PAYMENTS_DEV_MODE"] = "1"

from fastapi.testclient import TestClient  # noqa: E402
from tool_management.app import create_app  # noqa: E402
from tool_management.persistence.billing_store import get_billing_store  # noqa: E402
from tool_management.payments.core import sign_ap2_subject  # noqa: E402

RAILS = ["stripe", "x402", "circle", "coinbase_cdp", "l402", "cloudflare_pay"]


@pytest.fixture(scope="module")
def client():
    return TestClient(create_app())


def H(uid):
    return {"X-User-ID": uid}


def test_all_rails_enabled_in_dev(client):
    names = {p["name"] for p in client.get("/api/v1/pay/providers").json()["providers"]}
    for r in RAILS:
        assert r in names, f"{r} not enabled"


@pytest.mark.parametrize("rail", RAILS)
def test_topup_and_settle_credits_balance(client, rail):
    uid = f"user_{rail}"
    billing = get_billing_store()
    start = billing.get_balance(uid)

    intent = client.post(f"/api/v1/pay/{rail}/topup", json={"amount_usd": 5.0}, headers=H(uid)).json()
    assert intent["provider"] == rail
    assert intent["amount_cents"] == 500.0
    token = intent["dev_confirm_token"]

    r = client.post(f"/api/v1/pay/{rail}/settle", json={"dev_confirm_token": token}, headers=H(uid))
    assert r.status_code == 200, r.text
    assert billing.get_balance(uid) == pytest.approx(start + 500.0, abs=1e-9)

    # replay the same proof -> idempotent (ext_ref unique), no double credit
    client.post(f"/api/v1/pay/{rail}/settle", json={"dev_confirm_token": token}, headers=H(uid))
    assert billing.get_balance(uid) == pytest.approx(start + 500.0, abs=1e-9)


def test_topup_requires_identity(client):
    assert client.post("/api/v1/pay/x402/topup", json={"amount_usd": 1.0}).status_code == 401


def test_bad_dev_token_rejected(client):
    r = client.post("/api/v1/pay/x402/settle", json={"dev_confirm_token": "forged.deadbeef"}, headers=H("u"))
    assert r.status_code == 402


def test_x402_challenge_shape(client):
    ch = client.get("/api/v1/pay/x402/challenge", params={"price_cents": 0.2, "resource": "gateway"}).json()
    assert ch["status"] == 402
    assert ch["accepts"][0]["asset"] == "USDC"
    assert ch["accepts"][0]["network"] == "base"


def test_ap2_mandate_authorize(client):
    subject = {"user_id": "agent7", "max_amount_cents": 1000, "scope": "gateway"}
    mandate = {
        "verifiableCredential": {
            "credentialSubject": subject,
            "expirationDate": 9999999999,
            "proof": {"jws": sign_ap2_subject(subject)},
        }
    }
    r = client.post("/api/v1/pay/ap2/authorize", json={"mandate": mandate})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] and body["user_id"] == "agent7" and body["max_amount_cents"] == 1000.0


def test_ap2_expired_rejected(client):
    subject = {"user_id": "agent8"}
    mandate = {"verifiableCredential": {"credentialSubject": subject, "expirationDate": 1,
                                        "proof": {"jws": sign_ap2_subject(subject)}}}
    assert client.post("/api/v1/pay/ap2/authorize", json={"mandate": mandate}).status_code == 401


def test_settled_credits_are_spendable_on_gateway(client):
    """End-to-end: fund via x402 rail, then the credits gate a Gateway call."""
    uid = "spender1"
    intent = client.post("/api/v1/pay/x402/topup", json={"amount_usd": 1.0}, headers=H(uid)).json()
    client.post("/api/v1/pay/x402/settle", json={"dev_confirm_token": intent["dev_confirm_token"]}, headers=H(uid))
    assert get_billing_store().get_balance(uid) == pytest.approx(100.0, abs=1e-9)
