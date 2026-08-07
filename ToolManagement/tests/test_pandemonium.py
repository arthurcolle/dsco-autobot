"""Pandemonium forum: gated threads settle payments through the prepaid credit ledger."""
import os
import tempfile

import pytest

os.environ["DATA_DIR"] = tempfile.mkdtemp()  # isolate stores before app import

from fastapi.testclient import TestClient  # noqa: E402
from tool_management.app import create_app  # noqa: E402
from tool_management.persistence.billing_store import get_billing_store  # noqa: E402


@pytest.fixture(scope="module")
def client():
    return TestClient(create_app())


def H(uid):
    return {"X-User-ID": uid}


def test_public_thread_is_readable_by_anyone(client):
    r = client.post("/api/v1/forum/threads",
                    json={"title": "gm", "body": "hello agents", "visibility": "public"},
                    headers=H("alice"))
    assert r.status_code == 201
    tid = r.json()["thread_id"]
    r2 = client.get(f"/api/v1/forum/threads/{tid}/messages", headers=H("stranger"))
    assert r2.status_code == 200
    assert r2.json()["messages"][0]["content"] == "hello agents"


def test_gated_thread_requires_paid_unlock_and_pays_author(client):
    # alice publishes gated intel for 50 cents
    r = client.post("/api/v1/forum/threads",
                    json={"title": "alpha leak", "body": "buy TLV", "visibility": "gated",
                          "access_fee_cents": 50},
                    headers=H("alice"))
    assert r.status_code == 201
    tid = r.json()["thread_id"]

    billing = get_billing_store()
    a0 = billing.get_balance("alice")
    p0 = billing.get_balance("__platform__")

    # bob can't read until he pays
    assert client.get(f"/api/v1/forum/threads/{tid}/messages", headers=H("bob")).status_code == 402

    # fund bob and unlock
    billing.credit("bob", 100.0)
    u = client.post(f"/api/v1/forum/threads/{tid}/unlock", headers=H("bob"))
    assert u.status_code == 200 and u.json()["status"] == "unlocked"

    # settlement: bob -50, alice +40 (80%), platform +10 (20% rake)
    assert billing.get_balance("bob") == pytest.approx(50.0, abs=1e-9)
    assert billing.get_balance("alice") == pytest.approx(a0 + 40.0, abs=1e-9)
    assert billing.get_balance("__platform__") == pytest.approx(p0 + 10.0, abs=1e-9)

    # now bob can read the gated body and reply
    r2 = client.get(f"/api/v1/forum/threads/{tid}/messages", headers=H("bob"))
    assert r2.status_code == 200
    assert any(m["content"] == "buy TLV" for m in r2.json()["messages"])
    assert client.post(f"/api/v1/forum/threads/{tid}/reply",
                       json={"content": "confirmed"}, headers=H("bob")).status_code == 201


def test_unlock_is_idempotent_no_double_charge(client):
    r = client.post("/api/v1/forum/threads",
                    json={"title": "vip", "body": "x", "visibility": "gated", "access_fee_cents": 30},
                    headers=H("alice"))
    tid = r.json()["thread_id"]
    billing = get_billing_store()
    billing.credit("dave", 100.0)
    client.post(f"/api/v1/forum/threads/{tid}/unlock", headers=H("dave"))
    assert billing.get_balance("dave") == pytest.approx(70.0, abs=1e-9)
    again = client.post(f"/api/v1/forum/threads/{tid}/unlock", headers=H("dave"))
    assert again.json()["status"] == "already_unlocked"
    assert billing.get_balance("dave") == pytest.approx(70.0, abs=1e-9)  # not charged twice


def test_unlock_without_funds_is_402(client):
    r = client.post("/api/v1/forum/threads",
                    json={"title": "paywalled", "body": "y", "visibility": "gated", "access_fee_cents": 500},
                    headers=H("alice"))
    tid = r.json()["thread_id"]
    u = client.post(f"/api/v1/forum/threads/{tid}/unlock", headers=H("pauper"))
    assert u.status_code == 402
    assert u.json()["detail"]["error"] == "insufficient_credits"


def test_gated_create_requires_fee(client):
    r = client.post("/api/v1/forum/threads",
                    json={"title": "bad", "visibility": "gated", "access_fee_cents": 0},
                    headers=H("alice"))
    assert r.status_code == 400


def test_identity_required_for_mutations(client):
    assert client.post("/api/v1/forum/threads", json={"title": "x"}).status_code == 401


def test_tool_thread_fronts_gateway_endpoint(client):
    r = client.post("/api/v1/forum/threads",
                    json={"title": "premium search tool", "visibility": "gated", "kind": "tool",
                          "tool_ref": "search", "access_fee_cents": 25, "body": "unlock to use"},
                    headers=H("alice"))
    assert r.status_code == 201
    tid = r.json()["thread_id"]
    billing = get_billing_store()
    billing.credit("erin", 100.0)
    u = client.post(f"/api/v1/forum/threads/{tid}/unlock", headers=H("erin"))
    assert u.status_code == 200
    assert u.json()["tool_ref"] == "search"  # agent now routes to the gateway tool
