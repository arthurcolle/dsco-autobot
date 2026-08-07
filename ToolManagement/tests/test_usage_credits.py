"""Prepaid credit balance + usage-based enforcement primitives on BillingStore."""
import os
import tempfile
import threading

import pytest

from tool_management.persistence.billing_store import BillingStore, UCENTS_PER_CENT


@pytest.fixture()
def store(tmp_path):
    return BillingStore(db_path=str(tmp_path / "billing.duckdb"))


def test_credit_and_balance(store):
    assert store.get_balance("u1") == 0.0
    assert store.credit("u1", 500.0, reason="grant") == 500.0
    assert store.get_balance("u1") == 500.0
    store.credit("u1", 250.0)
    assert store.get_balance("u1") == 750.0


def test_debit_never_overdraws(store):
    store.credit("u1", 1.0)  # 1 cent
    # gateway_call default price 0.05c → 20 calls fit in 1c
    ok_count = sum(store.debit("u1", 0.05, "gateway_call", "t") for _ in range(30))
    assert ok_count == 20
    assert store.get_balance("u1") == pytest.approx(0.0, abs=1e-9)
    assert store.debit("u1", 0.05, "gateway_call", "t") is False  # broke


def test_try_charge_prices_and_debits(store):
    store.credit("u1", 1.0)
    r = store.try_charge("u1", "gateway_call", "route")
    assert r["allowed"] and r["price_cents"] == 0.05
    assert r["balance_cents"] == pytest.approx(0.95, abs=1e-9)


def test_price_override_env(store, monkeypatch):
    monkeypatch.setenv("USAGE_PRICES", '{"model_inference:llama-70b": 2.5, "gateway_call": 0.1}')
    assert store.price_cents("gateway_call") == 0.1
    assert store.price_cents("model_inference", "llama-70b") == 2.5
    assert store.price_cents("model_inference", "other") == 0.20  # default


def test_credit_idempotent_on_ext_ref(store):
    store.credit("u1", 100.0, reason="stripe", ext_ref="evt_1")
    store.credit("u1", 100.0, reason="stripe", ext_ref="evt_1")  # replay
    assert store.get_balance("u1") == 100.0


def test_insufficient_charge_is_denied(store):
    r = store.try_charge("u1", "model_inference", "gpt")  # no balance
    assert r["allowed"] is False
    assert store.get_balance("u1") == 0.0


def test_refund_returns_credits(store):
    store.credit("u1", 1.0)
    store.try_charge("u1", "gateway_call", "x")
    store.refund("u1", 0.05, "x")
    assert store.get_balance("u1") == pytest.approx(1.0, abs=1e-9)


def test_stripe_topup_credits_balance_idempotently(store):
    event = {
        "id": "evt_topup_1", "type": "checkout.session.completed", "created": 1000,
        "data": {"object": {
            "id": "cs_1", "object": "checkout.session", "mode": "payment",
            "client_reference_id": "u1", "amount_total": 2000,
            "metadata": {"user_id": "u1", "kind": "credit_topup", "credit_cents": 2000},
        }},
    }
    r1 = store.apply_stripe_event(event)
    assert r1["applied"] and r1.get("credit")
    assert store.get_balance("u1") == 2000.0
    r2 = store.apply_stripe_event(event)  # webhook retry
    assert r2["duplicate"] is True
    assert store.get_balance("u1") == 2000.0  # not double-credited


def test_concurrent_debits_never_overdraw(store):
    store.credit("u1", 5.0)  # 5 cents → 100 gateway_calls
    results = []

    def worker():
        for _ in range(40):
            results.append(store.debit("u1", 0.05, "gateway_call", "c"))

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # exactly 100 succeed, balance floors at 0, never negative
    assert sum(results) == 100
    assert store.get_balance("u1") == pytest.approx(0.0, abs=1e-9)
