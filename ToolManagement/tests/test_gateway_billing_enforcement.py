"""End-to-end enforcement on the real Gateway execute() endpoint.

Drives tool_management.api.gateway_routes.execute() with a stubbed execute_tool so we
test the payment gate + charge/refund wiring without needing a live tool backend.
"""
import pytest
from fastapi import HTTPException

from tool_management.api import gateway_routes as gw
from tool_management.persistence.billing_store import BillingStore
from tool_management.services import usage_billing as ub

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def store(tmp_path, monkeypatch):
    s = BillingStore(db_path=str(tmp_path / "billing.duckdb"))
    monkeypatch.setattr(gw, "get_billing_store", lambda: s)
    monkeypatch.setenv("USAGE_BILLING_ENFORCE", "1")
    return s


async def _ok_tool(**kwargs):
    return {"result": {"ok": True}, "trace_id": "t1", "cached": False, "backend": None}


async def _boom_tool(**kwargs):
    raise RuntimeError("backend exploded")


def _req(tool="model:llama-70b"):
    return gw.ExecuteRequest(tool=tool, input={})


async def test_funded_call_succeeds_and_debits(store, monkeypatch):
    monkeypatch.setattr(gw, "execute_tool", _ok_tool)
    store.credit("u1", 5.0)  # 5 cents
    resp = await gw.execute(_req(), client_id="c", _rate_limit=None, principal="u1")
    assert resp.status == "success"
    # model_inference default price = 0.20c debited
    assert store.get_balance("u1") == pytest.approx(4.80, abs=1e-9)


async def test_broke_call_is_402(store, monkeypatch):
    monkeypatch.setattr(gw, "execute_tool", _ok_tool)
    with pytest.raises(HTTPException) as ei:
        await gw.execute(_req(), client_id="c", _rate_limit=None, principal="u_broke")
    assert ei.value.status_code == 402
    assert ei.value.detail["reason"] == "insufficient_credits"


async def test_no_principal_is_402(store, monkeypatch):
    monkeypatch.setattr(gw, "execute_tool", _ok_tool)
    with pytest.raises(HTTPException) as ei:
        await gw.execute(_req(), client_id="c", _rate_limit=None, principal=None)
    assert ei.value.status_code == 402
    assert ei.value.detail["reason"] == "auth_required"


async def test_internal_caller_is_free(store, monkeypatch):
    monkeypatch.setattr(gw, "execute_tool", _ok_tool)
    resp = await gw.execute(_req(), client_id="c", _rate_limit=None, principal=ub.INTERNAL)
    assert resp.status == "success"
    assert store.get_balance(ub.INTERNAL) == 0.0  # nothing charged


async def test_failed_call_refunds(store, monkeypatch):
    monkeypatch.setattr(gw, "execute_tool", _boom_tool)
    store.credit("u1", 5.0)
    resp = await gw.execute(_req(), client_id="c", _rate_limit=None, principal="u1")
    assert resp.status == "error"
    assert store.get_balance("u1") == pytest.approx(5.0, abs=1e-9)  # charged then refunded


async def test_subscription_bypasses_balance(store, monkeypatch):
    monkeypatch.setattr(gw, "execute_tool", _ok_tool)
    monkeypatch.setenv("BILLING_DEV_MODE", "1")
    store.upsert_subscription("sub_user", provider="dev", status="active")
    resp = await gw.execute(_req(), client_id="c", _rate_limit=None, principal="sub_user")
    assert resp.status == "success"
    assert store.get_balance("sub_user") == 0.0  # subscription: metered, not debited


async def test_enforcement_off_by_default(tmp_path, monkeypatch):
    s = BillingStore(db_path=str(tmp_path / "b2.duckdb"))
    monkeypatch.setattr(gw, "get_billing_store", lambda: s)
    monkeypatch.delenv("USAGE_BILLING_ENFORCE", raising=False)
    monkeypatch.setattr(gw, "execute_tool", _ok_tool)
    # no principal, enforcement off -> still succeeds (backwards compatible)
    resp = await gw.execute(_req(), client_id="c", _rate_limit=None, principal=None)
    assert resp.status == "success"
