"""Honest sandbox/paper/live labeling for the finance fabric.

Regression tests for the audit fix: Alpaca credentials + the default paper-api
base URL previously reported mode="live" while orders were routed to a
simulated market. Mode must always reflect the endpoint actually hit.
"""
import pytest


@pytest.fixture
def fabric(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    from native.finance_fabric.server import FinanceFabricMCP
    import native.finance_fabric.server as mod

    server = FinanceFabricMCP(port=0)
    saved = (dict(mod.LIVE_CREDS), mod.ALPACA_BASE)
    yield server, mod
    mod.LIVE_CREDS.clear()
    mod.LIVE_CREDS.update(saved[0])
    globals_ = None  # keep linters quiet
    import native.finance_fabric.server as m2
    m2.ALPACA_BASE = saved[1]


def _set(mod, *, creds: bool, base: str):
    mod.LIVE_CREDS["alpaca"] = creds
    mod.ALPACA_BASE = base


def test_alpaca_no_credentials_is_sandbox(fabric):
    server, mod = fabric
    _set(mod, creds=False, base="https://paper-api.alpaca.markets")
    assert server._mode("alpaca") == "sandbox"


def test_alpaca_creds_with_paper_endpoint_is_not_live(fabric):
    """The core regression: real keys + default base must NOT read 'live'."""
    server, mod = fabric
    _set(mod, creds=True, base="https://paper-api.alpaca.markets")
    assert server._mode("alpaca") == "paper"


def test_alpaca_creds_with_live_endpoint_is_live(fabric):
    server, mod = fabric
    _set(mod, creds=True, base="https://api.alpaca.markets")
    assert server._mode("alpaca") == "live"


@pytest.mark.asyncio
async def test_provider_status_reports_effective_modes(fabric):
    server, mod = fabric
    _set(mod, creds=True, base="https://paper-api.alpaca.markets")
    result = await server._tool_provider_status({})
    assert result.get("isError") is not True
    import json
    payload = json.loads(result["content"][0]["text"])
    alpaca = payload["providers"]["alpaca"]
    assert alpaca["credentials_present"] is True
    assert alpaca["mode"] == "paper"
