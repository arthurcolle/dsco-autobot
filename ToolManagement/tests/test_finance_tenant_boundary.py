import json

import pytest


@pytest.mark.asyncio
async def test_tenant_cannot_fall_through_to_operator_alpaca_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from native.finance_fabric.server import FinanceFabricMCP

    server = FinanceFabricMCP(port=0)
    result = await server._execute_tool(
        "alpaca_brokerage_account",
        {
            "tenant_id": "tenant-a",
            "_pooled_ok": True,
            "_tenant_credential": "caller-controlled-value",
        },
    )

    assert result["isError"] is True
    payload = json.loads(result["content"][0]["text"])
    assert payload["error"].startswith("TENANT_BYOK_REQUIRED")


@pytest.mark.asyncio
async def test_tenant_finance_flow_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    from native.finance_fabric.server import FinanceFabricMCP

    server = FinanceFabricMCP(port=0)
    result = await server._execute_tool(
        "flow__payment_reconciliation",
        {"tenant_id": "tenant-a"},
    )

    assert result["isError"] is True
    payload = json.loads(result["content"][0]["text"])
    assert payload["error"].startswith("TENANT_BYOK_REQUIRED")
