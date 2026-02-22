import pytest

from tool_management.services.billing_hooks import BillingManager, InMemoryBillingHook
from tool_management.services.durable_store import RequestStatus, ToolRequest, ToolResponse


def _build_request(request_id: str = "req-1") -> ToolRequest:
    return ToolRequest(
        id=request_id,
        tool_name="search",
        tool_id="svc:search",
        service_id="svc",
        client_id="client-a",
        organization_id=None,
        inputs={"query": "weather"},
        status=RequestStatus.RUNNING,
    )


def _build_response(request_id: str, status: RequestStatus = RequestStatus.COMPLETED) -> ToolResponse:
    return ToolResponse(
        request_id=request_id,
        status=status,
        result={"ok": True} if status == RequestStatus.COMPLETED else None,
        execution_time_ms=120.0,
    )


@pytest.mark.asyncio
async def test_billing_reservation_settles_without_double_charge():
    hook = InMemoryBillingHook(default_balance_cents=1000)
    manager = BillingManager(billing_hook=hook)
    request = _build_request("req-reserve-equal")
    response = _build_response(request.id)

    allowed = await manager.pre_execute(request=request, estimated_cost_cents=200)
    assert allowed is True
    request.cost_cents = 200

    await manager.post_execute(request=request, response=response)
    assert hook.get_balance("client-a") == 800


@pytest.mark.asyncio
async def test_billing_reservation_refunds_when_actual_is_lower():
    hook = InMemoryBillingHook(default_balance_cents=1000)
    manager = BillingManager(billing_hook=hook)
    request = _build_request("req-reserve-low")
    response = _build_response(request.id)

    allowed = await manager.pre_execute(request=request, estimated_cost_cents=220)
    assert allowed is True
    request.cost_cents = 150

    await manager.post_execute(request=request, response=response)
    assert hook.get_balance("client-a") == 850


@pytest.mark.asyncio
async def test_billing_reservation_charges_delta_when_actual_exceeds_reserved():
    hook = InMemoryBillingHook(default_balance_cents=1000)
    manager = BillingManager(billing_hook=hook)
    request = _build_request("req-reserve-high")
    response = _build_response(request.id)

    allowed = await manager.pre_execute(request=request, estimated_cost_cents=200)
    assert allowed is True
    request.cost_cents = 260

    await manager.post_execute(request=request, response=response)
    assert hook.get_balance("client-a") == 740
