from fastapi.testclient import TestClient

from tool_management.integrations.mcp_adapter import MCPAdapter, MCPServerConfig, MCPTool
from tool_management.services.billing_hooks import BillingManager, InMemoryBillingHook
from tool_management.services.service_wrapper import ServiceConfig, ServiceWrapper


class DummyAdapter(MCPAdapter):
    def __init__(self):
        super().__init__(MCPServerConfig(name="dummy", url="http://dummy.local"))
        self._discovered_tools["search"] = MCPTool(
            name="search",
            description="Search tool",
            input_schema={
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
            server_name=self.name,
            server_url=self.url,
        )

    async def connect(self) -> bool:
        self._connected = True
        return True

    async def disconnect(self):
        self._connected = False

    async def execute_tool(self, tool_name: str, inputs):
        return {"tool": tool_name, "echo": dict(inputs)}


def _build_wrapper(db_path: str, *, hook: InMemoryBillingHook, cost_per_request: int) -> ServiceWrapper:
    config = ServiceConfig(
        service_id="svc-test",
        service_name="Service Test",
        db_path=db_path,
        require_api_key=False,
        enforce_quota=False,
        cost_per_request_cents=cost_per_request,
        track_usage=True,
        enforce_economic_relationship=True,
    )
    return ServiceWrapper(
        config=config,
        adapter=DummyAdapter(),
        billing_manager=BillingManager(billing_hook=hook),
    )


def test_service_wrapper_blocks_execution_when_balance_insufficient(tmp_path):
    hook = InMemoryBillingHook(default_balance_cents=5)
    wrapper = _build_wrapper(str(tmp_path / "low-balance.db"), hook=hook, cost_per_request=10)
    app = wrapper.create_app()

    with TestClient(app) as client:
        admission = client.post(
            "/economics/admission-check",
            json={"tool": "search", "inputs": {"query": "dc weather"}},
        )
        assert admission.status_code == 200
        payload = admission.json()
        assert payload["admitted"] is False
        assert "insufficient_balance" in payload["reasons"]

        blocked = client.post(
            "/execute",
            json={"tool": "search", "inputs": {"query": "dc weather"}},
        )
        assert blocked.status_code == 402


def test_service_wrapper_idempotency_prevents_duplicate_charges(tmp_path):
    hook = InMemoryBillingHook(default_balance_cents=1000)
    wrapper = _build_wrapper(str(tmp_path / "idempotent.db"), hook=hook, cost_per_request=200)
    app = wrapper.create_app()

    with TestClient(app) as client:
        first = client.post(
            "/execute",
            json={
                "tool": "search",
                "inputs": {"query": "distributed systems"},
                "idempotency_key": "idem-1",
            },
        )
        assert first.status_code == 200
        first_payload = first.json()
        assert first_payload["status"] == "completed"
        assert first_payload["economics"]["charged_cost_cents"] == 200

        second = client.post(
            "/execute",
            json={
                "tool": "search",
                "inputs": {"query": "distributed systems"},
                "idempotency_key": "idem-1",
            },
        )
        assert second.status_code == 200
        second_payload = second.json()
        assert second_payload["status"] == "completed"
        assert second_payload["economics"]["idempotent_replay"] is True

        # Charged only once.
        assert hook.get_balance("anonymous") == 800

        events = client.get("/economics/events")
        assert events.status_code == 200
        event_types = [row["event_type"] for row in events.json()["events"]]
        assert "charged" in event_types
        assert "idempotent_replay" in event_types
