import pytest

from tool_management.services.durable_store import DurableStore


@pytest.mark.asyncio
async def test_durable_store_persists_and_queries_economic_events(tmp_path):
    store = DurableStore(db_path=str(tmp_path / "events.db"))
    await store.initialize()

    try:
        first = await store.append_economic_event(
            event_type="charged",
            request_id="req-1",
            client_id="client-a",
            organization_id="org-1",
            amount_cents=25,
            payload={"tool": "search"},
        )
        second = await store.append_economic_event(
            event_type="idempotent_replay",
            request_id="req-1",
            client_id="client-a",
            organization_id="org-1",
            amount_cents=0,
            payload={"tool": "search"},
        )
        assert first["event_type"] == "charged"
        assert second["event_type"] == "idempotent_replay"

        all_events = await store.get_economic_events(client_id="client-a")
        assert len(all_events) == 2
        assert {row["event_type"] for row in all_events} == {"charged", "idempotent_replay"}

        charged = await store.get_economic_events(client_id="client-a", event_type="charged")
        assert len(charged) == 1
        assert charged[0]["amount_cents"] == 25
        assert charged[0]["payload"]["tool"] == "search"
    finally:
        await store.close()
