import pytest
from httpx import ASGITransport, AsyncClient

from parallel_websearch_server.state_store import ContextStateStore


@pytest.mark.asyncio
async def test_contexts_crud_flow(contexts_app, state_store: ContextStateStore):
    transport = ASGITransport(app=contexts_app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        create_resp = await client.post(
            "/v1alpha/contexts",
            json={"context_id": "ctx-http", "name": "Initial Context", "metadata": {"owner": "agent-1"}},
        )
        assert create_resp.status_code == 201
        context_id = create_resp.json()["context_id"]

        patch_resp = await client.patch(
            f"/v1alpha/contexts/{context_id}",
            json={"description": "updated", "state": {"progress": 0.5}},
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["state"]["progress"] == 0.5

        # Seed interaction history directly through the store to mimic cached operations.
        await state_store.record_interaction(
            context_id=context_id,
            interaction_type="search",
            status="completed",
            input_payload={"objective": "demo"},
            output_payload={"results": 1},
            summary="Search returned 1 result",
        )

        detail_resp = await client.get(f"/v1alpha/contexts/{context_id}?history_limit=5")
        assert detail_resp.status_code == 200
        payload = detail_resp.json()
        assert payload["context"]["context_id"] == context_id
        assert len(payload["history"]) == 1
        assert payload["history"][0]["interaction_type"] == "search"

        history_resp = await client.get(f"/v1alpha/contexts/{context_id}/history")
        assert history_resp.status_code == 200
        assert len(history_resp.json()) == 1

        list_resp = await client.get("/v1alpha/contexts")
        assert list_resp.status_code == 200
        assert any(item["context_id"] == context_id for item in list_resp.json())
