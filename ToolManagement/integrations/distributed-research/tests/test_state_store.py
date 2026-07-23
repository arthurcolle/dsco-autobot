import pytest

from parallel_websearch_server.state_store import ContextStateStore


@pytest.mark.asyncio
async def test_create_and_fetch_context(state_store: ContextStateStore):
    context = await state_store.create_context(
        context_id="ctx-123",
        name="demo",
        metadata={"owner": "agent-42"},
        state={"step": 1},
    )
    assert context["context_id"] == "ctx-123"
    assert context["metadata"]["owner"] == "agent-42"
    assert context["state"]["step"] == 1

    fetched = await state_store.get_context("ctx-123")
    assert fetched == context

    # ensure list API returns the stored context
    contexts = await state_store.list_contexts()
    assert any(entry["context_id"] == "ctx-123" for entry in contexts)


@pytest.mark.asyncio
async def test_update_context_merge_and_replace(state_store: ContextStateStore):
    await state_store.create_context(
        context_id="ctx-update",
        metadata={"a": 1},
        state={"step": 1, "notes": {"draft": True}},
    )

    updated = await state_store.update_context(
        "ctx-update",
        metadata={"b": 2},
        state={"notes": {"draft": False}, "result": "pending"},
    )
    assert updated["metadata"] == {"a": 1, "b": 2}
    assert updated["state"]["notes"]["draft"] is False
    assert updated["state"]["result"] == "pending"

    replaced = await state_store.update_context(
        "ctx-update",
        metadata={"c": 3},
        state={"fresh": True},
        replace_metadata=True,
        replace_state=True,
    )
    assert replaced["metadata"] == {"c": 3}
    assert replaced["state"] == {"fresh": True}


@pytest.mark.asyncio
async def test_interaction_history_and_run_updates(state_store: ContextStateStore):
    await state_store.create_context(context_id="ctx-history")
    await state_store.record_interaction(
        context_id="ctx-history",
        interaction_type="task.run",
        status="queued",
        input_payload={"objective": "demo"},
        output_payload={"status": "queued"},
        summary="Queued task",
        remote_run_id="run-1",
    )

    history = await state_store.list_interactions("ctx-history")
    assert len(history) == 1
    assert history[0]["status"] == "queued"

    await state_store.update_run_status(
        "run-1",
        status="completed",
        output_payload={"status": "completed"},
        summary="Task completed",
    )

    history = await state_store.list_interactions("ctx-history")
    assert history[0]["status"] == "completed"
    assert history[0]["summary"] == "Task completed"
