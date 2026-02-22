import asyncio
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import execution_routes
from tool_management.core.streaming import StreamingExecutor
from tool_management.persistence.execution_store import ExecutionStore


async def _drain_execution(executor: StreamingExecutor, execution_id: str):
    async for _event in executor.stream_events(execution_id, include_history=True):
        pass


async def _shutdown_executor(executor: StreamingExecutor):
    executor._shutdown = True
    executor._queue_event.set()
    task = executor._scheduler_task
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_execution_group_state_materializes_from_partial_chunks(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "execution_groups.db"))

    async def tool_executor(_tool_name, _inputs, progress_callback=None):
        if progress_callback:
            await progress_callback(progress=20, partial_data={"url": "https://a.example"})
            await progress_callback(progress=40, partial_data={"url": "https://b.example"})
            await progress_callback(progress=60, partial_data={"url": "https://a.example"})
        return {"ok": True}

    executor = StreamingExecutor(
        tool_executor=tool_executor,
        heartbeat_interval=60,
        max_concurrent=1,
        execution_store=store,
    )

    ex1 = await executor.execute(
        tool_name="search",
        inputs={"query": "dc weather"},
        convergence_key="weather:dc",
    )
    await _drain_execution(executor, ex1.execution_id)

    ex2 = await executor.execute(
        tool_name="search",
        inputs={"query": "dc weather followup"},
        convergence_key="weather:dc",
    )
    await _drain_execution(executor, ex2.execution_id)

    group = store.get_execution_group_state("weather:dc")
    assert group is not None
    assert group["tool_name"] == "search"
    assert int(group["chunk_count"]) >= 6
    assert int(group["execution_count"]) == 2
    state = group.get("state") if isinstance(group.get("state"), dict) else {}
    items = state.get("items") if isinstance(state.get("items"), list) else []
    # append_unique reducer should collapse duplicate chunk payloads.
    assert len(items) == 2

    await _shutdown_executor(executor)


def test_execution_group_api_list_get_delete(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "execution_groups_api.db"))
    store.upsert_execution_group_chunk(
        convergence_key="group_alpha",
        tool_name="search",
        chunk={"title": "alpha"},
        execution_id="exec_1",
        trace_id="tr_1",
        session_id="sess_1",
    )
    store.upsert_execution_group_chunk(
        convergence_key="group_beta",
        tool_name="deep_search",
        chunk={"title": "beta"},
        execution_id="exec_2",
        trace_id="tr_2",
        session_id="sess_2",
    )

    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)

    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    listed = client.get("/api/v1/execution-groups", params={"limit": 10})
    assert listed.status_code == 200
    payload = listed.json()
    assert payload["count"] == 2

    filtered = client.get("/api/v1/execution-groups", params={"tool_name": "search", "limit": 10})
    assert filtered.status_code == 200
    filtered_payload = filtered.json()
    assert filtered_payload["count"] == 1
    assert filtered_payload["groups"][0]["convergence_key"] == "group_alpha"

    fetched = client.get("/api/v1/execution-groups/group_alpha")
    assert fetched.status_code == 200
    fetched_payload = fetched.json()
    assert fetched_payload["tool_name"] == "search"
    assert fetched_payload["state"]["items"][0]["title"] == "alpha"

    deleted = client.delete("/api/v1/execution-groups/group_alpha")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True

    missing = client.get("/api/v1/execution-groups/group_alpha")
    assert missing.status_code == 404


def test_execution_group_advanced_reducers_and_compaction(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "execution_groups_reducers.db"))

    # top_k keeps highest scoring items.
    store.upsert_execution_group_chunk(
        convergence_key="scores_topk",
        tool_name="search",
        chunk=[
            {"id": "a", "score": 0.2},
            {"id": "b", "score": 0.9},
            {"id": "c", "score": 0.5},
        ],
        execution_id="exec_topk_1",
        reducer="top_k",
        reducer_config={"k": 2},
    )
    topk = store.get_execution_group_state("scores_topk")
    assert topk is not None
    topk_items = topk["state"]["items"]
    assert len(topk_items) == 2
    assert topk_items[0]["id"] == "b"

    # score_weighted aggregates same key across chunks.
    store.upsert_execution_group_chunk(
        convergence_key="scores_weighted",
        tool_name="search",
        chunk=[
            {"url": "https://a.example", "score": 0.4, "weight": 1.0},
            {"url": "https://b.example", "score": 0.8, "weight": 1.0},
        ],
        execution_id="exec_weight_1",
        reducer="score_weighted",
        reducer_config={"key_field": "url"},
    )
    store.upsert_execution_group_chunk(
        convergence_key="scores_weighted",
        tool_name="search",
        chunk=[
            {"url": "https://a.example", "score": 1.0, "weight": 3.0},
        ],
        execution_id="exec_weight_2",
        reducer="score_weighted",
        reducer_config={"key_field": "url"},
    )
    weighted = store.get_execution_group_state("scores_weighted")
    assert weighted is not None
    weighted_items = weighted["state"]["items"]
    by_key = {item["key"]: item for item in weighted_items}
    assert "https://a.example" in by_key
    assert by_key["https://a.example"]["weighted_score"] > 0.8
    assert by_key["https://a.example"]["count"] >= 2

    # windowed can drop stale items during compaction.
    old_ts = time.time() - 7200
    store.upsert_execution_group_chunk(
        convergence_key="windowed_group",
        tool_name="search",
        chunk=[
            {"id": "old", "timestamp": old_ts},
            {"id": "new", "timestamp": time.time()},
        ],
        execution_id="exec_win_1",
        reducer="windowed",
        reducer_config={"window_seconds": 3600},
    )
    compacted = store.compact_execution_group_state(
        convergence_key="windowed_group",
        max_age_seconds=3600,
    )
    assert compacted is not None
    compacted_ids = [item.get("id") for item in compacted["state"]["items"]]
    assert "new" in compacted_ids
    assert "old" not in compacted_ids


def test_execution_group_api_analytics_compact_and_cleanup(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "execution_groups_analytics.db"))
    now = time.time()
    store.upsert_execution_group_chunk(
        convergence_key="hot_group",
        tool_name="search",
        chunk={"id": "hot", "score": 0.9, "timestamp": now},
        execution_id="exec_hot",
        trace_id="tr_hot",
        session_id="sess_hot",
    )
    store.upsert_execution_group_chunk(
        convergence_key="stale_group",
        tool_name="search",
        chunk={"id": "stale", "score": 0.1, "timestamp": now - 90000},
        execution_id="exec_stale",
        trace_id="tr_stale",
        session_id="sess_stale",
    )
    # Force stale updated_at so cleanup can remove it.
    with store._conn() as conn:
        conn.execute(
            "UPDATE execution_group_states SET updated_at = ? WHERE convergence_key = ?",
            (now - 90000, "stale_group"),
        )

    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    analytics = client.get("/api/v1/execution-groups/analytics", params={"hours": 48, "limit": 100})
    assert analytics.status_code == 200
    analytics_payload = analytics.json()
    assert analytics_payload["totals"]["groups"] >= 2
    assert analytics_payload["hot_groups"][0]["convergence_key"] == "hot_group"

    compact = client.post(
        "/api/v1/execution-groups/hot_group/compact",
        json={"max_items": 1},
    )
    assert compact.status_code == 200
    assert compact.json()["chunk_count"] >= 1

    cleanup = client.post(
        "/api/v1/execution-groups/cleanup",
        json={"max_age_seconds": 86400},
    )
    assert cleanup.status_code == 200
    assert cleanup.json()["removed"] >= 1

    bulk_compact = client.post(
        "/api/v1/execution-groups/compact",
        json={"limit": 50, "max_items": 1},
    )
    assert bulk_compact.status_code == 200
    assert bulk_compact.json()["compacted"] >= 1


def test_follow_up_generation_and_status_lifecycle(tmp_path, monkeypatch):
    store = ExecutionStore(db_path=str(tmp_path / "follow_up_actions.db"))
    now = time.time()

    # Create a hot convergence group with high merge pressure.
    store.upsert_execution_group_chunk(
        convergence_key="followup_group",
        tool_name="search",
        chunk={"id": "a", "score": 0.8, "timestamp": now},
        execution_id="exec_fu_1",
        trace_id="tr_fu",
        session_id="sess_fu",
    )
    store.upsert_execution_group_chunk(
        convergence_key="followup_group",
        tool_name="search",
        chunk={"id": "b", "score": 0.7, "timestamp": now},
        execution_id="exec_fu_1",
        trace_id="tr_fu",
        session_id="sess_fu",
    )
    store.upsert_execution_group_chunk(
        convergence_key="followup_group",
        tool_name="search",
        chunk={"id": "c", "score": 0.6, "timestamp": now},
        execution_id="exec_fu_1",
        trace_id="tr_fu",
        session_id="sess_fu",
    )

    monkeypatch.setattr(execution_routes, "get_execution_store", lambda: store)
    app = FastAPI()
    app.include_router(execution_routes.router)
    client = TestClient(app)

    generated = client.post(
        "/api/v1/follow-ups/generate",
        json={
            "hours": 24,
            "limit": 10,
            "merge_pressure_threshold": 1.5,
            "items_threshold": 2,
            "auto_persist": True,
            "trace_id": "tr_fu",
            "session_id": "sess_fu",
        },
    )
    assert generated.status_code == 200
    generated_payload = generated.json()
    assert generated_payload["count"] >= 1
    assert generated_payload["persisted"] >= 1
    follow_up_id = generated_payload["actions"][0]["follow_up_id"]

    listed = client.get("/api/v1/follow-ups", params={"trace_id": "tr_fu", "limit": 50})
    assert listed.status_code == 200
    listed_payload = listed.json()
    assert listed_payload["count"] >= 1

    analytics = client.get("/api/v1/follow-ups/analytics", params={"hours": 24})
    assert analytics.status_code == 200
    analytics_payload = analytics.json()
    assert analytics_payload["totals"]["actions"] >= 1

    updated = client.post(
        f"/api/v1/follow-ups/{follow_up_id}/status",
        json={"status": "completed", "metadata": {"source": "unit_test"}},
    )
    assert updated.status_code == 200
    updated_payload = updated.json()
    assert updated_payload["action"]["status"] == "completed"

    fetched = client.get(f"/api/v1/follow-ups/{follow_up_id}")
    assert fetched.status_code == 200
    fetched_payload = fetched.json()
    assert fetched_payload["status"] == "completed"
