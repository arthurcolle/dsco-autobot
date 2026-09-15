"""Focused coverage for independent announce embedding and graph-projection gates."""

from pathlib import Path

import pytest

from tool_management.api import tools_api


@pytest.mark.asyncio
async def test_announce_graph_projection_can_run_without_embedding_sync(monkeypatch):
    calls = []

    async def record_embedding(*args):
        calls.append(("embedding", args))

    async def record_projection(*args):
        calls.append(("projection", args))

    monkeypatch.setenv("ENABLE_ANNOUNCE_EMBED_SYNC", "0")
    monkeypatch.setenv("ENABLE_ANNOUNCE_GRAPH_PROJECTION", "1")
    monkeypatch.setattr(tools_api, "_sync_announced_tool_embeddings", record_embedding)
    monkeypatch.setattr(tools_api, "_project_announced_tools_to_graph", record_projection)

    await tools_api._embed_and_graph_sync(
        "distributed-search", [{"name": "search"}], "https://search.example"
    )

    assert calls == [
        ("projection", ("distributed-search", [{"name": "search"}], "https://search.example"))
    ]


@pytest.mark.asyncio
async def test_announce_background_work_is_skipped_when_both_gates_are_disabled(monkeypatch):
    calls = []

    async def record_embedding(*args):
        calls.append("embedding")

    async def record_projection(*args):
        calls.append("projection")

    monkeypatch.setenv("ENABLE_ANNOUNCE_EMBED_SYNC", "0")
    monkeypatch.setenv("ENABLE_ANNOUNCE_GRAPH_PROJECTION", "0")
    monkeypatch.setattr(tools_api, "_sync_announced_tool_embeddings", record_embedding)
    monkeypatch.setattr(tools_api, "_project_announced_tools_to_graph", record_projection)

    await tools_api._embed_and_graph_sync("distributed-search", [], "https://search.example")

    assert calls == []


def test_modal_registry_enables_graph_projection_without_embedding_sync():
    modal_app = Path(__file__).resolve().parents[1] / "deploy" / "modal" / "modal_app.py"
    source = modal_app.read_text(encoding="utf-8")

    assert '"ENABLE_ANNOUNCE_EMBED_SYNC": "0"' in source
    assert '"ENABLE_ANNOUNCE_GRAPH_PROJECTION": "1"' in source
