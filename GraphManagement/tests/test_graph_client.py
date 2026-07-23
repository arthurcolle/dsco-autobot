from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys
from typing import List
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import Field

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

from graph_management import GraphManagementClient, GraphNodeModel, GraphRelationshipModel


def _mock_transport(handler):
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_node_and_relationship_registration():
    client = GraphManagementClient("https://example.test", session=httpx.AsyncClient(transport=_mock_transport(lambda r: httpx.Response(200, json={}))))

    @client.node(label="Team", description="A team")
    class Team(GraphNodeModel):
        id: UUID = Field(json_schema_extra={"unique": True})
        name: str
        size: int | None = Field(default=None, json_schema_extra={"indexed": True})

    @client.node(label="Project")
    class Project(GraphNodeModel):
        id: UUID
        name: str

    @client.relationship("WORKS_ON", start=Team, end=Project, description="Assignment")
    class WorksOn(GraphRelationshipModel):
        role: str
        since: datetime | None = None

    node_specs = client.registry.node_specs()
    labels = {spec.label for spec in node_specs}
    assert labels == {"Team", "Project"}
    team_spec = next(spec for spec in node_specs if spec.label == "Team")
    props = {p.name: p for p in team_spec.properties}
    assert props["id"].unique
    assert props["size"].indexed

    rel_specs = client.registry.relationship_specs()
    assert len(rel_specs) == 1
    rel_spec = rel_specs[0]
    assert rel_spec.type == "WORKS_ON"
    assert rel_spec.start_labels == ["Team"]
    assert rel_spec.end_labels == ["Project"]

    await client.aclose()


@pytest.mark.asyncio
async def test_sync_schema_posts_specs():
    calls: List[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content.decode())
        calls.append({"path": request.url.path, "json": payload})
        return httpx.Response(200, json={"ok": True}, headers={"content-type": "application/json"})

    transport = _mock_transport(handler)
    client = GraphManagementClient("https://example.test", session=httpx.AsyncClient(transport=transport))

    @client.node(label="Dataset")
    class Dataset(GraphNodeModel):
        id: UUID
        title: str

    @client.relationship("USES", start=Dataset, end="Tool")
    class Uses(GraphRelationshipModel):
        purpose: str

    try:
        result = await client.sync_schema()
    finally:
        await client.aclose()

    assert len(calls) == 2
    assert calls[0]["path"] == "/v1/registry/nodes"
    assert calls[1]["path"] == "/v1/registry/relationships"
    assert "spec" in calls[0]["json"]
    assert result["nodes"] and result["relationships"]


@pytest.mark.asyncio
async def test_query_decorator_parses_results():
    records = [
        {
            "team": {
                "properties": {"id": str(uuid4()), "name": "Apollo"},
            }
        }
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        assert body["query"].startswith("MATCH (t:Team)")
        return httpx.Response(200, json={"results": records}, headers={"content-type": "application/json"})

    client = GraphManagementClient("https://example.test", session=httpx.AsyncClient(transport=_mock_transport(handler)))

    @client.node(label="Team")
    class Team(GraphNodeModel):
        id: UUID
        name: str

    @client.query("MATCH (t:Team) RETURN t AS team", returns=List[Team], record_key="team")
    def teams_for_project(project_id: str):
        return {"project_id": project_id}

    try:
        result = await teams_for_project("proj-123")
    finally:
        await client.aclose()

    assert isinstance(result, list)
    assert isinstance(result[0], Team)
    assert result[0].name == "Apollo"
