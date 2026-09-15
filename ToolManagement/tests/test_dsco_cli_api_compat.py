import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import advanced_orchestration as orchestration
from tool_management.embeddings.advanced_tool_index import AdvancedToolIndex


class _FakeRecommender:
    def __init__(self):
        self.context = None

    async def recommend_workflow(self, context, max_steps=5):
        self.context = context
        return []


class _FakeOrchestrator:
    def __init__(self):
        self.recommender = _FakeRecommender()


class _FlatEmbeddings:
    name = "test-flat"

    async def embed(self, texts):
        # Force semantic ties so this test isolates the lexical ranking signal.
        return np.ones((len(texts), 8), dtype=np.float64)


def test_recommend_accepts_dsco_empty_intent_and_infers_transform(monkeypatch):
    fake = _FakeOrchestrator()
    monkeypatch.setattr(orchestration, "get_orchestrator", lambda: fake)

    app = FastAPI()
    app.include_router(orchestration.router, prefix="/api/v1")
    response = TestClient(app).post(
        "/api/v1/orchestration/recommend",
        json={
            "intent": "",
            "query": "add two numbers",
            "available_data": {},
            "max_steps": 2,
        },
    )

    assert response.status_code == 200
    assert response.json()["intent"] == "transform"
    assert fake.recommender.context.intent is orchestration.WorkflowIntent.TRANSFORM


@pytest.mark.asyncio
async def test_advanced_search_prefers_full_lexical_match(tmp_path):
    index = AdvancedToolIndex(db_path=str(tmp_path / "facets.db"))
    index.embedding_service = _FlatEmbeddings()
    await index.index_tools_batch(
        [
            {
                "name": "linear_project_add_comment",
                "description": "Add a comment to a Linear project.",
                "inputSchema": {"type": "object", "properties": {}},
                "outputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "add",
                "description": "Add two numbers together.",
                "inputSchema": {"type": "object", "properties": {}},
                "outputSchema": {"type": "number"},
            },
        ],
        backend="registry",
    )

    results = await index.search("add two numbers", limit=2)

    assert [result["tool"]["name"] for result in results] == [
        "add",
        "linear_project_add_comment",
    ]
    assert results[0]["lexical_score"] == 1.0
