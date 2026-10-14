import importlib.util
from pathlib import Path
import sys
import types

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest


@pytest.fixture
def discovery_client(monkeypatch):
    root = Path(__file__).parents[1] / "src/tool_management"
    for name, directory in (("discovery_test", root), ("discovery_test.api", root / "api"),
                            ("discovery_test.services", root / "services")):
        package = types.ModuleType(name)
        package.__path__ = [str(directory)]
        monkeypatch.setitem(sys.modules, name, package)
    pg = types.ModuleType("discovery_test.services.pg_retrieval")
    pg.retrieve_rerank = lambda query, top_k, **kwargs: {"query": query, "top_k": top_k, **kwargs}
    pg.hybrid_search = lambda query, sem, fts, top_k, rerank, **kwargs: {"rerank": rerank, **kwargs}
    pg.multi_hop_retrieve = lambda goal, steps, per_hop: {"goal": goal, "per_hop": per_hop}
    monkeypatch.setitem(sys.modules, pg.__name__, pg)
    spec = importlib.util.spec_from_file_location("discovery_test.api.discovery_routes", root / "api/discovery_routes.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    app = FastAPI()
    app.include_router(module.router)
    with TestClient(app) as client:
        yield client


def test_retrieve_forwards_requested_backend_and_recall(discovery_client):
    response = discovery_client.post("/api/v1/tool-retrieve", json={
        "query": "weather", "backend": "weather", "recall": 12, "top_k": 2})
    assert response.status_code == 200
    assert response.json()["backend_filter"] == "weather"
    assert response.json()["recall"] == 12


@pytest.mark.parametrize("path,body", [
    ("tool-retrieve", {"query": "weather", "top_k": -1}),
    ("tool-retrieve", {"query": ""}),
    ("tool-hybrid", {"query": "weather", "semantic_k": 1000000}),
    ("tool-multihop", {"goal": "weather", "per_hop": "invalid"}),
])
def test_invalid_retrieval_inputs_return_validation_error(discovery_client, path, body):
    assert discovery_client.post(f"/api/v1/{path}", json=body).status_code == 422


def test_hybrid_parses_false_string_as_false_and_forwards_backend(discovery_client):
    response = discovery_client.post("/api/v1/tool-hybrid", json={
        "query": "weather", "rerank": "false", "backend": "weather"})
    assert response.status_code == 200
    assert response.json() == {"rerank": False, "backend_filter": "weather"}


def test_catalog_outage_has_deliberate_service_unavailable_response(discovery_client, monkeypatch):
    pg = sys.modules["discovery_test.services.pg_retrieval"]
    def fail(*args, **kwargs):
        raise RuntimeError("database connection lost")
    monkeypatch.setattr(pg, "retrieve_rerank", fail)
    response = discovery_client.post("/api/v1/tool-retrieve", json={"query": "weather"})
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "TOOL_CATALOG_UNAVAILABLE"
