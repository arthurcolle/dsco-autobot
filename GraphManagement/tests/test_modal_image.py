from contextlib import asynccontextmanager
from pathlib import Path
import sys
from typing import Any

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from fastapi.testclient import TestClient
from neo4j.graph import Graph, Node
from neo4j.time import DateTime

import neo4j_modal_image as modal_app


class DummyResult:
    def __init__(self, records: list[dict[str, Any]]):
        self._records = records
        self._iter = iter(self._records)

    def __aiter__(self):
        self._iter = iter(self._records)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def single(self):
        return self._records[0]


class DummyTx:
    def __init__(self, results: list[dict[str, Any]]):
        self._results = results

    async def run(self, query: str, parameters: dict | None = None):
        return DummyResult(self._results)

    async def commit(self):
        return None

    async def rollback(self):
        return None


class DummySession:
    def __init__(self, results: list[dict[str, Any]]):
        self._results = results

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def run(self, query: str, parameters: dict | None = None):
        return DummyResult(self._results)

    async def begin_transaction(self):
        return DummyTx(self._results)


class DummyDriver:
    def __init__(self, records: list[dict[str, Any]]):
        self._records = records

    @asynccontextmanager
    async def session(self):
        yield DummySession(self._records)


@pytest.fixture
def client():
    dummy_driver = DummyDriver([{"ok": 1}])

    async def override() -> DummyDriver:
        return dummy_driver

    modal_app.fastapi_app.dependency_overrides[modal_app.driver_dep] = override
    try:
        client = TestClient(modal_app.fastapi_app)
        client.backend_driver = dummy_driver
        yield client
    finally:
        modal_app.fastapi_app.dependency_overrides.pop(modal_app.driver_dep, None)


def test_probe_endpoint(client: TestClient):
    response = client.get("/probe")
    assert response.status_code == 200
    assert response.json() == {"ok": 1}


def test_query_endpoint(client: TestClient):
    payload = {"query": "RETURN 1 AS ok"}
    response = client.post("/query", json=payload)
    assert response.status_code == 200
    assert response.json() == {"results": [{"ok": 1}]}


def test_query_requires_statement(client: TestClient):
    response = client.post("/query", json={})
    assert response.status_code == 400


def test_read_and_write_endpoints(client: TestClient):
    payload = {"query": "RETURN 1 AS ok"}
    r1 = client.post("/read", json=payload)
    r2 = client.post("/write", json=payload)
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json() == {"results": [{"ok": 1}]}
    assert r2.json() == {"results": [{"ok": 1}]}


def test_transaction_endpoint(client: TestClient):
    payload = {"statements": [
        {"query": "RETURN 1 AS ok"},
        {"query": "RETURN 1 AS ok"}
    ]}
    resp = client.post("/tx", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data and isinstance(data["results"], list)
    assert data["results"][0] == [{"ok": 1}]
    assert data["results"][1] == [{"ok": 1}]


def test_query_serializes_temporal_and_node_values(client: TestClient):
    ts = DateTime(2024, 1, 2, 3, 4, 5, 123456789)
    graph = Graph()
    node = Node(graph, "0", 0, ["Demo"], {"ts": ts})
    client.backend_driver._records = [{"ts": ts, "node": node}]

    payload = {"query": "RETURN datetime() AS ts"}
    resp = client.post("/query", json=payload)

    assert resp.status_code == 200
    data = resp.json()
    assert data["results"][0]["ts"] == ts.isoformat()
    assert data["results"][0]["node"]["properties"]["ts"] == ts.isoformat()
