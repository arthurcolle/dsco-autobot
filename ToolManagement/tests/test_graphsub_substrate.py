import pytest

from tool_management.services.graph_client import (
    GraphManagementClient,
    GraphSubClient,
    get_graph_substrate_client,
)


def test_graphsub_is_default_and_does_not_fall_back_to_neo4j(monkeypatch):
    monkeypatch.delenv("GRAPH_SUBSTRATE", raising=False)
    monkeypatch.delenv("GRAPHSUB_URL", raising=False)
    monkeypatch.setenv("NEO4J_URL", "http://neo4j.compat")
    assert get_graph_substrate_client() is None

    monkeypatch.setenv("GRAPHSUB_URL", "http://graphsub.internal:7879")
    assert isinstance(get_graph_substrate_client(), GraphSubClient)


def test_neo4j_is_only_an_explicit_compatibility_adapter(monkeypatch):
    monkeypatch.setenv("GRAPH_SUBSTRATE", "neo4j_compat")
    monkeypatch.setenv("NEO4J_URL", "http://neo4j.compat")
    assert isinstance(get_graph_substrate_client(), GraphManagementClient)


def test_graphsub_renders_controlled_parameters_without_leaving_bind_markers():
    rendered = GraphSubClient._render_query(
        "MERGE (t:Tool {tool_id: $tool_id}) SET t.name = $name, t.active = $active",
        {"tool_id": "integration:search", "name": 'quoted " name', "active": True},
    )
    assert "$tool_id" not in rendered
    assert '"integration:search"' in rendered
    assert '"quoted \\" name"' in rendered
    assert "true" in rendered


def test_graphsub_rejects_missing_or_unsafe_parameter_shapes():
    with pytest.raises(ValueError, match="Missing GraphSub Cypher parameter"):
        GraphSubClient._render_query("MATCH (n {id: $missing}) RETURN n", {})
    with pytest.raises(ValueError, match="map keys"):
        GraphSubClient._cypher_literal({"not-valid-key": "value"})


@pytest.mark.asyncio
async def test_graphsub_transaction_uses_bounded_durable_statement_batches(monkeypatch):
    from tool_management.services import graph_client

    requests = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"substrate": "graphsub", "results": [{"rows": [[1]]}]}

    class Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, *, headers, json):
            requests.append({"url": url, "headers": headers, "json": json})
            return Response()

    monkeypatch.setattr(graph_client.httpx, "AsyncClient", Client)
    client = GraphSubClient("https://graphsub.example", api_key="graphsub-secret")
    client.MAX_TRANSACTION_BATCH_SIZE = 2

    result = await client.execute_transaction([
        {"query": "RETURN $value AS value", "parameters": {"value": 1}},
        {"query": "RETURN $value AS value", "parameters": {"value": 2}},
        {"query": "RETURN $value AS value", "parameters": {"value": 3}},
    ])

    assert len(requests) == 2
    assert [len(request["json"]["statements"]) for request in requests] == [2, 1]
    assert requests[0]["json"]["statements"][0]["query"] == "RETURN 1 AS value"
    assert requests[1]["json"]["statements"][0]["query"] == "RETURN 3 AS value"
    assert all(request["headers"]["Authorization"] == "Bearer graphsub-secret" for request in requests)
    assert len(result["results"]) == 2
