"""Network-free regression coverage for registry GraphSub adapter boundaries."""
import httpx
import pytest

from tool_management.services.graph_client import get_graph_substrate_client
from tool_management.services.graphsub_client import GraphSubClient


def test_binding_does_not_rewrite_literals_comments_or_inserted_values():
    query = 'RETURN "$literal", $value, $other // $comment\n/* $block */'
    actual = GraphSubClient._render_query(query, {"value": "$other", "other": 2})
    assert actual == 'RETURN "$literal", "$other", 2 // $comment\n/* $block */'


@pytest.mark.parametrize("value", [float('nan'), float('inf'), object()])
def test_unsupported_literals_are_rejected(value):
    with pytest.raises(ValueError):
        GraphSubClient._cypher_literal(value)


def test_gateway_bearer_contract(monkeypatch):
    monkeypatch.setenv("GRAPH_SUBSTRATE", "graphsub")
    monkeypatch.setenv("GRAPHSUB_URL", "https://graphsub.example")
    monkeypatch.setenv("GRAPHSUB_BEARER_TOKEN", "fixture-only")
    assert get_graph_substrate_client().headers['Authorization'] == 'Bearer fixture-only'


@pytest.mark.asyncio
async def test_all_statements_validate_before_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Invalid batch reached the transport")
    monkeypatch.setattr(httpx, 'AsyncClient', forbidden)
    client = GraphSubClient('http://fixture.invalid')
    with pytest.raises(ValueError, match='Missing'):
        await client.execute_transaction([{'query': 'RETURN 1'}, {'query': 'RETURN $missing'}])
    assert await client.execute_transaction([]) == {'substrate': 'graphsub', 'results': []}


@pytest.mark.asyncio
async def test_failed_batch_stops_later_writes(monkeypatch):
    real_client = httpx.AsyncClient
    seen = []
    def upstream(request):
        seen.append(request)
        return httpx.Response(503)
    monkeypatch.setattr(httpx, 'AsyncClient', lambda **kw: real_client(transport=httpx.MockTransport(upstream), **kw))
    client = GraphSubClient('http://fixture.invalid')
    client.MAX_TRANSACTION_BATCH_SIZE = 1
    with pytest.raises(httpx.HTTPStatusError):
        await client.execute_transaction([{'query': 'RETURN 1'}, {'query': 'RETURN 2'}])
    assert len(seen) == 1
