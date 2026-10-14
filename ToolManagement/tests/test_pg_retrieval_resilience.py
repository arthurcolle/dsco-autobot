import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def retrieval():
    path = Path(__file__).parents[1] / "src/tool_management/services/pg_retrieval.py"
    spec = importlib.util.spec_from_file_location("pg_retrieval_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def candidate(name, backend="weather"):
    return {"tool_id": f"{backend}:{name}", "backend": backend, "name": name,
            "tool": {"name": name, "description": "Current conditions by city",
                     "inputSchema": {"properties": {"city": {"type": "string"}}}}}


def test_retrieve_uses_lexical_recall_when_embeddings_are_unavailable(retrieval, monkeypatch):
    monkeypatch.setattr(retrieval, "semantic_search", lambda *a, **k: [])
    monkeypatch.setattr(retrieval, "fts_search", lambda *a, **k: [candidate("weather_lookup")])
    monkeypatch.setattr(retrieval, "_rerank", lambda *a, **k: None)
    result = retrieval.retrieve_rerank("current weather", top_k=1)
    assert result["tools"][0]["tool_id"] == "weather:weather_lookup"
    assert result["reranked"] is False


def test_retrieve_preserves_lexical_recall_after_semantic_query_failure(retrieval, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("pgvector unavailable")
    monkeypatch.setattr(retrieval, "semantic_search", fail)
    monkeypatch.setattr(retrieval, "fts_search", lambda *a, **k: [candidate("weather_lookup")])
    monkeypatch.setattr(retrieval, "_rerank", lambda *a, **k: None)
    assert retrieval.retrieve_rerank("weather")["tools"]


def test_invalid_reranker_indices_cannot_select_wrong_tools(retrieval, monkeypatch):
    monkeypatch.setattr(retrieval, "semantic_search", lambda *a, **k: [candidate("first"), candidate("last")])
    monkeypatch.setattr(retrieval, "fts_search", lambda *a, **k: [])
    monkeypatch.setattr(retrieval, "_rerank", lambda *a, **k: [
        {"index": -1}, {"index": True}, {"index": "0"}, {"index": 999}])
    result = retrieval.retrieve_rerank("weather")
    assert [x["name"] for x in result["tools"]] == ["first", "last"]
    assert result["reranked"] is False


def test_partial_reranker_response_fills_remaining_candidates(retrieval, monkeypatch):
    monkeypatch.setattr(retrieval, "semantic_search", lambda *a, **k: [candidate("first"), candidate("second"), candidate("third")])
    monkeypatch.setattr(retrieval, "fts_search", lambda *a, **k: [])
    monkeypatch.setattr(retrieval, "_rerank", lambda *a, **k: [
        {"index": 2, "relevance_score": .9}, {"index": 2, "relevance_score": .8}])
    result = retrieval.retrieve_rerank("weather", top_k=3)
    assert [x["name"] for x in result["tools"]] == ["third", "first", "second"]


def test_hybrid_does_not_repeat_failed_embedding_request(retrieval, monkeypatch):
    calls = []
    monkeypatch.setattr(retrieval, "_embed_query", lambda query: calls.append(query))
    monkeypatch.setattr(retrieval, "fts_search", lambda *a, **k: [candidate("weather_lookup")])
    result = retrieval.hybrid_search("weather")
    assert calls == ["weather"]
    assert result["hybrid"][0]["tool"]["description"] == "Current conditions by city"


def test_lexical_query_carries_schema_and_backend_filter(retrieval, monkeypatch):
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def execute(self, query, params):
            assert "backend_id = %s" in query
            assert "weather" in params
        def fetchall(self):
            return [("weather:lookup", "weather", "lookup", "Current weather",
                     {"properties": {"city": {"type": "string"}}}, .6)]
    class Connection:
        def cursor(self):
            return Cursor()
    monkeypatch.setattr(retrieval, "_pg", Connection)
    rows = retrieval.fts_search("weather", 3, backend_filter="weather")
    assert "Current weather" in retrieval._doc(rows[0])
    assert "city" in retrieval._doc(rows[0])


def test_hybrid_reranker_indices_are_bounded_to_submitted_documents(retrieval, monkeypatch):
    rows = [candidate(f"tool{i}") for i in range(30)]
    monkeypatch.setattr(retrieval, "_embed_query", lambda query: [1.0])
    monkeypatch.setattr(retrieval, "semantic_search", lambda *a, **k: rows)
    monkeypatch.setattr(retrieval, "fts_search", lambda *a, **k: [])
    monkeypatch.setattr(retrieval, "_rerank", lambda *a, **k: [{"index": 25}])
    result = retrieval.hybrid_search("weather", top_k=1, rerank=True)
    assert result["hybrid"][0]["name"] == "tool0"


def test_hybrid_preserves_semantic_recall_when_lexical_query_fails(retrieval, monkeypatch):
    monkeypatch.setattr(retrieval, "_embed_query", lambda query: [1.0])
    monkeypatch.setattr(retrieval, "semantic_search", lambda *a, **k: [candidate("weather_lookup")])
    def fail(*args, **kwargs):
        raise RuntimeError("FTS index unavailable")
    monkeypatch.setattr(retrieval, "fts_search", fail)
    assert retrieval.hybrid_search("weather")["hybrid"][0]["name"] == "weather_lookup"


@pytest.mark.parametrize("score", [None, "invalid", float("nan"), float("inf"), True])
def test_invalid_reranker_scores_preserve_original_recall(retrieval, monkeypatch, score):
    monkeypatch.setattr(retrieval, "semantic_search", lambda *a, **k: [candidate("first"), candidate("last")])
    monkeypatch.setattr(retrieval, "_rerank", lambda *a, **k: [{"index": 1, "relevance_score": score}])
    result = retrieval.retrieve_rerank("weather")
    assert result["tools"][0]["name"] == "first"
    assert result["reranked"] is False
