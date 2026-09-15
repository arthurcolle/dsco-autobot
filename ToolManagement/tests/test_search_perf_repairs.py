import hashlib
import json
import sqlite3

import numpy as np
import pytest

from tool_management.embeddings.advanced_tool_index import AdvancedToolIndex
from tool_management.embeddings.openrouter_embeddings import cosine_similarity


class _StableEmbeddings:
    name = "stable-test"

    async def embed(self, texts):
        return np.array([self._vector(text) for text in texts], dtype=np.float64)

    @staticmethod
    def _vector(text):
        digest = hashlib.blake2b(str(text).encode("utf-8"), digest_size=16).digest()
        values = np.frombuffer(digest, dtype=np.uint8).astype(np.float64)
        vector = values[:8] - 127.5
        if not np.any(vector):
            vector[0] = 1.0
        return vector


class _FlatEmbeddings:
    name = "flat-test"

    async def embed(self, texts):
        return np.ones((len(texts), 8), dtype=np.float64)


def _tool(name, description, *, tags=None, category="utility"):
    return {
        "name": name,
        "description": description,
        "tags": tags or [],
        "category": category,
        "inputSchema": {"type": "object", "properties": {}},
        "outputSchema": {"type": "object", "properties": {}},
    }


@pytest.mark.asyncio
async def test_search_reuses_fresh_matrix_cache_without_refetch(tmp_path, monkeypatch):
    index = AdvancedToolIndex(db_path=str(tmp_path / "facets.db"))
    index.embedding_service = _StableEmbeddings()

    await index.index_tools_batch(
        [
            _tool("weather_lookup", "Find current weather by city", tags=["weather"]),
            _tool("document_search", "Search documents and return passages"),
            _tool("send_notification", "Send a notification to a user"),
        ],
        backend="registry",
    )

    first = await index.search("find weather", limit=2)
    assert first
    assert list(index._matrix_cache.keys()) == [None]

    monkeypatch.setattr(
        index,
        "_fetch_search_rows",
        lambda backend_filter: pytest.fail("fresh cache should not fetch rows"),
    )
    monkeypatch.setattr(
        index,
        "_search_corpus_fingerprint",
        lambda backend_filter: pytest.fail("fresh cache should not check the database"),
    )

    second = await index.search("find weather", limit=2)

    assert [result["tool_id"] for result in second] == [
        result["tool_id"] for result in first
    ]


@pytest.mark.asyncio
async def test_stale_matrix_cache_refreshes_when_database_fingerprint_changes(tmp_path):
    index = AdvancedToolIndex(db_path=str(tmp_path / "facets.db"))
    index.embedding_service = _StableEmbeddings()
    index._matrix_cache_ttl_seconds = 999.0

    await index.index_tool(
        _tool("alpha_lookup", "Old description"),
        backend="registry",
    )
    cached = await index.search("alpha lookup", limit=1)
    assert cached[0]["tool"]["description"] == "Old description"

    updated_tool = _tool("alpha_lookup", "New description after external sync")
    with sqlite3.connect(str(index.db_path)) as conn:
        conn.execute(
            """
            UPDATE tool_facets
            SET tool_data = ?, updated_at = ?
            WHERE tool_id = ?
            """,
            (
                json.dumps(updated_tool),
                "2099-01-01T00:00:00+00:00",
                "registry:alpha_lookup",
            ),
        )

    still_cached = await index.search("alpha lookup", limit=1)
    assert still_cached[0]["tool"]["description"] == "Old description"

    index._matrix_cache_ttl_seconds = 0.0
    refreshed = await index.search("alpha lookup", limit=1)

    assert refreshed[0]["tool"]["description"] == "New description after external sync"


@pytest.mark.asyncio
async def test_batched_cosine_scores_match_rowwise_cosine(tmp_path):
    index = AdvancedToolIndex(db_path=str(tmp_path / "facets.db"))
    embeddings = _StableEmbeddings()
    index.embedding_service = embeddings

    await index.index_tools_batch(
        [
            _tool("invoice_search", "Find invoices by vendor or amount"),
            _tool("invoice_export", "Export invoice records as CSV"),
            _tool("calendar_lookup", "Find available meeting times"),
        ],
        backend="registry",
    )

    query = "find invoices"
    results = await index.search(query, limit=10)
    query_vector = (await embeddings.embed([query]))[0]

    expected = {}
    with sqlite3.connect(str(index.db_path)) as conn:
        rows = conn.execute(
            """
            SELECT tool_id, purpose_embedding, input_embedding, output_embedding, example_embedding
            FROM tool_facets
            """
        ).fetchall()

    for tool_id, purpose_bytes, input_bytes, output_bytes, example_bytes in rows:
        purpose = np.frombuffer(purpose_bytes, dtype=np.float64)
        inputs = np.frombuffer(input_bytes, dtype=np.float64)
        outputs = np.frombuffer(output_bytes, dtype=np.float64)
        examples = np.frombuffer(example_bytes, dtype=np.float64)
        purpose_sim = cosine_similarity(query_vector, purpose)
        input_sim = cosine_similarity(query_vector, inputs)
        output_sim = cosine_similarity(query_vector, outputs)
        example_sim = cosine_similarity(query_vector, examples)
        expected[tool_id] = {
            "purpose": purpose_sim,
            "input": input_sim,
            "output": output_sim,
            "example": example_sim,
            "semantic": (
                0.4 * purpose_sim
                + 0.25 * example_sim
                + 0.2 * input_sim
                + 0.15 * output_sim
            ),
        }

    assert {result["tool_id"] for result in results} == set(expected)
    for result in results:
        scores = expected[result["tool_id"]]
        assert result["semantic_score"] == pytest.approx(scores["semantic"])
        for facet in ("purpose", "input", "output", "example"):
            assert result["facet_scores"][facet] == pytest.approx(scores[facet])


@pytest.mark.asyncio
async def test_exact_identifier_match_sorts_before_semantic_neighbors(tmp_path):
    index = AdvancedToolIndex(db_path=str(tmp_path / "facets.db"))
    index.embedding_service = _FlatEmbeddings()

    await index.index_tools_batch(
        [
            _tool("submit_job", "Submit a background job and return a status handle"),
            _tool("job_status", "Read the status of an existing background job"),
        ],
        backend="registry",
    )

    results = await index.search("job status", limit=2)

    assert [result["tool"]["name"] for result in results] == [
        "job_status",
        "submit_job",
    ]
    assert results[0]["identifier_score"] == 1.0
    assert results[0]["score"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_matrix_cache_is_bounded_by_lru_entries(tmp_path):
    index = AdvancedToolIndex(db_path=str(tmp_path / "facets.db"))
    index.embedding_service = _StableEmbeddings()
    index._matrix_cache_max_entries = 1

    await index.index_tool(_tool("alpha", "Alpha backend tool"), backend="alpha")
    await index.index_tool(_tool("beta", "Beta backend tool"), backend="beta")

    await index.search("alpha", backend_filter="alpha", limit=1)
    assert list(index._matrix_cache.keys()) == ["alpha"]

    await index.search("beta", backend_filter="beta", limit=1)

    assert list(index._matrix_cache.keys()) == ["beta"]
    assert len(index._matrix_cache) == 1
