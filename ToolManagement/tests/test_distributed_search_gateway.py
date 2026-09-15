from __future__ import annotations

import asyncio
import json

import pytest

from native.distributed_search_mcp.gateway import (
    CURRENT_EMBED_MODEL,
    CURRENT_RERANK_MODEL,
    apply_rerank,
    build_retrieval_plan,
    canonicalize_url,
    fuse_search_results,
    select_domain_diverse,
)
from native.distributed_search_mcp.server import create_request_store, create_server
from native.distributed_search_mcp import server as search_server_module
from tool_management.auth.service_context import (
    ServiceContextError,
    sign_service_context,
    verify_service_context,
)


def test_retrieval_plan_is_bounded_and_intent_aware():
    fast = build_retrieval_plan("Jina AI", max_results=6)
    assert fast.mode == "fast"
    assert fast.queries == ["Jina AI"]
    assert fast.rerank is False

    current = build_retrieval_plan("latest Jina embedding API", max_results=6)
    assert current.mode == "balanced"
    assert len(current.queries) == 2
    assert current.rerank is True
    assert current.hydrate_pages == 3

    thorough = build_retrieval_plan("comprehensive evidence about retrieval", max_results=20)
    assert thorough.mode == "thorough"
    assert thorough.search_width == 20
    assert thorough.hydrate_pages == 5
    assert thorough.max_per_domain == 1


def test_result_fusion_deduplicates_tracking_urls_and_enforces_domains():
    fused = fuse_search_results(
        [
            ("q1", [
                {"title": "A", "url": "https://www.example.com/a?utm_source=x", "description": "first"},
                {"title": "Blocked", "url": "https://spam.test/x"},
            ]),
            ("q2", [
                {"title": "A again", "url": "https://example.com/a#section", "content": "full"},
                {"title": "B", "url": "https://docs.example.com/b"},
            ]),
        ],
        allow_domains=["example.com"],
        block_domains=["spam.test"],
    )
    assert canonicalize_url("https://www.example.com/a?utm_source=x#z") == "https://www.example.com/a"
    # www and apex hosts remain distinct canonical URLs; exact URL duplicates
    # still collapse while subdomains obey the parent-domain policy.
    assert len(fused) == 3
    assert all(item["domain"].endswith("example.com") for item in fused)
    assert any(item.get("content") == "full" for item in fused)


def test_rerank_and_domain_diversity_preserve_original_evidence():
    candidates = [
        {"title": "A1", "url": "https://a.test/1", "domain": "a.test"},
        {"title": "A2", "url": "https://a.test/2", "domain": "a.test"},
        {"title": "B", "url": "https://b.test/1", "domain": "b.test"},
    ]
    ranked = apply_rerank(candidates, [
        {"index": 1, "relevance_score": 0.9},
        {"index": 2, "relevance_score": 0.8},
        {"index": 0, "relevance_score": 0.7},
    ])
    selected = select_domain_diverse(ranked, limit=3, max_per_domain=1)
    assert [item["title"] for item in selected[:2]] == ["A2", "B"]
    assert [item["rank"] for item in selected] == [1, 2, 3]


def test_retrieve_returns_stable_grounded_contract(tmp_path, monkeypatch):
    monkeypatch.setenv("SEARCH_REQUESTS_DB", str(tmp_path / "search.db"))
    server = create_server(api_key="test-key", registry_url="http://127.0.0.1:9")

    async def fake_search(query, num_results, **kwargs):
        return {
            "query": query,
            "success": True,
            "results": [
                {"title": "Official", "url": "https://docs.example.com/api?utm_source=test", "description": "API docs"},
                {"title": "Analysis", "url": "https://independent.test/review", "description": "Independent review"},
                {"title": "Duplicate", "url": "https://docs.example.com/api#top", "description": "same page"},
            ],
        }

    async def fake_rerank(args):
        return {
            "success": True,
            "results": [
                {"index": 1, "relevance_score": 0.95},
                {"index": 0, "relevance_score": 0.8},
            ],
        }

    async def fake_read(url, **kwargs):
        return {"url": url, "title": "Read title", "content": "Grounded page text " * 80, "success": True}

    async def fake_queue(**kwargs):
        return 0

    monkeypatch.setattr(server, "_jina_search_raw", fake_search)
    monkeypatch.setattr(server, "_execute_rerank", fake_rerank)
    monkeypatch.setattr(server, "_read_for_retrieval", fake_read)
    monkeypatch.setattr(server, "_queue_search_urls", fake_queue)

    result = asyncio.run(server._execute_retrieve({
        "query": "latest API evidence",
        "mode": "balanced",
        "max_results": 3,
        "include_content": True,
    }))

    assert result["success"] is True
    assert result["contract_version"]
    assert result["quality"]["reranked"] is True
    assert result["quality"]["duplicate_hits_removed"] >= 2
    assert result["quality"]["unique_domains"] == 2
    assert result["results"][0]["domain"] == "independent.test"
    assert result["results"][0]["content_preview"].startswith("Grounded page text")
    assert result["models"] == {
        "reranker": CURRENT_RERANK_MODEL,
        "default_embedding": CURRENT_EMBED_MODEL,
    }
    stored = server._store.get(request_id=result["request_id"])
    assert stored and stored["full_result"]["results"]


def test_current_models_are_tool_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("SEARCH_REQUESTS_DB", str(tmp_path / "search.db"))
    server = create_server(api_key="test-key", registry_url="http://127.0.0.1:9")
    assert server._tools["rerank"].input_schema["properties"]["model"]["default"] == CURRENT_RERANK_MODEL
    assert server._tools["embed"].input_schema["properties"]["model"]["default"] == CURRENT_EMBED_MODEL
    assert "retrieve" in server._tools
    assert "answer" in server._tools
    assert "platform_status" in server._tools
    assert len([name for name in server._tools if not name.startswith("_")]) == 24


def test_local_request_store_is_explicitly_not_replica_durable(tmp_path, monkeypatch):
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("SEARCH_POSTGRES_URL", raising=False)
    monkeypatch.delenv("SEARCH_REQUIRE_DURABLE_STORE", raising=False)
    monkeypatch.setenv("SEARCH_REQUESTS_DB", str(tmp_path / "local.db"))
    store = create_request_store()
    assert store.get_status()["backend"] == "sqlite"
    assert store.get_status()["durable"] is False


def test_production_refuses_ephemeral_request_store(monkeypatch):
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("SEARCH_POSTGRES_URL", raising=False)
    monkeypatch.setenv("SEARCH_REQUIRE_DURABLE_STORE", "1")
    with pytest.raises(RuntimeError, match="POSTGRES_URL"):
        create_request_store()


def test_production_does_not_return_phantom_request_handles(tmp_path, monkeypatch):
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    monkeypatch.delenv("SEARCH_POSTGRES_URL", raising=False)
    monkeypatch.delenv("SEARCH_REQUIRE_DURABLE_STORE", raising=False)
    monkeypatch.setenv("SEARCH_REQUESTS_DB", str(tmp_path / "local.db"))
    server = create_server(api_key="test-key", registry_url="http://127.0.0.1:9")

    def fail_save(**_kwargs):
        raise OSError("database unavailable")

    monkeypatch.setattr(server._store, "save", fail_save)
    monkeypatch.setenv("SEARCH_REQUIRE_DURABLE_STORE", "1")
    with pytest.raises(RuntimeError, match="failed to persist retrieve result"):
        server._record_request("retrieve", {"query": "test"}, {}, "test")


def test_service_context_is_audience_bound_and_tamper_evident():
    envelope = sign_service_context(
        "backend-secret",
        audience="distributed-search-mcp",
        account_id="account-a",
        principal_id="principal-a",
        issuer="https://auth.distributed.systems",
        identity_kind="customer",
        trace_id="trace-a",
        now=100,
    )
    claims = verify_service_context(
        envelope, "backend-secret", audience="distributed-search-mcp", now=110
    )
    assert claims["account_id"] == "account-a"
    assert claims["principal_id"] == "principal-a"
    assert claims["trace_id"] == "trace-a"

    tampered = json.loads(json.dumps(envelope))
    tampered["claims"]["account_id"] = "account-b"
    with pytest.raises(ServiceContextError, match="signature"):
        verify_service_context(
            tampered, "backend-secret", audience="distributed-search-mcp", now=110
        )
    with pytest.raises(ServiceContextError, match="audience"):
        verify_service_context(envelope, "backend-secret", audience="another-worker", now=110)


def test_stored_evidence_is_partitioned_by_signed_account_context(tmp_path, monkeypatch):
    monkeypatch.setenv("SEARCH_REQUESTS_DB", str(tmp_path / "tenant.db"))
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "backend-secret")
    monkeypatch.setenv("SEARCH_REQUIRE_SIGNED_CONTEXT", "1")
    server = create_server(api_key="test-key", registry_url="http://127.0.0.1:9")
    account_a = {
        "account_id": "account-a",
        "principal_id": "principal-a",
        "trace_id": "trace-a",
        "invocation_id": "invoke-a",
    }
    token = search_server_module._current_execution_context.set(account_a)
    try:
        request_id, _ = server._record_request(
            "retrieve", {"query": "private"}, {"results": [{"url": "https://example.test"}]}, "private"
        )
    finally:
        search_server_module._current_execution_context.reset(token)

    assert server._store.get(request_id=request_id, tenant_id="account-a") is not None
    assert server._store.get(request_id=request_id, tenant_id="account-b") is None

    async def listed_for(account_id):
        envelope = sign_service_context(
            "backend-secret",
            audience="distributed-search-mcp",
            account_id=account_id,
            principal_id=f"principal-{account_id}",
            issuer="https://auth.distributed.systems",
            identity_kind="customer",
        )
        response = await server.handle_tools_call({
            "name": "list_search_requests",
            "arguments": {"_dsco_context": envelope},
        })
        return json.loads(response["content"][0]["text"])

    assert asyncio.run(listed_for("account-a"))["count"] == 1
    assert asyncio.run(listed_for("account-b"))["count"] == 0

    with pytest.raises(ValueError, match="signed delegated"):
        asyncio.run(server.handle_tools_call({"name": "list_search_requests", "arguments": {}}))


def test_answer_uses_router_and_preserves_retrieval_lineage(tmp_path, monkeypatch):
    monkeypatch.setenv("SEARCH_REQUESTS_DB", str(tmp_path / "answer.db"))
    server = create_server(api_key="test-key", registry_url="http://127.0.0.1:9")

    async def fake_retrieve(_args):
        full = {
            "results": [
                {"title": "Primary", "url": "https://example.test/source", "content": "Ground truth."}
            ]
        }
        request_id, response_id = server._record_request(
            "retrieve", {"query": "question"}, full, "retrieve", sources=["https://example.test/source"]
        )
        return {
            "success": True,
            "request_id": request_id,
            "response_id": response_id,
            "results": full["results"],
            "quality": {"unique_domains": 1},
            "timings_ms": {"total": 12.5},
        }

    class Router:
        configured = True

        async def synthesize(self, **kwargs):
            assert kwargs["evidence"][0]["content"] == "Ground truth."
            assert kwargs["max_evidence_chars"] == 8000
            return {
                "answer": "Grounded answer [1].",
                "route": "dsco-managed",
                "router_request_id": kwargs["trace_id"],
                "usage": {"total_tokens": 42},
            }

    monkeypatch.setattr(server, "_execute_retrieve", fake_retrieve)
    server._router = Router()
    result = asyncio.run(server._execute_answer({"query": "question"}))

    assert result["success"] is True
    assert result["answer"] == "Grounded answer [1]."
    assert result["sources"] == [
        {"citation": 1, "title": "Primary", "url": "https://example.test/source"}
    ]
    assert result["retrieval_request_id"]
    stored = server._store.get(request_id=result["request_id"])
    assert stored["full_result"]["retrieval_request_id"] == result["retrieval_request_id"]
