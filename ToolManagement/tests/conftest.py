"""Test-session bootstrap.

Runs before any test module (and therefore before tool_management.oauth) is
imported, so the environment is in place when oauth reads it at module load:

  * REQUIRE_BEARER_AUTH=0  — global gate off for the in-process app.
  * DEFAULT_BEARER_TOKEN   — a fixed token the local app will accept, so tests can
    authenticate to the always-protected /mcp meta-tools endpoint without a real
    OAuth flow or a production secret.

These only affect the in-process test app. Live-deployment runs (TEST_BASE_URL set)
use TEST_BEARER_TOKEN instead and never see these.
"""
import os
import tempfile

# Isolate the persistent ToolStore database per pytest invocation.
# tool_store.py resolves DATA_DIR at import time (default: repo-relative
# data/tools.duckdb), so without this, stale rows from previous runs or local
# dev leak into the suite — duplicate-backend registration failures,
# phantom analytics counts ("assert 10 == 2"), and order-dependent flaps.
# conftest.py is imported before any test module, so this lands first.
if "DATA_DIR" not in os.environ:
    os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="dsco-tools-test-")

# Unit tests must not consume live embedding quota or flake on provider rate
# limits (observed: Jina 429 mid-suite). The app falls back to deterministic
# local-hash embeddings when no key is set, keeping semantic-search paths
# exercised without the network. Live-embedding behavior stays covered by
# integration runs against TEST_BASE_URL.
os.environ.pop("JINA_API_KEY", None)

os.environ.setdefault("REQUIRE_BEARER_AUTH", "0")
os.environ.setdefault("DEFAULT_BEARER_TOKEN", "test-local-bearer-token")

# Exposed for fixtures that build the in-process client.
LOCAL_TEST_BEARER = os.environ["DEFAULT_BEARER_TOKEN"]


def seed_offline_registry():
    """Register the four core backends' canonical tools into the ToolStore.

    In production, tools enter the persistent store when native MCP managers
    connect to live Modal servers and sync their catalogs. Offline tests can't
    do that, so we seed the same tool names deterministically. Idempotent.
    """
    from tool_management.persistence import RegisteredTool, get_tool_store

    catalog = {
        "distributed-search-mcp": [
            ("search", "search the web and documents; find information, results, links"),
            ("read", "read and fetch a URL or document content, extract text"),
            ("verify", "verify claims against sources; fact-check search results"),
            ("rerank", "rerank candidate results by relevance to a query"),
            ("embed", "create vector embeddings for text"),
        ],
        "distributed-research-mcp": [
            ("deep_search", "deep research across sources; multi-step web research and synthesis"),
            ("run_task", "run an async research task in the background"),
            ("extract_data", "extract structured data from pages and documents"),
            ("find_all", "find all occurrences of entities across sources"),
            ("synthesize", "synthesize findings from multiple sources into a report"),
        ],
        "distributed-memory-mcp": [
            ("consolidate", "consolidate and compress stored memories into summaries"),
            ("working_memory", "store and retrieve working memory items for the agent"),
            ("episodic_memory", "store and retrieve episodic memories of past events"),
            ("semantic_memory", "store and retrieve semantic memories, facts and knowledge"),
        ],
        "distributed-publish-mcp": [
            ("create_webhook", "create a webhook to receive events and notifications"),
            ("events", "publish and subscribe to events on the message queue"),
            ("notifications", "send notifications to subscribers"),
            ("substack_publish", "publish drafts and notes to Substack"),
        ],
    }
    urls = {
        "distributed-search-mcp": os.getenv("SEARCH_MCP_URL", "https://arthurcolle--distributed-search-mcp-web.modal.run"),
        "distributed-research-mcp": os.getenv("RESEARCH_MCP_URL", "https://arthurcolle--distributed-research-mcp-web.modal.run"),
        "distributed-memory-mcp": os.getenv("MEMORY_MCP_URL", "https://arthurcolle--distributed-memory-mcp-web.modal.run"),
        "distributed-publish-mcp": os.getenv("PUBLISH_MCP_URL", "https://arthurcolle--distributed-publish-mcp-web.modal.run"),
    }
    store = get_tool_store()
    for backend_id, tools in catalog.items():
        cat = backend_id.replace("distributed-", "").replace("-mcp", "")
        for name, desc in tools:
            store.register_tool(RegisteredTool(
                tool_id=f"{backend_id}:{name}",
                name=name,
                backend_id=backend_id,
                backend_url=urls[backend_id],
                description=desc,
                input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
                tags=[cat],
                category=cat,
            ))
        store.update_backend_health(backend_id, True)
    # Populate the semantic advanced index offline (local-hash embedder) so
    # search_tools / list_tool_categories meta-tools have content. In prod
    # this happens via reindex_all on live catalogs; here we run it against
    # the seeded store in the fixture thread's own event loop.
    import asyncio
    from tool_management.api.advanced_search_routes import reindex_all
    asyncio.run(reindex_all())
    # Refresh the MCP catalog snapshot: lifespan warmed it against the store
    # BEFORE seeding, so it cached an empty catalog for the whole session.
    from tool_management.integrations.mcp_server import warm_tools_list_snapshot
    warm_tools_list_snapshot()


import pytest


@pytest.fixture(autouse=True)
def _reset_global_singletons():
    """Reset process-global singletons between tests.

    Several core objects are lazily-built process-wide singletons whose state
    binds to whichever test constructed them first:

      * ToolStore.__new__ caches cls._instance, so a store built against one
        test's tmp_path keeps serving every later test (and writes into a
        directory pytest may have already torn down).
      * get_tool_store()'s module-global mirrors the same instance.
      * The embedding services hold async HTTP clients bound to an event loop;
        once that loop closes ("Event loop is closed"), every later test that
        touches semantic search fails.

    Clearing them per-test makes each construction fresh and loop-clean.
    """
    import asyncio

    import tool_management.embeddings.openrouter_embeddings as emb
    import tool_management.persistence.tool_store as tool_store_module

    yield

    # Close any embedding clients holding transports before dropping them.
    for attr in ("_embedding_service", "_resilient_embedding_service"):
        emb_mod = getattr(emb, attr, None)
        if emb_mod is not None:
            close = getattr(emb_mod, "close", None)
            if callable(close):
                try:
                    result = close()
                    import inspect
                    if inspect.iscoroutine(result) and asyncio.get_event_loop_policy().get_event_loop().is_running() is False:
                        try:
                            asyncio.get_event_loop().run_until_complete(result)
                        except RuntimeError:
                            result.close()
                except Exception:
                    pass
            setattr(emb, attr, None)

    # Drop every cached wrapper that captured an embedding service or store
    # connection at construction time — otherwise a stale index keeps serving
    # a client we just closed ("Cannot send a request, as the client has been
    # been closed").
    import tool_management.embeddings.advanced_tool_index as ati
    if hasattr(ati, "_advanced_index"):
        ati._advanced_index = None

    # NOTE: deliberately do NOT reset ToolStore._instance / _conn here.
    # Several suites use a session-scoped TestClient over the shared global
    # app, whose registered backends live in the process-wide store; tearing
    # it down per-test amputates their state. Tests that need isolation
    # (e.g. test_tool_store_rich_analytics) reset it in their own fixtures.

