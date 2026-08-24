import time

from tool_management.persistence import tool_store as tool_store_module
from tool_management.persistence.tool_store import RegisteredTool


def _fresh_store(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_store_module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(tool_store_module, "DB_PATH", tmp_path / "tools.duckdb")
    tool_store_module.ToolStore._instance = None
    old_conn = getattr(tool_store_module.ToolStore, "_conn", None)
    if old_conn is not None:
        try:
            old_conn.close()
        except Exception:
            pass
        tool_store_module.ToolStore._conn = None
    tool_store_module._tool_store = None
    return tool_store_module.ToolStore()


def _register_baseline_tools(store):
    store.register_backend(
        backend_id="distributed-search-mcp",
        name="Search MCP",
        url="http://search.local",
    )
    store.register_backend(
        backend_id="distributed-memory-mcp",
        name="Memory MCP",
        url="http://memory.local",
    )
    store.register_tool(
        RegisteredTool(
            tool_id="distributed-search-mcp:search",
            name="search",
            backend_id="distributed-search-mcp",
            backend_url="http://search.local",
            description="Search the web",
            input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
        )
    )
    store.register_tool(
        RegisteredTool(
            tool_id="distributed-search-mcp:read",
            name="read",
            backend_id="distributed-search-mcp",
            backend_url="http://search.local",
            description="Read URL",
            input_schema={"type": "object", "properties": {"url": {"type": "string"}}},
        )
    )
    store.register_tool(
        RegisteredTool(
            tool_id="distributed-memory-mcp:store_belief",
            name="store_belief",
            backend_id="distributed-memory-mcp",
            backend_url="http://memory.local",
            description="Store a memory",
            input_schema={"type": "object", "properties": {"belief": {"type": "string"}}},
        )
    )


def test_tool_latency_metrics_and_failure_modes(tmp_path, monkeypatch):
    store = _fresh_store(tmp_path, monkeypatch)
    _register_baseline_tools(store)

    store.record_call(
        "distributed-search-mcp:search",
        120.0,
        True,
        execution_mode="sync",
        trace_id="tr_a",
    )
    store.record_call(
        "distributed-search-mcp:search",
        240.0,
        True,
        execution_mode="job",
        trace_id="tr_a",
    )
    store.record_call(
        "distributed-search-mcp:search",
        600.0,
        False,
        execution_mode="job",
        error_code="HTTP_502",
        error_message="bad gateway",
    )
    time.sleep(0.01)
    store.record_call(
        "distributed-search-mcp:search",
        400.0,
        False,
        execution_mode="sync",
        error_code="TIMEOUT",
        error_message="timed out",
    )

    metrics = store.get_tool_latency_metrics("search", hours=24)
    assert metrics is not None
    assert metrics["calls"] == 4
    assert metrics["failures"] == 2
    assert metrics["successes"] == 2
    assert metrics["p95_latency_ms"] >= metrics["p50_latency_ms"]
    assert metrics["last_error"]["code"] in {"TIMEOUT", "HTTP_502"}

    failures = store.get_failure_modes(hours=24, tool_ref="search", limit=10)
    codes = {row["error_code"] for row in failures["modes"]}
    assert "TIMEOUT" in codes
    assert "HTTP_502" in codes


def test_tool_usage_timeseries(tmp_path, monkeypatch):
    store = _fresh_store(tmp_path, monkeypatch)
    _register_baseline_tools(store)

    for latency in (100.0, 110.0, 130.0):
        store.record_call("distributed-search-mcp:read", latency, True, execution_mode="sync")
    store.record_call(
        "distributed-search-mcp:read",
        350.0,
        False,
        execution_mode="sync",
        error_code="HTTP_500",
        error_message="internal",
    )

    series = store.get_tool_usage_timeseries(hours=24, bucket_minutes=60, tool_ref="read")
    assert series["tool_id"] == "distributed-search-mcp:read"
    assert len(series["series"]) >= 1
    total_calls = sum(bucket["calls"] for bucket in series["series"])
    assert total_calls == 4


def test_upsert_and_list_tool_dependencies(tmp_path, monkeypatch):
    store = _fresh_store(tmp_path, monkeypatch)
    _register_baseline_tools(store)

    store.upsert_tool_dependency(
        source_tool_ref="search",
        target_tool_ref="read",
        dependency_type="explicit",
        confidence=0.9,
        call_count=3,
        metadata={"source": "unit-test"},
    )
    store.upsert_tool_dependency(
        source_tool_ref="distributed-search-mcp:search",
        target_tool_ref="distributed-search-mcp:read",
        dependency_type="explicit",
        confidence=0.7,
        call_count=2,
        metadata={"source": "unit-test-update"},
    )

    deps = store.list_tool_dependencies(tool_ref="search", dependency_type="explicit")
    assert len(deps) == 1
    edge = deps[0]
    assert edge["source_tool_id"] == "distributed-search-mcp:search"
    assert edge["target_tool_id"] == "distributed-search-mcp:read"
    assert edge["call_count"] == 5
    assert edge["confidence"] >= 0.9


def test_backend_latency_metrics(tmp_path, monkeypatch):
    store = _fresh_store(tmp_path, monkeypatch)
    _register_baseline_tools(store)

    store.record_call("distributed-search-mcp:search", 100.0, True, execution_mode="sync")
    store.record_call("distributed-search-mcp:read", 200.0, True, execution_mode="job")
    store.record_call(
        "distributed-memory-mcp:store_belief",
        320.0,
        False,
        execution_mode="sync",
        error_code="HTTP_500",
        error_message="failed",
    )

    metrics = store.get_backend_latency_metrics(hours=24)
    assert "distributed-search-mcp" in metrics
    assert "distributed-memory-mcp" in metrics
    assert metrics["distributed-search-mcp"]["calls"] == 2
    assert metrics["distributed-memory-mcp"]["failures"] == 1
