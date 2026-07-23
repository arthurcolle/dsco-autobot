"""Regression tests for two live-observed registry defects.

1. Backend SSE streams that emit `meta` then stall (or a proxy that drops the
   long-lived connection without a terminal `done`/`error` frame) must not wedge
   a job forever at `stream_meta_received`. The streaming executor bounds the
   idle wait between frames and fails over to the non-streaming /mcp path.

2. Integration-fabric tools must carry `category` + `tags` so the registry can
   filter/rank by category instead of dumping all 1,300+ tools into 'general'.
   The registry-side category filter must also repair legacy rows (registered
   before per-tool taxonomy existed) via name/description/tags patterns.
"""

import asyncio
import importlib.util
import json
import sqlite3
import sys
import types

import pytest


# --------------------------------------------------------------------------- #
# Fix 2/3: integration catalog taxonomy + registry category fallback
# --------------------------------------------------------------------------- #

def test_integration_catalog_tools_carry_category_and_tags():
    from native.integration_fabric.catalog import build_tools, build_canonical

    prim, _ = build_tools()
    canon, _ = build_canonical()

    assert prim and canon
    # Every generated tool must declare taxonomy.
    for t in prim + canon:
        assert t.get("category"), f"missing category: {t['name']}"
        assert t.get("tags"), f"missing tags: {t['name']}"

    weather = [t for t in prim if t["category"] == "weather"]
    assert weather, "expected weather primitives"
    ow = next(t for t in weather if t["name"] == "openweather_weather_current")
    assert "weather" in ow["tags"] and "openweather" in ow["tags"]

    wcanon = next(t for t in canon if t["name"] == "weather__current")
    assert wcanon["category"] == "weather"
    assert "canonical" in wcanon["tags"]


def _load_tool_store_isolated():
    """Import tool_store.py without triggering the package __init__ (duckdb)."""
    if "duckdb" not in sys.modules:
        sys.modules["duckdb"] = types.ModuleType("duckdb")
    spec = importlib.util.spec_from_file_location(
        "ts_isolated",
        "src/tool_management/persistence/tool_store.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_category_filter_repairs_legacy_general_rows():
    mod = _load_tool_store_isolated()

    params = []
    frag = mod._category_where("weather", params)
    # DuckDB cast -> plain text column for the sqlite harness.
    frag = frag.replace("tags::VARCHAR", "tags")

    con = sqlite3.connect(":memory:")
    con.execute(
        "create table tools(name text, category text, backend_id text, "
        "description text, tags text)"
    )
    con.executemany(
        "insert into tools values (?,?,?,?,?)",
        [
            # legacy: registered as 'general' before taxonomy existed
            ("openweather_weather_current", "general", "distributed-integration",
             "[openweather] current (read) in weather", json.dumps([])),
            ("weather__current", "general", "distributed-integration",
             "[canonical:weather] current (read) in weather (default provider: openweather)",
             json.dumps([])),
            ("accuweather_weather_forecast", "general", "distributed-integration",
             "[accuweather] forecast (read) in weather", json.dumps([])),
            # new taxonomy row (tags carry the category)
            ("x_weather_new", "general", "distributed-integration",
             "desc", json.dumps(["weather", "openweather"])),
            # unrelated
            ("slack_communication_send", "general", "distributed-integration",
             "[slack] send (action) in communication", json.dumps([])),
            ("search", "search", "distributed-search-mcp",
             "Search the web", json.dumps([])),
        ],
    )

    got = {r[0] for r in con.execute(f"select name from tools where {frag}", params)}

    assert {"openweather_weather_current", "weather__current",
            "accuweather_weather_forecast", "x_weather_new"} <= got
    assert "slack_communication_send" not in got
    assert "search" not in got


# --------------------------------------------------------------------------- #
# Fix 1: stalled backend stream must fail over instead of hanging
# --------------------------------------------------------------------------- #

def test_stream_reader_source_has_idle_timeout_guard():
    """Regression guard: the SSE read loop must bound the wait between frames
    with asyncio.wait_for(...) against the configurable idle timeout, and read
    that timeout from BACKEND_STREAM_IDLE_TIMEOUT_SECONDS. Without this a
    backend that emits `meta` then stalls wedges the job at
    `stream_meta_received` forever."""
    with open("src/tool_management/core/streaming.py", "r") as fh:
        src = fh.read()

    assert "BACKEND_STREAM_IDLE_TIMEOUT_SECONDS" in src
    assert "_stream_idle_timeout" in src
    # The read of each SSE line must be wrapped in a bounded wait.
    assert "asyncio.wait_for(" in src
    assert "_line_iter.__anext__()" in src
    assert "timeout=_stream_idle_timeout" in src


@pytest.mark.asyncio
async def test_bounded_sse_read_returns_on_stall():
    """Reproduce the exact bounded-read pattern from _stream_backend_execute:
    a stream that yields `meta` then stalls must, under an idle timeout, abandon
    the stream (return None => caller falls back) rather than block forever."""

    idle_timeout = 0.2

    async def _stalled_lines():
        yield "event: meta"
        yield 'data: {"call_id": "c_stall"}'
        yield ""
        await asyncio.Event().wait()  # stall forever

    async def read_with_idle_bound():
        it = _stalled_lines().__aiter__()
        saw_meta = False
        while True:
            try:
                line = await asyncio.wait_for(it.__anext__(), timeout=idle_timeout)
            except StopAsyncIteration:
                return "completed"
            except asyncio.TimeoutError:
                return "fallback"  # mirrors `return None` in production
            if line.startswith("event:") and line.split(":", 1)[1].strip() == "meta":
                saw_meta = True
        # unreachable

    # Must resolve to fallback quickly, well under any hang threshold.
    result = await asyncio.wait_for(read_with_idle_bound(), timeout=2.0)
    assert result == "fallback"


@pytest.mark.asyncio
async def test_bounded_sse_read_completes_when_done_arrives():
    """The bounded read must still complete normally when the backend delivers
    its terminal frame within the idle window (no false-positive fallback)."""

    idle_timeout = 1.0

    async def _good_lines():
        yield "event: meta"
        yield 'data: {"call_id": "c_ok"}'
        yield ""
        await asyncio.sleep(0.05)
        yield "event: done"
        yield 'data: {"result": {"ok": true}}'.replace("true", "1")
        yield ""

    async def read_with_idle_bound():
        it = _good_lines().__aiter__()
        done = False
        while True:
            try:
                line = await asyncio.wait_for(it.__anext__(), timeout=idle_timeout)
            except StopAsyncIteration:
                return "done" if done else "completed_no_terminal"
            except asyncio.TimeoutError:
                return "fallback"
            if line.startswith("event:") and line.split(":", 1)[1].strip() == "done":
                done = True

    result = await asyncio.wait_for(read_with_idle_bound(), timeout=2.0)
    assert result in ("done", "completed_no_terminal")
    assert result != "fallback"
