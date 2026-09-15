import json

import pytest

from tool_management.integrations import mcp_server as m
from tool_management.integrations.mcp_server import MCPServer


class _FakeTool:
    pass


class _CatalogStore:
    def __init__(self, rows, *, version="v1"):
        self.rows = list(rows)
        self.version = version
        self.query_calls = 0
        self.version_calls = 0
        self.lookup_calls = []
        self.fail_query = False
        self.fail_version = False

    def query_tools(self, *, limit, offset, columns, unique_names=False, include_total=True):
        self.query_calls += 1
        if self.fail_query:
            raise RuntimeError("query unavailable")
        rows = self.rows[offset : offset + limit]
        if unique_names:
            seen = set()
            rows = [row for row in rows if not (row["name"] in seen or seen.add(row["name"]))]
            total = len({row["name"] for row in self.rows})
        else:
            total = len(self.rows)
        return rows, total

    def catalog_version(self):
        self.version_calls += 1
        if self.fail_version:
            raise RuntimeError("version unavailable")
        return self.version

    def get_tool_by_name(self, name):
        self.lookup_calls.append(name)
        if any(row["name"] == name or row["tool_id"] == name for row in self.rows):
            return _FakeTool()
        return None

    def get_tool(self, tool_id):
        self.lookup_calls.append(tool_id)
        if any(row["tool_id"] == tool_id for row in self.rows):
            return _FakeTool()
        return None


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, payload=None):
        self.posts = []
        self.payload = payload or {
            "status": "completed",
            "result": {"ok": True},
            "latency_ms": 7,
        }

    async def post(self, url, **kwargs):
        self.posts.append({"url": url, **kwargs})
        return _FakeResponse(self.payload)


def _row(name, *, backend="backend"):
    return {
        "tool_id": f"{backend}:{name}",
        "name": name,
        "description": f"{name} description",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
        },
        "output_schema": {},
        "category": "test",
        "tags": ["test"],
        "backend_id": backend,
        "version": "1.0.0",
    }


@pytest.fixture(autouse=True)
def _reset_tools_list_cache(monkeypatch):
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE", "1")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE_TTL_MS", "1000")
    with m._TOOLS_LIST_CACHE_LOCK:
        m._TOOLS_LIST_CACHE.clear()
    yield
    with m._TOOLS_LIST_CACHE_LOCK:
        m._TOOLS_LIST_CACHE.clear()


@pytest.mark.asyncio
async def test_advertised_catalog_tool_direct_call_uses_canonical_execute(monkeypatch):
    store = _CatalogStore([_row("weather_probe")])
    client = _FakeClient()
    async def _get_client():
        return client

    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m, "get_http_client", _get_client)
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")

    server = MCPServer(tool_storage={})
    advertised = await server._handle_tools_list({}, "operator-session")
    assert "weather_probe" in {tool["name"] for tool in advertised["tools"]}

    result = await server._handle_tools_call(
        {"name": "weather_probe", "arguments": {"location": "Boston"}},
        "operator-session",
    )

    assert result["isError"] is False
    assert len(client.posts) == 1
    post = client.posts[0]
    assert post["url"].endswith("/api/v1/execute")
    assert post["json"]["tool_name"] == "weather_probe"
    assert post["json"]["arguments"] == {"location": "Boston"}
    assert post["json"]["execution_mode"] == "auto"
    payload = json.loads(result["content"][0]["text"])
    assert payload["tool"] == "weather_probe"
    assert payload["result"] == {"ok": True}


@pytest.mark.asyncio
async def test_customer_session_direct_catalog_call_is_denied_before_execution(monkeypatch):
    store = _CatalogStore([_row("weather_probe")])
    client = _FakeClient()
    async def _get_client():
        return client

    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m, "get_http_client", _get_client)

    result = await MCPServer(tool_storage={})._handle_tools_call(
        {"name": "weather_probe", "arguments": {"location": "Boston"}},
        "customer:acct-1:session",
    )

    payload = json.loads(result["content"][0]["text"])
    assert result["isError"] is True
    assert payload["error"] == "customer_mcp_tool_denied"
    assert payload["allowed_tools"] == ["execute_tool", "search_tools"]
    assert store.lookup_calls == []
    assert client.posts == []


@pytest.mark.asyncio
async def test_raw_customer_execute_tool_session_fails_closed_without_route_identity(monkeypatch):
    store = _CatalogStore([_row("weather_probe")])
    client = _FakeClient()
    async def _get_client():
        return client

    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m, "get_http_client", _get_client)

    result = await MCPServer(tool_storage={})._handle_tools_call(
        {
            "name": "execute_tool",
            "arguments": {
                "tool_name": "weather_probe",
                "arguments": {"location": "Boston"},
            },
        },
        "customer:acct-1:session",
    )

    payload = json.loads(result["content"][0]["text"])
    assert result["isError"] is True
    assert payload["error"] == "customer_mcp_route_required"
    assert client.posts == []


@pytest.mark.asyncio
async def test_unknown_direct_tool_name_still_returns_unknown_meta_tool(monkeypatch):
    store = _CatalogStore([])
    client = _FakeClient()
    async def _get_client():
        return client

    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m, "get_http_client", _get_client)

    result = await MCPServer(tool_storage={})._handle_tools_call(
        {"name": "missing_tool", "arguments": {"query": "x"}},
        "operator-session",
    )

    payload = json.loads(result["content"][0]["text"])
    assert result["isError"] is True
    assert payload["error"] == "Unknown meta-tool: missing_tool"
    assert client.posts == []


@pytest.mark.asyncio
async def test_catalog_name_collision_keeps_meta_tool_semantics(monkeypatch):
    store = _CatalogStore([_row("get_tool")])
    client = _FakeClient()
    async def _get_client():
        return client

    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m, "get_http_client", _get_client)

    result = await MCPServer(tool_storage={})._handle_tools_call(
        {"name": "get_tool", "arguments": {"tool_name": "missing_tool"}},
        "operator-session",
    )

    payload = json.loads(result["content"][0]["text"])
    assert result["isError"] is True
    assert payload["error"] == "Tool not found: missing_tool"
    assert client.posts == []


@pytest.mark.asyncio
async def test_tools_list_warm_cache_skips_query_and_version_checks_within_ttl(monkeypatch):
    store = _CatalogStore([_row(f"tool_{i}") for i in range(5)])
    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE_TTL_MS", "60000")

    server = MCPServer(tool_storage={})
    await server._handle_tools_list({}, "session")
    assert store.query_calls == 1
    assert store.version_calls == 1
    store.query_calls = 0
    store.version_calls = 0

    for _ in range(10):
        page = await server._handle_tools_list({}, "session")
        assert "tool_0" in {tool["name"] for tool in page["tools"]}

    assert store.query_calls == 0
    assert store.version_calls == 0


@pytest.mark.asyncio
async def test_tools_list_revalidates_after_ttl_and_observes_version_change(monkeypatch):
    now = [100.0]
    store = _CatalogStore([_row("old_tool")], version="v1")
    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m.time, "monotonic", lambda: now[0])
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE_TTL_MS", "1000")

    server = MCPServer(tool_storage={})
    await server._handle_tools_list({}, "session")
    now[0] += 0.5
    await server._handle_tools_list({}, "session")
    assert store.query_calls == 1
    assert store.version_calls == 1

    store.rows = [_row("new_tool")]
    store.version = "v2"
    now[0] += 1.1
    page = await server._handle_tools_list({}, "session")

    assert store.query_calls == 2
    assert store.version_calls == 2
    names = {tool["name"] for tool in page["tools"]}
    assert "new_tool" in names
    assert "old_tool" not in names


@pytest.mark.asyncio
async def test_tools_list_cache_is_isolated_by_store_identity(monkeypatch):
    stores = [
        _CatalogStore([_row("store_a_tool")], version="same"),
        _CatalogStore([_row("store_b_tool")], version="same"),
    ]
    monkeypatch.setattr(m, "_get_tool_store", lambda: stores[0])
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE_TTL_MS", "60000")

    server = MCPServer(tool_storage={})
    first = await server._handle_tools_list({}, "session")
    monkeypatch.setattr(m, "_get_tool_store", lambda: stores[1])
    second = await server._handle_tools_list({}, "session")

    assert "store_a_tool" in {tool["name"] for tool in first["tools"]}
    assert "store_b_tool" in {tool["name"] for tool in second["tools"]}
    assert stores[0].query_calls == 1
    assert stores[1].query_calls == 1


@pytest.mark.asyncio
async def test_tools_list_cache_disabled_does_not_check_versions(monkeypatch):
    store = _CatalogStore([_row("fresh_tool")])
    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE", "0")

    server = MCPServer(tool_storage={})
    await server._handle_tools_list({}, "session")
    await server._handle_tools_list({}, "session")

    assert store.query_calls == 2
    assert store.version_calls == 0


@pytest.mark.asyncio
async def test_tools_list_refresh_failure_serves_stale_cached_page(monkeypatch):
    now = [100.0]
    store = _CatalogStore([_row("stale_tool")], version="v1")
    monkeypatch.setattr(m, "_get_tool_store", lambda: store)
    monkeypatch.setattr(m.time, "monotonic", lambda: now[0])
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE_TTL_MS", "1000")

    server = MCPServer(tool_storage={})
    await server._handle_tools_list({}, "session")
    store.version = "v2"
    store.fail_query = True
    now[0] += 1.1

    page = await server._handle_tools_list({}, "session")

    assert store.query_calls == 2
    assert store.version_calls == 2
    assert "stale_tool" in {tool["name"] for tool in page["tools"]}
