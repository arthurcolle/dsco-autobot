import pytest

from tool_management.integrations.mcp_server import MCPServer


class _Store:
    def __init__(self, rows, version="v0"):
        self.rows = rows
        self._version = version
        self.query_calls = 0
        self.version_calls = 0

    def query_tools(self, *, limit, offset, columns, unique_names=False, include_total=True):
        self.query_calls += 1
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
        return f"{len(self.rows)}:{self._version}"


def _tool(index):
    return {
        "tool_id": f"backend:tool_{index}",
        "name": f"tool_{index}",
        "description": f"Tool {index}",
        "input_schema": {
            "type": "object",
            "properties": {"value": {"type": "integer"}},
        },
        "category": "test",
        "tags": ["catalog"],
        "backend_id": "backend",
    }


async def _drain_all_pages(server, page_env=None):
    """Walk nextCursor to completion and return the concatenated tool list."""
    tools = []
    seen_cursors = set()
    params = {}
    guard = 0
    while True:
        guard += 1
        assert guard < 1000, "pagination did not terminate"
        page = await server._handle_tools_list(dict(params), "session")
        tools.extend(page["tools"])
        cursor = page.get("nextCursor")
        if not cursor:
            break
        assert cursor not in seen_cursors, "cursor repeated — pagination loop"
        seen_cursors.add(cursor)
        params = {"cursor": cursor}
    return tools


@pytest.mark.asyncio
async def test_tools_list_advertises_complete_durable_catalog(monkeypatch):
    """The full catalog is reachable across pages under the new bounded default."""
    rows = [_tool(i) for i in range(2501)]
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: _Store(rows)
    )
    monkeypatch.delenv("MCP_TOOLS_LIST_PAGE_SIZE", raising=False)

    server = MCPServer(tool_storage={})
    all_tools = await _drain_all_pages(server)
    names = [tool["name"] for tool in all_tools]

    # 2501 catalog tools + 33 meta-tools, all reachable via pagination.
    assert len(names) > 2500
    assert "search_tools" in names
    assert "tool_0" in names
    assert "tool_2500" in names
    # No duplicates across pages (meta-tools only advertised on page 1).
    assert len(names) == len(set(names))
    advertised = next(tool for tool in all_tools if tool["name"] == "tool_2500")
    assert advertised["inputSchema"]["properties"]["value"]["type"] == "integer"
    assert advertised["annotations"]["tool_id"] == "backend:tool_2500"


@pytest.mark.asyncio
async def test_tools_list_is_bounded_by_default(monkeypatch):
    """Default response is a bounded page, not the whole 2,600-tool catalog."""
    rows = [_tool(i) for i in range(2501)]
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: _Store(rows)
    )
    monkeypatch.delenv("MCP_TOOLS_LIST_PAGE_SIZE", raising=False)

    result = await MCPServer(tool_storage={})._handle_tools_list({}, "session")

    # 200 (default page) + 33 meta-tools on the first page.
    assert len(result["tools"]) <= 240
    assert "nextCursor" in result
    names = {tool["name"] for tool in result["tools"]}
    assert "search_tools" in names  # meta-tools ride the first page


@pytest.mark.asyncio
async def test_tools_list_legacy_unbounded_mode(monkeypatch):
    """MCP_TOOLS_LIST_PAGE_SIZE<=0 restores the single-page catalog."""
    rows = [_tool(i) for i in range(2501)]
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: _Store(rows)
    )
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "0")
    monkeypatch.setenv("MCP_CATALOG_READ_BATCH_SIZE", "700")

    result = await MCPServer(tool_storage={})._handle_tools_list({}, "session")
    names = [tool["name"] for tool in result["tools"]]

    assert len(names) > 2500
    assert "nextCursor" not in result
    assert "tool_2500" in names


@pytest.mark.asyncio
async def test_tools_list_supports_explicit_bounded_pagination(monkeypatch):
    rows = [_tool(i) for i in range(12)]
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: _Store(rows)
    )
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")

    server = MCPServer(tool_storage={})
    first = await server._handle_tools_list({}, "session")
    second = await server._handle_tools_list({"cursor": first["nextCursor"]}, "session")

    # First page: 10 catalog tools + 33 meta-tools.
    assert len([t for t in first["tools"] if t["name"].startswith("tool_")]) == 10
    assert any(t["name"] == "search_tools" for t in first["tools"])
    assert second["tools"]
    # Meta-tools must not reappear on the second page.
    assert not any(t["name"] == "search_tools" for t in second["tools"])
    assert {tool["name"] for tool in first["tools"]}.isdisjoint(
        {tool["name"] for tool in second["tools"]}
    )


@pytest.mark.asyncio
async def test_tools_list_bad_cursor_restarts_gracefully(monkeypatch):
    """A foreign/garbage cursor restarts at page 1 rather than erroring."""
    rows = [_tool(i) for i in range(12)]
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: _Store(rows)
    )
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")

    result = await MCPServer(tool_storage={})._handle_tools_list(
        {"cursor": "not-a-number"}, "session"
    )
    names = {tool["name"] for tool in result["tools"]}
    assert "search_tools" in names  # treated as first page


@pytest.fixture(autouse=True)
def _clear_tools_list_cache():
    """Isolate the module-level tools/list page cache between tests."""
    from tool_management.integrations import mcp_server as m
    with m._TOOLS_LIST_CACHE_LOCK:
        m._TOOLS_LIST_CACHE.clear()
    yield
    with m._TOOLS_LIST_CACHE_LOCK:
        m._TOOLS_LIST_CACHE.clear()


@pytest.mark.asyncio
async def test_tools_list_second_call_served_from_cache(monkeypatch):
    """A repeated page request hits the cache and does not re-query the store."""
    rows = [_tool(i) for i in range(50)]
    store = _Store(rows)
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: store
    )
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE", "1")

    server = MCPServer(tool_storage={})
    first = await server._handle_tools_list({}, "s")
    assert store.query_calls == 1
    second = await server._handle_tools_list({}, "s")
    # Served from cache: no additional query_tools call.
    assert store.query_calls == 1
    assert [t["name"] for t in first["tools"]] == [t["name"] for t in second["tools"]]
    # Returned object is a copy — mutating it must not poison the cache.
    second["tools"].append({"name": "poison"})
    third = await server._handle_tools_list({}, "s")
    assert all(t["name"] != "poison" for t in third["tools"])


@pytest.mark.asyncio
async def test_tools_list_cache_invalidated_on_catalog_change(monkeypatch):
    """A catalog-version change (new tool) busts the cached page."""
    rows = [_tool(i) for i in range(50)]
    store = _Store(rows, version="v0")
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: store
    )
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")

    server = MCPServer(tool_storage={})
    await server._handle_tools_list({}, "s")
    assert store.query_calls == 1

    # Catalog grows → version tag changes → cache miss → re-query.
    store.rows.append(_tool(999))
    await server._handle_tools_list({}, "s")
    assert store.query_calls == 2


@pytest.mark.asyncio
async def test_tools_list_cache_can_be_disabled(monkeypatch):
    """MCP_TOOLS_LIST_CACHE=0 forces a fresh query every call."""
    rows = [_tool(i) for i in range(50)]
    store = _Store(rows)
    monkeypatch.setattr(
        "tool_management.persistence.get_tool_store", lambda: store
    )
    monkeypatch.setenv("MCP_TOOLS_LIST_PAGE_SIZE", "10")
    monkeypatch.setenv("MCP_TOOLS_LIST_CACHE", "0")

    server = MCPServer(tool_storage={})
    await server._handle_tools_list({}, "s")
    await server._handle_tools_list({}, "s")
    assert store.query_calls == 2
