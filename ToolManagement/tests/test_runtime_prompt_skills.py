"""Protocol regressions using real handlers and isolated dependency doubles.

The snapshot in this repair workspace is sparse. Load handler files directly
and replace only unavailable registry/auth/store dependencies in semantic
routes; this exercises real protocol dispatch, not deployed provider calls.
"""

import asyncio
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest
from fastapi import Request
from fastapi.responses import JSONResponse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mcp_module():
    return _load("_prompt_skill_mcp_server", ROOT / "src/tool_management/integrations/mcp_server.py")


def _call(server, mcp_module, method, params=None):
    request = mcp_module.MCPRequest(id=1, method=method, params=params or {})
    return asyncio.run(server.handle_request(request, "test")).model_dump(exclude_none=True)


def test_standard_prompt_discovery_and_rendering(mcp_module):
    server = mcp_module.MCPServer({})
    initialized = _call(server, mcp_module, "initialize")["result"]
    assert initialized["capabilities"] == {"tools": {}, "prompts": {}, "resources": {}}
    prompts = _call(server, mcp_module, "prompts/list")["result"]["prompts"]
    assert {p["name"] for p in prompts} == {"tools_discovery", "verified_tool_execution"}
    for prompt in prompts:
        arguments = {arg["name"]: f"Test {arg['name']}" for arg in prompt["arguments"]}
        result = _call(server, mcp_module, "prompts/get", {"name": prompt["name"], "arguments": arguments})["result"]
        assert result["messages"][0]["content"]["type"] == "text"
        assert "Test task" in result["messages"][0]["content"]["text"]


@pytest.mark.parametrize("params", [
    {"name": "unknown", "arguments": {}},
    {"name": "tools_discovery"},
    {"name": "tools_discovery", "arguments": {"task": 1}},
    {"name": "tools_discovery", "arguments": {"task": " "}},
    {"name": "tools_discovery", "arguments": {"task": "weather", "injected": "value"}},
    {"name": "verified_tool_execution", "arguments": {"tool_name": "weather", "task": "read"}},
])
def test_prompt_get_rejects_invalid_parameters(mcp_module, params):
    response = _call(mcp_module.MCPServer({}), mcp_module, "prompts/get", params)
    assert response["error"]["code"] == -32602
    assert "result" not in response


def test_skill_resources_preserve_legacy_tool_resources(mcp_module):
    tool = {"id": "known", "name": "Known", "has_ui_effects": True}
    server = mcp_module.MCPServer({"known": tool})
    resources = _call(server, mcp_module, "resources/list")["result"]["resources"]
    uris = {resource["uri"] for resource in resources}
    assert uris == {
        "dsco://skills/tool-discovery", "dsco://skills/verified-execution",
        "tool://known/config", "tool://known/ui",
    }
    for uri in sorted(uris):
        content = _call(server, mcp_module, "resources/read", {"uri": uri})["result"]["contents"][0]
        assert content["uri"] == uri
        assert content["text"]
    assert _call(server, mcp_module, "resources/read", {"uri": "dsco://skills/../../etc/passwd"})["error"]["code"] == -32602


def test_native_rollout_support_and_empty_notification(tmp_path):
    from native.base import NativeMCPBase

    server = NativeMCPBase(port=0, data_dir=str(tmp_path))

    def call(method, params=None):
        response = asyncio.run(server._handle_mcp_request({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}))
        return json.loads(response.body)

    assert call("initialize")["result"]["capabilities"] == {"tools": {}, "prompts": {}, "resources": {}}
    assert len(call("prompts/list")["result"]["prompts"]) == 2
    assert call("prompts/get", {"name": "tools_discovery", "arguments": {"task": "DC weather"}})["result"]["messages"]
    assert call("prompts/get", {"name": "missing"})["error"]["code"] == -32602
    resources = call("resources/list")["result"]["resources"]
    for resource in resources:
        assert call("resources/read", {"uri": resource["uri"]})["result"]["contents"][0]["text"]
    assert call("resources/read", {"uri": "file:///etc/passwd"})["error"]["code"] == -32602
    response = asyncio.run(server._handle_mcp_request({"jsonrpc": "2.0", "method": "notifications/initialized"}))
    assert response.status_code == 202
    assert response.body == b""


def test_native_http_prompt_and_skill_access_requires_backend_auth(tmp_path, monkeypatch):
    import httpx
    from native.base import NativeMCPBase

    monkeypatch.setenv("NATIVE_MCP_REQUIRE_BEARER", "1")
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "regression-backend-token")
    server = NativeMCPBase(port=0, data_dir=str(tmp_path))

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=server.app), base_url="http://native") as client:
            prompt_call = {"jsonrpc": "2.0", "id": 1, "method": "prompts/list"}
            assert (await client.post("/mcp", json=prompt_call)).status_code == 401
            headers = {"Authorization": "Bearer regression-backend-token"}
            reply = await client.post("/mcp", json=prompt_call, headers=headers)
            assert reply.status_code == 200
            assert len(reply.json()["result"]["prompts"]) == 2
            reply = await client.post("/mcp", headers=headers, json={
                "jsonrpc": "2.0", "id": 2, "method": "resources/read",
                "params": {"uri": "dsco://skills/verified-execution"},
            })
            assert reply.json()["result"]["contents"][0]["mimeType"] == "text/markdown"
            notification = await client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "method": "notifications/initialized"})
            assert notification.status_code == 202
            assert not notification.content

    asyncio.run(exercise())


def _request(body, token="test-tenant-token"):
    payload = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": payload, "more_body": False}

    headers = [(b"x-session-id", b"original-session"), (b"x-dsco-account-id", b"original-account")]
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    return Request({"type": "http", "method": "POST", "path": "/semantic-mcp", "headers": headers}, receive)


@pytest.fixture
def semantic_module(monkeypatch, mcp_module):
    package = "_semantic_protocol_repair"
    modules = {}
    for name in (package, f"{package}.api", f"{package}.integrations", f"{package}.embeddings"):
        module = types.ModuleType(name)
        module.__path__ = []
        modules[name] = module
    modules[f"{package}.integrations.mcp_server"] = mcp_module
    modules[f"{package}.registry"] = types.SimpleNamespace(get_registry=lambda: None)
    modules[f"{package}.queue"] = types.SimpleNamespace(get_task_queue=lambda: None)
    modules[f"{package}.oauth"] = types.SimpleNamespace(
        require_bearer=lambda request: request.headers.get("authorization"),
        make_401_response=lambda request: JSONResponse(status_code=401, content={"error": "auth_required"}),
    )
    store = types.SimpleNamespace(mark_accessed=lambda name: None)
    modules[f"{package}.embeddings.semantic_tool_store"] = types.SimpleNamespace(get_tool_store=lambda **kwargs: store)
    calls = []

    async def canonical(request):
        calls.append(request)
        return {"jsonrpc": "2.0", "id": (await request.json()).get("id"), "result": {"canonical": True}}

    modules[f"{package}.api.mcp_routes"] = types.SimpleNamespace(mcp_jsonrpc=canonical)
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    module = _load(f"{package}.api.semantic_mcp_routes", ROOT / "src/tool_management/api/semantic_mcp_routes.py")
    return module, store, calls


@pytest.mark.parametrize("method", ["tools/call", "prompts/list", "resources/read"])
def test_semantic_preserves_original_authenticated_request(semantic_module, method):
    module, store, calls = semantic_module
    request = _request({"jsonrpc": "2.0", "id": 1, "method": method, "params": {"name": "execute_tool"}})
    result = asyncio.run(module.semantic_mcp_jsonrpc(request))
    assert result["result"]["canonical"]
    assert calls == [request]
    assert calls[0].headers["authorization"] == "Bearer test-tenant-token"
    assert calls[0].headers["x-dsco-account-id"] == "original-account"


def test_semantic_execution_survives_access_telemetry_failure(semantic_module):
    module, store, calls = semantic_module

    def broken(name):
        raise RuntimeError("telemetry store down")

    store.mark_accessed = broken
    request = _request({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "execute_tool"}})
    assert asyncio.run(module.semantic_mcp_jsonrpc(request))["result"]["canonical"]
    assert calls == [request]


def test_semantic_rejects_missing_auth_and_delegates_notifications(semantic_module):
    module, store, calls = semantic_module
    request = _request({"jsonrpc": "2.0", "id": 1, "method": "prompts/list"}, token=None)
    result = asyncio.run(module.semantic_mcp_jsonrpc(request))
    assert result.status_code == 401
    assert not calls
    notification = _request({"jsonrpc": "2.0", "method": "notifications/initialized"})
    asyncio.run(module.semantic_mcp_jsonrpc(notification))
    assert calls == [notification]


def test_semantic_advertises_only_implemented_features(semantic_module, mcp_module):
    module, store, calls = semantic_module
    reply = asyncio.run(module._handle_initialize(mcp_module.MCPRequest(id=1, method="initialize"), "test"))
    assert reply.result["capabilities"] == {"tools": {"semanticSearch": True}, "prompts": {}, "resources": {}}


def test_capability_rest_route_matches_handshake():
    """Inspect the real route AST without importing its unavailable dependencies."""
    import ast

    tree = ast.parse((ROOT / "src/tool_management/api/mcp_routes.py").read_text())
    function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "mcp_capabilities")
    function.decorator_list = []
    module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    namespace = {}
    exec(compile(module, "mcp_capabilities", "exec"), namespace)
    result = asyncio.run(namespace["mcp_capabilities"]())
    assert result["capabilities"]["tools"] == {}
    assert result["capabilities"]["prompts"] == {}
    assert result["capabilities"]["resources"] == {}
