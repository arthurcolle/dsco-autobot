"""Exercise the actual canonical execution function at its transport seam."""
import ast
import json
import logging
from pathlib import Path
import sys
import time
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, Optional
import os

import httpx
import pytest


@pytest.fixture
def canonical(monkeypatch):
    source = Path(__file__).parents[1] / "src" / "tool_management" / "api" / "tools_api.py"
    tree = ast.parse(source.read_text())
    function = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "execute_tool")
    records, transport, fallback, breaker = [], [], [], []

    class Store:
        def resolve_tool(self, tool):
            if tool == "local":
                raise ValueError("unknown remote tool")
            return "example", "https://worker.example", "read"
        def record_call(self, *args, **kwargs):
            records.append((args, kwargs))

    class Breaker:
        def allow_request(self, backend):
            return True, {}
        def record_success(self, backend):
            breaker.append("success")
        def record_failure(self, backend, **kwargs):
            breaker.append("failure")

    async def local_execute(**kwargs):
        fallback.append(kwargs)
        return {"local": True}
    registry = SimpleNamespace(get_by_name=lambda name: SimpleNamespace(metadata=None, execute=local_execute))
    modules = {
        "_sync_contract.registry": {"get_registry": lambda: registry},
        "_sync_contract.integrations.provider_catalog": {"provider_for_tool": lambda name: None},
    }
    for name, data in modules.items():
        module = ModuleType(name)
        module.__dict__.update(data)
        monkeypatch.setitem(sys.modules, name, module)
    ns = {"__name__": "_sync_contract.api.tools_api", "__package__": "_sync_contract.api",
          "ToolCall": SimpleNamespace, "ToolResult": SimpleNamespace, "Optional": Optional,
          "get_tool_store": lambda: Store(), "get_backend_circuit_breaker": lambda: Breaker(),
          "time": time, "os": os, "httpx": httpx, "json": json,
          "logger": logging.getLogger(__name__), "OUTPUT_GUARD_AVAILABLE": False,
          "_is_write_tool": lambda *args: False}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), ns)
    def make(response, *, customer=False, tool="example:read"):
        async def invoke(*args, **kwargs):
            transport.append((args, kwargs))
            if isinstance(response, Exception):
                raise response
            return 200, response
        ns["_mcp_tools_call"] = invoke
        call = SimpleNamespace(tool=tool, arguments={}, tenant_id="account-1" if customer else None,
            idempotency_key=None, trace_id=None, session_id=None, parent_execution_id=None,
            timeout=1, _execution_identity={"kind": "customer" if customer else "operator_service"})
        return ns["execute_tool"](call)
    return make, records, transport, fallback, breaker


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [[{"type": "text", "text": "provider quota exhausted"}], None, "malformed"])
async def test_explicit_tool_error_is_failed_without_transport_penalty(canonical, content):
    make, records, transport, fallback, breaker = canonical
    result = await make({"result": {"isError": True, "content": content}})
    assert result.success is False
    assert records[0][0][2] is False
    assert records[0][1]["error_code"] == "TOOL_EXECUTION_FAILED"
    assert breaker == ["success"]
    assert len(transport) == 1 and not fallback


@pytest.mark.asyncio
async def test_invalid_backend_response_never_replays_local_action(canonical):
    make, records, transport, fallback, _ = canonical
    result = await make(ValueError("JSON response truncated after side effect"))
    assert not result.success and "BACKEND_RESULT_INVALID" in result.error
    assert len(transport) == 1 and not fallback
    assert records[0][1]["error_code"] == "BACKEND_RESULT_INVALID"


@pytest.mark.asyncio
async def test_unknown_remote_tool_still_resolves_valid_local_action(canonical):
    make, _, transport, fallback, _ = canonical
    result = await make({}, tool="local")
    assert result.success and result.result == {"local": True}
    assert not transport and len(fallback) == 1


@pytest.mark.asyncio
async def test_customer_credential_store_failure_blocks_provider_invocation(canonical, monkeypatch):
    make, _, transport, fallback, _ = canonical
    provider = sys.modules["_sync_contract.integrations.provider_catalog"]
    provider.provider_for_tool = lambda name: "weatherapi"
    store_module = ModuleType("_sync_contract.services.tenant_credentials")
    def unavailable():
        raise RuntimeError("credential store offline")
    store_module._default_store = unavailable
    monkeypatch.setitem(sys.modules, store_module.__name__, store_module)
    result = await make({"result": {}}, customer=True)
    assert not result.success and "CREDENTIAL_RESOLUTION_FAILED" in result.error
    assert not transport and not fallback
