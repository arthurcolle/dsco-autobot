"""Release gates must reject catalog-only and synthetic success."""
import importlib.util
import json
from pathlib import Path

import httpx
import pytest

spec = importlib.util.spec_from_file_location("rollout_gate", Path(__file__).parents[1] / "scripts" / "verify_mcp_rollout.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def server():
    return {"id": "weather", "url": "https://weather.example/mcp", "required_features": ["prompts", "resources"],
        "probes": [{"tool": "current", "effect": "read", "arguments": {"id": "Washington, DC"}, "equals": {"mode": "live"}}],
        "prompts": [{"name": "tools_discovery", "arguments": {"query": "weather"}}],
        "skill_uris": ["skill://tool-management/discovery"]}


@pytest.mark.parametrize("body", [
    {"mode": "sandbox"}, {"mode": "live", "object": {"synthetic": True}},
    {"mode": "live", "nested": [{"mode": "mock"}]}, {"mode": "live", "error": "failed"},
    {"status": "failed"}, {"mode": "live", "object": {}},
])
def test_unqualified_results_fail(body):
    with pytest.raises(gate.VerificationError):
        gate.assert_contract(body, {"mode": "live", "object.location": "Washington"})


def test_explicit_tool_failure_is_not_a_transport_success():
    with pytest.raises(gate.VerificationError):
        gate.decoded_body({"isError": True, "content": [{"type": "text", "text": '{"mode":"live"}'}]})


@pytest.mark.parametrize("change", [
    {"url": "http://remote.example/mcp"}, {"url": "https://token:secret@host/mcp"},
    {"probes": []}, {"required_features": ["invented"]},
    {"probes": [{"tool": "send", "effect": "write", "equals": {"mode": "live"}}]},
    {"probes": [{"tool": "read", "effect": "read", "equals": {}}]},
])
def test_invalid_release_coverage_is_rejected(change):
    target = server()
    target.update(change)
    with pytest.raises(gate.VerificationError):
        gate.validate_manifest({"expected_server_ids": ["weather"], "servers": [target]})


def test_partial_inventory_does_not_pass_as_a_complete_rollout():
    with pytest.raises(gate.VerificationError, match="coverage"):
        gate.validate_manifest({"expected_server_ids": ["weather", "search"], "servers": [server()]})


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", [None, "prompt_missing", "skill_missing", "tool_error", "synthetic", "wrong_id"])
async def test_read_gate_checks_protocol_and_operations(broken):
    calls = []
    async def handler(request):
        body = json.loads(request.content)
        calls.append(body["method"])
        method = body["method"]
        if method == "notifications/initialized":
            assert request.headers["mcp-session-id"] == "test-session"
            return httpx.Response(202)
        result = {}
        if method == "initialize":
            result = {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}, "prompts": {}, "resources": {}}}
        elif method.endswith("/list"):
            result = {method.split("/")[0]: []}
        elif method == "tools/call":
            data = {"mode": "sandbox" if broken == "synthetic" else "live"}
            result = {"isError": broken == "tool_error", "content": [{"type": "text", "text": json.dumps(data)}]}
        elif method == "prompts/get":
            if broken == "prompt_missing":
                return httpx.Response(200, json={"id": body["id"], "error": {"code": -32601}})
            result = {"messages": [{"role": "user", "content": {"type": "text", "text": "Discover tools"}}]}
        elif method == "resources/read":
            result = {"contents": [] if broken == "skill_missing" else [{"uri": body["params"]["uri"], "text": "Lookup workflow"}]}
        return httpx.Response(200, json={"id": 999 if broken == "wrong_id" and method == "tools/call" else body["id"], "result": result},
            headers={"Mcp-Session-Id": "test-session"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        if broken:
            with pytest.raises(gate.VerificationError):
                await gate.verify_server(server(), client)
        else:
            report = await gate.verify_server(server(), client)
            assert report == {"id": "weather", "ok": True, "verified_tools": ["current"], "prompts_checked": 1, "skills_checked": 1}
    assert calls.count("tools/call") == 1
