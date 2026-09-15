"""Exercise the actual stdio process and the canonical REST boundary."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "tool_management_mcp.py"
spec = importlib.util.spec_from_file_location("lazy_tool_management_mcp", SCRIPT)
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)


def rpc(method, params=None, ident=1):
    return {"jsonrpc": "2.0", "id": ident, "method": method, "params": params or {}}


def test_stdio_initializes_without_credentials_or_backend(tmp_path):
    messages = [rpc("initialize", {"protocolVersion": "2025-03-26"}),
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                rpc("tools/list", ident=2), rpc("ping", ident=3)]
    process = subprocess.run(
        [sys.executable, str(SCRIPT), str(tmp_path / "nonexistent-config.json")],
        input="\n".join(json.dumps(m) for m in messages) + "\n",
        capture_output=True, text=True, timeout=5,
    )
    assert process.returncode == 0, process.stderr
    replies = [json.loads(line) for line in process.stdout.splitlines()]
    assert [r["id"] for r in replies] == [1, 2, 3]
    assert replies[0]["result"]["protocolVersion"] == "2025-03-26"
    assert [t["name"] for t in replies[1]["result"]["tools"]] == ["search_tools", "execute_tool"]
    assert "nextCursor" not in replies[1]["result"]
    assert replies[2]["result"] == {}


@pytest.fixture
def upstream(tmp_path):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((self.path, body, self.headers.get("Authorization")))
            if self.path.startswith("/api/v1/tools/query?"):
                result = {"tools": [{"tool_id": "documents:list_documents", "name": "list_documents",
                                    "input_schema": {"type": "object"}, "backend_url": "internal"}], "total": 1}
            else:
                assert self.path == "/api/v1/tools/execute"
                result = {"success": body["tool"] != "failed", "result": {"count": 3}}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = tmp_path / "api.json"
    config.write_text(json.dumps({"api_url": f"http://127.0.0.1:{server.server_port}",
                                 "http_headers": {"Authorization": "Bearer private-test-token"}}))
    yield bridge.Server(bridge.APIClient(config)), calls
    server.shutdown()
    server.server_close()
    thread.join()


def test_discovery_and_execution_use_rest_only(upstream):
    server, calls = upstream
    result = server.call_tool("search_tools", {"query": "list documents", "backend_id": "documents"})
    assert not result["isError"]
    payload = json.loads(result["content"][0]["text"])
    assert payload["tools"][0]["input_schema"] == {"type": "object"}
    assert "backend_url" not in payload["tools"][0]
    result = server.call_tool("execute_tool", {"tool_name": "documents:list_documents", "arguments": {},
                                               "idempotency_key": "stable-id", "trace_id": "trace"})
    assert not result["isError"]
    assert calls[0][0] == "/api/v1/tools/query?search=list+documents&limit=5&offset=0&backend_id=documents"
    assert calls[1][1] == {"tool": "documents:list_documents", "arguments": {}, "timeout": 60,
                            "idempotency_key": "stable-id", "trace_id": "trace"}
    assert all(call[2] == "Bearer private-test-token" for call in calls)


def test_execution_failure_is_mcp_tool_error(upstream):
    server, _ = upstream
    result = server.handle(rpc("tools/call", {"name": "execute_tool", "arguments": {"tool_name": "failed", "arguments": {}}}))
    assert result["result"]["isError"] is True


@pytest.mark.parametrize("args", [
    {"tool_name": "x", "arguments": [],},
    {"tool_name": "x", "arguments": {}, "tenant_id": "forged"},
    {"tool_name": "x", "arguments": {}, "timeout": 0},
    {"tool_name": "x", "arguments": {}, "timeout": float("nan")},
    {"tool_name": "x", "arguments": {}, "timeout": True},
    {"tool_name": "", "arguments": {}},
])
def test_invalid_inputs_never_reach_api(upstream, args):
    server, calls = upstream
    result = server.handle(rpc("tools/call", {"name": "execute_tool", "arguments": args}))
    assert result["result"]["isError"] is True
    assert not calls


def test_missing_configuration_fails_only_the_call(tmp_path):
    server = bridge.Server(bridge.APIClient(tmp_path / "missing.json"))
    result = server.handle(rpc("tools/call", {"name": "search_tools", "arguments": {"query": "video"}}))
    assert result["result"]["isError"] is True
    assert "result" in server.handle(rpc("ping"))


def test_slow_tool_does_not_block_stdio_ping(tmp_path):
    entered = threading.Event()
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            entered.set()
            release.wait(5)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"success":true}')

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = tmp_path / "api.json"
    config.write_text(json.dumps({"api_url": f"http://127.0.0.1:{server.server_port}"}))
    process = subprocess.Popen([sys.executable, str(SCRIPT), str(config)], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        process.stdin.write(json.dumps(rpc("tools/call", {"name": "execute_tool", "arguments": {"tool_name": "slow", "arguments": {}}})) + "\n")
        process.stdin.flush()
        assert entered.wait(3)
        process.stdin.write(json.dumps(rpc("ping", ident=2)) + "\n")
        process.stdin.flush()
        # A blocked main loop would return the tool response first after the upstream timeout.
        assert json.loads(process.stdout.readline())["id"] == 2
        release.set()
        assert json.loads(process.stdout.readline())["id"] == 1
        process.stdin.close()
        assert process.wait(timeout=5) == 0
    finally:
        release.set()
        if process.poll() is None:
            process.kill()
        process.wait()
        server.shutdown()
        server.server_close()
        thread.join()
