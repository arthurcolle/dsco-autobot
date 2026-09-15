#!/usr/bin/env python3
"""Lazy stdio MCP front door for the hosted Tool Management REST API.

Run this file directly: importing tool_management would load the server runtime.
Initialize/list/ping are local; credentials and HTTP are used only on tools/call.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


TOOLS = [
    {
        "name": "search_tools",
        "description": (
            "Find tools and their full input schemas in the hosted Tool Management "
            "registry, including document Q&A, video Q&A and OpenRouter integrations. "
            "Search by a short keyword or exact tool name; optionally filter by backend. "
            "Use the returned backend-qualified tool_id with execute_tool."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "backend_id": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 25, "default": 5},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "execute_tool",
        "description": (
            "Execute a discovered tool through the hosted Tool Management API, "
            "which handles integration routing, authentication and execution policy. "
            "Pass its backend-qualified tool_id as tool_name and its schema's inputs "
            "as arguments. Supply a stable idempotency_key for writes."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "tool_name": {"type": "string", "minLength": 1},
                "arguments": {"type": "object"},
                "timeout": {"type": "number", "minimum": 1, "maximum": 600, "default": 60},
                "idempotency_key": {"type": "string"},
                "trace_id": {"type": "string"},
                "session_id": {"type": "string"},
            },
            "required": ["tool_name", "arguments"],
            "additionalProperties": False,
        },
    },
]


def tool_result(payload, *, error=False):
    return {"content": [{"type": "text", "text": json.dumps(payload)}], "isError": error}


class APIClient:
    def __init__(self, config_path):
        self.config_path = config_path

    def request(self, path, payload, timeout):
        # Deliberately lazy: no HTTP imports, config reads or DNS at startup.
        import urllib.error
        import urllib.parse
        import urllib.request

        config = json.loads(Path(self.config_path).expanduser().read_text())
        base = config["api_url"].rstrip("/")
        parsed = urllib.parse.urlsplit(base)
        if parsed.scheme != "https" and not (
            parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        ):
            raise ValueError("Tool Management API requires HTTPS (or local loopback HTTP)")
        headers = dict(config.get("http_headers", {}))
        for name, env_name in config.get("env_http_headers", {}).items():
            headers[name] = os.environ[env_name]
        if config.get("bearer_token_env_var"):
            headers["Authorization"] = "Bearer " + os.environ[config["bearer_token_env_var"]]
        headers.update({"Accept": "application/json", "Content-Type": "application/json"})

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, hdrs, newurl):
                return None  # Never forward credentials or replay a write on redirect.

        request = urllib.request.Request(
            base + path, data=json.dumps(payload).encode(), headers=headers, method="POST"
        )
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            # Do not include the request, headers, or provider error bodies in diagnostics.
            raise RuntimeError(f"Tool Management API returned HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError):
            raise RuntimeError(
                "Tool Management API connection failed or timed out. Execution outcome may "
                "be unknown; check before repeating a write and reuse its idempotency_key."
            ) from None


class Server:
    def __init__(self, client):
        self.client = client

    def call_tool(self, name, args):
        from urllib.parse import urlencode

        schema = next((tool["inputSchema"] for tool in TOOLS if tool["name"] == name), None)
        if schema is None:
            raise ValueError("Unknown tool; use search_tools or execute_tool")
        if not isinstance(args, dict) or set(args) - set(schema["properties"]):
            raise ValueError("Invalid tool arguments")
        for key in schema.get("required", []):
            if key not in args:
                raise ValueError(f"Missing required argument: {key}")
        for key, value in args.items():
            spec = schema["properties"][key]
            kind = spec["type"]
            valid = {"string": isinstance(value, str), "object": isinstance(value, dict),
                     "integer": type(value) is int, "number": type(value) in (int, float)}[kind]
            if not valid or ("minimum" in spec and not spec["minimum"] <= value <= spec.get("maximum", float("inf"))):
                raise ValueError(f"Invalid argument: {key}")
        if name == "search_tools":
            query = {"search": args["query"], "limit": args.get("limit", 5),
                     "offset": args.get("offset", 0)}
            if args.get("backend_id"):
                query["backend_id"] = args["backend_id"]
            result = self.client.request("/api/v1/tools/query?" + urlencode(query), {}, 30)
            fields = {"tool_id", "name", "description", "backend_id", "input_schema"}
            result["tools"] = [{k: v for k, v in row.items() if k in fields}
                               for row in result.get("tools", [])]
            return tool_result(result)
        if not args["tool_name"].strip():
            raise ValueError("tool_name must not be empty")
        body = {"tool": args["tool_name"], "arguments": args["arguments"],
                "timeout": args.get("timeout", 60)}
        for key in ("idempotency_key", "trace_id", "session_id"):
            if key in args:
                body[key] = args[key]
        result = self.client.request("/api/v1/tools/execute", body, body["timeout"] + 10)
        return tool_result(result, error=result.get("success") is not True)

    def handle(self, message):
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
            return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid request"}}
        if "id" not in message:
            return None  # Notifications never produce a JSON-RPC response.
        response = {"jsonrpc": "2.0", "id": message["id"]}
        method = message["method"]
        params = message.get("params") or {}
        if not isinstance(params, dict):
            response["error"] = {"code": -32602, "message": "params must be an object"}
            return response
        if method == "initialize":
            version = params.get("protocolVersion")
            response["result"] = {
                "protocolVersion": version if version in {"2024-11-05", "2025-03-26", "2025-06-18"} else "2025-06-18",
                "serverInfo": {"name": "Tool Management API", "version": "1.0.0"},
                "capabilities": {"tools": {"listChanged": False}},
                "instructions": "Discover and execute integrations through search_tools and execute_tool. Backend connections happen only on demand.",
            }
        elif method == "tools/list":
            response["result"] = {"tools": TOOLS}
        elif method == "ping":
            response["result"] = {}
        elif method in {"resources/list", "resources/templates/list", "prompts/list"}:
            response["result"] = {{"resources/list": "resources", "resources/templates/list": "resourceTemplates", "prompts/list": "prompts"}[method]: []}
        elif method == "tools/call":
            try:
                response["result"] = self.call_tool(params.get("name"), params.get("arguments", {}))
            except (ValueError, RuntimeError) as exc:
                response["result"] = tool_result({"error": str(exc)}, error=True)
            except Exception:
                response["result"] = tool_result({"error": "Tool Management client failed; check its private API configuration and service availability."}, error=True)
        else:
            response["error"] = {"code": -32601, "message": "Method not found"}
        return response


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "~/.config/dsco/tool-management.json"
    server = Server(APIClient(config_path))
    output_lock = threading.Lock()

    def respond(message):
        result = server.handle(message)
        if result is not None:
            with output_lock:
                print(json.dumps(result), flush=True)

    # Slow executions do not block local ping/list requests or independent calls.
    with ThreadPoolExecutor(max_workers=8) as workers:
        for line in sys.stdin:
            try:
                message = json.loads(line)
            except ValueError:
                with output_lock:
                    print(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}), flush=True)
                continue
            if isinstance(message, dict) and message.get("method") == "tools/call":
                workers.submit(respond, message)
            else:
                respond(message)


if __name__ == "__main__":
    main()
