#!/usr/bin/env python3
"""
Codex Apps MCP Server — exposes the ToolManagement Codex Apps catalog to Claude.

This is a standalone MCP server that talks to the ToolManagement API
(default: https://tools.distributed.systems). It lets Claude:
- Browse the 1,887-connector Codex Apps catalog
- Activate/deactivate connectors
- Authorize connectors with tokens
- Call connector tools via the virtual bridge

Configure Claude Desktop to run:
    {
      "mcpServers": {
        "codex-apps": {
          "command": "python",
          "args": ["-m", "native.codex_apps_mcp.server"],
          "env": {
        "TOOLMANAGEMENT_API_URL": "https://tools.distributed.systems",
        "TOOLMANAGEMENT_BEARER_TOKEN": "<from secret manager>"
          }
        }
      }
    }
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

API_URL = os.getenv("TOOLMANAGEMENT_API_URL", "https://tools.distributed.systems").rstrip("/")
BEARER_TOKEN = os.getenv("TOOLMANAGEMENT_BEARER_TOKEN", "")


def api_call(method: str, path: str, json_data: Optional[Dict[str, Any]] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {}
    if BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    url = f"{API_URL}{path}"
    try:
        with httpx.Client(timeout=60.0) as client:
            r = client.request(method, url, headers=headers, json=json_data, params=params)
            return r.json() if r.text else {}
    except Exception as e:
        return {"error": str(e)}


def list_connectors(category: Optional[str] = None, limit: int = 20) -> str:
    params = {"limit": limit}
    if category:
        params["category"] = category
    data = api_call("GET", "/api/v1/codex-apps/connectors", params=params)
    connectors = data.get("connectors", [])
    lines = [f"Found {len(connectors)} connectors:"]
    for c in connectors[:limit]:
        lines.append(f"- {c.get('name')} ({c.get('id')}): {c.get('description', '')[:80]}")
    return "\n".join(lines)


def search_connectors(query: str, limit: int = 10) -> str:
    data = api_call("GET", "/api/v1/codex-apps/connectors", params={"q": query, "limit": limit})
    connectors = data.get("connectors", [])
    lines = [f"Search '{query}' returned {len(connectors)} connectors:"]
    for c in connectors[:limit]:
        lines.append(f"- {c.get('name')} ({c.get('id')}): {c.get('description', '')[:80]}")
    return "\n".join(lines)


def get_connector(key: str) -> str:
    data = api_call("GET", f"/api/v1/codex-apps/connectors/{key}")
    return json.dumps(data, indent=2)


def activate_connector(key: str) -> str:
    data = api_call("POST", f"/api/v1/codex-apps/connectors/{key}/activate")
    if data.get("activated"):
        return f"Activated {data.get('name')} as {data.get('server_name')}. Bridge URL: {data.get('url')}"
    return f"Activation failed: {data.get('error', 'unknown error')}"


def deactivate_connector(key: str) -> str:
    data = api_call("POST", f"/api/v1/codex-apps/connectors/{key}/deactivate")
    if data.get("deactivated"):
        return f"Deactivated {data.get('name', key)}"
    return f"Deactivation failed: {data.get('error', 'unknown error')}"


def list_activated() -> str:
    data = api_call("GET", "/api/v1/codex-apps/activated")
    activated = data.get("activated", [])
    lines = [f"{len(activated)} activated connectors:"]
    for a in activated:
        lines.append(f"- {a.get('server_name')}: {a.get('url')} (authorized={a.get('authorized')})")
    return "\n".join(lines)


def authorize_connector(key: str, token: str) -> str:
    # Resolve connector id first
    conn = api_call("GET", f"/api/v1/codex-apps/connectors/{key}")
    connector_id = conn.get("connector", {}).get("id") if "connector" in conn else key
    data = api_call("POST", "/api/v1/admin/codex-secrets", json={
        "connector_id": connector_id,
        "key": "token",
        "value": token,
    })
    if data.get("stored"):
        return f"Stored token for {data.get('connector_id')}"
    return f"Failed to store token: {data}"


def connector_status(key: str) -> str:
    data = api_call("GET", f"/api/v1/codex-apps/connectors/{key}/status")
    return json.dumps(data, indent=2)


def proxy_call(key: str, method: str, path: str, body: Optional[str] = None) -> str:
    # Activate if not already active
    status = api_call("GET", f"/api/v1/codex-apps/connectors/{key}/status")
    if not status.get("running"):
        activate_connector(key)

    # Get bridge URL from activated list
    activated = api_call("GET", "/api/v1/codex-apps/activated").get("activated", [])
    bridge_url = None
    for a in activated:
        if a.get("server_name") == f"codex_app_{key}" or key in a.get("url", ""):
            bridge_url = a.get("url")
            break
    if not bridge_url:
        return f"Connector {key} is not activated or has no bridge URL"

    body_obj = json.loads(body) if body else None
    r = api_call("POST", f"{bridge_url}/mcp".replace(API_URL, ""), json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "proxy_call",
            "arguments": {"method": method, "path": path, "body": body_obj},
        },
    })
    try:
        content = r["result"]["content"][0]["text"]
        return content
    except Exception:
        return json.dumps(r, indent=2)


TOOLS = {
    "codex_list_connectors": {
        "description": "List connectors from the Codex Apps catalog. Optionally filter by category.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {"type": "string", "description": "Optional category filter"},
                "limit": {"type": "integer", "description": "Max results", "default": 20},
            },
        },
        "handler": list_connectors,
    },
    "codex_search_connectors": {
        "description": "Search the Codex Apps catalog by keyword.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
            },
            "required": ["query"],
        },
        "handler": search_connectors,
    },
    "codex_get_connector": {
        "description": "Get full metadata for one connector by id, name, or slug.",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
        "handler": get_connector,
    },
    "codex_activate_connector": {
        "description": "Activate a connector so it becomes available as an MCP backend.",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
        "handler": activate_connector,
    },
    "codex_deactivate_connector": {
        "description": "Deactivate a connector and free its bridge.",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
        "handler": deactivate_connector,
    },
    "codex_list_activated": {
        "description": "List currently activated connectors and their bridge URLs.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": list_activated,
    },
    "codex_authorize_connector": {
        "description": "Store a token/API key for a connector.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Connector id/name/slug"},
                "token": {"type": "string"},
            },
            "required": ["key", "token"],
        },
        "handler": authorize_connector,
    },
    "codex_proxy_call": {
        "description": "Make a proxied API call through an activated connector.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "method": {"type": "string", "default": "GET"},
                "path": {"type": "string"},
                "body": {"type": "string", "description": "JSON-encoded request body"},
            },
            "required": ["key", "path"],
        },
        "handler": proxy_call,
    },
}


def send_message(obj: Dict[str, Any]) -> None:
    payload = json.dumps(obj)
    sys.stdout.write(f"Content-Length: {len(payload)}\r\n\r\n{payload}")
    sys.stdout.flush()


def read_message() -> Optional[Dict[str, Any]]:
    line = sys.stdin.readline()
    if not line:
        return None
    if not line.startswith("Content-Length:"):
        return None
    length = int(line.split(":")[1].strip())
    sys.stdin.read(2)  # \r\n
    payload = sys.stdin.read(length)
    return json.loads(payload)


def main():
    while True:
        msg = read_message()
        if msg is None:
            break
        req_id = msg.get("id")
        method = msg.get("method")
        params = msg.get("params", {})

        if method == "initialize":
            send_message({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "codex-apps-mcp", "version": "1.0.0"},
                },
            })
        elif method == "tools/list":
            send_message({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        {"name": k, "description": v["description"], "inputSchema": v["inputSchema"]}
                        for k, v in TOOLS.items()
                    ]
                },
            })
        elif method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments", {})
            tool = TOOLS.get(name)
            if not tool:
                send_message({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {name}"},
                })
                continue
            try:
                result = tool["handler"](**arguments)
            except Exception as e:
                result = f"Error: {e}"
            send_message({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": result}]
                },
            })
        else:
            send_message({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            })


def create_server(port: Optional[int] = None):
    """
    HTTP mode — same /health, /tools, /mcp surface as the other native MCPs
    so the launcher, probe, and registry treat this server uniformly.
    Stdio mode (main) remains for Claude Desktop.
    """
    import asyncio

    from native.base import NativeMCPBase

    class CodexAppsMCP(NativeMCPBase):
        SERVICE_ID = "codex-apps-mcp"
        SERVICE_NAME = "Codex Apps MCP"
        VERSION = "1.0.0"
        CAPABILITIES = ["codex_apps", "connector_catalog", "connector_activation"]
        TOOLS = [
            {"name": name, "description": spec["description"], "inputSchema": spec["inputSchema"]}
            for name, spec in TOOLS.items()
        ]

    server = CodexAppsMCP(port=port or int(os.getenv("PORT", "4011")))

    def _make_handler(fn):
        async def _handler(args: Dict[str, Any]) -> Dict[str, Any]:
            try:
                # Catalog handlers are sync httpx calls — keep the loop free
                text = await asyncio.to_thread(fn, **(args or {}))
                return {"content": [{"type": "text", "text": text}]}
            except Exception as e:
                return {"content": [{"type": "text", "text": f"Error: {e}"}], "isError": True}
        return _handler

    for name, spec in TOOLS.items():
        setattr(server, f"_tool_{name}", _make_handler(spec["handler"]))

    return server


if __name__ == "__main__":
    # HTTP mode when the launcher (or operator) provides a port; stdio otherwise
    if os.getenv("PORT") or "--port" in sys.argv:
        port = None
        if "--port" in sys.argv:
            port = int(sys.argv[sys.argv.index("--port") + 1])
        create_server(port).run()
    else:
        main()
