#!/usr/bin/env python3
"""
Codex Apps Bridge — lightweight MCP server for a single catalog connector.

Run via:
    CONNECTOR_ID=connector_github PORT=9001 python -m tool_management.integrations.codex_apps_bridge

This bridge turns a Codex Apps catalog entry into an addressable MCP backend.
It exposes synthetic tools (describe, authorize, install, proxy_call) and, when
possible, forwards proxy_call to the connector's install_url or stdio command.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# Add project paths when run as module
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from tool_management.integrations.codex_apps_adapter import (  # noqa: E402
    CodexAppsCatalogAdapter,
    CodexAppConnector,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("codex_apps_bridge")


@dataclass
class BridgeConfig:
    connector_id: str
    port: int
    host: str = "127.0.0.1"


class CodexAppsBridge:
    """MCP bridge for one Codex Apps connector."""

    def __init__(self, config: BridgeConfig):
        self.config = config
        self.adapter = CodexAppsCatalogAdapter()
        self.connector: Optional[CodexAppConnector] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._load_connector()

    def _load_connector(self) -> None:
        conn = self.adapter.get(self.config.connector_id)
        if conn is None:
            # Try by raw id
            for c in self.adapter.connectors():
                if c.id == self.config.connector_id:
                    conn = c
                    break
        if conn is None:
            raise ValueError(f"Connector not found: {self.config.connector_id}")
        self.connector = conn
        logger.info(f"Loaded connector: {conn.name} ({conn.id})")

    async def start(self):
        self._client = httpx.AsyncClient(timeout=60.0)

    async def stop(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    # -------------------------------------------------------------------------
    # Synthetic tools
    # -------------------------------------------------------------------------
    def tools(self) -> List[Dict[str, Any]]:
        if not self.connector:
            return []
        base = {
            "type": "object",
            "properties": {},
            "required": [],
        }
        return [
            {
                "name": "describe",
                "description": f"Return metadata for the {self.connector.name} connector.",
                "inputSchema": base,
            },
            {
                "name": "authorize",
                "description": f"Record or refresh authorization credentials for {self.connector.name}.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "token": {"type": "string", "description": "API token or OAuth token"},
                        "refresh_token": {"type": "string", "description": "Optional refresh token"},
                    },
                    "required": ["token"],
                },
            },
            {
                "name": "install",
                "description": f"Return installation instructions and install_url for {self.connector.name}.",
                "inputSchema": base,
            },
            {
                "name": "proxy_call",
                "description": (
                    f"Proxy a call to the {self.connector.name} connector. "
                    "Requires the connector to be installed/authorized first."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "method": {"type": "string", "description": "HTTP method or operation name"},
                        "path": {"type": "string", "description": "API path or resource"},
                        "body": {"type": "object", "description": "Request body/arguments"},
                        "headers": {"type": "object", "description": "Additional headers"},
                    },
                    "required": ["method", "path"],
                },
            },
        ]

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        if not self.connector:
            raise ValueError("Connector not loaded")

        if name == "describe":
            return {"connector": self.connector.to_dict()}

        if name == "install":
            return {
                "name": self.connector.name,
                "install_url": self.connector.install_url,
                "distribution_channel": self.connector.distribution_channel,
                "instructions": f"Open {self.connector.install_url} to install/authorize {self.connector.name}.",
            }

        if name == "authorize":
            token = arguments.get("token")
            # Persist token to a bridge-local file for now; runtime can read it.
            if token:
                _save_bridge_secret(self.connector.id, "token", token)
                return {"success": True, "message": f"Authorization recorded for {self.connector.name}"}
            return {"success": False, "error": "token is required"}

        if name == "proxy_call":
            return await self._proxy_call(arguments)

        raise ValueError(f"Unknown tool: {name}")

    async def _proxy_call(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        if not self.connector or not self.connector.install_url:
            return {
                "success": False,
                "error": "This connector has no install_url; proxy_call is not available.",
                "catalog_status": self.connector.catalog_status if self.connector else "unknown",
            }

        if not self.connector.is_accessible:
            # Catalog-only connector: return metadata so the caller can authorize first.
            return {
                "success": False,
                "error": (
                    f"{self.connector.name} is catalog-only. "
                    "Call the 'install' and 'authorize' tools first."
                ),
                "install_url": self.connector.install_url,
                "not_executable_reason": self.connector.to_dict().get("not_executable_reason"),
            }

        method = arguments.get("method", "GET").upper()
        path = arguments.get("path", "")
        body = arguments.get("body")
        headers = arguments.get("headers", {})

        # Load stored token if available
        token = _load_bridge_secret(self.connector.id, "token")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        url = f"{self.connector.install_url.rstrip('/')}/{path.lstrip('/')}"
        try:
            response = await self._client.request(
                method=method,
                url=url,
                json=body,
                headers=headers,
                timeout=60.0,
            )
            return {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "body": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # -------------------------------------------------------------------------
    # MCP protocol handlers
    # -------------------------------------------------------------------------
    async def handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {
                "name": f"codex-app-bridge-{self.connector.slug if self.connector else 'unknown'}",
                "version": "1.0.0",
            },
        }

    async def handle_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {"tools": self.tools()}

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        result = await self.call_tool(tool_name, arguments)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result, default=str),
                }
            ]
        }

    async def handle_mcp_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        method = payload.get("method")
        req_id = payload.get("id")
        params = payload.get("params", {})
        try:
            if method == "initialize":
                result = await self.handle_initialize(params)
            elif method == "tools/list":
                result = await self.handle_tools_list(params)
            elif method == "tools/call":
                result = await self.handle_tools_call(params)
            else:
                return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except Exception as e:
            logger.error(f"MCP request error: {e}")
            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(e)}}


# -------------------------------------------------------------------------
# Secret helpers (bridge-local only; replace with vault integration)
# -------------------------------------------------------------------------
def _bridge_secret_path(connector_id: str) -> Path:
    base = Path(os.getenv("DATA_DIR", "data")) / "codex_bridge_secrets"
    base.mkdir(parents=True, exist_ok=True)
    safe_id = connector_id.replace("/", "_").replace("\\", "_")
    return base / f"{safe_id}.json"


def _save_bridge_secret(connector_id: str, key: str, value: str) -> None:
    path = _bridge_secret_path(connector_id)
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    data[key] = value
    path.write_text(json.dumps(data), encoding="utf-8")


def _load_bridge_secret(connector_id: str, key: str) -> Optional[str]:
    path = _bridge_secret_path(connector_id)
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get(key)


# -------------------------------------------------------------------------
# FastAPI app factory
# -------------------------------------------------------------------------
def create_app(connector_id: Optional[str] = None, port: Optional[int] = None) -> FastAPI:
    config = BridgeConfig(
        connector_id=connector_id or os.getenv("CONNECTOR_ID", ""),
        port=port or int(os.getenv("PORT", "9001")),
        host=os.getenv("HOST", "127.0.0.1"),
    )
    bridge = CodexAppsBridge(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await bridge.start()
        yield
        await bridge.stop()

    app = FastAPI(
        title=f"Codex App Bridge — {bridge.connector.name if bridge.connector else config.connector_id}",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health():
        return {
            "status": "healthy",
            "connector_id": bridge.connector.id if bridge.connector else None,
            "connector_name": bridge.connector.name if bridge.connector else None,
            "catalog_status": bridge.connector.catalog_status if bridge.connector else "unknown",
        }

    @app.get("/tools")
    async def list_tools():
        return {"tools": bridge.tools()}

    @app.post("/mcp")
    async def mcp_endpoint(request: Request):
        payload = await request.json()
        result = await bridge.handle_mcp_request(payload)
        return JSONResponse(result)

    return app


def main():
    connector_id = os.getenv("CONNECTOR_ID")
    if not connector_id:
        print("CONNECTOR_ID environment variable is required", file=sys.stderr)
        sys.exit(1)
    port = int(os.getenv("PORT", "9001"))
    host = os.getenv("HOST", "127.0.0.1")
    uvicorn.run(
        "tool_management.integrations.codex_apps_bridge:create_app",
        factory=True,
        host=host,
        port=port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
