#!/usr/bin/env python3
"""
Codex Apps Virtual Bridge — one process serves all activated connectors.

Routes are parameterized by connector_id:
    GET  /{connector_id}/health
    GET  /{connector_id}/tools
    POST /{connector_id}/mcp

The bridge loads connector metadata from the catalog, reads stored credentials
from CodexAppsVault, and proxies proxy_call requests to the connector's
install_url when a token is available.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from tool_management.integrations.codex_apps_adapter import (  # noqa: E402
    CodexAppsCatalogAdapter,
    CodexAppConnector,
)
from tool_management.integrations.codex_apps_vault import (  # noqa: E402
    get_codex_apps_vault,
    CodexAppsVault,
)
from tool_management.integrations.codex_apps_oauth import (  # noqa: E402
    get_codex_apps_oauth,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("codex_apps_virtual_bridge")


class VirtualConnectorBridge:
    """Stateless handler for one connector within the virtual bridge."""

    def __init__(self, connector_id: str, adapter: CodexAppsCatalogAdapter, vault: CodexAppsVault):
        self.connector_id = connector_id
        self.adapter = adapter
        self.vault = vault
        self.connector: Optional[CodexAppConnector] = None
        self._load_connector()

    def _load_connector(self) -> None:
        conn = self.adapter.get(self.connector_id)
        if conn is None:
            for c in self.adapter.connectors():
                if c.id == self.connector_id:
                    conn = c
                    break
        self.connector = conn

    def health(self) -> Dict[str, Any]:
        if not self.connector:
            return {"status": "unknown_connector", "connector_id": self.connector_id}
        return {
            "status": "healthy",
            "connector_id": self.connector.id,
            "connector_name": self.connector.name,
            "catalog_status": self.connector.catalog_status,
            "authorized": self.vault.has_secret(self.connector.id, "token"),
        }

    def tools(self) -> List[Dict[str, Any]]:
        if not self.connector:
            return []
        base = {"type": "object", "properties": {}, "required": []}
        return [
            {
                "name": "describe",
                "description": f"Return metadata for the {self.connector.name} connector.",
                "inputSchema": base,
            },
            {
                "name": "authorize",
                "description": f"Store or update authorization credentials for {self.connector.name}. For OAuth connectors, start the flow via POST /api/v1/admin/codex-oauth/{self.connector.id}/authorize and complete the browser redirect.",
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
                "description": f"Proxy a call to the {self.connector.name} connector.",
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

    async def call_tool(self, name: str, arguments: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
        if not self.connector:
            raise ValueError(f"Connector not found: {self.connector_id}")

        if name == "describe":
            return {"connector": self.connector.to_dict()}

        if name == "install":
            oauth = get_codex_apps_oauth()
            provider = oauth._detect_provider(self.connector)
            return {
                "name": self.connector.name,
                "install_url": self.connector.install_url,
                "distribution_channel": self.connector.distribution_channel,
                "oauth_supported": provider is not None,
                "oauth_provider": provider,
                "instructions": (
                    f"Open {self.connector.install_url} to install/authorize {self.connector.name}. "
                    f"If OAuth is available, start the flow via POST /api/v1/admin/codex-oauth/{self.connector.id}/authorize."
                ),
            }

        if name == "authorize":
            token = arguments.get("token")
            if token:
                self.vault.set_secret(self.connector.id, "token", token)
                return {"success": True, "message": f"Authorization recorded for {self.connector.name}"}
            return {"success": False, "error": "token is required"}

        if name == "proxy_call":
            return await self._proxy_call(arguments, client)

        raise ValueError(f"Unknown tool: {name}")

    async def _proxy_call(self, arguments: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
        if not self.connector or not self.connector.install_url:
            return {
                "success": False,
                "error": "This connector has no install_url; proxy_call is not available.",
            }

        token = self.vault.get_secret(self.connector.id, "token")
        if not token:
            return {
                "success": False,
                "error": f"{self.connector.name} is not authorized. Call the 'authorize' tool first.",
                "install_url": self.connector.install_url,
            }

        method = arguments.get("method", "GET").upper()
        path = arguments.get("path", "")
        body = arguments.get("body")
        headers = dict(arguments.get("headers", {}))
        headers["Authorization"] = f"Bearer {token}"

        url = f"{self.connector.install_url.rstrip('/')}/{path.lstrip('/')}"
        try:
            response = await client.request(
                method=method,
                url=url,
                json=body,
                headers=headers,
                timeout=60.0,
            )
            content_type = response.headers.get("content-type", "")
            return {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "body": response.json() if content_type.startswith("application/json") else response.text,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def handle_mcp(self, payload: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
        method = payload.get("method")
        req_id = payload.get("id")
        params = payload.get("params", {})
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": f"codex-app-virtual-bridge-{self.connector_id}",
                        "version": "1.0.0",
                    },
                }
            elif method == "tools/list":
                result = {"tools": self.tools()}
            elif method == "tools/call":
                tool_result = await self.call_tool(params.get("name"), params.get("arguments", {}), client)
                result = {
                    "content": [
                        {"type": "text", "text": json.dumps(tool_result, default=str)}
                    ]
                }
            else:
                return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Method not found: {method}"}}
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except Exception as e:
            logger.error(f"MCP request error for {self.connector_id}: {e}")
            return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(e)}}


def create_app() -> FastAPI:
    return create_subapp(prefix="")


def create_subapp(prefix: str = "/codex-bridge") -> FastAPI:
    """
    Create the virtual bridge as a mountable sub-application.

    When prefix is empty, routes are `/{connector_id}/health` etc.
    When prefix is non-empty, the FastAPI app is mounted at that prefix and
    routes remain `/{connector_id}/health`, so the full path is
    `{prefix}/{connector_id}/health`.
    """
    adapter = CodexAppsCatalogAdapter()
    vault = get_codex_apps_vault()
    shared_client: Optional[httpx.AsyncClient] = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal shared_client
        shared_client = httpx.AsyncClient(timeout=60.0)
        yield
        await shared_client.aclose()

    app = FastAPI(
        title="Codex Apps Virtual Bridge",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/{connector_id}/health")
    async def health(connector_id: str):
        return VirtualConnectorBridge(connector_id, adapter, vault).health()

    @app.get("/{connector_id}/tools")
    async def list_tools(connector_id: str):
        return {"tools": VirtualConnectorBridge(connector_id, adapter, vault).tools()}

    @app.post("/{connector_id}/mcp")
    async def mcp_endpoint(connector_id: str, request: Request):
        payload = await request.json()
        result = await VirtualConnectorBridge(connector_id, adapter, vault).handle_mcp(payload, shared_client)
        return JSONResponse(result)

    return app


def main():
    port = int(os.getenv("PORT", "9020"))
    host = os.getenv("HOST", "127.0.0.1")
    uvicorn.run(create_app(), host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
