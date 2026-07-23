"""
Codex Apps Runtime — turn catalog connectors into registered MCP backends.

This runtime uses a single virtual bridge process that serves all activated
connectors at path-prefixed endpoints. Each connector is registered in the
existing MCPRegistry as a separate server pointing to its path on the bridge.

Credentials are stored securely via CodexAppsVault (macOS Keychain primary,
encrypted file fallback) and are read by the bridge at call time.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx

from .codex_apps_adapter import CodexAppsCatalogAdapter, CodexAppConnector, get_codex_apps_adapter
from .codex_apps_vault import get_codex_apps_vault
from .mcp_registry import MCPServerConfig, get_registry as get_mcp_registry

logger = logging.getLogger(__name__)

CODEX_REGISTRY_STATE_FILE = Path(os.getenv("DATA_DIR", "data")) / "codex_apps_runtime_state.json"
DEFAULT_VIRTUAL_BRIDGE_PORT = int(os.getenv("CODEX_VIRTUAL_BRIDGE_PORT", "0"))

# Rough mapping from connector category/name patterns to likely transport.
TRANSPORT_INFERENCE: List[tuple[str, str]] = [
    (r"\bmcp\b", "mcp"),
    (r"\bsse\b", "sse"),
    (r"\bstdio\b", "stdio"),
    (r"\bhttp\b", "http"),
    (r"\bopenapi\b", "openapi"),
]

# Secrets commonly required by SaaS connectors. These are hints, not exhaustive.
SECRET_HINTS: Dict[str, List[str]] = {
    "jina": ["JINA_API_KEY"],
    "parallel": ["PARALLEL_API_KEY"],
    "github": ["GITHUB_TOKEN"],
    "slack": ["SLACK_BOT_TOKEN", "SLACK_SIGNING_SECRET"],
    "notion": ["NOTION_TOKEN"],
    "hubspot": ["HUBSPOT_ACCESS_TOKEN"],
    "stripe": ["STRIPE_API_KEY"],
    "openai": ["OPENAI_API_KEY"],
    "alpaca": ["ALPACA_API_KEY", "ALPACA_API_SECRET"],
    "coinbase": ["COINBASE_API_KEY", "COINBASE_API_SECRET"],
    "plaid": ["PLAID_CLIENT_ID", "PLAID_SECRET"],
    "quickbooks": ["QUICKBOOKS_ACCESS_TOKEN", "QUICKBOOKS_REALM_ID"],
    "bigquery": ["GOOGLE_APPLICATION_CREDENTIALS"],
}


@dataclass
class ActivationResult:
    connector_id: str
    name: str
    activated: bool
    server_name: str
    transport: str
    url: str
    bridge_url: str
    error: Optional[str] = None
    secrets_hint: List[str] = field(default_factory=list)


class CodexAppsRuntime:
    """
    Manages activation of Codex Apps catalog connectors as MCP registry servers.

    Uses a single virtual bridge process for all connectors to avoid process
    sprawl. Connector credentials are stored separately via CodexAppsVault and
    are never held in this runtime.
    """

    def __init__(
        self,
        adapter: Optional[CodexAppsCatalogAdapter] = None,
        state_file: Optional[Path] = None,
        project_root: Optional[Path] = None,
        bridge_port: int = DEFAULT_VIRTUAL_BRIDGE_PORT,
        bridge_enabled: bool = True,
        bridge_mode: Optional[str] = None,
        inprocess_prefix: str = "/codex-bridge",
    ):
        self.adapter = adapter or get_codex_apps_adapter()
        self.state_file = state_file or CODEX_REGISTRY_STATE_FILE
        self.project_root = project_root or Path(__file__).resolve().parent.parent.parent.parent
        self.bridge_port = bridge_port
        self.bridge_enabled = bridge_enabled
        self.bridge_mode = bridge_mode or os.getenv("CODEX_BRIDGE_MODE", "subprocess")
        self.inprocess_prefix = inprocess_prefix
        self._state: Dict[str, Any] = {"activated": [], "activated_at": {}, "virtual_bridge": None}
        self._process: Optional[subprocess.Popen] = None
        self._load_state()

    # -------------------------------------------------------------------------
    # State persistence
    # -------------------------------------------------------------------------
    def _load_state(self) -> None:
        if self.state_file.exists():
            try:
                self._state = json.loads(self.state_file.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"Could not load Codex runtime state: {e}")
                self._state = {"activated": [], "activated_at": {}, "virtual_bridge": None}

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        # Don't persist process handles
        state = {k: v for k, v in self._state.items() if k != "virtual_bridge"}
        if self._state.get("virtual_bridge"):
            state["virtual_bridge"] = {
                k: v for k, v in self._state["virtual_bridge"].items() if k != "process"
            }
        self.state_file.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")

    # -------------------------------------------------------------------------
    # Virtual bridge management
    # -------------------------------------------------------------------------
    @staticmethod
    def _find_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @property
    def virtual_bridge_url(self) -> Optional[str]:
        vb = self._state.get("virtual_bridge")
        return vb.get("url") if vb else None

    async def _ensure_virtual_bridge(self) -> Optional[str]:
        """Return the base URL of the shared virtual bridge, starting it if needed."""
        if not self.bridge_enabled:
            return "http://disabled.test"
        if self.bridge_mode == "inprocess":
            # Bridge is mounted in-process on the main app; no startup needed.
            base = os.getenv("API_BASE_URL", "").rstrip("/")
            if base:
                return f"{base}{self.inprocess_prefix}"
            return self.inprocess_prefix
        if self._process and self._process.poll() is None:
            return self.virtual_bridge_url

        port = self.bridge_port or self._find_free_port()
        env = os.environ.copy()
        env["PORT"] = str(port)
        env["HOST"] = "127.0.0.1"
        env["PYTHONPATH"] = f"{self.project_root}/src:{env.get('PYTHONPATH', '')}"
        env["DATA_DIR"] = os.getenv("DATA_DIR", str(self.project_root / "data"))

        logger.info(f"Starting Codex Apps virtual bridge on port {port}...")
        proc = subprocess.Popen(
            [sys.executable, "-m", "tool_management.integrations.codex_apps_virtual_bridge"],
            cwd=str(self.project_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self._process = proc
        self._state["virtual_bridge"] = {"port": port, "url": f"http://127.0.0.1:{port}", "process": proc}

        healthy = await self._wait_for_bridge(port)
        if not healthy:
            self._stop_virtual_bridge()
            return None
        self._save_state()
        return self.virtual_bridge_url

    async def _wait_for_bridge(self, port: int, timeout: float = 30.0) -> bool:
        deadline = time.time() + timeout
        async with httpx.AsyncClient() as client:
            while time.time() < deadline:
                try:
                    r = await client.get(f"http://127.0.0.1:{port}/github/health", timeout=2.0)
                    if r.status_code == 200:
                        return True
                except (httpx.ConnectError, httpx.TimeoutException):
                    pass
                await asyncio.sleep(0.5)
        return False

    def _stop_virtual_bridge(self) -> None:
        if self._process and self._process.poll() is None:
            try:
                self._process.terminate()
                self._process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
            logger.info("Stopped Codex Apps virtual bridge")
        self._process = None
        self._state["virtual_bridge"] = None
        self._save_state()

    def bridge_status(self) -> Dict[str, Any]:
        if self.bridge_mode == "inprocess":
            return {
                "mode": "inprocess",
                "running": True,
                "prefix": self.inprocess_prefix,
                "activated_connectors": list(self._state.get("activated", [])),
            }
        running = self._process is not None and self._process.poll() is None
        return {
            "mode": "subprocess",
            "running": running,
            "port": self._state.get("virtual_bridge", {}).get("port"),
            "url": self.virtual_bridge_url,
            "pid": self._process.pid if running else None,
            "activated_connectors": list(self._state.get("activated", [])),
        }

    # -------------------------------------------------------------------------
    # Catalog helpers
    # -------------------------------------------------------------------------
    def _infer_transport(self, connector: CodexAppConnector) -> str:
        text = f"{connector.name} {connector.description} {connector.distribution_channel} {connector.categories}".lower()
        for pattern, transport in TRANSPORT_INFERENCE:
            if re.search(pattern, text):
                return transport
        if connector.install_url:
            return "http"
        return "stdio"

    def _infer_secrets(self, connector: CodexAppConnector) -> List[str]:
        hints: Set[str] = set()
        text = connector.name.lower()
        for key, secrets in SECRET_HINTS.items():
            if key in text:
                hints.update(secrets)
        for cat in connector.categories:
            cat_lower = cat.lower()
            if "finance" in cat_lower or "banking" in cat_lower or "crypto" in cat_lower:
                hints.update(["API_KEY", "API_SECRET"])
            elif "developer" in cat_lower or "devops" in cat_lower:
                hints.add("API_TOKEN")
            elif "crm" in cat_lower or "sales" in cat_lower or "support" in cat_lower:
                hints.add("ACCESS_TOKEN")
        return sorted(hints)

    def _server_name(self, connector: CodexAppConnector) -> str:
        return f"codex_app_{connector.slug}"

    def _build_config(self, connector: CodexAppConnector, bridge_url: str) -> MCPServerConfig:
        transport = self._infer_transport(connector)
        secrets = self._infer_secrets(connector)
        tags = list(connector.categories) + list(connector.action_flags)
        return MCPServerConfig(
            name=self._server_name(connector),
            url=f"{bridge_url}/{connector.id}",
            transport=transport,
            description=connector.description or f"Codex Apps connector: {connector.name}",
            tags=tags,
            env_vars={},
            defer_loading=True,
        )

    # -------------------------------------------------------------------------
    # Activation API
    # -------------------------------------------------------------------------
    async def activate_connector(self, key: str) -> ActivationResult:
        connector = self.adapter.get(key)
        if not connector:
            return ActivationResult(
                connector_id=key,
                name=key,
                activated=False,
                server_name="",
                transport="",
                url="",
                bridge_url="",
                error=f"Connector not found: {key}",
            )

        bridge_url = await self._ensure_virtual_bridge()
        if not bridge_url:
            return ActivationResult(
                connector_id=connector.id,
                name=connector.name,
                activated=False,
                server_name=self._server_name(connector),
                transport=self._infer_transport(connector),
                url="",
                bridge_url="",
                error="Virtual bridge failed to start",
                secrets_hint=self._infer_secrets(connector),
            )

        server_name = self._server_name(connector)
        config = self._build_config(connector, bridge_url)
        registry = get_mcp_registry()
        try:
            registry.register_server(config)
            if server_name not in self._state["activated"]:
                self._state["activated"].append(server_name)
            self._state["activated_at"][server_name] = datetime.now(timezone.utc).isoformat()
            self._save_state()
            logger.info(f"Activated Codex App connector: {connector.name} as {server_name}")
            return ActivationResult(
                connector_id=connector.id,
                name=connector.name,
                activated=True,
                server_name=server_name,
                transport=config.transport,
                url=config.url,
                bridge_url=bridge_url,
                secrets_hint=self._infer_secrets(connector),
            )
        except Exception as e:
            logger.error(f"Failed to activate {connector.name}: {e}")
            return ActivationResult(
                connector_id=connector.id,
                name=connector.name,
                activated=False,
                server_name=server_name,
                transport=config.transport,
                url=config.url,
                bridge_url=bridge_url,
                error=str(e),
                secrets_hint=self._infer_secrets(connector),
            )

    async def deactivate_connector(self, key: str) -> Dict[str, Any]:
        connector = self.adapter.get(key)
        if not connector:
            return {"connector_id": key, "deactivated": False, "error": "Connector not found"}

        server_name = self._server_name(connector)
        registry = get_mcp_registry()
        try:
            registry.unregister_server(server_name)
            self._state["activated"] = [s for s in self._state["activated"] if s != server_name]
            self._state["activated_at"].pop(server_name, None)
            self._save_state()
            if not self._state["activated"]:
                self._stop_virtual_bridge()
            logger.info(f"Deactivated Codex App connector: {connector.name}")
            return {"connector_id": connector.id, "name": connector.name, "server_name": server_name, "deactivated": True}
        except Exception as e:
            return {"connector_id": connector.id, "name": connector.name, "server_name": server_name, "deactivated": False, "error": str(e)}

    async def activate_bundle(self, bundle_id: str) -> Dict[str, Any]:
        try:
            bundle = self.adapter.bundle(bundle_id)
        except KeyError as e:
            return {"bundle_id": bundle_id, "error": str(e)}

        results = []
        activated = 0
        failed = 0
        for wrapper in bundle["connectors"]:
            res = await self.activate_connector(wrapper["connector_id"])
            results.append(res)
            if res.activated:
                activated += 1
            else:
                failed += 1

        return {
            "bundle_id": bundle_id,
            "name": bundle["name"],
            "activated": activated,
            "failed": failed,
            "results": [self._activation_to_dict(r) for r in results],
        }

    def list_activated(self) -> List[Dict[str, Any]]:
        registry = get_mcp_registry()
        vault = get_codex_apps_vault()
        activated = []
        for server_name in self._state.get("activated", []):
            info = registry.get_server(server_name)
            if not info:
                continue
            # Recover connector id from URL path
            connector_id = info.url.rsplit("/", 1)[-1]
            activated.append({
                "server_name": server_name,
                "url": info.url,
                "transport": info.transport,
                "description": info.description,
                "tags": info.tags,
                "activated_at": self._state["activated_at"].get(server_name),
                "authorized": vault.has_secret(connector_id, "token"),
            })
        return activated

    # -------------------------------------------------------------------------
    # Serialization helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _activation_to_dict(result: ActivationResult) -> Dict[str, Any]:
        return {
            "connector_id": result.connector_id,
            "name": result.name,
            "activated": result.activated,
            "server_name": result.server_name,
            "transport": result.transport,
            "url": result.url,
            "bridge_url": result.bridge_url,
            "error": result.error,
            "secrets_hint": result.secrets_hint,
        }

    def shutdown(self) -> None:
        """Stop the virtual bridge."""
        self._stop_virtual_bridge()


# Singleton factory
_runtime: Optional[CodexAppsRuntime] = None


def get_codex_apps_runtime() -> CodexAppsRuntime:
    """Get the global CodexAppsRuntime instance."""
    global _runtime
    if _runtime is None:
        _runtime = CodexAppsRuntime()
    return _runtime
