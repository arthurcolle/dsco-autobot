import json
from pathlib import Path

import pytest

from tool_management.integrations.codex_apps_adapter import CodexAppsCatalogAdapter
from tool_management.integrations.codex_apps_runtime import CodexAppsRuntime


def _write_fixture(tmp_path: Path):
    cache = tmp_path / "codex_apps.json"
    cache.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "connectors": [
                    {
                        "id": "connector_github",
                        "name": "GitHub",
                        "description": "Access repositories, issues, and pull requests.",
                        "distributionChannel": "DEFAULT_OAI_CATALOG",
                        "installUrl": "https://example.test/github",
                        "isEnabled": True,
                        "isAccessible": False,
                        "labels": {"retrievable": "true", "consequential": "true"},
                        "appMetadata": {"categories": ["DEVELOPER_TOOLS"], "review": {"status": "UNSET"}},
                    },
                    {
                        "id": "connector_jina",
                        "name": "Jina AI",
                        "description": "Search and read via Jina MCP",
                        "distributionChannel": "ECOSYSTEM_DIRECTORY",
                        "installUrl": "https://mcp.jina.ai/sse",
                        "isEnabled": True,
                        "isAccessible": False,
                        "labels": {"retrievable": "true"},
                        "appMetadata": {"categories": ["AI"], "review": {"status": "UNSET"}},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return cache


import asyncio


def test_runtime_activate_and_deactivate(tmp_path):
    cache = _write_fixture(tmp_path)
    adapter = CodexAppsCatalogAdapter([cache], [])
    runtime = CodexAppsRuntime(adapter=adapter, state_file=tmp_path / "state.json", project_root=tmp_path, bridge_enabled=False)

    result = asyncio.run(runtime.activate_connector("github"))
    assert result.activated
    assert result.server_name == "codex_app_github"
    assert result.transport == "http"
    assert "GITHUB_TOKEN" in result.secrets_hint

    activated = runtime.list_activated()
    assert len(activated) == 1
    assert activated[0]["server_name"] == "codex_app_github"

    deactivate = asyncio.run(runtime.deactivate_connector("github"))
    assert deactivate["deactivated"]
    assert len(runtime.list_activated()) == 0


def test_runtime_jina_transport_inference(tmp_path):
    cache = _write_fixture(tmp_path)
    adapter = CodexAppsCatalogAdapter([cache], [])
    runtime = CodexAppsRuntime(adapter=adapter, state_file=tmp_path / "state.json", project_root=tmp_path, bridge_enabled=False)
    result = asyncio.run(runtime.activate_connector("jina_ai"))
    assert result.activated
    assert result.transport == "mcp"
    assert "JINA_API_KEY" in result.secrets_hint
