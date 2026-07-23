import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api.codex_apps_routes import router
from tool_management.integrations.codex_apps_adapter import CodexAppsCatalogAdapter


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
                        "labels": {"retrievable": "true", "consequential": "true", "sync": "true"},
                        "appMetadata": {"categories": ["DEVELOPER_TOOLS"], "review": {"status": "UNSET"}},
                    },
                    {
                        "id": "connector_box",
                        "name": "Box",
                        "description": "Securely access Box content",
                        "distributionChannel": "ECOSYSTEM_DIRECTORY",
                        "isEnabled": True,
                        "isAccessible": True,
                        "labels": {"retrievable": "true", "sync": "true"},
                        "appMetadata": {"categories": ["BUSINESS"], "review": {"status": "UNSET"}},
                    },
                    {
                        "id": "connector_apollo",
                        "name": "Apollo.io",
                        "description": "Find buyers and close deals",
                        "distributionChannel": "ECOSYSTEM_DIRECTORY",
                        "isEnabled": True,
                        "isAccessible": False,
                        "labels": {"consequential": "true"},
                        "appMetadata": {"categories": ["BUSINESS"], "review": {"status": "UNSET"}},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    manifest = tmp_path / "github.app.json"
    manifest.write_text(
        json.dumps(
            {
                "apps": {
                    "github": {"id": "connector_github"},
                    "missing": {"id": "connector_missing"},
                }
            }
        ),
        encoding="utf-8",
    )
    return cache, manifest


def test_codex_apps_adapter_normalizes_connector_wrappers(tmp_path):
    cache, manifest = _write_fixture(tmp_path)
    adapter = CodexAppsCatalogAdapter([cache], [manifest])

    summary = adapter.summary()
    assert summary["total_connectors"] == 3
    assert summary["categories"]["BUSINESS"] == 2
    assert summary["labels"]["consequential=true"] == 2
    assert summary["curated_matched"] == 1
    assert summary["curated_stale"] == 1

    github = adapter.get("github")
    assert github is not None
    assert github.id == "connector_github"
    assert github.catalog_status == "catalog_only"

    wrapper = adapter.build_wrapper("github").to_dict()
    assert wrapper["name"] == "codex_app__github"
    assert wrapper["live"] is False
    assert wrapper["requires_confirmation"] is True
    assert "not_live" in wrapper["action_flags"]
    assert wrapper["metadata"]["not_executable_reason"]

    box = adapter.build_wrapper("Box").to_dict()
    assert box["live"] is True
    assert box["metadata"]["not_executable_reason"] is None
    assert "sync" in box["action_flags"]


def test_codex_apps_adapter_query_and_bundle(tmp_path):
    cache, manifest = _write_fixture(tmp_path)
    adapter = CodexAppsCatalogAdapter([cache], [manifest])

    business = adapter.query(category="business")
    assert business["total"] == 2

    consequential = adapter.query(label="consequential")
    assert {c["name"] for c in consequential["connectors"]} == {"GitHub", "Apollo.io"}

    bundle = adapter.bundle("gtm_support")
    assert any(c["display_name"] == "Apollo.io" for c in bundle["connectors"])

    stale = adapter.stale_curated_pointers()
    assert len(stale) == 1
    assert stale[0].connector_id == "connector_missing"


def test_codex_apps_routes_use_env_paths(tmp_path, monkeypatch):
    cache, manifest = _write_fixture(tmp_path)
    monkeypatch.setenv("CODEX_APP_DIRECTORY_CACHE", str(cache))
    monkeypatch.setenv("CODEX_APP_PLUGIN_MANIFESTS", str(manifest))

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    summary = client.get("/api/v1/codex-apps/summary")
    assert summary.status_code == 200
    assert summary.json()["total_connectors"] == 3

    wrappers = client.get("/api/v1/codex-apps/wrappers", params={"label": "sync"})
    assert wrappers.status_code == 200
    assert wrappers.json()["total"] == 2

    one = client.get("/api/v1/codex-apps/wrappers/github")
    assert one.status_code == 200
    assert one.json()["connector_id"] == "connector_github"
