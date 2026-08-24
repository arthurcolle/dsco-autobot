"""Production boundary coverage for the legacy Codex Apps credential vault."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tool_management.integrations.codex_apps_vault import CodexAppsVault
from tool_management.integrations.codex_apps_virtual_bridge import create_subapp


MODAL_APP = Path(__file__).resolve().parents[1] / "deploy" / "modal" / "modal_app.py"


def _force_file_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        CodexAppsVault,
        "_keychain_available",
        staticmethod(lambda: False),
    )


def test_production_vault_refuses_machine_derived_fallback(monkeypatch, tmp_path):
    _force_file_backend(monkeypatch)
    monkeypatch.setenv("DSCO_DEPLOYMENT_MODE", "production")
    monkeypatch.delenv("CODEX_VAULT_MASTER_KEY", raising=False)

    with pytest.raises(RuntimeError, match="CODEX_VAULT_MASTER_KEY is required"):
        CodexAppsVault(fallback_file=tmp_path / "vault.enc")


def test_production_bridge_startup_reaches_the_fail_closed_vault(monkeypatch):
    """The deployed in-process bridge constructs this vault during app creation."""
    _force_file_backend(monkeypatch)
    monkeypatch.setenv("DSCO_DEPLOYMENT_MODE", "production")
    monkeypatch.delenv("CODEX_VAULT_MASTER_KEY", raising=False)
    # Drop any vault cached by an earlier test in this process: the bridge
    # resolves via get_codex_apps_vault(), whose module-global would otherwise
    # short-circuit construction-time enforcement entirely.
    import tool_management.integrations.codex_apps_vault as vault_module
    monkeypatch.setattr(vault_module, "_vault", None)

    with pytest.raises(RuntimeError, match="CODEX_VAULT_MASTER_KEY is required"):
        create_subapp()


def test_nonproduction_keeps_local_encrypted_fallback(monkeypatch, tmp_path):
    _force_file_backend(monkeypatch)
    monkeypatch.delenv("DSCO_DEPLOYMENT_MODE", raising=False)
    monkeypatch.delenv("CODEX_VAULT_MASTER_KEY", raising=False)

    vault = CodexAppsVault(fallback_file=tmp_path / "vault.enc")
    assert vault.set_secret("connector", "token", "development-only-secret")
    assert vault.get_secret("connector", "token") == "development-only-secret"
    assert b"development-only-secret" not in (tmp_path / "vault.enc").read_bytes()


def test_production_vault_uses_explicit_key(monkeypatch, tmp_path):
    _force_file_backend(monkeypatch)
    monkeypatch.setenv("DSCO_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("CODEX_VAULT_MASTER_KEY", "unit-test-master-key")

    vault = CodexAppsVault(fallback_file=tmp_path / "vault.enc")
    assert vault.set_secret("connector", "token", "production-secret")
    assert vault.get_secret("connector", "token") == "production-secret"
    assert b"production-secret" not in (tmp_path / "vault.enc").read_bytes()


def test_modal_registry_requires_dedicated_vault_secret_for_all_registry_functions():
    tree = ast.parse(MODAL_APP.read_text(encoding="utf-8"))
    secret_assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "codex_apps_vault_secret"
            for target in node.targets
        )
    )
    call = secret_assignment.value
    assert isinstance(call, ast.Call)
    assert isinstance(call.func, ast.Attribute)
    assert call.func.attr == "from_name"
    assert isinstance(call.args[0], ast.Constant)
    assert call.args[0].value == "tool-registry-codex-apps-vault"
    required_keys = next(keyword.value for keyword in call.keywords if keyword.arg == "required_keys")
    assert ast.literal_eval(required_keys) == ["CODEX_VAULT_MASTER_KEY"]

    registry_secret_assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "registry_secrets"
            for target in node.targets
        )
    )
    assert any(
        isinstance(item, ast.Name) and item.id == "codex_apps_vault_secret"
        for item in registry_secret_assignment.value.elts
    )
