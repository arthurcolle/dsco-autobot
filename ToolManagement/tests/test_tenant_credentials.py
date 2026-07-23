import os

import pytest

from tool_management.services.tenant_credentials import (
    CredentialResolver,
    InMemoryTenantCredentialStore,
    OPERATOR_TENANT_ID,
)


def _resolver() -> CredentialResolver:
    return CredentialResolver(store=InMemoryTenantCredentialStore())


def test_no_credential_falls_back_to_sandbox():
    """Unchanged today's-behavior case: no tenant key, no pooled fallback -> None."""
    resolver = _resolver()
    assert resolver.resolve("tenant-a", "alpaca") is None
    assert resolver.is_live("tenant-a", "alpaca") is False


def test_tenant_byok_credential_resolves_and_flips_live():
    resolver = _resolver()
    resolver.store.set_secret("tenant-a", "alpaca", "key-abc123")

    assert resolver.resolve("tenant-a", "alpaca") == "key-abc123"
    assert resolver.is_live("tenant-a", "alpaca") is True

    # A different tenant with no key of their own must NOT see tenant-a's key.
    assert resolver.resolve("tenant-b", "alpaca") is None
    assert resolver.is_live("tenant-b", "alpaca") is False


def test_tenant_credential_takes_priority_over_pooled_fallback(monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "operator-pooled-key")
    resolver = _resolver()
    resolver.store.set_secret("tenant-a", "alphavantage", "tenant-own-key")

    result = resolver.resolve(
        "tenant-a", "alphavantage",
        env_fallback_var="ALPHA_VANTAGE_API_KEY",
        allow_pooled_fallback=True,
    )
    assert result == "tenant-own-key"


def test_pooled_fallback_only_when_explicitly_allowed(monkeypatch):
    monkeypatch.setenv("ALPHA_VANTAGE_API_KEY", "operator-pooled-key")
    resolver = _resolver()

    # allow_pooled_fallback=False (default) -> no pooled key leaks even though
    # the operator's env var is present. Caller must opt in per category.
    assert resolver.resolve("tenant-a", "alphavantage", env_fallback_var="ALPHA_VANTAGE_API_KEY") is None

    # Explicit opt-in for an allow-listed (cheap/read-only) category.
    result = resolver.resolve(
        "tenant-a", "alphavantage",
        env_fallback_var="ALPHA_VANTAGE_API_KEY",
        allow_pooled_fallback=True,
    )
    assert result == "operator-pooled-key"


def test_operator_tenant_id_never_reads_tenant_store(monkeypatch):
    """OPERATOR_TENANT_ID is reserved: it must never be looked up in the
    per-tenant store, only ever resolve via the pooled/env fallback path."""
    monkeypatch.setenv("STRIPE_API_KEY", "operator-stripe-key")
    resolver = _resolver()
    # Even if something accidentally wrote a secret under the reserved id,
    # resolve() must skip the tenant-store lookup for OPERATOR_TENANT_ID.
    resolver.store.set_secret(OPERATOR_TENANT_ID, "stripe", "should-never-be-read")

    result = resolver.resolve(
        OPERATOR_TENANT_ID, "stripe",
        env_fallback_var="STRIPE_API_KEY",
        allow_pooled_fallback=True,
    )
    assert result == "operator-stripe-key"


def test_delete_and_relist_providers():
    resolver = _resolver()
    resolver.store.set_secret("tenant-a", "github", "gh-token-1")
    resolver.store.set_secret("tenant-a", "slack", "xoxb-token-1")

    assert resolver.store.list_providers("tenant-a") == ["github", "slack"]

    assert resolver.store.delete_secret("tenant-a", "github") is True
    assert resolver.store.list_providers("tenant-a") == ["slack"]
    assert resolver.resolve("tenant-a", "github") is None


def test_metadata_never_exposes_secret_value():
    resolver = _resolver()
    resolver.store.set_secret("tenant-a", "notion", "secret-value-xyz", label="Notion workspace")

    meta = resolver.store.get_metadata("tenant-a", "notion")
    assert meta is not None
    assert meta.provider == "notion"
    assert meta.tenant_id == "tenant-a"
    assert meta.label == "Notion workspace"
    # Metadata object has no attribute that could hold the raw secret.
    assert not hasattr(meta, "value")
    assert not hasattr(meta, "secret")


def test_file_backend_round_trip(tmp_path, monkeypatch):
    from tool_management.services.tenant_credentials import FileTenantCredentialStore

    monkeypatch.setenv("TENANT_VAULT_MASTER_KEY", "test-master-key-for-unit-tests")
    store = FileTenantCredentialStore(vault_file=tmp_path / "vault.enc")

    store.set_secret("tenant-x", "github", "gh-secret-1")
    assert store.has_secret("tenant-x", "github") is True
    assert store.get_secret("tenant-x", "github") == "gh-secret-1"

    # File on disk must not contain the plaintext secret (encrypted at rest).
    raw = (tmp_path / "vault.enc").read_bytes()
    assert b"gh-secret-1" not in raw

    # A fresh store instance pointed at the same file + same master key
    # must be able to decrypt and read the same secret back.
    store2 = FileTenantCredentialStore(vault_file=tmp_path / "vault.enc")
    assert store2.get_secret("tenant-x", "github") == "gh-secret-1"
