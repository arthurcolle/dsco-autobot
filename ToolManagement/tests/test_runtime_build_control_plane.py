"""Focused RuntimeSpec approval/build control-plane tests."""
from __future__ import annotations

import asyncio
import base64
import copy
import os
import pathlib
import tempfile

import pytest
from fastapi import HTTPException

ROOT = pathlib.Path(__file__).resolve().parents[3]
FACTORY = ROOT / "dsco-cli" / "scripts" / "cloud_build_factory.py"


def runtime_spec() -> dict:
    # This is intentionally the public configurator shape: no lease, source
    # context, signer reference, or credential is allowed at this boundary.
    spec = {
        "apiVersion": "distributed.systems/v1alpha1",
        "kind": "RuntimeSpec",
        "metadata": {"name": "tenant-a.runtime-01", "reproducible": True},
        "connections": [
            {"provider": "openai", "ref": "account://openai", "credentialMaterial": "excluded"},
            {"provider": "local", "ref": "local://discovery", "credentialMaterial": "excluded"},
        ],
        "routing": {"primary": {"provider": "openai", "model": "openai/codex-default"}, "fallback": "next-connected", "eligibleProviders": ["openai", "local"], "routes": [{"provider": "openai", "model": "openai/codex-default"}, {"provider": "local", "model": "local/auto"}]},
        "tools": [{"id": "repository", "scope": "runtime-policy"}],
        "governance": {"preset": "guarded", "approvals": ["external_write"], "perRunBudgetUsd": 3, "secretIsolation": "account-vault-reference-only", "chronicle": "required"},
        "build": {"artifact": "signed-runtime-bundle", "isolation": "customer-specific", "hardening": ["policy-embedded"], "reproducibleFrom": "this-runtime-spec"},
        "deployment": {"target": "dsco-cloud", "authority": "signed-runtime-spec"},
        "network": {"allowedHosts": ["tools.distributed.systems"]},
    }
    import importlib.util
    factory_spec = importlib.util.spec_from_file_location("test_factory", FACTORY)
    factory = importlib.util.module_from_spec(factory_spec)
    assert factory_spec and factory_spec.loader
    factory_spec.loader.exec_module(factory)
    spec["integrity"] = {"canonicalization": "JCS-compatible", "digest": "sha256:" + factory.runtime_spec_digest(spec), "signature": "issued-after-account-approval", "artifactAttestation": "issued-with-customer-build"}
    return spec


@pytest.fixture()
def control_plane(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setenv("DATA_DIR", tmp)
        monkeypatch.setenv("DSCO_CLOUD_BUILD_FACTORY_PATH", str(FACTORY))
        from tool_management.persistence import runtime_build_store
        from tool_management.persistence import billing_store
        from tool_management.api import runtime_build_routes as routes
        from tool_management.core import rate_limit

        runtime_build_store._store = None
        billing_store._billing_store = None
        rate_limit._quota_manager = None
        routes._factory.cache_clear()
        monkeypatch.setattr(routes, "resolve_execution_identity", lambda request: {"kind": "customer", "tenant_id": "tenant-a"})
        yield routes
        runtime_build_store._store = None
        billing_store._billing_store = None
        rate_limit._quota_manager = None


def test_runtime_build_is_tenant_owned_idempotent_and_fail_closed(control_plane):
    routes = control_plane
    body = routes.RuntimeBuildCreate(runtime_spec=runtime_spec())
    first = asyncio.run(routes.create_runtime_build(body, object(), "idem-runtime-001"))
    assert first["status"] == "pending_approval"
    assert first["approval"] == "required"
    assert first["artifact"] is None
    assert first["idempotent_replay"] is False

    replay = asyncio.run(routes.create_runtime_build(body, object(), "idem-runtime-001"))
    assert replay["request_id"] == first["request_id"]
    assert replay["idempotent_replay"] is True

    approved = asyncio.run(routes.approve_runtime_build(first["request_id"], object()))
    assert approved["approval"] == "approved"
    assert approved["status"] == "blocked"
    assert approved["artifact"] is None
    assert approved["blocker_code"] == "private_build_worker_unavailable"


def test_runtime_build_persists_capsule_identity(control_plane):
    routes = control_plane
    binding = routes.EntityBinding(
        organization_id="11111111-1111-4111-8111-111111111111",
        deployment_id="22222222-2222-4222-8222-222222222222",
        capsule_sha256="a" * 64,
        policy_sha256="b" * 64,
        source_sha256="c" * 64,
    )
    body = routes.RuntimeBuildCreate(runtime_spec=runtime_spec(), entity_binding=binding)
    first = asyncio.run(routes.create_runtime_build(body, object(), "idem-runtime-bound-001"))
    replay = asyncio.run(routes.create_runtime_build(body, object(), "idem-runtime-bound-001"))
    assert first["entity_binding"] == binding.model_dump()
    assert replay["request_id"] == first["request_id"]


def test_runtime_build_rejects_credential_fields_before_persistence(control_plane):
    routes = control_plane
    spec = runtime_spec()
    spec["connections"][0]["api_key"] = "never-store-this"
    with pytest.raises(HTTPException) as error:
        routes._validate_runtime_spec(spec)
    assert error.value.status_code == 422
    assert error.value.detail["error"] == "runtime_spec_credentials_forbidden"


def test_runtime_build_idempotency_cannot_be_reused_for_different_spec(control_plane):
    routes = control_plane
    first_spec = runtime_spec()
    asyncio.run(routes.create_runtime_build(routes.RuntimeBuildCreate(runtime_spec=first_spec), object(), "idem-runtime-002"))
    second_spec = copy.deepcopy(first_spec)
    second_spec["governance"]["perRunBudgetUsd"] = 4
    # Keep the client-side RuntimeSpec self-digest valid so this specifically
    # proves the idempotency collision, rather than schema validation.
    import importlib.util
    factory_spec = importlib.util.spec_from_file_location("test_factory_2", FACTORY)
    factory = importlib.util.module_from_spec(factory_spec)
    assert factory_spec and factory_spec.loader
    factory_spec.loader.exec_module(factory)
    second_spec["integrity"]["digest"] = "sha256:" + factory.runtime_spec_digest(second_spec)
    with pytest.raises(HTTPException) as error:
        asyncio.run(routes.create_runtime_build(routes.RuntimeBuildCreate(runtime_spec=second_spec), object(), "idem-runtime-002"))
    assert error.value.status_code == 409


def test_account_page_accepts_fragment_handoff_without_server_key_parameter():
    from tool_management.api.user_routes import _ACCOUNT_PAGE
    assert "runtime-spec" in _ACCOUNT_PAGE
    assert "history.replaceState" in _ACCOUNT_PAGE
    assert "crypto.randomUUID" in _ACCOUNT_PAGE
    assert "'='.repeat((4 - (base64.length % 4)) % 4)" in _ACCOUNT_PAGE
    assert "bytes.byteLength > 65536" in _ACCOUNT_PAGE
    assert "RuntimeSpec imported securely. Sign in or create an account" in _ACCOUNT_PAGE
