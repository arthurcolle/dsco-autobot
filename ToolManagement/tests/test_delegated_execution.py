"""Regressions for signed worker delegation without a customer auth bypass."""

import importlib.util
from pathlib import Path
import sys
from types import ModuleType

import pytest


SOURCE = Path(__file__).parents[1] / "src" / "tool_management"


@pytest.fixture
def delegation(monkeypatch):
    prefix = "_delegated_contract"
    for suffix, directory in (("", SOURCE), (".core", SOURCE / "core"), (".auth", SOURCE / "auth")):
        package = ModuleType(prefix + suffix)
        package.__path__ = [str(directory)]
        monkeypatch.setitem(sys.modules, package.__name__, package)
    modules = []
    for relative in ("auth.service_context", "core.delegated_execution"):
        path = SOURCE.joinpath(*relative.split(".")).with_suffix(".py")
        spec = importlib.util.spec_from_file_location(prefix + "." + relative, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, module.__name__, module)
        spec.loader.exec_module(module)
        modules.append(module)
    monkeypatch.setenv("BACKEND_BEARER_TOKEN", "test-only-backend-secret")
    monkeypatch.delenv("SIGNED_CONTEXT_BACKENDS", raising=False)
    return modules[1], modules[0]


def customer():
    return {
        "kind": "customer_trial", "tenant_id": "account-7",
        "user": {"principal_id": "human-3", "identity_issuer": "https://id.example", "access_token": "never-forward"},
        "authorization": "Bearer never-forward",
    }


def test_customer_delegation_preserves_account_principal_and_audience(delegation):
    helper, signer = delegation
    inputs = {"texts": ["weather"], "labels": ["weather", "software"]}
    outbound = helper.prepare_delegated_arguments(
        "distributed-search-mcp", inputs, customer(), trace_id="trace-1", session_id="session-2")
    claims = signer.verify_service_context(
        outbound["_dsco_context"], "test-only-backend-secret", audience="distributed-search-mcp")
    assert claims["kind"] == "customer_trial"
    assert claims["account_id"] == "account-7"
    assert claims["principal_id"] == "human-3"
    assert claims["iss"] == "https://id.example"
    assert claims["trace_id"] == "trace-1"
    assert claims["session_id"] == "session-2"
    assert outbound["texts"] == inputs["texts"]
    assert "_dsco_context" not in inputs
    assert "never-forward" not in repr(outbound)
    with pytest.raises(signer.ServiceContextError, match="audience mismatch"):
        signer.verify_service_context(outbound["_dsco_context"], "test-only-backend-secret", audience="different-worker")


def test_durable_snapshot_excludes_bearer_and_unneeded_claims(delegation):
    helper, _ = delegation
    snapshot = helper.validated_identity_snapshot(customer())
    assert snapshot == {
        "kind": "customer_trial", "tenant_id": "account-7",
        "identity": {"principal_id": "human-3", "issuer": "https://id.example"},
    }
    assert helper.validated_identity_snapshot(snapshot) == snapshot


@pytest.mark.parametrize("identity", [None, {}, {"kind": []}, {"kind": "unknown"}, {"kind": "customer"},
                                        {"kind": "customer_trial", "tenant_id": ""},
                                        {"kind": "customer", "tenant_id": 9}])
def test_missing_or_malformed_identity_never_falls_back_to_operator(delegation, identity):
    helper, _ = delegation
    with pytest.raises(helper.DelegatedExecutionError, match="EXECUTION_IDENTITY"):
        helper.prepare_delegated_arguments("distributed-search-mcp", {}, identity)


def test_signed_backend_without_shared_secret_fails_closed(delegation, monkeypatch):
    helper, _ = delegation
    monkeypatch.delenv("BACKEND_BEARER_TOKEN")
    with pytest.raises(helper.DelegatedExecutionError, match="EXECUTION_CONTEXT_UNAVAILABLE"):
        helper.prepare_delegated_arguments("distributed-search-mcp", {}, customer())


@pytest.mark.parametrize("backend", ["distributed-search-mcp", "unrelated-worker"])
def test_caller_cannot_supply_signed_context_even_if_valid(delegation, backend):
    helper, signer = delegation
    forged_arguments = {"_dsco_context": signer.sign_service_context(
        "test-only-backend-secret", audience=backend, account_id=None,
        principal_id=None, issuer=None, identity_kind="operator_service")}
    with pytest.raises(helper.DelegatedExecutionError, match="EXECUTION_CONTEXT_FORBIDDEN"):
        helper.prepare_delegated_arguments(backend, forged_arguments, customer())


def test_operator_context_is_signed_without_customer_bearer(delegation):
    helper, signer = delegation
    outbound = helper.prepare_delegated_arguments(
        "distributed-search", {"query": "weather"}, {"kind": "operator_service", "tenant_id": None,
                                                     "identity": {"subject": "service-1", "issuer": "https://id.example"}})
    claims = signer.verify_service_context(outbound["_dsco_context"], "test-only-backend-secret", audience="distributed-search")
    assert claims["kind"] == "operator_service"
    assert claims["account_id"] is None
    assert claims["principal_id"] == "service-1"


def test_context_is_minted_per_invocation_and_unsigned_workers_remain_unchanged(delegation):
    helper, _ = delegation
    identity = customer()
    first = helper.prepare_delegated_arguments("distributed-search-mcp", {}, identity)
    second = helper.prepare_delegated_arguments("distributed-search-mcp", {}, identity)
    assert first["_dsco_context"]["claims"]["invocation_id"] != second["_dsco_context"]["claims"]["invocation_id"]
    assert helper.prepare_delegated_arguments("distributed-memory-mcp", {"limit": 1}, identity) == {"limit": 1}


def test_explicit_signed_backend_configuration_uses_same_envelope(delegation, monkeypatch):
    helper, signer = delegation
    monkeypatch.setenv("SIGNED_CONTEXT_BACKENDS", " custom-worker , distributed-search-mcp ")
    outbound = helper.prepare_delegated_arguments("custom-worker", {}, customer())
    claims = signer.verify_service_context(outbound["_dsco_context"], "test-only-backend-secret", audience="custom-worker")
    assert claims["account_id"] == "account-7"
