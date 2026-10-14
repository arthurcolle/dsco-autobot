"""Delegate only server-validated execution identity to native workers.

The HTTP/MCP boundary authenticates the caller before invoking these helpers.
The snapshot lives outside public tool arguments, contains no caller bearer,
and is signed afresh for the selected backend when execution begins.
"""

from __future__ import annotations

from collections.abc import Mapping
import os
from typing import Any, Dict, Optional

from ..auth.service_context import ServiceContextError, sign_service_context


class DelegatedExecutionError(ServiceContextError):
    """A trusted caller identity cannot be delegated safely."""


def validated_identity_snapshot(identity: Mapping[str, Any]) -> Dict[str, Any]:
    """Keep only the claims needed from an already authenticated identity.

    This function does not authenticate arbitrary dictionaries. Callers must
    supply the result of ``resolve_execution_identity`` or its private bound
    copy, never data read from public tool arguments or request JSON.
    """
    if not isinstance(identity, Mapping):
        raise DelegatedExecutionError("EXECUTION_IDENTITY_REQUIRED: authenticated caller identity is required")
    kind = identity.get("kind")
    if not isinstance(kind, str) or kind not in {"operator_service", "customer", "customer_trial"}:
        raise DelegatedExecutionError("EXECUTION_IDENTITY_REQUIRED: validated identity kind is required")
    account_id = identity.get("tenant_id")
    if account_id is not None and (not isinstance(account_id, str) or not account_id.strip()):
        raise DelegatedExecutionError("EXECUTION_IDENTITY_INVALID: account must be a non-empty string")
    if str(kind).startswith("customer") and not account_id:
        raise DelegatedExecutionError("EXECUTION_IDENTITY_INVALID: customer identity requires an account")
    record = identity.get("user") or identity.get("identity") or {}
    if not isinstance(record, Mapping):
        raise DelegatedExecutionError("EXECUTION_IDENTITY_INVALID: principal identity must be an object")
    principal_id = (
        record.get("principal_id") or record.get("subject")
        or record.get("sub") or record.get("user_id")
    )
    issuer = record.get("identity_issuer") or record.get("issuer")
    return {
        "kind": kind,
        "tenant_id": account_id,
        "identity": {
            "principal_id": str(principal_id) if principal_id else None,
            "issuer": str(issuer) if issuer else None,
        },
    }


def prepare_delegated_arguments(
    backend_id: str,
    arguments: Mapping[str, Any],
    execution_identity: Mapping[str, Any],
    *,
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Copy tool arguments and attach an audience-bound signed context.

    Reuse the canonical sync path's ``_dsco_context`` envelope and shared
    backend secret. A missing identity or signing key fails before a provider
    invocation; it never changes a customer call into an operator call.
    """
    if "_dsco_context" in arguments:
        raise DelegatedExecutionError("EXECUTION_CONTEXT_FORBIDDEN: caller-supplied delegated context is forbidden")
    identity = validated_identity_snapshot(execution_identity)
    outbound = dict(arguments)
    signed_backends = {
        item.strip()
        for item in os.getenv(
            "SIGNED_CONTEXT_BACKENDS", "distributed-search-mcp,distributed-search"
        ).split(",")
        if item.strip()
    }
    if backend_id not in signed_backends:
        return outbound
    signing_secret = os.getenv("BACKEND_BEARER_TOKEN", "")
    if not signing_secret:
        raise DelegatedExecutionError("EXECUTION_CONTEXT_UNAVAILABLE: backend signing secret is not configured")
    record = identity["identity"]
    outbound["_dsco_context"] = sign_service_context(
        signing_secret,
        audience=backend_id,
        account_id=identity["tenant_id"],
        principal_id=record["principal_id"],
        issuer=record["issuer"],
        identity_kind=identity["kind"],
        trace_id=trace_id,
        session_id=session_id,
    )
    return outbound
