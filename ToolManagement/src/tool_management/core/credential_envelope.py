"""Short-lived, backend-bound transport envelopes for tenant BYOK secrets."""

from __future__ import annotations

import base64
import json
import os
import time
from typing import Any, Dict

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CredentialEnvelopeError(ValueError):
    pass


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii")


def _b64decode(value: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(value.encode("ascii"))
    except Exception as exc:
        raise CredentialEnvelopeError("malformed credential envelope") from exc


def _key_for_backend(backend_id: str) -> bytes:
    env_name = "BYOK_ENVELOPE_KEY_" + "".join(
        char if char.isalnum() else "_" for char in backend_id.upper()
    )
    value = os.getenv(env_name, "").strip()
    if not value:
        raise CredentialEnvelopeError(f"credential envelope key is not configured for {backend_id}")
    try:
        key = _b64decode(value)
    except CredentialEnvelopeError:
        try:
            key = bytes.fromhex(value)
        except ValueError as exc:
            raise CredentialEnvelopeError("credential envelope key must be base64 or hex") from exc
    if len(key) not in (16, 24, 32):
        raise CredentialEnvelopeError("credential envelope key must be 128, 192, or 256 bits")
    return key


def _aad(claims: Dict[str, Any]) -> bytes:
    return (
        "dsco:byok-envelope:v1\0{backend_id}\0{tenant_id}\0{provider}\0{exp}".format(**claims)
    ).encode("utf-8")


def seal_credential_envelope(
    credential: str,
    *,
    tenant_id: str,
    provider: str,
    backend_id: str,
    ttl_seconds: int = 60,
) -> str:
    if not credential or not tenant_id or not provider or not backend_id:
        raise CredentialEnvelopeError("credential envelope requires credential, tenant, provider, and backend")
    claims = {
        "v": 1,
        "tenant_id": tenant_id,
        "provider": provider,
        "backend_id": backend_id,
        "exp": int(time.time()) + max(1, min(int(ttl_seconds), 300)),
    }
    nonce = os.urandom(12)
    ciphertext = AESGCM(_key_for_backend(backend_id)).encrypt(
        nonce, credential.encode("utf-8"), _aad(claims)
    )
    return _b64encode(json.dumps({**claims, "nonce": _b64encode(nonce), "ciphertext": _b64encode(ciphertext)}, separators=(",", ":")).encode("utf-8"))


def open_credential_envelope(
    envelope: str,
    *,
    expected_backend_id: str,
    expected_provider: str,
) -> Dict[str, str]:
    try:
        claims = json.loads(_b64decode(envelope).decode("utf-8"))
    except Exception as exc:
        raise CredentialEnvelopeError("malformed credential envelope") from exc
    if not isinstance(claims, dict) or claims.get("v") != 1:
        raise CredentialEnvelopeError("unsupported credential envelope")
    if claims.get("backend_id") != expected_backend_id or claims.get("provider") != expected_provider:
        raise CredentialEnvelopeError("credential envelope audience mismatch")
    if not isinstance(claims.get("tenant_id"), str) or int(claims.get("exp", 0)) < int(time.time()):
        raise CredentialEnvelopeError("credential envelope expired or invalid")
    try:
        credential = AESGCM(_key_for_backend(expected_backend_id)).decrypt(
            _b64decode(str(claims["nonce"])),
            _b64decode(str(claims["ciphertext"])),
            _aad(claims),
        ).decode("utf-8")
    except CredentialEnvelopeError:
        raise
    except Exception as exc:
        raise CredentialEnvelopeError("credential envelope authentication failed") from exc
    if not credential:
        raise CredentialEnvelopeError("credential envelope is empty")
    return {"credential": credential, "tenant_id": claims["tenant_id"]}
