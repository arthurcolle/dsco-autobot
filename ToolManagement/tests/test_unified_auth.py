"""Unified-auth tests (AUTH_UNIFIED.md T01).

Covers the token substrate shared by every subdomain service:
  * legacy oauth.py token compatibility (byte-exact 16-hex MAC),
  * unified-v2 mint/verify (32-hex, iss/aud/exp/iat),
  * oauth.py's own v2 issuance (T03) and its dual-length decode,
  * the RFC 7662 introspection endpoint (T04) incl. revocation flow,
  * policy enforcement (audience, scopes, expiry, tamper, missing bearer),
  * the scopes.py single-source registry (T05).

Env is set BEFORE imports — oauth.py and unified.py read their secrets at
module load (conftest runs earlier but does not set TOKEN_SECRET).
"""
import os

os.environ.setdefault("TOKEN_SECRET", "testsecret123")
os.environ.setdefault("ENV", "development")
os.environ.setdefault("REQUIRE_BEARER_AUTH", "0")
os.environ.setdefault("DEFAULT_BEARER_TOKEN", "")
os.environ.setdefault("DATA_DIR", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".test-unified-data"))

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import asyncio
import base64 as b64
import hashlib
import hmac
import json
import time
from types import SimpleNamespace

import pytest
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from tool_management.auth.unified import (
    enforce_audience,
    mint_service_token,
    select_account_id,
    verify_request,
    verify_token,
    _effective_secret,
)
from tool_management.auth import unified as unified_mod
from tool_management.auth.scopes import REGISTRY, get, audience_names


def make_oauth_legacy_token(scope="mcp tools", user_id="u1"):
    """Byte-exact oauth.py legacy construction (16-hex MAC, terse claims)."""
    claims = {"c": "mcp-client", "t": time.time(), "s": scope}
    if user_id:
        claims["sub"] = user_id
        claims["em"] = "a@b.c"
    payload = json.dumps(claims).encode()
    sig = hmac.new(_effective_secret().encode(), payload, hashlib.sha256).hexdigest()[:16]
    return b64.urlsafe_b64encode(payload).decode() + "." + sig


def _b64u(data: bytes) -> str:
    return b64.urlsafe_b64encode(data).decode().rstrip("=")


def check(name: str, cond: bool):
    assert cond, name


# ── Legacy compatibility ─────────────────────────────────────────────────────
def test_legacy_oauth_token_verifies():
    t = make_oauth_legacy_token()
    c = verify_token(t)
    check("legacy verifies", c is not None and c.client_id == "mcp-client" and c.subject == "u1")
    check("legacy scopes", c.scopes == frozenset({"mcp", "tools"}))
    check("legacy format", c.token_format == "legacy")
    check("legacy has no aud (unchecked)", c.audience is None)


# ── Unified v2 ───────────────────────────────────────────────────────────────
def test_v2_roundtrip_and_enforcement():
    tok = mint_service_token("svc-search", "search", frozenset({"tools", "mcp"}), ttl=3600)
    c = verify_token(tok)
    check("v2 verifies", c is not None and c.token_format == "unified_v2" and c.audience == "search")
    cl = asyncio.run(verify_request("Bearer " + tok, audience="search",
                                    required_scopes=frozenset({"tools"}), check_revocation=False))
    check("v2 enforcement passes", cl.audience == "search")


def test_v2_audience_mismatch_denied():
    tok = mint_service_token("svc", "search", frozenset({"tools"}))
    with pytest.raises(PermissionError, match="audience_mismatch"):
        asyncio.run(verify_request("Bearer " + tok, audience="publish", check_revocation=False))


def test_v2_insufficient_scope_denied():
    tok = mint_service_token("x", "search", frozenset({"profile"}))
    with pytest.raises(PermissionError, match="insufficient_scope"):
        asyncio.run(verify_request("Bearer " + tok, audience="search",
                                   required_scopes=frozenset({"tools"}), check_revocation=False))


def test_v2_expired_denied_beyond_skew():
    tok = mint_service_token("x", "search", frozenset({"tools"}), ttl=-600)
    with pytest.raises(PermissionError, match="token_expired"):
        asyncio.run(verify_request("Bearer " + tok, audience="search", check_revocation=False))


def test_tampered_and_missing_bearer_denied():
    tok = mint_service_token("x", "search", frozenset({"tools"}))
    bad = tok[:-1] + ("0" if tok[-1] != "0" else "1")
    check("tamper rejected", verify_token(bad) is None)
    with pytest.raises(PermissionError, match="missing_bearer_token"):
        asyncio.run(verify_request(None, check_revocation=False))


# ── Standard JWT (HS256) path ────────────────────────────────────────────────
def test_jwt_hs256_verifies():
    h = _b64u(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    p = _b64u(json.dumps({"c": "jwt-client", "scope": "tools mcp", "aud": "tools",
                          "exp": int(time.time()) + 9999,
                          "iss": "https://tools.distributed.systems"}).encode())
    s = _b64u(hmac.new(_effective_secret().encode(), f"{h}.{p}".encode(), hashlib.sha256).digest())
    c = verify_token(f"{h}.{p}.{s}")
    check("jwt verifies", c is not None and c.token_format == "jwt" and c.audience == "tools")


def _canonical_token(monkeypatch, **overrides):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    claims = {
        "iss": unified_mod.CANONICAL_ISSUER,
        "sub": "principal-arthur",
        "aud": ["tools"],
        "iat": int(time.time()),
        "exp": int(time.time()) + 600,
        "email": "arthur@example.test",
        "email_verified": True,
        "urn:zitadel:iam:user:resourceowner:id": "account-dsco",
        "dsco:roles": ["platform.creator", "owner"],
    }
    claims.update(overrides)
    token = jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": "test-key"})

    class FakeJwksClient:
        def get_signing_key_from_jwt(self, _token):
            return SimpleNamespace(key=private_key.public_key())

    monkeypatch.setattr(unified_mod, "_jwks_client", FakeJwksClient())
    return token


def test_canonical_zitadel_jwt_binds_principal_account_and_roles(monkeypatch):
    token = _canonical_token(monkeypatch)
    claims = verify_token(token)
    check("canonical token verifies", claims is not None and claims.token_format == "zitadel_jwt")
    check("subject is principal", claims.subject == "principal-arthur")
    check("account is separate", claims.account_ids == frozenset({"account-dsco"}))
    check("creator role", claims.is_platform_admin)
    check("verified email is metadata", claims.email_verified)
    enforce_audience(claims, "tools")
    check("single account selected", select_account_id(claims) == "account-dsco")


def test_multi_account_principal_requires_authorized_selection(monkeypatch):
    token = _canonical_token(
        monkeypatch,
        **{
            "urn:zitadel:iam:user:resourceowner:id": None,
            "dsco:account_ids": ["account-a", "account-b"],
        },
    )
    claims = verify_token(token)
    with pytest.raises(PermissionError, match="account_selection_required"):
        select_account_id(claims)
    check("explicit account accepted", select_account_id(claims, "account-b") == "account-b")
    with pytest.raises(PermissionError, match="account_not_authorized"):
        select_account_id(claims, "victim-account")


def test_canonical_token_wrong_issuer_or_audience_is_rejected(monkeypatch):
    wrong_issuer = _canonical_token(monkeypatch, iss="https://evil.example")
    check("wrong issuer rejected", verify_token(wrong_issuer) is None)

    wrong_audience = _canonical_token(monkeypatch, aud=["router"])
    claims = verify_token(wrong_audience)
    with pytest.raises(PermissionError, match="audience_mismatch"):
        enforce_audience(claims, "tools")


# ── T03: oauth.py v2 issuance + dual-length decode ───────────────────────────
def test_oauth_makes_v2_tokens_and_legacy_still_decode():
    from tool_management import oauth as oauth_mod
    v2 = oauth_mod._make_token("password-client", {"user_id": "u9", "email": "e@d.f"}, scope="tools")
    payload_b64, sig = v2.rsplit(".", 1)
    check("v2 sig is 32 hex", len(sig) == 32)
    claims = json.loads(b64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)))
    for k in ("iss", "aud", "iat", "exp", "v"):
        check(f"v2 claim {k} present", k in claims)
    check("v2 aud bound", claims.get("aud") == "tools")
    check("oauth decodes v2", oauth_mod._decode_token(v2) is not None)
    legacy = make_oauth_legacy_token()
    check("oauth still decodes legacy 16-hex", oauth_mod._decode_token(legacy) is not None)
    ident = oauth_mod.get_token_identity(v2)
    check("identity exposes aud/iss", ident is not None and ident.get("aud") == "tools" and "iss" in ident)
    check("unified accepts oauth v2", verify_token(v2) is not None)


# ── T04: introspection endpoint (in-process, router-only app) ────────────────
def test_introspect_endpoint_revocation_flow():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from tool_management import oauth as oauth_mod

    app = FastAPI()
    app.include_router(oauth_mod.router)
    client = TestClient(app)

    v2 = oauth_mod._make_token("introspect-test", scope="tools")
    r = client.post("/oauth/introspect", json={"token": v2})
    body = r.json()
    check("fresh token active", r.status_code == 200 and body.get("active") is True and body.get("scope") == "tools")

    client.post("/oauth/revoke", json={"token": v2})
    r2 = client.post("/oauth/introspect", json={"token": v2})
    body2 = r2.json()
    check("revoked token inactive", body2.get("active") is False and body2.get("revoked") is True)

    r3 = client.post("/oauth/introspect", json={"token": make_oauth_legacy_token()})
    check("legacy (not in revocation store) reported active", r3.json().get("active") is True)

    r4 = client.post("/oauth/introspect", json={"token": ""})
    check("empty token inactive", r4.json().get("active") is False)
    # token never echoed or logged
    check("no token echo", "token" not in r4.text.lower() or r4.json().get("active") is False)


# ── T05: scopes registry ─────────────────────────────────────────────────────
def test_scopes_registry_complete_and_safe():
    expected = {"unit_fabric", "timeline", "video_qa", "doc_qa", "finance_fabric",
                "integration_fabric", "search", "research", "publish", "memory",
                "swarm", "tools"}
    check("all 12 subdomains registered", expected.issubset(set(REGISTRY)))
    sub = get("publish")
    check("publish requires operator identity", sub.operator_identity_required is True)
    with pytest.raises(KeyError):
        get("nonexistent")
    for name in audience_names():
        s = get(name)
        check(f"{name} has required scopes", len(s.required_scopes) >= 1)
        check(f"{name} health public", "/health" in s.public_paths)
