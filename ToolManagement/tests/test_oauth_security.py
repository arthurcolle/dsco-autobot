import base64
import hashlib
import time

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management import oauth
from tool_management.persistence.oauth_store import OAuthStore


REDIRECT_URI = "https://client.example/callback"


class _Users:
    def create_user(self, email, password, display_name=None):
        if email == "new@example.com" and len(password) >= 8:
            return {
                "user_id": "u_new",
                "email": email,
                "display_name": display_name,
            }
        raise ValueError("invalid signup")

    def authenticate(self, email, password):
        if email == "user@example.com" and password == "correct-password":
            return {
                "user_id": "u_test",
                "email": email,
                "display_name": "Test User",
            }
        return None

    def get_by_id(self, user_id):
        if user_id == "u_test":
            return {
                "user_id": "u_test",
                "email": "user@example.com",
                "display_name": "Test User",
            }
        return None

    def get_by_api_key(self, _api_key):
        return None


@pytest.fixture
def oauth_app(tmp_path, monkeypatch):
    store = OAuthStore(str(tmp_path / "oauth.duckdb"))
    users = _Users()
    monkeypatch.setattr(oauth, "_oauth_store", lambda: store)
    monkeypatch.setattr(oauth, "_user_store", lambda: users)
    monkeypatch.setattr(oauth, "SERVER_PASSWORD", "operator-test-password")
    monkeypatch.setattr(oauth, "TOKEN_SECRET", "oauth-test-token-secret")
    monkeypatch.setattr(oauth, "DEFAULT_BEARER_TOKEN", "")
    oauth._auth_codes.clear()

    app = FastAPI()
    app.include_router(oauth.router)
    with TestClient(app) as client:
        yield client, store
    oauth._auth_codes.clear()


def _save_client(
    store: OAuthStore,
    client_id: str,
    *,
    client_secret=None,
    auth_method="none",
    redirect_uri=REDIRECT_URI,
):
    store.save_client(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "name": client_id,
            "redirect_uris": [redirect_uri],
            "created_at": time.time(),
            "token_endpoint_auth_method": auth_method,
        }
    )


def _pkce(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _save_code(client_id: str, *, verifier=None, redirect_uri=REDIRECT_URI):
    code = f"code-{client_id}"
    oauth._auth_codes[code] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": _pkce(verifier) if verifier else "",
        "code_challenge_method": "S256",
        "scope": "mcp",
        "user": None,
        "expires": time.time() + 300,
    }
    return code


def test_dynamic_registration_preserves_secretless_public_pkce_clients(oauth_app):
    client, store = oauth_app
    response = client.post(
        "/oauth/register",
        json={
            "client_name": "Native MCP Client",
            "redirect_uris": [REDIRECT_URI],
            "token_endpoint_auth_method": "none",
        },
    )

    assert response.status_code == 200
    registration = response.json()
    assert registration["token_endpoint_auth_method"] == "none"
    assert "client_secret" not in registration
    stored = store.get_client(registration["client_id"])
    assert stored["client_secret"] is None
    assert stored["token_endpoint_auth_method"] == "none"

    missing_pkce = client.get(
        "/oauth/authorize",
        params={"client_id": registration["client_id"], "redirect_uri": REDIRECT_URI},
    )
    assert missing_pkce.status_code == 400

    with_pkce = client.get(
        "/oauth/authorize",
        params={
            "client_id": registration["client_id"],
            "redirect_uri": REDIRECT_URI,
            "code_challenge": _pkce("public-verifier"),
            "code_challenge_method": "S256",
        },
    )
    assert with_pkce.status_code == 200
    assert 'href="https://testserver/account"' in with_pkce.text
    assert 'href="https://testserver/connect"' not in with_pkce.text
    assert "Operator access" not in with_pkce.text

    signup = client.get(
        "/oauth/authorize",
        params={
            "client_id": registration["client_id"],
            "redirect_uri": REDIRECT_URI,
            "code_challenge": _pkce("public-verifier"),
            "code_challenge_method": "S256",
            "mode": "signup",
        },
    )
    assert signup.status_code == 200
    assert 'setMode("signup")' in signup.text
    assert "Join Distributed & continue" in signup.text


def test_authorize_requires_an_exact_registered_redirect_uri(oauth_app):
    client, store = oauth_app
    _save_client(
        store,
        "confidential-client",
        client_secret="client-secret",
        auth_method="client_secret_post",
    )

    response = client.get(
        "/oauth/authorize",
        params={
            "client_id": "confidential-client",
            "redirect_uri": "https://attacker.example/callback",
        },
    )

    assert response.status_code == 400
    assert "not registered" in response.json()["detail"]

    submitted = client.post(
        "/oauth/authorize",
        data={
            "client_id": "confidential-client",
            "redirect_uri": "https://attacker.example/callback",
            "email": "user@example.com",
            "password": "correct-password",
        },
        follow_redirects=False,
    )
    assert submitted.status_code == 400
    assert "not registered" in submitted.json()["detail"]
    assert not oauth._auth_codes


def test_authorize_can_create_account_inline_and_issue_code(oauth_app):
    client, store = oauth_app
    _save_client(store, "public-signup")
    verifier = "inline-signup-verifier"

    response = client.post(
        "/oauth/authorize",
        data={
            "client_id": "public-signup",
            "redirect_uri": REDIRECT_URI,
            "code_challenge": _pkce(verifier),
            "code_challenge_method": "S256",
            "scope": "mcp",
            "mode": "signup",
            "display_name": "New User",
            "email": "new@example.com",
            "password": "new-password",
            "state": "signup-state",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["location"].startswith(REDIRECT_URI)
    assert "state=signup-state" in response.headers["location"]
    issued = next(iter(oauth._auth_codes.values()))
    assert issued["user"]["user_id"] == "u_new"
    assert store.has_grant("u_new", "public-signup")


def test_authorization_code_is_bound_to_its_client_and_not_burned_on_mismatch(oauth_app):
    client, store = oauth_app
    _save_client(store, "public-a")
    _save_client(store, "public-b", redirect_uri="https://other.example/callback")
    verifier = "correct-public-verifier"
    code = _save_code("public-a", verifier=verifier)

    wrong_client = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "public-b",
            "code": code,
            "code_verifier": verifier,
        },
    )
    assert wrong_client.status_code == 400
    assert wrong_client.json()["error"] == "invalid_grant"

    correct_client = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "public-a",
            "code": code,
            "code_verifier": verifier,
            "redirect_uri": REDIRECT_URI,
        },
    )
    assert correct_client.status_code == 200
    assert correct_client.json()["token_type"] == "bearer"


def test_confidential_client_secret_is_required_for_code_exchange(oauth_app):
    client, store = oauth_app
    _save_client(
        store,
        "confidential-client",
        client_secret="correct-client-secret",
        auth_method="client_secret_post",
    )
    code = _save_code("confidential-client")

    missing_secret = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "confidential-client",
            "code": code,
        },
    )
    assert missing_secret.status_code == 401
    assert missing_secret.json()["error"] == "invalid_client"

    correct_secret = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": "confidential-client",
            "client_secret": "correct-client-secret",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
    )
    assert correct_secret.status_code == 200


def test_refresh_token_is_client_bound_and_bad_attempts_do_not_consume_it(oauth_app):
    client, store = oauth_app
    _save_client(
        store,
        "client-a",
        client_secret="secret-a",
        auth_method="client_secret_post",
    )
    _save_client(
        store,
        "client-b",
        client_secret="secret-b",
        auth_method="client_secret_post",
    )
    store.save_refresh_token("refresh-a", "client-a", None, "mcp", ttl=300)

    wrong_client = client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "client-b",
            "client_secret": "secret-b",
            "refresh_token": "refresh-a",
        },
    )
    assert wrong_client.status_code == 400
    assert wrong_client.json()["error"] == "invalid_grant"

    wrong_secret = client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "client-a",
            "client_secret": "wrong-secret",
            "refresh_token": "refresh-a",
        },
    )
    assert wrong_secret.status_code == 401
    assert wrong_secret.json()["error"] == "invalid_client"

    correct_client = client.post(
        "/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": "client-a",
            "client_secret": "secret-a",
            "refresh_token": "refresh-a",
        },
    )
    assert correct_client.status_code == 200
    assert correct_client.json()["refresh_token"] != "refresh-a"


def test_legacy_client_table_is_migrated_with_confidential_auth(tmp_path):
    db_path = str(tmp_path / "legacy-oauth.duckdb")
    with duckdb.connect(db_path) as con:
        con.execute(
            """
            CREATE TABLE oauth_clients (
                client_id VARCHAR PRIMARY KEY,
                client_secret VARCHAR,
                name VARCHAR,
                redirect_uris VARCHAR,
                logo_uri VARCHAR,
                client_uri VARCHAR,
                created_at DOUBLE
            )
            """
        )
        con.execute(
            "INSERT INTO oauth_clients VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                "legacy-client",
                "legacy-secret",
                "Legacy",
                '["https://legacy.example/callback"]',
                None,
                None,
                time.time(),
            ],
        )

    store = OAuthStore(db_path)
    migrated = store.get_client("legacy-client")
    assert migrated["token_endpoint_auth_method"] == "client_secret_post"
