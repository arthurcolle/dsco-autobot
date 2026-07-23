from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

import graph_management.security as security  # noqa: E402


@pytest.fixture(autouse=True)
def clear_auth_env(monkeypatch):
    """Ensure auth-related env vars are reset between tests."""
    monkeypatch.delenv("AUTH_API_KEYS", raising=False)
    monkeypatch.delenv("AUTH_MODE", raising=False)


def test_api_key_from_authorization_header(monkeypatch):
    monkeypatch.setenv("AUTH_API_KEYS", json.dumps({"demo-key": {"sub": "svc-demo", "roles": ["org_admin"]}}))
    headers = {"Authorization": "Bearer demo-key"}

    principal = security.principal_from_headers(headers)

    assert principal.sub == "svc-demo"
    assert principal.roles == ["org_admin"]


def test_api_key_from_x_api_key_header(monkeypatch):
    monkeypatch.setenv("AUTH_API_KEYS", json.dumps({"legacy-secret": {"roles": ["developer"]}}))
    headers = {"X-API-Key": "legacy-secret"}

    principal = security.principal_from_headers(headers)

    assert principal.roles == ["developer"]


def test_missing_api_key_rejected(monkeypatch):
    monkeypatch.setenv("AUTH_API_KEYS", json.dumps({"demo-key": {"roles": ["developer"]}}))

    with pytest.raises(HTTPException) as excinfo:
        security.principal_from_headers({})

    assert excinfo.value.status_code == 401
    assert "Missing API key" in excinfo.value.detail
