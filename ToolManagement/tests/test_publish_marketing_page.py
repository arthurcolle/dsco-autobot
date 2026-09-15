"""Public-page contracts for the Distributed Publish launch surface."""

from fastapi.testclient import TestClient

from tool_management.web.publish import (
    publish_page_headers,
    render_publish_authorization,
    render_publish_marketing,
)


def test_marketing_page_is_focused_on_accountable_human_authorization():
    page = render_publish_marketing("page-nonce")

    assert "Your voice is not" in page
    assert "Assistance without abdication" in page
    assert "Nothing is posted or emailed without an explicit approval signal" in page
    assert "Create an authorization request" in page
    assert "/api/v1/publish/authorizations" in page
    assert "sessionStorage.getItem('dsco_user_key')" in page
    assert "does not scrape it from a browser" in page
    assert "The SID never appears in connection status or webhook payloads" in page
    assert "localStorage" not in page
    assert "navigator.clipboard" not in page
    assert 'nonce="page-nonce"' in page


def test_authorization_page_consumes_fragment_then_removes_it_from_history():
    request_id = f"pubauth_{'a' * 32}"
    page = render_publish_authorization(request_id, "authorization-nonce")

    assert request_id in page
    assert "location.hash.slice(1)" in page
    assert "history.replaceState(null,'',location.pathname)" in page
    assert "substack.sid" in page
    assert "authorize_connection:true" in page
    assert "publishing still requires a separate explicit confirmation" in page.lower()
    assert "clipboard" not in page.lower()
    assert 'type="password"' in page
    assert 'autocomplete="off"' in page
    assert 'nonce="authorization-nonce"' in page


def test_page_security_headers_are_fail_closed():
    headers = publish_page_headers("secure-nonce")

    assert headers["Cache-Control"] == "no-store, private"
    assert headers["Referrer-Policy"] == "no-referrer"
    assert headers["X-Frame-Options"] == "DENY"
    assert "default-src 'none'" in headers["Content-Security-Policy"]
    assert "script-src 'nonce-secure-nonce'" in headers["Content-Security-Policy"]
    assert "connect-src 'self'" in headers["Content-Security-Policy"]


def test_global_gate_exposes_only_the_page_and_one_time_completion_shape(monkeypatch):
    from tool_management.app import create_app

    monkeypatch.setenv("REQUIRE_BEARER_AUTH", "1")
    monkeypatch.setenv("ENABLE_DOCS", "0")
    client = TestClient(create_app())
    request_id = f"pubauth_{'a' * 32}"

    assert client.get("/publish").status_code == 200
    assert client.get(f"/publish/authorize/{request_id}").status_code == 200
    assert client.get("/publish/authorize/not-a-request").status_code == 401
    assert client.post("/api/v1/publish/authorizations", json={}).status_code == 401
    # The gate allows the one-time shape through; the route itself rejects the
    # missing request/token instead of exposing tenant-owned metadata.
    complete = client.post(
        f"/api/v1/publish/authorizations/{request_id}/complete",
        json={
            "authorization_token": "x" * 43,
            "sid": "placeholder-session-value",
            "authorize_connection": True,
        },
    )
    assert complete.status_code == 403
