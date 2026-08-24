"""Focused contracts for the public product and tenant-aware execution path."""

import re

import pytest
from pydantic import ValidationError
from starlette.requests import Request

from tool_management.api import tools_api
from tool_management.api.tools_api import ToolCall
from tool_management.web.landing import render_landing


def _request(token: str = "sk-user-test") -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "https",
            "path": "/api/v1/tools/execute",
            "raw_path": b"/api/v1/tools/execute",
            "query_string": b"",
            "headers": [(b"authorization", f"Bearer {token}".encode())],
            "client": ("127.0.0.1", 1234),
            "server": ("tools.distributed.systems", 443),
        }
    )


def test_landing_contains_complete_customer_journey():
    page = render_landing(tool_count=2524, backend_count=10, healthy_backends=10)
    assert "Business &amp; pleasure." in page
    assert '<span class="marker-phrase">Less logistics.<svg class="marker-stroke"' in page
    assert 'viewBox="0 0 1000 72"' in page
    assert 'aria-hidden="true" focusable="false"' in page
    assert "private AI butler" in page
    assert "Adjustable autonomy" in page
    assert "On call" in page
    assert "Attentive" in page
    assert "Proactive" in page
    assert "Steward" in page
    assert "new hire" not in page.lower()
    assert "/api/v1/users/" in page
    assert "/api/v1/me/billing/checkout-session" in page
    assert "/api/v1/me/billing/portal-session" in page
    assert "/api/v1/tools/execute" in page
    assert "distributed-email-mcp:search_email" in page
    assert "Tool Management, running itself" in page
    assert "Build my brief" in page
    assert "This page is the product." in page
    assert "/api/v1/demo/ultimate/runs" in page
    assert "Slack draft" in page
    assert "Substack draft" in page
    assert "The whole of you stays local. Only the job leaves." in page
    assert "Sees the brief—not the person." in page
    assert "Full context never follows it out" in page
    assert "0 actions without approval" in page
    assert "Nothing has been booked, posted, or published." in page
    assert "renderDemoArtifact(artifact)" in page
    assert "openDemoDraft('calendar')" in page
    for workflow in (
        "day_in_hand",
        "training_window",
        "arrival_brief",
        "home_watch",
        "weekend_host",
        "field_note",
    ):
        assert f'data-workflow="{workflow}"' in page
    assert "Operating near Aspen, Colorado" in page
    assert "navigator.geolocation.getCurrentPosition" in page
    assert "Math.round(position.coords.latitude*1000)/1000" in page
    assert "JSON.stringify(payload)" in page
    assert "Notification.requestPermission()" in page
    assert "getUserMedia({audio:true,video:true})" in page
    assert "stream.getTracks().forEach(function(track){track.stop();})" in page
    assert "navigator.storage.persist()" in page
    assert "navigator.clipboard.readText" not in page
    assert "Device permissions are browser-origin scoped." in page
    assert "Platform grants are portable." in page
    assert '<a href="/health">Network health</a>' in page
    assert 'href="/mcp/health"' not in page
    assert "2,524" not in page  # Formatting is performed from live JSON in-browser.
    assert '"tool_count": 2524' in page


def test_landing_skins_are_allowlisted_and_separate_from_customer_identity():
    page = render_landing(tool_count=1, backend_count=1, healthy_backends=1)

    assert '<html lang="en" data-skin="alpine">' in page
    assert "distributed_life_skin_v1" in page
    assert "Alpine Signal" in page
    assert "Carbon Ice" in page
    assert "Gallery" in page
    assert "Graffiti Atelier" in page
    assert "const allowed=['alpine','carbon','gallery','graffiti']" in page
    for skin in ("alpine", "carbon", "gallery", "graffiti"):
        assert f'html[data-skin="{skin}"]' in page
        assert f'data-skin-choice="{skin}"' in page
        assert f'data-mark="{skin}"' in page
    assert len(re.findall(r'data-skin-choice="[^"]+" aria-selected="true"', page)) == 1
    assert "graffiti:{label:'Graffiti Atelier'}" in page
    for token in (
        "--p-canvas:",
        "--p-surface-1:",
        "--p-inverse:",
        "--p-text-1:",
        "--p-line-1:",
        "--p-accent:",
        "--p-highlight:",
        "--p-focus:",
        "--p-panel-1:",
        "--font-display:",
        "--shadow-card:",
        "--marker-primary:",
    ):
        graffiti_block = page.split('html[data-skin="graffiti"] {', 1)[1].split("\n    }", 1)[0]
        assert token in graffiti_block
    assert "localStorage.setItem('distributed_life_skin_v1',id)" in page
    assert "sessionStorage.setItem('dsco_user_key',data.api_key)" in page
    assert "localStorage.setItem('dsco_user_key'" not in page


def test_landing_and_account_share_the_same_session_identity_key():
    page = render_landing(tool_count=1, backend_count=1, healthy_backends=1)
    assert "sessionStorage.getItem('dsco_user_key')" in page
    assert "sessionStorage.setItem('dsco_user_key',data.api_key)" in page
    assert "if(data.checkout_url)" in page


def test_tenant_id_is_not_accepted_from_customer_json():
    with pytest.raises(ValidationError):
        ToolCall.model_validate(
            {"tool": "search", "arguments": {}, "tenant_id": "victim-user"}
        )


def test_customer_identity_is_bound_to_every_call(monkeypatch):
    from tool_management.api import user_routes

    monkeypatch.setattr(
        tools_api,
        "resolve_execution_identity",
        lambda request, tool_names=None: {"kind": "customer", "tenant_id": "real-user"},
    )
    calls = [ToolCall(tool="one"), ToolCall(tool="two")]

    user = tools_api._bind_customer_identity(_request(), calls)

    assert user["tenant_id"] == "real-user"
    assert [call.tenant_id for call in calls] == ["real-user", "real-user"]


def test_email_backend_headers_are_server_side_and_tenant_scoped(monkeypatch):
    monkeypatch.setenv("TOOLMANAGEMENT_EMAIL_BEARER_TOKEN", "service-secret")

    headers = tools_api._backend_headers("distributed-email-mcp", "owner-user")

    assert headers == {
        "Authorization": "Bearer service-secret",
        "X-DSCO-Tenant-ID": "owner-user",
    }
