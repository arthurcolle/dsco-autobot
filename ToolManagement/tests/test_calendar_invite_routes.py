from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import calendar_invite_routes as calendar


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("CALENDAR_INVITE_DB_PATH", str(tmp_path / "calendar-invites.db"))
    monkeypatch.setenv("CALENDAR_INVITE_SIGNING_SECRET", "test-calendar-signing-secret")

    def identity(request):
        tenant = request.headers.get("x-test-tenant", "tenant-a")
        if tenant == "operator":
            return {"kind": "operator_service", "tenant_id": None, "identity": {"client_id": "operator"}}
        return {"kind": "customer", "tenant_id": tenant, "user": {"user_id": tenant}}

    monkeypatch.setattr(calendar, "resolve_execution_identity", identity)
    app = FastAPI()
    app.include_router(calendar.router)
    return TestClient(app)


def _payload(**overrides):
    payload = {
        "summary": "Board prep, alpine edition",
        "description": "Decisions, risks; and owners",
        "location": "Library, 2nd floor",
        "starts_at": "2026-08-05T09:00:00-04:00",
        "ends_at": "2026-08-05T10:00:00-04:00",
        "timezone": "America/New_York",
        "organizer": {"email": "host@example.com", "name": "Host Person"},
        "attendees": [
            {
                "email": "guest@example.com",
                "name": "Guest Person",
                "role": "REQ-PARTICIPANT",
                "rsvp_required": True,
            }
        ],
    }
    payload.update(overrides)
    return payload


def _create(client, *, key="create-invite-0001", tenant="tenant-a", **overrides):
    return client.post(
        "/api/v1/calendar/invites",
        json=_payload(**overrides),
        headers={"Idempotency-Key": key, "x-test-tenant": tenant},
    )


def _download(client, url):
    parsed = urlsplit(url)
    return client.get(f"{parsed.path}?{parsed.query}")


def _reply(uid, sequence=0, email="guest@example.com", status="ACCEPTED"):
    return (
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "METHOD:REPLY\r\n"
        "BEGIN:VEVENT\r\n"
        f"UID:{uid}\r\n"
        f"SEQUENCE:{sequence}\r\n"
        f"ATTENDEE;PARTSTAT={status}:mailto:{email}\r\n"
        "END:VEVENT\r\n"
        "END:VCALENDAR\r\n"
    )


def test_create_returns_signed_native_icalendar_artifact(client):
    response = _create(client)

    assert response.status_code == 201
    body = response.json()
    assert body["created"] is True
    assert body["invite"]["sequence"] == 0
    assert body["calendar"]["method"] == "REQUEST"

    download = _download(client, body["calendar"]["download_url"])
    assert download.status_code == 200
    assert download.headers["content-type"].startswith("text/calendar")
    assert download.content.endswith(b"\r\n")
    text = download.text
    assert "METHOD:REQUEST\r\n" in text
    assert "SEQUENCE:0\r\n" in text
    assert "DTSTART:20260805T130000Z\r\n" in text
    assert "DESCRIPTION:Decisions\\, risks\\; and owners\r\n" in text
    assert "ATTENDEE;" in text
    assert "PARTSTAT=NEEDS-ACTION" in text
    assert ":mailto:guest@example.com\r\n" in text


def test_create_idempotency_replays_and_rejects_key_reuse(client):
    first = _create(client)
    replay = _create(client)
    conflict = _create(client, summary="A different event")

    assert replay.status_code == 201
    assert replay.json()["created"] is False
    assert replay.json()["replayed"] is True
    assert replay.json()["invite"]["invite_id"] == first.json()["invite"]["invite_id"]
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "idempotency_key_reused"


def test_update_increments_sequence_and_invalidates_old_download(client):
    created = _create(client).json()
    invite_id = created["invite"]["invite_id"]
    old_url = created["calendar"]["download_url"]

    updated = client.patch(
        f"/api/v1/calendar/invites/{invite_id}",
        json={"expected_sequence": 0, "location": "North library"},
        headers={"Idempotency-Key": "update-invite-0001", "x-test-tenant": "tenant-a"},
    )
    stale = client.patch(
        f"/api/v1/calendar/invites/{invite_id}",
        json={"expected_sequence": 0, "location": "South library"},
        headers={"Idempotency-Key": "update-invite-0002", "x-test-tenant": "tenant-a"},
    )

    assert updated.status_code == 200
    assert updated.json()["invite"]["sequence"] == 1
    assert updated.json()["invite"]["location"] == "North library"
    assert stale.status_code == 409
    assert stale.json()["detail"] == {"error": "stale_sequence", "current_sequence": 1}
    assert _download(client, old_url).status_code == 409
    revised = _download(client, updated.json()["calendar"]["download_url"])
    assert "SEQUENCE:1\r\n" in revised.text


def test_customer_ownership_isolated_while_operator_can_inspect(client):
    created = _create(client).json()
    invite_id = created["invite"]["invite_id"]

    denied = client.get(
        f"/api/v1/calendar/invites/{invite_id}", headers={"x-test-tenant": "tenant-b"}
    )
    operator = client.get(
        f"/api/v1/calendar/invites/{invite_id}", headers={"x-test-tenant": "operator"}
    )

    assert denied.status_code == 404
    assert operator.status_code == 200
    assert operator.json()["invite"]["invite_id"] == invite_id
    assert "owner_id" not in operator.json()["invite"]


def test_cancel_and_email_dispatch_are_approval_gated_plan_only(client):
    created = _create(client).json()
    invite_id = created["invite"]["invite_id"]
    cancelled = client.post(
        f"/api/v1/calendar/invites/{invite_id}/cancel",
        json={"expected_sequence": 0},
        headers={"Idempotency-Key": "cancel-invite-0001", "x-test-tenant": "tenant-a"},
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["invite"]["sequence"] == 1
    assert cancelled.json()["calendar"]["method"] == "CANCEL"
    assert "METHOD:CANCEL\r\n" in _download(client, cancelled.json()["calendar"]["download_url"]).text

    blocked = client.post(
        f"/api/v1/calendar/invites/{invite_id}/dispatch-plan",
        json={"expected_sequence": 1, "channel": "email", "approved": False},
        headers={"x-test-tenant": "tenant-a"},
    )
    plan = client.post(
        f"/api/v1/calendar/invites/{invite_id}/dispatch-plan",
        json={
            "expected_sequence": 1,
            "channel": "email",
            "approved": True,
            "approval_reference": "approval-ui-42",
        },
        headers={"x-test-tenant": "tenant-a"},
    )

    assert blocked.status_code == 409
    assert blocked.json()["detail"]["error"] == "dispatch_approval_required"
    assert plan.status_code == 200
    assert plan.json()["mode"] == "plan_only"
    assert plan.json()["executed"] is False
    step = plan.json()["steps"][0]
    assert step["tool"] == "distributed-email-mcp:send_with_attachment"
    assert step["arguments"]["to"] == "guest@example.com"
    assert step["arguments"]["source"].endswith(step["arguments"]["source"].split("?")[-1])
    assert step["arguments"]["idempotency_key"].startswith("calendar:")


def test_calendar_provider_handoff_uses_native_attendees_and_send_updates(client):
    created = _create(client).json()
    invite_id = created["invite"]["invite_id"]
    plan = client.post(
        f"/api/v1/calendar/invites/{invite_id}/dispatch-plan",
        json={
            "expected_sequence": 0,
            "channel": "google_calendar",
            "approved": True,
            "approval_reference": "approval-ui-43",
        },
        headers={"x-test-tenant": "tenant-a"},
    )

    assert plan.status_code == 200
    step = plan.json()["steps"][0]
    assert step["tool"] == "google_calendar:create_event"
    assert step["arguments"]["attendees"] == [{"email": "guest@example.com"}]
    assert step["arguments"]["send_updates"] == "all"
    assert step["arguments"]["i_cal_uid"] == created["invite"]["uid"]


def test_text_calendar_reply_updates_rsvp_and_replay_is_idempotent(client):
    created = _create(client).json()
    uid = created["invite"]["uid"]
    reply = _reply(uid)

    accepted = client.post(
        "/api/v1/calendar/replies",
        content=reply,
        headers={"Content-Type": "text/calendar", "x-test-tenant": "tenant-a"},
    )
    duplicate = client.post(
        "/api/v1/calendar/replies",
        content=reply,
        headers={"Content-Type": "text/calendar", "x-test-tenant": "tenant-a"},
    )

    assert accepted.status_code == 200
    assert accepted.json()["applied"] is True
    assert accepted.json()["reply"]["participation_status"] == "ACCEPTED"
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert duplicate.json()["applied"] is False


def test_reply_rejects_stale_sequence_and_unknown_attendee(client):
    created = _create(client).json()
    invite_id = created["invite"]["invite_id"]
    uid = created["invite"]["uid"]
    updated = client.patch(
        f"/api/v1/calendar/invites/{invite_id}",
        json={"expected_sequence": 0, "summary": "Revised board prep"},
        headers={"Idempotency-Key": "update-invite-0003", "x-test-tenant": "tenant-a"},
    )
    assert updated.status_code == 200

    stale = client.post(
        "/api/v1/calendar/replies",
        content=_reply(uid, sequence=0),
        headers={"Content-Type": "text/calendar", "x-test-tenant": "tenant-a"},
    )
    stranger = client.post(
        "/api/v1/calendar/replies",
        content=_reply(uid, sequence=1, email="stranger@example.com"),
        headers={"Content-Type": "text/calendar", "x-test-tenant": "tenant-a"},
    )

    assert stale.status_code == 409
    assert stale.json()["detail"]["error"] == "stale_reply_sequence"
    assert stranger.status_code == 422
    assert stranger.json()["detail"]["error"] == "reply_attendee_not_invited"


def test_global_auth_gate_only_bypasses_the_signed_download_shape(monkeypatch):
    from tool_management.app import create_app

    monkeypatch.setenv("REQUIRE_BEARER_AUTH", "1")
    monkeypatch.setenv("ENABLE_DOCS", "0")
    gated = TestClient(create_app())
    invite_id = f"cal_{'a' * 32}"

    signed_shape = gated.get(
        f"/api/v1/calendar/invites/{invite_id}/calendar.ics?token=invalid"
    )
    missing_token = gated.get(
        f"/api/v1/calendar/invites/{invite_id}/calendar.ics"
    )
    invalid_id = gated.get(
        "/api/v1/calendar/invites/not-an-id/calendar.ics?token=invalid"
    )
    metadata = gated.get(f"/api/v1/calendar/invites/{invite_id}")
    replies = gated.post("/api/v1/calendar/replies", content="")
    public_demo = gated.get("/api/v1/demo/ultimate/definition")
    demo_lookalike = gated.get("/api/v1/demo/ultimately-private")

    assert signed_shape.status_code == 403  # Route ran and rejected the bad HMAC.
    assert missing_token.status_code == 401
    assert invalid_id.status_code == 401
    assert metadata.status_code == 401
    assert replies.status_code == 401
    assert public_demo.status_code == 200
    assert demo_lookalike.status_code == 401
