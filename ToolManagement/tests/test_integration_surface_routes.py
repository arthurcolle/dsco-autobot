from fastapi import FastAPI
from fastapi.testclient import TestClient

from tool_management.api import integration_surface_routes as surfaces
from tool_management.core.streaming import EventType, StreamingExecution, ToolEvent


def _client(monkeypatch, identity=None):
    monkeypatch.setattr(
        surfaces,
        "resolve_execution_identity",
        lambda _request: identity
        or {"kind": "customer", "tenant_id": "tenant-a"},
    )
    app = FastAPI()
    app.include_router(surfaces.router)
    return TestClient(app)


def test_manifest_hands_realtime_and_webhook_delivery_to_jobs(monkeypatch):
    validated = []
    monkeypatch.setattr(
        surfaces,
        "validate_callback_url",
        lambda url: validated.append(url) or url,
    )
    client = _client(monkeypatch)

    response = client.post(
        "/api/v1/integration-surfaces/manifests",
        json={
            "integration_ref": "distributed-email-mcp:search_email",
            "display_name": "Private correspondence",
            "widget": {"density": "compact", "poll_interval_ms": 2500, "max_partial_items": 3},
            "webhook": {
                "callback_url": "https://events.example.com/tool-management",
                "events": ["progress", "complete"],
            },
        },
    )

    assert response.status_code == 200
    manifest = response.json()
    assert manifest["schema"] == surfaces.SURFACE_SCHEMA
    assert manifest["widget"]["projection"]["path_template"].endswith("/{job_id}/widget")
    assert manifest["realtime"] == {
        "owner": "jobs",
        "transport": "server_sent_events",
        "stream_path_template": "/api/v1/jobs/{job_id}/stream",
        "resume": {
            "cursor_query": "since_event_id",
            "cursor_field": "latest_event_id",
            "history_query": "include_history",
        },
    }
    assert manifest["webhook"]["owner"] == "jobs"
    assert manifest["webhook"]["registration"] == "per_job_submission"
    assert manifest["webhook"]["delivery_created"] is False
    assert manifest["execution_handoff"] == {
        "method": "POST",
        "path": "/api/v1/jobs/submit",
        "body": {
            "tool": "distributed-email-mcp:search_email",
            "arguments": {},
            "callback_url": "https://events.example.com/tool-management",
            "callback_events": ["progress", "complete"],
        },
    }
    assert validated == ["https://events.example.com/tool-management"]


def test_manifest_rejects_unsafe_callback_without_creating_delivery(monkeypatch):
    def reject(_url):
        raise ValueError("private target")

    monkeypatch.setattr(surfaces, "validate_callback_url", reject)
    client = _client(monkeypatch)
    response = client.post(
        "/api/v1/integration-surfaces/manifests",
        json={
            "integration_ref": "calendar:create_event",
            "webhook": {
                "callback_url": "https://127.0.0.1/private",
                "events": ["complete"],
            },
        },
    )

    assert response.status_code == 422
    assert response.json()["detail"] == {"error": "unsafe_callback_url"}


def test_manifest_is_strict_and_bounded(monkeypatch):
    client = _client(monkeypatch)
    bad_ref = client.post(
        "/api/v1/integration-surfaces/manifests",
        json={"integration_ref": "calendar tool <script>"},
    )
    extra = client.post(
        "/api/v1/integration-surfaces/manifests",
        json={"integration_ref": "calendar:create_event", "tenant_id": "victim"},
    )
    duplicate_events = client.post(
        "/api/v1/integration-surfaces/manifests",
        json={
            "integration_ref": "calendar:create_event",
            "webhook": {
                "callback_url": "https://events.example.com/hook",
                "events": ["complete", "complete"],
            },
        },
    )

    assert bad_ref.status_code == 422
    assert extra.status_code == 422
    assert duplicate_events.status_code == 422


def test_widget_projection_is_bounded_and_redacts_secrets():
    execution = StreamingExecution(
        execution_id="job_owned",
        tool_name="distributed-email-mcp:search_email",
        tenant_id="tenant-a",
        status="completed",
        progress=100,
        queued_at=10.0,
        completed_at=20.0,
    )
    for index in range(8):
        execution.events.append(
            ToolEvent(
                type=EventType.PARTIAL,
                execution_id=execution.execution_id,
                event_id=index + 1,
                data={"index": index, "api_key": f"secret-{index}"},
            )
        )
    execution.events.append(
        ToolEvent(
            type=EventType.COMPLETED,
            execution_id=execution.execution_id,
            event_id=9,
            data={
                "summary": "ready",
                "authorization": "Bearer do-not-display",
                "nested": {"password": "hidden"},
            },
        )
    )
    execution.control_state.update(
        {
            "callback_url": "https://events.example.com/private-path",
            "callback_events": ["complete", "made_up"],
            "webhook_attempts": 2,
        }
    )

    widget = surfaces.project_execution_widget(execution, max_partial_items=3)

    assert widget["state"] == "completed"
    assert widget["latest_event_id"] == 9
    assert len(widget["partial_items"]) == 3
    assert widget["partial_items"][0]["index"] == 5
    assert widget["partial_items"][0]["api_key"] == "[redacted]"
    assert widget["result"]["authorization"] == "[redacted]"
    assert widget["result"]["nested"]["password"] == "[redacted]"
    assert widget["webhook"] == {
        "configured": True,
        "events": ["complete"],
        "last_event": None,
        "last_success": None,
        "attempts": 2,
    }
    assert "private-path" not in str(widget)
    assert [action["id"] for action in widget["actions"]] == ["refresh", "stream"]


def test_widget_endpoint_enforces_tenant_ownership(monkeypatch):
    own = StreamingExecution(
        execution_id="own",
        tool_name="calendar:create_event",
        tenant_id="tenant-a",
        status="running",
        progress=35,
    )
    foreign = StreamingExecution(
        execution_id="foreign",
        tool_name="calendar:create_event",
        tenant_id="tenant-b",
        status="completed",
        progress=100,
    )

    class Executor:
        def get_execution(self, job_id, tenant_id=None):
            execution = {"own": own, "foreign": foreign}.get(job_id)
            if execution is None or (tenant_id is not None and execution.tenant_id != tenant_id):
                return None
            return execution

    monkeypatch.setattr(
        "tool_management.core.streaming.get_streaming_executor",
        lambda: Executor(),
    )
    client = _client(monkeypatch)

    own_response = client.get(
        "/api/v1/integration-surfaces/jobs/own/widget",
        params={"max_partial_items": 2},
    )
    foreign_response = client.get("/api/v1/integration-surfaces/jobs/foreign/widget")

    assert own_response.status_code == 200
    assert own_response.headers["cache-control"] == "private, no-store"
    assert own_response.headers["vary"] == "Authorization"
    assert own_response.json()["job_id"] == "own"
    assert own_response.json()["state"] == "running"
    assert foreign_response.status_code == 404


def test_operator_widget_lookup_preserves_jobs_control_plane_semantics(monkeypatch):
    execution = StreamingExecution(
        execution_id="operator-job",
        tool_name="workflow:map_reduce",
        tenant_id="some-tenant",
        status="queued",
    )

    class Executor:
        def get_execution(self, job_id, tenant_id=None):
            assert tenant_id is None
            return execution if job_id == execution.execution_id else None

    monkeypatch.setattr(
        "tool_management.core.streaming.get_streaming_executor",
        lambda: Executor(),
    )
    client = _client(
        monkeypatch,
        identity={"kind": "operator_service", "tenant_id": None},
    )

    response = client.get(
        "/api/v1/integration-surfaces/jobs/operator-job/widget"
    )

    assert response.status_code == 200
    assert response.json()["state"] == "queued"
