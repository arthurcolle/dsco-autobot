"""Contracts for the safe public-source map/reduce demonstration."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any, Mapping

import httpx
import pytest
from fastapi import FastAPI

from tool_management.api import ultimate_demo_routes as demo


def _payloads() -> dict[str, Mapping[str, Any]]:
    return {
        "weather": {
            "current": {
                "time": "2026-08-03T18:00",
                "temperature_2m": 73.4,
                "apparent_temperature": 71.2,
                "precipitation": 0,
                "weather_code": 2,
                "wind_speed_10m": 8.1,
                "wind_gusts_10m": 18.4,
            },
            "hourly": {
                "precipitation_probability": [10] * 23 + [65] + [99] * 200,
                "temperature_2m": list(range(300)),
                "wind_gusts_10m": [12] * 23 + [36] + [200] * 200,
            },
        },
        "air_quality": {
            "current": {
                "time": "2026-08-03T18:00",
                "us_aqi": 42,
                "pm2_5": 7.2,
                "grass_pollen": 3,
                "birch_pollen": 0,
                "alder_pollen": 0,
            }
        },
        "nws_alerts": {
            "features": [
                {
                    "properties": {
                        "event": "Red Flag Warning",
                        "headline": "Dry and windy conditions are expected",
                        "severity": "Severe",
                        "urgency": "Expected",
                        "onset": "2026-08-03T18:00:00Z",
                        "expires": "2026-08-04T03:00:00Z",
                    }
                }
            ]
        },
        "solar_timing": {
            "results": {
                "sunrise": "2026-08-03T12:08:00+00:00",
                "sunset": "2026-08-04T02:17:00+00:00",
                "civil_twilight_begin": "2026-08-03T11:39:00+00:00",
                "civil_twilight_end": "2026-08-04T02:46:00+00:00",
                "day_length": 50_940,
            },
            "status": "OK",
        },
    }


@pytest.mark.asyncio
async def test_map_runs_concurrently_and_reduces_partial_sources_into_drafts():
    payloads = _payloads()
    active = 0
    max_active = 0

    async def fetcher(source: demo.SourceSpec, operational_date: date):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        if source.id == "nws_alerts":
            raise demo.SourceFetchError("source_timeout")
        return payloads[source.id]

    service = demo.UltimateDemoService(fetcher=fetcher)
    run = await service.start(
        demo.DemoRunRequest(
            operational_date=date(2026, 8, 3),
            min_successful_sources=2,
        )
    )
    run = await service.wait(run.run_id)

    assert max_active == 4
    assert run.status == "completed"
    assert run.artifact is not None
    assert run.artifact["source_coverage"] == {
        "successful": 3,
        "total": 4,
        "degraded": True,
        "missing_source_ids": ["nws_alerts"],
    }
    assert run.artifact["conditions"]["weather"]["next_24h"]["samples_used"] == 24
    assert run.artifact["conditions"]["weather"]["next_24h"][
        "max_precipitation_probability_pct"
    ] == 65
    assert run.drafts is not None
    assert set(run.drafts) == {"calendar", "slack", "substack"}
    for draft in run.drafts.values():
        assert draft["status"] == "draft"
        assert draft["approval_required"] is True
        assert draft["executed"] is False
        assert draft["destination"] is None
    assert "ATTENDEE" not in run.drafts["calendar"]["ics"]
    assert "STATUS:TENTATIVE" in run.drafts["calendar"]["ics"]
    assert run.drafts["slack"]["channel"] is None
    assert run.drafts["substack"]["publish"] is False
    assert len(run.artifact_markdown or "") <= demo.MAX_MARKDOWN_CHARS


@pytest.mark.asyncio
async def test_location_and_workflow_bind_safe_source_params_and_outputs():
    payloads = _payloads()
    observed_params: dict[str, Mapping[str, Any]] = {}

    async def fetcher(source: demo.SourceSpec, operational_date: date):
        observed_params[source.id] = source.params
        return payloads[source.id]

    service = demo.UltimateDemoService(fetcher=fetcher)
    run = await service.start(
        demo.DemoRunRequest(
            operational_date=date(2026, 8, 3),
            workflow_id="training_window",
            location={
                "name": "Brooklyn, New York",
                "latitude": 40.678,
                "longitude": -73.944,
                "timezone": "America/New_York",
            },
        )
    )
    run = await service.wait(run.run_id)

    assert run.status == "completed"
    assert observed_params["weather"]["latitude"] == 40.678
    assert observed_params["weather"]["longitude"] == -73.944
    assert observed_params["weather"]["timezone"] == "America/New_York"
    assert observed_params["nws_alerts"]["point"] == "40.6780,-73.9440"
    assert observed_params["solar_timing"]["lat"] == 40.678
    assert run.artifact is not None
    assert run.artifact["workflow_id"] == "training_window"
    assert run.artifact["title"] == "Brooklyn training brief"
    assert run.artifact["location"] == {
        "name": "Brooklyn, New York",
        "latitude": 40.678,
        "longitude": -73.944,
        "timezone": "America/New_York",
    }
    assert any("outdoor training window" in item for item in run.artifact["recommendations"])
    assert run.drafts is not None
    assert "LOCATION:Brooklyn\\, New York" in run.drafts["calendar"]["ics"]


@pytest.mark.asyncio
async def test_run_fails_closed_when_source_floor_is_not_met():
    payloads = _payloads()

    async def fetcher(source: demo.SourceSpec, operational_date: date):
        if source.id != "weather":
            raise demo.SourceFetchError("source_unavailable")
        return payloads[source.id]

    service = demo.UltimateDemoService(fetcher=fetcher)
    run = await service.start(
        demo.DemoRunRequest(
            operational_date=date(2026, 8, 3),
            min_successful_sources=2,
        )
    )
    run = await service.wait(run.run_id)

    assert run.status == "failed"
    assert run.error == "insufficient_source_coverage:1/2"
    assert run.artifact is None
    assert run.drafts is None
    assert run.events[-1]["type"] == "run.failed"


@pytest.mark.asyncio
async def test_http_contract_exposes_status_sse_widget_and_artifact_formats(monkeypatch):
    payloads = _payloads()

    async def fetcher(source: demo.SourceSpec, operational_date: date):
        await asyncio.sleep(0)
        return payloads[source.id]

    service = demo.UltimateDemoService(fetcher=fetcher)
    monkeypatch.setattr(demo, "_service", service)
    app = FastAPI()
    app.include_router(demo.router)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
        definition = await client.get("/api/v1/demo/ultimate/definition")
        assert definition.status_code == 200
        assert len(definition.json()["sources"]) == 4
        assert len(definition.json()["workflows"]) == 6
        assert definition.json()["default_location"]["name"] == "Aspen, Colorado"
        assert definition.json()["side_effect_policy"]["external_actions_executed"] is False

        started = await client.post(
            "/api/v1/demo/ultimate/runs",
            json={"operational_date": "2026-08-03"},
        )
        assert started.status_code == 202
        run_id = started.json()["run_id"]
        await service.wait(run_id)

        status = await client.get(f"/api/v1/demo/ultimate/runs/{run_id}")
        assert status.status_code == 200
        body = status.json()
        assert body["status"] == "completed"
        assert body["external_actions_executed"] is False
        artifact_id = body["artifact_id"]
        assert body["links"]["artifact_json"].endswith(artifact_id)

        events = await client.get(f"/api/v1/demo/ultimate/runs/{run_id}/events")
        assert events.status_code == 200
        assert "text/event-stream" in events.headers["content-type"]
        assert "event: run.completed" in events.text
        assert "event: artifact.created" in events.text

        widget = await client.get(f"/api/v1/demo/ultimate/runs/{run_id}/widget")
        assert widget.status_code == 200
        widget_body = widget.json()
        assert widget_body["kind"] == "distributed.realtime.operational_brief"
        assert widget_body["realtime"]["transport"] == "sse"
        assert len(widget_body["draft_actions"]) == 3

        artifact = await client.get(f"/api/v1/demo/ultimate/artifacts/{artifact_id}")
        assert artifact.status_code == 200
        assert artifact.json()["artifact_id"] == artifact_id

        markdown = await client.get(
            f"/api/v1/demo/ultimate/artifacts/{artifact_id}?format=markdown"
        )
        assert markdown.status_code == 200
        assert "text/markdown" in markdown.headers["content-type"]
        assert markdown.text.startswith("# Aspen operating brief")

        drafts = await client.get(f"/api/v1/demo/ultimate/runs/{run_id}/drafts")
        assert drafts.status_code == 200
        assert drafts.json()["approval_required"] is True
        assert drafts.json()["external_actions_executed"] is False


@pytest.mark.asyncio
async def test_http_request_cannot_inject_a_source_or_publish_target(monkeypatch):
    async def should_not_fetch(source: demo.SourceSpec, operational_date: date):
        raise AssertionError("invalid request must not start work")

    monkeypatch.setattr(demo, "_service", demo.UltimateDemoService(fetcher=should_not_fetch))
    app = FastAPI()
    app.include_router(demo.router)
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="https://test") as client:
        response = await client.post(
            "/api/v1/demo/ultimate/runs",
            json={
                "source_url": "http://127.0.0.1/private",
                "slack_channel": "#general",
                "publish": True,
            },
        )

    assert response.status_code == 422
