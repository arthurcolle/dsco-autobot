"""Safe, public-source map/reduce demo for the Distributed Life butler.

The workflow is deliberately narrow and inspectable:

1. Map four allowlisted, read-only public data sources in parallel.
2. Reduce their bounded projections into a location-aware operational brief.
3. Derive a calendar plan plus Slack and Substack *draft* payloads.

Nothing in this module publishes, sends, or mutates an external system.  Every
downstream payload is marked ``approval_required`` and omits a destination.
That makes the same router safe to mount in the production Tool Management app
or in the isolated Modal product preview.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Literal, Mapping, Optional, Sequence
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator


router = APIRouter(prefix="/api/v1/demo/ultimate", tags=["ultimate-demo"])

SCHEMA_VERSION = "2026-08-03"
ASPEN_LATITUDE = 39.1911
ASPEN_LONGITUDE = -106.8175
ASPEN_TIMEZONE = "America/Denver"
MAX_SOURCE_BYTES = 512_000
MAX_MARKDOWN_CHARS = 24_000
MAX_RUNS = 128
RUN_TTL_SECONDS = 60 * 60
SOURCE_TIMEOUT_SECONDS = 8.0
TERMINAL_STATUSES = frozenset({"completed", "failed"})


WorkflowId = Literal[
    "day_in_hand",
    "training_window",
    "arrival_brief",
    "home_watch",
    "weekend_host",
    "field_note",
]


@dataclass(frozen=True)
class WorkflowSpec:
    id: str
    label: str
    description: str
    default_title: str


WORKFLOWS: Mapping[str, WorkflowSpec] = {
    "day_in_hand": WorkflowSpec(
        id="day_in_hand",
        label="Day in hand",
        description="Shape the day around weather, air, alerts, and daylight.",
        default_title="operating brief",
    ),
    "training_window": WorkflowSpec(
        id="training_window",
        label="Training window",
        description="Protect the best outdoor block and prepare an indoor fallback.",
        default_title="training brief",
    ),
    "arrival_brief": WorkflowSpec(
        id="arrival_brief",
        label="Arrival intelligence",
        description="Prepare arrival timing, transfers, and weather-sensitive handoffs.",
        default_title="arrival brief",
    ),
    "home_watch": WorkflowSpec(
        id="home_watch",
        label="Home watch",
        description="Turn local conditions into calm, practical house checks.",
        default_title="home watch brief",
    ),
    "weekend_host": WorkflowSpec(
        id="weekend_host",
        label="Weekend host",
        description="Coordinate guests, outdoor plans, and the useful daylight window.",
        default_title="weekend host brief",
    ),
    "field_note": WorkflowSpec(
        id="field_note",
        label="Local field note",
        description="Create a sourced local artifact ready for Slack or Substack review.",
        default_title="local field note",
    ),
}


@dataclass(frozen=True)
class SourceSpec:
    id: str
    label: str
    provider: str
    url: str
    params: Mapping[str, Any]


SOURCES: Sequence[SourceSpec] = (
    SourceSpec(
        id="weather",
        label="Weather",
        provider="Open-Meteo Forecast",
        url="https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": ASPEN_LATITUDE,
            "longitude": ASPEN_LONGITUDE,
            "current": (
                "temperature_2m,apparent_temperature,precipitation,weather_code,"
                "wind_speed_10m,wind_gusts_10m"
            ),
            "hourly": "precipitation_probability,temperature_2m,wind_gusts_10m",
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "precipitation_unit": "inch",
            "timezone": ASPEN_TIMEZONE,
            "forecast_days": 2,
        },
    ),
    SourceSpec(
        id="air_quality",
        label="Air quality",
        provider="Open-Meteo Air Quality",
        url="https://air-quality-api.open-meteo.com/v1/air-quality",
        params={
            "latitude": ASPEN_LATITUDE,
            "longitude": ASPEN_LONGITUDE,
            "current": "us_aqi,pm2_5,grass_pollen,birch_pollen,alder_pollen",
            "timezone": ASPEN_TIMEZONE,
        },
    ),
    SourceSpec(
        id="nws_alerts",
        label="Public alerts",
        provider="U.S. National Weather Service",
        url="https://api.weather.gov/alerts/active",
        params={"point": f"{ASPEN_LATITUDE},{ASPEN_LONGITUDE}"},
    ),
    SourceSpec(
        id="solar_timing",
        label="Solar timing",
        provider="Sunrise-Sunset.org",
        url="https://api.sunrise-sunset.org/json",
        params={
            "lat": ASPEN_LATITUDE,
            "lng": ASPEN_LONGITUDE,
            "formatted": 0,
        },
    ),
)

_SOURCE_BY_ID = {source.id: source for source in SOURCES}
_ALLOWED_TARGETS = frozenset(
    (urlparse(source.url).hostname, urlparse(source.url).path) for source in SOURCES
)


class DemoLocation(BaseModel):
    """Caller-selected coordinates; network hosts remain fixed and allowlisted."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(default="Aspen, Colorado", min_length=2, max_length=120)
    latitude: float = Field(default=ASPEN_LATITUDE, ge=-90, le=90)
    longitude: float = Field(default=ASPEN_LONGITUDE, ge=-180, le=180)
    timezone: str = Field(default=ASPEN_TIMEZONE, min_length=1, max_length=80)

    @field_validator("name")
    @classmethod
    def _normalize_name(cls, value: str) -> str:
        normalized = _clean_text(value, 120)
        if len(normalized) < 2:
            raise ValueError("location name must contain at least 2 visible characters")
        return normalized

    @field_validator("timezone")
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        normalized = _clean_text(value, 80)
        try:
            ZoneInfo(normalized)
        except (KeyError, ValueError) as exc:
            raise ValueError("timezone must be a valid IANA time zone") from exc
        return normalized


class DemoRunRequest(BaseModel):
    """Small, fixed-input request; callers cannot supply network targets."""

    model_config = ConfigDict(extra="forbid")

    operational_date: Optional[date] = Field(
        default=None,
        description="Local date; defaults to today in the selected time zone.",
    )
    workflow_id: WorkflowId = "day_in_hand"
    location: DemoLocation = Field(default_factory=DemoLocation)
    brief_name: Optional[str] = Field(default=None, min_length=3, max_length=120)
    min_successful_sources: int = Field(default=2, ge=1, le=len(SOURCES))

    @field_validator("brief_name")
    @classmethod
    def _normalize_brief_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = _clean_text(value, 120)
        if len(normalized) < 3:
            raise ValueError("brief_name must contain at least 3 visible characters")
        return normalized


class SourceFetchError(RuntimeError):
    """A sanitized, stable source failure safe to expose in demo state."""


@dataclass
class DemoRun:
    run_id: str
    request: DemoRunRequest
    workflow: WorkflowSpec
    location: DemoLocation
    sources: Sequence[SourceSpec]
    brief_name: str
    operational_date: date
    created_at: datetime
    status: str = "queued"
    stage: str = "queued"
    progress: int = 0
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    source_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    artifact: Optional[Dict[str, Any]] = None
    artifact_markdown: Optional[str] = None
    drafts: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    events: List[Dict[str, Any]] = field(default_factory=list)
    task: Optional[asyncio.Task[Any]] = field(default=None, repr=False)


Fetcher = Callable[[SourceSpec, date], Awaitable[Mapping[str, Any]]]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_text(value: Any, limit: int = 240) -> str:
    if value is None:
        return ""
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text[:limit]


def _sources_for_location(location: DemoLocation) -> Sequence[SourceSpec]:
    """Bind safe query parameters to the fixed, allowlisted source templates."""

    bound: List[SourceSpec] = []
    for source in SOURCES:
        params = dict(source.params)
        if source.id in {"weather", "air_quality"}:
            params.update(
                {
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "timezone": location.timezone,
                }
            )
        elif source.id == "nws_alerts":
            params["point"] = f"{location.latitude:.4f},{location.longitude:.4f}"
        elif source.id == "solar_timing":
            params.update({"lat": location.latitude, "lng": location.longitude})
        bound.append(
            SourceSpec(
                id=source.id,
                label=source.label,
                provider=source.provider,
                url=source.url,
                params=params,
            )
        )
    return tuple(bound)


def _resolved_brief_name(request: DemoRunRequest) -> str:
    if request.brief_name:
        return request.brief_name
    workflow = WORKFLOWS[request.workflow_id]
    short_location = request.location.name.split(",", 1)[0].strip()
    return f"{short_location} {workflow.default_title}"


def _markdown_escape(value: Any) -> str:
    text = _clean_text(value, 2000)
    return re.sub(r"([\\`*{}\[\]()<>#+.!_|-])", r"\\\1", text)


def _slack_escape(value: Any) -> str:
    return (
        _clean_text(value, 2800)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _number(
    value: Any,
    *,
    minimum: float = -1_000_000,
    maximum: float = 1_000_000,
    digits: int = 1,
) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < minimum or number > maximum:
        return None
    return round(number, digits)


def _bounded_numeric_values(
    values: Any,
    *,
    limit: int = 24,
    minimum: float = -1_000_000,
    maximum: float = 1_000_000,
) -> List[float]:
    if not isinstance(values, list):
        return []
    bounded: List[float] = []
    for value in values[:limit]:
        parsed = _number(value, minimum=minimum, maximum=maximum)
        if parsed is not None:
            bounded.append(parsed)
    return bounded


def _validate_fixed_target(source: SourceSpec) -> None:
    parsed = urlparse(source.url)
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in (None, 443)
        or (parsed.hostname, parsed.path) not in _ALLOWED_TARGETS
    ):
        raise SourceFetchError("source_target_not_allowlisted")


async def _fetch_source(source: SourceSpec, operational_date: date) -> Mapping[str, Any]:
    """Fetch one fixed GET endpoint with redirect, time, and byte bounds."""

    _validate_fixed_target(source)
    params = dict(source.params)
    if source.id == "solar_timing":
        params["date"] = operational_date.isoformat()

    timeout = httpx.Timeout(SOURCE_TIMEOUT_SECONDS, connect=3.0)
    headers = {
        "Accept": "application/json, application/geo+json",
        "User-Agent": "Distributed-Life-Ultimate-Demo/1.0 (tools.distributed.systems)",
    }
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
        ) as client:
            async with client.stream("GET", source.url, params=params, headers=headers) as response:
                if response.is_redirect:
                    raise SourceFetchError("source_redirect_rejected")
                if response.status_code < 200 or response.status_code >= 300:
                    raise SourceFetchError(f"source_http_{response.status_code}")
                content_type = response.headers.get("content-type", "").lower()
                if "json" not in content_type:
                    raise SourceFetchError("source_non_json_response")
                chunks: List[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_SOURCE_BYTES:
                        raise SourceFetchError("source_response_too_large")
                    chunks.append(chunk)
    except SourceFetchError:
        raise
    except httpx.TimeoutException as exc:
        raise SourceFetchError("source_timeout") from exc
    except httpx.HTTPError as exc:
        raise SourceFetchError("source_unavailable") from exc

    try:
        payload = json.loads(b"".join(chunks))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SourceFetchError("source_invalid_json") from exc
    if not isinstance(payload, Mapping):
        raise SourceFetchError("source_invalid_shape")
    return payload


def _summarize_weather(payload: Mapping[str, Any]) -> Dict[str, Any]:
    current = payload.get("current") if isinstance(payload.get("current"), Mapping) else {}
    hourly = payload.get("hourly") if isinstance(payload.get("hourly"), Mapping) else {}
    precip = _bounded_numeric_values(
        hourly.get("precipitation_probability"), minimum=0, maximum=100
    )
    gusts = _bounded_numeric_values(hourly.get("wind_gusts_10m"), minimum=0, maximum=250)
    temperatures = _bounded_numeric_values(
        hourly.get("temperature_2m"), minimum=-100, maximum=150
    )
    return {
        "observed_at": _clean_text(current.get("time"), 40) or None,
        "temperature_f": _number(current.get("temperature_2m"), minimum=-100, maximum=150),
        "feels_like_f": _number(current.get("apparent_temperature"), minimum=-150, maximum=175),
        "precipitation_in": _number(current.get("precipitation"), minimum=0, maximum=100, digits=2),
        "weather_code": _number(current.get("weather_code"), minimum=0, maximum=99, digits=0),
        "wind_mph": _number(current.get("wind_speed_10m"), minimum=0, maximum=250),
        "gust_mph": _number(current.get("wind_gusts_10m"), minimum=0, maximum=300),
        "next_24h": {
            "max_precipitation_probability_pct": max(precip) if precip else None,
            "max_gust_mph": max(gusts) if gusts else None,
            "low_f": min(temperatures) if temperatures else None,
            "high_f": max(temperatures) if temperatures else None,
            "samples_used": max(len(precip), len(gusts), len(temperatures)),
        },
    }


def _summarize_air_quality(payload: Mapping[str, Any]) -> Dict[str, Any]:
    current = payload.get("current") if isinstance(payload.get("current"), Mapping) else {}
    return {
        "observed_at": _clean_text(current.get("time"), 40) or None,
        "us_aqi": _number(current.get("us_aqi"), minimum=0, maximum=500, digits=0),
        "pm2_5_ug_m3": _number(current.get("pm2_5"), minimum=0, maximum=2000),
        "pollen": {
            "grass": _number(current.get("grass_pollen"), minimum=0, maximum=10_000),
            "birch": _number(current.get("birch_pollen"), minimum=0, maximum=10_000),
            "alder": _number(current.get("alder_pollen"), minimum=0, maximum=10_000),
        },
    }


_ALERT_SEVERITY_ORDER = {"extreme": 0, "severe": 1, "moderate": 2, "minor": 3, "unknown": 4}


def _summarize_alerts(payload: Mapping[str, Any]) -> Dict[str, Any]:
    features = payload.get("features") if isinstance(payload.get("features"), list) else []
    alerts: List[Dict[str, Any]] = []
    for feature in features[:12]:
        if not isinstance(feature, Mapping):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, Mapping):
            continue
        severity = _clean_text(properties.get("severity"), 24).lower() or "unknown"
        alerts.append(
            {
                "event": _clean_text(properties.get("event"), 100) or "Weather alert",
                "headline": _clean_text(properties.get("headline"), 260),
                "severity": severity,
                "urgency": _clean_text(properties.get("urgency"), 30).lower() or "unknown",
                "onset": _clean_text(properties.get("onset"), 40) or None,
                "expires": _clean_text(properties.get("expires"), 40) or None,
            }
        )
    alerts.sort(key=lambda item: (_ALERT_SEVERITY_ORDER.get(item["severity"], 4), item["event"]))
    return {"active_count": len(alerts), "alerts": alerts}


def _summarize_solar(payload: Mapping[str, Any]) -> Dict[str, Any]:
    results = payload.get("results") if isinstance(payload.get("results"), Mapping) else {}
    return {
        "sunrise": _clean_text(results.get("sunrise"), 48) or None,
        "sunset": _clean_text(results.get("sunset"), 48) or None,
        "civil_twilight_begin": _clean_text(results.get("civil_twilight_begin"), 48) or None,
        "civil_twilight_end": _clean_text(results.get("civil_twilight_end"), 48) or None,
        "day_length_seconds": _number(
            results.get("day_length"), minimum=0, maximum=86_400, digits=0
        ),
    }


_SUMMARIZERS: Mapping[str, Callable[[Mapping[str, Any]], Dict[str, Any]]] = {
    "weather": _summarize_weather,
    "air_quality": _summarize_air_quality,
    "nws_alerts": _summarize_alerts,
    "solar_timing": _summarize_solar,
}


def _source_record(
    source: SourceSpec,
    *,
    status: str,
    fetched_at: datetime,
    data: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "source_id": source.id,
        "label": source.label,
        "provider": source.provider,
        "status": status,
        "fetched_at": _iso(fetched_at),
        "data": data,
        "error": error,
    }


def _format_metric(value: Any, suffix: str = "") -> str:
    return "unavailable" if value is None else f"{value:g}{suffix}"


def _format_local_clock(value: Any, timezone_name: str) -> str:
    if not isinstance(value, str):
        return "unavailable"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(ZoneInfo(timezone_name)).strftime("%I:%M %p").lstrip("0")
    except (KeyError, ValueError):
        return "unavailable"


def _workflow_recommendations(workflow_id: str) -> List[str]:
    return {
        "day_in_hand": [
            "Place the most weather-sensitive commitment inside the calmest available window.",
            "Keep one reversible fallback ready before the day starts moving.",
        ],
        "training_window": [
            "Protect an outdoor training window only when air, wind, and precipitation cooperate.",
            "Prepare an indoor session so consistency does not depend on conditions.",
        ],
        "arrival_brief": [
            "Add an arrival buffer around the most weather-sensitive transfer.",
            "Prepare one concise arrival note for the people handling the handoff.",
        ],
        "home_watch": [
            "Check exposed outdoor items, heating or cooling, and access before conditions change.",
            "Escalate only the house checks that require a person on site.",
        ],
        "weekend_host": [
            "Put the primary guest plan inside the best daylight and weather window.",
            "Prepare a nearby indoor alternative without changing the tone of the weekend.",
        ],
        "field_note": [
            "Keep every public claim attached to source coverage before sharing the draft.",
            "Prepare channel-specific versions; publish nothing until the final review.",
        ],
    }.get(workflow_id, [])


def _build_artifact(run: DemoRun) -> Dict[str, Any]:
    successful = {
        source_id: record
        for source_id, record in run.source_results.items()
        if record.get("status") == "ok"
    }
    failed = sorted({source.id for source in run.sources} - set(successful))
    weather = (successful.get("weather") or {}).get("data") or {}
    air = (successful.get("air_quality") or {}).get("data") or {}
    alerts = (successful.get("nws_alerts") or {}).get("data") or {}
    solar = (successful.get("solar_timing") or {}).get("data") or {}

    summary_parts: List[str] = []
    if weather:
        summary_parts.append(
            "Conditions are "
            f"{_format_metric(weather.get('temperature_f'), '°F')}"
            f" with gusts {_format_metric(weather.get('gust_mph'), ' mph')}."
        )
    if air:
        summary_parts.append(f"Air quality is AQI {_format_metric(air.get('us_aqi'))}.")
    if alerts:
        count = int(alerts.get("active_count") or 0)
        summary_parts.append(f"{count} bounded NWS alert{'s are' if count != 1 else ' is'} active.")
    if solar:
        sunrise = _format_local_clock(solar.get("sunrise"), run.location.timezone)
        sunset = _format_local_clock(solar.get("sunset"), run.location.timezone)
        summary_parts.append(
            f"Sunrise is {sunrise}; sunset is {sunset}."
        )
    if failed:
        summary_parts.append(f"Operating with partial coverage: {', '.join(failed)} unavailable.")

    attention: List[str] = []
    for alert in alerts.get("alerts", [])[:4] if isinstance(alerts.get("alerts"), list) else []:
        attention.append(
            f"{str(alert.get('severity') or 'unknown').title()} — "
            f"{_clean_text(alert.get('event'), 100)}: {_clean_text(alert.get('headline'), 180)}"
        )
    next_24h = weather.get("next_24h") if isinstance(weather.get("next_24h"), Mapping) else {}
    precipitation_probability = next_24h.get("max_precipitation_probability_pct")
    if isinstance(precipitation_probability, (int, float)) and precipitation_probability >= 50:
        attention.append(f"Precipitation probability reaches {precipitation_probability:g}% in the next 24 hours.")
    max_gust = next_24h.get("max_gust_mph")
    if isinstance(max_gust, (int, float)) and max_gust >= 30:
        attention.append(f"Forecast gusts reach {max_gust:g} mph in the next 24 hours.")
    aqi = air.get("us_aqi")
    if isinstance(aqi, (int, float)) and aqi >= 101:
        attention.append(f"AQI {aqi:g} is unhealthy for sensitive groups or worse.")
    attention = attention[:8]

    recommendations: List[str] = []
    if alerts.get("active_count"):
        recommendations.append("Review active NWS guidance before locking outdoor or travel plans.")
    if solar.get("sunrise") and solar.get("sunset"):
        recommendations.append("Keep the primary outdoor block inside the available daylight window.")
    if isinstance(max_gust, (int, float)) and max_gust >= 30:
        recommendations.append("Move wind-sensitive logistics to a protected window or venue.")
    if isinstance(aqi, (int, float)) and aqi >= 101:
        recommendations.append("Use an indoor training fallback and limit prolonged outdoor exertion.")
    recommendations.extend(_workflow_recommendations(run.workflow.id))
    recommendations.append("Recheck live conditions before any irreversible travel or event decision.")
    recommendations = recommendations[:8]

    canonical = {
        "schema_version": SCHEMA_VERSION,
        "kind": f"location_{run.workflow.id}_brief",
        "workflow_id": run.workflow.id,
        "location": {
            "name": run.location.name,
            "latitude": run.location.latitude,
            "longitude": run.location.longitude,
            "timezone": run.location.timezone,
        },
        "operational_date": run.operational_date.isoformat(),
        "brief_name": run.brief_name,
        "source_data": {
            source_id: successful[source_id]["data"] for source_id in sorted(successful)
        },
        "attention_items": attention,
        "recommendations": recommendations,
    }
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    artifact_id = f"life_brief_{digest}"

    provenance = []
    for source in run.sources:
        record = run.source_results.get(source.id) or {}
        provenance.append(
            {
                "source_id": source.id,
                "provider": source.provider,
                "status": record.get("status", "missing"),
                "fetched_at": record.get("fetched_at"),
                "error": record.get("error"),
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_id": artifact_id,
        "kind": f"location_{run.workflow.id}_brief",
        "workflow_id": run.workflow.id,
        "workflow_label": run.workflow.label,
        "title": run.brief_name,
        "location": {
            "name": run.location.name,
            "latitude": run.location.latitude,
            "longitude": run.location.longitude,
            "timezone": run.location.timezone,
        },
        "operational_date": run.operational_date.isoformat(),
        "generated_at": _iso(_now()),
        "source_coverage": {
            "successful": len(successful),
            "total": len(run.sources),
            "degraded": bool(failed),
            "missing_source_ids": failed,
        },
        "executive_summary": " ".join(summary_parts)[:1800],
        "conditions": {
            "weather": weather or None,
            "air_quality": air or None,
            "public_alerts": alerts or None,
            "solar_timing": solar or None,
        },
        "attention_items": attention,
        "recommendations": recommendations,
        "provenance": provenance,
        "safety": {
            "decision_support_only": True,
            "live_conditions_require_recheck": True,
            "external_actions_executed": False,
        },
    }


def _artifact_to_markdown(artifact: Mapping[str, Any]) -> str:
    coverage = artifact["source_coverage"]
    location = artifact.get("location") or {}
    lines = [
        f"# {_markdown_escape(artifact['title'])}",
        "",
        f"**{_markdown_escape(location.get('name') or 'Selected location')} · {artifact['operational_date']}**",
        "",
        _markdown_escape(artifact["executive_summary"]),
        "",
        "## Needs your eye",
        "",
    ]
    attention = artifact.get("attention_items") or []
    lines.extend(f"- {_markdown_escape(item)}" for item in attention)
    if not attention:
        lines.append("- No threshold-triggered issues in the available source projections.")
    lines.extend(["", "## Quietly prepared", ""])
    lines.extend(f"- {_markdown_escape(item)}" for item in artifact.get("recommendations") or [])
    lines.extend(
        [
            "",
            "## Source coverage",
            "",
            f"{coverage['successful']} of {coverage['total']} public sources responded.",
            "",
        ]
    )
    for source in artifact.get("provenance") or []:
        suffix = f" ({source['error']})" if source.get("error") else ""
        lines.append(
            f"- {_markdown_escape(source['provider'])}: "
            f"{_markdown_escape(source['status'])}{_markdown_escape(suffix)}"
        )
    lines.extend(
        [
            "",
            "---",
            "Decision support only. Recheck live conditions before acting.",
        ]
    )
    return "\n".join(lines)[:MAX_MARKDOWN_CHARS]


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ical_escape(value: Any) -> str:
    text = _clean_text(value, 700)
    return (
        text.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def _fold_ical_line(line: str, limit: int = 73) -> List[str]:
    """Conservatively fold UTF-8 content lines below RFC 5545's 75-octet limit."""

    chunks: List[str] = []
    current = ""
    current_bytes = 0
    for character in line:
        width = len(character.encode("utf-8"))
        if current and current_bytes + width > limit:
            chunks.append(current)
            current = " " + character
            current_bytes = 1 + width
        else:
            current += character
            current_bytes += width
    chunks.append(current)
    return chunks


def _build_ics(artifact: Mapping[str, Any]) -> tuple[str, str, str]:
    solar = ((artifact.get("conditions") or {}).get("solar_timing") or {})
    location = artifact.get("location") if isinstance(artifact.get("location"), Mapping) else {}
    location_name = _clean_text(location.get("name"), 120) or "Selected location"
    location_timezone = _clean_text(location.get("timezone"), 80) or "UTC"
    start = _parse_datetime(solar.get("sunrise"))
    if start is None:
        local = datetime.combine(
            date.fromisoformat(str(artifact["operational_date"])),
            datetime.min.time(),
            tzinfo=ZoneInfo(location_timezone),
        ).replace(hour=8)
        start = local.astimezone(timezone.utc)
    else:
        start += timedelta(minutes=30)
    end = start + timedelta(minutes=30)
    uid = f"{artifact['artifact_id']}@tools.distributed.systems"
    description = (
        f"Review the generated {location_name} operating brief. {artifact['executive_summary']} "
        "No external action has been taken."
    )
    raw_lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Distributed Life//Ultimate Demo//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        f"DTSTART:{start.strftime('%Y%m%dT%H%M%SZ')}",
        f"DTEND:{end.strftime('%Y%m%dT%H%M%SZ')}",
        f"SUMMARY:{_ical_escape(artifact['title'])}",
        f"LOCATION:{_ical_escape(location_name)}",
        f"DESCRIPTION:{_ical_escape(description)}",
        "STATUS:TENTATIVE",
        "END:VEVENT",
        "END:VCALENDAR",
    ]
    lines: List[str] = []
    for line in raw_lines:
        lines.extend(_fold_ical_line(line))
    return "\r\n".join(lines) + "\r\n", _iso(start), _iso(end)


def _build_drafts(artifact: Mapping[str, Any], markdown: str) -> Dict[str, Any]:
    ics, starts_at, ends_at = _build_ics(artifact)
    attention = artifact.get("attention_items") or []
    recommendations = artifact.get("recommendations") or []
    slack_lines = [
        f"*{_slack_escape(artifact['title'])} · {_slack_escape(artifact['operational_date'])}*",
        _slack_escape(artifact["executive_summary"]),
    ]
    if attention:
        slack_lines.extend(
            ["", "*Needs your eye*", *[f"• {_slack_escape(item)}" for item in attention[:4]]]
        )
    if recommendations:
        slack_lines.extend(
            ["", "*Prepared*", *[f"• {_slack_escape(item)}" for item in recommendations[:4]]]
        )

    common = {
        "status": "draft",
        "approval_required": True,
        "executed": False,
        "destination": None,
        "artifact_id": artifact["artifact_id"],
    }
    return {
        "calendar": {
            **common,
            "integration": "calendar",
            "operation": "create_event",
            "starts_at": starts_at,
            "ends_at": ends_at,
            "title": artifact["title"],
            "attendees": [],
            "send_updates": "none",
            "ics": ics,
        },
        "slack": {
            **common,
            "integration": "slack",
            "operation": "chat.postMessage",
            "channel": None,
            "text": "\n".join(slack_lines)[:2800],
        },
        "substack": {
            **common,
            "integration": "substack",
            "operation": "create_draft",
            "publication": None,
            "title": artifact["title"],
            "body_markdown": markdown,
            "publish": False,
        },
    }


class UltimateDemoService:
    """Bounded in-memory coordinator for the safe async demonstration."""

    def __init__(
        self,
        *,
        fetcher: Optional[Fetcher] = None,
        max_runs: int = MAX_RUNS,
        run_ttl_seconds: int = RUN_TTL_SECONDS,
    ) -> None:
        self.fetcher = fetcher or _fetch_source
        self.max_runs = max(1, min(int(max_runs), 1024))
        self.run_ttl_seconds = max(60, min(int(run_ttl_seconds), 86_400))
        self.runs: "OrderedDict[str, DemoRun]" = OrderedDict()

    def _prune(self) -> None:
        cutoff = time.time() - self.run_ttl_seconds
        for run_id, run in list(self.runs.items()):
            if run.status in TERMINAL_STATUSES and run.updated_at.timestamp() < cutoff:
                self.runs.pop(run_id, None)
        while len(self.runs) >= self.max_runs:
            terminal_id = next(
                (run_id for run_id, run in self.runs.items() if run.status in TERMINAL_STATUSES),
                None,
            )
            if terminal_id is None:
                raise RuntimeError("demo_capacity_reached")
            self.runs.pop(terminal_id, None)

    def _emit(self, run: DemoRun, event_type: str, data: Mapping[str, Any]) -> None:
        now = _now()
        run.updated_at = now
        run.events.append(
            {
                "event_id": len(run.events) + 1,
                "type": event_type,
                "timestamp": _iso(now),
                "data": dict(data),
            }
        )

    async def start(self, request: DemoRunRequest) -> DemoRun:
        self._prune()
        location = request.location
        workflow = WORKFLOWS[request.workflow_id]
        sources = _sources_for_location(location)
        operational_date = request.operational_date or datetime.now(
            ZoneInfo(location.timezone)
        ).date()
        run = DemoRun(
            run_id=f"demo_{uuid4().hex}",
            request=request,
            workflow=workflow,
            location=location,
            sources=sources,
            brief_name=_resolved_brief_name(request),
            operational_date=operational_date,
            created_at=_now(),
        )
        self.runs[run.run_id] = run
        self._emit(
            run,
            "run.created",
            {
                "status": "queued",
                "source_count": len(sources),
                "workflow_id": workflow.id,
                "location": location.name,
                "workflow": "map_reduce_artifact_to_drafts",
            },
        )
        run.task = asyncio.create_task(self._execute(run), name=f"ultimate-demo:{run.run_id}")
        return run

    def get(self, run_id: str) -> DemoRun:
        run = self.runs.get(run_id)
        if run is None:
            raise KeyError(run_id)
        return run

    def get_by_artifact_id(self, artifact_id: str) -> DemoRun:
        for run in reversed(self.runs.values()):
            if run.artifact and run.artifact.get("artifact_id") == artifact_id:
                return run
        raise KeyError(artifact_id)

    async def wait(self, run_id: str) -> DemoRun:
        run = self.get(run_id)
        if run.task is not None:
            await run.task
        return run

    async def _map_one(self, run: DemoRun, source: SourceSpec) -> None:
        self._emit(run, "source.started", {"source_id": source.id, "provider": source.provider})
        fetched_at = _now()
        try:
            payload = await asyncio.wait_for(
                self.fetcher(source, run.operational_date),
                timeout=SOURCE_TIMEOUT_SECONDS + 1.0,
            )
            if not isinstance(payload, Mapping):
                raise SourceFetchError("source_invalid_shape")
            projected = _SUMMARIZERS[source.id](payload)
            run.source_results[source.id] = _source_record(
                source, status="ok", fetched_at=fetched_at, data=projected
            )
            self._emit(
                run,
                "source.completed",
                {"source_id": source.id, "status": "ok"},
            )
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            error = "source_timeout"
            run.source_results[source.id] = _source_record(
                source, status="failed", fetched_at=fetched_at, error=error
            )
            self._emit(
                run,
                "source.failed",
                {"source_id": source.id, "status": "failed", "error": error},
            )
        except SourceFetchError as exc:
            error = _clean_text(str(exc), 80) or "source_unavailable"
            run.source_results[source.id] = _source_record(
                source, status="failed", fetched_at=fetched_at, error=error
            )
            self._emit(
                run,
                "source.failed",
                {"source_id": source.id, "status": "failed", "error": error},
            )
        except Exception:
            run.source_results[source.id] = _source_record(
                source, status="failed", fetched_at=fetched_at, error="source_projection_failed"
            )
            self._emit(
                run,
                "source.failed",
                {
                    "source_id": source.id,
                    "status": "failed",
                    "error": "source_projection_failed",
                },
            )
        completed = len(run.source_results)
        run.progress = min(70, 10 + int((completed / len(run.sources)) * 60))

    async def _execute(self, run: DemoRun) -> None:
        try:
            run.status = "running"
            run.stage = "mapping_sources"
            run.progress = 10
            self._emit(run, "run.started", {"status": run.status, "stage": run.stage})
            await asyncio.gather(*(self._map_one(run, source) for source in run.sources))

            succeeded = sum(
                1 for record in run.source_results.values() if record.get("status") == "ok"
            )
            if succeeded < run.request.min_successful_sources:
                run.status = "failed"
                run.stage = "insufficient_source_coverage"
                run.progress = 100
                run.error = (
                    f"insufficient_source_coverage:{succeeded}/"
                    f"{run.request.min_successful_sources}"
                )
                self._emit(
                    run,
                    "run.failed",
                    {
                        "status": run.status,
                        "error": run.error,
                        "successful_sources": succeeded,
                    },
                )
                return

            run.stage = "reducing_brief"
            run.progress = 78
            self._emit(
                run,
                "reduce.started",
                {"successful_sources": succeeded, "total_sources": len(run.sources)},
            )
            run.artifact = _build_artifact(run)
            run.artifact_markdown = _artifact_to_markdown(run.artifact)
            self._emit(
                run,
                "artifact.created",
                {
                    "artifact_id": run.artifact["artifact_id"],
                    "kind": run.artifact["kind"],
                },
            )

            run.stage = "deriving_action_drafts"
            run.progress = 90
            run.drafts = _build_drafts(run.artifact, run.artifact_markdown)
            self._emit(
                run,
                "drafts.created",
                {
                    "integrations": ["calendar", "slack", "substack"],
                    "approval_required": True,
                    "external_actions_executed": False,
                },
            )

            run.status = "completed"
            run.stage = "completed"
            run.progress = 100
            self._emit(
                run,
                "run.completed",
                {
                    "status": run.status,
                    "artifact_id": run.artifact["artifact_id"],
                    "degraded": run.artifact["source_coverage"]["degraded"],
                },
            )
        except asyncio.CancelledError:
            run.status = "failed"
            run.stage = "cancelled"
            run.progress = 100
            run.error = "run_cancelled"
            self._emit(run, "run.failed", {"status": run.status, "error": run.error})
            raise
        except Exception:
            run.status = "failed"
            run.stage = "internal_error"
            run.progress = 100
            run.error = "demo_execution_failed"
            self._emit(run, "run.failed", {"status": run.status, "error": run.error})

    def status_payload(self, run: DemoRun) -> Dict[str, Any]:
        links = {
            "self": f"/api/v1/demo/ultimate/runs/{run.run_id}",
            "events": f"/api/v1/demo/ultimate/runs/{run.run_id}/events",
            "widget": f"/api/v1/demo/ultimate/runs/{run.run_id}/widget",
        }
        if run.artifact is not None:
            artifact_id = run.artifact["artifact_id"]
            links.update(
                {
                    "artifact_json": f"/api/v1/demo/ultimate/artifacts/{artifact_id}",
                    "artifact_markdown": (
                        f"/api/v1/demo/ultimate/artifacts/{artifact_id}?format=markdown"
                    ),
                    "drafts": f"/api/v1/demo/ultimate/runs/{run.run_id}/drafts",
                }
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": run.run_id,
            "workflow": "map_reduce_artifact_to_drafts",
            "status": run.status,
            "stage": run.stage,
            "progress": run.progress,
            "operational_date": run.operational_date.isoformat(),
            "workflow_id": run.workflow.id,
            "workflow_label": run.workflow.label,
            "location": run.location.model_dump(),
            "created_at": _iso(run.created_at),
            "updated_at": _iso(run.updated_at),
            "sources": [
                run.source_results.get(source.id)
                or {
                    "source_id": source.id,
                    "label": source.label,
                    "provider": source.provider,
                    "status": "pending",
                    "fetched_at": None,
                    "data": None,
                    "error": None,
                }
                for source in run.sources
            ],
            "artifact_id": run.artifact.get("artifact_id") if run.artifact else None,
            "approval_required": bool(run.drafts),
            "external_actions_executed": False,
            "error": run.error,
            "latest_event_id": len(run.events),
            "links": links,
        }

    def widget_payload(self, run: DemoRun) -> Dict[str, Any]:
        successful = sum(
            1 for record in run.source_results.values() if record.get("status") == "ok"
        )
        failed = sum(
            1 for record in run.source_results.values() if record.get("status") == "failed"
        )
        artifact = run.artifact or {}
        draft_states = [
            {
                "integration": name,
                "status": payload["status"],
                "approval_required": payload["approval_required"],
                "executed": payload["executed"],
            }
            for name, payload in (run.drafts or {}).items()
        ]
        return {
            "schema_version": SCHEMA_VERSION,
            "widget_id": f"ultimate_demo:{run.run_id}",
            "kind": "distributed.realtime.operational_brief",
            "title": run.brief_name,
            "realtime": {
                "transport": "sse",
                "events_url": f"/api/v1/demo/ultimate/runs/{run.run_id}/events",
                "resume_parameter": "after_event_id",
                "latest_event_id": len(run.events),
            },
            "state": {
                "status": run.status,
                "stage": run.stage,
                "progress": run.progress,
                "source_coverage": {
                    "successful": successful,
                    "failed": failed,
                    "total": len(run.sources),
                },
            },
            "content": {
                "summary": artifact.get("executive_summary"),
                "attention_items": artifact.get("attention_items", [])[:8],
                "recommendations": artifact.get("recommendations", [])[:8],
            },
            "draft_actions": draft_states,
            "external_actions_executed": False,
            "links": self.status_payload(run)["links"],
        }


_service = UltimateDemoService()


def _require_run(run_id: str) -> DemoRun:
    try:
        return _service.get(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="ultimate demo run not found") from exc


def _require_artifact(run_id: str) -> DemoRun:
    run = _require_run(run_id)
    if run.artifact is None:
        raise HTTPException(
            status_code=409,
            detail={"error": "artifact_not_ready", "run_status": run.status},
        )
    return run


def _require_artifact_id(artifact_id: str) -> DemoRun:
    try:
        return _service.get_by_artifact_id(artifact_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="ultimate demo artifact not found") from exc


@router.get("/definition")
async def get_demo_definition() -> Dict[str, Any]:
    """Return the location-aware declarative workflows without starting work."""

    return {
        "schema_version": SCHEMA_VERSION,
        "workflow_id": "location-map-reduce-artifact-demo",
        "default_location": DemoLocation().model_dump(),
        "workflows": [
            {
                "id": workflow.id,
                "label": workflow.label,
                "description": workflow.description,
            }
            for workflow in WORKFLOWS.values()
        ],
        "structure": [
            {"id": "map", "mode": "parallel", "source_ids": [source.id for source in SOURCES]},
            {"id": "reduce", "mode": "deterministic_reduce", "depends_on": ["map"]},
            {"id": "artifact", "mode": "create_artifact", "depends_on": ["reduce"]},
            {
                "id": "draft_actions",
                "mode": "derive_only",
                "depends_on": ["artifact"],
                "integrations": ["calendar", "slack", "substack"],
                "approval_required": True,
            },
        ],
        "sources": [
            {
                "id": source.id,
                "label": source.label,
                "provider": source.provider,
                "method": "GET",
                "host": urlparse(source.url).hostname,
            }
            for source in SOURCES
        ],
        "network_policy": {
            "caller_supplied_urls": False,
            "redirects": False,
            "max_source_bytes": MAX_SOURCE_BYTES,
            "timeout_seconds": SOURCE_TIMEOUT_SECONDS,
        },
        "side_effect_policy": {
            "external_actions_executed": False,
            "outputs": "approval-gated drafts only",
        },
    }


@router.post("/runs", status_code=202)
async def start_demo_run(request: DemoRunRequest) -> Dict[str, Any]:
    try:
        run = await _service.start(request)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _service.status_payload(run)


@router.get("/runs/{run_id}")
async def get_demo_run(run_id: str) -> Dict[str, Any]:
    return _service.status_payload(_require_run(run_id))


@router.get("/runs/{run_id}/widget")
async def get_demo_widget(run_id: str) -> Dict[str, Any]:
    return _service.widget_payload(_require_run(run_id))


@router.get("/artifacts/{artifact_id}")
async def get_demo_artifact(
    artifact_id: str,
    artifact_format: Literal["json", "markdown"] = Query(default="json", alias="format"),
):
    run = _require_artifact_id(artifact_id)
    if artifact_format == "markdown":
        return PlainTextResponse(
            run.artifact_markdown or "",
            media_type="text/markdown; charset=utf-8",
            headers={"Content-Disposition": f'inline; filename="{artifact_id}-brief.md"'},
        )
    return run.artifact or {}


@router.get("/runs/{run_id}/drafts")
async def get_demo_drafts(run_id: str) -> Dict[str, Any]:
    run = _require_artifact(run_id)
    return {
        "run_id": run.run_id,
        "artifact_id": run.artifact["artifact_id"] if run.artifact else None,
        "approval_required": True,
        "external_actions_executed": False,
        "drafts": run.drafts or {},
    }


@router.get("/runs/{run_id}/events")
async def stream_demo_events(
    run_id: str,
    request: Request,
    after_event_id: int = Query(default=0, ge=0),
) -> StreamingResponse:
    run = _require_run(run_id)

    async def event_stream():
        cursor = after_event_id
        heartbeat_at = time.monotonic()
        while True:
            while cursor < len(run.events):
                event = run.events[cursor]
                cursor = int(event["event_id"])
                yield (
                    f"id: {event['event_id']}\n"
                    f"event: {event['type']}\n"
                    f"data: {json.dumps(event['data'], separators=(',', ':'))}\n\n"
                )
                heartbeat_at = time.monotonic()
            if run.status in TERMINAL_STATUSES:
                break
            if await request.is_disconnected():
                break
            if time.monotonic() - heartbeat_at >= 15:
                yield ": keep-alive\n\n"
                heartbeat_at = time.monotonic()
            await asyncio.sleep(0.05)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


__all__ = [
    "DemoRunRequest",
    "SourceFetchError",
    "UltimateDemoService",
    "router",
]
