"""Live-weather adapter regressions.

The catalog accepts a natural-language city identifier.  OpenWeather's current
weather endpoint, however, does not resolve the common ``City, ST`` form unless
the country code is present (for example, ``Washington, DC`` must be sent as
``Washington, DC, US``).  These tests preserve that translation and the live
response contract without making a network call.
"""

import json

import httpx
import pytest

from native.integration_fabric.server import IntegrationFabricMCP
from native.base import backend_bearer_is_valid

# Preserve the concrete class before monkeypatching the server module's shared
# httpx module.  The mock context manager delegates to this original client.
_REAL_ASYNC_CLIENT = httpx.AsyncClient


def test_native_integration_server_auth_is_fail_closed_in_production(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("NATIVE_MCP_REQUIRE_BEARER", "1")
    monkeypatch.delenv("BACKEND_BEARER_TOKEN", raising=False)
    assert backend_bearer_is_valid("Bearer arbitrary") == (True, False)


def _body(response):
    return json.loads(response["content"][0]["text"])


@pytest.mark.asyncio
async def test_openweather_normalizes_us_city_state_natural_key(monkeypatch):
    requested = {}

    async def handler(request):
        requested.update(dict(request.url.params))
        return httpx.Response(
            200,
            json={
                "name": "Washington, D.C.",
                "sys": {"country": "US"},
                "main": {"temp": 21.5, "feels_like": 20.9, "humidity": 44},
                "weather": [{"main": "Clear", "description": "clear sky"}],
                "wind": {"speed": 3.6, "deg": 270},
            },
        )

    class Client:
        def __init__(self, *args, **kwargs):
            self._client = _REAL_ASYNC_CLIENT(transport=httpx.MockTransport(handler))

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, *args):
            await self._client.aclose()

    monkeypatch.setattr("native.integration_fabric.server.httpx.AsyncClient", Client)
    server = IntegrationFabricMCP(port=0)

    body = _body(await server._live_openweather("current", {"id": "Washington, DC"}, "test-key"))

    assert requested["q"] == "Washington, DC, US"
    assert requested["appid"] == "test-key"
    assert requested["units"] == "metric"
    assert body["mode"] == "live"
    assert body["object"] == {
        "location": "Washington, D.C.",
        "temp_c": 21.5,
        "humidity": 44,
        "conditions": "clear sky",
        "wind_mps": 3.6,
    }


@pytest.mark.asyncio
async def test_openweather_preserves_country_qualified_location(monkeypatch):
    requested = {}

    async def handler(request):
        requested.update(dict(request.url.params))
        return httpx.Response(200, json={"name": "Paris", "main": {}, "weather": []})

    class Client:
        def __init__(self, *args, **kwargs):
            self._client = _REAL_ASYNC_CLIENT(transport=httpx.MockTransport(handler))

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, *args):
            await self._client.aclose()

    monkeypatch.setattr("native.integration_fabric.server.httpx.AsyncClient", Client)
    server = IntegrationFabricMCP(port=0)

    await server._live_openweather("current", {"id": "Paris, FR"}, "test-key")

    assert requested["q"] == "Paris, FR"
