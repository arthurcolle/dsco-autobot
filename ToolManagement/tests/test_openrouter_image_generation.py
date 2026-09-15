"""Offline regression tests for the real OpenRouter image adapter."""
import base64
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from native.integration_fabric import server


@pytest.fixture
def fabric(monkeypatch):
    # The tested methods require no app lifecycle, DB, or production credential.
    obj = object.__new__(server.IntegrationFabricMCP)
    monkeypatch.setattr(server, "OPENROUTER_KEY", "fake-pooled-key")
    return obj


def body(result):
    return json.loads(result["content"][0]["text"])


@pytest.mark.asyncio
async def test_real_image_dispatch_and_parameters(fabric, monkeypatch):
    seen = []
    encoded = base64.b64encode(b"test-image-bytes").decode()
    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"created": 123, "data": [
            {"b64_json": encoded, "media_type": "image/webp"}
        ], "usage": {"cost": 0.13}})
    client = httpx.AsyncClient
    monkeypatch.setattr(server.httpx, "AsyncClient", lambda **kwargs: client(
        transport=httpx.MockTransport(handler), **kwargs))
    params = {"prompt": "HEAT TRADING", "model": "openai/gpt-image-2",
              "quality": "high", "aspect_ratio": "16:9", "n": 1,
              "provider": {"allow_fallbacks": False}, "api_key": "must-not-forward"}
    result = await fabric._execute_tool("openrouter_ai_image_generate", {"params": params})
    assert not result.get("isError")
    assert len(seen) == 1
    assert str(seen[0].url) == "https://openrouter.ai/api/v1/images"
    assert seen[0].headers["Authorization"] == "Bearer fake-pooled-key"
    payload = json.loads(seen[0].content)
    assert payload == {k: v for k, v in params.items() if k != "api_key"}
    assert body(result)["mode"] == "live"
    assert body(result)["usage"]["cost"] == 0.13
    assert result["content"][1] == {"type": "image", "data": encoded, "mimeType": "image/webp"}
    assert encoded not in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_byok_overrides_pooled_credential(fabric, monkeypatch):
    seen = []
    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"data": [{"b64_json": "aGVsbG8="}]})
    client = httpx.AsyncClient
    monkeypatch.setattr(server.httpx, "AsyncClient", lambda **kwargs: client(
        transport=httpx.MockTransport(handler), **kwargs))
    result = await fabric._live_openrouter_image({"params": {"prompt": "test"}}, api_key="fake-tenant-key")
    assert seen[0].headers["Authorization"] == "Bearer fake-tenant-key"
    assert body(result)["model"] == "openai/gpt-image-2"


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [{}, {"prompt": " "}, {"prompt": "test", "n": 0},
                                   {"prompt": "test", "n": 11}, {"prompt": "test", "n": True}])
async def test_bad_input_never_calls_provider(fabric, monkeypatch, params):
    def forbidden(**kwargs):
        raise AssertionError("invalid input must not call the provider")
    monkeypatch.setattr(server.httpx, "AsyncClient", forbidden)
    result = await fabric._live_openrouter_image({"params": params})
    assert result["isError"]


@pytest.mark.asyncio
async def test_missing_key_is_error_not_sandbox(fabric, monkeypatch):
    monkeypatch.setattr(server, "OPENROUTER_KEY", "")
    result = await fabric._execute_tool("openrouter_ai_image_generate", {"params": {"prompt": "test"}})
    assert result["isError"]
    assert "not configured" in body(result)["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [{"data": []}, {"data": [{}]}, {"data": [{"b64_json": ""}]}])
async def test_empty_provider_output_is_error(fabric, monkeypatch, response):
    client = httpx.AsyncClient
    monkeypatch.setattr(server.httpx, "AsyncClient", lambda **kwargs: client(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=response)), **kwargs))
    result = await fabric._live_openrouter_image({"params": {"prompt": "test"}})
    assert result["isError"]


@pytest.mark.asyncio
async def test_provider_failure_not_retried_or_secret_leaked(fabric, monkeypatch):
    calls = []
    def handler(req):
        calls.append(req)
        return httpx.Response(401, json={"error": "Unauthorized"})
    client = httpx.AsyncClient
    monkeypatch.setattr(server.httpx, "AsyncClient", lambda **kwargs: client(
        transport=httpx.MockTransport(handler), **kwargs))
    result = await fabric._execute_tool("openrouter_ai_image_generate", {"params": {"prompt": "test"}})
    assert result["isError"]
    assert len(calls) == 1
    assert "fake-pooled-key" not in json.dumps(result)


@pytest.mark.asyncio
async def test_existing_chat_routing_unchanged(fabric, monkeypatch):
    monkeypatch.setitem(server.OPENAI_COMPAT, "openrouter", ("https://openrouter.ai/api/v1", "default", "fake"))
    call = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr(fabric, "_live_openai_chat", call)
    assert await fabric._live("ai", "chat", "openrouter", {"params": {"prompt": "test"}}) == {"ok": True}
    call.assert_awaited_once()
