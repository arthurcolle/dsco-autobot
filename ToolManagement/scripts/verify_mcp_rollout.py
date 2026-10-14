#!/usr/bin/env python3
"""Read-only MCP release gate driven by an operator-supplied coverage manifest.

Checks declared protocol features and explicit operation contracts, not just
heartbeats. Never retries a tools/call or follows HTTP redirects with credentials.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any

import httpx


class VerificationError(RuntimeError):
    pass


def decoded_body(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("isError"):
        raise VerificationError("MCP tool reported isError")
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    for block in result.get("content", []):
        if block.get("type") == "text":
            try:
                body = json.loads(block["text"])
            except (KeyError, TypeError, ValueError):
                continue
            if isinstance(body, dict):
                return body
    raise VerificationError("tool result has no structured JSON object")


def assert_contract(body: dict[str, Any], checks: dict[str, Any]) -> None:
    if body.get("error") or body.get("status") in {"error", "failed"}:
        raise VerificationError("operation returned an error")
    def synthetic(value: Any) -> bool:
        if isinstance(value, dict):
            return value.get("synthetic") is True or value.get("mode") in {"sandbox", "mock"} or any(synthetic(v) for v in value.values())
        if isinstance(value, list):
            return any(synthetic(v) for v in value)
        return False
    if synthetic(body):
        raise VerificationError("synthetic result cannot qualify a rollout")
    for path, expected in checks.items():
        value: Any = body
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                raise VerificationError(f"missing contract field: {path}")
            value = value[part]
        if value != expected:
            raise VerificationError(f"contract mismatch: {path}")


def validate_manifest(manifest: dict[str, Any]) -> None:
    servers = manifest.get("servers")
    if not isinstance(servers, list) or not servers:
        raise VerificationError("manifest requires at least one server")
    expected_ids = manifest.get("expected_server_ids")
    if not isinstance(expected_ids, list) or not expected_ids or len(set(expected_ids)) != len(expected_ids):
        raise VerificationError("manifest requires a nonempty, unique expected_server_ids inventory")
    ids = set()
    for server in servers:
        sid = server.get("id")
        if not sid or sid in ids:
            raise VerificationError("server ids must be unique and nonempty")
        ids.add(sid)
        url = httpx.URL(server.get("url", ""))
        if url.scheme not in {"http", "https"} or not url.host or url.userinfo or url.query:
            raise VerificationError("server URL must be HTTP(S), without credentials or query")
        if url.scheme == "http" and url.host not in {"localhost", "127.0.0.1", "::1"}:
            raise VerificationError("remote verification requires HTTPS")
        probes = server.get("probes")
        if not isinstance(probes, list) or not probes:
            raise VerificationError(f"{sid}: explicit operation probes required")
        features = server.get("required_features", [])
        if not isinstance(features, list) or any(f not in {"tools", "prompts", "resources"} for f in features):
            raise VerificationError(f"{sid}: invalid required features")
        for probe in probes:
            if probe.get("effect") != "read" or not probe.get("tool"):
                raise VerificationError(f"{sid}: probes must name an operator-approved read operation")
            if not isinstance(probe.get("equals"), dict) or not probe["equals"]:
                raise VerificationError(f"{sid}: every probe needs a result contract")
    if ids != set(expected_ids):
        raise VerificationError("server coverage does not match expected_server_ids inventory")


async def verify_server(server: dict[str, Any], client: httpx.AsyncClient) -> dict[str, Any]:
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    token_env = server.get("token_env")
    if token_env:
        token = os.environ.get(token_env)
        if not token:
            raise VerificationError(f"{server['id']}: token environment variable missing")
        headers["Authorization"] = f"Bearer {token}"
    request_id = 0

    async def rpc(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        nonlocal request_id
        request_id += 1
        response = await client.post(server["url"], headers=headers, json={
            "jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {},
        })
        response.raise_for_status()
        if response.headers.get("mcp-session-id"):
            headers["Mcp-Session-Id"] = response.headers["mcp-session-id"]
        if "text/event-stream" in response.headers.get("content-type", ""):
            envelopes = []
            for event in response.text.replace("\r\n", "\n").split("\n\n"):
                lines = [line[5:].lstrip() for line in event.splitlines() if line.startswith("data:")]
                if lines:
                    try:
                        envelopes.append(json.loads("\n".join(lines)))
                    except ValueError:
                        continue
            matching = [e for e in envelopes if isinstance(e, dict) and e.get("id") == request_id]
            if len(matching) != 1:
                raise VerificationError("MCP stream lacked exactly one matching response")
            envelope = matching[0]
        else:
            envelope = response.json()
        if envelope.get("id") != request_id or envelope.get("error"):
            raise VerificationError(f"{method}: MCP response error or mismatched request id")
        result = envelope.get("result")
        if not isinstance(result, dict):
            raise VerificationError(f"{method}: missing result")
        return result

    initialized = await rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
        "clientInfo": {"name": "dsco-release-verifier", "version": "1"}})
    protocol = initialized.get("protocolVersion")
    if server.get("expected_version") and initialized.get("serverInfo", {}).get("version") != server["expected_version"]:
        raise VerificationError("deployed server version does not match release manifest")
    if protocol:
        headers["MCP-Protocol-Version"] = protocol
    response = await client.post(server["url"], headers=headers,
        json={"jsonrpc": "2.0", "method": "notifications/initialized"})
    response.raise_for_status()
    capabilities = initialized.get("capabilities", {})
    for feature in server.get("required_features", []):
        if feature not in capabilities:
            raise VerificationError(f"missing required MCP feature: {feature}")
    for feature, key in (("tools", "tools"), ("prompts", "prompts"), ("resources", "resources")):
        if feature not in capabilities:
            continue
        listed = await rpc(f"{feature}/list")
        if not isinstance(listed.get(key), list):
            raise VerificationError(f"{feature}/list did not return a list")
    checked = []
    for probe in server["probes"]:
        result = await rpc("tools/call", {"name": probe["tool"], "arguments": probe.get("arguments", {})})
        assert_contract(decoded_body(result), probe["equals"])
        checked.append(probe["tool"])
    for prompt in server.get("prompts", []):
        result = await rpc("prompts/get", prompt)
        if not isinstance(result.get("messages"), list) or not result["messages"]:
            raise VerificationError("prompts/get returned no messages")
    for uri in server.get("skill_uris", []):
        result = await rpc("resources/read", {"uri": uri})
        if not any(item.get("uri") == uri and item.get("text") for item in result.get("contents", [])):
            raise VerificationError("skill resource returned no text")
    return {"id": server["id"], "ok": True, "verified_tools": checked,
            "prompts_checked": len(server.get("prompts", [])), "skills_checked": len(server.get("skill_uris", []))}


async def verify(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    validate_manifest(manifest)
    results = []
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as client:
        for server in manifest["servers"]:
            try:
                results.append(await verify_server(server, client))
            except Exception as exc:
                # No raw HTTP payloads, credential-bearing URLs, or tool results
                # in public reports. Details stay in the server's private logs.
                results.append({"id": server["id"], "ok": False, "error_type": type(exc).__name__,
                                "error": str(exc) if isinstance(exc, VerificationError) else "request failed"})
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", help="JSON file enumerating every required integration server and read contract")
    args = parser.parse_args()
    try:
        with open(args.manifest, encoding="utf-8") as source:
            results = asyncio.run(verify(json.load(source)))
    except Exception as exc:
        print(json.dumps({"ok": False, "error_type": type(exc).__name__}))
        return 1
    ok = all(r["ok"] for r in results)
    print(json.dumps({"ok": ok, "servers": results}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
