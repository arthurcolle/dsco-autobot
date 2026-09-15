"""
Distributed Swarm MCP — native integration server.

Wraps openrouter-mcp with private DSCO additions. Two ideas are first-class:

1. CONTEXT WINDOWS AS FLUIDS. A context window is not a box, it is a
   volume. Context can be POURED (segmented into window-sized measures
   with overlap so no seam loses meaning), DISTILLED (mapped through a
   cheap model so a large volume flows downhill into a smaller one), and
   CHANNELLED (routed through many models and merged downstream).

2. SWARMS AS AIs USING TOOLS ACROSS A DISTRIBUTED SUBSTRATE. A swarm is
   not one model called twice — it is N agents, each holding its own
   context volume, each able to call tools on any backend registered in
   the Tool Management substrate (tools.distributed.systems). FANOUT is
   the primitive; DELEGATE is the substrate hop.

Deployed exactly like distributed_memory_mcp: FastAPI, /health, /mcp
(JSON-RPC), /tools, /mcp/sse, bearer middleware, announce + heartbeat to
the Tool Management registry.

Env:
  OPENROUTER_API_KEY        required for fanout/flow (catalog works without)
  OPENROUTER_BASE_URL       default https://openrouter.ai/api/v1
  REGISTRY_URL              default https://tools.distributed.systems
  REGISTRY_ANNOUNCE_TOKEN   x-registry-token for announce/heartbeat
  REGISTRY_BEARER_TOKEN     bearer for calling registry + sibling backends
  SERVICE_URL               public URL of this deployment (announced)
  SWARM_DEFAULT_MODELS      csv fallback fanout set
  SWARM_MAX_FANOUT          hard cap, default 8
  PORT                      default 8000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

try:  # deployed under ToolManagement/ -> native package importable
    from native.base import install_backend_bearer_middleware
except Exception:  # standalone fallback
    def install_backend_bearer_middleware(app, service_id, public_paths=()):
        return None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("distributed_swarm")

SERVICE_ID = "distributed-swarm"
SERVICE_NAME = "Distributed Swarm MCP"
REGISTRY_URL = os.environ.get("REGISTRY_URL", "https://tools.distributed.systems").rstrip("/")
REGISTRY_ANNOUNCE_TOKEN = os.environ.get("REGISTRY_ANNOUNCE_TOKEN", "")
REGISTRY_BEARER_TOKEN = os.environ.get("REGISTRY_BEARER_TOKEN", "")
SERVICE_URL = os.environ.get(
    "SERVICE_URL", "https://distributed-swarm-web.modal.run"
)
OPENROUTER_BASE = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")
SWARM_MAX_FANOUT = int(os.environ.get("SWARM_MAX_FANOUT", "8"))
DEFAULT_MODELS = [
    m.strip()
    for m in os.environ.get(
        "SWARM_DEFAULT_MODELS",
        "qwen/qwen3.7-flash,inclusionai/ling-2.6-flash,mistralai/mistral-nemo",
    ).split(",")
    if m.strip()
]

CAPABILITIES = [
    "swarm-fanout",
    "model-routing",
    "context-fluid",
    "substrate-delegation",
    "openrouter",
]

# ---------------------------------------------------------------------------
# Catalog cache — the fluid reservoir: every model is a context volume with
# a price of admission.
# ---------------------------------------------------------------------------

_catalog_cache: Dict[str, Any] = {"fetched_at": 0.0, "models": []}
_CATALOG_TTL = 300.0


async def _catalog(force: bool = False) -> List[Dict[str, Any]]:
    if not force and time.time() - _catalog_cache["fetched_at"] < _CATALOG_TTL and _catalog_cache["models"]:
        return _catalog_cache["models"]
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{OPENROUTER_BASE}/models")
        r.raise_for_status()
        models = r.json().get("data", [])
    _catalog_cache.update({"fetched_at": time.time(), "models": models})
    return models


def _price(model: Dict[str, Any]) -> Dict[str, float]:
    p = model.get("pricing", {})

    def f(k: str) -> float:
        try:
            return float(p.get(k) or 0)
        except (TypeError, ValueError):
            return 0.0

    return {"prompt_per_m": f("prompt") * 1e6, "completion_per_m": f("completion") * 1e6}


def _model_summary(m: Dict[str, Any]) -> Dict[str, Any]:
    pr = _price(m)
    return {
        "id": m.get("id"),
        "name": m.get("name"),
        "context_length": m.get("context_length") or 0,
        "free": pr["prompt_per_m"] == 0 and pr["completion_per_m"] == 0,
        **pr,
    }


# ---------------------------------------------------------------------------
# OpenRouter chat — one agent, one context volume.
# ---------------------------------------------------------------------------


async def _chat(model: str, messages: List[Dict[str, str]], max_tokens: int = 2048,
                temperature: float = 0.7) -> Dict[str, Any]:
    if not OPENROUTER_KEY:
        return {"model": model, "error": "OPENROUTER_API_KEY not configured"}
    headers = {
        "Authorization": f"Bearer {OPENROUTER_KEY}",
        "Content-Type": "application/json",
    }
    body = {"model": model, "messages": messages, "max_tokens": max_tokens,
            "temperature": temperature}
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(f"{OPENROUTER_BASE}/chat/completions",
                                  headers=headers, json=body)
            r.raise_for_status()
            data = r.json()
        usage = data.get("usage", {})
        return {
            "model": model,
            "content": data["choices"][0]["message"].get("content", ""),
            "latency_s": round(time.time() - t0, 3),
            "usage": usage,
        }
    except Exception as e:
        return {"model": model, "error": str(e), "latency_s": round(time.time() - t0, 3)}


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


async def tool_swarm_catalog(args: Dict[str, Any]) -> Dict[str, Any]:
    """List the reservoir: filter OpenRouter models by price/context/free."""
    models = await _catalog(force=bool(args.get("refresh")))
    max_price = args.get("max_prompt_price_per_m")
    min_ctx = int(args.get("min_context") or 0)
    free_only = bool(args.get("free_only"))
    query = (args.get("query") or "").lower()
    out = []
    for m in models:
        s = _model_summary(m)
        if free_only and not s["free"]:
            continue
        if max_price is not None and s["prompt_per_m"] > float(max_price):
            continue
        if min_ctx and s["context_length"] < min_ctx:
            continue
        if query and query not in (s["id"] or "").lower():
            continue
        out.append(s)
    out.sort(key=lambda s: (s["prompt_per_m"], -s["context_length"]))
    return {"count": len(out), "models": out[: int(args.get("limit") or 50)]}


async def tool_swarm_route(args: Dict[str, Any]) -> Dict[str, Any]:
    """Pick the optimal context volume(s) for a task."""
    min_ctx = int(args.get("min_context") or 8000)
    max_price = args.get("max_prompt_price_per_m")
    k = min(int(args.get("k") or 1), SWARM_MAX_FANOUT)
    prefer_free = bool(args.get("prefer_free"))
    models = await _catalog()
    cands = []
    for m in models:
        s = _model_summary(m)
        if s["context_length"] < min_ctx:
            continue
        if max_price is not None and s["prompt_per_m"] > float(max_price):
            continue
        cands.append(s)
    if prefer_free:
        free = [c for c in cands if c["free"]]
        if free:
            cands = free
    cands.sort(key=lambda s: (s["prompt_per_m"], -s["context_length"]))
    chosen = cands[:k]
    return {
        "task": args.get("task", ""),
        "chosen": chosen,
        "rationale": "cheapest prompt price meeting context floor; free tier preferred"
        if prefer_free else "cheapest prompt price meeting context floor",
    }


async def tool_swarm_fanout(args: Dict[str, Any]) -> Dict[str, Any]:
    """THE swarm primitive: one prompt poured into N context volumes at once."""
    prompt = args.get("prompt") or ""
    system = args.get("system")
    models = args.get("models") or DEFAULT_MODELS
    models = models[:SWARM_MAX_FANOUT]
    max_tokens = int(args.get("max_tokens") or 2048)
    temperature = float(args.get("temperature") or 0.7)
    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    t0 = time.time()
    results = await asyncio.gather(*[_chat(m, messages, max_tokens, temperature) for m in models])
    ok = [r for r in results if "error" not in r]
    return {
        "swarm_id": uuid.uuid4().hex[:12],
        "models": models,
        "succeeded": len(ok),
        "failed": len(results) - len(ok),
        "wall_time_s": round(time.time() - t0, 3),
        "results": results,
    }


def _pour(text: str, window: int, overlap: int) -> List[str]:
    """Segment a volume into window-sized measures; overlap keeps seams wet."""
    if window <= 0:
        raise ValueError("window must be > 0")
    overlap = max(0, min(overlap, window // 2))
    chunks, i = [], 0
    while i < len(text):
        chunks.append(text[i : i + window])
        i += window - overlap
    return chunks or [""]


async def tool_context_flow(args: Dict[str, Any]) -> Dict[str, Any]:
    """Context as fluid.

    op=pour:     text -> [{index, volume}] window-sized chunks.
    op=distill:  text -> pour -> each chunk mapped through `model` with
                 `instruction` -> summaries concatenated: a large volume
                 flows downhill into a small one.
    op=channel:  text -> pour -> chunks fanned out across `models` ->
                 merged per-chunk downstream.
    """
    op = args.get("op") or "pour"
    text = args.get("text") or ""
    window = int(args.get("window_chars") or 24000)
    overlap = int(args.get("overlap_chars") or 800)
    chunks = _pour(text, window, overlap)
    if op == "pour":
        return {"op": op, "chunks": len(chunks),
                "volumes": [{"index": i, "chars": len(c), "text": c} for i, c in enumerate(chunks)]}
    if op == "distill":
        model = args.get("model") or DEFAULT_MODELS[0]
        instruction = args.get("instruction") or (
            "Distill this segment to its essential facts, decisions, and open "
            "questions. Be terse; this summary flows into a smaller context."
        )
        async def distill_one(i: int, chunk: str) -> Dict[str, Any]:
            msgs = [{"role": "system", "content": instruction},
                    {"role": "user", "content": f"[segment {i + 1}/{len(chunks)}]\n{chunk}"}]
            r = await _chat(model, msgs, max_tokens=int(args.get("max_tokens") or 1024))
            return {"index": i, "summary": r.get("content", ""), "error": r.get("error")}
        distilled = await asyncio.gather(*[distill_one(i, c) for i, c in enumerate(chunks)])
        return {"op": op, "model": model, "chunks": len(chunks),
                "input_chars": len(text),
                "output_chars": sum(len(d["summary"]) for d in distilled),
                "distilled": distilled,
                "flow": "\n\n".join(d["summary"] for d in distilled if d["summary"])}
    if op == "channel":
        models = (args.get("models") or DEFAULT_MODELS)[:SWARM_MAX_FANOUT]
        instruction = args.get("instruction") or "Process this segment per the task."
        task = args.get("task") or ""
        async def channel_one(i: int, chunk: str) -> Dict[str, Any]:
            msgs = [{"role": "system", "content": f"{instruction}\nTask: {task}"},
                    {"role": "user", "content": f"[segment {i + 1}/{len(chunks)}]\n{chunk}"}]
            rs = await asyncio.gather(*[_chat(m, msgs, int(args.get("max_tokens") or 1024)) for m in models])
            return {"index": i, "responses": rs}
        merged = await asyncio.gather(*[channel_one(i, c) for i, c in enumerate(chunks)])
        return {"op": op, "models": models, "chunks": len(chunks), "channels": merged}
    return {"error": f"unknown op: {op}", "ops": ["pour", "distill", "channel"]}


async def tool_swarm_delegate(args: Dict[str, Any]) -> Dict[str, Any]:
    """Substrate hop: find a backend in the Tool Management registry by
    capability and call one of its tools. Swarms using tools across the
    distributed system, not just models."""
    capability = args.get("capability") or ""
    tool_name = args.get("tool") or ""
    tool_args = args.get("arguments") or {}
    headers = {"Authorization": f"Bearer {REGISTRY_BEARER_TOKEN}"} if REGISTRY_BEARER_TOKEN else {}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{REGISTRY_URL}/api/v1/tools", headers=headers)
        r.raise_for_status()
        registry = r.json()
    backends = registry.get("tools", registry if isinstance(registry, list) else [])
    match = None
    for b in backends:
        caps = b.get("capabilities", [])
        names = [t.get("name") for t in b.get("tools", []) if isinstance(t, dict)]
        if capability in caps or tool_name in names:
            match = b
            break
    if not match:
        return {"error": "no backend matches", "capability": capability,
                "seen": [{"id": b.get("backend_id"), "capabilities": b.get("capabilities")}
                         for b in backends]}
    url = (match.get("url") or "").rstrip("/") + "/mcp"
    payload = {"jsonrpc": "2.0", "id": uuid.uuid4().hex[:8], "method": "tools/call",
               "params": {"name": tool_name, "arguments": tool_args}}
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(url, json=payload, headers=headers)
    return {"backend": match.get("backend_id"), "url": url, "status": r.status_code,
            "response": r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:4000]}


async def tool_swarm_status(args: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {REGISTRY_BEARER_TOKEN}"} if REGISTRY_BEARER_TOKEN else {}
    registry_ok = False
    backend_count = 0
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{REGISTRY_URL}/api/v1/tools", headers=headers)
            registry_ok = r.status_code == 200
            if registry_ok:
                data = r.json()
                backend_count = len(data.get("tools", data if isinstance(data, list) else []))
    except Exception as e:
        logger.warning("registry probe failed: %s", e)
    return {
        "service": SERVICE_ID,
        "registry_url": REGISTRY_URL,
        "registry_reachable": registry_ok,
        "backends_visible": backend_count,
        "openrouter_configured": bool(OPENROUTER_KEY),
        "catalog_models": len(_catalog_cache["models"]),
        "catalog_age_s": round(time.time() - _catalog_cache["fetched_at"], 1) if _catalog_cache["fetched_at"] else None,
        "max_fanout": SWARM_MAX_FANOUT,
    }


TOOLS: Dict[str, Dict[str, Any]] = {
    "swarm_catalog": {
        "description": "List the OpenRouter model reservoir, filtered by price, context size, free tier.",
        "schema": {"type": "object", "properties": {
            "query": {"type": "string"}, "free_only": {"type": "boolean"},
            "max_prompt_price_per_m": {"type": "number"},
            "min_context": {"type": "integer"}, "limit": {"type": "integer"},
            "refresh": {"type": "boolean"}}},
        "fn": tool_swarm_catalog,
    },
    "swarm_route": {
        "description": "Route a task to the optimal model(s): cheapest context volume meeting requirements.",
        "schema": {"type": "object", "properties": {
            "task": {"type": "string"}, "min_context": {"type": "integer"},
            "max_prompt_price_per_m": {"type": "number"},
            "k": {"type": "integer"}, "prefer_free": {"type": "boolean"}}},
        "fn": tool_swarm_route,
    },
    "swarm_fanout": {
        "description": "Swarm primitive: one prompt fanned out to N models concurrently; returns every completion with latency and usage.",
        "schema": {"type": "object", "required": ["prompt"], "properties": {
            "prompt": {"type": "string"}, "system": {"type": "string"},
            "models": {"type": "array", "items": {"type": "string"}},
            "max_tokens": {"type": "integer"}, "temperature": {"type": "number"}}},
        "fn": tool_swarm_fanout,
    },
    "context_flow": {
        "description": "Context windows as fluids: pour (segment), distill (map-reduce shrink), channel (fanout per segment and merge).",
        "schema": {"type": "object", "required": ["op", "text"], "properties": {
            "op": {"enum": ["pour", "distill", "channel"]}, "text": {"type": "string"},
            "window_chars": {"type": "integer"}, "overlap_chars": {"type": "integer"},
            "model": {"type": "string"}, "models": {"type": "array", "items": {"type": "string"}},
            "instruction": {"type": "string"}, "task": {"type": "string"},
            "max_tokens": {"type": "integer"}}},
        "fn": tool_context_flow,
    },
    "swarm_delegate": {
        "description": "Substrate hop: resolve a backend by capability in the Tool Management registry and call its tool.",
        "schema": {"type": "object", "required": ["tool"], "properties": {
            "capability": {"type": "string"}, "tool": {"type": "string"},
            "arguments": {"type": "object"}}},
        "fn": tool_swarm_delegate,
    },
    "swarm_status": {
        "description": "Swarm health: registry reachability, visible backends, OpenRouter configuration, catalog cache age.",
        "schema": {"type": "object", "properties": {}},
        "fn": tool_swarm_status,
    },
}

# ---------------------------------------------------------------------------
# MCP JSON-RPC plumbing (same contract as distributed_memory_mcp)
# ---------------------------------------------------------------------------


def _rpc_result(rid: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rid, "result": result}


def _rpc_error(rid: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}


async def _handle_rpc(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rid = msg.get("id")
    method = msg.get("method", "")
    if method == "initialize":
        return _rpc_result(rid, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVICE_ID, "version": "0.1.0"},
        })
    if method in ("notifications/initialized", "initialized"):
        return None
    if method == "tools/list":
        return _rpc_result(rid, {"tools": [
            {"name": name, "description": t["description"], "inputSchema": t["schema"]}
            for name, t in TOOLS.items()
        ]})
    if method == "tools/call":
        params = msg.get("params", {})
        name = params.get("name", "")
        if name not in TOOLS:
            return _rpc_error(rid, -32602, f"unknown tool: {name}")
        try:
            result = await TOOLS[name]["fn"](params.get("arguments") or {})
            return _rpc_result(rid, {"content": [
                {"type": "text", "text": json.dumps(result, indent=2, default=str)}
            ]})
        except Exception as e:
            logger.exception("tool %s failed", name)
            return _rpc_error(rid, -32000, str(e))
    if method == "ping":
        return _rpc_result(rid, {})
    return _rpc_error(rid, -32601, f"method not found: {method}")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title=SERVICE_NAME)
install_backend_bearer_middleware(app, public_paths={"/health"})


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok", "service": SERVICE_ID, "tools": len(TOOLS),
            "registry": REGISTRY_URL}


@app.get("/tools")
async def list_tools() -> Dict[str, Any]:
    return {"tools": [{"name": n, "description": t["description"]} for n, t in TOOLS.items()]}


@app.post("/mcp")
async def mcp(request: Request) -> Any:
    try:
        msg = await request.json()
    except Exception:
        return JSONResponse(_rpc_error(None, -32700, "parse error"), status_code=400)
    if isinstance(msg, list):  # batch
        out = [r for r in (await asyncio.gather(*[_handle_rpc(m) for m in msg])) if r]
        return JSONResponse(out)
    resp = await _handle_rpc(msg)
    if resp is None:
        return JSONResponse({}, status_code=202)
    return JSONResponse(resp)


@app.get("/mcp/sse")
async def mcp_sse() -> StreamingResponse:
    async def stream():
        yield f"data: {json.dumps({'jsonrpc': '2.0', 'method': 'notifications/ready', 'params': {'service': SERVICE_ID}})}\n\n"
        while True:
            await asyncio.sleep(15)
            yield f"data: {json.dumps({'jsonrpc': '2.0', 'method': 'notifications/heartbeat'})}\n\n"
    return StreamingResponse(stream(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Registry announce + heartbeat — same contract as distributed_memory_mcp
# ---------------------------------------------------------------------------


async def _announce_loop() -> None:
    await asyncio.sleep(2)
    payload = {
        "backend_id": SERVICE_ID,
        "name": SERVICE_NAME,
        "url": SERVICE_URL,
        "capabilities": CAPABILITIES,
        "tools": [{"name": n, "description": t["description"]} for n, t in TOOLS.items()],
    }
    headers = {"x-registry-token": REGISTRY_ANNOUNCE_TOKEN} if REGISTRY_ANNOUNCE_TOKEN else {}
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(f"{REGISTRY_URL}/api/v1/tools/announce",
                                      json=payload, headers=headers)
                logger.info("announce -> %s (%d tools)", r.status_code, len(TOOLS))
        except Exception as e:
            logger.warning("announce failed: %s", e)
        await asyncio.sleep(60)


@app.on_event("startup")
async def _startup() -> None:
    asyncio.create_task(_announce_loop())


def main() -> None:
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
