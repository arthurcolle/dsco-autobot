#!/usr/bin/env python3
"""Discover MCP servers declared on this machine and ingest their tools into the registry.

Reads server specs from ~/.claude.json (global + per-project mcpServers) plus an
optional local suite, connects to each over its native transport (stdio or SSE),
lists its tools, and push-announces them to the Tool Management registry.

Usage:
  python scripts/ingest_mcps.py                 # ingest blessed servers from ~/.claude.json
  python scripts/ingest_mcps.py --suite         # also ingest the intl_x_agent FastMCP suite
  python scripts/ingest_mcps.py --only email neon
  python scripts/ingest_mcps.py --timeout 60
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

try:
    from mcp.client.sse import sse_client
except Exception:  # pragma: no cover
    sse_client = None

REGISTRY = os.getenv("REGISTRY_URL", "http://localhost:2016").rstrip("/")
HOME = Path.home()
_VAR = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _expand(s: str) -> str:
    return _VAR.sub(lambda m: os.environ.get(m.group(1), ""), s)


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


def _flatten_err(e: BaseException, depth: int = 0) -> str:
    """Unwrap ExceptionGroup/TaskGroup errors to the underlying cause."""
    excs = getattr(e, "exceptions", None)
    if excs and depth < 4:
        return "; ".join(_flatten_err(x, depth + 1) for x in excs)
    return f"{type(e).__name__}: {e}"


def load_claude_servers() -> Dict[str, Dict[str, Any]]:
    """Collect distinct mcpServers from ~/.claude.json (global + all projects)."""
    path = HOME / ".claude.json"
    out: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return out
    data = json.loads(path.read_text())
    scopes = [data.get("mcpServers") or {}]
    for proj in (data.get("projects") or {}).values():
        scopes.append(proj.get("mcpServers") or {})
    for scope in scopes:
        for name, cfg in scope.items():
            out.setdefault(name, cfg)
    return out


def load_suite() -> Dict[str, Dict[str, Any]]:
    """The intl_x_agent FastMCP suite (stdio, launched via uv with fastmcp)."""
    base = HOME / "Dsco/dsco-mcp-openrouter-client/intl_x_agent/mcp"
    out: Dict[str, Dict[str, Any]] = {}
    if not base.exists():
        return out
    for d in sorted(base.iterdir()):
        server = d / "server.py"
        if d.is_dir() and server.exists():
            out[f"ixa-{d.name}"] = {
                "command": "uv",
                "args": ["run", "--with", "fastmcp", "python", str(server)],
            }
    return out


async def _session_list_tools(session: ClientSession):
    await session.initialize()
    resp = await session.list_tools()
    return resp.tools


async def list_tools_stdio(cfg: Dict[str, Any]):
    command = cfg["command"]
    args = [_expand(a) for a in cfg.get("args", [])]
    env = {**os.environ, **{k: _expand(str(v)) for k, v in (cfg.get("env") or {}).items()}}
    params = StdioServerParameters(command=command, args=args, env=env)
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            return await _session_list_tools(session)


async def list_tools_sse(cfg: Dict[str, Any]):
    if sse_client is None:
        raise RuntimeError("sse_client unavailable")
    url = _expand(cfg["url"])
    headers = {k: _expand(str(v)) for k, v in (cfg.get("headers") or {}).items()}
    async with sse_client(url, headers=headers) as (read, write):
        async with ClientSession(read, write) as session:
            return await _session_list_tools(session)


def to_announce(tools) -> List[Dict[str, Any]]:
    out = []
    for t in tools:
        out.append({
            "name": t.name,
            "description": (t.description or "")[:2000],
            "inputSchema": getattr(t, "inputSchema", None) or {},
        })
    return out


async def announce(backend_id: str, name: str, url: str, tools) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(
            f"{REGISTRY}/api/v1/tools/announce",
            json={
                "backend_id": backend_id,
                "name": name,
                "url": url,
                "capabilities": [],
                "tools": to_announce(tools),
            },
        )
        r.raise_for_status()
        return r.json()


# --- Static catalog ingestion (pre-extracted dsco MCP integrations) ---

# Maps TOOL_SCHEMAS.json server keys -> (backend_id, display name, service url).
DSCO_CATALOG_BACKENDS = {
    "shell_tools":     ("dsco-shell-tools", "Dsco Shell Tools", "http://localhost:8000"),
    "code_execution":  ("dsco-code-execution", "Dsco Code Execution Tools", "http://localhost:8001"),
    "orchestration":   ("dsco-orchestration", "Dsco Orchestration Server", "http://localhost:2040"),
    "analytics":       ("dsco-analytics", "Dsco Analytics Server", "http://localhost:8787"),
    "jina_ai":         ("dsco-jina-ai", "Dsco Jina AI", "http://localhost:2046"),
    "parallel_search": ("dsco-parallel-search", "Dsco Parallel Web Search", "http://localhost:8888"),
}


def load_dsco_catalog(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Build announce tool lists from a TOOL_SCHEMAS.json catalog.

    Each schema entry is a JSON Schema object (type/object, title, description,
    properties). We treat the whole object as the tool inputSchema.
    """
    data = json.loads(path.read_text())
    schemas = data.get("schemas") or {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for server_key, tools in schemas.items():
        if not isinstance(tools, dict):
            continue
        announce_tools: List[Dict[str, Any]] = []
        for tool_name, schema in tools.items():
            schema = schema if isinstance(schema, dict) else {}
            desc = schema.get("description") or schema.get("title") or tool_name
            announce_tools.append({
                "name": tool_name,
                "description": str(desc)[:2000],
                "inputSchema": schema,
            })
        out[server_key] = announce_tools
    return out


async def announce_raw(backend_id: str, name: str, url: str, tools: List[Dict[str, Any]]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(
            f"{REGISTRY}/api/v1/tools/announce",
            json={"backend_id": backend_id, "name": name, "url": url,
                  "capabilities": [], "tools": tools},
        )
        r.raise_for_status()
        return r.json()


async def ingest_dsco_catalog(path: Path) -> List[Dict[str, Any]]:
    catalog = load_dsco_catalog(path)
    results: List[Dict[str, Any]] = []
    for server_key, tools in catalog.items():
        backend_id, name, url = DSCO_CATALOG_BACKENDS.get(
            server_key, (_slug(server_key), server_key, f"catalog://{server_key}"))
        if not tools:
            results.append({"name": name, "transport": "catalog", "ok": True, "tools": 0, "registered": 0})
            continue
        try:
            res = await announce_raw(backend_id, name, url, tools)
        except Exception as e:
            results.append({"name": name, "transport": "catalog", "ok": False,
                            "tools": len(tools), "error": f"announce failed: {str(e)[:160]}"})
            continue
        results.append({"name": name, "transport": "catalog", "ok": True,
                        "tools": len(tools), "registered": res.get("registered", 0)})
    return results


async def ingest_one(name: str, cfg: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    backend_id = _slug(name)
    is_sse = bool(cfg.get("url"))
    transport = "sse" if is_sse else "stdio"
    url = _expand(cfg.get("url", "")) or f"stdio://{cfg.get('command','')}"
    try:
        lister = list_tools_sse(cfg) if is_sse else list_tools_stdio(cfg)
        tools = await asyncio.wait_for(lister, timeout=timeout)
    except Exception as e:
        return {"name": name, "transport": transport, "ok": False, "error": _flatten_err(e)[:240]}

    if not tools:
        return {"name": name, "transport": transport, "ok": True, "tools": 0, "registered": 0}
    try:
        res = await announce(backend_id, name, url, tools)
    except Exception as e:
        return {"name": name, "transport": transport, "ok": False, "tools": len(tools),
                "error": f"announce failed: {str(e)[:160]}"}
    return {"name": name, "transport": transport, "ok": True,
            "tools": len(tools), "registered": res.get("registered", 0)}


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", action="store_true", help="also ingest the intl_x_agent FastMCP suite")
    ap.add_argument("--suite-only", action="store_true", help="ingest ONLY the intl_x_agent FastMCP suite")
    ap.add_argument("--only", nargs="*", help="only ingest these server names")
    ap.add_argument("--timeout", type=float, default=45.0)
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--dsco-catalog", nargs="?", const=str(HOME / "Dsco/dsco-mcp-integrations/TOOL_SCHEMAS.json"),
                    help="ingest pre-extracted dsco MCP integrations from a TOOL_SCHEMAS.json catalog")
    ap.add_argument("--dsco-only", action="store_true", help="ingest ONLY the dsco catalog (implies --dsco-catalog)")
    args = ap.parse_args()

    catalog_results: List[Dict[str, Any]] = []
    if args.dsco_only or args.dsco_catalog:
        cat_path = Path(args.dsco_catalog or str(HOME / "Dsco/dsco-mcp-integrations/TOOL_SCHEMAS.json"))
        print(f"Registry: {REGISTRY}")
        print(f"Ingesting dsco catalog: {cat_path}\n")
        catalog_results = await ingest_dsco_catalog(cat_path)
        if args.dsco_only:
            _print_summary(catalog_results)
            return

    if args.suite_only:
        servers = load_suite()
    else:
        servers = load_claude_servers()
        if args.suite:
            servers.update(load_suite())
    if args.only:
        servers = {k: v for k, v in servers.items() if k in set(args.only)}

    print(f"Registry: {REGISTRY}")
    print(f"Discovered {len(servers)} MCP server(s) to ingest:\n")

    sem = asyncio.Semaphore(args.concurrency)

    async def guarded(n, c):
        async with sem:
            print(f"  -> connecting {n} ...", flush=True)
            return await ingest_one(n, c, args.timeout)

    results = await asyncio.gather(*(guarded(n, c) for n, c in servers.items()))
    _print_summary(list(results) + catalog_results)


def _print_summary(results: List[Dict[str, Any]]) -> None:
    print("\n=== INGESTION SUMMARY ===")
    total_tools = 0
    ok = 0
    for r in sorted(results, key=lambda x: (-x.get("registered", 0), x["name"])):
        if r["ok"]:
            ok += 1
            total_tools += r.get("registered", 0)
            print(f"  [ok]   {r['name']:34} {r['transport']:8} "
                  f"tools={r.get('tools',0):<4} registered={r.get('registered',0)}")
        else:
            print(f"  [FAIL] {r['name']:34} {r['transport']:8} {r.get('error','')}")
    print(f"\n{ok}/{len(results)} servers ingested, {total_tools} tools registered.")


if __name__ == "__main__":
    asyncio.run(main())
