"""
Integration Fabric MCP

A universal integration layer: every SaaS / API category we care about is
exposed behind the standard native MCP protocol so it routes through the Tool
Management registry over HTTP. The surface is generated from a catalog of
(category x provider x operation) spanning communication, email, sms/voice,
calendar, crm, support, project mgmt, docs, storage, vcs, ci, cloud,
monitoring, incident, ecommerce, maps, social, analytics, hr, esign, video,
ai, search, weather, forms, translation, and knowledge.

Operations are classified into a small set of CRUD-ish *kinds*
(create/read/list/update/delete/action/search). A single generic handler
shapes a deterministic, kind-appropriate response for any operation, backed by
a SQLite object store so create/read/update/delete/list stay coherent within a
session. Providers run in SANDBOX by default; a provider with credentials +
a live adapter can override. Every tool reports its mode.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase
from native.integration_fabric.catalog import (
    build_tools, build_canonical, CATEGORY_OPS, CATEGORY_PROVIDERS,
)
from native.integration_fabric import tool_space

try:
    from tool_management.core.credential_envelope import (
        CredentialEnvelopeError,
        open_credential_envelope,
    )
except Exception:  # noqa: BLE001
    CredentialEnvelopeError = ValueError  # type: ignore[misc,assignment]
    open_credential_envelope = None  # type: ignore[assignment]

try:
    # Optional: multi-tenant BYOK credential resolution. Import is best-effort
    # so this fabric still runs standalone/offline without the tool_management
    # package on the path (e.g. isolated test harnesses). When unavailable,
    # every call falls back to today's pure-env-var behavior, unchanged.
    from tool_management.services.tenant_credentials import resolve_credential
except Exception:  # noqa: BLE001
    def resolve_credential(tenant_id, provider, env_fallback_var=None, allow_pooled_fallback=False):  # type: ignore
        return os.getenv(env_fallback_var) if (allow_pooled_fallback and env_fallback_var) else None

# Categories where pooling the operator's own key for tenants without their
# own BYOK credential is acceptable: cheap, read-only, low abuse surface.
# See TENANT_CREDENTIALS_WIRING.md for the full tiering rationale. Anything
# not listed here must NEVER resolve through allow_pooled_fallback=True.
POOLABLE_CATEGORIES = {"weather", "econ"}


REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:2016").rstrip("/")

# Live-adapter credentials. When present, the matching (provider, category)
# makes a real outbound API call instead of returning a sandbox object.
OPENWEATHER_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GROQ_KEY = os.getenv("GROQ_API_KEY", "")
XAI_KEY = os.getenv("XAI_API_KEY", "")
DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY", "")
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
TOGETHER_KEY = os.getenv("TOGETHER_API_KEY", "")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
ALPHAVANTAGE_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
JINA_KEY = os.getenv("JINA_API_KEY", "")
HF_TOKEN = os.getenv("HF_TOKEN", "")
FRED_KEY = os.getenv("FRED_API_KEY", "")

# Self-hosted media stack: provider -> (env_url_var, env_key_var, api_path_prefix).
MEDIA_PROVIDERS = {
    "sonarr": ("SONARR_URL", "SONARR_API_KEY", "/api/v3"),
    "radarr": ("RADARR_URL", "RADARR_API_KEY", "/api/v3"),
    "lidarr": ("LIDARR_URL", "LIDARR_API_KEY", "/api/v1"),
    "prowlarr": ("PROWLARR_URL", "PROWLARR_API_KEY", "/api/v1"),
    "bazarr": ("BAZARR_URL", "BAZARR_API_KEY", "/api"),
    "sabnzbd": ("SABNZBD_URL", "SABNZBD_API_KEY", "/api"),
    "overseerr": ("OVERSEERR_URL", "OVERSEERR_API_KEY", "/api/v1"),
    "nzbhydra2": ("NZBHYDRA2_URL", "NZBHYDRA2_API_KEY", "/api"),
}

# provider -> (base_url, default_model, api_key) for OpenAI-compatible chat APIs.
OPENAI_COMPAT = {
    "openai": ("https://api.openai.com/v1", "gpt-4o-mini", OPENAI_KEY),
    "groq": ("https://api.groq.com/openai/v1", "llama-3.3-70b-versatile", GROQ_KEY),
    "xai": ("https://api.x.ai/v1", "grok-3-mini", XAI_KEY),
    "deepseek": ("https://api.deepseek.com/v1", "deepseek-chat", DEEPSEEK_KEY),
    "openrouter": ("https://openrouter.ai/api/v1", "openai/gpt-4o-mini", OPENROUTER_KEY),
    "together": ("https://api.together.xyz/v1", "meta-llama/Llama-3.3-70B-Instruct-Turbo", TOGETHER_KEY),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hid(*parts: Any) -> str:
    h = hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()
    return h[:12]


def _seed(*parts: Any) -> int:
    return int(hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest(), 16)


class ObjectStore:
    """SQLite-backed generic resource store for sandbox coherence."""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS objects (
                category TEXT, provider TEXT, id TEXT, op TEXT,
                data TEXT, created_at TEXT, updated_at TEXT,
                PRIMARY KEY (category, id)
            )"""
        )
        self.db.commit()

    def create(self, category: str, provider: str, op: str, oid: str, data: Dict[str, Any]) -> Dict[str, Any]:
        now = _now()
        body = json.dumps(data, default=str)
        self.db.execute(
            "INSERT OR REPLACE INTO objects VALUES (?,?,?,?,?,?,?)",
            (category, provider, oid, op, body, now, now),
        )
        self.db.commit()
        return {"id": oid, **data, "created_at": now}

    def get(self, category: str, oid: str) -> Optional[Dict[str, Any]]:
        row = self.db.execute(
            "SELECT * FROM objects WHERE category=? AND id=?", (category, oid)
        ).fetchone()
        if not row:
            return None
        return {"id": row["id"], **json.loads(row["data"]),
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def update(self, category: str, oid: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cur = self.get(category, oid)
        if not cur:
            return None
        merged = {k: v for k, v in cur.items() if k not in ("id", "created_at", "updated_at")}
        merged.update(data)
        now = _now()
        self.db.execute(
            "UPDATE objects SET data=?, updated_at=? WHERE category=? AND id=?",
            (json.dumps(merged, default=str), now, category, oid),
        )
        self.db.commit()
        return {"id": oid, **merged, "updated_at": now}

    def delete(self, category: str, oid: str) -> bool:
        cur = self.db.execute("DELETE FROM objects WHERE category=? AND id=?", (category, oid))
        self.db.commit()
        return cur.rowcount > 0

    def list(self, category: str, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.db.execute(
            "SELECT * FROM objects WHERE category=? ORDER BY created_at DESC LIMIT ?",
            (category, limit),
        ).fetchall()
        return [{"id": r["id"], **json.loads(r["data"]), "created_at": r["created_at"]} for r in rows]


class IntegrationFabricMCP(NativeMCPBase):
    SERVICE_ID = "distributed-integration"
    SERVICE_NAME = "Distributed Integration MCP"
    VERSION = "1.0.0"

    CAPABILITIES = sorted(set(list(CATEGORY_OPS.keys()) + ["integration", "saas", "api"]))

    _PRIM_TOOLS, _PRIM_DISPATCH = build_tools()
    _CANON_TOOLS, _CANON_DISPATCH = build_canonical()
    _DISPATCH = {**_PRIM_DISPATCH, **_CANON_DISPATCH}

    TOOLS = [
        {"name": "provider_status",
         "description": "Show integration categories and their providers, plus catalog totals.",
         "inputSchema": {"type": "object", "properties": {}}},
        {"name": "catalog_summary",
         "description": "Summarize the integration catalog: categories, ops, providers, totals.",
         "inputSchema": {"type": "object", "properties": {}}},
        {"name": "list_categories",
         "description": "List every integration category with its operations.",
         "inputSchema": {"type": "object", "properties": {}}},
        {"name": "probe_live",
         "description": "Ping every provider that has credentials/a live adapter and report "
                        "which are LIVE, DEAD (auth/credit error), or CONFIGURED-but-unreachable.",
         "inputSchema": {"type": "object", "properties": {}}},
        {"name": "toolspace_cardinality",
         "description": "Report the size of the combinatorial tool address space: |U| canonical "
                        "ops and |U|^d composed pipelines for depths 1..max_depth (billions by depth 4).",
         "inputSchema": {"type": "object",
                         "properties": {"max_depth": {"type": "integer", "default": 5}}}},
        {"name": "toolspace_sample",
         "description": "Deterministically sample n pipeline addresses from the depth-d tool space.",
         "inputSchema": {"type": "object",
                         "properties": {"n": {"type": "integer", "default": 5},
                                        "depth": {"type": "integer", "default": 3},
                                        "seed": {"type": "integer", "default": 0}}}},
        {"name": "toolspace_describe",
         "description": "Decode a tool-space address: resolve an integer index (and depth) into its "
                        "concrete canonical-op pipeline, or encode an explicit op list into its index.",
         "inputSchema": {"type": "object",
                         "properties": {"index": {"type": "integer"},
                                        "depth": {"type": "integer", "default": 3},
                                        "ops": {"type": "array", "items": {"type": "string"}}}}},
        {"name": "pipe_exec",
         "description": "Materialize and execute an addressed pipeline end-to-end through the registry, "
                        "auto-piping each step's output into the next. Supply an integer index+depth or "
                        "an explicit list of canonical ops, plus optional initial inputs.",
         "inputSchema": {"type": "object",
                         "properties": {"index": {"type": "integer"},
                                        "depth": {"type": "integer", "default": 3},
                                        "ops": {"type": "array", "items": {"type": "string"}},
                                        "inputs": {"type": "object"}}}},
    ] + _CANON_TOOLS + _PRIM_TOOLS

    def __init__(self, port: int, **kwargs):
        super().__init__(port, **kwargs)
        self.store = ObjectStore(self.data_dir / "integration_fabric.sqlite")

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _ok(payload: Dict[str, Any], mode: str, provider: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"mode": mode}
        if provider:
            body["provider"] = provider
        body.update(payload)
        return {"content": [{"type": "text", "text": json.dumps(body, indent=2, default=str)}]}

    @staticmethod
    def _err(msg: str) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps({"error": msg})}], "isError": True}

    # --------------------------------------------------------------- dispatch
    async def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        route = self._DISPATCH.get(tool_name)
        if route is None:
            return await super()._execute_tool(tool_name, arguments)
        category, op, kind, provider = route
        args = dict(arguments or {})
        # Tenant BYOK secrets arrive only as authenticated, short-lived
        # envelopes. Raw credentials and raw tenant selectors are rejected so
        # a public MCP caller cannot query another tenant's vault.
        if "_tenant_credential" in args or "tenant_id" in args:
            return self._err("raw tenant credentials or tenant_id are not accepted")
        envelope = args.pop("_credential_envelope", None)
        tenant_id = None
        passed_credential = None
        if envelope:
            if open_credential_envelope is None:
                return self._err("credential envelope support is unavailable")
            try:
                opened = open_credential_envelope(
                    str(envelope),
                    expected_backend_id="integration-fabric",
                    expected_provider=provider,
                )
                tenant_id = opened["tenant_id"]
                passed_credential = opened["credential"]
            except CredentialEnvelopeError as exc:
                return self._err(f"credential envelope rejected: {exc}")
        # Set by the registry when the caller pays for the managed add-on —
        # only then may a tenant ride the operator's pooled env keys. Callers
        # with no tenant (operator/static tokens) are unrestricted as before.
        pooled_ok = bool(args.pop("_pooled_ok", False))
        try:
            if passed_credential or pooled_ok or tenant_id is None:
                live = await self._live(category, op, provider, args, tenant_id, passed_credential)
                if live is not None:
                    return live
            return self._handle(category, op, kind, provider, args)
        except Exception as e:  # noqa: BLE001
            return self._err(f"{provider}.{category}.{op} failed: {e}")

    # ---------------------------------------------------------- live adapters
    async def _live(self, category: str, op: str, provider: str,
                    args: Dict[str, Any],
                    tenant_id: Optional[str] = None,
                    passed_credential: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Make a real API call when (provider, category) has creds + an adapter.

        Returns a tool response dict on a live call, or None to fall through to
        the deterministic sandbox handler.

        Credential resolution order per provider: tenant's own BYOK key first
        (via resolve_credential), then the operator's env-var key ONLY for
        categories in POOLABLE_CATEGORIES. Providers not yet migrated to
        resolve_credential still use the plain module-level env constant
        (e.g. ANTHROPIC_KEY) -- unchanged pending the same conversion; see
        TENANT_CREDENTIALS_WIRING.md for the remaining migration list.
        """
        if category == "weather" and provider == "openweather":
            key = passed_credential or resolve_credential(
                tenant_id, "openweather",
                env_fallback_var="OPENWEATHERMAP_API_KEY",
                allow_pooled_fallback=category in POOLABLE_CATEGORIES,
            )
            if key:
                return await self._live_openweather(op, args, key)
        if category == "ai" and op in ("chat", "complete"):
            if provider == "anthropic" and (passed_credential or ANTHROPIC_KEY):
                return await self._live_anthropic_chat(args, api_key=passed_credential)
            base = OPENAI_COMPAT.get(provider)
            if base and (passed_credential or base[2]):
                return await self._live_openai_chat(args, provider, api_key=passed_credential)
        if category == "ai" and op == "image_generate" and provider == "openrouter":
            return await self._live_openrouter_image(args, api_key=passed_credential)
        if category == "ai" and op == "embed":
            if provider == "openai" and (passed_credential or OPENAI_KEY):
                return await self._live_openai_embed(args, api_key=passed_credential)
            if provider == "jina" and (passed_credential or JINA_KEY):
                return await self._live_jina_embed(args, api_key=passed_credential)
        if category == "ai" and op in ("chat", "complete") and provider == "huggingface" and (passed_credential or HF_TOKEN):
            return await self._live_hf_chat(args, api_key=passed_credential)
        if category == "vcs" and provider == "github" and (passed_credential or GITHUB_TOKEN):
            return await self._live_github(op, args, token=passed_credential)
        if category == "econ" and provider == "fred" and (passed_credential or FRED_KEY):
            return await self._live_fred(op, args, api_key=passed_credential)
        if category == "media" and provider in MEDIA_PROVIDERS:
            return await self._live_media(op, provider, args)
        return None

    async def _live_jina_embed(self, args: Dict[str, Any],
                               api_key: Optional[str] = None) -> Dict[str, Any]:
        params = dict(args.get("params") or {})
        text = params.get("input") or params.get("text") or str(args.get("id") or "hello")
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.jina.ai/v1/embeddings",
                             headers={"Authorization": f"Bearer {api_key or JINA_KEY}"},
                             json={"model": params.get("model", "jina-embeddings-v3"),
                                   "input": [text]})
        r.raise_for_status()
        d = r.json()
        vec = d["data"][0]["embedding"]
        return self._ok({"category": "ai", "operation": "embed", "model": d.get("model"),
                         "dimensions": len(vec), "preview": vec[:5], "usage": d.get("usage")},
                        "live", "jina")

    async def _live_hf_chat(self, args: Dict[str, Any],
                            api_key: Optional[str] = None) -> Dict[str, Any]:
        params = dict(args.get("params") or {})
        prompt = params.get("prompt") or str(args.get("id") or "Say hello.")
        model = params.get("model", "meta-llama/Llama-3.1-8B-Instruct")
        async with httpx.AsyncClient(timeout=60.0) as c:
            r = await c.post("https://router.huggingface.co/v1/chat/completions",
                             headers={"Authorization": f"Bearer {api_key or HF_TOKEN}"},
                             json={"model": model, "max_tokens": int(params.get("max_tokens", 128)),
                                   "messages": [{"role": "user", "content": prompt}]})
        r.raise_for_status()
        d = r.json()
        return self._ok({"category": "ai", "operation": "chat", "model": d.get("model"),
                         "text": d["choices"][0]["message"]["content"], "usage": d.get("usage")},
                        "live", "huggingface")

    async def _live_fred(self, op: str, args: Dict[str, Any],
                         api_key: Optional[str] = None) -> Dict[str, Any]:
        base = "https://api.stlouisfed.org/fred"
        common = {"api_key": api_key or FRED_KEY, "file_type": "json"}
        async with httpx.AsyncClient(timeout=20.0) as c:
            if op in ("observations", "series"):
                sid = str(args.get("id") or (args.get("query") or "GDP")).upper()
                r = await c.get(f"{base}/series/observations",
                                params={**common, "series_id": sid, "sort_order": "desc",
                                        "limit": int(args.get("limit", 5) or 5)})
                r.raise_for_status()
                obs = r.json().get("observations", [])
                payload = {"series_id": sid, "observations": obs}
                key = "object" if op == "series" else "items"
                return self._ok({"category": "econ", "operation": op,
                                 key: obs if op != "series" else payload,
                                 **({"count": len(obs)} if op != "series" else {})},
                                "live", "fred")
            if op == "search":
                r = await c.get(f"{base}/series/search",
                                params={**common, "search_text": str(args.get("query", "")),
                                        "limit": int(args.get("limit", 5) or 5)})
                r.raise_for_status()
                series = [{"id": s["id"], "title": s["title"], "frequency": s.get("frequency"),
                           "units": s.get("units")} for s in r.json().get("seriess", [])]
                return self._ok({"category": "econ", "operation": op, "query": args.get("query"),
                                 "results": series, "count": len(series)}, "live", "fred")
            if op == "releases":
                r = await c.get(f"{base}/releases", params={**common, "limit": int(args.get("limit", 5) or 5)})
                r.raise_for_status()
                rel = [{"id": x["id"], "name": x["name"]} for x in r.json().get("releases", [])]
                return self._ok({"category": "econ", "operation": op, "items": rel,
                                 "count": len(rel)}, "live", "fred")
        return self._handle("econ", op, "list", "fred", args)

    async def _live_media(self, op: str, provider: str, args: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url_var, key_var, prefix = MEDIA_PROVIDERS[provider]
        base = os.getenv(url_var, "").rstrip("/")
        key = os.getenv(key_var, "")
        if not base or not key:
            return None  # not configured -> sandbox
        ep = {"system_status": "/system/status", "list_queue": "/queue",
              "list_history": "/history", "list_items": "/series" if provider == "sonarr" else "/movie"}.get(op)
        if ep is None:
            return None
        headers = {"X-Api-Key": key}
        try:
            async with httpx.AsyncClient(timeout=5.0, headers=headers) as c:
                r = await c.get(f"{base}{prefix}{ep}")
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            return self._ok({"category": "media", "operation": op, "reachable": False,
                             "endpoint": f"{base}{prefix}{ep}", "note": str(e)[:120]},
                            "configured", provider)
        body = r.json()
        items = body if isinstance(body, list) else [body]
        return self._ok({"category": "media", "operation": op, "count": len(items),
                         "items": items[:10]}, "live", provider)

    @staticmethod
    def _openweather_location(args: Dict[str, Any]) -> str:
        """Translate a friendly location into OpenWeather's city query format.

        OpenWeather does not resolve the common US ``City, ST`` form by itself:
        ``Washington, DC`` returns 404, while ``Washington, DC, US`` resolves.
        Keep already country-qualified locations intact and avoid guessing for
        non-US / non-state input.
        """
        raw = str(args.get("id") or (args.get("params") or {}).get("q") or "London").strip()
        parts = [part.strip() for part in raw.split(",")]
        if len(parts) == 2 and len(parts[1]) == 2 and parts[1].upper() in {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "IA",
            "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
            "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
            "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI",
            "WV", "WY", "DC",
        }:
            return f"{parts[0]}, {parts[1].upper()}, US"
        return raw

    async def _live_openweather(self, op: str, args: Dict[str, Any],
                                api_key: Optional[str] = None) -> Dict[str, Any]:
        loc = self._openweather_location(args)
        path = "forecast" if op == "forecast" else "weather"
        key = api_key or OPENWEATHER_KEY  # fallback preserves any other direct caller
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.get(f"https://api.openweathermap.org/data/2.5/{path}",
                            params={"q": loc, "appid": key, "units": "metric"})
        r.raise_for_status()
        d = r.json()
        if path == "weather":
            obj = {"location": d.get("name"), "temp_c": d.get("main", {}).get("temp"),
                   "humidity": d.get("main", {}).get("humidity"),
                   "conditions": (d.get("weather") or [{}])[0].get("description"),
                   "wind_mps": d.get("wind", {}).get("speed")}
        else:
            obj = {"location": d.get("city", {}).get("name"),
                   "entries": [{"dt": e.get("dt_txt"), "temp_c": e.get("main", {}).get("temp"),
                                "conditions": (e.get("weather") or [{}])[0].get("description")}
                               for e in (d.get("list") or [])[:5]]}
        return self._ok({"category": "weather", "operation": op, "object": obj}, "live", "openweather")

    async def _live_openai_chat(self, args: Dict[str, Any], provider: str,
                                api_key: Optional[str] = None) -> Dict[str, Any]:
        params = dict(args.get("params") or {})
        prompt = params.get("prompt") or params.get("input") or str(args.get("id") or "Say hello.")
        base, model, key = OPENAI_COMPAT[provider]
        key = api_key or key
        model = params.get("model", model)
        async with httpx.AsyncClient(timeout=40.0) as c:
            r = await c.post(f"{base}/chat/completions",
                             headers={"Authorization": f"Bearer {key}"},
                             json={"model": model, "max_tokens": int(params.get("max_tokens", 128)),
                                   "messages": [{"role": "user", "content": prompt}]})
        r.raise_for_status()
        d = r.json()
        return self._ok({"category": "ai", "operation": "chat", "model": d.get("model"),
                         "text": d["choices"][0]["message"]["content"],
                         "usage": d.get("usage")}, "live", provider)

    async def _live_openrouter_image(self, args: Dict[str, Any],
                                     api_key: Optional[str] = None) -> Dict[str, Any]:
        """Generate actual images through OpenRouter's dedicated Images API.

        Preserve model pins and provider routing. Never report a sandbox object
        as an image, and return standard MCP image blocks without duplicating
        the base64 payload inside the metadata text.
        """
        key = api_key or OPENROUTER_KEY
        if not key:
            return self._err("OPENROUTER_API_KEY is not configured")
        params = dict(args.get("params") or {})
        prompt = params.get("prompt") or params.get("input") or args.get("id")
        if not isinstance(prompt, str) or not prompt.strip():
            return self._err("image_generate requires a non-empty params.prompt")
        count = params.get("n", 1)
        if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 10:
            return self._err("params.n must be an integer between 1 and 10")
        payload = {
            "model": params.get("model", "openai/gpt-image-2"),
            "prompt": prompt,
            "n": count,
        }
        for name in ("size", "resolution", "aspect_ratio", "quality", "output_format",
                     "background", "output_compression", "input_references", "provider", "seed"):
            if name in params:
                payload[name] = params[name]
        # Collected MCP response; generation can take 90+ seconds. Never retry
        # a billable POST here because its completion may be ambiguous.
        async with httpx.AsyncClient(timeout=httpx.Timeout(240.0, connect=15.0)) as c:
            r = await c.post(
                "https://openrouter.ai/api/v1/images",
                headers={"Authorization": f"Bearer {key}"},
                json=payload,
            )
        r.raise_for_status()
        data = r.json()
        images = data.get("data")
        if not isinstance(images, list) or not images:
            return self._err("OpenRouter returned no generated images")
        blocks = []
        for image in images:
            if not isinstance(image, dict) or not isinstance(image.get("b64_json"), str) or not image["b64_json"]:
                return self._err("OpenRouter returned an image without base64 data")
            blocks.append({"type": "image", "data": image["b64_json"],
                           "mimeType": image.get("media_type") or "image/png"})
        result = self._ok({
            "category": "ai", "operation": "image_generate", "model": payload["model"],
            "created": data.get("created"), "image_count": len(blocks),
            "usage": data.get("usage"),
        }, "live", "openrouter")
        result["content"].extend(blocks)
        return result

    async def _live_anthropic_chat(self, args: Dict[str, Any],
                                   api_key: Optional[str] = None) -> Dict[str, Any]:
        params = dict(args.get("params") or {})
        prompt = params.get("prompt") or params.get("input") or str(args.get("id") or "Say hello.")
        model = params.get("model", "claude-haiku-4-5-20251001")
        async with httpx.AsyncClient(timeout=40.0) as c:
            r = await c.post("https://api.anthropic.com/v1/messages",
                             headers={"x-api-key": api_key or ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                                      "content-type": "application/json"},
                             json={"model": model, "max_tokens": int(params.get("max_tokens", 128)),
                                   "messages": [{"role": "user", "content": prompt}]})
        r.raise_for_status()
        d = r.json()
        return self._ok({"category": "ai", "operation": "chat", "model": d.get("model"),
                         "text": (d.get("content") or [{}])[0].get("text"),
                         "usage": d.get("usage")}, "live", "anthropic")

    async def _live_openai_embed(self, args: Dict[str, Any],
                                 api_key: Optional[str] = None) -> Dict[str, Any]:
        params = dict(args.get("params") or {})
        text = params.get("input") or params.get("text") or str(args.get("id") or "hello")
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post("https://api.openai.com/v1/embeddings",
                             headers={"Authorization": f"Bearer {api_key or OPENAI_KEY}"},
                             json={"model": params.get("model", "text-embedding-3-small"), "input": text})
        r.raise_for_status()
        d = r.json()
        vec = d["data"][0]["embedding"]
        return self._ok({"category": "ai", "operation": "embed", "model": d.get("model"),
                         "dimensions": len(vec), "preview": vec[:5], "usage": d.get("usage")},
                        "live", "openai")

    async def _live_github(self, op: str, args: Dict[str, Any],
                           token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Bearer {token or GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
        async with httpx.AsyncClient(timeout=20.0, headers=headers) as c:
            if op == "list_repos":
                r = await c.get("https://api.github.com/user/repos",
                                params={"per_page": int(args.get("limit", 10) or 10), "sort": "updated"})
                r.raise_for_status()
                items = [{"full_name": x["full_name"], "private": x["private"],
                          "stars": x["stargazers_count"], "lang": x.get("language")} for x in r.json()]
                return self._ok({"category": "vcs", "operation": op, "items": items,
                                 "count": len(items)}, "live", "github")
            if op == "get_repo":
                full = str(args.get("id", ""))
                r = await c.get(f"https://api.github.com/repos/{full}")
                r.raise_for_status()
                x = r.json()
                return self._ok({"category": "vcs", "operation": op,
                                 "object": {"full_name": x["full_name"], "stars": x["stargazers_count"],
                                            "forks": x["forks_count"], "open_issues": x["open_issues_count"],
                                            "lang": x.get("language")}}, "live", "github")
        return None  # other vcs ops fall back to sandbox

    def _handle(self, category: str, op: str, kind: str, provider: str,
                args: Dict[str, Any]) -> Dict[str, Any]:
        mode = "sandbox"

        if kind == "create":
            data = dict(args.get("data") or {})
            oid = f"{category[:3]}_{_hid(provider, category, op, json.dumps(data, sort_keys=True, default=str), args.get('idempotency_key', _now()))}"
            obj = self.store.create(category, provider, op, oid, data)
            return self._ok({"category": category, "operation": op, "object": obj, "status": "created"},
                            mode, provider)

        if kind == "read":
            oid = str(args.get("id", ""))
            obj = self.store.get(category, oid) if oid else None
            if obj is None:
                # deterministic synthetic resource so reads always resolve
                obj = self._synth(category, op, provider, oid or _hid(category, op))
            return self._ok({"category": category, "operation": op, "object": obj}, mode, provider)

        if kind == "list":
            limit = int(args.get("limit", 20) or 20)
            items = self.store.list(category, limit)
            if not items:
                items = [self._synth(category, op, provider, _hid(category, op, i)) for i in range(min(limit, 3))]
            return self._ok({"category": category, "operation": op, "items": items,
                             "count": len(items), "cursor": None}, mode, provider)

        if kind == "update":
            oid = str(args.get("id", ""))
            data = dict(args.get("data") or {})
            obj = self.store.update(category, oid, data)
            if obj is None:
                obj = self.store.create(category, provider, op, oid or f"{category[:3]}_{_hid(category, op)}", data)
            return self._ok({"category": category, "operation": op, "object": obj, "status": "updated"},
                            mode, provider)

        if kind == "delete":
            oid = str(args.get("id", ""))
            existed = self.store.delete(category, oid) if oid else False
            return self._ok({"category": category, "operation": op, "id": oid,
                             "status": "deleted" if existed else "not_found"}, mode, provider)

        if kind == "action":
            oid = args.get("id")
            params = dict(args.get("params") or {})
            action_id = f"act_{_hid(provider, category, op, oid, json.dumps(params, sort_keys=True, default=str), _now())}"
            return self._ok({"category": category, "operation": op, "target": oid,
                             "action_id": action_id, "params": params, "status": "accepted"},
                            mode, provider)

        if kind == "search":
            query = str(args.get("query", ""))
            limit = int(args.get("limit", 10) or 10)
            results = [
                self._synth(category, op, provider, _hid(category, query, i), query=query, rank=i + 1)
                for i in range(min(limit, 5))
            ]
            return self._ok({"category": category, "operation": op, "query": query,
                             "results": results, "count": len(results)}, mode, provider)

        return self._err(f"unknown kind {kind}")

    def _synth(self, category: str, op: str, provider: str, oid: str, **extra: Any) -> Dict[str, Any]:
        s = _seed(category, op, provider, oid)
        obj = {
            "id": f"{category[:3]}_{oid}",
            "provider": provider,
            "name": f"{category}-{oid}",
            "synthetic": True,
            "score": round((s % 1000) / 1000.0, 3),
        }
        obj.update(extra)
        return obj

    # ----------------------------------------------------------- standalone
    async def _tool_provider_status(self, args):
        return self._ok({
            "categories": {c: CATEGORY_PROVIDERS[c] for c in CATEGORY_PROVIDERS},
            "tool_count": len(self.TOOLS),
            "registry": REGISTRY_URL,
        }, "info")

    async def _tool_catalog_summary(self, args):
        by_cat = {}
        for _n, (cat, _op, _k, _p) in self._PRIM_DISPATCH.items():
            by_cat[cat] = by_cat.get(cat, 0) + 1
        return self._ok({
            "total_tools": len(self.TOOLS),
            "primitives": len(self._PRIM_TOOLS),
            "canonical_ops": len(self._CANON_TOOLS),
            "categories": len(CATEGORY_OPS),
            "providers": sum(len(p) for p in CATEGORY_PROVIDERS.values()),
            "layers": {
                "L0_primitive": "{provider}_{category}_{op}",
                "L1_operation": "{category}__{op} (default provider)",
            },
            "primitives_per_category": by_cat,
        }, "info")

    async def _tool_probe_live(self, args):
        """Exercise one representative live call per credentialed provider."""
        probes: List[tuple] = []
        for p in ("openai", "groq", "xai", "deepseek", "openrouter", "together"):
            if OPENAI_COMPAT.get(p, (None, None, ""))[2]:
                probes.append((p, "ai", "chat", {"params": {"prompt": "ping", "max_tokens": 5}}))
        if ANTHROPIC_KEY:
            probes.append(("anthropic", "ai", "chat", {"params": {"prompt": "ping", "max_tokens": 5}}))
        if HF_TOKEN:
            probes.append(("huggingface", "ai", "chat", {"params": {"prompt": "ping", "max_tokens": 5}}))
        if OPENAI_KEY:
            probes.append(("openai", "ai", "embed", {"params": {"input": "ping"}}))
        if JINA_KEY:
            probes.append(("jina", "ai", "embed", {"params": {"input": "ping"}}))
        if OPENWEATHER_KEY:
            probes.append(("openweather", "weather", "current", {"id": "London"}))
        if FRED_KEY:
            probes.append(("fred", "econ", "observations", {"id": "GDP", "limit": 1}))
        if GITHUB_TOKEN:
            probes.append(("github", "vcs", "list_repos", {"limit": 1}))
        for prov in MEDIA_PROVIDERS:
            if os.getenv(MEDIA_PROVIDERS[prov][0]) and os.getenv(MEDIA_PROVIDERS[prov][1]):
                probes.append((prov, "media", "system_status", {}))

        results = []
        for provider, category, op, a in probes:
            try:
                res = await self._live(category, op, provider, a)
                if res is None:
                    results.append({"provider": provider, "category": category, "status": "no_adapter"})
                    continue
                body = json.loads(res["content"][0]["text"])
                err = body.get("error") or res.get("isError")
                results.append({"provider": provider, "category": category,
                                "status": body.get("mode", "error") if not err else "dead",
                                "detail": (body.get("error") or "")[:80] if err else "ok"})
            except Exception as e:  # noqa: BLE001
                msg = str(e)
                status = "dead" if any(x in msg for x in ("401", "402", "403")) else "error"
                results.append({"provider": provider, "category": category,
                                "status": status, "detail": msg[:80]})
        live = [r["provider"] for r in results if r["status"] == "live"]
        return self._ok({"probed": len(results), "live": live,
                         "live_count": len(live), "results": results}, "info")

    async def _tool_list_categories(self, args):
        return self._ok({
            "categories": {
                cat: {"operations": [{"op": op, "kind": kind} for op, kind in ops],
                      "providers": CATEGORY_PROVIDERS.get(cat, [])}
                for cat, ops in CATEGORY_OPS.items()
            }
        }, "info")

    # ----------------------------------------------------- combinatorial space
    async def _tool_toolspace_cardinality(self, args):
        max_depth = int(args.get("max_depth", 5) or 5)
        u = tool_space.U
        per_depth = {str(d): u ** d for d in range(1, max_depth + 1)}
        first_billion = next((d for d in range(1, max_depth + 1) if u ** d >= 1_000_000_000), None)
        return self._ok({
            "universe_size": u,
            "fabrics": ["integration-fabric", "finance-fabric"],
            "per_depth": per_depth,
            "total_upto": tool_space.total_upto(max_depth),
            "first_depth_over_1e9": first_billion,
        }, "info")

    async def _tool_toolspace_sample(self, args):
        n = int(args.get("n", 5) or 5)
        depth = int(args.get("depth", 3) or 3)
        seed = int(args.get("seed", 0) or 0)
        return self._ok({
            "depth": depth,
            "universe_size": tool_space.U,
            "cardinality": tool_space.cardinality(depth),
            "samples": tool_space.sample(n, depth, seed),
        }, "info")

    async def _tool_toolspace_describe(self, args):
        ops = args.get("ops")
        if ops:
            ops = [tool_space._qualify_short(o) for o in ops]
            idx = tool_space.pipeline_to_index(ops)
            return self._ok({"ops": ops, "address": tool_space.address(ops),
                             "index": idx, "depth": len(ops)}, "info")
        if args.get("index") is None:
            return self._err("supply either 'ops' or 'index'+'depth'")
        depth = int(args.get("depth", 3) or 3)
        idx = int(args["index"])
        ops = tool_space.index_to_pipeline(idx, depth)
        return self._ok({"index": idx, "depth": depth, "ops": ops,
                         "address": tool_space.address(ops)}, "info")

    async def _tool_pipe_exec(self, args):
        ops = args.get("ops")
        if not ops:
            if args.get("index") is None:
                return self._err("supply either 'ops' or 'index'+'depth'")
            depth = int(args.get("depth", 3) or 3)
            ops = tool_space.index_to_pipeline(int(args["index"]), depth)
        result = await tool_space.PipelineExecutor(REGISTRY_URL).run(
            list(ops), dict(args.get("inputs") or {}))
        return self._ok(result, "compose")


def create_server(port: Optional[int] = None) -> IntegrationFabricMCP:
    port = port or int(os.environ.get("PORT", "4010"))
    return IntegrationFabricMCP(port=port)


# Module-level ASGI app for Modal @asgi_app import (factory target)
app = create_server().app


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Integration Fabric MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4010")))
    args = parser.parse_args()
    create_server(args.port).run()
