"""Dynamic, fast, health-checked index of all distributed-* backends.

Purpose: the router must *always* know which backends are live so it can call
out to them immediately and fast-fail (never block on a cold/dead hop). This is
the fix for multi-hop retrieval stalls — a cold backend is skipped in
milliseconds instead of hanging on a 45s proxy timeout.

Substrate: a small SharedStore abstraction so the index + latency + a multi-hop
result cache are shared across every registry container.
  - Real Redis when REDIS_URL is set (drop in an Upstash rediss:// URL).
  - Modal's native modal.Dict otherwise (cross-container, no provisioning).
  - An in-process dict as a last resort so nothing ever crashes on store loss.

Everything here is defensive: a store or probe failure degrades to "assume
healthy" rather than taking the gateway down.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Health index freshness / probe tuning
HEALTH_INTERVAL_S = float(os.getenv("SERVICE_HEALTH_INTERVAL_S", "10"))
HEALTH_PROBE_TIMEOUT_S = float(os.getenv("SERVICE_HEALTH_TIMEOUT_S", "2.5"))
# A backend is considered live if seen healthy within this window even if a
# single probe is mid-flight, so transient blips don't flap the router.
HEALTH_STALE_S = float(os.getenv("SERVICE_HEALTH_STALE_S", "35"))

_INDEX_KEY = "svc:index"          # hash: backend_id -> json status
_CACHE_PREFIX = "svc:cache:"      # multi-hop result cache


# ─────────────────────────────── Shared store ───────────────────────────────

class _SharedStore:
    """Redis → modal.Dict → in-process dict, chosen once at first use."""

    def __init__(self) -> None:
        self._kind: Optional[str] = None
        self._redis = None
        self._mdict = None
        self._local: Dict[str, str] = {}

    def _ensure(self) -> None:
        if self._kind is not None:
            return
        url = os.getenv("REDIS_URL", "").strip()
        if url and not url.startswith("redis://redis:"):  # skip docker-internal
            try:
                import redis  # redis==5.2.0 ships in the image

                self._redis = redis.Redis.from_url(
                    url, socket_timeout=2, socket_connect_timeout=2,
                    decode_responses=True,
                )
                self._redis.ping()
                self._kind = "redis"
                logger.info("service_index: using Redis at %s", url.split("@")[-1])
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning("service_index: Redis unavailable (%s); falling back", exc)
        # In-process dict by default. modal.Dict was removed as the fallback:
        # its operations are BLOCKING network calls, and we were invoking them
        # from the async health loop and from is_healthy() on every tool
        # execute — which froze the event loop. With the registry pinned to a
        # single container (DuckDB single-writer), in-process state is correct
        # and never blocks. Cross-container sharing, if ever needed, goes through
        # Redis (REDIS_URL) above, not modal.Dict.
        self._kind = "local"

    def hset_json(self, key: str, field: str, value: Any) -> None:
        self._ensure()
        blob = json.dumps(value)
        try:
            if self._kind == "redis":
                self._redis.hset(key, field, blob)
            elif self._kind == "modal_dict":
                self._mdict[f"{key}::{field}"] = blob
            else:
                self._local[f"{key}::{field}"] = blob
        except Exception as exc:  # noqa: BLE001
            logger.debug("service_index hset failed: %s", exc)

    def hgetall_json(self, key: str) -> Dict[str, Any]:
        self._ensure()
        out: Dict[str, Any] = {}
        try:
            if self._kind == "redis":
                for f, v in (self._redis.hgetall(key) or {}).items():
                    out[f] = json.loads(v)
            elif self._kind == "modal_dict":
                prefix = f"{key}::"
                for k in list(self._mdict.keys()):
                    if isinstance(k, str) and k.startswith(prefix):
                        out[k[len(prefix):]] = json.loads(self._mdict[k])
            else:
                prefix = f"{key}::"
                for k, v in self._local.items():
                    if k.startswith(prefix):
                        out[k[len(prefix):]] = json.loads(v)
        except Exception as exc:  # noqa: BLE001
            logger.debug("service_index hgetall failed: %s", exc)
        return out

    def cache_get(self, key: str) -> Optional[Any]:
        self._ensure()
        full = _CACHE_PREFIX + key
        try:
            if self._kind == "redis":
                v = self._redis.get(full)
                return json.loads(v) if v else None
            store = self._mdict if self._kind == "modal_dict" else self._local
            entry = store.get(full)
            if not entry:
                return None
            exp, val = json.loads(entry)
            if exp and exp < time.time():
                return None
            return val
        except Exception:  # noqa: BLE001
            return None

    def cache_set(self, key: str, value: Any, ttl_s: int) -> None:
        self._ensure()
        full = _CACHE_PREFIX + key
        try:
            if self._kind == "redis":
                self._redis.set(full, json.dumps(value), ex=ttl_s)
            else:
                store = self._mdict if self._kind == "modal_dict" else self._local
                store[full] = json.dumps([time.time() + ttl_s, value])
        except Exception as exc:  # noqa: BLE001
            logger.debug("service_index cache_set failed: %s", exc)


_store = _SharedStore()


# ─────────────────────────────── Health index ───────────────────────────────

def _backends() -> List[Dict[str, str]]:
    """Resolve the set of backends to health-check from the running registry."""
    try:
        from ..app import BACKEND_REGISTRY  # populated at startup if present
        if BACKEND_REGISTRY:
            return list(BACKEND_REGISTRY)
    except Exception:  # noqa: BLE001
        pass
    # Fallback: the canonical distributed-* fleet.
    base = "https://arthurcolle--distributed-{}-web.modal.run"
    fleet = [
        "search", "research", "memory", "publish", "integration",
        "video-qa", "document-qa", "unit", "finance", "timeline",
    ]
    return [
        {"backend_id": f"distributed-{name}-mcp", "url": base.format(name)}
        for name in fleet
    ]


async def _probe_one(client, b: Dict[str, str]) -> Dict[str, Any]:
    url = b["url"].rstrip("/") + "/health"
    t0 = time.time()
    try:
        r = await client.get(url, timeout=HEALTH_PROBE_TIMEOUT_S)
        healthy = r.status_code < 500
        return {
            "backend_id": b["backend_id"],
            "url": b["url"],
            "healthy": healthy,
            "status": r.status_code,
            "latency_ms": round((time.time() - t0) * 1000, 1),
            "checked_at": time.time(),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "backend_id": b["backend_id"],
            "url": b["url"],
            "healthy": False,
            "status": 0,
            "error": str(exc)[:120],
            "latency_ms": round((time.time() - t0) * 1000, 1),
            "checked_at": time.time(),
        }


async def refresh_once() -> Dict[str, Any]:
    """Probe every backend concurrently and write results to the shared index."""
    import httpx

    backends = _backends()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*(_probe_one(client, b) for b in backends))
    for res in results:
        _store.hset_json(_INDEX_KEY, res["backend_id"], res)
    healthy = sum(1 for r in results if r["healthy"])
    logger.info("service_index: %d/%d backends healthy", healthy, len(results))
    return {"total": len(results), "healthy": healthy, "services": results}


async def health_loop() -> None:
    """Background task: keep the index fresh forever."""
    while True:
        try:
            await refresh_once()
        except Exception as exc:  # noqa: BLE001
            logger.warning("service_index health loop error: %s", exc)
        await asyncio.sleep(HEALTH_INTERVAL_S)


def get_services() -> List[Dict[str, Any]]:
    """Fast read of the current index (sorted, freshest-first)."""
    idx = _store.hgetall_json(_INDEX_KEY)
    return sorted(idx.values(), key=lambda s: s.get("backend_id", ""))


def is_healthy(backend_id: str) -> bool:
    """Router fast-fail check. Unknown/stale → assume healthy (fail-open) so a
    missing index never blocks traffic; only a *known-recent-unhealthy* backend
    is skipped."""
    idx = _store.hgetall_json(_INDEX_KEY)
    s = idx.get(backend_id)
    if not s:
        return True  # not yet indexed — don't block
    if time.time() - s.get("checked_at", 0) > HEALTH_STALE_S:
        return True  # stale reading — don't block on old data
    return bool(s.get("healthy", True))


# ─────────────────────────── Multi-hop result cache ─────────────────────────

def cached_result(cache_key: str):
    return _store.cache_get(cache_key)


def store_result(cache_key: str, value: Any, ttl_s: int = 120) -> None:
    _store.cache_set(cache_key, value, ttl_s)
