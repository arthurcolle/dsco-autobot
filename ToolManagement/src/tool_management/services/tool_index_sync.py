"""Full-catalog faceted-index sync + refresh heartbeat.

The catalog (DuckDB) holds all ~2,500 tools across every backend, but the
multi-faceted *semantic* index (advanced_tool_index) is what makes a tool
discoverable via search_tools. Without this sync only a handful of backends
were indexed, so most integration tools were executable-but-unfindable.

This module indexes EVERY tool across EVERY backend into the faceted index,
incrementally (skips already-indexed tool_ids), and re-runs on a heartbeat so
newly-announced tools become discoverable automatically.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
from typing import Dict

logger = logging.getLogger(__name__)

SYNC_INTERVAL_S = float(os.getenv("TOOL_INDEX_SYNC_INTERVAL_S", "600"))  # 10 min
SYNC_CHUNK = int(os.getenv("TOOL_INDEX_SYNC_CHUNK", "40"))  # tools per embed pass
_last_run: Dict[str, object] = {
    "started": 0.0, "finished": 0.0, "indexed": 0, "running": False,
}
_sync_lock = asyncio.Lock()


def _schema_hash(tool_dict: Dict) -> str:
    """Stable hash of a tool's schema — the only thing that gates re-embedding."""
    return hashlib.sha1(
        json.dumps(tool_dict, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def _existing_hashes(index) -> Dict[str, str]:
    """{tool_id: schema_hash} already in the index, so we re-embed ONLY tools
    that are new or whose schema changed. tool_data is the exact dict we stored
    at index time, so hashing it reproduces the label→content fingerprint."""
    out: Dict[str, str] = {}
    try:
        conn = sqlite3.connect(str(index.db_path))
        rows = conn.execute("SELECT tool_id, tool_data FROM tool_facets").fetchall()
        conn.close()
        for tid, tdata in rows:
            try:
                out[tid] = _schema_hash(json.loads(tdata)) if tdata else ""
            except Exception:  # noqa: BLE001
                out[tid] = ""
    except Exception as exc:  # noqa: BLE001
        logger.warning("tool_index_sync: could not read existing hashes: %s", exc)
    return out


def _tool_dict(t) -> Dict:
    """Normalize a RegisteredTool (or dict) to the shape index_tool expects."""
    get = (lambda k, d=None: t.get(k, d)) if isinstance(t, dict) else (lambda k, d=None: getattr(t, k, d))
    return {
        "name": get("name", ""),
        "description": get("description", "") or "",
        "inputSchema": get("input_schema") or get("inputSchema") or {},
        "outputSchema": get("output_schema") or get("outputSchema") or {},
    }


async def faceted_sync_once() -> Dict:
    """Index every not-yet-indexed tool across every backend. Incremental.

    Guarded so the heartbeat and any manual trigger can't run concurrently and
    double-embed the catalog (wasteful + rate-limit risk); a second caller
    returns immediately with the in-flight status.
    """
    from ..embeddings.advanced_tool_index import get_advanced_index
    from ..persistence import get_tool_store

    if _sync_lock.locked():
        return {"already_running": True, **sync_status()}

    async with _sync_lock:
        index = get_advanced_index()
        store = get_tool_store()
        existing = _existing_hashes(index)  # {tool_id: schema_hash}
        _last_run.update(started=time.time(), finished=0.0, indexed=0, running=True)

        from . import tool_expand, tool_fts
        backends = store.list_backends()
        indexed = 0
        current_ids = set()
        fts_rows = []          # (tool_id, backend, name, text) for the lexical index
        todo = []              # (backend_id, expanded_tool_dict) needing (re)embed

        # ── Phase A (fast): scan catalog, expand descriptions, decide re-embeds,
        # and rebuild the lexical index NOW — FTS/hybrid must reflect the current
        # catalog immediately, never blocked behind the slow embedding pass.
        for backend in backends:
            bid = backend.get("backend_id") or backend.get("id")
            if not bid:
                continue
            try:
                tools = store.list_tools_by_backend(bid)
            except Exception as exc:  # noqa: BLE001
                logger.warning("tool_index_sync: list_tools_by_backend(%s) failed: %s", bid, exc)
                continue
            for t in tools:
                d = _tool_dict(t)
                if not d["name"]:
                    continue
                tid = f"{bid}:{d['name']}"
                current_ids.add(tid)
                prop_keys = list((d.get("inputSchema") or {}).get("properties", {}).keys())
                # Pre-embedding document expansion: fold 3-5 synthetic example
                # requests into the description (the field both the embedder and
                # FTS read). Cached by the ORIGINAL schema hash so it regenerates
                # only when the tool actually changes.
                orig_hash = _schema_hash(d)
                d["description"] = tool_expand.expand_description(
                    tid, d["name"], d["description"], prop_keys, orig_hash
                )
                fts_rows.append((tid, bid, d["name"],
                                 f"{d['name']} {d['description']} {', '.join(prop_keys[:16])}"))
                # Re-embed only if the *expanded* doc changed (stable given the
                # cache, so unchanged tools are still skipped).
                if existing.get(tid) != _schema_hash(d):
                    todo.append((bid, d))

        fts_n = 0
        try:
            fts_n = tool_fts.rebuild(fts_rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning("tool_index_sync: FTS rebuild failed: %s", exc)

        # ── Phase B (slow): embed only new/changed tools, grouped by backend.
        for i in range(0, len(todo), SYNC_CHUNK):
            by_backend = {}
            for bid, d in todo[i:i + SYNC_CHUNK]:
                by_backend.setdefault(bid, []).append(d)
            for bid, chunk in by_backend.items():
                try:
                    await index.index_tools_batch(chunk, bid)
                    indexed += len(chunk)
                    _last_run["indexed"] = indexed  # live progress
                    await asyncio.sleep(0)  # yield to the event loop
                except Exception as exc:  # noqa: BLE001
                    logger.warning("tool_index_sync: batch index %s failed: %s", bid, exc)

        # Evict embeddings for tools no longer in the catalog (renamed/removed
        # backends or endpoints) so stale entries never surface in retrieval.
        evicted = 0
        if current_ids:
            try:
                conn = sqlite3.connect(str(index.db_path))
                have = {r[0] for r in conn.execute("SELECT tool_id FROM tool_facets").fetchall()}
                stale = list(have - current_ids)
                for i in range(0, len(stale), 500):
                    batch = stale[i:i + 500]
                    conn.execute(
                        f"DELETE FROM tool_facets WHERE tool_id IN ({','.join('?' * len(batch))})",
                        batch,
                    )
                conn.commit()
                conn.close()
                evicted = len(stale)
                if evicted:
                    logger.info("tool_index_sync: evicted %d stale tools", evicted)
            except Exception as exc:  # noqa: BLE001
                logger.warning("tool_index_sync: eviction failed: %s", exc)

        _last_run.update(finished=time.time(), indexed=indexed, evicted=evicted,
                         fts=fts_n, running=False)
        total_now = len(existing) + indexed - evicted
        logger.info("tool_index_sync: +%d indexed, -%d evicted (index now ~%d)", indexed, evicted, total_now)
        return {"newly_indexed": indexed, "evicted": evicted, "index_total": total_now, "backends": len(backends)}


async def faceted_sync_loop() -> None:
    """Heartbeat: seed on startup, then refresh forever so new tools get found."""
    # Small initial delay so catalog sync (DuckDB) lands first.
    await asyncio.sleep(float(os.getenv("TOOL_INDEX_SYNC_START_DELAY_S", "20")))
    while True:
        try:
            await faceted_sync_once()
        except Exception as exc:  # noqa: BLE001
            logger.warning("tool_index_sync loop error: %s", exc)
        await asyncio.sleep(SYNC_INTERVAL_S)


def sync_status() -> Dict:
    return dict(_last_run)
