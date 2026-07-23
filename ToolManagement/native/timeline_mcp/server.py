#!/usr/bin/env python3
"""
Distributed Timeline MCP — temporal-aware multimodal embedding store.

Everything (text, images, URLs) is embedded with jina-embeddings-v4
(2048-dim, multimodal) and placed on a persistent timeline. Search is
semantic + time-windowed. A built-in scheduler ingests sources on an
interval (via Jina Reader), so the timeline stays current without manual
pushes.

Vector spaces are tagged per embedder (jina-embeddings-v4 vs the offline
local-hash fallback) and never cross-compared.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import sqlite3
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

from native.base import NativeMCPBase  # noqa: E402

logger = logging.getLogger("timeline-mcp")

JINA_MODEL = "jina-embeddings-v4"
JINA_DIMS = 2048
EMBEDDINGS_URL = "https://api.jina.ai/v1/embeddings"
READER_URL = "https://r.jina.ai/"

DATA_DIR = Path(os.getenv("TIMELINE_DATA_DIR", os.getenv("DATA_DIR", "data")))
DB_PATH = DATA_DIR / "timeline.db"

SCHEDULER_TICK_SECONDS = 15
MIN_SCHEDULE_INTERVAL = 60


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse_ts(value: Optional[str]) -> datetime:
    if not value:
        return _now()
    v = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class JinaV4Embedder:
    """Multimodal embeddings via jina-embeddings-v4; offline hash fallback."""

    def __init__(self):
        self.api_key = os.getenv("JINA_API_KEY", "")
        self._local = None  # lazy LocalHashEmbeddings

    @property
    def active_tag(self) -> str:
        return JINA_MODEL if self.api_key else "local-hash-v1"

    async def _local_embed(self, texts: List[str]) -> tuple[List[np.ndarray], str]:
        if self._local is None:
            from tool_management.embeddings.openrouter_embeddings import LocalHashEmbeddings
            self._local = LocalHashEmbeddings()
        arr = await self._local.embed(texts)
        return [np.asarray(v, dtype=np.float32) for v in arr], "local-hash-v1"

    async def embed(self, inputs: List[Dict[str, str]], task: str) -> tuple[List[np.ndarray], str]:
        """
        inputs: [{"text": ...} | {"image": url-or-base64}], task: retrieval.query|retrieval.passage
        Returns (vectors, embedder_tag). Falls back to local hashing for text
        when the Jina API is unavailable; image inputs require the API.
        """
        if self.api_key:
            try:
                async with httpx.AsyncClient(timeout=90.0) as client:
                    r = await client.post(
                        EMBEDDINGS_URL,
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={"model": JINA_MODEL, "task": task, "input": inputs},
                    )
                if r.status_code == 200:
                    data = r.json()["data"]
                    return [np.asarray(d["embedding"], dtype=np.float32) for d in data], JINA_MODEL
                raise RuntimeError(f"Jina embeddings {r.status_code}: {r.text[:200]}")
            except Exception as e:
                if any("image" in i for i in inputs):
                    raise  # images cannot fall back to the text hasher
                logger.warning(f"Jina embed failed, using local fallback: {e}")
        texts = [i.get("text", "") for i in inputs]
        if any("image" in i for i in inputs):
            raise RuntimeError("Image embedding requires JINA_API_KEY")
        return await self._local_embed(texts)


class TimelineStore:
    """SQLite-backed timeline with embedded vectors (float32 blobs)."""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    content TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    session_id TEXT,
                    tags TEXT NOT NULL DEFAULT '[]',
                    meta TEXT NOT NULL DEFAULT '{}',
                    embedder TEXT NOT NULL,
                    dims INTEGER NOT NULL,
                    embedding BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_items_ts ON items(ts);
                CREATE INDEX IF NOT EXISTS idx_items_session ON items(session_id);
                CREATE TABLE IF NOT EXISTS schedules (
                    schedule_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_url TEXT NOT NULL,
                    interval_seconds INTEGER NOT NULL,
                    tags TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    next_run_at TEXT NOT NULL,
                    last_run_at TEXT,
                    last_status TEXT,
                    runs INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
            """)
            self._conn.commit()

    def add_item(self, *, kind: str, content: str, ts: datetime, session_id: Optional[str],
                 tags: List[str], meta: Dict[str, Any], embedder: str, vector: np.ndarray) -> str:
        item_id = str(uuid.uuid4())
        with self._lock:
            self._conn.execute(
                "INSERT INTO items VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [item_id, kind, content[:20000], _iso(ts), _iso(_now()), session_id,
                 json.dumps(tags), json.dumps(meta), embedder, int(vector.shape[0]),
                 vector.astype(np.float32).tobytes()],
            )
            self._conn.commit()
        return item_id

    def query(self, *, time_from: Optional[datetime], time_to: Optional[datetime],
              tags: Optional[List[str]], session_id: Optional[str], embedder: Optional[str],
              limit: int = 2000) -> List[sqlite3.Row]:
        sql = "SELECT * FROM items WHERE 1=1"
        params: List[Any] = []
        if time_from:
            sql += " AND ts >= ?"; params.append(_iso(time_from))
        if time_to:
            sql += " AND ts <= ?"; params.append(_iso(time_to))
        if session_id:
            sql += " AND session_id = ?"; params.append(session_id)
        if embedder:
            sql += " AND embedder = ?"; params.append(embedder)
        sql += " ORDER BY ts DESC LIMIT ?"; params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        if tags:
            wanted = set(tags)
            rows = [r for r in rows if wanted & set(json.loads(r["tags"]))]
        return rows

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
            by_kind = dict(self._conn.execute(
                "SELECT kind, COUNT(*) FROM items GROUP BY kind").fetchall())
            by_embedder = dict(self._conn.execute(
                "SELECT embedder, COUNT(*) FROM items GROUP BY embedder").fetchall())
            span = self._conn.execute("SELECT MIN(ts), MAX(ts) FROM items").fetchone()
            schedules = self._conn.execute(
                "SELECT COUNT(*), SUM(enabled) FROM schedules").fetchone()
        return {
            "total_items": total, "by_kind": by_kind, "by_embedder": by_embedder,
            "earliest": span[0], "latest": span[1],
            "schedules": {"total": schedules[0] or 0, "enabled": schedules[1] or 0},
        }

    # ---- schedules ----
    def add_schedule(self, name: str, source_url: str, interval_seconds: int,
                     tags: List[str], first_run: datetime) -> str:
        sid = str(uuid.uuid4())
        with self._lock:
            self._conn.execute(
                "INSERT INTO schedules (schedule_id, name, source_url, interval_seconds, tags,"
                " enabled, next_run_at, created_at) VALUES (?,?,?,?,?,1,?,?)",
                [sid, name, source_url, interval_seconds, json.dumps(tags),
                 _iso(first_run), _iso(_now())],
            )
            self._conn.commit()
        return sid

    def due_schedules(self) -> List[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM schedules WHERE enabled = 1 AND next_run_at <= ?",
                [_iso(_now())]).fetchall()

    def list_schedules(self) -> List[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM schedules ORDER BY created_at DESC").fetchall()

    def get_schedule(self, sid: str) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM schedules WHERE schedule_id = ?", [sid]).fetchone()

    def mark_schedule_run(self, sid: str, status: str, interval_seconds: int):
        with self._lock:
            self._conn.execute(
                "UPDATE schedules SET last_run_at = ?, last_status = ?, runs = runs + 1,"
                " next_run_at = ? WHERE schedule_id = ?",
                [_iso(_now()), status[:500], _iso(_now() + timedelta(seconds=interval_seconds)), sid],
            )
            self._conn.commit()

    def delete_schedule(self, sid: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM schedules WHERE schedule_id = ?", [sid])
            self._conn.commit()
            return cur.rowcount > 0


def _item_brief(row: sqlite3.Row, score: Optional[float] = None) -> Dict[str, Any]:
    d = {
        "item_id": row["item_id"],
        "kind": row["kind"],
        "ts": row["ts"],
        "snippet": row["content"][:200],
        "tags": json.loads(row["tags"]),
        "session_id": row["session_id"],
    }
    if score is not None:
        d["score"] = round(float(score), 4)
    return d


class TimelineMCP(NativeMCPBase):
    SERVICE_ID = "distributed-timeline"
    SERVICE_NAME = "Distributed Timeline MCP"
    VERSION = "1.0.0"
    CAPABILITIES = [
        "timeline", "temporal_search", "multimodal_embeddings",
        "image_embeddings", "scheduling", "ingestion",
    ]
    TOOLS = [
        {
            "name": "timeline_add",
            "description": "Embed content (text, image URL/base64, or a web URL fetched via Jina Reader) with jina-embeddings-v4 and place it on the timeline. Exactly one of text/image/url.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "image": {"type": "string", "description": "Image URL or base64"},
                    "url": {"type": "string", "description": "Web page URL — fetched via Jina Reader, embedded as text"},
                    "ts": {"type": "string", "description": "Event time ISO-8601 (default now)"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "session_id": {"type": "string"},
                    "meta": {"type": "object"},
                },
            },
        },
        {
            "name": "timeline_search",
            "description": "Semantic search over the timeline (text or image query) with optional time window, tags, and session filters. Compact results.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "image": {"type": "string", "description": "Image URL/base64 as the query"},
                    "time_from": {"type": "string"},
                    "time_to": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "session_id": {"type": "string"},
                    "top_k": {"type": "integer", "default": 10},
                },
            },
        },
        {
            "name": "timeline_window",
            "description": "Chronological slice of the timeline between two times (no embedding, fast).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "time_from": {"type": "string"},
                    "time_to": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "limit": {"type": "integer", "default": 50},
                },
                "required": ["time_from"],
            },
        },
        {
            "name": "timeline_get",
            "description": "Fetch one timeline item in full by item_id.",
            "inputSchema": {
                "type": "object",
                "properties": {"item_id": {"type": "string"}},
                "required": ["item_id"],
            },
        },
        {
            "name": "timeline_stats",
            "description": "Timeline counts by kind/embedder, time span, and schedule totals.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "schedule_create",
            "description": "Create a recurring ingestion: fetch a URL via Jina Reader every interval_seconds, embed it, append to the timeline.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "url": {"type": "string"},
                    "interval_seconds": {"type": "integer", "minimum": 60},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "run_immediately": {"type": "boolean", "default": True},
                },
                "required": ["name", "url", "interval_seconds"],
            },
        },
        {
            "name": "schedule_list",
            "description": "List ingestion schedules with next/last run info.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "schedule_run_now",
            "description": "Trigger a schedule immediately.",
            "inputSchema": {
                "type": "object",
                "properties": {"schedule_id": {"type": "string"}},
                "required": ["schedule_id"],
            },
        },
        {
            "name": "schedule_delete",
            "description": "Delete a schedule.",
            "inputSchema": {
                "type": "object",
                "properties": {"schedule_id": {"type": "string"}},
                "required": ["schedule_id"],
            },
        },
    ]

    def __init__(self, port: int, **kwargs):
        super().__init__(port=port, **kwargs)
        self.store = TimelineStore(DB_PATH)
        self.embedder = JinaV4Embedder()
        self._scheduler_task: Optional[asyncio.Task] = None
        self.on_start(self._start_scheduler)

    # ------------------------------------------------------------------
    # Scheduler
    # ------------------------------------------------------------------
    async def _start_scheduler(self):
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Timeline scheduler started")

    async def _scheduler_loop(self):
        while True:
            try:
                for row in self.store.due_schedules():
                    try:
                        await self._run_schedule(row)
                    except Exception as e:
                        self.store.mark_schedule_run(
                            row["schedule_id"], f"error: {e}", row["interval_seconds"])
            except Exception as e:
                logger.warning(f"Scheduler tick failed: {e}")
            await asyncio.sleep(SCHEDULER_TICK_SECONDS)

    async def _run_schedule(self, row: sqlite3.Row):
        content = await self._read_url(row["source_url"])
        vectors, tag = await self.embedder.embed(
            [{"text": content[:8000]}], task="retrieval.passage")
        self.store.add_item(
            kind="url", content=content[:8000], ts=_now(), session_id=None,
            tags=json.loads(row["tags"]),
            meta={"url": row["source_url"], "schedule_id": row["schedule_id"],
                  "schedule_name": row["name"]},
            embedder=tag, vector=vectors[0],
        )
        self.store.mark_schedule_run(row["schedule_id"], "ok", row["interval_seconds"])
        logger.info(f"Schedule {row['name']} ingested {row['source_url']}")

    async def _read_url(self, url: str) -> str:
        headers = {"Accept": "text/plain"}
        if self.embedder.api_key:
            headers["Authorization"] = f"Bearer {self.embedder.api_key}"
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            r = await client.get(f"{READER_URL}{url}", headers=headers)
            if r.status_code != 200:
                raise RuntimeError(f"Reader {r.status_code} for {url}")
            return r.text

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _ok(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps(payload, indent=2)}]}

    @staticmethod
    def _err(msg: str) -> Dict[str, Any]:
        return {"content": [{"type": "text", "text": json.dumps({"error": msg})}],
                "isError": True}

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------
    async def _tool_timeline_add(self, args: Dict[str, Any]) -> Dict[str, Any]:
        provided = [k for k in ("text", "image", "url") if args.get(k)]
        if len(provided) != 1:
            return self._err("Provide exactly one of: text, image, url")
        kind = provided[0]
        try:
            if kind == "text":
                content = args["text"]
                inputs = [{"text": content}]
            elif kind == "image":
                content = args["image"]
                inputs = [{"image": content}]
            else:
                content = await self._read_url(args["url"])
                content = content[:8000]
                inputs = [{"text": content}]
            vectors, tag = await self.embedder.embed(inputs, task="retrieval.passage")
            meta = dict(args.get("meta") or {})
            if kind == "url":
                meta["url"] = args["url"]
            if kind == "image":
                # don't store megabytes of base64 as content
                content = content if content.startswith("http") else f"<base64 image {len(content)}b>"
                meta["image_source"] = "url" if content.startswith("http") else "base64"
            item_id = self.store.add_item(
                kind=kind, content=content, ts=_parse_ts(args.get("ts")),
                session_id=args.get("session_id"), tags=list(args.get("tags") or []),
                meta=meta, embedder=tag, vector=vectors[0],
            )
            return self._ok({"item_id": item_id, "kind": kind, "embedder": tag,
                             "dims": int(vectors[0].shape[0])})
        except Exception as e:
            return self._err(f"timeline_add failed: {e}")

    async def _tool_timeline_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        query, image = args.get("query"), args.get("image")
        if bool(query) == bool(image):
            return self._err("Provide exactly one of: query (text) or image")
        try:
            q_input = [{"text": query}] if query else [{"image": image}]
            q_vecs, tag = await self.embedder.embed(q_input, task="retrieval.query")
            qv = q_vecs[0]
            rows = self.store.query(
                time_from=_parse_ts(args["time_from"]) if args.get("time_from") else None,
                time_to=_parse_ts(args["time_to"]) if args.get("time_to") else None,
                tags=args.get("tags"), session_id=args.get("session_id"),
                embedder=tag,  # only compare same-space vectors
            )
            qn = qv / (np.linalg.norm(qv) or 1.0)
            scored = []
            for r in rows:
                v = np.frombuffer(r["embedding"], dtype=np.float32)
                if v.shape[0] != qv.shape[0]:
                    continue
                score = float(np.dot(qn, v / (np.linalg.norm(v) or 1.0)))
                scored.append((score, r))
            scored.sort(key=lambda x: x[0], reverse=True)
            top = scored[: int(args.get("top_k", 10))]
            return self._ok({
                "count": len(top),
                "embedder": tag,
                "candidates_considered": len(rows),
                "results": [_item_brief(r, s) for s, r in top],
            })
        except Exception as e:
            return self._err(f"timeline_search failed: {e}")

    async def _tool_timeline_window(self, args: Dict[str, Any]) -> Dict[str, Any]:
        try:
            rows = self.store.query(
                time_from=_parse_ts(args["time_from"]),
                time_to=_parse_ts(args["time_to"]) if args.get("time_to") else None,
                tags=args.get("tags"), session_id=None, embedder=None,
                limit=int(args.get("limit", 50)),
            )
            items = [_item_brief(r) for r in rows]
            items.sort(key=lambda d: d["ts"])
            return self._ok({"count": len(items), "items": items})
        except Exception as e:
            return self._err(f"timeline_window failed: {e}")

    async def _tool_timeline_get(self, args: Dict[str, Any]) -> Dict[str, Any]:
        with self.store._lock:
            row = self.store._conn.execute(
                "SELECT * FROM items WHERE item_id = ?", [args.get("item_id")]).fetchone()
        if not row:
            return self._err("item not found")
        return self._ok({
            "item_id": row["item_id"], "kind": row["kind"], "ts": row["ts"],
            "ingested_at": row["ingested_at"], "content": row["content"],
            "tags": json.loads(row["tags"]), "meta": json.loads(row["meta"]),
            "session_id": row["session_id"], "embedder": row["embedder"],
            "dims": row["dims"],
        })

    async def _tool_timeline_stats(self, args: Dict[str, Any]) -> Dict[str, Any]:
        stats = self.store.stats()
        stats["active_embedder"] = self.embedder.active_tag
        return self._ok(stats)

    async def _tool_schedule_create(self, args: Dict[str, Any]) -> Dict[str, Any]:
        interval = max(int(args["interval_seconds"]), MIN_SCHEDULE_INTERVAL)
        run_now = bool(args.get("run_immediately", True))
        first = _now() if run_now else _now() + timedelta(seconds=interval)
        sid = self.store.add_schedule(
            args["name"], args["url"], interval, list(args.get("tags") or []), first)
        return self._ok({"schedule_id": sid, "name": args["name"], "url": args["url"],
                         "interval_seconds": interval, "next_run_at": _iso(first)})

    async def _tool_schedule_list(self, args: Dict[str, Any]) -> Dict[str, Any]:
        rows = self.store.list_schedules()
        return self._ok({"count": len(rows), "schedules": [
            {"schedule_id": r["schedule_id"], "name": r["name"], "url": r["source_url"],
             "interval_seconds": r["interval_seconds"], "enabled": bool(r["enabled"]),
             "next_run_at": r["next_run_at"], "last_run_at": r["last_run_at"],
             "last_status": r["last_status"], "runs": r["runs"],
             "tags": json.loads(r["tags"])} for r in rows]})

    async def _tool_schedule_run_now(self, args: Dict[str, Any]) -> Dict[str, Any]:
        row = self.store.get_schedule(args.get("schedule_id", ""))
        if not row:
            return self._err("schedule not found")
        try:
            await self._run_schedule(row)
            return self._ok({"schedule_id": row["schedule_id"], "status": "ok"})
        except Exception as e:
            self.store.mark_schedule_run(row["schedule_id"], f"error: {e}", row["interval_seconds"])
            return self._err(f"schedule run failed: {e}")

    async def _tool_schedule_delete(self, args: Dict[str, Any]) -> Dict[str, Any]:
        ok = self.store.delete_schedule(args.get("schedule_id", ""))
        return self._ok({"deleted": ok})


def create_server(port: Optional[int] = None) -> TimelineMCP:
    return TimelineMCP(port=port or int(os.getenv("PORT", "4012")))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Timeline MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4012")))
    args = parser.parse_args()
    create_server(args.port).run()
