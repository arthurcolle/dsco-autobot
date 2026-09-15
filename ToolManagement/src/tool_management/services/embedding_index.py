"""
Embedding-based semantic search over registered tools, backed by Postgres.

Design constraints (production):
- Jina rate limits (429) are expected: all writes go through bounded retry
  with exponential backoff; backfill is incremental and paced.
- The web container must never block: psycopg runs via asyncio.to_thread.
- Degrades gracefully: if embeddings are unavailable, callers fall back to
  the existing lexical paths.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

EMBED_DIM = 1024
_BATCH = 32
_RETRY_429S = 5

_conn = None
_ready = False


def _get_conn():
    global _conn
    if _conn is None:
        import psycopg
        _conn = psycopg.connect(os.environ["POSTGRES_URL"], autocommit=True)
    return _conn


async def ensure_schema() -> bool:
    """Create the embeddings table. Returns True when ready for use."""
    global _ready
    if _ready:
        return True
    try:
        conn = await asyncio.to_thread(_ensure_schema_sync)
        _ready = conn
        return _ready
    except Exception as e:
        logger.warning(f"embedding index schema unavailable: {e}")
        return False


def _ensure_schema_sync() -> bool:
    import psycopg
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tool_embeddings (
                    tool_id TEXT PRIMARY KEY,
                    embedding vector(1024),
                    provider TEXT DEFAULT 'together',
                    updated_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            # migrate pre-existing table lacking provider column; clear stale
            # vectors so the corpus is never a mix of providers.
            cur.execute("""
                DO $$ BEGIN
                    ALTER TABLE tool_embeddings ADD COLUMN provider TEXT DEFAULT 'together';
                EXCEPTION WHEN duplicate_column THEN NULL; END $$;
            """)
            cur.execute("DELETE FROM tool_embeddings WHERE provider IS NULL OR provider = ''")
        return True
    except Exception as e:
        logger.warning(f"pgvector unavailable ({e}); semantic index disabled")
        return False


# Provider priority: Together first (separate quota from other services that
# hammer the shared Jina key), Jina as fallback. Both produce 1024-dim vectors;
# NEVER mix providers within one index - backfill drops stale rows via
# _reset_index_if_provider_changed().
_ACTIVE_PROVIDER_FILE = "/tmp/tool_embed_provider"

def _provider() -> str:
    """Pick embedding provider; prefer continuity with the indexed corpus."""
    cur = None
    try:
        with open(_ACTIVE_PROVIDER_FILE) as fh:
            cur = fh.read().strip()
    except Exception:
        pass
    if cur == "together" and os.getenv("TOGETHER_API_KEY"):
        return "together"
    if cur == "jina" and os.getenv("JINA_API_KEY"):
        return "jina"
    if os.getenv("TOGETHER_API_KEY"):
        return "together"
    return "jina"


def _mark_provider(p: str):
    try:
        with open(_ACTIVE_PROVIDER_FILE, "w") as fh:
            fh.write(p)
    except Exception:
        pass


async def _embed_together(texts: List[str], task: str):
    import httpx
    key = os.getenv("TOGETHER_API_KEY", "")
    if not key:
        return None
    delay = 2.0
    for attempt in range(_RETRY_429S):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(
                    "https://api.together.xyz/v1/embeddings",
                    headers={"Authorization": f"Bearer {key}"},
                    json={"model": "BAAI/bge-large-en-v1.5", "input": texts},
                )
            if r.status_code == 429:
                logger.info(f"together 429 (attempt {attempt+1}), backing off {delay:.0f}s")
                await asyncio.sleep(delay); delay *= 2; continue
            r.raise_for_status()
            data = sorted(r.json()["data"], key=lambda d: d["index"])
            return [d["embedding"] for d in data]
        except Exception as e:
            logger.warning(f"together embed failed: {e}")
            await asyncio.sleep(delay); delay *= 2
    return None


async def _embed_jina(texts: List[str], task: str):
    import httpx
    key = os.getenv("JINA_API_KEY", "")
    if not key:
        return None
    delay = 2.0
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.post(
                    "https://api.jina.ai/v1/embeddings",
                    headers={"Authorization": f"Bearer {key}"},
                    json={"model": "jina-embeddings-v3", "dimensions": EMBED_DIM,
                          "task": f"retrieval.{task}", "input": texts},
                )
            if r.status_code == 429:
                logger.info(f"jina 429 (attempt {attempt+1}), backing off {delay:.0f}s")
                await asyncio.sleep(delay); delay *= 2; continue
            r.raise_for_status()
            data = sorted(r.json()["data"], key=lambda d: d["index"])
            return [d["embedding"] for d in data]
        except Exception as e:
            logger.warning(f"jina embed failed: {e}")
            await asyncio.sleep(delay); delay *= 2
    return None


async def _embed_any(texts: List[str], task: str = "passage") -> tuple:
    """Returns (vectors, provider) or (None, None)."""
    p = _provider()
    vecs = await (_embed_together(texts, task) if p == "together" else _embed_jina(texts, task))
    if vecs is None:
        # fall through to the other provider only if the index is empty anyway
        alt = "jina" if p == "together" else "together"
        vecs = await (_embed_together(texts, task) if alt == "together" else _embed_jina(texts, task))
        if vecs is not None:
            p = alt
    if vecs is not None:
        _mark_provider(p)
    return vecs, (p if vecs is not None else None)


async def embed_texts(texts: List[str]) -> Optional[List[List[float]]]:
    if not texts:
        return None
    vecs, _p = await _embed_any(texts, "passage")
    return vecs


async def embed_query(query: str) -> Optional[List[float]]:
    vecs, _p = await _embed_any([query], "query")
    return vecs[0] if vecs else None


def _reset_if_provider_changed(provider: str) -> bool:
    """Drop all vectors when the active provider differs from what's stored."""
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM tool_embeddings")
        n = cur.fetchone()[0]
        cur.execute("SELECT DISTINCT provider FROM tool_embeddings")
        rows = [r[0] for r in cur.fetchall()]
    if n and provider not in rows:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM tool_embeddings")
        return True
    return False


def _backfill_batch_sync(rows, provider="together") -> int:
    conn = _get_conn()
    written = 0
    with conn.cursor() as cur:
        for tool_id, vec in rows:
            cur.execute(
                """INSERT INTO tool_embeddings (tool_id, embedding, provider, updated_at)
                   VALUES (%s, %s, %s, now())
                   ON CONFLICT (tool_id) DO NOTHING""",
                (tool_id, vec, provider),
            )
            written += cur.rowcount
    return written


async def backfill(limit: int = 256, min_interval_seconds: float = 0.0) -> dict:
    """Incrementally embed tools that lack vectors. Paced and idempotent."""
    if not await ensure_schema():
        return {"ok": False, "reason": "schema unavailable"}
    from ..persistence.tool_store import get_tool_store
    store = get_tool_store()

    def _missing():
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT tool_id FROM tool_embeddings")
            have = {r[0] for r in cur.fetchall()}
        out = []
        for t in store.list_tools(limit=limit + len(have)):
            if t.tool_id not in have:
                text = f"{t.name}: {t.description or ''} [{t.backend_id}] tags={t.tags}"
                out.append((t.tool_id, text))
                if len(out) >= limit:
                    break
        return out

    missing = await asyncio.to_thread(_missing)
    done = 0
    for i in range(0, len(missing), _BATCH):
        chunk = missing[i:i + _BATCH]
        vecs, prov = await _embed_any([c[1] for c in chunk], "passage")
        if not vecs:
            break
        await asyncio.to_thread(_reset_if_provider_changed, prov)
        done += await asyncio.to_thread(
            _backfill_batch_sync, [(chunk[j][0], v) for j, v in enumerate(vecs)], prov
        )
        if min_interval_seconds:
            await asyncio.sleep(min_interval_seconds)
    return {"ok": True, "embedded": done, "remaining_estimate": max(0, len(missing) - done)}


def search_sync(query_vec: List[float], limit: int = 10):
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT te.tool_id, t.name, t.backend_id, t.description,
                      1 - (te.embedding <=> %s::vector) AS score
               FROM tool_embeddings te
               JOIN tools t ON t.tool_id = te.tool_id
               ORDER BY te.embedding <=> %s::vector
               LIMIT %s""",
            (query_vec, query_vec, limit),
        )
        return [
            {"name": n, "backend": b, "description": (d or "")[:200], "score": round(float(s), 4)}
            for tid, n, b, d, s in cur.fetchall()
        ]


async def semantic_search(query: str, limit: int = 10):
    """Returns results list, or None when the index can't serve."""
    if not await ensure_schema():
        return None
    try:
        def _stored_provider():
            conn = _get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT provider FROM tool_embeddings")
                return [r[0] for r in cur.fetchall()]
        stored = await asyncio.to_thread(_stored_provider)
    except Exception:
        stored = []
    if "together" in stored and os.getenv("TOGETHER_API_KEY"):
        qv = await _embed_together([query], "query")
    elif "jina" in stored and os.getenv("JINA_API_KEY"):
        qv = await _embed_jina([query], "query")
    else:
        qv = await embed_query(query)
    if qv is None:
        return None
    if qv is None or (isinstance(qv, list) and not qv):
        return None
    try:
        return await asyncio.to_thread(search_sync, qv, limit)
    except Exception as e:
        logger.warning(f"semantic search failed: {e}")
        return None
