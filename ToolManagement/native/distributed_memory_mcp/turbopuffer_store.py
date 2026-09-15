"""
Turbopuffer-backed vector index for Distributed Memory MCP.

Mirrors episodic/semantic memories into a per-tenant turbopuffer namespace
(`dsco-memory-{tenant}`) with Jina v3 embeddings, and serves hybrid
(ANN + BM25) recall. The local SQLite stores remain the source of truth;
turbopuffer replaces the brute-force cosine scan for similarity search.

Enabled when both TURBOPUFFER_API_KEY and JINA_API_KEY are set.

Env:
    TURBOPUFFER_API_KEY   required
    JINA_API_KEY          required
    TURBOPUFFER_REGION    default gcp-us-central1
    MEMORY_TENANT         default "default" (namespace suffix)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

JINA_EMBED_URL = os.getenv("JINA_API_BASE", "https://api.jina.ai").rstrip("/") + "/v1/embeddings"
JINA_MODEL = os.getenv("MEMORY_EMBED_MODEL", "jina-embeddings-v3")

_SCHEMA = {
    "text": {"type": "string", "full_text_search": True, "regex": True},
    "tier": {"type": "string"},
    "event_type": {"type": "string"},
    "session_id": {"type": "string"},
    "ts": {"type": "float"},
    "meta": {"type": "string"},
}

_RRF_K = 60


class TurbopufferMemoryIndex:
    """Async Jina-embeddings + turbopuffer index, one namespace per tenant."""

    def __init__(self, tpuf_key: str, jina_key: str, region: str, tenant: str):
        self.region = region
        self.tenant = tenant  # default tenant when a call doesn't specify one
        self.namespace = f"dsco-memory-{tenant}"
        self._tpuf_headers = {"Authorization": f"Bearer {tpuf_key}"}
        self._jina_headers = {"Authorization": f"Bearer {jina_key}"}
        self._client = httpx.AsyncClient(timeout=30.0)
        self.embedder_tag = JINA_MODEL

    def namespace_for(self, tenant: Optional[str]) -> str:
        slug = "".join(
            c if c.isalnum() or c in "-_" else "-" for c in (tenant or self.tenant)
        ).strip("-").lower() or self.tenant
        return f"dsco-memory-{slug}"

    def _url(self, tenant: Optional[str]) -> str:
        return f"https://{self.region}.turbopuffer.com/v2/namespaces/{self.namespace_for(tenant)}"

    @classmethod
    def from_env(cls) -> Optional["TurbopufferMemoryIndex"]:
        tpuf_key = os.getenv("TURBOPUFFER_API_KEY")
        jina_key = os.getenv("JINA_API_KEY")
        if not tpuf_key or not jina_key:
            return None
        return cls(
            tpuf_key=tpuf_key,
            jina_key=jina_key,
            region=os.getenv("TURBOPUFFER_REGION", "gcp-us-central1"),
            tenant=os.getenv("MEMORY_TENANT", "default"),
        )

    async def _embed(self, texts: List[str], task: str) -> List[List[float]]:
        resp = await self._client.post(
            JINA_EMBED_URL,
            headers=self._jina_headers,
            json={"model": JINA_MODEL, "task": task, "input": texts},
        )
        resp.raise_for_status()
        return [d["embedding"] for d in resp.json()["data"]]

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def upsert(
        self,
        mem_id: str,
        tier: str,
        text: str,
        *,
        event_type: str = "",
        session_id: Optional[str] = None,
        timestamp: float = 0.0,
        meta: Optional[Dict[str, Any]] = None,
        tenant: Optional[str] = None,
    ) -> bool:
        """Embed and upsert one memory row. Returns True on success."""
        try:
            vec = (await self._embed([text[:8000]], "retrieval.passage"))[0]
            resp = await self._client.post(
                self._url(tenant),
                headers=self._tpuf_headers,
                json={
                    "upsert_rows": [{
                        "id": mem_id,
                        "vector": vec,
                        "text": text[:8000],
                        "tier": tier,
                        "event_type": event_type or "",
                        "session_id": session_id or "",
                        "ts": float(timestamp),
                        "meta": json.dumps(meta or {})[:4000],
                    }],
                    "distance_metric": "cosine_distance",
                    "schema": _SCHEMA,
                },
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            logger.warning(f"turbopuffer upsert failed for {tier}/{mem_id}: {e}")
            return False

    def upsert_background(self, *args, **kwargs) -> None:
        """Fire-and-forget upsert; never blocks or raises into the caller."""
        try:
            asyncio.get_running_loop().create_task(self.upsert(*args, **kwargs))
        except RuntimeError:
            pass  # no running loop (e.g. sync test context)

    async def delete(self, mem_ids: List[str], *, tenant: Optional[str] = None) -> int:
        if not mem_ids:
            return 0
        try:
            resp = await self._client.post(
                self._url(tenant),
                headers=self._tpuf_headers,
                json={"deletes": mem_ids},
            )
            resp.raise_for_status()
            return int(resp.json().get("rows_affected", 0))
        except Exception as e:
            logger.warning(f"turbopuffer delete failed: {e}")
            return 0

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _filters(self, tiers: Optional[List[str]], session_id: Optional[str]):
        clauses: List[Any] = []
        if tiers:
            clauses.append(["tier", "In", tiers])
        if session_id:
            clauses.append(["session_id", "Eq", session_id])
        if not clauses:
            return None
        return clauses[0] if len(clauses) == 1 else ["And", clauses]

    async def search(
        self,
        query: str,
        *,
        tiers: Optional[List[str]] = None,
        session_id: Optional[str] = None,
        limit: int = 10,
        mode: str = "hybrid",
        tenant: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search memories. mode: 'vector', 'fts', or 'hybrid' (RRF merge).
        Returns [{id, tier, text, event_type, session_id, ts, meta, score}].
        """
        filters = self._filters(tiers, session_id)
        attrs = ["text", "tier", "event_type", "session_id", "ts", "meta"]

        queries: List[Dict[str, Any]] = []
        if mode in ("vector", "hybrid"):
            qvec = (await self._embed([query], "retrieval.query"))[0]
            q: Dict[str, Any] = {
                "rank_by": ["vector", "ANN", qvec],
                "limit": limit,
                "include_attributes": attrs,
            }
            if filters:
                q["filters"] = filters
            queries.append(q)
        if mode in ("fts", "hybrid"):
            q = {
                "rank_by": ["text", "BM25", query],
                "limit": limit,
                "include_attributes": attrs,
            }
            if filters:
                q["filters"] = filters
            queries.append(q)

        try:
            resp = await self._client.post(
                f"{self._url(tenant)}/query",
                headers=self._tpuf_headers,
                json={"queries": queries} if len(queries) > 1 else queries[0],
            )
            if resp.status_code == 404:  # namespace not created yet
                return []
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"turbopuffer search failed: {e}")
            return []

        result_sets = [r.get("rows", []) for r in data["results"]] if "results" in data else [data.get("rows", [])]

        # Reciprocal-rank-fusion merge across result sets
        fused: Dict[str, Dict[str, Any]] = {}
        scores: Dict[str, float] = {}
        for rows in result_sets:
            for rank, row in enumerate(rows):
                rid = str(row["id"])
                scores[rid] = scores.get(rid, 0.0) + 1.0 / (_RRF_K + rank + 1)
                if rid not in fused:
                    fused[rid] = row

        out = []
        for rid, row in fused.items():
            meta_raw = row.get("meta") or "{}"
            try:
                meta = json.loads(meta_raw)
            except (TypeError, ValueError):
                meta = {}
            out.append({
                "id": rid,
                "tier": row.get("tier"),
                "text": row.get("text"),
                "event_type": row.get("event_type") or None,
                "session_id": row.get("session_id") or None,
                "ts": row.get("ts"),
                "meta": meta,
                "score": round(scores[rid], 6),
            })
        out.sort(key=lambda r: r["score"], reverse=True)
        return out[:limit]

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    async def stats(self, *, tenant: Optional[str] = None) -> Dict[str, Any]:
        try:
            resp = await self._client.post(
                f"{self._url(tenant)}/query",
                headers=self._tpuf_headers,
                json={"aggregate_by": {"count": ["Count"]}, "group_by": ["tier"]},
            )
            if resp.status_code == 404:
                return {"namespace": self.namespace_for(tenant), "region": self.region, "rows_by_tier": {}}
            resp.raise_for_status()
            groups = resp.json().get("aggregation_groups", [])
            return {
                "namespace": self.namespace_for(tenant),
                "region": self.region,
                "embedder": self.embedder_tag,
                "rows_by_tier": {g.get("tier", "?"): g.get("count", 0) for g in groups},
            }
        except Exception as e:
            return {"namespace": self.namespace_for(tenant), "error": str(e)}

    async def close(self):
        await self._client.aclose()
