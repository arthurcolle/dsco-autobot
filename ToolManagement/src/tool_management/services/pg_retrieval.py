"""Neon-backed tool retrieval: pgvector (HNSW) semantic recall + Postgres FTS
(tsvector) lexical recall -> RRF fusion -> Jina cross-encoder rerank.

Replaces the per-container sqlite faceted index. Everything reads the shared
Neon `registry` schema, so retrieval is consistent across all containers and
scales horizontally with the catalog. All functions here are SYNCHRONOUS and
must be called via asyncio.to_thread from async routes — psycopg and the Jina
HTTP calls are blocking and would otherwise stall the event loop.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

_conn = None


def _pg():
    global _conn
    import psycopg
    if _conn is None or getattr(_conn, "closed", True):
        url = os.environ["POSTGRES_URL"]
        for frag in ("&channel_binding=require", "channel_binding=require&", "?channel_binding=require"):
            url = url.replace(frag, "?" if frag.startswith("?") else "")
        _conn = psycopg.connect(url, autocommit=True)
    return _conn


def _embed_query(text: str) -> Optional[List[float]]:
    key = os.getenv("JINA_API_KEY")
    if not key or not text:
        return None
    try:
        r = httpx.post(
            "https://api.jina.ai/v1/embeddings",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": "jina-embeddings-v3", "task": "retrieval.query",
                  "dimensions": 1024, "normalized": True, "embedding_type": "float",
                  "input": [text]},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        return r.json()["data"][0]["embedding"]
    except Exception as exc:  # noqa: BLE001
        logger.warning("query embed failed: %s", exc)
        return None


def _vec(v: List[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def _tool_obj(name, desc, schema):
    return {"name": name, "description": desc or "",
            "inputSchema": schema if isinstance(schema, dict) else {}}


def semantic_search(query: str, k: int, qvec: Optional[List[float]] = None) -> List[Dict]:
    qvec = qvec if qvec is not None else _embed_query(query)
    if not qvec:
        return []
    cur = _pg().cursor()
    cur.execute(
        "SELECT f.tool_id, f.backend, t.name, t.description, t.input_schema, "
        "1 - (f.purpose_embedding <=> %s::vector) AS score "
        "FROM registry.tool_facets f JOIN registry.tools t ON f.tool_id = t.tool_id "
        "WHERE f.purpose_embedding IS NOT NULL "
        "ORDER BY f.purpose_embedding <=> %s::vector LIMIT %s",
        (_vec(qvec), _vec(qvec), k),
    )
    out = []
    for tid, backend, name, desc, sch, score in cur.fetchall():
        out.append({"tool_id": tid, "backend": backend, "name": name,
                    "tool": _tool_obj(name, desc, sch), "semantic_score": float(score)})
    return out


def fts_search(query: str, k: int) -> List[Dict]:
    cur = _pg().cursor()
    cur.execute(
        "SELECT tool_id, backend_id, name, ts_rank(fts, websearch_to_tsquery('english', %s)) AS r "
        "FROM registry.tools WHERE fts @@ websearch_to_tsquery('english', %s) "
        "ORDER BY r DESC LIMIT %s",
        (query, query, k),
    )
    return [{"tool_id": tid, "backend": backend, "name": name, "fts_score": float(r)}
            for tid, backend, name, r in cur.fetchall()]


def _rerank(query: str, docs: List[str], top_k: int) -> Optional[List[Dict]]:
    key = os.getenv("JINA_API_KEY")
    if not key or not docs:
        return None
    try:
        r = httpx.post(
            "https://api.jina.ai/v1/rerank",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": "jina-reranker-v2-base-multilingual", "query": query,
                  "documents": docs, "top_n": top_k, "return_documents": False},
            timeout=20,
        )
        return r.json().get("results", []) if r.status_code == 200 else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("rerank failed: %s", exc)
        return None


def _doc(t: Dict) -> str:
    tool = t.get("tool", {})
    props = ", ".join(list((tool.get("inputSchema") or {}).get("properties", {}).keys())[:10])
    return f"{tool.get('name','')} [{t.get('backend','')}]: {tool.get('description','')}" + (f" (params: {props})" if props else "")


def retrieve_rerank(query: str, top_k: int = 8, recall: int = 40) -> Dict[str, Any]:
    cands = semantic_search(query, recall)
    if not cands:
        return {"query": query, "reranked": False, "tools": []}
    rr = _rerank(query, [_doc(c) for c in cands], top_k)
    if rr is not None:
        out = []
        for item in rr:
            i = item.get("index")
            if i is not None and i < len(cands):
                c = dict(cands[i]); c["rerank_score"] = item.get("relevance_score"); out.append(c)
        return {"query": query, "reranked": True, "candidates_considered": len(cands), "tools": out}
    return {"query": query, "reranked": False, "candidates_considered": len(cands), "tools": cands[:top_k]}


def hybrid_search(query: str, semantic_k: int = 17, fts_k: int = 11, top_k: int = 10,
                  rerank: bool = False) -> Dict[str, Any]:
    qvec = _embed_query(query)
    sem = semantic_search(query, semantic_k, qvec=qvec)
    fts = fts_search(query, fts_k)
    RRF = 60
    fused: Dict[str, Dict] = {}
    for rank, s in enumerate(sem):
        fused[s["tool_id"]] = {**s, "sem_rank": rank}
    for rank, f in enumerate(fts):
        e = fused.setdefault(f["tool_id"], {"tool_id": f["tool_id"], "backend": f.get("backend"), "name": f.get("name")})
        e["fts_rank"] = rank
    for e in fused.values():
        score = 0.0
        if "sem_rank" in e:
            score += 1.0 / (RRF + e["sem_rank"])
        if "fts_rank" in e:
            score += 1.0 / (RRF + e["fts_rank"])
        e["fused_score"] = round(score, 6)
        e["signals"] = {"semantic": "sem_rank" in e, "lexical": "fts_rank" in e}
    ranked = sorted(fused.values(), key=lambda x: x["fused_score"], reverse=True)
    if rerank and ranked:
        rr = _rerank(query, [_doc(r) for r in ranked[:max(top_k * 3, 20)]], top_k)
        if rr is not None:
            reordered = []
            for item in rr:
                i = item.get("index")
                if i is not None and i < len(ranked):
                    x = dict(ranked[i]); x["rerank_score"] = item.get("relevance_score"); reordered.append(x)
            ranked = reordered or ranked
    return {"query": query, "method": "neon-pgvector+fts+rrf" + ("+rerank" if rerank else ""),
            "hybrid": ranked[:top_k],
            "semantic_only": [{"tool_id": s["tool_id"], "backend": s.get("backend")} for s in sem[:top_k]],
            "lexical_only": [{"tool_id": f["tool_id"], "backend": f.get("backend")} for f in fts[:top_k]]}


import re
_SPLIT = re.compile(r"\s*(?:->|\bthen\b|\band then\b|;|→|(?<!\w),(?!\w))\s*", re.IGNORECASE)


def multi_hop_retrieve(goal: str, steps: Optional[List[str]] = None, per_hop: int = 3) -> Dict[str, Any]:
    hops = [s.strip() for s in steps if s.strip()] if steps else \
        [p.strip() for p in _SPLIT.split(goal) if p.strip()] or [goal]
    chain = []
    for i, hop in enumerate(hops):
        res = retrieve_rerank(hop, top_k=per_hop)
        tools = res.get("tools", [])
        chosen = tools[0] if tools else None
        chain.append({"hop": i + 1, "goal": hop,
                      "tool_id": chosen.get("tool_id") if chosen else None,
                      "backend": chosen.get("backend") if chosen else None,
                      "name": (chosen.get("tool") or {}).get("name") if chosen else None,
                      "rerank_score": chosen.get("rerank_score") if chosen else None,
                      "alternatives": [{"tool_id": t["tool_id"], "backend": t.get("backend")} for t in tools[1:per_hop]]})
    return {"goal": goal, "hops": len(hops), "chain": chain,
            "backends_spanned": sorted({c["backend"] for c in chain if c.get("backend")})}
