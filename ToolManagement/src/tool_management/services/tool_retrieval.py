"""Hardened tool retrieval: embed-retrieve -> cross-encoder rerank, plus
multi-hop planning that assembles a cross-backend tool chain for a goal.

Retrieval stages:
  1. RETRIEVE  — bi-encoder cosine over the stored faceted embeddings
                 (advanced_tool_index.search) returns a wide candidate set.
  2. RERANK    — Jina cross-encoder (jina-reranker) rescoring each candidate's
                 full schema text against the query. Far more precise than
                 cosine alone; degrades gracefully to cosine order if the
                 reranker is unavailable.

Multi-hop:
  A goal is split into ordered sub-goals (explicit `steps`, or a heuristic
  split on connectors). Each hop runs retrieve->rerank independently, so the
  chain can span backends (research -> finance -> communication ...). Adjacent
  hops are annotated with type-compatibility from the composition graph when
  present, but the chain never depends on learned execution history existing.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

RERANK_MODEL = os.getenv("TOOL_RERANK_MODEL", "jina-reranker-v2-base-multilingual")
RERANK_URL = os.getenv("JINA_RERANK_URL", "https://api.jina.ai/v1/rerank")
DEFAULT_RECALL = int(os.getenv("TOOL_RETRIEVE_RECALL", "40"))


def _tool_doc(t: Dict[str, Any]) -> str:
    """Compact reranker document for a tool: name + description + params."""
    tool = t.get("tool", t)
    name = tool.get("name", "")
    desc = tool.get("description", "")
    props = ((tool.get("inputSchema") or {}).get("properties") or {})
    params = ", ".join(list(props.keys())[:12])
    backend = t.get("backend", "")
    return f"{name} [{backend}]: {desc}" + (f" (params: {params})" if params else "")


async def _jina_rerank(query: str, docs: List[str], top_k: int) -> Optional[List[Dict]]:
    """Return [{index, relevance_score}] top_k, or None if unavailable."""
    key = os.getenv("JINA_API_KEY")
    if not key or not docs:
        return None
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(
                RERANK_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": RERANK_MODEL,
                    "query": query,
                    "documents": docs,
                    "top_n": top_k,
                    "return_documents": False,
                },
            )
        if r.status_code != 200:
            logger.warning("rerank HTTP %s: %s", r.status_code, r.text[:160])
            return None
        return r.json().get("results", [])
    except Exception as exc:  # noqa: BLE001
        logger.warning("rerank failed: %s", exc)
        return None


async def retrieve_rerank(
    query: str,
    top_k: int = 8,
    recall: int = DEFAULT_RECALL,
    backend_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """RETRIEVE (cosine, wide) -> RERANK (cross-encoder, precise) -> top_k."""
    from ..embeddings.advanced_tool_index import get_advanced_index

    index = get_advanced_index()
    candidates = await index.search(query, limit=recall, backend_filter=backend_filter)
    if not candidates:
        return {"query": query, "reranked": False, "tools": []}

    docs = [_tool_doc(c) for c in candidates]
    ranked = await _jina_rerank(query, docs, top_k)

    if ranked is not None:
        out = []
        for r in ranked:
            idx = r.get("index")
            if idx is None or idx >= len(candidates):
                continue
            c = dict(candidates[idx])
            c["rerank_score"] = r.get("relevance_score")
            c["retrieve_score"] = c.get("score")
            out.append(c)
        return {"query": query, "reranked": True, "candidates_considered": len(candidates), "tools": out}

    # graceful fallback: cosine order
    return {"query": query, "reranked": False, "candidates_considered": len(candidates), "tools": candidates[:top_k]}


def _usage_scores(index, tool_ids: List[str]) -> Dict[str, float]:
    """Learned preference per tool: success-weighted, log-damped call volume
    from the execution feedback recorded in tool_facets. 0 until a tool is used."""
    import sqlite3
    scores: Dict[str, float] = {}
    if not tool_ids:
        return scores
    try:
        conn = sqlite3.connect(str(index.db_path))
        q = f"SELECT tool_id, call_count, success_count FROM tool_facets WHERE tool_id IN ({','.join('?' * len(tool_ids))})"
        for tid, calls, succ in conn.execute(q, tool_ids).fetchall():
            calls = calls or 0
            succ = succ or 0
            if calls <= 0:
                scores[tid] = 0.0
                continue
            success_rate = succ / calls
            import math
            scores[tid] = success_rate * (math.log1p(calls) / math.log1p(50))
        conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("usage_scores failed: %s", exc)
    return scores


async def hybrid_search(
    query: str,
    semantic_k: int = 17,
    fts_k: int = 11,
    top_k: int = 10,
    pref_weight: float = 0.25,
    rerank: bool = False,
) -> Dict[str, Any]:
    """Fuse the semantic top-`semantic_k` and lexical (FTS/BM25) top-`fts_k`
    rankings with Reciprocal Rank Fusion, then apply a learned-preference boost
    from execution feedback. Returns the fused ranking AND both source rankings
    so the two can be cross-compared."""
    from ..embeddings.advanced_tool_index import get_advanced_index
    from . import tool_fts

    index = get_advanced_index()
    sem = await index.search(query, limit=semantic_k)
    fts = tool_fts.search(query, fts_k)

    RRF_K = 60
    fused: Dict[str, Dict[str, Any]] = {}
    for rank, s in enumerate(sem):
        tid = s["tool_id"]
        fused[tid] = {
            "tool_id": tid, "backend": s.get("backend"),
            "name": (s.get("tool") or {}).get("name"),
            "tool": s.get("tool"), "sem_rank": rank, "semantic_score": s.get("semantic_score"),
        }
    for f in fts:
        tid = f["tool_id"]
        e = fused.setdefault(tid, {"tool_id": tid, "backend": f.get("backend"), "name": f.get("name")})
        e["fts_rank"] = f["fts_rank"]
        e["fts_score"] = f["fts_score"]

    usage = _usage_scores(index, list(fused.keys()))

    for tid, e in fused.items():
        rrf = 0.0
        if "sem_rank" in e:
            rrf += 1.0 / (RRF_K + e["sem_rank"])
        if "fts_rank" in e:
            rrf += 1.0 / (RRF_K + e["fts_rank"])
        pref = usage.get(tid, 0.0)
        e["pref_score"] = round(pref, 4)
        e["fused_score"] = round(rrf + pref_weight * pref, 6)
        e["signals"] = {
            "semantic": "sem_rank" in e,
            "lexical": "fts_rank" in e,
            "learned_pref": pref > 0,
        }

    ranked = sorted(fused.values(), key=lambda x: x["fused_score"], reverse=True)

    if rerank and ranked:
        docs = [_tool_doc(r) for r in ranked[: max(top_k * 3, 20)]]
        rr = await _jina_rerank(query, docs, top_k)
        if rr is not None:
            reordered = []
            for item in rr:
                idx = item.get("index")
                if idx is not None and idx < len(ranked):
                    r = dict(ranked[idx])
                    r["rerank_score"] = item.get("relevance_score")
                    reordered.append(r)
            ranked = reordered or ranked

    return {
        "query": query,
        "method": "hybrid(rrf + learned_pref" + (" + rerank" if rerank else "") + ")",
        "params": {"semantic_k": semantic_k, "fts_k": fts_k, "pref_weight": pref_weight},
        "hybrid": ranked[:top_k],
        "semantic_only": [{"tool_id": s["tool_id"], "backend": s.get("backend")} for s in sem[:top_k]],
        "lexical_only": [{"tool_id": f["tool_id"], "backend": f.get("backend")} for f in fts[:top_k]],
    }


_SPLIT_RE = re.compile(r"\s*(?:->|\bthen\b|\band then\b|;|→|(?<!\w),(?!\w))\s*", re.IGNORECASE)


def decompose_goal(goal: str, steps: Optional[List[str]] = None) -> List[str]:
    """Ordered sub-goals: explicit steps win; else split on chain connectors."""
    if steps:
        return [s.strip() for s in steps if s and s.strip()]
    parts = [p.strip() for p in _SPLIT_RE.split(goal) if p and p.strip()]
    return parts if len(parts) > 1 else [goal.strip()]


async def multi_hop_retrieve(
    goal: str,
    steps: Optional[List[str]] = None,
    per_hop: int = 3,
) -> Dict[str, Any]:
    """Assemble a cross-backend tool chain: retrieve->rerank the best tool for
    each hop. Annotates each hop's alternatives and whether the prior hop's
    output is type-composable into this one (when the graph knows)."""
    from ..embeddings.advanced_tool_index import get_advanced_index

    hops_text = decompose_goal(goal, steps)
    index = get_advanced_index()
    chain: List[Dict[str, Any]] = []
    prev_tool_id: Optional[str] = None

    for i, hop in enumerate(hops_text):
        res = await retrieve_rerank(hop, top_k=per_hop)
        tools = res.get("tools", [])
        if not tools:
            chain.append({"hop": i + 1, "goal": hop, "tool": None, "alternatives": []})
            continue
        chosen = tools[0]
        composable = None
        if prev_tool_id:
            try:
                fwd = await index.find_composable_tools(prev_tool_id, direction="forward", limit=20)
                composable = any(c.get("tool_id") == chosen.get("tool_id") for c in fwd)
            except Exception:  # noqa: BLE001
                composable = None
        chain.append({
            "hop": i + 1,
            "goal": hop,
            "tool_id": chosen.get("tool_id"),
            "backend": chosen.get("backend"),
            "name": (chosen.get("tool") or {}).get("name"),
            "rerank_score": chosen.get("rerank_score"),
            "composable_from_prev": composable,
            "alternatives": [
                {"tool_id": t.get("tool_id"), "backend": t.get("backend")}
                for t in tools[1:per_hop]
            ],
        })
        prev_tool_id = chosen.get("tool_id")

    return {
        "goal": goal,
        "hops": len(hops_text),
        "reranked": True,
        "chain": chain,
        "backends_spanned": sorted({c["backend"] for c in chain if c.get("backend")}),
    }
