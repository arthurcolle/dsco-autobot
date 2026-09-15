"""Planning and ranking primitives for the Distributed Search gateway.

This module is intentionally provider-agnostic.  The MCP server owns network
calls; these helpers turn one or more provider result lists into a stable,
deduplicated, domain-diverse evidence set for agents.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


CURRENT_EMBED_MODEL = "jina-embeddings-v5-text-small"
CURRENT_OMNI_EMBED_MODEL = "jina-embeddings-v5-omni-small"
CURRENT_RERANK_MODEL = "jina-reranker-v3.5"
RETRIEVAL_CONTRACT_VERSION = "2026-09-03"

_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "source",
}
_THOROUGH_TERMS = {
    "comprehensive", "deep dive", "investigate", "literature review",
    "due diligence", "all evidence", "exhaustive",
}
_BALANCED_TERMS = {
    "compare", "current", "evidence", "fact check", "latest", "research",
    "sources", "verify", "why", "how",
}
_TIME_TERMS = {
    "current", "latest", "new", "news", "recent", "today", "this week",
    "this month", "this year",
}


@dataclass(frozen=True)
class RetrievalPlan:
    mode: str
    queries: List[str]
    search_width: int
    rerank: bool
    hydrate_pages: int
    reader_engine: str
    max_per_domain: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _contains_any(text: str, terms: Iterable[str]) -> bool:
    lowered = text.casefold()
    return any(term in lowered for term in terms)


def choose_mode(query: str, requested: str = "auto") -> str:
    """Choose a bounded latency/quality profile without an LLM call."""
    if requested in {"fast", "balanced", "thorough"}:
        return requested
    if requested != "auto":
        raise ValueError("mode must be auto, fast, balanced, or thorough")
    if _contains_any(query, _THOROUGH_TERMS):
        return "thorough"
    if _contains_any(query, _BALANCED_TERMS) or len(query.split()) >= 9:
        return "balanced"
    return "fast"


def build_retrieval_plan(
    query: str,
    *,
    mode: str = "auto",
    max_results: int = 8,
    include_content: Optional[bool] = None,
    rerank: Optional[bool] = None,
    query_variants: Optional[Sequence[str]] = None,
) -> RetrievalPlan:
    """Build an inspectable plan for a retrieval request."""
    query = " ".join(str(query or "").split())
    if not query:
        raise ValueError("query is required")
    if len(query) > 4000:
        raise ValueError("query must be at most 4000 characters")

    selected_mode = choose_mode(query, mode)
    max_results = max(1, min(int(max_results), 20))
    settings = {
        "fast": {"width": max(max_results, 5), "rerank": False,
                 "hydrate": 0, "engine": "direct", "per_domain": 2},
        "balanced": {"width": min(max(max_results * 2, 10), 20), "rerank": True,
                     "hydrate": min(3, max_results), "engine": "direct", "per_domain": 2},
        "thorough": {"width": 20, "rerank": True,
                     "hydrate": min(5, max_results), "engine": "browser", "per_domain": 1},
    }[selected_mode]

    queries = [query]
    for variant in query_variants or []:
        normalized = " ".join(str(variant).split())
        if normalized and normalized.casefold() not in {q.casefold() for q in queries}:
            queries.append(normalized)

    # Automatic expansion is deliberately small and legible.  It improves
    # freshness and provenance without silently changing the user's subject.
    if selected_mode != "fast" and _contains_any(query, _TIME_TERMS):
        year = datetime.now(timezone.utc).year
        queries.append(f"{query} {year}")
    if selected_mode == "thorough" and len(queries) < 3:
        queries.append(f"{query} primary sources")

    max_queries = {"fast": 1, "balanced": 2, "thorough": 3}[selected_mode]
    queries = list(dict.fromkeys(queries))[:max_queries]

    hydrate_pages = settings["hydrate"] if include_content is not False else 0
    if include_content is True and hydrate_pages == 0:
        hydrate_pages = min(2, max_results)

    return RetrievalPlan(
        mode=selected_mode,
        queries=queries,
        search_width=settings["width"],
        rerank=settings["rerank"] if rerank is None else bool(rerank),
        hydrate_pages=hydrate_pages,
        reader_engine=settings["engine"],
        max_per_domain=settings["per_domain"],
    )


def domain_for_url(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").casefold().rstrip(".")
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def canonicalize_url(url: str) -> str:
    """Remove fragments and common tracking parameters for result dedupe."""
    try:
        parsed = urlparse(str(url).strip())
    except ValueError:
        return str(url).strip()
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return str(url).strip()
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.casefold().startswith("utm_") and key.casefold() not in _TRACKING_PARAMS
    ]
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((parsed.scheme.casefold(), parsed.netloc.casefold(), path, "", urlencode(query), ""))


def domain_matches(domain: str, policies: Sequence[str]) -> bool:
    normalized = domain.casefold().rstrip(".")
    return any(
        normalized == p.casefold().lstrip(".").rstrip(".")
        or normalized.endswith("." + p.casefold().lstrip(".").rstrip("."))
        for p in policies
    )


def fuse_search_results(
    result_sets: Sequence[tuple[str, Sequence[Dict[str, Any]]]],
    *,
    allow_domains: Optional[Sequence[str]] = None,
    block_domains: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Deduplicate query variants and rank candidates with reciprocal-rank fusion."""
    fused: Dict[str, Dict[str, Any]] = {}
    for source_query, results in result_sets:
        for rank, raw in enumerate(results, start=1):
            if not isinstance(raw, dict):
                continue
            url = raw.get("url") or raw.get("link") or raw.get("href")
            if not isinstance(url, str) or not url.strip():
                continue
            canonical = canonicalize_url(url)
            domain = domain_for_url(canonical)
            if not domain:
                continue
            if allow_domains and not domain_matches(domain, allow_domains):
                continue
            if block_domains and domain_matches(domain, block_domains):
                continue

            candidate = fused.get(canonical)
            score = 1.0 / (60.0 + rank)
            if candidate is None:
                candidate = {
                    "title": raw.get("title") or raw.get("name") or domain,
                    "url": canonical,
                    "domain": domain,
                    "snippet": raw.get("description") or raw.get("snippet") or "",
                    "content": raw.get("content") or raw.get("text") or "",
                    "published_at": raw.get("publishedTime") or raw.get("published_at")
                                    or raw.get("date"),
                    "rrf_score": 0.0,
                    "source_queries": [],
                    "best_search_rank": rank,
                }
                fused[canonical] = candidate
            candidate["rrf_score"] += score
            candidate["best_search_rank"] = min(candidate["best_search_rank"], rank)
            if source_query not in candidate["source_queries"]:
                candidate["source_queries"].append(source_query)
            if len(str(raw.get("content") or "")) > len(str(candidate.get("content") or "")):
                candidate["content"] = raw.get("content")
            if not candidate.get("snippet"):
                candidate["snippet"] = raw.get("description") or raw.get("snippet") or ""

    return sorted(
        fused.values(),
        key=lambda item: (-float(item["rrf_score"]), int(item["best_search_rank"]), item["url"]),
    )


def apply_rerank(
    candidates: Sequence[Dict[str, Any]],
    rerank_results: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply Jina's returned original indices without trusting returned documents."""
    ordered: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for result in rerank_results:
        try:
            index = int(result["index"])
        except (KeyError, TypeError, ValueError):
            continue
        if index < 0 or index >= len(candidates) or index in seen:
            continue
        item = dict(candidates[index])
        if result.get("relevance_score") is not None:
            item["relevance_score"] = float(result["relevance_score"])
        ordered.append(item)
        seen.add(index)
    ordered.extend(dict(item) for index, item in enumerate(candidates) if index not in seen)
    return ordered


def select_domain_diverse(
    candidates: Sequence[Dict[str, Any]],
    *,
    limit: int,
    max_per_domain: int = 2,
    prefer_domains: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Select a diverse first page, then backfill if diversity is impossible."""
    limit = max(1, int(limit))
    max_per_domain = max(1, int(max_per_domain))
    preferred = list(prefer_domains or [])
    ranked = list(candidates)
    if preferred:
        ranked.sort(key=lambda item: not domain_matches(str(item.get("domain", "")), preferred))

    selected: List[Dict[str, Any]] = []
    overflow: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}
    for item in ranked:
        domain = str(item.get("domain") or "")
        if counts.get(domain, 0) >= max_per_domain:
            overflow.append(item)
            continue
        selected.append(dict(item))
        counts[domain] = counts.get(domain, 0) + 1
        if len(selected) >= limit:
            break
    if len(selected) < limit:
        selected.extend(dict(item) for item in overflow[: limit - len(selected)])
    for rank, item in enumerate(selected, start=1):
        item["rank"] = rank
    return selected


def compact_evidence(item: Dict[str, Any], content_chars: int = 700) -> Dict[str, Any]:
    """Return the stable, agent-facing evidence shape."""
    result = {
        key: item[key]
        for key in (
            "rank", "title", "url", "domain", "snippet", "published_at",
            "relevance_score", "rrf_score", "source_queries",
        )
        if item.get(key) not in (None, "", [])
    }
    content = str(item.get("content") or "").strip()
    if content:
        result["content_preview"] = content[:content_chars].rstrip() + ("…" if len(content) > content_chars else "")
        result["content_chars"] = len(content)
    return result
