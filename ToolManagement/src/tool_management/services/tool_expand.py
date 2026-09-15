"""Pre-embedding document expansion: enrich each tool's `description` with 3-5
synthetic example requests before it is embedded + FTS-indexed.

Why: tool docs are under-specified, so retrievers miss them (Re-Invoke /
ScaleMCP-TDWA / "Tools are under-documented"). Folding example user-phrasings
into the description — the convenient, standard field every retrieval path
already reads — lifts recall with no schema changes and no new facet.

Design:
  * Deterministic template examples by default (free, instant, stable).
  * Optional LLM examples (OpenAI-compatible, env-configured) for higher
    quality; cached by schema hash so a tool is expanded once, not every sync,
    and unchanged tools never regenerate (keeps embed change-detection stable).
  * The final description is hard-capped to TOOL_DESC_MAX_TOKENS, with the
    examples preserved (the base description is truncated first).
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

TOOL_DESC_MAX_TOKENS = int(os.getenv("TOOL_DESC_MAX_TOKENS", "2048"))
_CHARS_PER_TOKEN = 4  # rough; keeps us safely under real tokenizer counts
NUM_EXAMPLES = max(3, min(5, int(os.getenv("TOOL_EXPAND_EXAMPLES", "4"))))

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:  # noqa: BLE001
    DATA_DIR = Path("/tmp/tool_index_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
_CACHE_DB = str(DATA_DIR / "tool_expansions.db")

_EXPAND_MARK = "\n\nExample requests:\n"


# ── deterministic template examples ─────────────────────────────────────────

def template_examples(name: str, description: str, props: List[str]) -> List[str]:
    """Natural-language phrasings derived from a structured tool name
    (provider_domain_action) + description + params."""
    raw = re.sub(r"[^a-z0-9]+", "_", (name or "").lower())
    segs = [s for s in raw.split("_") if s]
    action = segs[-1] if segs else (name or "")
    obj = " ".join(segs[:-1]) if len(segs) > 1 else (name or "")
    provider = segs[0] if len(segs) > 2 else ""
    cands: List[str] = [f"{action} {obj}", f"how do I {action} {obj}"]
    if provider:
        mid = " ".join(segs[1:-1])
        cands.append(f"{action} {mid} on {provider}".replace("  ", " "))
    if props:
        cands.append(f"{action} {obj} with {', '.join(props[:3])}")
    if description:
        first = description.strip().split(".")[0].strip().lower()
        if first:
            cands.append(first)
    seen, out = set(), []
    for c in cands:
        c = " ".join(c.split())
        if c and c.lower() not in seen:
            seen.add(c.lower())
            out.append(c)
        if len(out) >= NUM_EXAMPLES:
            break
    return out


# ── optional LLM examples (OpenAI-compatible, env-configured) ────────────────

def _llm_examples(name: str, description: str, props: List[str]) -> Optional[List[str]]:
    api_key = os.getenv("EXPAND_LLM_API_KEY")
    base = os.getenv("EXPAND_LLM_URL", "https://openrouter.ai/api/v1")
    model = os.getenv("EXPAND_LLM_MODEL", "openai/gpt-4o-mini")
    if not api_key:
        return None
    try:
        import httpx

        prompt = (
            f"Tool: {name}\nDescription: {description}\nParameters: {', '.join(props[:12])}\n\n"
            f"Write {NUM_EXAMPLES} short, diverse real user requests this tool would satisfy. "
            "Vary phrasing and synonyms. Return a JSON array of strings only."
        )
        with httpx.Client(timeout=20) as c:
            r = c.post(
                f"{base.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": model, "messages": [{"role": "user", "content": prompt}],
                      "temperature": 0.7, "max_tokens": 300},
            )
        if r.status_code != 200:
            return None
        text = r.json()["choices"][0]["message"]["content"]
        m = re.search(r"\[.*\]", text, re.DOTALL)
        arr = json.loads(m.group(0)) if m else []
        out = [str(x).strip() for x in arr if str(x).strip()][:NUM_EXAMPLES]
        return out or None
    except Exception as exc:  # noqa: BLE001
        logger.debug("LLM expand failed for %s: %s", name, exc)
        return None


# ── build + cap ─────────────────────────────────────────────────────────────

def build_expanded(description: str, examples: List[str], max_tokens: int = TOOL_DESC_MAX_TOKENS) -> str:
    """description + example block, hard-capped to max_tokens. Examples are
    preserved; the base description is truncated first if the cap is tight."""
    max_chars = max(64, max_tokens * _CHARS_PER_TOKEN)
    ex_block = ""
    if examples:
        ex_block = _EXPAND_MARK + "\n".join(f"- {e}" for e in examples)
    base_budget = max(0, max_chars - len(ex_block))
    base = (description or "").strip()
    if len(base) > base_budget:
        base = base[:base_budget].rstrip()
    return (base + ex_block)[:max_chars]


# ── cache (schema-hash keyed) ───────────────────────────────────────────────

def _cache_get(tool_id: str, schema_hash: str) -> Optional[str]:
    try:
        conn = sqlite3.connect(_CACHE_DB)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tool_expansions "
            "(tool_id TEXT PRIMARY KEY, schema_hash TEXT, expanded TEXT)"
        )
        row = conn.execute(
            "SELECT schema_hash, expanded FROM tool_expansions WHERE tool_id=?", (tool_id,)
        ).fetchone()
        conn.close()
        if row and row[0] == schema_hash:
            return row[1]
    except Exception as exc:  # noqa: BLE001
        logger.debug("expand cache get failed: %s", exc)
    return None


def _cache_put(tool_id: str, schema_hash: str, expanded: str) -> None:
    try:
        conn = sqlite3.connect(_CACHE_DB)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tool_expansions "
            "(tool_id TEXT PRIMARY KEY, schema_hash TEXT, expanded TEXT)"
        )
        conn.execute(
            "INSERT OR REPLACE INTO tool_expansions (tool_id, schema_hash, expanded) VALUES (?,?,?)",
            (tool_id, schema_hash, expanded),
        )
        conn.commit()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        logger.debug("expand cache put failed: %s", exc)


def expand_description(
    tool_id: str, name: str, description: str, props: List[str], schema_hash: str
) -> str:
    """Return the token-capped, example-enriched description for a tool.
    Cached by schema_hash so generation runs once per tool (and again only when
    its schema changes). Idempotent for deterministic template generation."""
    cached = _cache_get(tool_id, schema_hash)
    if cached is not None:
        return cached
    examples = _llm_examples(name, description, props) or template_examples(name, description, props)
    expanded = build_expanded(description, examples)
    _cache_put(tool_id, schema_hash, expanded)
    return expanded
