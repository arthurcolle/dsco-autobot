"""Full-text (BM25) index over the tool corpus — the lexical half of hybrid
search. Maintained by the same sync that owns the embeddings (rebuilt from the
current catalog each pass; text-only so it's ~free), and queried at search time
with SQLite FTS5 BM25 ranking.

FTS complements the embedding index: it nails exact tool-name / keyword hits
(e.g. "stripe refund", "s3 upload") that a bi-encoder can rank below fuzzier
semantic neighbors, while the embeddings handle intent ("cancel a payment").
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:  # noqa: BLE001
    DATA_DIR = Path("/tmp/tool_index_data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
FTS_DB = str(DATA_DIR / "tool_fts.db")

_TOKEN = re.compile(r"[A-Za-z0-9_]+")


def _match_expr(query: str) -> str:
    """Sanitize a free-text query into a safe FTS5 MATCH expression: tokenized,
    OR-joined for recall, with a prefix on the last token."""
    toks = [t for t in _TOKEN.findall(query.lower()) if len(t) > 1]
    if not toks:
        return ""
    toks = toks[:16]
    expr = " OR ".join(toks)
    if len(toks[-1]) >= 3:
        expr += f" OR {toks[-1]}*"
    return expr


def rebuild(rows: List[Tuple[str, str, str, str]]) -> int:
    """Rebuild the FTS table from (tool_id, backend, name, text) rows.
    Cheap (text only, no network) so a full rebuild each sync keeps it exact."""
    if not rows:
        return 0
    conn = sqlite3.connect(FTS_DB)
    try:
        conn.execute(
            "CREATE VIRTUAL TABLE IF NOT EXISTS tool_fts "
            "USING fts5(tool_id UNINDEXED, backend UNINDEXED, name, text)"
        )
        conn.execute("DELETE FROM tool_fts")
        conn.executemany(
            "INSERT INTO tool_fts (tool_id, backend, name, text) VALUES (?, ?, ?, ?)", rows
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def search(query: str, k: int = 11) -> List[Dict]:
    """BM25 top-k. Returns [{tool_id, backend, name, fts_score, fts_rank}]."""
    expr = _match_expr(query)
    if not expr:
        return []
    try:
        conn = sqlite3.connect(FTS_DB)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT tool_id, backend, name, bm25(tool_fts) AS score "
            "FROM tool_fts WHERE tool_fts MATCH ? ORDER BY score LIMIT ?",
            (expr, k),
        )
        out = []
        for rank, r in enumerate(cur.fetchall()):
            out.append({
                "tool_id": r["tool_id"], "backend": r["backend"], "name": r["name"],
                "fts_score": r["score"], "fts_rank": rank,  # bm25: lower is better
            })
        conn.close()
        return out
    except Exception as exc:  # noqa: BLE001
        logger.warning("tool_fts search failed: %s", exc)
        return []


def count() -> int:
    try:
        conn = sqlite3.connect(FTS_DB)
        n = conn.execute("SELECT count(*) FROM tool_fts").fetchone()[0]
        conn.close()
        return n
    except Exception:  # noqa: BLE001
        return 0
