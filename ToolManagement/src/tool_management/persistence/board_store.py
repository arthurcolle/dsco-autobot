"""Pandemonium board — the declarative blackboard/market layer.

Typed posts with a state machine (REQUEST → CONTRIBUTION → RESOLUTION), capability
self-selection (agents advertise capabilities; requests carry required_caps), reputation,
and durable attachments (image/file upload, addressable by id). Escrow + settlement of
bounties is done by the route layer through BillingStore; this store holds board state.

Durable in DuckDB on DATA_DIR, matching BillingStore / ForumStore.
"""
import json
import logging
import os
import secrets
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)


class PostType(str, Enum):
    REQUEST = "request"          # a need, optionally with a bounty + required_caps
    CONTRIBUTION = "contribution"  # an answer/artifact that refs a request
    RESOLUTION = "resolution"    # marks a winning contribution (via resolve endpoint)
    ARTIFACT = "artifact"        # externalized reasoning/data, addressable
    NOTE = "note"                # free-form


class PostState(str, Enum):
    OPEN = "open"
    RESOLVED = "resolved"
    CANCELLED = "cancelled"
    NA = "na"


class BoardStore:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path or data_dir / "board.duckdb")
        self.artifact_dir = data_dir / "board_artifacts"
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_db(self) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS board_posts (
                    post_id VARCHAR PRIMARY KEY,
                    board VARCHAR,
                    author VARCHAR,
                    post_type VARCHAR,
                    title VARCHAR,
                    body VARCHAR,
                    refs VARCHAR,            -- json list of post ids this refers to
                    required_caps VARCHAR,   -- json list of capability tags
                    attachments VARCHAR,     -- json list of artifact ids
                    bounty_cents DOUBLE DEFAULT 0,
                    state VARCHAR,
                    winner_post_id VARCHAR,
                    created_at DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_caps (
                    agent_id VARCHAR,
                    capability VARCHAR,
                    PRIMARY KEY (agent_id, capability)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS reputation (
                    agent_id VARCHAR PRIMARY KEY,
                    ema_quality DOUBLE DEFAULT 0.5,
                    fills BIGINT DEFAULT 0,
                    earned_cents DOUBLE DEFAULT 0,
                    updated_at DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS attachments (
                    artifact_id VARCHAR PRIMARY KEY,
                    owner VARCHAR,
                    filename VARCHAR,
                    content_type VARCHAR,
                    size BIGINT,
                    path VARCHAR,
                    created_at DOUBLE
                )
                """
            )

    # ------------------------------------------------------------------ posts
    @staticmethod
    def _row(r: Any) -> Optional[Dict[str, Any]]:
        if not r:
            return None
        return {
            "post_id": r[0], "board": r[1], "author": r[2], "post_type": r[3],
            "title": r[4], "body": r[5],
            "refs": json.loads(r[6] or "[]"), "required_caps": json.loads(r[7] or "[]"),
            "attachments": json.loads(r[8] or "[]"),
            "bounty_cents": r[9], "state": r[10], "winner_post_id": r[11], "created_at": r[12],
        }

    def create_post(
        self, author: str, post_type: str, title: str = "", body: str = "",
        board: str = "general", refs: Optional[List[str]] = None,
        required_caps: Optional[List[str]] = None, attachments: Optional[List[str]] = None,
        bounty_cents: float = 0.0, post_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        post_id = post_id or "bp_" + secrets.token_hex(8)
        state = PostState.OPEN.value if post_type == PostType.REQUEST.value else PostState.NA.value
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO board_posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [post_id, board, author, post_type, title, body,
                 json.dumps(refs or []), json.dumps(required_caps or []),
                 json.dumps(attachments or []), float(bounty_cents), state, None, time.time()],
            )
        return self.get_post(post_id)  # type: ignore[return-value]

    def get_post(self, post_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            r = con.execute(
                "SELECT post_id, board, author, post_type, title, body, refs, required_caps, "
                "attachments, bounty_cents, state, winner_post_id, created_at "
                "FROM board_posts WHERE post_id = ?", [post_id],
            ).fetchone()
        return self._row(r)

    def list_posts(
        self, board: Optional[str] = None, post_type: Optional[str] = None,
        state: Optional[str] = None, limit: int = 100,
    ) -> List[Dict[str, Any]]:
        clauses, params = [], []
        for col, val in (("board", board), ("post_type", post_type), ("state", state)):
            if val is not None:
                clauses.append(f"{col} = ?")
                params.append(val)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._lock, self._connect() as con:
            rows = con.execute(
                "SELECT post_id, board, author, post_type, title, body, refs, required_caps, "
                f"attachments, bounty_cents, state, winner_post_id, created_at FROM board_posts "
                f"{where} ORDER BY created_at DESC LIMIT ?", params,
            ).fetchall()
        return [self._row(r) for r in rows]  # type: ignore[misc]

    def feed(self, caps: List[str], limit: int = 50) -> List[Dict[str, Any]]:
        """Open requests the agent can self-select: required_caps ⊆ the agent's caps
        (or the request has no required caps). This is the token-efficient pull model."""
        cap_set = set(caps)
        open_reqs = self.list_posts(post_type=PostType.REQUEST.value, state=PostState.OPEN.value, limit=500)
        matched = [p for p in open_reqs if set(p["required_caps"]).issubset(cap_set)]
        return matched[:limit]

    def contributions_for(self, request_id: str) -> List[Dict[str, Any]]:
        return [
            p for p in self.list_posts(post_type=PostType.CONTRIBUTION.value, limit=500)
            if request_id in p["refs"]
        ]

    def resolve(self, request_id: str, winner_post_id: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                "UPDATE board_posts SET state = ?, winner_post_id = ? WHERE post_id = ?",
                [PostState.RESOLVED.value, winner_post_id, request_id],
            )

    def set_state(self, post_id: str, state: str) -> None:
        with self._lock, self._connect() as con:
            con.execute("UPDATE board_posts SET state = ? WHERE post_id = ?", [state, post_id])

    # ------------------------------------------------------------------ caps / reputation
    def add_capabilities(self, agent_id: str, caps: List[str]) -> List[str]:
        with self._lock, self._connect() as con:
            for c in caps:
                con.execute("INSERT INTO agent_caps VALUES (?, ?) ON CONFLICT DO NOTHING", [agent_id, c])
        return self.get_capabilities(agent_id)

    def get_capabilities(self, agent_id: str) -> List[str]:
        with self._lock, self._connect() as con:
            rows = con.execute("SELECT capability FROM agent_caps WHERE agent_id = ?", [agent_id]).fetchall()
        return [r[0] for r in rows]

    def bump_reputation(self, agent_id: str, quality: float, earned_cents: float = 0.0) -> Dict[str, Any]:
        q = min(max(float(quality), 0.0), 1.0)
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT ema_quality, fills, earned_cents FROM reputation WHERE agent_id = ?", [agent_id]
            ).fetchone()
            if row:
                ema = 0.7 * float(row[0]) + 0.3 * q
                fills = int(row[1]) + 1
                earned = float(row[2]) + earned_cents
            else:
                ema, fills, earned = q, 1, earned_cents
            con.execute(
                "INSERT INTO reputation VALUES (?, ?, ?, ?, ?) ON CONFLICT (agent_id) DO UPDATE SET "
                "ema_quality = excluded.ema_quality, fills = excluded.fills, "
                "earned_cents = excluded.earned_cents, updated_at = excluded.updated_at",
                [agent_id, ema, fills, earned, time.time()],
            )
        return self.get_reputation(agent_id)

    def get_reputation(self, agent_id: str) -> Dict[str, Any]:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT ema_quality, fills, earned_cents FROM reputation WHERE agent_id = ?", [agent_id]
            ).fetchone()
        if not row:
            return {"agent_id": agent_id, "ema_quality": 0.5, "fills": 0, "earned_cents": 0.0}
        return {"agent_id": agent_id, "ema_quality": row[0], "fills": int(row[1]), "earned_cents": row[2]}

    # ------------------------------------------------------------------ attachments
    def save_attachment(self, owner: str, filename: str, content_type: str, data: bytes) -> Dict[str, Any]:
        artifact_id = "art_" + secrets.token_hex(10)
        ext = ("." + filename.rsplit(".", 1)[-1]) if "." in filename else ""
        path = self.artifact_dir / f"{artifact_id}{ext}"
        path.write_bytes(data)
        rec = {
            "artifact_id": artifact_id, "owner": owner, "filename": filename,
            "content_type": content_type, "size": len(data), "path": str(path),
            "created_at": time.time(),
        }
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO attachments VALUES (?, ?, ?, ?, ?, ?, ?)",
                [rec["artifact_id"], owner, filename, content_type, rec["size"], rec["path"], rec["created_at"]],
            )
        return rec

    def get_attachment(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            r = con.execute(
                "SELECT artifact_id, owner, filename, content_type, size, path, created_at "
                "FROM attachments WHERE artifact_id = ?", [artifact_id],
            ).fetchone()
        if not r:
            return None
        return {"artifact_id": r[0], "owner": r[1], "filename": r[2], "content_type": r[3],
                "size": r[4], "path": r[5], "created_at": r[6]}


_board_store: Optional[BoardStore] = None
_store_lock = threading.Lock()


def get_board_store() -> BoardStore:
    global _board_store
    with _store_lock:
        if _board_store is None:
            _board_store = BoardStore()
        return _board_store
