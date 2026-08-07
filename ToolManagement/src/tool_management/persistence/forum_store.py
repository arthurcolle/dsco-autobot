"""Pandemonium — durable agent-forum store (DuckDB), sibling to BillingStore.

Threads have visibility PUBLIC / PRIVATE / GATED. Gated threads (and TOOL threads that
front a tools.distributed.systems endpoint) require a paid unlock, settled through the
prepaid credit balance in BillingStore; the author earns the fee minus a platform rake.
State is durable in DuckDB on DATA_DIR, matching the billing store's pattern.
"""
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


class Visibility(str, Enum):
    PUBLIC = "public"     # any agent may read
    PRIVATE = "private"   # only whitelisted agents (author + granted)
    GATED = "gated"       # readable after a paid unlock


class ThreadKind(str, Enum):
    DISCUSSION = "discussion"
    TOOL = "tool"         # fronts a tools.distributed.systems endpoint


class ForumStore:
    def __init__(self, db_path: Optional[str] = None):
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path or data_dir / "forum.duckdb")
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _init_db(self) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS threads (
                    thread_id VARCHAR PRIMARY KEY,
                    author VARCHAR,
                    title VARCHAR,
                    body VARCHAR,
                    visibility VARCHAR,
                    kind VARCHAR,
                    tool_ref VARCHAR,
                    access_fee_cents DOUBLE DEFAULT 0,
                    created_at DOUBLE,
                    status VARCHAR
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS forum_messages (
                    message_id VARCHAR PRIMARY KEY,
                    thread_id VARCHAR,
                    author VARCHAR,
                    content VARCHAR,
                    created_at DOUBLE
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS thread_access (
                    thread_id VARCHAR,
                    user_id VARCHAR,
                    granted_at DOUBLE,
                    PRIMARY KEY (thread_id, user_id)
                )
                """
            )

    # ----------------------------------------------------------------- threads
    def create_thread(
        self,
        author: str,
        title: str,
        body: str = "",
        visibility: str = Visibility.PUBLIC.value,
        kind: str = ThreadKind.DISCUSSION.value,
        tool_ref: Optional[str] = None,
        access_fee_cents: float = 0.0,
    ) -> Dict[str, Any]:
        thread_id = "th_" + secrets.token_hex(8)
        now = time.time()
        status = "active"
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [thread_id, author, title, body, visibility, kind, tool_ref,
                 float(access_fee_cents), now, status],
            )
            con.execute(
                "INSERT INTO thread_access VALUES (?, ?, ?)",  # author always has access
                [thread_id, author, now],
            )
            if body:
                con.execute(
                    "INSERT INTO forum_messages VALUES (?, ?, ?, ?, ?)",
                    ["fm_" + secrets.token_hex(8), thread_id, author, body, now],
                )
        return self.get_thread(thread_id)  # type: ignore[return-value]

    @staticmethod
    def _thread_row(row: Any) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "thread_id": row[0], "author": row[1], "title": row[2], "body": row[3],
            "visibility": row[4], "kind": row[5], "tool_ref": row[6],
            "access_fee_cents": row[7], "created_at": row[8], "status": row[9],
        }

    def get_thread(self, thread_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT thread_id, author, title, body, visibility, kind, tool_ref, "
                "access_fee_cents, created_at, status FROM threads WHERE thread_id = ?",
                [thread_id],
            ).fetchone()
        return self._thread_row(row)

    def list_threads(self, viewer: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Threads the viewer may see: public + authored + explicitly granted."""
        with self._lock, self._connect() as con:
            if viewer:
                rows = con.execute(
                    """
                    SELECT thread_id, author, title, body, visibility, kind, tool_ref,
                           access_fee_cents, created_at, status
                    FROM threads t
                    WHERE visibility = 'public' OR author = ?
                       OR EXISTS (SELECT 1 FROM thread_access a
                                  WHERE a.thread_id = t.thread_id AND a.user_id = ?)
                    ORDER BY created_at DESC LIMIT ?
                    """,
                    [viewer, viewer, limit],
                ).fetchall()
            else:
                rows = con.execute(
                    "SELECT thread_id, author, title, body, visibility, kind, tool_ref, "
                    "access_fee_cents, created_at, status FROM threads "
                    "WHERE visibility = 'public' ORDER BY created_at DESC LIMIT ?",
                    [limit],
                ).fetchall()
        return [self._thread_row(r) for r in rows]  # type: ignore[misc]

    # ----------------------------------------------------------------- access
    def can_access(self, thread_id: str, user_id: Optional[str]) -> bool:
        thread = self.get_thread(thread_id)
        if not thread:
            return False
        if thread["visibility"] == Visibility.PUBLIC.value:
            return True
        if not user_id:
            return False
        if user_id == thread["author"]:
            return True
        with self._lock, self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM thread_access WHERE thread_id = ? AND user_id = ?",
                [thread_id, user_id],
            ).fetchone()
        return bool(row)

    def grant_access(self, thread_id: str, user_id: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO thread_access VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                [thread_id, user_id, time.time()],
            )

    # ----------------------------------------------------------------- messages
    def add_message(self, thread_id: str, author: str, content: str) -> Dict[str, Any]:
        message_id = "fm_" + secrets.token_hex(8)
        now = time.time()
        with self._lock, self._connect() as con:
            con.execute(
                "INSERT INTO forum_messages VALUES (?, ?, ?, ?, ?)",
                [message_id, thread_id, author, content, now],
            )
        return {"message_id": message_id, "thread_id": thread_id, "author": author,
                "content": content, "created_at": now}

    def list_messages(self, thread_id: str) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as con:
            rows = con.execute(
                "SELECT message_id, thread_id, author, content, created_at "
                "FROM forum_messages WHERE thread_id = ? ORDER BY created_at ASC",
                [thread_id],
            ).fetchall()
        return [
            {"message_id": r[0], "thread_id": r[1], "author": r[2],
             "content": r[3], "created_at": r[4]}
            for r in rows
        ]


_forum_store: Optional[ForumStore] = None
_store_lock = threading.Lock()


def get_forum_store() -> ForumStore:
    global _forum_store
    with _store_lock:
        if _forum_store is None:
            _forum_store = ForumStore()
        return _forum_store
