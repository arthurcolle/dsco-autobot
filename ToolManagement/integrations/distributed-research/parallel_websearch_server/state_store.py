"""SQLite-backed context state and request/response cache."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import HTTPException, Request


def _utc_timestamp() -> str:
    """Return an ISO 8601 timestamp in UTC."""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _dumps(data: Optional[Dict[str, Any]]) -> Optional[str]:
    """Serialize dictionaries to JSON strings."""
    if data is None:
        return None
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def _loads(raw: Optional[str]) -> Dict[str, Any]:
    """Deserialize JSON strings into dictionaries."""
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


class ContextStateStore:
    """Asynchronous helper that persists contexts and interactions in SQLite."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = Lock()

    async def initialize(self) -> None:
        """Create the SQLite connection and bootstrap schema."""
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        """Synchronous portion of initialization so it can run in a thread."""
        if self._conn is not None:
            return

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
            isolation_level=None,  # autocommit
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS contexts (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                metadata TEXT,
                state TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS interactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context_id TEXT,
                interaction_type TEXT NOT NULL,
                status TEXT NOT NULL,
                summary TEXT,
                input_payload TEXT,
                output_payload TEXT,
                remote_run_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (context_id) REFERENCES contexts(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_interactions_context
                ON interactions (context_id, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_interactions_run
                ON interactions (remote_run_id);
            """
        )
        self._conn = conn

    async def close(self) -> None:
        """Close the SQLite connection."""
        if self._conn is None:
            return

        await asyncio.to_thread(self._close_sync)

    def _close_sync(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # ------------------------------------------------------------------ #
    # Context helpers
    # ------------------------------------------------------------------ #

    async def create_context(
        self,
        *,
        context_id: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create and return a context record."""
        ctx_id = context_id or uuid4().hex
        metadata = metadata or {}
        state = state or {}
        now = _utc_timestamp()

        def _create() -> Dict[str, Any]:
            with self._lock:
                try:
                    self._conn.execute(
                        """
                        INSERT INTO contexts (id, name, description, metadata, state, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            ctx_id,
                            name,
                            description,
                            _dumps(metadata),
                            _dumps(state),
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as exc:  # pragma: no cover - defensive
                    raise ValueError(f"context '{ctx_id}' already exists") from exc
            return self._get_context_sync(ctx_id)

        return await asyncio.to_thread(_create)

    async def ensure_context(self, context_id: str) -> Dict[str, Any]:
        """Return existing context or create a blank one."""
        existing = await self.get_context(context_id)
        if existing:
            return existing
        return await self.create_context(context_id=context_id)

    async def get_context(self, context_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single context."""
        return await asyncio.to_thread(self._get_context_sync, context_id)

    def _get_context_sync(self, context_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM contexts WHERE id = ?",
                (context_id,),
            )
            row = cursor.fetchone()
        if not row:
            return None
        return self._row_to_context(row)

    async def list_contexts(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Return paginated list of contexts."""
        def _list() -> List[Dict[str, Any]]:
            with self._lock:
                cursor = self._conn.execute(
                    """
                    SELECT * FROM contexts
                    ORDER BY updated_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                )
                rows = cursor.fetchall()
            return [self._row_to_context(row) for row in rows]

        return await asyncio.to_thread(_list)

    async def update_context(
        self,
        context_id: str,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
        replace_metadata: bool = False,
        replace_state: bool = False,
    ) -> Dict[str, Any]:
        """Update mutable fields on a context."""
        def _update() -> Dict[str, Any]:
            row = self._get_context_sync(context_id)
            if not row:
                raise KeyError(context_id)

            existing_meta = dict(row["metadata"] or {})
            existing_state = dict(row["state"] or {})
            if replace_metadata:
                meta = dict(metadata or {})
            else:
                meta = {**existing_meta, **(metadata or {})}
            if replace_state:
                ctx_state = dict(state or {})
            else:
                ctx_state = {**existing_state, **(state or {})}
            now = _utc_timestamp()

            with self._lock:
                self._conn.execute(
                    """
                    UPDATE contexts
                    SET name = COALESCE(?, name),
                        description = COALESCE(?, description),
                        metadata = ?,
                        state = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        description,
                        _dumps(meta),
                        _dumps(ctx_state),
                        now,
                        context_id,
                    ),
                )
            return {
                **row,
                "name": name if name is not None else row["name"],
                "description": description if description is not None else row["description"],
                "metadata": meta,
                "state": ctx_state,
                "updated_at": now,
            }

        return await asyncio.to_thread(_update)

    # ------------------------------------------------------------------ #
    # Interaction helpers
    # ------------------------------------------------------------------ #

    async def record_interaction(
        self,
        *,
        context_id: Optional[str],
        interaction_type: str,
        status: str,
        input_payload: Dict[str, Any],
        output_payload: Optional[Dict[str, Any]] = None,
        summary: Optional[str] = None,
        remote_run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a single input/output exchange."""
        now = _utc_timestamp()

        def _record() -> Dict[str, Any]:
            with self._lock:
                cursor = self._conn.execute(
                    """
                    INSERT INTO interactions (
                        context_id,
                        interaction_type,
                        status,
                        summary,
                        input_payload,
                        output_payload,
                        remote_run_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        context_id,
                        interaction_type,
                        status,
                        summary,
                        _dumps(input_payload),
                        _dumps(output_payload),
                        remote_run_id,
                        now,
                        now,
                    ),
                )
                interaction_id = cursor.lastrowid
                cursor = self._conn.execute(
                    "SELECT * FROM interactions WHERE id = ?",
                    (interaction_id,),
                )
                row = cursor.fetchone()
            return self._row_to_interaction(row)

        return await asyncio.to_thread(_record)

    async def list_interactions(
        self,
        context_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch interaction history for a context."""
        def _list() -> List[Dict[str, Any]]:
            with self._lock:
                cursor = self._conn.execute(
                    """
                    SELECT * FROM interactions
                    WHERE context_id = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    (context_id, limit, offset),
                )
                rows = cursor.fetchall()
            return [self._row_to_interaction(row) for row in rows]

        return await asyncio.to_thread(_list)

    async def update_run_status(
        self,
        run_id: str,
        *,
        status: Optional[str] = None,
        output_payload: Optional[Dict[str, Any]] = None,
        summary: Optional[str] = None,
    ) -> None:
        """Update an interaction by remote run id. No-op if run unknown."""
        if not run_id:
            return
        now = _utc_timestamp()

        def _update() -> None:
            with self._lock:
                self._conn.execute(
                    """
                    UPDATE interactions
                    SET status = COALESCE(?, status),
                        output_payload = COALESCE(?, output_payload),
                        summary = COALESCE(?, summary),
                        updated_at = ?
                    WHERE remote_run_id = ?
                    """,
                    (
                        status,
                        _dumps(output_payload),
                        summary,
                        now,
                        run_id,
                    ),
                )

        await asyncio.to_thread(_update)

    async def get_context_id_for_run(self, run_id: str) -> Optional[str]:
        """Return the context_id associated with a run, if any."""
        if not run_id:
            return None

        def _lookup() -> Optional[str]:
            with self._lock:
                cursor = self._conn.execute(
                    "SELECT context_id FROM interactions WHERE remote_run_id = ?",
                    (run_id,),
                )
                row = cursor.fetchone()
            if not row:
                return None
            return row["context_id"]

        return await asyncio.to_thread(_lookup)

    # ------------------------------------------------------------------ #
    # Row conversion helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _row_to_context(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "context_id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "metadata": _loads(row["metadata"]),
            "state": _loads(row["state"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _row_to_interaction(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "interaction_id": row["id"],
            "context_id": row["context_id"],
            "interaction_type": row["interaction_type"],
            "status": row["status"],
            "summary": row["summary"],
            "input_payload": _loads(row["input_payload"]),
            "output_payload": _loads(row["output_payload"]),
            "remote_run_id": row["remote_run_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }


def get_state_store(request: Request) -> ContextStateStore:
    """FastAPI dependency for retrieving the shared context store."""
    store = getattr(request.app.state, "context_store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="Context state store is not initialized")
    return store
