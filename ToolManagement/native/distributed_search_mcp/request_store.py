"""Shared durable storage for Distributed Search evidence payloads."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional


class PostgresSearchRequestStore:
    """Postgres-backed request store safe across gateway replicas."""

    backend = "postgres"
    durable = True
    ready = True

    def __init__(self, database_url: str):
        if not database_url:
            raise ValueError("database_url is required")
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - deployment dependency
            raise RuntimeError("psycopg is required for the Postgres search store") from exc
        self._psycopg = psycopg
        self.database_url = database_url
        self._lock = threading.Lock()
        self._ensure_table()

    def _connect(self):
        return self._psycopg.connect(self.database_url, autocommit=True)

    def _ensure_table(self) -> None:
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS search_gateway_requests (
                    request_id TEXT PRIMARY KEY,
                    response_id TEXT UNIQUE NOT NULL,
                    tenant_id TEXT NOT NULL DEFAULT '__system__',
                    principal_id TEXT,
                    trace_id TEXT,
                    tool TEXT NOT NULL,
                    arguments_json TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    full_result_json TEXT NOT NULL,
                    sources_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                "ALTER TABLE search_gateway_requests ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '__system__'"
            )
            cur.execute(
                "ALTER TABLE search_gateway_requests ADD COLUMN IF NOT EXISTS principal_id TEXT"
            )
            cur.execute(
                "ALTER TABLE search_gateway_requests ADD COLUMN IF NOT EXISTS trace_id TEXT"
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_search_gateway_requests_tool_created
                ON search_gateway_requests (tool, created_at DESC)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_search_gateway_requests_tenant_created
                ON search_gateway_requests (tenant_id, created_at DESC)
                """
            )

    def save(
        self,
        request_id: str,
        response_id: str,
        tool: str,
        arguments: Dict[str, Any],
        summary: str,
        full_result: Any,
        sources: Any = None,
        status: str = "ok",
        tenant_id: str = "__system__",
        principal_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        values = (
            request_id,
            response_id,
            tenant_id,
            principal_id,
            trace_id,
            tool,
            json.dumps(arguments, default=str),
            summary,
            json.dumps(full_result, default=str),
            json.dumps(sources or [], default=str),
            status,
        )
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO search_gateway_requests (
                    request_id, response_id, tenant_id, principal_id, trace_id,
                    tool, arguments_json, summary,
                    full_result_json, sources_json, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (request_id) DO UPDATE SET
                    response_id = EXCLUDED.response_id,
                    tenant_id = EXCLUDED.tenant_id,
                    principal_id = EXCLUDED.principal_id,
                    trace_id = EXCLUDED.trace_id,
                    tool = EXCLUDED.tool,
                    arguments_json = EXCLUDED.arguments_json,
                    summary = EXCLUDED.summary,
                    full_result_json = EXCLUDED.full_result_json,
                    sources_json = EXCLUDED.sources_json,
                    status = EXCLUDED.status
                """,
                values,
            )

    @staticmethod
    def _decode(raw: Any) -> Any:
        if raw is None or not isinstance(raw, str):
            return raw
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def get(
        self,
        request_id: Optional[str] = None,
        response_id: Optional[str] = None,
        tenant_id: str = "__system__",
    ) -> Optional[Dict[str, Any]]:
        if not request_id and not response_id:
            return None
        column = "request_id" if request_id else "response_id"
        value = request_id or response_id
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT request_id, response_id, tenant_id, principal_id, trace_id,
                       tool, arguments_json, summary, full_result_json,
                       sources_json, status, created_at
                FROM search_gateway_requests WHERE {column} = %s AND tenant_id = %s
                """,
                (value, tenant_id),
            )
            row = cur.fetchone()
        if not row:
            return None
        created_at = row[11].isoformat() if isinstance(row[11], datetime) else str(row[11])
        return {
            "request_id": row[0],
            "response_id": row[1],
            "tenant_id": row[2],
            "principal_id": row[3],
            "trace_id": row[4],
            "tool": row[5],
            "arguments": self._decode(row[6]),
            "summary": row[7],
            "full_result": self._decode(row[8]),
            "sources": self._decode(row[9]),
            "status": row[10],
            "created_at": created_at,
        }

    def list(
        self,
        limit: int = 20,
        tool: Optional[str] = None,
        tenant_id: str = "__system__",
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit or 20), 200))
        where = " WHERE tenant_id = %s"
        params: tuple = (tenant_id,)
        if tool:
            where += " AND tool = %s"
            params += (tool,)
        params += (limit,)
        with self._lock, self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_id, response_id, tool, summary, status, created_at, trace_id
                FROM search_gateway_requests
                """ + where + " ORDER BY created_at DESC LIMIT %s",
                params,
            )
            rows = cur.fetchall()
        return [
            {
                "request_id": row[0],
                "response_id": row[1],
                "tool": row[2],
                "summary": row[3],
                "status": row[4],
                "created_at": row[5].isoformat() if isinstance(row[5], datetime) else str(row[5]),
                "trace_id": row[6],
            }
            for row in rows
        ]

    def get_status(self) -> Dict[str, Any]:
        return {"backend": self.backend, "durable": self.durable, "ready": self.ready}
