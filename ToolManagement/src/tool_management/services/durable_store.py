"""
Durable Store - Persistent storage for all tool requests and responses.

Provides:
- Request/response persistence with full payloads
- Idempotency keys for deduplication
- Request replay capability
- Usage tracking for billing
- Audit trail
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import aiosqlite
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class RequestStatus(str, Enum):
    """Status of a tool request."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class ToolRequest(BaseModel):
    """Durable tool request."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    idempotency_key: Optional[str] = None

    # Tool info (abstracted - no vendor details exposed)
    tool_name: str
    tool_id: str
    service_id: str  # Our service ID, not vendor

    # Request data
    inputs: Dict[str, Any] = Field(default_factory=dict)
    input_hash: Optional[str] = None

    # Client info
    client_id: Optional[str] = None
    api_key_id: Optional[str] = None
    organization_id: Optional[str] = None

    # Tracking
    status: RequestStatus = RequestStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Billing
    tokens_input: int = 0
    tokens_output: int = 0
    cost_cents: int = 0

    # Metadata (internal only - not exposed to clients)
    _vendor: Optional[str] = None
    _vendor_request_id: Optional[str] = None

    class Config:
        extra = "allow"

    def compute_input_hash(self) -> str:
        """Compute deterministic hash of inputs for deduplication."""
        data = json.dumps(self.inputs, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()[:16]


class ToolResponse(BaseModel):
    """Durable tool response."""
    id: str = Field(default_factory=lambda: str(uuid4()))
    request_id: str

    # Response data
    status: RequestStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    error_code: Optional[str] = None

    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow)
    execution_time_ms: float = 0

    # Usage
    tokens_input: int = 0
    tokens_output: int = 0

    class Config:
        extra = "allow"


class DurableStore:
    """
    Durable storage for tool requests and responses.

    Features:
    - SQLite/Turso backend (easily swappable)
    - Automatic cleanup of old records
    - Idempotency support
    - Request replay
    - Usage aggregation for billing

    Usage:
        store = DurableStore("./data/requests.db")
        await store.initialize()

        # Store request
        request = ToolRequest(tool_name="search", inputs={"query": "AI"})
        await store.save_request(request)

        # Store response
        response = ToolResponse(request_id=request.id, status=RequestStatus.COMPLETED, result={...})
        await store.save_response(response)

        # Get request history
        requests = await store.get_requests(client_id="client-123", limit=100)
    """

    def __init__(
        self,
        db_path: str = "./data/tool_requests.db",
        retention_days: int = 90,
    ):
        self.db_path = db_path
        self.retention_days = retention_days
        self._db: Optional[aiosqlite.Connection] = None
        self._initialized = False

    async def initialize(self):
        """Initialize database and create tables."""
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)

        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        await self._create_tables()
        self._initialized = True
        logger.info(f"DurableStore initialized: {self.db_path}")

    async def _create_tables(self):
        """Create database tables."""
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS tool_requests (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT,
                tool_name TEXT NOT NULL,
                tool_id TEXT NOT NULL,
                service_id TEXT NOT NULL,
                inputs TEXT NOT NULL,
                input_hash TEXT,
                client_id TEXT,
                api_key_id TEXT,
                organization_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                tokens_input INTEGER DEFAULT 0,
                tokens_output INTEGER DEFAULT 0,
                cost_cents INTEGER DEFAULT 0,
                vendor TEXT,
                vendor_request_id TEXT
            );

            CREATE TABLE IF NOT EXISTS tool_responses (
                id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                error TEXT,
                error_code TEXT,
                created_at TEXT NOT NULL,
                execution_time_ms REAL DEFAULT 0,
                tokens_input INTEGER DEFAULT 0,
                tokens_output INTEGER DEFAULT 0,
                FOREIGN KEY (request_id) REFERENCES tool_requests(id)
            );

            CREATE TABLE IF NOT EXISTS economic_events (
                id TEXT PRIMARY KEY,
                request_id TEXT,
                client_id TEXT,
                organization_id TEXT,
                event_type TEXT NOT NULL,
                amount_cents INTEGER DEFAULT 0,
                payload TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_requests_client ON tool_requests(client_id);
            CREATE INDEX IF NOT EXISTS idx_requests_org ON tool_requests(organization_id);
            CREATE INDEX IF NOT EXISTS idx_requests_tool ON tool_requests(tool_name);
            CREATE INDEX IF NOT EXISTS idx_requests_status ON tool_requests(status);
            CREATE INDEX IF NOT EXISTS idx_requests_created ON tool_requests(created_at);
            CREATE INDEX IF NOT EXISTS idx_requests_idempotency ON tool_requests(idempotency_key);
            CREATE INDEX IF NOT EXISTS idx_responses_request ON tool_responses(request_id);
            CREATE INDEX IF NOT EXISTS idx_economic_events_request ON economic_events(request_id);
            CREATE INDEX IF NOT EXISTS idx_economic_events_client ON economic_events(client_id);
            CREATE INDEX IF NOT EXISTS idx_economic_events_created ON economic_events(created_at);
        """)
        await self._db.commit()

    async def close(self):
        """Close database connection."""
        if self._db:
            await self._db.close()
            self._db = None

    # =========================================================================
    # REQUEST OPERATIONS
    # =========================================================================

    async def save_request(self, request: ToolRequest) -> ToolRequest:
        """Save a tool request."""
        if not request.input_hash:
            request.input_hash = request.compute_input_hash()

        await self._db.execute("""
            INSERT INTO tool_requests (
                id, idempotency_key, tool_name, tool_id, service_id,
                inputs, input_hash, client_id, api_key_id, organization_id,
                status, created_at, started_at, completed_at,
                tokens_input, tokens_output, cost_cents, vendor, vendor_request_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.id,
            request.idempotency_key,
            request.tool_name,
            request.tool_id,
            request.service_id,
            json.dumps(request.inputs),
            request.input_hash,
            request.client_id,
            request.api_key_id,
            request.organization_id,
            request.status.value,
            request.created_at.isoformat(),
            request.started_at.isoformat() if request.started_at else None,
            request.completed_at.isoformat() if request.completed_at else None,
            request.tokens_input,
            request.tokens_output,
            request.cost_cents,
            request._vendor,
            request._vendor_request_id,
        ))
        await self._db.commit()
        return request

    async def update_request(self, request: ToolRequest) -> ToolRequest:
        """Update an existing request."""
        await self._db.execute("""
            UPDATE tool_requests SET
                status = ?,
                started_at = ?,
                completed_at = ?,
                tokens_input = ?,
                tokens_output = ?,
                cost_cents = ?,
                vendor = ?,
                vendor_request_id = ?
            WHERE id = ?
        """, (
            request.status.value,
            request.started_at.isoformat() if request.started_at else None,
            request.completed_at.isoformat() if request.completed_at else None,
            request.tokens_input,
            request.tokens_output,
            request.cost_cents,
            request._vendor,
            request._vendor_request_id,
            request.id,
        ))
        await self._db.commit()
        return request

    async def get_request(self, request_id: str) -> Optional[ToolRequest]:
        """Get a request by ID."""
        async with self._db.execute(
            "SELECT * FROM tool_requests WHERE id = ?",
            (request_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_request(row)
        return None

    async def get_by_idempotency_key(
        self,
        idempotency_key: str,
        client_id: Optional[str] = None
    ) -> Optional[ToolRequest]:
        """Get request by idempotency key (for deduplication)."""
        query = "SELECT * FROM tool_requests WHERE idempotency_key = ?"
        params = [idempotency_key]

        if client_id:
            query += " AND client_id = ?"
            params.append(client_id)

        async with self._db.execute(query, params) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_request(row)
        return None

    async def get_requests(
        self,
        client_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        status: Optional[RequestStatus] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[ToolRequest]:
        """Get requests with filters."""
        query = "SELECT * FROM tool_requests WHERE 1=1"
        params = []

        if client_id:
            query += " AND client_id = ?"
            params.append(client_id)
        if organization_id:
            query += " AND organization_id = ?"
            params.append(organization_id)
        if tool_name:
            query += " AND tool_name = ?"
            params.append(tool_name)
        if status:
            query += " AND status = ?"
            params.append(status.value)
        if since:
            query += " AND created_at >= ?"
            params.append(since.isoformat())

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        requests = []
        async with self._db.execute(query, params) as cursor:
            async for row in cursor:
                requests.append(self._row_to_request(row))
        return requests

    # =========================================================================
    # RESPONSE OPERATIONS
    # =========================================================================

    async def save_response(self, response: ToolResponse) -> ToolResponse:
        """Save a tool response."""
        await self._db.execute("""
            INSERT INTO tool_responses (
                id, request_id, status, result, error, error_code,
                created_at, execution_time_ms, tokens_input, tokens_output
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            response.id,
            response.request_id,
            response.status.value,
            json.dumps(response.result) if response.result else None,
            response.error,
            response.error_code,
            response.created_at.isoformat(),
            response.execution_time_ms,
            response.tokens_input,
            response.tokens_output,
        ))
        await self._db.commit()
        return response

    async def get_response(self, request_id: str) -> Optional[ToolResponse]:
        """Get response for a request."""
        async with self._db.execute(
            "SELECT * FROM tool_responses WHERE request_id = ? ORDER BY created_at DESC LIMIT 1",
            (request_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_response(row)
        return None

    # =========================================================================
    # USAGE / BILLING
    # =========================================================================

    async def get_usage_summary(
        self,
        client_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Get usage summary for billing."""
        query = """
            SELECT
                tool_name,
                COUNT(*) as request_count,
                SUM(tokens_input) as total_tokens_input,
                SUM(tokens_output) as total_tokens_output,
                SUM(cost_cents) as total_cost_cents,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count
            FROM tool_requests
            WHERE 1=1
        """
        params = []

        if client_id:
            query += " AND client_id = ?"
            params.append(client_id)
        if organization_id:
            query += " AND organization_id = ?"
            params.append(organization_id)
        if since:
            query += " AND created_at >= ?"
            params.append(since.isoformat())
        if until:
            query += " AND created_at <= ?"
            params.append(until.isoformat())

        query += " GROUP BY tool_name"

        by_tool = {}
        totals = {
            "request_count": 0,
            "tokens_input": 0,
            "tokens_output": 0,
            "cost_cents": 0,
            "completed": 0,
            "failed": 0,
        }

        async with self._db.execute(query, params) as cursor:
            async for row in cursor:
                tool = row["tool_name"]
                by_tool[tool] = {
                    "request_count": row["request_count"],
                    "tokens_input": row["total_tokens_input"] or 0,
                    "tokens_output": row["total_tokens_output"] or 0,
                    "cost_cents": row["total_cost_cents"] or 0,
                    "completed": row["completed_count"],
                    "failed": row["failed_count"],
                }
                totals["request_count"] += row["request_count"]
                totals["tokens_input"] += row["total_tokens_input"] or 0
                totals["tokens_output"] += row["total_tokens_output"] or 0
                totals["cost_cents"] += row["total_cost_cents"] or 0
                totals["completed"] += row["completed_count"]
                totals["failed"] += row["failed_count"]

        return {
            "period": {
                "since": since.isoformat() if since else None,
                "until": until.isoformat() if until else None,
            },
            "by_tool": by_tool,
            "totals": totals,
        }

    async def append_economic_event(
        self,
        *,
        event_type: str,
        request_id: Optional[str] = None,
        client_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        amount_cents: int = 0,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Persist an economic event for durable accounting/audit."""
        event_id = str(uuid4())
        created_at = datetime.utcnow().isoformat()
        await self._db.execute(
            """
            INSERT INTO economic_events (
                id, request_id, client_id, organization_id, event_type, amount_cents, payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                request_id,
                client_id,
                organization_id,
                event_type,
                int(amount_cents),
                json.dumps(payload or {}),
                created_at,
            ),
        )
        await self._db.commit()
        return {
            "id": event_id,
            "request_id": request_id,
            "client_id": client_id,
            "organization_id": organization_id,
            "event_type": event_type,
            "amount_cents": int(amount_cents),
            "payload": payload or {},
            "created_at": created_at,
        }

    async def get_economic_events(
        self,
        *,
        client_id: Optional[str] = None,
        organization_id: Optional[str] = None,
        request_id: Optional[str] = None,
        event_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Query persisted economic events."""
        query = "SELECT * FROM economic_events WHERE 1=1"
        params: List[Any] = []

        if client_id:
            query += " AND client_id = ?"
            params.append(client_id)
        if organization_id:
            query += " AND organization_id = ?"
            params.append(organization_id)
        if request_id:
            query += " AND request_id = ?"
            params.append(request_id)
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        if since:
            query += " AND created_at >= ?"
            params.append(since.isoformat())

        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(max(1, int(limit)))

        events: List[Dict[str, Any]] = []
        async with self._db.execute(query, params) as cursor:
            async for row in cursor:
                payload = row["payload"]
                events.append(
                    {
                        "id": row["id"],
                        "request_id": row["request_id"],
                        "client_id": row["client_id"],
                        "organization_id": row["organization_id"],
                        "event_type": row["event_type"],
                        "amount_cents": int(row["amount_cents"] or 0),
                        "payload": json.loads(payload) if payload else {},
                        "created_at": row["created_at"],
                    }
                )
        return events

    # =========================================================================
    # MAINTENANCE
    # =========================================================================

    async def cleanup_old_records(self, days: Optional[int] = None) -> int:
        """Remove records older than retention period."""
        days = days or self.retention_days
        cutoff = datetime.utcnow() - timedelta(days=days)

        cursor = await self._db.execute(
            "DELETE FROM tool_responses WHERE request_id IN (SELECT id FROM tool_requests WHERE created_at < ?)",
            (cutoff.isoformat(),)
        )
        responses_deleted = cursor.rowcount

        cursor = await self._db.execute(
            "DELETE FROM tool_requests WHERE created_at < ?",
            (cutoff.isoformat(),)
        )
        requests_deleted = cursor.rowcount

        await self._db.commit()
        logger.info(f"Cleaned up {requests_deleted} requests, {responses_deleted} responses older than {days} days")
        return requests_deleted

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _row_to_request(self, row) -> ToolRequest:
        """Convert database row to ToolRequest."""
        return ToolRequest(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            tool_name=row["tool_name"],
            tool_id=row["tool_id"],
            service_id=row["service_id"],
            inputs=json.loads(row["inputs"]),
            input_hash=row["input_hash"],
            client_id=row["client_id"],
            api_key_id=row["api_key_id"],
            organization_id=row["organization_id"],
            status=RequestStatus(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            tokens_input=row["tokens_input"],
            tokens_output=row["tokens_output"],
            cost_cents=row["cost_cents"],
            _vendor=row["vendor"],
            _vendor_request_id=row["vendor_request_id"],
        )

    def _row_to_response(self, row) -> ToolResponse:
        """Convert database row to ToolResponse."""
        return ToolResponse(
            id=row["id"],
            request_id=row["request_id"],
            status=RequestStatus(row["status"]),
            result=json.loads(row["result"]) if row["result"] else None,
            error=row["error"],
            error_code=row["error_code"],
            created_at=datetime.fromisoformat(row["created_at"]),
            execution_time_ms=row["execution_time_ms"],
            tokens_input=row["tokens_input"],
            tokens_output=row["tokens_output"],
        )
