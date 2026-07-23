#!/usr/bin/env python3
"""
Distributed Research MCP - Native MCP Server

First-class MCP server providing:
- Deep research capabilities
- Parallel task execution
- Structured data extraction
- Batch processing
- Web monitoring

This is a NATIVE MCP server - it implements the MCP protocol directly
rather than wrapping an external service.

Run with:
    python -m native.distributed_research_mcp
    # or
    uvicorn native.distributed_research_mcp:app --port 4002

Environment:
    PARALLEL_API_KEY: Parallel AI API key (optional - degrades gracefully)
    REGISTRY_URL: Central registry URL (optional)
    PORT: Server port (default: 4002)
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output guard for enforcing size limits and URL capture
try:
    from tool_management.guards.output_guard import guard_mcp_content, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False
    logger.info("Output guard not available - running without output limits")


# =============================================================================
# MODELS
# =============================================================================

class MCPProtocolVersion(str, Enum):
    V1 = "2024-11-05"


class ConnectionStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProcessorType(str, Enum):
    LITE = "lite"
    TURBO = "turbo"
    PREMIUM = "premium"


# Parallel.ai Task API processors (https://docs.parallel.ai/task-api/guides/choose-a-processor)
PARALLEL_PROCESSORS = [
    "lite", "base", "core", "core2x", "pro", "ultra", "ultra2x", "ultra4x", "ultra8x",
    "lite-fast", "base-fast", "core-fast", "core2x-fast", "pro-fast",
    "ultra-fast", "ultra2x-fast", "ultra4x-fast", "ultra8x-fast",
]

# Legacy processor names still accepted by run_task, mapped to real Parallel processors
LEGACY_PROCESSOR_MAP = {
    "turbo": "core",
    "premium": "pro",
}

# Parallel.ai Search API modes (POST /v1/search)
PARALLEL_SEARCH_MODES = ["turbo", "basic", "advanced"]
# Legacy v1beta search "processor" values mapped onto current modes
LEGACY_SEARCH_PROCESSOR_MAP = {"base": "basic", "pro": "advanced"}

# Task run statuses considered terminal
TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled"}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunStore:
    """Durable sqlite store for Parallel task runs (webhook-updated)."""

    def __init__(self, db_path: Optional[str] = None):
        path = Path(db_path or os.getenv("RESEARCH_RUNS_DB", "data/research_runs.db"))
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    objective TEXT,
                    processor TEXT,
                    status TEXT,
                    result_json TEXT,
                    webhook_received_at TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    error TEXT
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)"
            )
            self._conn.commit()

    def insert(
        self,
        run_id: str,
        objective: str = "",
        processor: str = "",
        status: str = "queued",
    ) -> None:
        now = _utcnow_iso()
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO runs (run_id, objective, processor, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, objective, processor, status, now, now),
            )
            self._conn.commit()

    def update(self, run_id: str, **fields: Any) -> None:
        allowed = {"objective", "processor", "status", "result_json", "webhook_received_at", "error"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return
        updates["updated_at"] = _utcnow_iso()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        with self._lock:
            self._conn.execute(
                f"UPDATE runs SET {set_clause} WHERE run_id = ?",
                (*updates.values(), run_id),
            )
            self._conn.commit()

    def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return dict(row) if row else None

    def list(self, limit: int = 20, status: Optional[str] = None) -> List[Dict[str, Any]]:
        query = "SELECT * FROM runs"
        params: List[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def active_runs(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE status NOT IN ('completed', 'failed', 'cancelled')"
            ).fetchall()
        return [dict(r) for r in rows]

    def count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS c FROM runs").fetchone()
        return int(row["c"]) if row else 0


@dataclass
class MCPTool:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Optional[Callable] = None


@dataclass
class ConnectionState:
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    connected_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    error_message: Optional[str] = None
    session_id: Optional[str] = None
    protocol_version: str = MCPProtocolVersion.V1.value


@dataclass
class Task:
    id: str
    processor: ProcessorType
    input: str
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None


class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)


class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


# =============================================================================
# NATIVE MCP SERVER
# =============================================================================

class DistributedResearchMCP:
    """
    Native MCP server for distributed research capabilities.

    This is our second native MCP - it implements parallel task execution
    and deep research capabilities. The connectivity test verifies:

    1. MCP protocol handshake works
    2. Tool discovery functions
    3. Task management works
    4. Parallel execution functions
    """

    SERVICE_ID = "distributed-research-mcp"
    SERVICE_NAME = "Distributed Research MCP"
    VERSION = "1.2.0"

    # Parallel AI endpoints (optional - degrades gracefully)
    PARALLEL_API_URL = "https://api.parallel.ai"

    # Model mapping by processor tier
    MODEL_MAP = {
        ProcessorType.LITE: "claude-haiku-4-5-20251001",
        ProcessorType.TURBO: "claude-sonnet-4-5-20250929",
        ProcessorType.PREMIUM: "claude-opus-4-6",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        port: int = 4002,
        registry_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("PARALLEL_API_KEY")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        self.jina_api_key = os.getenv("JINA_API_KEY")
        self.webhook_secret = os.getenv("PARALLEL_WEBHOOK_SECRET")
        self.port = port
        self.registry_url = registry_url or os.getenv("REGISTRY_URL")

        self._client: Optional[httpx.AsyncClient] = None
        self._tools: Dict[str, MCPTool] = {}
        self._state = ConnectionState()

        # In-memory task storage (native task management)
        self._tasks: Dict[str, Task] = {}
        self._task_groups: Dict[str, List[str]] = {}

        # Durable Parallel run store + async wakeup primitives
        self.run_store = RunStore()
        self._run_events: Dict[str, asyncio.Event] = {}
        self._watchers: Dict[str, asyncio.Task] = {}

        # Register native tools
        self._register_tools()

    # =========================================================================
    # PARALLEL.AI API CLIENT (Task API v1 + Search API v1)
    # =========================================================================

    def _webhook_url(self) -> Optional[str]:
        """Public webhook URL derived from SERVICE_URL (only usable if publicly reachable)."""
        service_url = os.getenv("SERVICE_URL", "").strip().rstrip("/")
        if service_url.startswith("http://") or service_url.startswith("https://"):
            return f"{service_url}/webhooks/parallel"
        return None

    async def _parallel_request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 60.0,
        beta: Optional[str] = None,
    ) -> tuple[bool, Any, Optional[str]]:
        """
        Call the Parallel.ai API. Returns (ok, data, error_text).
        On HTTP errors, error_text contains the API's response body verbatim.
        `beta` sets the `parallel-beta` feature header required by beta endpoints.
        """
        if not self.api_key:
            return False, None, (
                "PARALLEL_API_KEY not configured - set it in the environment to enable "
                "Parallel.ai Task/Search API calls"
            )
        headers = {"x-api-key": self.api_key}
        if beta:
            headers["parallel-beta"] = beta
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.request(
                    method,
                    f"{self.PARALLEL_API_URL}{path}",
                    json=json_body,
                    params=params,
                    headers=headers,
                )
        except Exception as e:
            return False, None, f"Parallel API request failed: {e}"

        if resp.status_code >= 400:
            return False, resp, f"Parallel API error {resp.status_code}: {resp.text}"
        try:
            return True, resp.json(), None
        except Exception:
            return True, {"raw": resp.text}, None

    async def _parallel_create_run(
        self,
        input_text: Any,
        processor: str,
        output_schema: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source_policy: Optional[Dict[str, Any]] = None,
        mcp_servers: Optional[list] = None,
        location: Optional[str] = None,
        enable_events: Optional[bool] = None,
        previous_interaction_id: Optional[str] = None,
    ) -> tuple[bool, Any, Optional[str], bool]:
        """POST /v1/tasks/runs. Returns (ok, data, error, webhook_attached)."""
        body: Dict[str, Any] = {"input": input_text, "processor": processor}
        if output_schema:
            body["task_spec"] = {"output_schema": output_schema}
        if metadata:
            body["metadata"] = {str(k)[:16]: str(v)[:512] for k, v in metadata.items()}
        if source_policy:
            body["source_policy"] = source_policy
        if location:
            body["advanced_settings"] = {"location": location}
        if enable_events is not None:
            body["enable_events"] = bool(enable_events)
        if previous_interaction_id:
            body["previous_interaction_id"] = previous_interaction_id
        beta_flags = []
        if mcp_servers:
            body["mcp_servers"] = mcp_servers
            beta_flags.append("mcp-server-2025-07-17")
        webhook_url = self._webhook_url()
        if webhook_url:
            body["webhook"] = {"url": webhook_url, "event_types": ["task_run.status"]}
            beta_flags.append("webhook-2025-08-12")
        ok, data, err = await self._parallel_request(
            "POST", "/v1/tasks/runs", json_body=body,
            beta=",".join(beta_flags) or None,
        )
        return ok, data, err, bool(webhook_url)

    async def _parallel_get_run(self, run_id: str) -> tuple[bool, Any, Optional[str]]:
        """GET /v1/tasks/runs/{run_id}."""
        return await self._parallel_request("GET", f"/v1/tasks/runs/{run_id}")

    async def _parallel_get_result(
        self, run_id: str, api_timeout: int = 30
    ) -> tuple[bool, Any, Optional[str]]:
        """GET /v1/tasks/runs/{run_id}/result (blocks server-side up to `timeout`)."""
        return await self._parallel_request(
            "GET",
            f"/v1/tasks/runs/{run_id}/result",
            params={"timeout": api_timeout},
            timeout=float(api_timeout) + 30.0,
        )

    # =========================================================================
    # RUN LIFECYCLE: store updates, webhook processing, fallback watcher
    # =========================================================================

    def _notify_run(self, run_id: str) -> None:
        """Wake any await_run waiters for this run."""
        event = self._run_events.get(run_id)
        if event:
            event.set()

    def _run_event(self, run_id: str) -> asyncio.Event:
        event = self._run_events.get(run_id)
        if event is None:
            event = asyncio.Event()
            self._run_events[run_id] = event
        return event

    def _ensure_watcher(self, run_id: str) -> None:
        """Spawn the fallback poller for a run (no-op if already running)."""
        existing = self._watchers.get(run_id)
        if existing and not existing.done():
            return
        try:
            self._watchers[run_id] = asyncio.create_task(self._watch_run(run_id))
        except RuntimeError:
            # No running loop (e.g. sync test context); watcher is best-effort fallback
            pass

    async def _watch_run(self, run_id: str) -> None:
        """
        Fallback watcher for runs whose webhook never arrives.
        Exponential backoff starting at 5s, capped at 60s. Exits as soon as the
        store shows a terminal status (webhook delivery also terminates it).
        """
        delay = 5.0
        while True:
            await asyncio.sleep(delay)
            row = self.run_store.get(run_id)
            if not row or row["status"] in TERMINAL_RUN_STATUSES:
                return

            ok, data, err = await self._parallel_get_run(run_id)
            if ok and isinstance(data, dict):
                status = data.get("status") or row["status"]
                updates: Dict[str, Any] = {"status": status}
                if data.get("error"):
                    updates["error"] = json.dumps(data["error"], default=str)
                self.run_store.update(run_id, **updates)
                if status == "completed":
                    await self._fetch_and_store_result(run_id)
                if status in TERMINAL_RUN_STATUSES:
                    self._notify_run(run_id)
                    return
            elif err:
                logger.debug(f"Watcher poll for {run_id} failed: {err}")
                # 404 => run unknown upstream; mark failed so we stop polling forever
                if isinstance(data, httpx.Response) and data.status_code == 404:
                    self.run_store.update(run_id, status="failed", error=err)
                    self._notify_run(run_id)
                    return

            delay = min(delay * 2, 60.0)

    async def _fetch_and_store_result(self, run_id: str) -> None:
        """Fetch the full result for a completed run and persist it."""
        row = self.run_store.get(run_id)
        if row and row.get("result_json"):
            return
        ok, data, err = await self._parallel_get_result(run_id, api_timeout=60)
        if ok and isinstance(data, dict):
            self.run_store.update(run_id, result_json=json.dumps(data, default=str))
        elif err:
            logger.warning(f"Result fetch for {run_id} failed: {err}")
            if not (row and row.get("result_json")):
                self.run_store.update(run_id, error=err)

    def verify_parallel_webhook(self, headers: Dict[str, str], body: bytes) -> bool:
        """
        Verify Parallel webhook HMAC signature (Standard Webhooks style):
        signed content = "{webhook-id}.{webhook-timestamp}.{body}",
        HMAC-SHA256, base64. Only enforced when PARALLEL_WEBHOOK_SECRET is set.
        """
        if not self.webhook_secret:
            return True

        lowered = {k.lower(): v for k, v in headers.items()}
        msg_id = lowered.get("webhook-id")
        timestamp = lowered.get("webhook-timestamp")
        signature_header = lowered.get("webhook-signature")
        if not (msg_id and timestamp and signature_header):
            return False

        signed_content = f"{msg_id}.{timestamp}.".encode() + body

        secrets: List[bytes] = [self.webhook_secret.encode()]
        if self.webhook_secret.startswith("whsec_"):
            try:
                secrets.append(base64.b64decode(self.webhook_secret[len("whsec_"):]))
            except Exception:
                pass

        expected = {
            base64.b64encode(hmac.new(secret, signed_content, hashlib.sha256).digest()).decode()
            for secret in secrets
        }
        for part in signature_header.split():
            candidate = part.split(",", 1)[-1]
            for exp in expected:
                if hmac.compare_digest(candidate, exp):
                    return True
        return False

    async def process_parallel_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a Parallel webhook event ({timestamp, type, data: TaskRun}).
        Updates the durable store, fetches the full result on completion, and
        wakes any await_run waiters.
        """
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        run_id = data.get("run_id")
        if not run_id:
            return {"error": "missing run_id in webhook payload", "success": False}

        status = data.get("status") or "completed"
        now = _utcnow_iso()

        if not self.run_store.get(run_id):
            self.run_store.insert(
                run_id,
                objective="(created externally - received via webhook)",
                processor=str(data.get("processor") or ""),
                status=status,
            )

        updates: Dict[str, Any] = {"status": status, "webhook_received_at": now}
        if data.get("processor"):
            updates["processor"] = str(data["processor"])
        if data.get("error"):
            updates["error"] = json.dumps(data["error"], default=str)
        # Some payloads (and synthetic tests) can carry the output inline
        if "output" in data:
            run_obj = {k: v for k, v in data.items() if k != "output"}
            updates["result_json"] = json.dumps({"run": run_obj, "output": data["output"]}, default=str)
        self.run_store.update(run_id, **updates)

        if status == "completed" and "output" not in data and self.api_key:
            await self._fetch_and_store_result(run_id)

        if status in TERMINAL_RUN_STATUSES:
            self._notify_run(run_id)

        return {"ok": True, "run_id": run_id, "status": status, "success": True}

    def resume_active_watchers(self) -> int:
        """Re-attach fallback watchers for runs still active in the durable store."""
        active = self.run_store.active_runs()
        for row in active:
            self._ensure_watcher(row["run_id"])
        return len(active)

    # =========================================================================
    # LLM + JINA HELPERS
    # =========================================================================

    async def _call_llm(
        self,
        prompt: str,
        system: str = "You are a helpful research assistant.",
        max_tokens: int = 4096,
        processor: ProcessorType = ProcessorType.LITE,
    ) -> Optional[str]:
        """LLM completion: Anthropic first, Parallel Chat API as fallback.

        The Parallel fallback keeps synthesize/deep_search producing real
        output when the Anthropic key is absent or out of credit.
        """
        if self.anthropic_api_key:
            model = self.MODEL_MAP.get(processor, self.MODEL_MAP[ProcessorType.LITE])
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    resp = await client.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={
                            "x-api-key": self.anthropic_api_key,
                            "anthropic-version": "2023-06-01",
                            "content-type": "application/json",
                        },
                        json={
                            "model": model,
                            "max_tokens": max_tokens,
                            "system": system,
                            "messages": [{"role": "user", "content": prompt}],
                        },
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        content = data.get("content", [])
                        return content[0]["text"] if content else None
                    logger.warning(f"LLM call failed ({resp.status_code}): {resp.text[:200]}")
            except Exception as e:
                logger.error(f"LLM call error: {e}")

        # Fallback: Parallel Chat API (model "speed" ~ fast completions)
        if self.api_key:
            ok, data, err = await self._parallel_request(
                "POST", "/v1beta/chat/completions",
                json_body={
                    "model": "speed",
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": prompt},
                    ],
                    "stream": False,
                },
                timeout=120.0,
            )
            if ok:
                try:
                    choice = (data.get("choices") or [{}])[0]
                    return (choice.get("message") or {}).get("content")
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"Parallel chat fallback parse error: {e}")
            else:
                logger.warning(f"Parallel chat fallback failed: {str(err)[:200]}")
        return None

    async def _jina_read(self, url: str) -> Optional[str]:
        """Fetch URL content via Jina Reader API."""
        headers = {}
        if self.jina_api_key:
            headers["Authorization"] = f"Bearer {self.jina_api_key}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(f"https://r.jina.ai/{url}", headers=headers)
                if resp.status_code == 200:
                    return resp.text
                return None
        except Exception as e:
            logger.error(f"Jina read error for {url}: {e}")
            return None

    async def _jina_search(self, query: str, num_results: int = 5) -> List[Dict[str, Any]]:
        """Search via Jina Search API."""
        headers = {"Accept": "application/json"}
        if self.jina_api_key:
            headers["Authorization"] = f"Bearer {self.jina_api_key}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    f"https://s.jina.ai/{query}",
                    headers=headers,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("data", [])[:num_results]
                return []
        except Exception as e:
            logger.error(f"Jina search error: {e}")
            return []

    def _register_tools(self):
        """Register all native MCP tools."""

        # Deep research/search
        self._tools["deep_search"] = MCPTool(
            name="deep_search",
            description="Perform deep research on a topic, gathering and synthesizing information from multiple sources.",
            input_schema={
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "Research objective or question"
                    },
                    "depth": {
                        "type": "string",
                        "enum": ["quick", "standard", "thorough"],
                        "description": "Research depth level",
                        "default": "standard"
                    },
                    "max_sources": {
                        "type": "integer",
                        "description": "Maximum sources to consult",
                        "default": 10
                    }
                },
                "required": ["objective"]
            },
            handler=self._execute_deep_search
        )

        # Run task (legacy-compatible synchronous-ish wrapper, backed by the run store)
        self._tools["run_task"] = MCPTool(
            name="run_task",
            description=(
                "Execute a task on Parallel.ai's Task API and wait briefly for the result. "
                "Creates a durable run (see research_task) then awaits up to 120s; if still "
                "running, returns the run_id for later get_run/await_run. Legacy processors "
                "turbo/premium map to core/pro."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "description": "Task input/prompt"
                    },
                    "processor": {
                        "type": "string",
                        "enum": PARALLEL_PROCESSORS + list(LEGACY_PROCESSOR_MAP.keys()),
                        "description": "Parallel.ai processor (lite|base|core|core2x|pro|ultra... plus -fast variants; legacy turbo/premium accepted)",
                        "default": "lite"
                    },
                    "output_schema": {
                        "type": "object",
                        "description": "Optional JSON schema for structured output"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Max seconds to wait for completion before returning the run_id (capped at 120)",
                        "default": 120
                    }
                },
                "required": ["input"]
            },
            handler=self._execute_run_task
        )

        # Research task: webhook-native async run creation (returns immediately)
        self._tools["research_task"] = MCPTool(
            name="research_task",
            description=(
                "Create a Parallel.ai Task API run (POST /v1/tasks/runs) and return IMMEDIATELY "
                "with the run_id. Results arrive asynchronously via webhook (with a polling "
                "fallback) into the durable run store - use await_run or get_run to collect them. "
                "No long polling required."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "description": "Task input / research objective (alias: objective)"
                    },
                    "objective": {
                        "type": "string",
                        "description": "Alias for input"
                    },
                    "processor": {
                        "type": "string",
                        "enum": PARALLEL_PROCESSORS,
                        "description": "Parallel.ai processor tier",
                        "default": "lite"
                    },
                    "output_schema": {
                        "description": "Optional output schema (JSON schema object or text description) passed as task_spec.output_schema"
                    },
                    "metadata": {
                        "type": "object",
                        "description": "Optional user metadata (string key/values) attached to the run"
                    },
                    "source_policy": {
                        "type": "object",
                        "description": "{include_domains, exclude_domains, after_date} source restrictions"
                    },
                    "mcp_servers": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Custom MCP servers the task can call ({type:'url', url, name, headers, allowed_tools})"
                    },
                    "location": {
                        "type": "string",
                        "description": "ISO-3166 alpha-2 country code for geo-targeted research"
                    },
                    "enable_events": {"type": "boolean"},
                    "previous_interaction_id": {
                        "type": "string",
                        "description": "Continue from a prior interaction (Interactions)"
                    }
                }
            },
            handler=self._execute_research_task
        )

        # Get run (local durable store, webhook-updated)
        self._tools["get_run"] = MCPTool(
            name="get_run",
            description="Get a Parallel task run from the LOCAL durable store (updated by webhooks/watcher). Includes the full result when the run is done.",
            input_schema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "Parallel task run ID"
                    }
                },
                "required": ["run_id"]
            },
            handler=self._execute_get_run
        )

        # Await run (async-native: wakes on webhook delivery)
        self._tools["await_run"] = MCPTool(
            name="await_run",
            description="Wait for a Parallel task run to finish. Returns as soon as the webhook (or fallback watcher) marks the run terminal, or when the timeout elapses.",
            input_schema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "Parallel task run ID"
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Max seconds to wait (capped at 120)",
                        "default": 60,
                        "maximum": 120
                    }
                },
                "required": ["run_id"]
            },
            handler=self._execute_await_run
        )

        # List runs
        self._tools["list_runs"] = MCPTool(
            name="list_runs",
            description="List Parallel task runs from the durable local store.",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "default": 20
                    },
                    "status": {
                        "type": "string",
                        "enum": ["queued", "action_required", "running", "completed", "failed", "cancelling", "cancelled"],
                        "description": "Optional status filter"
                    }
                }
            },
            handler=self._execute_list_runs
        )

        # Parallel Search API (synchronous)
        self._tools["parallel_search"] = MCPTool(
            name="parallel_search",
            description="Synchronous web search via Parallel.ai's Search API (POST /v1/search). Returns ranked results with excerpts.",
            input_schema={
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "Natural-language description of the search goal"
                    },
                    "search_queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Concise keyword queries (3-6 words each). Derived from objective if omitted."
                    },
                    "mode": {
                        "type": "string",
                        "enum": PARALLEL_SEARCH_MODES,
                        "description": "Search mode (default: advanced)"
                    },
                    "processor": {
                        "type": "string",
                        "enum": list(LEGACY_SEARCH_PROCESSOR_MAP.keys()),
                        "description": "Legacy alias for mode: base->basic, pro->advanced"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Max results (advanced_settings.max_results)",
                        "default": 10
                    },
                    "max_chars_per_result": {
                        "type": "integer",
                        "description": "Excerpt character cap per result"
                    },
                    "max_chars_total": {
                        "type": "integer",
                        "description": "Character limit across all results"
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Session id for cross-call research context"
                    },
                    "client_model": {
                        "type": "string",
                        "description": "Consuming LLM name (tailors output, e.g. claude-opus-4-7)"
                    },
                    "source_policy": {
                        "type": "object",
                        "description": "{include_domains, exclude_domains, after_date}"
                    },
                    "fetch_policy": {
                        "type": "object",
                        "description": "{max_age_seconds, timeout_seconds, disable_cache_fallback}"
                    },
                    "location": {
                        "type": "string",
                        "description": "ISO-3166 alpha-2 country code"
                    }
                }
            },
            handler=self._execute_parallel_search
        )

        # Parallel Extract API (synchronous URL -> clean content)
        self._tools["parallel_extract"] = MCPTool(
            name="parallel_extract",
            description="Extract clean, LLM-optimized content from public URLs via Parallel.ai's Extract API (POST /v1/extract). Up to 20 URLs per call.",
            input_schema={
                "type": "object",
                "properties": {
                    "urls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "URLs to extract (max 20)"
                    },
                    "objective": {
                        "type": "string",
                        "description": "Natural-language description of what to extract"
                    },
                    "search_queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional keyword queries to focus excerpts"
                    },
                    "full_content": {
                        "type": "boolean",
                        "description": "Return full page content instead of just excerpts",
                        "default": False
                    },
                    "max_chars_total": {
                        "type": "integer",
                        "description": "Character limit across all results"
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Session id for cross-call research context"
                    },
                    "client_model": {
                        "type": "string",
                        "description": "Consuming LLM name (tailors output)"
                    },
                    "fetch_policy": {
                        "type": "object",
                        "description": "{max_age_seconds, timeout_seconds, disable_cache_fallback}"
                    }
                },
                "required": ["urls"]
            },
            handler=self._execute_parallel_extract
        )

        # Extract data
        self._tools["extract_data"] = MCPTool(
            name="extract_data",
            description="Extract structured data from a URL based on a schema.",
            input_schema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to extract data from"
                    },
                    "schema": {
                        "type": "object",
                        "description": "JSON schema defining what to extract"
                    },
                    "instructions": {
                        "type": "string",
                        "description": "Additional extraction instructions"
                    }
                },
                "required": ["url"]
            },
            handler=self._execute_extract_data
        )

        # Create task group
        self._tools["create_task_group"] = MCPTool(
            name="create_task_group",
            description="Create a batch of tasks to execute in parallel.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Group name"
                    },
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "input": {"type": "string"},
                                "processor": {"type": "string"}
                            }
                        },
                        "description": "List of tasks to execute"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["parallel", "sequential", "adaptive"],
                        "default": "parallel"
                    }
                },
                "required": ["tasks"]
            },
            handler=self._execute_create_task_group
        )

        # Find all (Parallel FindAll API v1beta)
        self._tools["find_all"] = MCPTool(
            name="find_all",
            description=(
                "Find all entities matching criteria across the web via the Parallel "
                "FindAll API (prospecting). Creates a findall run; poll with "
                "get_findall_run until terminal. Falls back to a local stub without "
                "PARALLEL_API_KEY."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "What to find (e.g., 'AI startups in San Francisco')"
                    },
                    "entity_type": {
                        "type": "string",
                        "description": "Kind of entity to collect (e.g., 'company', 'person')"
                    },
                    "match_conditions": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "object",
                                 "properties": {
                                     "name": {"type": "string"},
                                     "description": {"type": "string"}
                                 }}
                            ]
                        },
                        "description": "Conditions an entity must satisfy (strings or {name, description}); defaults to the objective"
                    },
                    "generator": {
                        "type": "string",
                        "description": "Candidate generator tier (base, core, pro, preview)",
                        "default": "base"
                    },
                    "schema": {
                        "type": "object",
                        "description": "Schema for extracted entities"
                    },
                    "result_limit": {
                        "type": "integer",
                        "description": "Maximum entities to collect",
                        "default": 50
                    },
                    "extra": {
                        "type": "object",
                        "description": "Extra request fields merged verbatim into the API body"
                    }
                },
                "required": ["objective"]
            },
            handler=self._execute_find_all
        )

        self._tools["get_findall_run"] = MCPTool(
            name="get_findall_run",
            description="Get status and collected entities of a Parallel FindAll run.",
            input_schema={
                "type": "object",
                "properties": {
                    "findall_id": {
                        "type": "string",
                        "description": "FindAll run id returned by find_all"
                    }
                },
                "required": ["findall_id"]
            },
            handler=self._execute_get_findall_run
        )

        self._tools["cancel_findall_run"] = MCPTool(
            name="cancel_findall_run",
            description="Cancel an active Parallel FindAll run.",
            input_schema={
                "type": "object",
                "properties": {
                    "findall_id": {
                        "type": "string",
                        "description": "FindAll run id returned by find_all"
                    }
                },
                "required": ["findall_id"]
            },
            handler=self._execute_cancel_findall_run
        )

        # Monitors (Parallel Monitor API - GA /v1/monitors)
        self._tools["create_monitor"] = MCPTool(
            name="create_monitor",
            description=(
                "Create a Parallel monitor: an event_stream watch over a query that "
                "emits events on new information, or a snapshot re-run of a prior "
                "task run, on a fixed frequency (1h..30d)."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["event_stream", "snapshot"],
                             "default": "event_stream"},
                    "query": {"type": "string",
                              "description": "What to watch for (event_stream monitors)"},
                    "frequency": {"type": "string",
                                  "description": "Re-check cadence, e.g. 1h, 24h, 7d"},
                    "processor": {"type": "string", "enum": ["lite", "base"],
                                  "default": "lite"},
                    "output_schema": {"type": "object",
                                      "description": "Structured schema for detected events"},
                    "include_backfill": {"type": "boolean"},
                    "task_run_id": {"type": "string",
                                    "description": "Run to re-execute (snapshot monitors)"},
                    "webhook": {"type": "object",
                                "description": "{url, event_types:[monitor.event.detected, ...]}"},
                    "metadata": {"type": "object"},
                    "settings": {"type": "object",
                                 "description": "Raw settings object (overrides the fields above)"}
                }
            },
            handler=self._execute_create_monitor
        )

        self._tools["list_monitors"] = MCPTool(
            name="list_monitors",
            description="List Parallel monitors (GET /v1/monitors).",
            input_schema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max monitors to return"},
                    "cursor": {"type": "string"},
                    "type": {"type": "string", "enum": ["event_stream", "snapshot"]},
                    "status": {"type": "string", "enum": ["active", "cancelled"]}
                }
            },
            handler=self._execute_list_monitors
        )

        self._tools["get_monitor_events"] = MCPTool(
            name="get_monitor_events",
            description="Get events emitted by a Parallel monitor.",
            input_schema={
                "type": "object",
                "properties": {
                    "monitor_id": {
                        "type": "string",
                        "description": "Monitor id returned by create_monitor"
                    },
                    "event_group_id": {"type": "string"},
                    "cursor": {"type": "string"},
                    "limit": {"type": "integer", "description": "Max events (<=100)"},
                    "include_completions": {"type": "boolean"}
                },
                "required": ["monitor_id"]
            },
            handler=self._execute_get_monitor_events
        )

        # Compact registration for the remaining Parallel API surface. Each
        # entry: (name, description, properties, required, handler).
        _extra_tools = [
            ("get_monitor", "Get a Parallel monitor's config and status.",
             {"monitor_id": {"type": "string"}}, ["monitor_id"],
             self._execute_get_monitor),
            ("trigger_monitor", "Manually trigger a Parallel monitor run now.",
             {"monitor_id": {"type": "string"}}, ["monitor_id"],
             self._execute_trigger_monitor),
            ("cancel_monitor", "Cancel (deactivate) a Parallel monitor.",
             {"monitor_id": {"type": "string"}}, ["monitor_id"],
             self._execute_cancel_monitor),
            ("findall_ingest",
             "Turn a plain-language objective into a ready-to-run FindAll spec "
             "(entity_type, match_conditions, generator) via POST /v1beta/findall/ingest.",
             {"objective": {"type": "string"}}, ["objective"],
             self._execute_findall_ingest),
            ("enrich_findall_run",
             "Add enrichment columns to a FindAll run's entities using a task processor.",
             {"findall_id": {"type": "string"},
              "processor": {"type": "string", "default": "core"},
              "output_schema": {"type": "object"},
              "mcp_servers": {"type": "array", "items": {"type": "object"}}},
             ["findall_id"], self._execute_enrich_findall_run),
            ("extend_findall_run",
             "Raise the match limit of an existing FindAll run to collect more entities.",
             {"findall_id": {"type": "string"},
              "additional_match_limit": {"type": "integer", "default": 10}},
             ["findall_id"], self._execute_extend_findall_run),
            ("entity_search",
             "Fast entity search (people or companies) via the FindAll Entity Search API - "
             "quicker and cheaper than a full FindAll run.",
             {"objective": {"type": "string"},
              "entity_type": {"type": "string", "enum": ["people", "companies"],
                              "default": "companies"},
              "match_limit": {"type": "integer", "default": 100}},
             ["objective"], self._execute_entity_search),
            ("chat",
             "Parallel Chat API: low-latency web-research chat completions. Model 'speed' "
             "(~3s) or research models lite/base/core (return cited basis).",
             {"prompt": {"type": "string", "description": "Single-turn prompt (or use messages)"},
              "messages": {"type": "array", "items": {"type": "object"},
                           "description": "OpenAI-style chat messages"},
              "model": {"type": "string", "enum": ["speed", "lite", "base", "core"],
                        "default": "speed"},
              "response_format": {"type": "object",
                                  "description": "OpenAI-style response_format (json_schema etc.)"},
              "previous_interaction_id": {"type": "string"}},
             [], self._execute_chat),
            ("suggest_task_spec",
             "Task Ingest API: convert a natural-language intent into a recommended "
             "Parallel task spec (input/output schemas).",
             {"user_intent": {"type": "string"},
              "previous_task": {"type": "object"}},
             ["user_intent"], self._execute_suggest_task_spec),
            ("suggest_processor",
             "Task Ingest API: recommend the best Parallel processor for a task.",
             {"user_intent": {"type": "string"},
              "task_spec": {"type": "object"}},
             [], self._execute_suggest_processor),
            ("get_run_input",
             "Retrieve the original input of a Parallel task run.",
             {"run_id": {"type": "string"}}, ["run_id"],
             self._execute_get_run_input),
            ("create_group",
             "Create a durable server-side Parallel task group (GA /v1/tasks/groups).",
             {"metadata": {"type": "object"}}, [],
             self._execute_create_group),
            ("add_runs_to_group",
             "Add up to 1000 task runs to a Parallel task group in one call.",
             {"taskgroup_id": {"type": "string"},
              "inputs": {"type": "array",
                         "items": {"anyOf": [{"type": "string"}, {"type": "object"}]},
                         "description": "Run inputs: strings or full TaskRunInput objects"},
              "processor": {"type": "string", "default": "lite"},
              "default_task_spec": {"type": "object"}},
             ["taskgroup_id", "inputs"], self._execute_add_runs_to_group),
            ("get_group",
             "Get aggregate status/metrics of a Parallel task group.",
             {"taskgroup_id": {"type": "string"}}, ["taskgroup_id"],
             self._execute_get_group),
            ("list_group_runs",
             "List runs in a Parallel task group (optionally with inputs/outputs).",
             {"taskgroup_id": {"type": "string"},
              "status": {"type": "string"},
              "include_input": {"type": "boolean"},
              "include_output": {"type": "boolean"},
              "last_event_id": {"type": "string"}},
             ["taskgroup_id"], self._execute_list_group_runs),
        ]
        for _name, _desc, _props, _req, _handler in _extra_tools:
            self._tools[_name] = MCPTool(
                name=_name,
                description=_desc,
                input_schema={"type": "object", "properties": _props,
                              **({"required": _req} if _req else {})},
                handler=_handler,
            )

        # List tasks
        self._tools["list_tasks"] = MCPTool(
            name="list_tasks",
            description="List all tasks with their status and results.",
            input_schema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["pending", "running", "completed", "failed", "all"],
                        "default": "all"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 20
                    }
                }
            },
            handler=self._execute_list_tasks
        )

        # Get task
        self._tools["get_task"] = MCPTool(
            name="get_task",
            description="Get details and result of a specific task.",
            input_schema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task ID"
                    }
                },
                "required": ["task_id"]
            },
            handler=self._execute_get_task
        )

        # Synthesize
        self._tools["synthesize"] = MCPTool(
            name="synthesize",
            description="Synthesize information from multiple sources into a coherent summary.",
            input_schema={
                "type": "object",
                "properties": {
                    "sources": {
                        "type": "array",
                        "items": {
                            "anyOf": [
                                {"type": "string"},
                                {"type": "object",
                                 "properties": {
                                     "url": {"type": "string"},
                                     "title": {"type": "string"},
                                     "content": {"type": "string"}
                                 }}
                            ]
                        },
                        "description": "Texts, URLs, or {url|title|content} objects to synthesize"
                    },
                    "objective": {
                        "type": "string",
                        "description": "What to focus on in synthesis (alias: question)"
                    },
                    "format": {
                        "type": "string",
                        "enum": ["summary", "bullets", "report", "analysis"],
                        "default": "summary"
                    }
                },
                "required": ["sources"]
            },
            handler=self._execute_synthesize
        )

        # Connectivity test
        self._tools["_connectivity_test"] = MCPTool(
            name="_connectivity_test",
            description="Internal connectivity test for native MCP verification.",
            input_schema={
                "type": "object",
                "properties": {
                    "echo": {
                        "type": "string",
                        "description": "Echo string for verification"
                    }
                }
            },
            handler=self._execute_connectivity_test
        )

        # Integration feedback tool
        self._tools["integration_feedback"] = MCPTool(
            name="integration_feedback",
            description=(
                "Submit feedback about your experience using a tool on this integration "
                "server (correctness, schema clarity, missing capabilities, errors, etc). "
                "Call this after using a tool if something was confusing, broken, or "
                "worked great — it helps us improve the API for AI agents."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": "Which tool this feedback concerns (use 'general' for service-wide feedback)",
                    },
                    "outcome": {
                        "type": "string",
                        "enum": ["success", "partial", "failure"],
                        "description": "How the tool call(s) being reported on went",
                    },
                    "category": {
                        "type": "string",
                        "enum": [
                            "correctness", "schema_clarity", "performance", "error_handling",
                            "documentation", "missing_capability", "rate_limit", "auth", "other",
                        ],
                        "default": "other",
                    },
                    "rating": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 5,
                        "description": "1 (poor) to 5 (excellent)",
                    },
                    "message": {
                        "type": "string",
                        "description": "Free-text feedback: what happened, what was confusing, what would help",
                    },
                    "suggested_fix": {"type": "string"},
                    "context": {
                        "type": "object",
                        "description": "Optional supporting context, e.g. the arguments used or error text received",
                    },
                    "session_id": {"type": "string"},
                    "trace_id": {"type": "string"},
                },
                "required": ["message"],
            },
            handler=self._execute_integration_feedback
        )

    async def start(self):
        """Start the MCP server and establish connectivity."""
        logger.info(f"Starting {self.SERVICE_NAME}...")

        self._state.status = ConnectionStatus.CONNECTING
        self._state.session_id = str(uuid.uuid4())

        # Initialize HTTP client
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        self._client = httpx.AsyncClient(
            timeout=120.0,
            headers=headers
        )

        # Run connectivity test
        try:
            result = await self._execute_connectivity_test({"echo": "native-research-mcp-init"})
            if result.get("success"):
                self._state.status = ConnectionStatus.CONNECTED
                self._state.connected_at = datetime.now(timezone.utc)
                self._state.last_heartbeat = datetime.now(timezone.utc)
                logger.info(f"✓ {self.SERVICE_NAME} connected (session: {self._state.session_id[:8]}...)")
            else:
                raise RuntimeError("Connectivity test failed")
        except Exception as e:
            self._state.status = ConnectionStatus.ERROR
            self._state.error_message = str(e)
            logger.error(f"✗ {self.SERVICE_NAME} failed to connect: {e}")
            raise

        # Re-attach fallback watchers for runs still active in the durable store
        try:
            resumed = self.resume_active_watchers()
            if resumed:
                logger.info(f"Resumed watchers for {resumed} active Parallel run(s)")
        except Exception as e:
            logger.warning(f"Could not resume run watchers: {e}")

        # Announce to Tool Management
        await self._announce_to_registry()

    async def stop(self):
        """Stop the MCP server."""
        logger.info(f"Stopping {self.SERVICE_NAME}...")

        for watcher in self._watchers.values():
            if not watcher.done():
                watcher.cancel()
        self._watchers.clear()

        if self._client:
            await self._client.aclose()
            self._client = None

        self._state.status = ConnectionStatus.DISCONNECTED
        self._state.connected_at = None
        logger.info(f"✓ {self.SERVICE_NAME} stopped")

    async def _announce_to_registry(self):
        """Announce this server and its tools to Tool Management."""
        registry_url = self.registry_url or os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
        service_url = os.getenv("SERVICE_URL", f"http://localhost:{self.port}")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    f"{registry_url}/api/v1/tools/announce",
                    json={
                        "backend_id": self.SERVICE_ID,
                        "name": self.SERVICE_NAME,
                        "url": service_url,
                        "health_endpoint": "/health",
                        "mcp_endpoint": "/mcp",
                        "capabilities": list(self._tools.keys()),
                        "tools": self.tools,
                    },
                    timeout=15.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Announced {result.get('registered', 0)} tools to {registry_url}")
                else:
                    logger.warning(f"Announce returned {response.status_code}: {response.text[:200]}")
        except Exception as e:
            logger.warning(f"Could not announce to registry: {e}")

    # =========================================================================
    # MCP PROTOCOL HANDLERS
    # =========================================================================

    async def handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP initialize request."""
        return {
            "protocolVersion": MCPProtocolVersion.V1.value,
            "capabilities": {
                "tools": {},
            },
            "serverInfo": {
                "name": self.SERVICE_NAME,
                "version": self.VERSION,
            }
        }

    async def handle_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request."""
        tools = []
        for name, tool in self._tools.items():
            if not name.startswith("_"):
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.input_schema
                })
        return {"tools": tools}

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request."""
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")

        if not tool.handler:
            raise ValueError(f"Tool {tool_name} has no handler")

        result = await tool.handler(arguments)

        mcp_result = {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result) if isinstance(result, dict) else str(result)
                }
            ]
        }

        # Apply output guard: enforce size limits, cap links, capture URLs
        if OUTPUT_GUARD_AVAILABLE:
            mcp_result = guard_mcp_content(
                mcp_result,
                tool_name=tool_name,
                source_context={"server": self.SERVICE_ID},
            )

        return mcp_result

    async def handle_mcp_request(self, request: MCPRequest) -> MCPResponse:
        """Route MCP request to appropriate handler."""
        try:
            handlers = {
                "initialize": self.handle_initialize,
                "tools/list": self.handle_tools_list,
                "tools/call": self.handle_tools_call,
            }

            handler = handlers.get(request.method)
            if not handler:
                return MCPResponse(
                    id=request.id,
                    error={
                        "code": -32601,
                        "message": f"Method not found: {request.method}"
                    }
                )

            result = await handler(request.params or {})
            return MCPResponse(id=request.id, result=result)

        except Exception as e:
            logger.error(f"MCP request error: {e}")
            return MCPResponse(
                id=request.id,
                error={
                    "code": -32000,
                    "message": str(e)
                }
            )

    # =========================================================================
    # TOOL IMPLEMENTATIONS
    # =========================================================================

    async def _execute_deep_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute deep research on a topic using Jina search + LLM synthesis."""
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")

        depth = args.get("depth", "standard")
        max_sources = args.get("max_sources", 10)

        # 1. Generate search queries
        queries = self._generate_research_queries(objective, depth)

        # 2. Search: prefer Parallel's Search API (one call, all queries), fall back to Jina
        findings: List[Dict[str, Any]] = []
        search_backend = "jina"
        if self.api_key:
            search_result = await self._execute_parallel_search({
                "objective": objective,
                "search_queries": queries[:5],
                "mode": "advanced" if depth == "thorough" else "basic",
                "max_results": max_sources,
            })
            if search_result.get("success"):
                search_backend = "parallel"
                for r in search_result.get("results", []):
                    excerpts = r.get("excerpts") or []
                    findings.append({
                        "title": r.get("title", ""),
                        "url": r.get("url", ""),
                        "description": " ".join(excerpts)[:500] if excerpts else "",
                        "relevant": True,
                    })
            else:
                logger.warning(f"Parallel search failed, falling back to Jina: {search_result.get('error')}")

        if search_backend != "parallel":
            # Jina fallback: per-query searches capped at 5 concurrent
            sem = asyncio.Semaphore(5)

            async def _search_one(query: str) -> List[Dict[str, Any]]:
                async with sem:
                    jina_results = await self._jina_search(query, num_results=3)
                    if jina_results:
                        return [
                            {
                                "query": query,
                                "title": r.get("title", ""),
                                "url": r.get("url", ""),
                                "description": r.get("description", r.get("content", ""))[:500],
                                "relevant": True,
                            }
                            for r in jina_results
                        ]
                    return [{"query": query, "note": f"Native search result for: {query}", "relevant": True}]

            search_tasks = [_search_one(q) for q in queries[:max_sources]]
            search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

            for res in search_results:
                if isinstance(res, Exception):
                    logger.warning(f"Search task failed: {res}")
                    continue
                findings.extend(res)

        # 3. Synthesize findings with LLM if available
        synthesis = None
        processor = ProcessorType.TURBO if depth == "thorough" else ProcessorType.LITE
        if findings:
            findings_text = "\n".join(
                f"- {f.get('title', f.get('query', ''))}: {f.get('description', f.get('note', ''))}"
                for f in findings[:20]
            )
            llm_result = await self._call_llm(
                prompt=f"Research objective: {objective}\n\nFindings:\n{findings_text}\n\nProvide a comprehensive synthesis of these findings.",
                system="You are a research analyst. Synthesize the provided findings into a clear, structured summary.",
                processor=processor,
            )
            if llm_result:
                synthesis = llm_result

        # 4. Create task to track this research
        task = Task(
            id=str(uuid.uuid4()),
            processor=processor,
            input=objective,
            status=TaskStatus.COMPLETED,
            result={"findings": findings, "synthesis": synthesis},
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        result = {
            "objective": objective,
            "depth": depth,
            "queries_generated": len(queries),
            "search_backend": search_backend,
            "findings": findings,
            "task_id": task.id,
            "success": True,
        }
        if synthesis:
            result["synthesis"] = synthesis

        return result

    def _generate_research_queries(self, objective: str, depth: str) -> List[str]:
        """Generate diverse research queries from an objective."""
        base_queries = [
            objective,
            f"{objective} overview",
            f"{objective} latest developments",
            f"{objective} key players",
            f"{objective} challenges",
        ]

        if depth == "thorough":
            base_queries.extend([
                f"{objective} case studies",
                f"{objective} statistics",
                f"{objective} future trends",
                f"{objective} expert opinions",
                f"{objective} comparison analysis",
            ])

        return base_queries

    # =========================================================================
    # PARALLEL RUN TOOLS (webhook-native)
    # =========================================================================

    @staticmethod
    def _row_to_run_dict(row: Dict[str, Any], include_result: bool = True) -> Dict[str, Any]:
        out = {
            "run_id": row["run_id"],
            "objective": row.get("objective"),
            "processor": row.get("processor"),
            "status": row.get("status"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "webhook_received_at": row.get("webhook_received_at"),
            "error": row.get("error"),
            "has_result": bool(row.get("result_json")),
        }
        if include_result and row.get("result_json"):
            try:
                out["result"] = json.loads(row["result_json"])
            except (json.JSONDecodeError, TypeError):
                out["result"] = row["result_json"]
        return out

    async def _execute_research_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Parallel Task API run; return immediately with the run_id."""
        input_text = args.get("input") or args.get("objective")
        if not input_text:
            raise ValueError("input (or objective) is required")

        processor = args.get("processor", "lite")
        processor = LEGACY_PROCESSOR_MAP.get(processor, processor)
        if processor not in PARALLEL_PROCESSORS:
            raise ValueError(
                f"Invalid processor '{processor}'. Valid: {', '.join(PARALLEL_PROCESSORS)}"
            )

        if not self.api_key:
            return {
                "error": (
                    "PARALLEL_API_KEY not configured - cannot create Parallel task runs. "
                    "Set PARALLEL_API_KEY in the environment."
                ),
                "success": False,
            }

        ok, data, err, webhook_attached = await self._parallel_create_run(
            input_text,
            processor,
            output_schema=args.get("output_schema"),
            metadata=args.get("metadata"),
            source_policy=args.get("source_policy"),
            mcp_servers=args.get("mcp_servers"),
            location=args.get("location"),
            enable_events=args.get("enable_events"),
            previous_interaction_id=args.get("previous_interaction_id"),
        )
        if not ok:
            return {"error": err, "success": False}

        run_id = data.get("run_id")
        if not run_id:
            return {"error": f"Parallel API returned no run_id: {json.dumps(data, default=str)[:500]}", "success": False}

        status = data.get("status", "queued")
        self.run_store.insert(
            run_id,
            objective=str(input_text)[:2000],
            processor=processor,
            status=status,
        )
        # Fallback watcher in case the webhook never arrives (or SERVICE_URL is unset)
        self._ensure_watcher(run_id)

        return {
            "run_id": run_id,
            "status": status,
            "processor": processor,
            "webhook": webhook_attached,
            "is_active": data.get("is_active", True),
            "created_at": data.get("created_at"),
            "success": True,
        }

    async def _execute_get_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Read a run from the local durable store (webhook-updated)."""
        run_id = args.get("run_id")
        if not run_id:
            raise ValueError("run_id is required")

        row = self.run_store.get(run_id)
        if not row and self.api_key:
            # Unknown locally (e.g. created elsewhere): hydrate from the API once
            ok, data, err = await self._parallel_get_run(run_id)
            if ok and isinstance(data, dict) and data.get("run_id"):
                self.run_store.insert(
                    run_id,
                    objective="(hydrated from Parallel API)",
                    processor=str(data.get("processor") or ""),
                    status=data.get("status", "queued"),
                )
                self.run_store.update(run_id, status=data.get("status", "queued"))
                if data.get("status") == "completed":
                    await self._fetch_and_store_result(run_id)
                elif data.get("status") not in TERMINAL_RUN_STATUSES:
                    self._ensure_watcher(run_id)
                row = self.run_store.get(run_id)
            elif err and not isinstance(data, httpx.Response):
                return {"error": err, "success": False}

        if not row:
            return {"error": f"Run not found: {run_id}", "success": False}

        result = self._row_to_run_dict(row)
        result["success"] = True
        return result

    async def _execute_await_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Wait for webhook/watcher completion via asyncio.Event (no client-side polling)."""
        run_id = args.get("run_id")
        if not run_id:
            raise ValueError("run_id is required")

        timeout_seconds = min(float(args.get("timeout_seconds", 60) or 60), 120.0)

        row = self.run_store.get(run_id)
        if not row:
            return {"error": f"Run not found: {run_id}", "success": False}

        if row["status"] not in TERMINAL_RUN_STATUSES:
            event = self._run_event(run_id)
            self._ensure_watcher(run_id)
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
            except asyncio.TimeoutError:
                row = self.run_store.get(run_id) or row
                result = self._row_to_run_dict(row, include_result=False)
                result.update({
                    "timed_out": True,
                    "waited_seconds": timeout_seconds,
                    "success": True,
                    "note": "Run still active - webhook/watcher will update the store; call await_run or get_run again.",
                })
                return result
            row = self.run_store.get(run_id) or row

        result = self._row_to_run_dict(row)
        result["timed_out"] = False
        result["success"] = True
        return result

    async def _execute_list_runs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List runs from the durable store."""
        limit = int(args.get("limit", 20) or 20)
        status = args.get("status")
        rows = self.run_store.list(limit=limit, status=status)
        return {
            "runs": [self._row_to_run_dict(r, include_result=False) for r in rows],
            "total": self.run_store.count(),
            "returned": len(rows),
            "success": True,
        }

    async def _execute_parallel_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous Parallel Search API call (POST /v1/search)."""
        objective = args.get("objective")
        search_queries = args.get("search_queries") or []
        if isinstance(search_queries, str):
            search_queries = [search_queries]
        if not search_queries and objective:
            search_queries = [objective[:200]]
        if not search_queries:
            raise ValueError("objective or search_queries is required")

        if not self.api_key:
            return {
                "error": (
                    "PARALLEL_API_KEY not configured - cannot call the Parallel Search API. "
                    "Set PARALLEL_API_KEY in the environment."
                ),
                "success": False,
            }

        mode = args.get("mode")
        legacy_processor = args.get("processor")
        if not mode and legacy_processor:
            mode = LEGACY_SEARCH_PROCESSOR_MAP.get(legacy_processor)
        if mode and mode not in PARALLEL_SEARCH_MODES:
            raise ValueError(f"Invalid mode '{mode}'. Valid: {', '.join(PARALLEL_SEARCH_MODES)}")

        body: Dict[str, Any] = {"search_queries": search_queries}
        if objective:
            body["objective"] = objective
        if mode:
            body["mode"] = mode
        for key in ("max_chars_total", "session_id", "client_model"):
            if args.get(key) is not None:
                body[key] = args[key]

        advanced: Dict[str, Any] = {}
        if args.get("max_results"):
            advanced["max_results"] = int(args["max_results"])
        if args.get("max_chars_per_result"):
            advanced["excerpt_settings"] = {"max_chars_per_result": int(args["max_chars_per_result"])}
        for key in ("source_policy", "fetch_policy", "location"):
            if args.get(key) is not None:
                advanced[key] = args[key]
        if advanced:
            body["advanced_settings"] = advanced

        ok, data, err = await self._parallel_request("POST", "/v1/search", json_body=body, timeout=180.0)
        if not ok:
            return {"error": err, "success": False}

        return {
            "search_id": data.get("search_id"),
            "results": data.get("results", []),
            "objective": objective,
            "search_queries": search_queries,
            "mode": mode or "advanced",
            "success": True,
        }

    async def _execute_parallel_extract(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous Parallel Extract API call (POST /v1/extract)."""
        urls = args.get("urls") or []
        if isinstance(urls, str):
            urls = [urls]
        if not urls:
            raise ValueError("urls is required")
        if len(urls) > 20:
            raise ValueError("Extract API accepts at most 20 URLs per call")

        if not self.api_key:
            return {
                "error": (
                    "PARALLEL_API_KEY not configured - cannot call the Parallel Extract API. "
                    "Set PARALLEL_API_KEY in the environment."
                ),
                "success": False,
            }

        body: Dict[str, Any] = {"urls": urls}
        if args.get("objective"):
            body["objective"] = args["objective"]
        if args.get("search_queries"):
            body["search_queries"] = args["search_queries"]
        if args.get("max_chars_total"):
            body["max_chars_total"] = int(args["max_chars_total"])
        for key in ("session_id", "client_model"):
            if args.get(key) is not None:
                body[key] = args[key]
        advanced: Dict[str, Any] = {}
        if args.get("full_content"):
            advanced["full_content"] = True
        if args.get("fetch_policy") is not None:
            advanced["fetch_policy"] = args["fetch_policy"]
        if advanced:
            body["advanced_settings"] = advanced

        ok, data, err = await self._parallel_request("POST", "/v1/extract", json_body=body, timeout=180.0)
        if not ok:
            return {"error": err, "success": False}

        return {
            "extract_id": data.get("extract_id"),
            "results": data.get("results", []),
            "errors": data.get("errors", []),
            "success": True,
        }

    async def _execute_run_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a task. With PARALLEL_API_KEY: creates a durable Parallel run and
        awaits briefly (webhook-native). Without a key: falls back to local LLM.
        """
        input_text = args.get("input")
        if not input_text:
            raise ValueError("input is required")

        raw_processor = args.get("processor", "lite")
        output_schema = args.get("output_schema")
        timeout = args.get("timeout", 120)

        if self.api_key:
            created = await self._execute_research_task({
                "input": input_text,
                "processor": raw_processor,
                "output_schema": output_schema,
            })
            if not created.get("success"):
                # Surface the API's error verbatim - no silent LLM fallback with a configured key
                return {"task_id": None, "status": "failed", "error": created.get("error"), "success": False}

            run_id = created["run_id"]
            awaited = await self._execute_await_run({
                "run_id": run_id,
                "timeout_seconds": min(int(timeout or 120), 120),
            })
            status = awaited.get("status", created.get("status"))
            response: Dict[str, Any] = {
                "task_id": run_id,
                "run_id": run_id,
                "processor": created.get("processor"),
                "status": status,
                "webhook": created.get("webhook"),
                "success": status != "failed",
            }
            if awaited.get("result") is not None:
                response["result"] = awaited["result"]
            if awaited.get("error"):
                response["error"] = awaited["error"]
            if awaited.get("timed_out"):
                response["timed_out"] = True
                response["note"] = "Run still in progress - use await_run/get_run with this run_id."
            return response

        # ------- legacy local-LLM fallback (no PARALLEL_API_KEY configured) -------
        legacy = args.get("processor", "lite")
        try:
            processor = ProcessorType(legacy)
        except ValueError:
            processor = ProcessorType.LITE

        # Create task
        task = Task(
            id=str(uuid.uuid4()),
            processor=processor,
            input=input_text,
            status=TaskStatus.RUNNING
        )
        self._tasks[task.id] = task

        try:
            start_time = time.time()

            # Try real LLM processing
            system_prompt = "You are a helpful assistant. Complete the following task."
            if output_schema:
                system_prompt += f"\n\nReturn your response as JSON matching this schema: {json.dumps(output_schema)}"

            llm_output = await self._call_llm(
                prompt=input_text,
                system=system_prompt,
                processor=processor,
            )

            processing_time_ms = (time.time() - start_time) * 1000

            if llm_output:
                output = llm_output
            else:
                # Fallback to stub behavior
                output = f"Processed result for: {input_text[:100]}..."

            result = {
                "input": input_text,
                "processor": processor.value,
                "output": output,
                "metadata": {
                    "tokens_used": len(input_text.split()) * 2,
                    "processing_time_ms": round(processing_time_ms),
                    "llm_used": llm_output is not None,
                }
            }

            if output_schema:
                result["schema_applied"] = True

            task.status = TaskStatus.COMPLETED
            task.result = result
            task.completed_at = datetime.now(timezone.utc)

            return {
                "task_id": task.id,
                "status": task.status.value,
                "result": result,
                "success": True
            }

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            return {
                "task_id": task.id,
                "status": task.status.value,
                "error": str(e),
                "success": False
            }

    async def _execute_extract_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from URL using Jina reader + LLM."""
        url = args.get("url")
        if not url:
            raise ValueError("url is required")

        schema = args.get("schema", {})
        instructions = args.get("instructions", "")

        # Fetch page content via Jina Reader
        page_content = await self._jina_read(url)

        extracted = {
            "title": f"Content from {url}",
            "data": {},
            "schema_used": bool(schema),
        }

        if page_content:
            # Use LLM to extract structured data
            prompt = f"Extract structured data from the following web page content.\n\nURL: {url}\n"
            if schema:
                prompt += f"\nExtract data matching this schema: {json.dumps(schema)}\n"
            if instructions:
                prompt += f"\nAdditional instructions: {instructions}\n"
            prompt += f"\nPage content:\n{page_content[:8000]}\n\nReturn the extracted data as JSON."

            llm_result = await self._call_llm(
                prompt=prompt,
                system="You are a data extraction specialist. Extract structured data from web content and return it as clean JSON.",
                processor=ProcessorType.LITE,
            )

            if llm_result:
                # Try to parse as JSON
                try:
                    extracted["data"] = json.loads(llm_result)
                except json.JSONDecodeError:
                    extracted["data"] = {"raw_extraction": llm_result}
                extracted["llm_used"] = True
            else:
                extracted["page_content_length"] = len(page_content)

        task = Task(
            id=str(uuid.uuid4()),
            processor=ProcessorType.LITE,
            input=f"Extract from {url}",
            status=TaskStatus.COMPLETED,
            result={"url": url, "extracted": extracted},
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        return {
            "url": url,
            "task_id": task.id,
            "extracted": extracted,
            "success": True,
        }

    async def _execute_create_task_group(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create and execute a batch of parallel tasks."""
        tasks_config = args.get("tasks", [])
        if not tasks_config:
            raise ValueError("tasks list is required")

        name = args.get("name", f"group-{uuid.uuid4().hex[:8]}")
        strategy = args.get("strategy", "parallel")

        # Create tasks
        task_ids = []
        for task_config in tasks_config:
            task = Task(
                id=str(uuid.uuid4()),
                processor=ProcessorType(task_config.get("processor", "lite")),
                input=task_config.get("input", ""),
                status=TaskStatus.PENDING
            )
            self._tasks[task.id] = task
            task_ids.append(task.id)

        # Store group
        self._task_groups[name] = task_ids

        # Execute tasks based on strategy
        if strategy == "parallel":
            # Execute all in parallel
            results = await asyncio.gather(*[
                self._execute_run_task({"input": self._tasks[tid].input, "processor": self._tasks[tid].processor.value})
                for tid in task_ids
            ], return_exceptions=True)
        else:
            # Execute sequentially
            results = []
            for tid in task_ids:
                result = await self._execute_run_task({
                    "input": self._tasks[tid].input,
                    "processor": self._tasks[tid].processor.value
                })
                results.append(result)

        return {
            "group_name": name,
            "strategy": strategy,
            "task_count": len(task_ids),
            "task_ids": task_ids,
            "results": results,
            "success": True
        }

    async def _execute_find_all(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Find all matching entities via the Parallel FindAll API (v1beta)."""
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")

        schema = args.get("schema") or {}
        result_limit = int(args.get("result_limit") or args.get("max_results") or 50)

        if not self.api_key:
            # Graceful degradation: keep the legacy local stub when no key is set
            task = Task(
                id=str(uuid.uuid4()),
                processor=ProcessorType.TURBO,
                input=f"FindAll: {objective}",
                status=TaskStatus.COMPLETED,
                result={"objective": objective, "entities": [], "total_found": 0,
                        "schema_used": bool(schema)},
                completed_at=datetime.now(timezone.utc)
            )
            self._tasks[task.id] = task
            return {
                "objective": objective,
                "task_id": task.id,
                "entities": [],
                "total_found": 0,
                "note": "PARALLEL_API_KEY not configured - returning empty local stub",
                "success": True
            }

        # Verified request shape (2026-07): objective + entity_type +
        # match_conditions[{name, description}] + generator + match_limit (5-1000).
        conditions_in = args.get("match_conditions") or args.get("match_criteria") or []
        conditions: List[Dict[str, str]] = []
        for i, cond in enumerate(conditions_in):
            if isinstance(cond, str):
                conditions.append({"name": f"criterion_{i + 1}", "description": cond})
            elif isinstance(cond, dict):
                desc = cond.get("description") or cond.get("criterion") or cond.get("name") or ""
                conditions.append({
                    "name": str(cond.get("name") or f"criterion_{i + 1}"),
                    "description": str(desc),
                })
        if not conditions:
            conditions = [{"name": "matches_objective", "description": objective}]

        body: Dict[str, Any] = {
            "objective": objective,
            "entity_type": str(args.get("entity_type") or "entity"),
            "match_conditions": conditions,
            "generator": str(args.get("generator") or "base"),
            "match_limit": min(1000, max(5, result_limit)),
        }
        if args.get("metadata") is not None:
            body["metadata"] = args["metadata"]
        if isinstance(args.get("extra"), dict):
            body.update(args["extra"])

        ok, data, err = await self._parallel_request(
            "POST", "/v1beta/findall/runs", json_body=body, timeout=120.0,
        )
        if not ok:
            return {"objective": objective, "error": err, "success": False}

        findall_id = data.get("findall_id") or data.get("run_id") or data.get("id")
        return {
            "objective": objective,
            "findall_id": findall_id,
            "status": data.get("status"),
            "hint": "Poll with get_findall_run {findall_id}; cancel with cancel_findall_run",
            "success": True,
        }

    async def _execute_get_findall_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Status + collected entities for a FindAll run (GET .../result)."""
        findall_id = args.get("findall_id") or args.get("run_id")
        if not findall_id:
            raise ValueError("findall_id is required")
        ok, data, err = await self._parallel_request(
            "GET", f"/v1beta/findall/runs/{findall_id}/result", timeout=60.0,
        )
        if not ok:
            # Fall back to the plain run-status endpoint
            ok, data, err = await self._parallel_request(
                "GET", f"/v1beta/findall/runs/{findall_id}", timeout=60.0,
            )
        if not ok:
            return {"findall_id": findall_id, "error": err, "success": False}
        return {"findall_id": findall_id, "result": data, "success": True}

    async def _execute_cancel_findall_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/findall/runs/{findall_id}/cancel."""
        findall_id = args.get("findall_id") or args.get("run_id")
        if not findall_id:
            raise ValueError("findall_id is required")
        ok, _data, err = await self._parallel_request(
            "POST", f"/v1beta/findall/runs/{findall_id}/cancel", timeout=30.0,
        )
        if not ok:
            return {"findall_id": findall_id, "error": err, "success": False}
        return {"findall_id": findall_id, "cancelled": True, "success": True}

    async def _execute_findall_ingest(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/findall/ingest - turn a plain objective into a full run spec."""
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")
        ok, data, err = await self._parallel_request(
            "POST", "/v1beta/findall/ingest", json_body={"objective": objective}, timeout=120.0,
        )
        if not ok:
            return {"objective": objective, "error": err, "success": False}
        return {"objective": objective, "spec": data,
                "hint": "Pass these fields to find_all to launch the run", "success": True}

    async def _execute_enrich_findall_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/findall/runs/{findall_id}/enrich - add task-processor columns."""
        findall_id = args.get("findall_id")
        if not findall_id:
            raise ValueError("findall_id is required")
        body: Dict[str, Any] = {"processor": args.get("processor") or "core"}
        if args.get("output_schema") is not None:
            body["output_schema"] = args["output_schema"]
        if args.get("mcp_servers") is not None:
            body["mcp_servers"] = args["mcp_servers"]
        ok, data, err = await self._parallel_request(
            "POST", f"/v1beta/findall/runs/{findall_id}/enrich", json_body=body, timeout=120.0,
        )
        if not ok:
            return {"findall_id": findall_id, "error": err, "success": False}
        return {"findall_id": findall_id, "result": data, "success": True}

    async def _execute_extend_findall_run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/findall/runs/{findall_id}/extend - raise the match limit."""
        findall_id = args.get("findall_id")
        if not findall_id:
            raise ValueError("findall_id is required")
        additional = int(args.get("additional_match_limit") or 10)
        ok, data, err = await self._parallel_request(
            "POST", f"/v1beta/findall/runs/{findall_id}/extend",
            json_body={"additional_match_limit": additional}, timeout=60.0,
        )
        if not ok:
            return {"findall_id": findall_id, "error": err, "success": False}
        return {"findall_id": findall_id, "extended_by": additional, "result": data, "success": True}

    async def _execute_entity_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/findall/entity-search - fast people/company entity search."""
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")
        body = {
            "objective": objective,
            "entity_type": args.get("entity_type") or "companies",
            "match_limit": min(1000, max(5, int(args.get("match_limit") or 100))),
        }
        ok, data, err = await self._parallel_request(
            "POST", "/v1beta/findall/entity-search", json_body=body, timeout=180.0,
        )
        if not ok:
            return {"objective": objective, "error": err, "success": False}
        return {"objective": objective, "result": data, "success": True}

    async def _execute_chat(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/chat/completions - Parallel Chat API (web-research chat)."""
        messages = args.get("messages")
        if not messages:
            prompt = args.get("prompt") or args.get("input")
            if not prompt:
                raise ValueError("messages (or prompt) is required")
            messages = [{"role": "user", "content": str(prompt)}]
        body: Dict[str, Any] = {
            "model": args.get("model") or "speed",
            "messages": messages,
            "stream": False,
        }
        for key in ("response_format", "previous_interaction_id"):
            if args.get(key) is not None:
                body[key] = args[key]
        ok, data, err = await self._parallel_request(
            "POST", "/v1beta/chat/completions", json_body=body, timeout=180.0,
        )
        if not ok:
            return {"error": err, "success": False}
        choice = (data.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        return {
            "model": body["model"],
            "content": message.get("content"),
            "basis": message.get("basis") or data.get("basis"),
            "interaction_id": data.get("id"),
            "usage": data.get("usage"),
            "success": True,
        }

    async def _execute_suggest_task_spec(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/tasks/suggest - natural-language intent -> task spec."""
        user_intent = args.get("user_intent") or args.get("intent") or args.get("objective")
        if not user_intent:
            raise ValueError("user_intent is required")
        body: Dict[str, Any] = {"user_intent": user_intent}
        if args.get("previous_task") is not None:
            body["previous_task"] = args["previous_task"]
        ok, data, err = await self._parallel_request(
            "POST", "/v1beta/tasks/suggest", json_body=body, timeout=120.0,
        )
        if not ok:
            return {"error": err, "success": False}
        return {"suggestion": data, "success": True}

    async def _execute_suggest_processor(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1beta/tasks/suggest-processor - recommend a processor.

        The API requires a full task_spec {input_schema, output_schema}; when
        only user_intent is given, derive one via /v1beta/tasks/suggest first.
        """
        task_spec = args.get("task_spec")
        derived = False
        if not task_spec:
            user_intent = args.get("user_intent") or args.get("intent") or args.get("objective")
            if not user_intent:
                raise ValueError("task_spec or user_intent is required")
            ok, data, err = await self._parallel_request(
                "POST", "/v1beta/tasks/suggest",
                json_body={"user_intent": user_intent}, timeout=120.0,
            )
            if not ok:
                return {"error": f"Could not derive task_spec: {err}", "success": False}
            task_spec = (data or {}).get("task_spec") or data
            derived = True
        ok, data, err = await self._parallel_request(
            "POST", "/v1beta/tasks/suggest-processor",
            json_body={"task_spec": task_spec}, timeout=120.0,
        )
        if not ok:
            return {"error": err, "success": False}
        result = {"recommendation": data, "success": True}
        if derived:
            result["derived_task_spec"] = task_spec
        return result

    async def _execute_get_run_input(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/tasks/runs/{run_id}/input - original input of a task run."""
        run_id = args.get("run_id")
        if not run_id:
            raise ValueError("run_id is required")
        ok, data, err = await self._parallel_request(
            "GET", f"/v1/tasks/runs/{run_id}/input", timeout=60.0,
        )
        if not ok:
            return {"run_id": run_id, "error": err, "success": False}
        return {"run_id": run_id, "input": data, "success": True}

    # ------------------------------------------------------------ Task Groups
    # GA at /v1/tasks/groups (moved from /v1beta). create_task_group remains
    # the local fan-out convenience; these speak the durable server-side API.
    async def _execute_create_group(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/tasks/groups."""
        body: Dict[str, Any] = {}
        if args.get("metadata") is not None:
            body["metadata"] = args["metadata"]
        ok, data, err = await self._parallel_request(
            "POST", "/v1/tasks/groups", json_body=body, timeout=60.0,
        )
        if not ok:
            return {"error": err, "success": False}
        return {"group": data, "taskgroup_id": data.get("taskgroup_id") or data.get("id"),
                "success": True}

    async def _execute_add_runs_to_group(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/tasks/groups/{taskgroup_id}/runs - batch-add up to 1000 runs."""
        group_id = args.get("taskgroup_id") or args.get("group_id")
        inputs = args.get("inputs") or args.get("runs")
        if not group_id or not inputs:
            raise ValueError("taskgroup_id and inputs are required")
        norm_inputs = []
        default_processor = args.get("processor") or "lite"
        for item in inputs:
            if isinstance(item, str):
                norm_inputs.append({"input": item, "processor": default_processor})
            else:
                run = dict(item)
                run.setdefault("processor", default_processor)
                norm_inputs.append(run)
        body: Dict[str, Any] = {"inputs": norm_inputs}
        if args.get("default_task_spec") is not None:
            body["default_task_spec"] = args["default_task_spec"]
        ok, data, err = await self._parallel_request(
            "POST", f"/v1/tasks/groups/{group_id}/runs", json_body=body, timeout=120.0,
        )
        if not ok:
            return {"taskgroup_id": group_id, "error": err, "success": False}
        return {"taskgroup_id": group_id, "result": data, "success": True}

    async def _execute_get_group(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/tasks/groups/{taskgroup_id} - aggregate status."""
        group_id = args.get("taskgroup_id") or args.get("group_id")
        if not group_id:
            raise ValueError("taskgroup_id is required")
        ok, data, err = await self._parallel_request(
            "GET", f"/v1/tasks/groups/{group_id}", timeout=60.0,
        )
        if not ok:
            return {"taskgroup_id": group_id, "error": err, "success": False}
        return {"taskgroup_id": group_id, "group": data, "success": True}

    async def _execute_list_group_runs(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/tasks/groups/{taskgroup_id}/runs."""
        group_id = args.get("taskgroup_id") or args.get("group_id")
        if not group_id:
            raise ValueError("taskgroup_id is required")
        params = {k: args[k] for k in ("last_event_id", "status", "include_input", "include_output")
                  if args.get(k) is not None}
        ok, data, err = await self._parallel_request(
            "GET", f"/v1/tasks/groups/{group_id}/runs", params=params or None, timeout=120.0,
        )
        if not ok:
            return {"taskgroup_id": group_id, "error": err, "success": False}
        return {"taskgroup_id": group_id, "runs": data, "success": True}

    # Monitor API went GA at /v1/monitors (2026-05). GA contract: settings
    # wrapper keyed by type (event_stream|snapshot); cancel/update/trigger are
    # POST subresources. v1alpha kept as a fallback for older deployments.
    async def _monitor_request(self, method: str, path: str, **kw):
        ok, data, err = await self._parallel_request(method, f"/v1/monitors{path}", **kw)
        if not ok and err and " 404" in (err or ""):
            ok, data, err = await self._parallel_request(method, f"/v1alpha/monitors{path}", **kw)
        return ok, data, err

    async def _execute_create_monitor(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/monitors - recurring watch (event_stream) or snapshot re-run."""
        mtype = args.get("type") or "event_stream"
        body: Dict[str, Any] = {"type": mtype}
        if args.get("frequency") or args.get("cadence"):
            body["frequency"] = args.get("frequency") or args.get("cadence")
        if args.get("processor"):
            body["processor"] = args["processor"]
        for key in ("webhook", "metadata"):
            if args.get(key) is not None:
                body[key] = args[key]
        settings = args.get("settings")
        if settings is None:
            if mtype == "event_stream":
                query = args.get("query") or args.get("objective")
                if not query:
                    raise ValueError("query (or settings) is required for an event_stream monitor")
                settings = {"query": query}
                if args.get("output_schema") is not None:
                    settings["output_schema"] = args["output_schema"]
                if args.get("include_backfill") is not None:
                    settings["include_backfill"] = bool(args["include_backfill"])
            elif mtype == "snapshot":
                if not args.get("task_run_id"):
                    raise ValueError("task_run_id (or settings) is required for a snapshot monitor")
                settings = {"task_run_id": args["task_run_id"]}
        if settings is not None:
            body["settings"] = settings
        ok, data, err = await self._monitor_request("POST", "", json_body=body, timeout=60.0)
        if not ok:
            return {"error": err, "success": False}
        return {"monitor": data, "success": True}

    async def _execute_list_monitors(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/monitors."""
        params = {k: args[k] for k in ("cursor", "limit", "type", "status")
                  if args.get(k) is not None}
        ok, data, err = await self._monitor_request("GET", "", params=params or None, timeout=60.0)
        if not ok:
            return {"error": err, "success": False}
        return {"monitors": data, "success": True}

    async def _execute_get_monitor(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/monitors/{monitor_id}."""
        monitor_id = args.get("monitor_id")
        if not monitor_id:
            raise ValueError("monitor_id is required")
        ok, data, err = await self._monitor_request("GET", f"/{monitor_id}", timeout=60.0)
        if not ok:
            return {"monitor_id": monitor_id, "error": err, "success": False}
        return {"monitor": data, "success": True}

    async def _execute_get_monitor_events(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """GET /v1/monitors/{monitor_id}/events."""
        monitor_id = args.get("monitor_id")
        if not monitor_id:
            raise ValueError("monitor_id is required")
        params = {k: args[k] for k in ("event_group_id", "cursor", "limit", "include_completions")
                  if args.get(k) is not None}
        ok, data, err = await self._monitor_request(
            "GET", f"/{monitor_id}/events", params=params or None, timeout=60.0
        )
        if not ok:
            return {"monitor_id": monitor_id, "error": err, "success": False}
        return {"monitor_id": monitor_id, "events": data, "success": True}

    async def _execute_trigger_monitor(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/monitors/{monitor_id}/trigger - run the monitor now."""
        monitor_id = args.get("monitor_id")
        if not monitor_id:
            raise ValueError("monitor_id is required")
        ok, data, err = await self._monitor_request("POST", f"/{monitor_id}/trigger", timeout=60.0)
        if not ok:
            return {"monitor_id": monitor_id, "error": err, "success": False}
        return {"monitor_id": monitor_id, "triggered": True, "result": data, "success": True}

    async def _execute_cancel_monitor(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/monitors/{monitor_id}/cancel."""
        monitor_id = args.get("monitor_id")
        if not monitor_id:
            raise ValueError("monitor_id is required")
        ok, data, err = await self._monitor_request("POST", f"/{monitor_id}/cancel", timeout=60.0)
        if not ok:
            return {"monitor_id": monitor_id, "error": err, "success": False}
        return {"monitor_id": monitor_id, "cancelled": True, "success": True}

    @staticmethod
    def _legacy_status(store_status: Optional[str]) -> str:
        """Map Parallel run statuses onto the legacy task status vocabulary."""
        return {
            "queued": "pending",
            "action_required": "running",
            "running": "running",
            "cancelling": "running",
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
        }.get(store_status or "", store_status or "pending")

    async def _execute_list_tasks(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List all tasks (legacy in-memory tasks merged with durable Parallel runs)."""
        status_filter = args.get("status", "all")
        limit = args.get("limit", 20)

        entries: List[Dict[str, Any]] = []

        # Durable Parallel runs
        for row in self.run_store.list(limit=max(limit * 2, 50)):
            entries.append({
                "id": row["run_id"],
                "processor": row.get("processor") or "",
                "status": self._legacy_status(row.get("status")),
                "run_status": row.get("status"),
                "input_preview": (row.get("objective") or "")[:100],
                "created_at": row.get("created_at"),
                "completed_at": row.get("updated_at") if row.get("status") in TERMINAL_RUN_STATUSES else None,
                "source": "parallel",
            })

        # Legacy in-memory tasks
        for t in self._tasks.values():
            entries.append({
                "id": t.id,
                "processor": t.processor.value,
                "status": t.status.value,
                "input_preview": t.input[:100] + "..." if len(t.input) > 100 else t.input,
                "created_at": t.created_at.isoformat(),
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                "source": "local",
            })

        if status_filter != "all":
            entries = [e for e in entries if e["status"] == status_filter]

        entries.sort(key=lambda e: e.get("created_at") or "", reverse=True)
        total = len(self._tasks) + self.run_store.count()
        entries = entries[:limit]

        return {
            "tasks": entries,
            "total": total,
            "returned": len(entries),
            "success": True
        }

    async def _execute_get_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get a specific task (durable Parallel run or legacy in-memory task)."""
        task_id = args.get("task_id")
        if not task_id:
            raise ValueError("task_id is required")

        # Durable Parallel run first
        row = self.run_store.get(task_id)
        if row:
            run = self._row_to_run_dict(row)
            return {
                "task": {
                    "id": row["run_id"],
                    "processor": row.get("processor") or "",
                    "input": row.get("objective"),
                    "status": self._legacy_status(row.get("status")),
                    "run_status": row.get("status"),
                    "result": run.get("result"),
                    "error": row.get("error"),
                    "created_at": row.get("created_at"),
                    "completed_at": row.get("updated_at") if row.get("status") in TERMINAL_RUN_STATUSES else None,
                    "webhook_received_at": row.get("webhook_received_at"),
                    "source": "parallel",
                },
                "success": True
            }

        task = self._tasks.get(task_id)
        if not task:
            return {
                "error": f"Task not found: {task_id}",
                "success": False
            }

        return {
            "task": {
                "id": task.id,
                "processor": task.processor.value,
                "input": task.input,
                "status": task.status.value,
                "result": task.result,
                "error": task.error,
                "created_at": task.created_at.isoformat(),
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "source": "local",
            },
            "success": True
        }

    async def _execute_synthesize(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synthesize information from multiple sources using Jina + LLM."""
        sources = args.get("sources", [])
        if not sources:
            raise ValueError("sources list is required")

        objective = args.get("objective") or args.get("question") or ""
        format_type = args.get("format", "summary")

        # Fetch content from URL sources; accept dict sources ({url} or
        # {title, content/text/snippet}) and plain strings alike.
        source_contents = []
        for source in sources:
            if isinstance(source, dict):
                url = source.get("url") or source.get("link")
                text = source.get("content") or source.get("text") or source.get("snippet") or ""
                if url and not text:
                    fetched = await self._jina_read(url)
                    text = fetched[:4000] if fetched else "(Could not fetch)"
                label = url or source.get("title") or "inline source"
                source_contents.append(f"[Source: {label}]\n{str(text)[:4000]}")
                continue
            source = str(source)
            if source.startswith("http://") or source.startswith("https://"):
                content = await self._jina_read(source)
                if content:
                    source_contents.append(f"[Source: {source}]\n{content[:4000]}")
                else:
                    source_contents.append(f"[Source: {source}]\n(Could not fetch)")
            else:
                source_contents.append(source)

        combined = "\n\n---\n\n".join(source_contents)

        # Try LLM synthesis
        format_instructions = {
            "summary": "Provide a concise summary.",
            "bullets": "Provide key points as a bulleted list.",
            "report": "Provide a structured report with sections.",
            "analysis": "Provide a critical analysis comparing the sources.",
        }

        prompt = f"Synthesize the following {len(sources)} sources"
        if objective:
            prompt += f", focusing on: {objective}"
        prompt += f"\n\nFormat: {format_instructions.get(format_type, format_type)}\n\nSources:\n{combined[:12000]}"

        synthesis = await self._call_llm(
            prompt=prompt,
            system="You are a research synthesist. Combine information from multiple sources into a coherent output.",
            processor=ProcessorType.TURBO,
        )

        if not synthesis:
            # Fallback stub
            synthesis = f"Synthesized content from {len(sources)} sources" + (f" focusing on: {objective}" if objective else "")

        task = Task(
            id=str(uuid.uuid4()),
            processor=ProcessorType.TURBO,
            input=f"Synthesize {len(sources)} sources",
            status=TaskStatus.COMPLETED,
            result={
                "sources_count": len(sources),
                "format": format_type,
                "synthesis": synthesis,
            },
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        return {
            "task_id": task.id,
            "sources_processed": len(sources),
            "format": format_type,
            "synthesis": synthesis,
            "success": True,
        }

    async def _execute_connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Internal connectivity verification."""
        echo = args.get("echo", "test")

        return {
            "success": True,
            "echo": echo,
            "server_id": self.SERVICE_ID,
            "version": self.VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": self._state.session_id,
            "tools_registered": len(self._tools),
            "tasks_in_memory": len(self._tasks),
            "api_key_configured": bool(self.api_key),
        }

    async def _execute_integration_feedback(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Forward agent feedback about this server's tools to the central registry."""
        message = args.get("message")
        if not message:
            return {
                "content": [{"type": "text", "text": "message is required"}],
                "isError": True,
            }

        payload = {
            "backend_id": self.SERVICE_ID,
            "message": message,
            "tool_name": args.get("tool_name"),
            "outcome": args.get("outcome"),
            "category": args.get("category") or "other",
            "rating": args.get("rating"),
            "suggested_fix": args.get("suggested_fix"),
            "context": args.get("context"),
            "session_id": args.get("session_id"),
            "trace_id": args.get("trace_id"),
        }

        registry_url = self.registry_url or os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{registry_url}/api/v1/integration-feedback",
                    json=payload,
                    timeout=10.0,
                )
            if response.status_code == 200:
                return {"content": [{"type": "text", "text": json.dumps(response.json())}]}
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
        except Exception as e:
            fallback_path = Path("data") / "feedback_fallback.jsonl"
            try:
                fallback_path.parent.mkdir(parents=True, exist_ok=True)
                record = dict(payload)
                record["recorded_at"] = datetime.now(timezone.utc).isoformat()
                record["registry_error"] = str(e)
                with open(fallback_path, "a") as f:
                    f.write(json.dumps(record, default=str) + "\n")
            except Exception as write_err:
                logger.warning(f"Failed to write feedback fallback: {write_err}")
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "status": "recorded_locally",
                        "warning": f"registry unreachable ({e}); saved to {fallback_path}",
                    }),
                }]
            }

    # =========================================================================
    # PROPERTIES
    # =========================================================================

    @property
    def is_connected(self) -> bool:
        return self._state.status == ConnectionStatus.CONNECTED

    @property
    def tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema
            }
            for name, t in self._tools.items()
            if not name.startswith("_")
        ]

    def get_status(self) -> Dict[str, Any]:
        return {
            "service_id": self.SERVICE_ID,
            "service_name": self.SERVICE_NAME,
            "version": self.VERSION,
            "status": self._state.status.value,
            "connected_at": self._state.connected_at.isoformat() if self._state.connected_at else None,
            "session_id": self._state.session_id,
            "tools_count": len([t for t in self._tools if not t.startswith("_")]),
            "tasks_count": len(self._tasks),
            "task_groups_count": len(self._task_groups),
            "api_key_configured": bool(self.api_key),
            "parallel": {
                "api_key_configured": bool(self.api_key),
                "webhook_url": self._webhook_url(),
                "webhook_secret_configured": bool(self.webhook_secret),
                "runs_tracked": self.run_store.count(),
                "active_watchers": sum(1 for t in self._watchers.values() if not t.done()),
                "run_store_path": str(self.run_store.path),
            },
        }


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

def create_server(
    api_key: Optional[str] = None,
    port: int = 8102,
    registry_url: Optional[str] = None,
) -> DistributedResearchMCP:
    """Create and return a DistributedResearchMCP instance."""
    return DistributedResearchMCP(
        api_key=api_key,
        port=port,
        registry_url=registry_url
    )


# Global server instance
_server: Optional[DistributedResearchMCP] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage server lifecycle."""
    global _server

    _server = create_server(
        port=int(os.getenv("PORT", "4002")),
        registry_url=os.getenv("REGISTRY_URL")
    )

    await _server.start()

    # Background heartbeat: re-announce to registry every 60s
    _heartbeat_task = None

    async def _heartbeat_loop():
        await asyncio.sleep(30)
        while True:
            try:
                await _server._announce_to_registry()
                logger.debug("Heartbeat: re-announced to registry")
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
            await asyncio.sleep(60)

    _heartbeat_task = asyncio.create_task(_heartbeat_loop())

    yield

    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass

    await _server.stop()


app = FastAPI(
    title="Distributed Research MCP",
    description="Native MCP server for deep research and parallel task execution",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    """Service info."""
    if _server:
        return _server.get_status()
    return {"status": "initializing"}


@app.get("/health")
async def health():
    """Health check."""
    if _server and _server.is_connected:
        return {"status": "healthy", "service": DistributedResearchMCP.SERVICE_ID}
    return JSONResponse(
        status_code=503,
        content={"status": "unhealthy", "service": DistributedResearchMCP.SERVICE_ID}
    )


@app.get("/tools")
async def list_tools():
    """List available MCP tools."""
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return {"tools": _server.tools}


@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP JSON-RPC endpoint."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    response = await _server.handle_mcp_request(request)
    return response.model_dump(exclude_none=True)


@app.post("/execute")
async def execute_tool(request: ToolCallRequest):
    """Execute a tool directly (non-MCP interface)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")

    if not tool.handler:
        raise HTTPException(500, f"Tool {request.name} has no handler")

    result = await tool.handler(request.arguments)
    return {"result": result}


def _sse_event(event_name: str, data: Dict[str, Any]) -> str:
    return f"event: {event_name}\ndata: {json.dumps(data, default=str)}\n\n"


@app.post("/execute/stream")
async def execute_tool_stream(request: ToolCallRequest, raw_request: Request):
    """Execute a tool with SSE streaming and progressive chunk emission."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")
    if not tool.handler:
        raise HTTPException(500, f"Tool {request.name} has no handler")

    async def stream_deep_search(call_id: str, started_at: float) -> AsyncGenerator[str, None]:
        args = dict(request.arguments or {})
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")
        depth = args.get("depth", "standard")
        max_sources = args.get("max_sources", 10)
        queries = _server._generate_research_queries(objective, depth)[:max_sources]
        sem = asyncio.Semaphore(5)

        async def _search_one(query: str) -> List[Dict[str, Any]]:
            async with sem:
                jina_results = await _server._jina_search(query, num_results=3)
                if jina_results:
                    return [
                        {
                            "query": query,
                            "title": r.get("title", ""),
                            "url": r.get("url", ""),
                            "description": r.get("description", r.get("content", ""))[:500],
                            "relevant": True,
                        }
                        for r in jina_results
                    ]
                return [{"query": query, "note": f"Native search result for: {query}", "relevant": True}]

        tasks = [asyncio.create_task(_search_one(query)) for query in queries]
        findings: List[Dict[str, Any]] = []
        completed_queries = 0
        synthesis = None
        processor = ProcessorType.TURBO if depth == "thorough" else ProcessorType.LITE

        try:
            for task in asyncio.as_completed(tasks):
                if await raw_request.is_disconnected():
                    for pending in tasks:
                        if not pending.done():
                            pending.cancel()
                    return

                try:
                    task_results = await task
                except Exception as exc:
                    logger.warning(f"Deep search stream query failed: {exc}")
                    task_results = []
                completed_queries += 1

                for item in task_results:
                    findings.append(item)
                    progress = min(0.85, completed_queries / max(1, len(queries)))
                    yield _sse_event(
                        "chunk",
                        {
                            "index": len(findings) - 1,
                            "type": "result",
                            "data": item,
                            "progress": progress,
                        },
                    )

                if completed_queries >= 2:
                    yield _sse_event(
                        "signal",
                        {
                            "type": "sufficient",
                            "reason": f"{completed_queries} query batches completed",
                            "can_stop": True,
                        },
                    )

            if findings:
                findings_text = "\n".join(
                    f"- {f.get('title', f.get('query', ''))}: {f.get('description', f.get('note', ''))}"
                    for f in findings[:20]
                )
                llm_result = await _server._call_llm(
                    prompt=(
                        f"Research objective: {objective}\n\nFindings:\n{findings_text}\n\n"
                        "Provide a comprehensive synthesis of these findings."
                    ),
                    system="You are a research analyst. Synthesize the provided findings into a clear, structured summary.",
                    processor=processor,
                )
                if llm_result:
                    synthesis = llm_result
                    yield _sse_event(
                        "chunk",
                        {
                            "index": len(findings),
                            "type": "synthesis",
                            "data": {"synthesis": synthesis},
                            "progress": 0.95,
                        },
                    )

            task_obj = Task(
                id=str(uuid.uuid4()),
                processor=processor,
                input=objective,
                status=TaskStatus.COMPLETED,
                result={"findings": findings, "synthesis": synthesis},
                completed_at=datetime.now(timezone.utc),
            )
            _server._tasks[task_obj.id] = task_obj

            result = {
                "objective": objective,
                "depth": depth,
                "queries_generated": len(queries),
                "findings": findings,
                "task_id": task_obj.id,
                "success": True,
            }
            if synthesis:
                result["synthesis"] = synthesis

            yield _sse_event(
                "done",
                {
                    "call_id": call_id,
                    "duration_ms": (time.time() - started_at) * 1000.0,
                    "total_chunks": len(findings) + (1 if synthesis else 0),
                    "stopped_early": False,
                    "data": result,
                },
            )
        finally:
            for pending in tasks:
                if not pending.done():
                    pending.cancel()

    async def stream_generic(call_id: str, started: float) -> AsyncGenerator[str, None]:
        result = await tool.handler(request.arguments)
        chunks = result if isinstance(result, list) else [result]
        for idx, chunk in enumerate(chunks):
            if await raw_request.is_disconnected():
                break
            yield _sse_event(
                "chunk",
                {
                    "index": idx,
                    "type": "result",
                    "data": chunk,
                    "progress": float(idx + 1) / max(1, len(chunks)),
                },
            )
        yield _sse_event(
            "done",
            {
                "call_id": call_id,
                "duration_ms": (time.time() - started) * 1000.0,
                "total_chunks": len(chunks),
                "stopped_early": False,
                "data": result,
            },
        )

    async def event_generator() -> AsyncGenerator[str, None]:
        call_id = f"c_{uuid.uuid4().hex[:12]}"
        started_at = time.time()
        yield _sse_event(
            "meta",
            {
                "call_id": call_id,
                "tool": request.name,
                "backend": DistributedResearchMCP.SERVICE_ID,
                "started_at": started_at,
            },
        )
        try:
            if request.name == "deep_search":
                async for event in stream_deep_search(call_id, started_at):
                    yield event
            else:
                async for event in stream_generic(call_id, started_at):
                    yield event
        except Exception as exc:
            yield _sse_event(
                "error",
                {
                    "call_id": call_id,
                    "error": {
                        "code": "STREAM_EXECUTION_FAILED",
                        "message": str(exc),
                        "retryable": True,
                        "category": "transient",
                    },
                },
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/webhooks/parallel")
async def parallel_webhook(request: Request):
    """
    Parallel.ai task-run webhook receiver (event type: task_run.status).

    Verifies the HMAC signature when PARALLEL_WEBHOOK_SECRET is set
    (webhook-id/webhook-timestamp/webhook-signature headers, HMAC-SHA256,
    base64 - Standard Webhooks scheme), updates the durable run store,
    fetches the full result for completed runs, and wakes await_run waiters.
    """
    if not _server:
        raise HTTPException(503, "Server not initialized")

    body = await request.body()
    if not _server.verify_parallel_webhook(dict(request.headers), body):
        return JSONResponse(status_code=401, content={"error": "invalid webhook signature"})

    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return JSONResponse(status_code=400, content={"error": "invalid JSON payload"})

    result = await _server.process_parallel_webhook(payload)
    if not result.get("success", False):
        return JSONResponse(status_code=400, content=result)
    return result


@app.get("/runs")
async def list_runs_endpoint(status: Optional[str] = None, limit: int = 20):
    """List Parallel task runs from the durable store."""
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return await _server._execute_list_runs({"status": status, "limit": limit})


@app.get("/runs/{run_id}")
async def get_run_endpoint(run_id: str):
    """Get a Parallel task run from the durable store."""
    if not _server:
        raise HTTPException(503, "Server not initialized")
    result = await _server._execute_get_run({"run_id": run_id})
    if not result.get("success"):
        raise HTTPException(404, result.get("error", "Run not found"))
    return result


@app.get("/tasks")
async def list_all_tasks(status: str = "all", limit: int = 20):
    """List all tasks."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    result = await _server._execute_list_tasks({"status": status, "limit": limit})
    return result


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    """Get a specific task."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    result = await _server._execute_get_task({"task_id": task_id})
    if not result.get("success"):
        raise HTTPException(404, result.get("error", "Task not found"))
    return result


@app.get("/mcp/sse")
@app.get("/sse")
async def sse_endpoint(request: Request):
    """Server-Sent Events endpoint for MCP streaming."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    async def event_generator() -> AsyncGenerator[str, None]:
        yield f"data: {json.dumps({'type': 'connected', 'session_id': _server._state.session_id})}\n\n"

        while True:
            if await request.is_disconnected():
                break

            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat(), 'tasks_count': len(_server._tasks)})}\n\n"
            await asyncio.sleep(30)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.post("/mcp/sse")
@app.post("/sse")
async def sse_post_endpoint(request: Request):
    """Handle MCP JSON-RPC POST requests via SSE URL (mcp-remote pattern)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    body = await request.json()
    mcp_request = MCPRequest(**body)
    response = await _server.handle_mcp_request(mcp_request)
    return response.model_dump(exclude_none=True)


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "4002"))

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║         DISTRIBUTED RESEARCH MCP - Native Server                 ║
║               Second Native MCP Implementation                   ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-research-mcp                            ║
║  Port: {port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools:                                                          ║
║  • research_task     - Async Parallel task run (webhook-native)  ║
║  • await_run         - Wait for webhook-delivered results        ║
║  • get_run           - Read run from durable local store         ║
║  • list_runs         - List durable Parallel runs                ║
║  • parallel_search   - Parallel Search API (sync)                ║
║  • parallel_extract  - Parallel Extract API (sync)               ║
║  • deep_search       - Multi-source research                     ║
║  • run_task          - Execute autonomous tasks                  ║
║  • extract_data      - Structured data extraction                ║
║  • create_task_group - Parallel task batches                     ║
║  • find_all          - Entity prospecting                        ║
║  • list_tasks        - View task history                         ║
║  • get_task          - Get task details                          ║
║  • synthesize        - Combine multiple sources                  ║
╠══════════════════════════════════════════════════════════════════╣
║  Endpoints:                                                      ║
║  • GET  /            Service info                                ║
║  • GET  /health      Health check                                ║
║  • GET  /tools       List tools                                  ║
║  • POST /mcp         MCP JSON-RPC endpoint                       ║
║  • POST /execute     Direct tool execution                       ║
║  • POST /webhooks/parallel  Parallel.ai webhook receiver         ║
║  • GET  /runs, /runs/:id    Durable run store                    ║
║  • GET  /tasks       List all tasks                              ║
║  • GET  /tasks/:id   Get task details                            ║
║  • GET  /sse         Server-Sent Events stream                   ║
╚══════════════════════════════════════════════════════════════════╝

Starting native MCP server...
""")

    # Use import string to ensure proper module loading and route registration
    # This avoids the double-module issue when running with python -m
    uvicorn.run(
        "native.distributed_research_mcp.server:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
