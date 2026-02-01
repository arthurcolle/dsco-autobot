"""
Durable Task Queue Manager for Tool Management API.

Provides persistent task storage with SQLite, admin operations,
and integration with multiple MCP backends.
"""

import asyncio
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
import logging
import httpx

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Task lifecycle states."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RETRYING = "retrying"


class TaskPriority(int, Enum):
    """Task priority levels."""
    LOW = 0
    NORMAL = 5
    HIGH = 10
    URGENT = 20
    CRITICAL = 100


@dataclass
class Task:
    """Represents a queued task."""
    id: str
    name: str
    input_data: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    backend: Optional[str] = None  # Which MCP server handles this
    backend_task_id: Optional[str] = None  # ID on the backend
    output_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "input_data": self.input_data,
            "status": self.status.value,
            "priority": self.priority.value,
            "backend": self.backend,
            "backend_task_id": self.backend_task_id,
            "output_data": self.output_data,
            "error": self.error,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        return cls(
            id=data["id"],
            name=data["name"],
            input_data=data.get("input_data", {}),
            status=TaskStatus(data.get("status", "pending")),
            priority=TaskPriority(data.get("priority", 5)),
            backend=data.get("backend"),
            backend_task_id=data.get("backend_task_id"),
            output_data=data.get("output_data"),
            error=data.get("error"),
            retries=data.get("retries", 0),
            max_retries=data.get("max_retries", 3),
            timeout_seconds=data.get("timeout_seconds", 300),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.utcnow(),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            metadata=data.get("metadata", {}),
        )


@dataclass
class MCPBackend:
    """Represents a registered MCP server backend."""
    id: str
    name: str
    url: str
    health_endpoint: str = "/health"
    mcp_endpoint: str = "/mcp"
    is_healthy: bool = True
    last_health_check: Optional[datetime] = None
    capabilities: List[str] = field(default_factory=list)
    current_load: int = 0
    max_concurrent: int = 10
    priority: int = 0  # Higher = preferred
    metadata: Dict[str, Any] = field(default_factory=dict)


class DurableTaskQueue:
    """
    Persistent task queue with SQLite backend.

    Features:
    - Durable storage across restarts
    - Multi-backend support (parallel-ai, jina-ai, etc.)
    - Priority queue with admin operations
    - Automatic retry and timeout handling
    - Health monitoring for backends
    """

    def __init__(
        self,
        db_path: str = "task_queue.db",
        max_concurrent: int = 10,
        health_check_interval: int = 30,
    ):
        self.db_path = Path(db_path)
        self.max_concurrent = max_concurrent
        self.health_check_interval = health_check_interval

        self._backends: Dict[str, MCPBackend] = {}
        self._running_tasks: Dict[str, Task] = {}
        self._lock = threading.Lock()
        self._shutdown = False
        self._worker_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None

        self._init_db()

    def _init_db(self):
        """Initialize SQLite database schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    input_data TEXT,
                    status TEXT DEFAULT 'pending',
                    priority INTEGER DEFAULT 5,
                    backend TEXT,
                    backend_task_id TEXT,
                    output_data TEXT,
                    error TEXT,
                    retries INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    timeout_seconds INTEGER DEFAULT 300,
                    created_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    metadata TEXT,
                    UNIQUE(id)
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority DESC);
                CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_backend ON tasks(backend);

                CREATE TABLE IF NOT EXISTS backends (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    health_endpoint TEXT DEFAULT '/health',
                    mcp_endpoint TEXT DEFAULT '/mcp',
                    is_healthy INTEGER DEFAULT 1,
                    last_health_check TEXT,
                    capabilities TEXT,
                    current_load INTEGER DEFAULT 0,
                    max_concurrent INTEGER DEFAULT 10,
                    priority INTEGER DEFAULT 0,
                    metadata TEXT,
                    UNIQUE(id)
                );

                CREATE TABLE IF NOT EXISTS task_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT,
                    data TEXT,
                    FOREIGN KEY(task_id) REFERENCES tasks(id)
                );

                CREATE INDEX IF NOT EXISTS idx_logs_task ON task_logs(task_id);
            """)

    @contextmanager
    def _get_conn(self):
        """Get thread-safe database connection."""
        conn = sqlite3.connect(str(self.db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # =========================================================================
    # Backend Management
    # =========================================================================

    def register_backend(self, backend: MCPBackend) -> None:
        """Register an MCP backend server."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO backends
                (id, name, url, health_endpoint, mcp_endpoint, is_healthy,
                 last_health_check, capabilities, current_load, max_concurrent,
                 priority, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                backend.id, backend.name, backend.url, backend.health_endpoint,
                backend.mcp_endpoint, int(backend.is_healthy),
                backend.last_health_check.isoformat() if backend.last_health_check else None,
                json.dumps(backend.capabilities), backend.current_load,
                backend.max_concurrent, backend.priority, json.dumps(backend.metadata)
            ))

        self._backends[backend.id] = backend
        logger.info(f"Registered backend: {backend.name} at {backend.url}")

    def unregister_backend(self, backend_id: str) -> None:
        """Unregister an MCP backend."""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM backends WHERE id = ?", (backend_id,))
        self._backends.pop(backend_id, None)

    def list_backends(self) -> List[MCPBackend]:
        """List all registered backends."""
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM backends").fetchall()
            return [self._row_to_backend(row) for row in rows]

    def _row_to_backend(self, row: sqlite3.Row) -> MCPBackend:
        return MCPBackend(
            id=row["id"],
            name=row["name"],
            url=row["url"],
            health_endpoint=row["health_endpoint"],
            mcp_endpoint=row["mcp_endpoint"],
            is_healthy=bool(row["is_healthy"]),
            last_health_check=datetime.fromisoformat(row["last_health_check"]) if row["last_health_check"] else None,
            capabilities=json.loads(row["capabilities"]) if row["capabilities"] else [],
            current_load=row["current_load"],
            max_concurrent=row["max_concurrent"],
            priority=row["priority"],
            metadata=json.loads(row["metadata"]) if row["metadata"] else {},
        )

    async def check_backend_health(self, backend_id: str) -> bool:
        """Check health of a specific backend."""
        backend = self._backends.get(backend_id)
        if not backend:
            return False

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(f"{backend.url}{backend.health_endpoint}")
                is_healthy = response.status_code == 200
        except Exception as e:
            logger.warning(f"Backend {backend.name} health check failed: {e}")
            is_healthy = False

        backend.is_healthy = is_healthy
        backend.last_health_check = datetime.utcnow()

        with self._get_conn() as conn:
            conn.execute("""
                UPDATE backends SET is_healthy = ?, last_health_check = ?
                WHERE id = ?
            """, (int(is_healthy), backend.last_health_check.isoformat(), backend_id))

        return is_healthy

    async def check_all_backends(self) -> Dict[str, bool]:
        """Check health of all backends."""
        results = {}
        for backend_id in list(self._backends.keys()):
            results[backend_id] = await self.check_backend_health(backend_id)
        return results

    def get_available_backend(self, capabilities: Optional[List[str]] = None) -> Optional[MCPBackend]:
        """Get the best available backend for a task."""
        available = [
            b for b in self._backends.values()
            if b.is_healthy and b.current_load < b.max_concurrent
        ]

        if capabilities:
            available = [
                b for b in available
                if all(cap in b.capabilities for cap in capabilities)
            ]

        if not available:
            return None

        # Sort by priority (desc) then load (asc)
        available.sort(key=lambda b: (-b.priority, b.current_load))
        return available[0]

    # =========================================================================
    # Task Operations
    # =========================================================================

    def enqueue(
        self,
        name: str,
        input_data: Dict[str, Any],
        priority: TaskPriority = TaskPriority.NORMAL,
        backend: Optional[str] = None,
        timeout_seconds: int = 300,
        max_retries: int = 3,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Task:
        """Add a task to the queue."""
        task = Task(
            id=f"task_{uuid.uuid4().hex[:16]}",
            name=name,
            input_data=input_data,
            status=TaskStatus.QUEUED,
            priority=priority,
            backend=backend,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            metadata=metadata or {},
        )

        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO tasks
                (id, name, input_data, status, priority, backend, timeout_seconds,
                 max_retries, created_at, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.id, task.name, json.dumps(task.input_data),
                task.status.value, task.priority.value, task.backend,
                task.timeout_seconds, task.max_retries,
                task.created_at.isoformat(), json.dumps(task.metadata)
            ))

        self._log_task(task.id, "INFO", f"Task enqueued: {name}")
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row:
                return self._row_to_task(row)
        return None

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            name=row["name"],
            input_data=json.loads(row["input_data"]) if row["input_data"] else {},
            status=TaskStatus(row["status"]),
            priority=TaskPriority(row["priority"]),
            backend=row["backend"],
            backend_task_id=row["backend_task_id"],
            output_data=json.loads(row["output_data"]) if row["output_data"] else None,
            error=row["error"],
            retries=row["retries"],
            max_retries=row["max_retries"],
            timeout_seconds=row["timeout_seconds"],
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else datetime.utcnow(),
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            metadata=json.loads(row["metadata"]) if row["metadata"] else {},
        )

    def update_task(self, task: Task) -> None:
        """Update a task in the database."""
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE tasks SET
                    status = ?, priority = ?, backend = ?, backend_task_id = ?,
                    output_data = ?, error = ?, retries = ?, started_at = ?,
                    completed_at = ?, metadata = ?
                WHERE id = ?
            """, (
                task.status.value, task.priority.value, task.backend,
                task.backend_task_id,
                json.dumps(task.output_data) if task.output_data else None,
                task.error, task.retries,
                task.started_at.isoformat() if task.started_at else None,
                task.completed_at.isoformat() if task.completed_at else None,
                json.dumps(task.metadata), task.id
            ))

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        backend: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Task]:
        """List tasks with optional filtering."""
        query = "SELECT * FROM tasks WHERE 1=1"
        params = []

        if status:
            query += " AND status = ?"
            params.append(status.value)
        if backend:
            query += " AND backend = ?"
            params.append(backend)

        query += " ORDER BY priority DESC, created_at ASC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_task(row) for row in rows]

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        with self._get_conn() as conn:
            stats = {}

            # Count by status
            rows = conn.execute("""
                SELECT status, COUNT(*) as count FROM tasks GROUP BY status
            """).fetchall()
            stats["by_status"] = {row["status"]: row["count"] for row in rows}

            # Count by backend
            rows = conn.execute("""
                SELECT backend, COUNT(*) as count FROM tasks
                WHERE backend IS NOT NULL GROUP BY backend
            """).fetchall()
            stats["by_backend"] = {row["backend"]: row["count"] for row in rows}

            # Queue depth (pending + queued)
            row = conn.execute("""
                SELECT COUNT(*) as count FROM tasks
                WHERE status IN ('pending', 'queued')
            """).fetchone()
            stats["queue_depth"] = row["count"]

            # Running count
            row = conn.execute("""
                SELECT COUNT(*) as count FROM tasks WHERE status = 'running'
            """).fetchone()
            stats["running"] = row["count"]

            # Oldest queued task
            row = conn.execute("""
                SELECT created_at FROM tasks
                WHERE status IN ('pending', 'queued')
                ORDER BY created_at ASC LIMIT 1
            """).fetchone()
            stats["oldest_queued"] = row["created_at"] if row else None

            # Backend health
            stats["backends"] = {
                b.id: {"healthy": b.is_healthy, "load": b.current_load}
                for b in self._backends.values()
            }

            return stats

    # =========================================================================
    # Admin Operations
    # =========================================================================

    def cancel_task(self, task_id: str, reason: str = "Cancelled by admin") -> bool:
        """Cancel a specific task."""
        task = self.get_task(task_id)
        if not task:
            return False

        if task.status in (TaskStatus.COMPLETED, TaskStatus.CANCELLED):
            return False

        task.status = TaskStatus.CANCELLED
        task.error = reason
        task.completed_at = datetime.utcnow()
        self.update_task(task)
        self._log_task(task_id, "INFO", f"Task cancelled: {reason}")
        return True

    def cancel_tasks_by_filter(
        self,
        status: Optional[TaskStatus] = None,
        backend: Optional[str] = None,
        older_than: Optional[datetime] = None,
        name_pattern: Optional[str] = None,
    ) -> int:
        """Cancel multiple tasks matching filter criteria."""
        query = """
            UPDATE tasks SET
                status = 'cancelled',
                error = 'Bulk cancelled by admin',
                completed_at = ?
            WHERE status NOT IN ('completed', 'cancelled')
        """
        params = [datetime.utcnow().isoformat()]

        if status:
            query += " AND status = ?"
            params.append(status.value)
        if backend:
            query += " AND backend = ?"
            params.append(backend)
        if older_than:
            query += " AND created_at < ?"
            params.append(older_than.isoformat())
        if name_pattern:
            query += " AND name LIKE ?"
            params.append(f"%{name_pattern}%")

        with self._get_conn() as conn:
            cursor = conn.execute(query, params)
            return cursor.rowcount

    def cancel_stale_tasks(self, older_than_hours: int = 24) -> int:
        """Cancel all queued tasks older than specified hours."""
        cutoff = datetime.utcnow() - timedelta(hours=older_than_hours)
        return self.cancel_tasks_by_filter(
            status=TaskStatus.QUEUED,
            older_than=cutoff
        )

    def reprioritize_task(self, task_id: str, priority: TaskPriority) -> bool:
        """Change priority of a queued task."""
        task = self.get_task(task_id)
        if not task or task.status not in (TaskStatus.PENDING, TaskStatus.QUEUED):
            return False

        task.priority = priority
        self.update_task(task)
        self._log_task(task_id, "INFO", f"Priority changed to {priority.name}")
        return True

    def retry_task(self, task_id: str) -> bool:
        """Reset a failed task for retry."""
        task = self.get_task(task_id)
        if not task or task.status != TaskStatus.FAILED:
            return False

        task.status = TaskStatus.QUEUED
        task.error = None
        task.retries = 0
        task.started_at = None
        task.completed_at = None
        self.update_task(task)
        self._log_task(task_id, "INFO", "Task reset for retry")
        return True

    def cleanup_old_tasks(self, days: int = 30) -> int:
        """Delete completed/cancelled tasks older than specified days."""
        cutoff = datetime.utcnow() - timedelta(days=days)

        with self._get_conn() as conn:
            # First delete logs
            conn.execute("""
                DELETE FROM task_logs WHERE task_id IN (
                    SELECT id FROM tasks
                    WHERE status IN ('completed', 'cancelled')
                    AND completed_at < ?
                )
            """, (cutoff.isoformat(),))

            # Then delete tasks
            cursor = conn.execute("""
                DELETE FROM tasks
                WHERE status IN ('completed', 'cancelled')
                AND completed_at < ?
            """, (cutoff.isoformat(),))
            return cursor.rowcount

    def _log_task(self, task_id: str, level: str, message: str, data: Optional[Dict] = None):
        """Log a task event."""
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO task_logs (task_id, timestamp, level, message, data)
                VALUES (?, ?, ?, ?, ?)
            """, (task_id, datetime.utcnow().isoformat(), level, message,
                  json.dumps(data) if data else None))

    def get_task_logs(self, task_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get logs for a specific task."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM task_logs WHERE task_id = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (task_id, limit)).fetchall()
            return [dict(row) for row in rows]

    # =========================================================================
    # Queue Processing
    # =========================================================================

    async def start(self):
        """Start the queue processor."""
        self._shutdown = False
        self._worker_task = asyncio.create_task(self._process_queue())
        self._health_task = asyncio.create_task(self._health_check_loop())
        logger.info("Task queue started")

    async def stop(self):
        """Stop the queue processor."""
        self._shutdown = True
        if self._worker_task:
            self._worker_task.cancel()
        if self._health_task:
            self._health_task.cancel()
        logger.info("Task queue stopped")

    async def _process_queue(self):
        """Main queue processing loop."""
        while not self._shutdown:
            try:
                # Get next task
                task = self._get_next_task()
                if not task:
                    await asyncio.sleep(1)
                    continue

                # Find available backend
                backend = self.get_available_backend()
                if not backend:
                    await asyncio.sleep(5)
                    continue

                # Execute task
                await self._execute_task(task, backend)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Queue processing error: {e}")
                await asyncio.sleep(5)

    def _get_next_task(self) -> Optional[Task]:
        """Get the next task to process."""
        with self._lock:
            if len(self._running_tasks) >= self.max_concurrent:
                return None

            with self._get_conn() as conn:
                row = conn.execute("""
                    SELECT * FROM tasks
                    WHERE status = 'queued'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                """).fetchone()

                if row:
                    task = self._row_to_task(row)
                    task.status = TaskStatus.RUNNING
                    task.started_at = datetime.utcnow()
                    self.update_task(task)
                    self._running_tasks[task.id] = task
                    return task

        return None

    async def _execute_task(self, task: Task, backend: MCPBackend):
        """Execute a task on a backend."""
        try:
            backend.current_load += 1
            self._log_task(task.id, "INFO", f"Executing on backend {backend.name}")

            async with httpx.AsyncClient(timeout=task.timeout_seconds) as client:
                response = await client.post(
                    f"{backend.url}{backend.mcp_endpoint}",
                    json={
                        "jsonrpc": "2.0",
                        "id": task.id,
                        "method": "tools/call",
                        "params": {
                            "name": task.name,
                            "arguments": task.input_data
                        }
                    },
                    headers={"Content-Type": "application/json"}
                )

                result = response.json()

                if "error" in result:
                    raise Exception(result["error"].get("message", "Unknown error"))

                task.output_data = result.get("result", result)
                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.utcnow()
                self._log_task(task.id, "INFO", "Task completed successfully")

        except asyncio.TimeoutError:
            task.status = TaskStatus.TIMEOUT
            task.error = f"Task timed out after {task.timeout_seconds}s"
            task.completed_at = datetime.utcnow()
            self._log_task(task.id, "ERROR", task.error)

        except Exception as e:
            task.retries += 1
            if task.retries < task.max_retries:
                task.status = TaskStatus.QUEUED
                task.error = f"Attempt {task.retries} failed: {str(e)}"
                self._log_task(task.id, "WARNING", f"Retry {task.retries}: {e}")
            else:
                task.status = TaskStatus.FAILED
                task.error = str(e)
                task.completed_at = datetime.utcnow()
                self._log_task(task.id, "ERROR", f"Task failed after {task.retries} retries: {e}")

        finally:
            backend.current_load -= 1
            self.update_task(task)
            self._running_tasks.pop(task.id, None)

    async def _health_check_loop(self):
        """Periodic health check for backends."""
        while not self._shutdown:
            try:
                await self.check_all_backends()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(self.health_check_interval)


# Global instance
_task_queue: Optional[DurableTaskQueue] = None


def get_task_queue(db_path: str = "data/task_queue.db") -> DurableTaskQueue:
    """Get or create the global task queue instance."""
    global _task_queue
    if _task_queue is None:
        _task_queue = DurableTaskQueue(db_path=db_path)
    return _task_queue
