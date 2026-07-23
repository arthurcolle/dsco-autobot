#!/usr/bin/env python3
"""
ADVANCED AUTONOMOUS AGENT - Sophisticated Multi-Tool Architecture

Features:
- Meta-tools: call_tools, chain, spawn_agent, parallel_map
- Working memory to avoid context pollution
- Parallel execution with asyncio
- Tool chaining with dependency resolution
- Context compression for large results
- Sub-agent delegation for complex subtasks
- MCP server integration (Parallel AI, Jina, custom)
- Async task management with timeouts
"""

import os
import sys
import json
import time
import asyncio
import hashlib
import readline
import signal
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from functools import partial
import threading
import queue
import uuid
import socket

try:
    import anthropic
except ImportError:
    print("Error: anthropic package required. Run: uv pip install anthropic")
    sys.exit(1)

# Import our systems
from meta_protocol import UnifiedRuntime, Message, MessageType


# =============================================================================
# MCP CLIENT - Connect to MCP servers
# =============================================================================

@dataclass
class MCPServer:
    """Configuration for an MCP server."""
    name: str
    url: str
    description: str = ""
    tools: List[Dict] = field(default_factory=list)
    connected: bool = False


class MCPClient:
    """Client for connecting to MCP servers via HTTP/SSE."""

    def __init__(self):
        self.servers: Dict[str, MCPServer] = {}
        self.pending_tasks: Dict[str, Dict] = {}  # task_id -> task info

    def register_server(self, name: str, url: str, description: str = ""):
        """Register an MCP server."""
        # Ensure URL ends with /mcp/sse for proper MCP protocol
        base_url = url.rstrip('/')
        if not base_url.endswith('/mcp/sse'):
            base_url = f"{base_url}/mcp/sse"
        self.servers[name] = MCPServer(name=name, url=base_url, description=description)

    def _get_base_url(self, server: MCPServer) -> str:
        """Get base URL without /mcp/sse suffix for REST calls."""
        url = server.url
        if url.endswith('/mcp/sse'):
            return url[:-8]  # Remove /mcp/sse
        return url

    def connect(self, name: str) -> Dict:
        """Connect to an MCP server and discover its tools."""
        if name not in self.servers:
            return {"success": False, "error": f"Unknown server: {name}"}

        server = self.servers[name]
        base_url = self._get_base_url(server)

        try:
            # Try MCP tools/list endpoint first
            tools_url = f"{base_url}/mcp/tools/list"
            req = urllib.request.Request(tools_url, method="POST")
            req.add_header("Accept", "application/json")
            req.add_header("Content-Type", "application/json")
            req.data = b"{}"

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
                server.tools = data.get("tools", data.get("result", {}).get("tools", []))
                server.connected = True
                return {
                    "success": True,
                    "server": name,
                    "url": server.url,
                    "tools": [t.get("name") for t in server.tools],
                    "tool_count": len(server.tools)
                }
        except Exception as e1:
            try:
                # Fallback: try /tools endpoint
                tools_url = f"{base_url}/tools"
                req = urllib.request.Request(tools_url, method="GET")
                req.add_header("Accept", "application/json")

                with urllib.request.urlopen(req, timeout=10) as response:
                    data = json.loads(response.read().decode())
                    server.tools = data.get("tools", [])
                    server.connected = True
                    return {
                        "success": True,
                        "server": name,
                        "url": server.url,
                        "tools": [t.get("name") for t in server.tools],
                        "tool_count": len(server.tools)
                    }
            except Exception as e2:
                # Assume connected, will fail on call if not
                server.connected = True
                return {
                    "success": True,
                    "server": name,
                    "url": server.url,
                    "note": f"Connected but couldn't list tools: {e1}",
                    "tools": []
                }

    def call_tool(self, server_name: str, tool_name: str, params: Dict,
                  timeout: Optional[float] = None, async_mode: bool = False) -> Dict:
        """Call a tool on an MCP server."""
        if server_name not in self.servers:
            return {"success": False, "error": f"Unknown server: {server_name}"}

        server = self.servers[server_name]
        base_url = self._get_base_url(server)

        try:
            # Use MCP tools/call endpoint
            call_url = f"{base_url}/mcp/tools/call"
            body = json.dumps({
                "name": tool_name,
                "arguments": params
            }).encode()
            req = urllib.request.Request(call_url, data=body, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("Accept", "application/json")

            if async_mode:
                req.add_header("X-Async", "true")

            effective_timeout = timeout or 60

            with urllib.request.urlopen(req, timeout=effective_timeout) as response:
                result = json.loads(response.read().decode())

                # Check if this is an async task
                if async_mode and "task_id" in result:
                    task_id = result["task_id"]
                    self.pending_tasks[task_id] = {
                        "server": server_name,
                        "tool": tool_name,
                        "params": params,
                        "started": time.time()
                    }
                    return {
                        "success": True,
                        "async": True,
                        "task_id": task_id,
                        "message": "Task submitted, use wait_task to get result"
                    }

                # Handle MCP response format
                if "result" in result:
                    return {"success": True, "result": result["result"]}
                elif "content" in result:
                    return {"success": True, "result": result["content"]}
                return {"success": True, "result": result}

        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            return {"success": False, "error": f"HTTP {e.code}: {e.reason}", "details": error_body}
        except urllib.error.URLError as e:
            return {"success": False, "error": f"Connection error: {e.reason}"}
        except socket.timeout:
            return {"success": False, "error": f"Timeout after {timeout}s"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def wait_task(self, task_id: str, timeout: float = 300, poll_interval: float = 2) -> Dict:
        """Wait for an async task to complete."""
        if task_id not in self.pending_tasks:
            return {"success": False, "error": f"Unknown task: {task_id}"}

        task_info = self.pending_tasks[task_id]
        server = self.servers[task_info["server"]]
        base_url = self._get_base_url(server)
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                status_url = f"{base_url}/tasks/{task_id}"
                req = urllib.request.Request(status_url, method="GET")
                req.add_header("Accept", "application/json")

                with urllib.request.urlopen(req, timeout=10) as response:
                    result = json.loads(response.read().decode())

                    status = result.get("status", "unknown")
                    if status in ("completed", "done", "success"):
                        del self.pending_tasks[task_id]
                        return {"success": True, "result": result.get("result", result)}
                    elif status in ("failed", "error"):
                        del self.pending_tasks[task_id]
                        return {"success": False, "error": result.get("error", "Task failed")}
                    # Still running, continue polling

            except Exception as e:
                pass  # Continue polling on transient errors

            time.sleep(poll_interval)

        return {"success": False, "error": f"Task timed out after {timeout}s", "task_id": task_id}

    def list_servers(self) -> Dict:
        """List all registered MCP servers."""
        return {
            "servers": {
                name: {
                    "url": s.url,
                    "description": s.description,
                    "connected": s.connected,
                    "tool_count": len(s.tools)
                }
                for name, s in self.servers.items()
            }
        }


# =============================================================================
# ASYNC TASK MANAGER - Handle long-running operations
# =============================================================================

@dataclass
class AsyncTask:
    """A long-running async task."""
    task_id: str
    name: str
    status: str  # pending, running, completed, failed, timeout
    result: Any = None
    error: Optional[str] = None
    started: float = 0
    completed: float = 0
    timeout: float = 300


class AsyncTaskManager:
    """Manages long-running async tasks with timeouts."""

    def __init__(self, max_workers: int = 5):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.tasks: Dict[str, AsyncTask] = {}
        self.results_queue = queue.Queue()

    def submit(self, name: str, func: Callable, args: tuple = (),
               kwargs: dict = None, timeout: float = 300) -> str:
        """Submit a task for async execution."""
        task_id = str(uuid.uuid4())[:8]
        task = AsyncTask(
            task_id=task_id,
            name=name,
            status="pending",
            started=time.time(),
            timeout=timeout
        )
        self.tasks[task_id] = task

        def run_with_timeout():
            task.status = "running"
            try:
                result = func(*args, **(kwargs or {}))
                task.result = result
                task.status = "completed"
                task.completed = time.time()
            except Exception as e:
                task.error = str(e)
                task.status = "failed"
                task.completed = time.time()
            self.results_queue.put(task_id)

        self.executor.submit(run_with_timeout)
        return task_id

    def wait(self, task_id: str, timeout: Optional[float] = None) -> Dict:
        """Wait for a task to complete."""
        if task_id not in self.tasks:
            return {"success": False, "error": f"Unknown task: {task_id}"}

        task = self.tasks[task_id]
        effective_timeout = timeout or task.timeout
        start_wait = time.time()

        while task.status in ("pending", "running"):
            if time.time() - start_wait > effective_timeout:
                task.status = "timeout"
                return {"success": False, "error": "Task timed out", "task_id": task_id}
            time.sleep(0.1)

        if task.status == "completed":
            return {"success": True, "result": task.result, "duration": task.completed - task.started}
        else:
            return {"success": False, "error": task.error, "status": task.status}

    def get_status(self, task_id: str) -> Dict:
        """Get task status without waiting."""
        if task_id not in self.tasks:
            return {"success": False, "error": f"Unknown task: {task_id}"}

        task = self.tasks[task_id]
        return {
            "task_id": task_id,
            "name": task.name,
            "status": task.status,
            "elapsed": time.time() - task.started if task.status == "running" else None,
            "result": task.result if task.status == "completed" else None,
            "error": task.error if task.status == "failed" else None
        }

    def list_tasks(self) -> Dict:
        """List all tasks."""
        return {
            "tasks": {
                tid: {
                    "name": t.name,
                    "status": t.status,
                    "elapsed": time.time() - t.started
                }
                for tid, t in self.tasks.items()
            }
        }


# =============================================================================
# ASYNC TASK SCHEDULER - Cron-like scheduling with persistence
# =============================================================================

@dataclass
class ScheduledTask:
    """A scheduled task with advanced configuration."""
    task_id: str
    name: str
    task_type: str  # 'once', 'interval', 'cron', 'event', 'dependency', 'workflow'
    handler: str  # Tool name or agent task to execute
    params: Dict = field(default_factory=dict)
    schedule: Optional[str] = None  # Cron expression or interval spec
    next_run: Optional[float] = None
    last_run: Optional[float] = None
    run_count: int = 0
    max_runs: Optional[int] = None  # None = unlimited
    status: str = "pending"  # pending, running, completed, failed, paused, waiting, dead
    priority: int = 5  # 1-10, lower = higher priority
    created: float = field(default_factory=time.time)
    metadata: Dict = field(default_factory=dict)
    error: Optional[str] = None
    # Advanced features
    depends_on: List[str] = field(default_factory=list)  # Task IDs this depends on
    retry_count: int = 0
    max_retries: int = 3
    retry_delay: float = 1.0  # Base delay for exponential backoff
    timeout: float = 300.0  # Task timeout in seconds
    webhook_url: Optional[str] = None  # URL to call on completion
    condition: Optional[str] = None  # Python expression to evaluate
    output: Optional[Dict] = None  # Task output for chaining
    workflow_id: Optional[str] = None  # Parent workflow ID
    tags: List[str] = field(default_factory=list)  # Tags for filtering
    concurrency_key: Optional[str] = None  # Key for concurrency control
    rate_limit: Optional[str] = None  # Rate limit spec (e.g., "10/m")


@dataclass
class Workflow:
    """A workflow is a DAG of tasks."""
    workflow_id: str
    name: str
    tasks: List[str]  # Ordered task IDs
    status: str = "pending"  # pending, running, completed, failed, paused
    created: float = field(default_factory=time.time)
    started: Optional[float] = None
    completed: Optional[float] = None
    metadata: Dict = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class TaskTemplate:
    """Reusable task configuration template."""
    template_id: str
    name: str
    handler: str
    params: Dict = field(default_factory=dict)
    task_type: str = "once"
    schedule: Optional[str] = None
    priority: int = 5
    max_retries: int = 3
    timeout: float = 300.0
    tags: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)


@dataclass
class Metric:
    """A collected metric data point."""
    metric_id: str
    name: str
    value: float
    timestamp: float
    tags: Dict = field(default_factory=dict)
    source: str = ""  # Task ID or agent that produced this


@dataclass
class Alert:
    """An alert triggered by monitoring conditions."""
    alert_id: str
    name: str
    severity: str  # 'info', 'warning', 'critical'
    message: str
    timestamp: float
    source: str = ""
    acknowledged: bool = False
    resolved: bool = False
    metadata: Dict = field(default_factory=dict)


@dataclass
class AgentMemory:
    """Persistent memory for an agent across runs."""
    agent_id: str
    key: str
    value: Any
    updated: float
    expires: Optional[float] = None


class TaskScheduler:
    """
    Async task scheduler with SQLite persistence.

    Features:
    - One-time scheduled tasks
    - Interval-based recurring tasks
    - Cron-like scheduling
    - Event-driven triggers
    - Priority queue
    - Persistent storage
    - Async execution
    """

    SCHEDULER_DIR = Path(".agent_scheduler")
    DB_FILE = "scheduler.db"

    def __init__(self):
        self.db_path = self.SCHEDULER_DIR / self.DB_FILE
        self.tasks: Dict[str, ScheduledTask] = {}
        self.workflows: Dict[str, Workflow] = {}
        self.templates: Dict[str, TaskTemplate] = {}
        self.event_handlers: Dict[str, List[str]] = {}  # event_name -> [task_ids]
        self.running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._lock = threading.Lock()
        self._concurrency_locks: Dict[str, str] = {}  # concurrency_key -> task_id
        self._rate_limiters: Dict[str, Dict] = {}  # key -> {count, window_start}
        # Advanced monitoring
        self.alerts: Dict[str, Alert] = {}  # Active alerts
        self.alert_rules: Dict[str, Dict] = {}  # Alert rule definitions
        self.metric_aggregators: Dict[str, List[float]] = {}  # Rolling metric windows
        self._init_db()
        self._load_tasks()
        self._load_workflows()
        self._load_templates()
        self._load_alerts()

    def _init_db(self):
        """Initialize SQLite database for task persistence."""
        import sqlite3
        self.SCHEDULER_DIR.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                task_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                handler TEXT NOT NULL,
                params TEXT,
                schedule TEXT,
                next_run REAL,
                last_run REAL,
                run_count INTEGER DEFAULT 0,
                max_runs INTEGER,
                status TEXT DEFAULT 'pending',
                priority INTEGER DEFAULT 5,
                created REAL,
                metadata TEXT,
                error TEXT,
                depends_on TEXT,
                retry_count INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 3,
                retry_delay REAL DEFAULT 1.0,
                timeout REAL DEFAULT 300.0,
                webhook_url TEXT,
                condition TEXT,
                output TEXT,
                workflow_id TEXT,
                tags TEXT,
                concurrency_key TEXT,
                rate_limit TEXT
            )
        """)

        # Add missing columns for existing databases
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN depends_on TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN retry_count INTEGER DEFAULT 0")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN max_retries INTEGER DEFAULT 3")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN retry_delay REAL DEFAULT 1.0")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN timeout REAL DEFAULT 300.0")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN webhook_url TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN condition TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN output TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN workflow_id TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN tags TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN concurrency_key TEXT")
        except: pass
        try:
            cursor.execute("ALTER TABLE scheduled_tasks ADD COLUMN rate_limit TEXT")
        except: pass

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                run_time REAL,
                duration REAL,
                status TEXT,
                result TEXT,
                error TEXT,
                FOREIGN KEY (task_id) REFERENCES scheduled_tasks(task_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_subscriptions (
                event_name TEXT,
                task_id TEXT,
                PRIMARY KEY (event_name, task_id),
                FOREIGN KEY (task_id) REFERENCES scheduled_tasks(task_id)
            )
        """)

        # Workflows table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                tasks TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created REAL,
                started REAL,
                completed REAL,
                metadata TEXT,
                error TEXT
            )
        """)

        # Task templates table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_templates (
                template_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                handler TEXT NOT NULL,
                params TEXT,
                task_type TEXT DEFAULT 'once',
                schedule TEXT,
                priority INTEGER DEFAULT 5,
                max_retries INTEGER DEFAULT 3,
                timeout REAL DEFAULT 300.0,
                tags TEXT,
                metadata TEXT
            )
        """)

        # Dead letter queue for failed tasks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                task_data TEXT,
                error TEXT,
                failed_at REAL,
                retry_count INTEGER,
                FOREIGN KEY (task_id) REFERENCES scheduled_tasks(task_id)
            )
        """)

        # Concurrency locks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS concurrency_locks (
                lock_key TEXT PRIMARY KEY,
                task_id TEXT,
                acquired_at REAL,
                expires_at REAL
            )
        """)

        # Rate limit tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rate_limits (
                key TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0,
                window_start REAL,
                window_size REAL
            )
        """)

        # Metrics table for time-series data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_id TEXT UNIQUE,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp REAL NOT NULL,
                tags TEXT,
                source TEXT
            )
        """)

        # Alerts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT,
                timestamp REAL NOT NULL,
                source TEXT,
                acknowledged INTEGER DEFAULT 0,
                resolved INTEGER DEFAULT 0,
                resolved_at REAL,
                metadata TEXT
            )
        """)

        # Alert rules for automatic alert triggering
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_rules (
                rule_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                condition TEXT NOT NULL,
                threshold REAL,
                severity TEXT DEFAULT 'warning',
                cooldown REAL DEFAULT 300,
                last_triggered REAL,
                enabled INTEGER DEFAULT 1,
                metadata TEXT
            )
        """)

        # Agent memory - persistent key-value store per agent
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS agent_memory (
                agent_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                updated REAL,
                expires REAL,
                PRIMARY KEY (agent_id, key)
            )
        """)

        # Metric aggregations for trend analysis
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metric_aggregations (
                name TEXT NOT NULL,
                period TEXT NOT NULL,
                timestamp REAL NOT NULL,
                min_val REAL,
                max_val REAL,
                avg_val REAL,
                sum_val REAL,
                count INTEGER,
                PRIMARY KEY (name, period, timestamp)
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_next_run ON scheduled_tasks(next_run)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(resolved)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_status ON scheduled_tasks(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_priority ON scheduled_tasks(priority)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflow ON scheduled_tasks(workflow_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tags ON scheduled_tasks(tags)")

        conn.commit()
        conn.close()

    def _load_tasks(self):
        """Load tasks from database with all advanced fields."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row  # Access columns by name
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM scheduled_tasks WHERE status NOT IN ('completed', 'cancelled', 'dead')")
        rows = cursor.fetchall()

        for row in rows:
            try:
                task = ScheduledTask(
                    task_id=row['task_id'],
                    name=row['name'],
                    task_type=row['task_type'],
                    handler=row['handler'],
                    params=json.loads(row['params']) if row['params'] else {},
                    schedule=row['schedule'],
                    next_run=row['next_run'],
                    last_run=row['last_run'],
                    run_count=row['run_count'] or 0,
                    max_runs=row['max_runs'],
                    status=row['status'],
                    priority=row['priority'] or 5,
                    created=row['created'],
                    metadata=json.loads(row['metadata']) if row['metadata'] else {},
                    error=row['error'],
                    depends_on=json.loads(row['depends_on']) if row['depends_on'] else [],
                    retry_count=row['retry_count'] or 0,
                    max_retries=row['max_retries'] or 3,
                    retry_delay=row['retry_delay'] or 1.0,
                    timeout=row['timeout'] or 300.0,
                    webhook_url=row['webhook_url'],
                    condition=row['condition'],
                    output=json.loads(row['output']) if row['output'] else None,
                    workflow_id=row['workflow_id'],
                    tags=json.loads(row['tags']) if row['tags'] else [],
                    concurrency_key=row['concurrency_key'],
                    rate_limit=row['rate_limit']
                )
                self.tasks[task.task_id] = task
            except Exception as e:
                # Handle old schema rows gracefully
                try:
                    task = ScheduledTask(
                        task_id=row[0], name=row[1], task_type=row[2], handler=row[3],
                        params=json.loads(row[4]) if row[4] else {},
                        schedule=row[5], next_run=row[6], last_run=row[7],
                        run_count=row[8] or 0, max_runs=row[9], status=row[10],
                        priority=row[11] or 5, created=row[12],
                        metadata=json.loads(row[13]) if row[13] else {},
                        error=row[14] if len(row) > 14 else None
                    )
                    self.tasks[task.task_id] = task
                except:
                    pass  # Skip malformed rows

        # Load event subscriptions
        cursor.execute("SELECT event_name, task_id FROM event_subscriptions")
        for event_name, task_id in cursor.fetchall():
            if event_name not in self.event_handlers:
                self.event_handlers[event_name] = []
            self.event_handlers[event_name].append(task_id)

        conn.close()

    def _save_task(self, task: ScheduledTask):
        """Save task to database with all advanced fields."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR REPLACE INTO scheduled_tasks
            (task_id, name, task_type, handler, params, schedule, next_run, last_run,
             run_count, max_runs, status, priority, created, metadata, error,
             depends_on, retry_count, max_retries, retry_delay, timeout,
             webhook_url, condition, output, workflow_id, tags, concurrency_key, rate_limit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task.task_id, task.name, task.task_type, task.handler,
            json.dumps(task.params), task.schedule, task.next_run, task.last_run,
            task.run_count, task.max_runs, task.status, task.priority,
            task.created, json.dumps(task.metadata), task.error,
            json.dumps(task.depends_on), task.retry_count, task.max_retries,
            task.retry_delay, task.timeout, task.webhook_url, task.condition,
            json.dumps(task.output) if task.output else None, task.workflow_id,
            json.dumps(task.tags), task.concurrency_key, task.rate_limit
        ))

        conn.commit()
        conn.close()

    def _record_history(self, task_id: str, duration: float, status: str,
                        result: Any = None, error: str = None):
        """Record task execution history."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO task_history (task_id, run_time, duration, status, result, error)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (task_id, time.time(), duration, status,
              json.dumps(result) if result else None, error))

        conn.commit()
        conn.close()

    def _parse_interval(self, schedule: str) -> float:
        """Parse interval specification like '5m', '1h', '30s'."""
        if not schedule:
            return 60.0

        unit = schedule[-1].lower()
        try:
            value = float(schedule[:-1])
        except ValueError:
            return 60.0

        multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        return value * multipliers.get(unit, 60)

    def _parse_cron(self, cron_expr: str) -> Optional[float]:
        """Parse cron expression and return next run time.

        Simple cron format: minute hour day month weekday
        Supports: *, specific values, ranges (1-5), steps (*/5)
        """
        try:
            parts = cron_expr.split()
            if len(parts) != 5:
                return None

            from datetime import datetime, timedelta
            now = datetime.now()

            # Simple implementation: find next matching time
            def matches(value: int, expr: str, max_val: int) -> bool:
                if expr == '*':
                    return True
                if expr.startswith('*/'):
                    step = int(expr[2:])
                    return value % step == 0
                if '-' in expr:
                    start, end = map(int, expr.split('-'))
                    return start <= value <= end
                return value == int(expr)

            # Check next 1440 minutes (24 hours)
            for offset in range(1, 1441):
                check_time = now + timedelta(minutes=offset)
                minute, hour = check_time.minute, check_time.hour
                day, month, weekday = check_time.day, check_time.month, check_time.weekday()

                if (matches(minute, parts[0], 59) and
                    matches(hour, parts[1], 23) and
                    matches(day, parts[2], 31) and
                    matches(month, parts[3], 12) and
                    matches(weekday, parts[4], 6)):
                    return check_time.timestamp()

            return None
        except Exception:
            return None

    def _calculate_next_run(self, task: ScheduledTask) -> Optional[float]:
        """Calculate next run time for a task."""
        if task.task_type == 'once':
            return task.next_run if task.run_count == 0 else None

        if task.task_type == 'interval':
            interval = self._parse_interval(task.schedule)
            base = task.last_run or time.time()
            return base + interval

        if task.task_type == 'cron':
            return self._parse_cron(task.schedule)

        if task.task_type == 'event':
            return None  # Event-driven, no scheduled time

        return None

    def cancel_task(self, task_id: str) -> Dict:
        """Cancel a scheduled task."""
        with self._lock:
            if task_id not in self.tasks:
                return {"success": False, "error": f"Task not found: {task_id}"}

            task = self.tasks[task_id]
            task.status = "cancelled"
            self._save_task(task)
            del self.tasks[task_id]

        return {"success": True, "message": f"Task {task_id} cancelled"}

    def pause_task(self, task_id: str) -> Dict:
        """Pause a scheduled task."""
        with self._lock:
            if task_id not in self.tasks:
                return {"success": False, "error": f"Task not found: {task_id}"}

            task = self.tasks[task_id]
            task.status = "paused"
            self._save_task(task)

        return {"success": True, "message": f"Task {task_id} paused"}

    def resume_task(self, task_id: str) -> Dict:
        """Resume a paused task."""
        with self._lock:
            if task_id not in self.tasks:
                return {"success": False, "error": f"Task not found: {task_id}"}

            task = self.tasks[task_id]
            if task.status != "paused":
                return {"success": False, "error": "Task is not paused"}

            task.status = "pending"
            task.next_run = self._calculate_next_run(task)
            self._save_task(task)

        return {"success": True, "message": f"Task {task_id} resumed"}

    def emit_event(self, event_name: str, event_data: Dict = None) -> Dict:
        """
        Emit an event to trigger subscribed tasks.

        Args:
            event_name: Name of the event
            event_data: Data to pass to triggered tasks

        Returns:
            Dict with triggered task IDs
        """
        triggered = []

        with self._lock:
            if event_name in self.event_handlers:
                for task_id in self.event_handlers[event_name]:
                    if task_id in self.tasks:
                        task = self.tasks[task_id]
                        if task.status == "pending":
                            # Mark for immediate execution
                            task.next_run = time.time()
                            task.metadata["event_data"] = event_data or {}
                            self._save_task(task)
                            triggered.append(task_id)

        return {
            "success": True,
            "event": event_name,
            "triggered_tasks": triggered,
            "count": len(triggered)
        }

    def get_task(self, task_id: str) -> Dict:
        """Get task details."""
        if task_id not in self.tasks:
            return {"success": False, "error": f"Task not found: {task_id}"}

        task = self.tasks[task_id]
        return {
            "success": True,
            "task": {
                "task_id": task.task_id,
                "name": task.name,
                "task_type": task.task_type,
                "handler": task.handler,
                "params": task.params,
                "schedule": task.schedule,
                "next_run": task.next_run,
                "last_run": task.last_run,
                "run_count": task.run_count,
                "max_runs": task.max_runs,
                "status": task.status,
                "priority": task.priority,
                "metadata": task.metadata,
                "error": task.error
            }
        }

    def list_tasks(self, status: str = None, task_type: str = None) -> Dict:
        """List all scheduled tasks."""
        tasks = []
        for task in self.tasks.values():
            if status and task.status != status:
                continue
            if task_type and task.task_type != task_type:
                continue
            tasks.append({
                "task_id": task.task_id,
                "name": task.name,
                "task_type": task.task_type,
                "handler": task.handler,
                "status": task.status,
                "next_run": task.next_run,
                "run_count": task.run_count,
                "priority": task.priority
            })

        # Sort by priority then next_run
        tasks.sort(key=lambda t: (t["priority"], t["next_run"] or float('inf')))

        return {"success": True, "tasks": tasks, "count": len(tasks)}

    def mark_running(self, task_id: str):
        """Mark task as running."""
        with self._lock:
            if task_id in self.tasks:
                self.tasks[task_id].status = "running"
                self._save_task(self.tasks[task_id])

    def get_history(self, task_id: str = None, limit: int = 50) -> Dict:
        """Get task execution history."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        if task_id:
            cursor.execute("""
                SELECT task_id, run_time, duration, status, result, error
                FROM task_history WHERE task_id = ?
                ORDER BY run_time DESC LIMIT ?
            """, (task_id, limit))
        else:
            cursor.execute("""
                SELECT task_id, run_time, duration, status, result, error
                FROM task_history ORDER BY run_time DESC LIMIT ?
            """, (limit,))

        history = []
        for row in cursor.fetchall():
            history.append({
                "task_id": row[0],
                "run_time": row[1],
                "duration": row[2],
                "status": row[3],
                "result": json.loads(row[4]) if row[4] else None,
                "error": row[5]
            })

        conn.close()
        return {"success": True, "history": history}

    def stats(self) -> Dict:
        """Get scheduler statistics."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM scheduled_tasks")
        total_tasks = cursor.fetchone()[0]

        cursor.execute("SELECT status, COUNT(*) FROM scheduled_tasks GROUP BY status")
        by_status = dict(cursor.fetchall())

        cursor.execute("SELECT COUNT(*) FROM task_history")
        total_runs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM task_history WHERE status = 'completed'")
        successful_runs = cursor.fetchone()[0]

        conn.close()

        return {
            "success": True,
            "stats": {
                "total_tasks": total_tasks,
                "active_tasks": len(self.tasks),
                "by_status": by_status,
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "success_rate": successful_runs / total_runs if total_runs > 0 else 0,
                "registered_events": list(self.event_handlers.keys()),
                "workflows": len(self.workflows),
                "templates": len(self.templates),
                "db_path": str(self.db_path)
            }
        }

    # =========================================================================
    # WORKFLOW MANAGEMENT
    # =========================================================================

    def _load_workflows(self):
        """Load workflows from database."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM workflows WHERE status NOT IN ('completed', 'cancelled')")
            for row in cursor.fetchall():
                wf = Workflow(
                    workflow_id=row[0],
                    name=row[1],
                    tasks=json.loads(row[2]) if row[2] else [],
                    status=row[3],
                    created=row[4],
                    started=row[5],
                    completed=row[6],
                    metadata=json.loads(row[7]) if row[7] else {},
                    error=row[8]
                )
                self.workflows[wf.workflow_id] = wf
        except:
            pass
        conn.close()

    def _load_templates(self):
        """Load task templates from database."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM task_templates")
            for row in cursor.fetchall():
                tpl = TaskTemplate(
                    template_id=row[0],
                    name=row[1],
                    handler=row[2],
                    params=json.loads(row[3]) if row[3] else {},
                    task_type=row[4],
                    schedule=row[5],
                    priority=row[6],
                    max_retries=row[7],
                    timeout=row[8],
                    tags=json.loads(row[9]) if row[9] else [],
                    metadata=json.loads(row[10]) if row[10] else {}
                )
                self.templates[tpl.template_id] = tpl
        except:
            pass
        conn.close()

    def _save_workflow(self, wf: Workflow):
        """Save workflow to database."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO workflows
            (workflow_id, name, tasks, status, created, started, completed, metadata, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (wf.workflow_id, wf.name, json.dumps(wf.tasks), wf.status,
              wf.created, wf.started, wf.completed, json.dumps(wf.metadata), wf.error))
        conn.commit()
        conn.close()

    def create_workflow(self, name: str, steps: List[Dict], metadata: Dict = None) -> Dict:
        """
        Create a workflow (DAG of tasks).

        Args:
            name: Workflow name
            steps: List of task definitions with optional 'depends_on' fields
                   Each step: {name, handler, params, depends_on: [step_names]}
            metadata: Optional workflow metadata

        Returns:
            Dict with workflow_id and created task_ids
        """
        workflow_id = str(uuid.uuid4())[:12]
        task_ids = []
        step_name_to_id = {}

        # First pass: create all tasks
        for step in steps:
            step_name = step.get("name", f"step_{len(task_ids)}")
            task_result = self.schedule_task(
                name=f"{name}:{step_name}",
                handler=step.get("handler", ""),
                task_type="dependency",
                params=step.get("params", {}),
                priority=step.get("priority", 5),
                max_retries=step.get("max_retries", 3),
                timeout=step.get("timeout", 300),
                metadata={**step.get("metadata", {}), "workflow_step": step_name},
                workflow_id=workflow_id,
                tags=step.get("tags", [])
            )
            if task_result.get("success"):
                task_id = task_result["task_id"]
                task_ids.append(task_id)
                step_name_to_id[step_name] = task_id

        # Second pass: set up dependencies
        for i, step in enumerate(steps):
            step_name = step.get("name", f"step_{i}")
            task_id = step_name_to_id.get(step_name)
            if task_id and task_id in self.tasks:
                deps = step.get("depends_on", [])
                dep_ids = [step_name_to_id[d] for d in deps if d in step_name_to_id]
                self.tasks[task_id].depends_on = dep_ids
                if dep_ids:
                    self.tasks[task_id].status = "waiting"
                else:
                    # No dependencies - ready to run immediately
                    self.tasks[task_id].status = "pending"
                    self.tasks[task_id].next_run = time.time()
                self._save_task(self.tasks[task_id])

        # Create workflow record
        wf = Workflow(
            workflow_id=workflow_id,
            name=name,
            tasks=task_ids,
            metadata=metadata or {}
        )
        self.workflows[workflow_id] = wf
        self._save_workflow(wf)

        return {
            "success": True,
            "workflow_id": workflow_id,
            "name": name,
            "task_ids": task_ids,
            "step_map": step_name_to_id
        }

    def get_workflow(self, workflow_id: str) -> Dict:
        """Get workflow details and status."""
        if workflow_id not in self.workflows:
            return {"success": False, "error": f"Workflow not found: {workflow_id}"}

        wf = self.workflows[workflow_id]
        task_statuses = {}
        for task_id in wf.tasks:
            if task_id in self.tasks:
                t = self.tasks[task_id]
                task_statuses[task_id] = {
                    "name": t.name,
                    "status": t.status,
                    "depends_on": t.depends_on,
                    "output": t.output
                }

        return {
            "success": True,
            "workflow": {
                "workflow_id": wf.workflow_id,
                "name": wf.name,
                "status": wf.status,
                "tasks": task_statuses,
                "created": wf.created,
                "started": wf.started,
                "completed": wf.completed,
                "error": wf.error
            }
        }

    def list_workflows(self, status: str = None) -> Dict:
        """List all workflows."""
        workflows = []
        for wf in self.workflows.values():
            if status and wf.status != status:
                continue
            workflows.append({
                "workflow_id": wf.workflow_id,
                "name": wf.name,
                "status": wf.status,
                "task_count": len(wf.tasks),
                "created": wf.created
            })
        return {"success": True, "workflows": workflows, "count": len(workflows)}

    def cancel_workflow(self, workflow_id: str) -> Dict:
        """Cancel a workflow and all its tasks."""
        if workflow_id not in self.workflows:
            return {"success": False, "error": f"Workflow not found: {workflow_id}"}

        wf = self.workflows[workflow_id]
        cancelled_tasks = []
        for task_id in wf.tasks:
            if task_id in self.tasks:
                self.cancel_task(task_id)
                cancelled_tasks.append(task_id)

        wf.status = "cancelled"
        self._save_workflow(wf)

        return {"success": True, "cancelled_tasks": cancelled_tasks}

    # =========================================================================
    # TASK TEMPLATES
    # =========================================================================

    def _save_template(self, tpl: TaskTemplate):
        """Save template to database."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO task_templates
            (template_id, name, handler, params, task_type, schedule, priority,
             max_retries, timeout, tags, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tpl.template_id, tpl.name, tpl.handler, json.dumps(tpl.params),
              tpl.task_type, tpl.schedule, tpl.priority, tpl.max_retries,
              tpl.timeout, json.dumps(tpl.tags), json.dumps(tpl.metadata)))
        conn.commit()
        conn.close()

    def create_template(self, name: str, handler: str, params: Dict = None,
                       task_type: str = "once", schedule: str = None,
                       priority: int = 5, max_retries: int = 3,
                       timeout: float = 300, tags: List[str] = None,
                       metadata: Dict = None) -> Dict:
        """Create a reusable task template."""
        template_id = str(uuid.uuid4())[:12]
        tpl = TaskTemplate(
            template_id=template_id,
            name=name,
            handler=handler,
            params=params or {},
            task_type=task_type,
            schedule=schedule,
            priority=priority,
            max_retries=max_retries,
            timeout=timeout,
            tags=tags or [],
            metadata=metadata or {}
        )
        self.templates[template_id] = tpl
        self._save_template(tpl)

        return {
            "success": True,
            "template_id": template_id,
            "name": name
        }

    def schedule_from_template(self, template_id: str, name: str = None,
                               params_override: Dict = None,
                               delay: float = 0) -> Dict:
        """Schedule a task from a template."""
        if template_id not in self.templates:
            return {"success": False, "error": f"Template not found: {template_id}"}

        tpl = self.templates[template_id]
        merged_params = {**tpl.params, **(params_override or {})}

        return self.schedule_task(
            name=name or f"{tpl.name}_{int(time.time())}",
            handler=tpl.handler,
            task_type=tpl.task_type,
            schedule=tpl.schedule,
            params=merged_params,
            delay=delay,
            priority=tpl.priority,
            max_retries=tpl.max_retries,
            timeout=tpl.timeout,
            tags=tpl.tags,
            metadata={**tpl.metadata, "from_template": template_id}
        )

    def list_templates(self) -> Dict:
        """List all task templates."""
        templates = []
        for tpl in self.templates.values():
            templates.append({
                "template_id": tpl.template_id,
                "name": tpl.name,
                "handler": tpl.handler,
                "task_type": tpl.task_type,
                "tags": tpl.tags
            })
        return {"success": True, "templates": templates, "count": len(templates)}

    def delete_template(self, template_id: str) -> Dict:
        """Delete a template."""
        if template_id not in self.templates:
            return {"success": False, "error": f"Template not found: {template_id}"}

        del self.templates[template_id]
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM task_templates WHERE template_id = ?", (template_id,))
        conn.commit()
        conn.close()

        return {"success": True, "deleted": template_id}

    # =========================================================================
    # RETRY & ERROR HANDLING
    # =========================================================================

    def retry_task(self, task_id: str) -> Dict:
        """Manually retry a failed task."""
        if task_id not in self.tasks:
            # Check if in dead letter queue
            return self._resurrect_from_dlq(task_id)

        task = self.tasks[task_id]
        if task.status not in ("failed", "dead"):
            return {"success": False, "error": "Task is not in failed state"}

        task.status = "pending"
        task.retry_count = 0
        task.error = None
        task.next_run = time.time()
        self._save_task(task)

        return {"success": True, "message": f"Task {task_id} queued for retry"}

    def _handle_task_failure(self, task: ScheduledTask, error: str) -> bool:
        """
        Handle task failure with exponential backoff retry.
        Returns True if task will be retried, False if moved to DLQ.
        """
        task.retry_count += 1
        task.error = error

        if task.retry_count <= task.max_retries:
            # Exponential backoff: delay * 2^(retry_count - 1)
            backoff = task.retry_delay * (2 ** (task.retry_count - 1))
            # Add jitter (0-25% of backoff)
            import random
            jitter = backoff * random.random() * 0.25
            task.next_run = time.time() + backoff + jitter
            task.status = "pending"
            self._save_task(task)
            return True
        else:
            # Move to dead letter queue
            self._move_to_dlq(task)
            return False

    def _move_to_dlq(self, task: ScheduledTask):
        """Move failed task to dead letter queue."""
        import sqlite3
        task.status = "dead"
        self._save_task(task)

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dead_letter_queue (task_id, task_data, error, failed_at, retry_count)
            VALUES (?, ?, ?, ?, ?)
        """, (task.task_id, json.dumps({
            "name": task.name,
            "handler": task.handler,
            "params": task.params,
            "metadata": task.metadata
        }), task.error, time.time(), task.retry_count))
        conn.commit()
        conn.close()

        # Remove from active tasks
        if task.task_id in self.tasks:
            del self.tasks[task.task_id]

    def _resurrect_from_dlq(self, task_id: str) -> Dict:
        """Resurrect a task from dead letter queue."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT task_data FROM dead_letter_queue WHERE task_id = ?", (task_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return {"success": False, "error": "Task not found in dead letter queue"}

        task_data = json.loads(row[0])
        cursor.execute("DELETE FROM dead_letter_queue WHERE task_id = ?", (task_id,))
        conn.commit()
        conn.close()

        # Re-schedule the task
        return self.schedule_task(
            name=task_data["name"],
            handler=task_data["handler"],
            params=task_data["params"],
            metadata={**task_data.get("metadata", {}), "resurrected_from_dlq": True}
        )

    def get_dead_letter_queue(self, limit: int = 50) -> Dict:
        """Get tasks in dead letter queue."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            SELECT task_id, task_data, error, failed_at, retry_count
            FROM dead_letter_queue ORDER BY failed_at DESC LIMIT ?
        """, (limit,))
        items = []
        for row in cursor.fetchall():
            items.append({
                "task_id": row[0],
                "task_data": json.loads(row[1]) if row[1] else {},
                "error": row[2],
                "failed_at": row[3],
                "retry_count": row[4]
            })
        conn.close()
        return {"success": True, "items": items, "count": len(items)}

    def clear_dead_letter_queue(self) -> Dict:
        """Clear all tasks from dead letter queue."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dead_letter_queue")
        count = cursor.fetchone()[0]
        cursor.execute("DELETE FROM dead_letter_queue")
        conn.commit()
        conn.close()
        return {"success": True, "cleared": count}

    # =========================================================================
    # CONCURRENCY CONTROL
    # =========================================================================

    def _acquire_lock(self, key: str, task_id: str, timeout: float = 300) -> bool:
        """Acquire a concurrency lock."""
        with self._lock:
            if key in self._concurrency_locks:
                # Check if lock is expired
                import sqlite3
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                cursor.execute("SELECT expires_at FROM concurrency_locks WHERE lock_key = ?", (key,))
                row = cursor.fetchone()
                conn.close()
                if row and row[0] > time.time():
                    return False  # Lock held by another task
                # Lock expired, we can take it

            self._concurrency_locks[key] = task_id
            expires_at = time.time() + timeout

            import sqlite3
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO concurrency_locks (lock_key, task_id, acquired_at, expires_at)
                VALUES (?, ?, ?, ?)
            """, (key, task_id, time.time(), expires_at))
            conn.commit()
            conn.close()
            return True

    def _release_lock(self, key: str, task_id: str):
        """Release a concurrency lock."""
        with self._lock:
            if self._concurrency_locks.get(key) == task_id:
                del self._concurrency_locks[key]

            import sqlite3
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            cursor.execute("DELETE FROM concurrency_locks WHERE lock_key = ? AND task_id = ?",
                          (key, task_id))
            conn.commit()
            conn.close()

    def can_run_with_concurrency(self, task: ScheduledTask) -> bool:
        """Check if task can run given concurrency constraints."""
        if not task.concurrency_key:
            return True
        return self._acquire_lock(task.concurrency_key, task.task_id, task.timeout)

    # =========================================================================
    # RATE LIMITING
    # =========================================================================

    def _parse_rate_limit(self, spec: str) -> Tuple[int, float]:
        """Parse rate limit spec like '10/m', '100/h' -> (count, window_seconds)."""
        if not spec:
            return (0, 0)
        try:
            count, unit = spec.split('/')
            count = int(count)
            windows = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
            return (count, windows.get(unit, 60))
        except:
            return (0, 0)

    def _check_rate_limit(self, key: str, limit_spec: str) -> bool:
        """Check if action is within rate limit. Returns True if allowed."""
        max_count, window_size = self._parse_rate_limit(limit_spec)
        if max_count == 0:
            return True

        now = time.time()
        with self._lock:
            if key not in self._rate_limiters:
                self._rate_limiters[key] = {"count": 0, "window_start": now}

            rl = self._rate_limiters[key]
            if now - rl["window_start"] >= window_size:
                # Reset window
                rl["count"] = 0
                rl["window_start"] = now

            if rl["count"] < max_count:
                rl["count"] += 1
                return True
            return False

    def can_run_with_rate_limit(self, task: ScheduledTask) -> bool:
        """Check if task can run given rate limit constraints."""
        if not task.rate_limit:
            return True
        key = f"rate:{task.handler}"
        return self._check_rate_limit(key, task.rate_limit)

    # =========================================================================
    # CONDITIONAL EXECUTION
    # =========================================================================

    def evaluate_condition(self, task: ScheduledTask) -> bool:
        """Evaluate task condition. Returns True if task should run."""
        if not task.condition:
            return True

        try:
            # Build evaluation context
            context = {
                "task": task,
                "time": time,
                "datetime": __import__("datetime").datetime,
                "now": time.time(),
                "hour": __import__("datetime").datetime.now().hour,
                "weekday": __import__("datetime").datetime.now().weekday(),
                "metadata": task.metadata,
                "run_count": task.run_count,
                "last_run": task.last_run,
            }

            # Add outputs from dependent tasks
            for dep_id in task.depends_on:
                if dep_id in self.tasks and self.tasks[dep_id].output:
                    context[f"dep_{dep_id}"] = self.tasks[dep_id].output

            result = eval(task.condition, {"__builtins__": {}}, context)
            return bool(result)
        except Exception as e:
            # Log error but allow task to run
            task.metadata["condition_error"] = str(e)
            return True

    # =========================================================================
    # WEBHOOK NOTIFICATIONS
    # =========================================================================

    def _send_webhook(self, task: ScheduledTask, status: str, result: Any = None, error: str = None):
        """Send webhook notification for task completion."""
        if not task.webhook_url:
            return

        try:
            payload = {
                "task_id": task.task_id,
                "name": task.name,
                "status": status,
                "handler": task.handler,
                "run_count": task.run_count,
                "result": result,
                "error": error,
                "timestamp": time.time()
            }

            req = urllib.request.Request(
                task.webhook_url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            # Log but don't fail
            task.metadata["webhook_error"] = str(e)

    # =========================================================================
    # DEPENDENCY CHECKING
    # =========================================================================

    def check_dependencies(self, task: ScheduledTask) -> bool:
        """Check if all task dependencies are satisfied."""
        if not task.depends_on:
            return True

        for dep_id in task.depends_on:
            if dep_id in self.tasks:
                dep = self.tasks[dep_id]
                if dep.status != "completed":
                    return False
            else:
                # Check if completed in history
                history = self.get_history(dep_id, limit=1)
                if not history.get("history"):
                    return False
                last = history["history"][0]
                if last["status"] != "completed":
                    return False
        return True

    def get_dependency_outputs(self, task: ScheduledTask) -> Dict:
        """Get outputs from all completed dependencies."""
        outputs = {}
        for dep_id in task.depends_on:
            if dep_id in self.tasks:
                outputs[dep_id] = self.tasks[dep_id].output
        return outputs

    # =========================================================================
    # ENHANCED TASK SCHEDULING
    # =========================================================================

    def schedule_task(self, name: str, handler: str, task_type: str = 'once',
                      schedule: str = None, params: Dict = None,
                      delay: float = 0, priority: int = 5,
                      max_runs: int = None, metadata: Dict = None,
                      event_trigger: str = None,
                      # New advanced options
                      depends_on: List[str] = None,
                      max_retries: int = 3,
                      retry_delay: float = 1.0,
                      timeout: float = 300.0,
                      webhook_url: str = None,
                      condition: str = None,
                      workflow_id: str = None,
                      tags: List[str] = None,
                      concurrency_key: str = None,
                      rate_limit: str = None) -> Dict:
        """
        Schedule a new task with advanced options.

        New options:
            depends_on: List of task IDs that must complete first
            max_retries: Max retry attempts on failure (default 3)
            retry_delay: Base delay for exponential backoff (default 1s)
            timeout: Task execution timeout (default 300s)
            webhook_url: URL to POST on completion
            condition: Python expression that must be True to run
            workflow_id: Parent workflow ID
            tags: Tags for filtering/grouping
            concurrency_key: Key for mutual exclusion
            rate_limit: Rate limit spec (e.g., "10/m")
        """
        task_id = str(uuid.uuid4())[:12]

        # Calculate initial next_run
        if task_type == 'once':
            next_run = time.time() + delay
        elif task_type == 'interval':
            next_run = time.time() + self._parse_interval(schedule)
        elif task_type == 'cron':
            next_run = self._parse_cron(schedule)
        elif task_type == 'dependency':
            next_run = None  # Will be set when dependencies complete
        else:
            next_run = None

        # Set initial status
        initial_status = "pending"
        if depends_on:
            initial_status = "waiting"

        task = ScheduledTask(
            task_id=task_id,
            name=name,
            task_type=task_type,
            handler=handler,
            params=params or {},
            schedule=schedule,
            next_run=next_run,
            priority=priority,
            max_runs=max_runs,
            metadata=metadata or {},
            status=initial_status,
            depends_on=depends_on or [],
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            webhook_url=webhook_url,
            condition=condition,
            workflow_id=workflow_id,
            tags=tags or [],
            concurrency_key=concurrency_key,
            rate_limit=rate_limit
        )

        with self._lock:
            self.tasks[task_id] = task
            self._save_task(task)

            # Register event subscription
            if task_type == 'event' and event_trigger:
                if event_trigger not in self.event_handlers:
                    self.event_handlers[event_trigger] = []
                self.event_handlers[event_trigger].append(task_id)

                import sqlite3
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO event_subscriptions (event_name, task_id) VALUES (?, ?)",
                    (event_trigger, task_id)
                )
                conn.commit()
                conn.close()

        return {
            "success": True,
            "task_id": task_id,
            "name": name,
            "next_run": next_run,
            "status": initial_status,
            "message": f"Task '{name}' scheduled"
        }

    def get_due_tasks(self) -> List[ScheduledTask]:
        """Get tasks that are due for execution with advanced checks."""
        now = time.time()
        due = []

        with self._lock:
            # First, check waiting tasks for dependency satisfaction
            for task in self.tasks.values():
                if task.status == "waiting":
                    if self.check_dependencies(task):
                        task.status = "pending"
                        task.next_run = now
                        # Inject dependency outputs into params
                        dep_outputs = self.get_dependency_outputs(task)
                        if dep_outputs:
                            task.params["_dependency_outputs"] = dep_outputs
                        self._save_task(task)

            # Now get pending tasks that are due
            for task in self.tasks.values():
                if task.status == "pending" and task.next_run and task.next_run <= now:
                    # Check condition
                    if not self.evaluate_condition(task):
                        continue
                    # Check rate limit
                    if not self.can_run_with_rate_limit(task):
                        continue
                    # Check concurrency
                    if not self.can_run_with_concurrency(task):
                        continue
                    due.append(task)

        # Sort by priority
        due.sort(key=lambda t: t.priority)
        return due

    def mark_completed(self, task_id: str, result: Any = None, error: str = None):
        """Mark task as completed with advanced handling."""
        with self._lock:
            if task_id not in self.tasks:
                return

            task = self.tasks[task_id]
            duration = time.time() - (task.next_run or task.created)
            task.last_run = time.time()
            task.run_count += 1

            # Record history
            status = "failed" if error else "completed"
            self._record_history(task_id, duration, status, result, error)

            if error:
                # Handle failure with retry logic
                will_retry = self._handle_task_failure(task, error)
                if not will_retry:
                    self._send_webhook(task, "dead", error=error)
            else:
                task.error = None
                task.output = result  # Store output for dependent tasks

                # Check if max runs reached
                if task.max_runs and task.run_count >= task.max_runs:
                    task.status = "completed"
                else:
                    # Schedule next run for recurring tasks
                    task.next_run = self._calculate_next_run(task)
                    if task.next_run:
                        task.status = "pending"
                    else:
                        task.status = "completed"

                # Send webhook
                self._send_webhook(task, "completed", result=result)

            # Release concurrency lock
            if task.concurrency_key:
                self._release_lock(task.concurrency_key, task_id)

            self._save_task(task)

            # Update workflow if part of one
            if task.workflow_id and task.workflow_id in self.workflows:
                self._update_workflow_status(task.workflow_id)

            # Remove completed/dead tasks from memory
            if task.status in ("completed", "dead"):
                del self.tasks[task_id]

    def _update_workflow_status(self, workflow_id: str):
        """Update workflow status based on task statuses."""
        if workflow_id not in self.workflows:
            return

        wf = self.workflows[workflow_id]
        statuses = []
        for task_id in wf.tasks:
            if task_id in self.tasks:
                statuses.append(self.tasks[task_id].status)
            else:
                # Check history for completed
                history = self.get_history(task_id, limit=1)
                if history.get("history"):
                    statuses.append(history["history"][0]["status"])

        if all(s == "completed" for s in statuses):
            wf.status = "completed"
            wf.completed = time.time()
        elif any(s in ("dead", "failed") for s in statuses):
            wf.status = "failed"
        elif any(s == "running" for s in statuses):
            wf.status = "running"
            if not wf.started:
                wf.started = time.time()

        self._save_workflow(wf)

    # =========================================================================
    # TASK SEARCH & FILTERING
    # =========================================================================

    def search_tasks(self, query: str = None, tags: List[str] = None,
                     status: str = None, handler: str = None,
                     workflow_id: str = None) -> Dict:
        """Search tasks with multiple filters."""
        results = []
        for task in self.tasks.values():
            # Apply filters
            if status and task.status != status:
                continue
            if handler and task.handler != handler:
                continue
            if workflow_id and task.workflow_id != workflow_id:
                continue
            if tags and not any(t in task.tags for t in tags):
                continue
            if query and query.lower() not in task.name.lower():
                continue

            results.append({
                "task_id": task.task_id,
                "name": task.name,
                "status": task.status,
                "handler": task.handler,
                "tags": task.tags,
                "priority": task.priority,
                "next_run": task.next_run
            })

        return {"success": True, "tasks": results, "count": len(results)}

    def bulk_cancel(self, task_ids: List[str] = None, tags: List[str] = None,
                    workflow_id: str = None) -> Dict:
        """Cancel multiple tasks at once."""
        cancelled = []
        for task in list(self.tasks.values()):
            should_cancel = False
            if task_ids and task.task_id in task_ids:
                should_cancel = True
            if tags and any(t in task.tags for t in tags):
                should_cancel = True
            if workflow_id and task.workflow_id == workflow_id:
                should_cancel = True

            if should_cancel:
                self.cancel_task(task.task_id)
                cancelled.append(task.task_id)

        return {"success": True, "cancelled": cancelled, "count": len(cancelled)}

    # =========================================================================
    # METRICS COLLECTION & TIME-SERIES
    # =========================================================================

    def record_metric(self, name: str, value: float, tags: Dict = None, source: str = None) -> Dict:
        """Record a metric data point."""
        import sqlite3
        import random
        metric_id = f"{name}_{int(time.time()*1000)}_{random.randint(0, 999999):06d}"

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO metrics (metric_id, name, value, timestamp, tags, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (metric_id, name, value, time.time(), json.dumps(tags or {}), source or ""))
        conn.commit()
        conn.close()

        # Update rolling aggregator
        if name not in self.metric_aggregators:
            self.metric_aggregators[name] = []
        self.metric_aggregators[name].append(value)
        # Keep last 100 values in memory
        if len(self.metric_aggregators[name]) > 100:
            self.metric_aggregators[name] = self.metric_aggregators[name][-100:]

        # Check alert rules
        self._check_alert_rules(name, value)

        return {"success": True, "metric_id": metric_id, "name": name, "value": value}

    def get_metrics(self, name: str, start_time: float = None, end_time: float = None,
                    limit: int = 100) -> Dict:
        """Get metric history."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        query = "SELECT metric_id, name, value, timestamp, tags, source FROM metrics WHERE name = ?"
        params = [name]

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        metrics = []
        for row in cursor.fetchall():
            metrics.append({
                "metric_id": row[0],
                "name": row[1],
                "value": row[2],
                "timestamp": row[3],
                "tags": json.loads(row[4]) if row[4] else {},
                "source": row[5]
            })
        conn.close()

        return {"success": True, "metrics": metrics, "count": len(metrics)}

    def get_metric_stats(self, name: str, window_seconds: float = 3600) -> Dict:
        """Get aggregated statistics for a metric over a time window."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        start_time = time.time() - window_seconds
        cursor.execute("""
            SELECT
                COUNT(*) as count,
                MIN(value) as min_val,
                MAX(value) as max_val,
                AVG(value) as avg_val,
                SUM(value) as sum_val
            FROM metrics
            WHERE name = ? AND timestamp >= ?
        """, (name, start_time))

        row = cursor.fetchone()
        conn.close()

        if not row or row[0] == 0:
            return {"success": True, "stats": None, "message": "No data in window"}

        return {
            "success": True,
            "stats": {
                "name": name,
                "window_seconds": window_seconds,
                "count": row[0],
                "min": row[1],
                "max": row[2],
                "avg": row[3],
                "sum": row[4]
            }
        }

    def get_metric_trend(self, name: str, periods: int = 5, period_seconds: float = 300) -> Dict:
        """Analyze metric trend over multiple periods."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        now = time.time()
        period_stats = []

        for i in range(periods):
            end_time = now - (i * period_seconds)
            start_time = end_time - period_seconds

            cursor.execute("""
                SELECT AVG(value), COUNT(*) FROM metrics
                WHERE name = ? AND timestamp >= ? AND timestamp < ?
            """, (name, start_time, end_time))

            row = cursor.fetchone()
            if row and row[1] > 0:
                period_stats.append({
                    "period": i,
                    "start": start_time,
                    "end": end_time,
                    "avg": row[0],
                    "count": row[1]
                })

        conn.close()

        # Calculate trend
        trend = "stable"
        if len(period_stats) >= 2:
            recent = period_stats[0]["avg"] if period_stats[0]["avg"] else 0
            older = period_stats[-1]["avg"] if period_stats[-1]["avg"] else 0
            if older > 0:
                change = (recent - older) / older * 100
                if change > 10:
                    trend = "increasing"
                elif change < -10:
                    trend = "decreasing"

        return {
            "success": True,
            "name": name,
            "trend": trend,
            "periods": period_stats
        }

    # =========================================================================
    # ALERTING SYSTEM
    # =========================================================================

    def _load_alerts(self):
        """Load active alerts from database."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM alerts WHERE resolved = 0")
            for row in cursor.fetchall():
                alert = Alert(
                    alert_id=row[0],
                    name=row[1],
                    severity=row[2],
                    message=row[3],
                    timestamp=row[4],
                    source=row[5],
                    acknowledged=bool(row[6]),
                    resolved=bool(row[7]),
                    metadata=json.loads(row[9]) if row[9] else {}
                )
                self.alerts[alert.alert_id] = alert

            # Load alert rules
            cursor.execute("SELECT * FROM alert_rules WHERE enabled = 1")
            for row in cursor.fetchall():
                self.alert_rules[row[0]] = {
                    "rule_id": row[0],
                    "name": row[1],
                    "metric_name": row[2],
                    "condition": row[3],
                    "threshold": row[4],
                    "severity": row[5],
                    "cooldown": row[6],
                    "last_triggered": row[7],
                    "metadata": json.loads(row[9]) if row[9] else {}
                }
        except:
            pass
        conn.close()

    def create_alert_rule(self, name: str, metric_name: str, condition: str,
                          threshold: float, severity: str = "warning",
                          cooldown: float = 300, metadata: Dict = None) -> Dict:
        """
        Create an alert rule that triggers when metric conditions are met.

        Conditions: 'gt' (>), 'lt' (<), 'gte' (>=), 'lte' (<=), 'eq' (==)

        Example:
            create_alert_rule("high_latency", "api_latency", "gt", 1000, "critical")
        """
        import sqlite3
        rule_id = str(uuid.uuid4())[:12]

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO alert_rules
            (rule_id, name, metric_name, condition, threshold, severity, cooldown, enabled, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        """, (rule_id, name, metric_name, condition, threshold, severity, cooldown,
              json.dumps(metadata or {})))
        conn.commit()
        conn.close()

        self.alert_rules[rule_id] = {
            "rule_id": rule_id,
            "name": name,
            "metric_name": metric_name,
            "condition": condition,
            "threshold": threshold,
            "severity": severity,
            "cooldown": cooldown,
            "last_triggered": None,
            "metadata": metadata or {}
        }

        return {"success": True, "rule_id": rule_id, "name": name}

    def _check_alert_rules(self, metric_name: str, value: float):
        """Check if any alert rules should fire for this metric."""
        for rule_id, rule in self.alert_rules.items():
            if rule["metric_name"] != metric_name:
                continue

            # Check cooldown
            if rule["last_triggered"]:
                if time.time() - rule["last_triggered"] < rule["cooldown"]:
                    continue

            # Evaluate condition
            triggered = False
            condition = rule["condition"]
            threshold = rule["threshold"]

            if condition == "gt" and value > threshold:
                triggered = True
            elif condition == "lt" and value < threshold:
                triggered = True
            elif condition == "gte" and value >= threshold:
                triggered = True
            elif condition == "lte" and value <= threshold:
                triggered = True
            elif condition == "eq" and value == threshold:
                triggered = True

            if triggered:
                self._trigger_alert(rule, value)
                rule["last_triggered"] = time.time()
                # Update DB
                import sqlite3
                conn = sqlite3.connect(str(self.db_path))
                cursor = conn.cursor()
                cursor.execute("UPDATE alert_rules SET last_triggered = ? WHERE rule_id = ?",
                              (time.time(), rule_id))
                conn.commit()
                conn.close()

    def _trigger_alert(self, rule: Dict, value: float):
        """Trigger an alert from a rule."""
        alert_id = str(uuid.uuid4())[:12]
        alert = Alert(
            alert_id=alert_id,
            name=rule["name"],
            severity=rule["severity"],
            message=f"{rule['metric_name']} {rule['condition']} {rule['threshold']} (value: {value})",
            timestamp=time.time(),
            source=f"rule:{rule['rule_id']}",
            metadata={"rule": rule, "trigger_value": value}
        )

        self.alerts[alert_id] = alert

        # Persist to DB
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO alerts (alert_id, name, severity, message, timestamp, source, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (alert.alert_id, alert.name, alert.severity, alert.message,
              alert.timestamp, alert.source, json.dumps(alert.metadata)))
        conn.commit()
        conn.close()

        # Emit event for alert subscribers
        self.emit_event(f"alert:{alert.severity}", {
            "alert_id": alert_id,
            "name": alert.name,
            "message": alert.message
        })

    def create_alert(self, name: str, severity: str, message: str,
                     source: str = None, metadata: Dict = None) -> Dict:
        """Create a manual alert."""
        alert_id = str(uuid.uuid4())[:12]
        alert = Alert(
            alert_id=alert_id,
            name=name,
            severity=severity,
            message=message,
            timestamp=time.time(),
            source=source or "",
            metadata=metadata or {}
        )

        self.alerts[alert_id] = alert

        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO alerts (alert_id, name, severity, message, timestamp, source, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (alert.alert_id, alert.name, alert.severity, alert.message,
              alert.timestamp, alert.source, json.dumps(alert.metadata)))
        conn.commit()
        conn.close()

        return {"success": True, "alert_id": alert_id}

    def acknowledge_alert(self, alert_id: str) -> Dict:
        """Acknowledge an alert."""
        if alert_id not in self.alerts:
            return {"success": False, "error": "Alert not found"}

        self.alerts[alert_id].acknowledged = True

        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("UPDATE alerts SET acknowledged = 1 WHERE alert_id = ?", (alert_id,))
        conn.commit()
        conn.close()

        return {"success": True, "alert_id": alert_id}

    def resolve_alert(self, alert_id: str) -> Dict:
        """Resolve an alert."""
        if alert_id not in self.alerts:
            return {"success": False, "error": "Alert not found"}

        self.alerts[alert_id].resolved = True

        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("UPDATE alerts SET resolved = 1, resolved_at = ? WHERE alert_id = ?",
                      (time.time(), alert_id))
        conn.commit()
        conn.close()

        del self.alerts[alert_id]

        return {"success": True, "alert_id": alert_id}

    def get_alerts(self, severity: str = None, unacknowledged_only: bool = False) -> Dict:
        """Get active alerts."""
        alerts = []
        for alert in self.alerts.values():
            if severity and alert.severity != severity:
                continue
            if unacknowledged_only and alert.acknowledged:
                continue
            alerts.append({
                "alert_id": alert.alert_id,
                "name": alert.name,
                "severity": alert.severity,
                "message": alert.message,
                "timestamp": alert.timestamp,
                "acknowledged": alert.acknowledged,
                "source": alert.source
            })

        # Sort by severity then timestamp
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        alerts.sort(key=lambda a: (severity_order.get(a["severity"], 3), -a["timestamp"]))

        return {"success": True, "alerts": alerts, "count": len(alerts)}

    # =========================================================================
    # AGENT MEMORY (Persistent Key-Value Store)
    # =========================================================================

    def agent_memory_set(self, agent_id: str, key: str, value: Any,
                         ttl: float = None) -> Dict:
        """Set a value in agent's persistent memory."""
        import sqlite3
        expires = time.time() + ttl if ttl else None

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO agent_memory (agent_id, key, value, updated, expires)
            VALUES (?, ?, ?, ?, ?)
        """, (agent_id, key, json.dumps(value), time.time(), expires))
        conn.commit()
        conn.close()

        return {"success": True, "agent_id": agent_id, "key": key}

    def agent_memory_get(self, agent_id: str, key: str, default: Any = None) -> Dict:
        """Get a value from agent's persistent memory."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            SELECT value, expires FROM agent_memory
            WHERE agent_id = ? AND key = ?
        """, (agent_id, key))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return {"success": True, "value": default, "found": False}

        # Check expiration
        if row[1] and row[1] < time.time():
            self.agent_memory_delete(agent_id, key)
            return {"success": True, "value": default, "found": False, "expired": True}

        return {"success": True, "value": json.loads(row[0]), "found": True}

    def agent_memory_delete(self, agent_id: str, key: str) -> Dict:
        """Delete a key from agent's memory."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("DELETE FROM agent_memory WHERE agent_id = ? AND key = ?",
                      (agent_id, key))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()

        return {"success": True, "deleted": deleted}

    def agent_memory_list(self, agent_id: str) -> Dict:
        """List all keys in agent's memory."""
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            SELECT key, updated, expires FROM agent_memory
            WHERE agent_id = ? AND (expires IS NULL OR expires > ?)
        """, (agent_id, time.time()))

        keys = []
        for row in cursor.fetchall():
            keys.append({
                "key": row[0],
                "updated": row[1],
                "expires": row[2]
            })
        conn.close()

        return {"success": True, "agent_id": agent_id, "keys": keys, "count": len(keys)}

    def agent_memory_increment(self, agent_id: str, key: str, delta: float = 1) -> Dict:
        """Atomically increment a numeric value in agent memory."""
        current = self.agent_memory_get(agent_id, key, default=0)
        new_value = current["value"] + delta
        self.agent_memory_set(agent_id, key, new_value)
        return {"success": True, "value": new_value, "previous": current["value"]}

    # =========================================================================
    # ADAPTIVE SCHEDULING
    # =========================================================================

    def adjust_task_schedule(self, task_id: str, new_schedule: str = None,
                             new_interval: float = None) -> Dict:
        """Dynamically adjust a task's schedule."""
        if task_id not in self.tasks:
            return {"success": False, "error": "Task not found"}

        task = self.tasks[task_id]
        old_schedule = task.schedule

        if new_schedule:
            task.schedule = new_schedule

        if new_interval:
            task.schedule = f"{int(new_interval)}s"

        # Recalculate next run
        task.next_run = self._calculate_next_run(task)
        self._save_task(task)

        return {
            "success": True,
            "task_id": task_id,
            "old_schedule": old_schedule,
            "new_schedule": task.schedule,
            "next_run": task.next_run
        }

    def set_adaptive_scheduling(self, task_id: str, min_interval: float, max_interval: float,
                                metric_name: str, target_value: float) -> Dict:
        """
        Configure adaptive scheduling based on a metric.

        The task will speed up when metric is above target, slow down when below.
        """
        if task_id not in self.tasks:
            return {"success": False, "error": "Task not found"}

        task = self.tasks[task_id]
        task.metadata["adaptive"] = {
            "enabled": True,
            "min_interval": min_interval,
            "max_interval": max_interval,
            "metric_name": metric_name,
            "target_value": target_value
        }
        self._save_task(task)

        return {"success": True, "task_id": task_id, "adaptive_config": task.metadata["adaptive"]}

    def _apply_adaptive_scheduling(self, task: ScheduledTask):
        """Apply adaptive scheduling after task completion."""
        if "adaptive" not in task.metadata or not task.metadata["adaptive"].get("enabled"):
            return

        config = task.metadata["adaptive"]
        metric_stats = self.get_metric_stats(config["metric_name"], window_seconds=300)

        if not metric_stats.get("stats"):
            return

        current_avg = metric_stats["stats"]["avg"]
        target = config["target_value"]
        min_interval = config["min_interval"]
        max_interval = config["max_interval"]

        # Parse current interval
        current_interval = self._parse_interval(task.schedule) if task.schedule else 60

        # Adjust based on metric
        if current_avg > target * 1.2:  # 20% above target - speed up
            new_interval = max(min_interval, current_interval * 0.8)
        elif current_avg < target * 0.8:  # 20% below target - slow down
            new_interval = min(max_interval, current_interval * 1.2)
        else:
            return  # Within acceptable range

        # Update schedule
        task.schedule = f"{int(new_interval)}s"
        self._save_task(task)

    # =========================================================================
    # OBSERVABILITY PIPELINE
    # =========================================================================

    def create_observability_pipeline(self, name: str, config: Dict) -> Dict:
        """
        Create a complete observability pipeline with metrics, alerts, and agents.

        Config example:
        {
            "metrics": ["cpu_usage", "memory_usage", "request_latency"],
            "alerts": [
                {"name": "high_cpu", "metric": "cpu_usage", "condition": "gt", "threshold": 80, "severity": "warning"},
                {"name": "critical_latency", "metric": "request_latency", "condition": "gt", "threshold": 5000, "severity": "critical"}
            ],
            "agents": {
                "collector": {"schedule": "1m", "handler": "agent:Collect system metrics"},
                "analyzer": {"schedule": "5m", "handler": "agent:Analyze recent metrics and identify anomalies"},
                "reporter": {"schedule": "1h", "handler": "agent:Generate hourly observability report"}
            }
        }
        """
        created = {"metrics": [], "alert_rules": [], "tasks": []}

        # Create alert rules
        for alert_config in config.get("alerts", []):
            result = self.create_alert_rule(
                name=alert_config["name"],
                metric_name=alert_config["metric"],
                condition=alert_config["condition"],
                threshold=alert_config["threshold"],
                severity=alert_config.get("severity", "warning")
            )
            if result["success"]:
                created["alert_rules"].append(result["rule_id"])

        # Create scheduled agents
        for agent_name, agent_config in config.get("agents", {}).items():
            schedule = agent_config.get("schedule", "5m")
            task_type = "cron" if " " in schedule else "interval"

            result = self.schedule_task(
                name=f"{name}:{agent_name}",
                handler=agent_config["handler"],
                task_type=task_type,
                schedule=schedule,
                tags=["observability", name],
                metadata={"pipeline": name, "role": agent_name}
            )
            if result["success"]:
                created["tasks"].append(result["task_id"])

        # Create alert handler agents
        for severity in ["critical", "warning"]:
            result = self.schedule_task(
                name=f"{name}:alert_handler_{severity}",
                handler=f"agent:Handle {severity} alert - investigate the issue and suggest remediation",
                task_type="event",
                event_trigger=f"alert:{severity}",
                tags=["observability", name, "alert-handler"],
                metadata={"pipeline": name, "role": "alert_handler", "severity": severity}
            )
            if result["success"]:
                created["tasks"].append(result["task_id"])

        created["metrics"] = config.get("metrics", [])

        return {
            "success": True,
            "pipeline": name,
            "created": created
        }

    def get_system_health(self) -> Dict:
        """Get overall system health status."""
        # Count alerts by severity
        critical_alerts = sum(1 for a in self.alerts.values() if a.severity == "critical")
        warning_alerts = sum(1 for a in self.alerts.values() if a.severity == "warning")

        # Get task stats
        task_stats = self.stats()["stats"]

        # Calculate health score
        health_score = 100
        health_score -= critical_alerts * 25
        health_score -= warning_alerts * 10

        if task_stats["success_rate"] < 0.9:
            health_score -= 20
        elif task_stats["success_rate"] < 0.95:
            health_score -= 10

        health_score = max(0, health_score)

        if health_score >= 90:
            status = "healthy"
        elif health_score >= 70:
            status = "degraded"
        elif health_score >= 50:
            status = "warning"
        else:
            status = "critical"

        return {
            "success": True,
            "health": {
                "score": health_score,
                "status": status,
                "critical_alerts": critical_alerts,
                "warning_alerts": warning_alerts,
                "active_tasks": task_stats["active_tasks"],
                "success_rate": task_stats["success_rate"],
                "workflows": task_stats.get("workflows", 0)
            }
        }

    # =========================================================================
    # SMART CRON WITH HISTORY-AWARE SCHEDULING
    # =========================================================================

    def create_smart_cron(self, name: str, handler: str, base_schedule: str,
                          learning_enabled: bool = True,
                          optimization_metric: str = None) -> Dict:
        """
        Create a smart cron job that learns from execution history.

        Features:
        - Tracks execution times and adjusts schedule for optimal performance
        - Learns patterns from successful/failed runs
        - Can optimize based on a custom metric
        """
        task_result = self.schedule_task(
            name=name,
            handler=handler,
            task_type="interval" if not " " in base_schedule else "cron",
            schedule=base_schedule,
            tags=["smart-cron", "learning"],
            metadata={
                "smart_cron": {
                    "learning_enabled": learning_enabled,
                    "base_schedule": base_schedule,
                    "optimization_metric": optimization_metric,
                    "execution_times": [],
                    "success_rate_history": [],
                    "schedule_adjustments": []
                }
            }
        )

        return task_result

    def get_cron_insights(self, task_id: str) -> Dict:
        """Get insights about a smart cron job's performance."""
        if task_id not in self.tasks:
            # Check history
            history = self.get_history(task_id, limit=50)
            if not history.get("history"):
                return {"success": False, "error": "Task not found"}
        else:
            history = self.get_history(task_id, limit=50)

        executions = history.get("history", [])
        if not executions:
            return {"success": True, "insights": None, "message": "No execution history"}

        # Analyze patterns
        durations = [e["duration"] for e in executions if e.get("duration")]
        successes = sum(1 for e in executions if e["status"] == "completed")
        failures = sum(1 for e in executions if e["status"] == "failed")

        # Time-of-day analysis
        from datetime import datetime
        hour_distribution = {}
        for e in executions:
            hour = datetime.fromtimestamp(e["run_time"]).hour
            hour_distribution[hour] = hour_distribution.get(hour, 0) + 1

        insights = {
            "total_executions": len(executions),
            "success_rate": successes / len(executions) if executions else 0,
            "avg_duration": sum(durations) / len(durations) if durations else 0,
            "min_duration": min(durations) if durations else 0,
            "max_duration": max(durations) if durations else 0,
            "hour_distribution": hour_distribution,
            "peak_hours": sorted(hour_distribution.keys(),
                                key=lambda h: hour_distribution[h], reverse=True)[:3]
        }

        return {"success": True, "task_id": task_id, "insights": insights}


class AsyncAgentRuntime:
    """
    Async runtime for agent execution with scheduling support.

    Provides:
    - Async event loop management
    - Task execution with proper async/await
    - Integration with TaskScheduler
    - Concurrent task execution
    """

    def __init__(self, agent: 'AdvancedAgent', scheduler: TaskScheduler):
        self.agent = agent
        self.scheduler = scheduler
        self.running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._main_task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=10)

    async def execute_handler(self, handler: str, params: Dict,
                               metadata: Dict = None) -> Dict:
        """Execute a task handler asynchronously."""
        try:
            if handler.startswith("agent:"):
                # Agent task - spawn sub-agent
                task_description = handler[6:]  # Remove 'agent:' prefix
                result = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: self.agent._execute_spawn_agent({
                        "task": task_description,
                        "context": metadata or {},
                        **params
                    })
                )
                return result
            else:
                # Tool execution
                result = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: self.agent.execute_tool(handler, params)
                )
                return {"success": True, "result": json.loads(result)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def process_due_tasks(self):
        """Process all due tasks."""
        due_tasks = self.scheduler.get_due_tasks()

        for task in due_tasks:
            self.scheduler.mark_running(task.task_id)

            try:
                result = await self.execute_handler(
                    task.handler,
                    task.params,
                    task.metadata
                )

                if result.get("success"):
                    self.scheduler.mark_completed(task.task_id, result=result.get("result"))
                else:
                    self.scheduler.mark_completed(task.task_id, error=result.get("error"))

            except Exception as e:
                self.scheduler.mark_completed(task.task_id, error=str(e))

    async def scheduler_loop(self, check_interval: float = 1.0):
        """Main scheduler loop - checks for due tasks periodically."""
        while self.running:
            try:
                await self.process_due_tasks()
            except Exception as e:
                print(f"\033[91mScheduler error: {e}\033[0m")

            await asyncio.sleep(check_interval)

    def start(self):
        """Start the async runtime."""
        if self.running:
            return

        self.running = True

        def run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._main_task = self._loop.create_task(self.scheduler_loop())

            try:
                self._loop.run_until_complete(self._main_task)
            except asyncio.CancelledError:
                pass
            finally:
                self._loop.close()

        self._thread = threading.Thread(target=run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the async runtime."""
        self.running = False
        if self._main_task:
            self._main_task.cancel()
        if self._executor:
            self._executor.shutdown(wait=False)

    def emit_event(self, event_name: str, event_data: Dict = None) -> Dict:
        """Emit an event to trigger subscribed tasks."""
        return self.scheduler.emit_event(event_name, event_data)

    async def run_task_now(self, task_id: str) -> Dict:
        """Run a specific task immediately."""
        task_info = self.scheduler.get_task(task_id)
        if not task_info.get("success"):
            return task_info

        task = task_info["task"]
        self.scheduler.mark_running(task_id)

        try:
            result = await self.execute_handler(
                task["handler"],
                task["params"],
                task.get("metadata", {})
            )

            if result.get("success"):
                self.scheduler.mark_completed(task_id, result=result.get("result"))
            else:
                self.scheduler.mark_completed(task_id, error=result.get("error"))

            return result
        except Exception as e:
            self.scheduler.mark_completed(task_id, error=str(e))
            return {"success": False, "error": str(e)}


# =============================================================================
# DYNAMIC TOOL REGISTRY - Create tools at runtime
# =============================================================================

@dataclass
class DynamicTool:
    """A dynamically created tool."""
    name: str
    description: str
    code: str  # Python function code
    schema: Dict  # Input schema
    created: float
    modified: float
    version: int = 1
    enabled: bool = True
    author: str = "agent"


class DynamicToolRegistry:
    """Registry for dynamically created tools.

    Allows agents to create, modify, and delete tools at runtime.
    Tools are persisted to disk and loaded on startup.
    """

    TOOLS_DIR = Path(".agent_tools")
    TOOLS_FILE = "dynamic_tools.json"

    def __init__(self):
        self.tools: Dict[str, DynamicTool] = {}
        self._compiled: Dict[str, Callable] = {}  # Compiled functions
        self._load_tools()

    def _get_tools_path(self) -> Path:
        """Get path to tools file."""
        self.TOOLS_DIR.mkdir(exist_ok=True)
        return self.TOOLS_DIR / self.TOOLS_FILE

    def _load_tools(self):
        """Load tools from disk."""
        path = self._get_tools_path()
        if path.exists():
            try:
                data = json.loads(path.read_text())
                for name, tool_data in data.items():
                    tool = DynamicTool(
                        name=tool_data["name"],
                        description=tool_data["description"],
                        code=tool_data["code"],
                        schema=tool_data["schema"],
                        created=tool_data.get("created", time.time()),
                        modified=tool_data.get("modified", time.time()),
                        version=tool_data.get("version", 1),
                        enabled=tool_data.get("enabled", True),
                        author=tool_data.get("author", "agent")
                    )
                    self.tools[name] = tool
                    if tool.enabled:
                        self._compile_tool(tool)
            except Exception as e:
                print(f"\033[93mWarning: Could not load dynamic tools: {e}\033[0m")

    def _save_tools(self):
        """Save tools to disk."""
        path = self._get_tools_path()
        data = {}
        for name, tool in self.tools.items():
            data[name] = {
                "name": tool.name,
                "description": tool.description,
                "code": tool.code,
                "schema": tool.schema,
                "created": tool.created,
                "modified": tool.modified,
                "version": tool.version,
                "enabled": tool.enabled,
                "author": tool.author
            }
        path.write_text(json.dumps(data, indent=2))

    def _compile_tool(self, tool: DynamicTool) -> Optional[str]:
        """Compile a tool's code into a callable function.

        Returns error message if compilation fails, None on success.
        """
        try:
            # Create a namespace for the function
            namespace = {
                "__builtins__": __builtins__,
                "json": json,
                "time": time,
                "Path": Path,
                "os": os,
                "re": __import__("re"),
                "math": __import__("math"),
                "datetime": __import__("datetime"),
                "urllib": urllib,
                "subprocess": __import__("subprocess"),
            }

            # Execute the code to define the function
            exec(tool.code, namespace)

            # The function should be named 'execute' or same as tool name
            func_name = tool.name if tool.name in namespace else "execute"
            if func_name not in namespace:
                return f"Tool code must define a function named 'execute' or '{tool.name}'"

            func = namespace[func_name]
            if not callable(func):
                return f"'{func_name}' is not callable"

            self._compiled[tool.name] = func
            return None

        except SyntaxError as e:
            return f"Syntax error: {e}"
        except Exception as e:
            return f"Compilation error: {e}"

    def _validate_tool_name(self, name: str) -> Optional[str]:
        """Validate tool name. Returns error message or None."""
        if not name:
            return "Tool name cannot be empty"
        if not name.replace("_", "").isalnum():
            return "Tool name must be alphanumeric (underscores allowed)"
        if name.startswith("_"):
            return "Tool name cannot start with underscore"
        if len(name) > 64:
            return "Tool name too long (max 64 chars)"
        # Reserved names
        reserved = {"create_tool", "delete_tool", "edit_tool", "list_dynamic_tools",
                    "get_tool_code", "call_tools", "chain", "spawn_agent", "remember",
                    "recall", "think", "conditional", "execute", "run", "eval"}
        if name in reserved:
            return f"'{name}' is a reserved tool name"
        return None

    def create(self, name: str, description: str, code: str,
               schema: Dict, author: str = "agent") -> Dict:
        """Create a new dynamic tool.

        Args:
            name: Unique tool name
            description: What the tool does
            code: Python code defining an 'execute(params)' function
            schema: JSON schema for input parameters
            author: Who created this tool

        Returns:
            {"success": bool, "error"?: str, "tool"?: dict}
        """
        # Validate name
        err = self._validate_tool_name(name)
        if err:
            return {"success": False, "error": err}

        if name in self.tools:
            return {"success": False, "error": f"Tool '{name}' already exists. Use edit_tool to modify."}

        # Create tool object
        now = time.time()
        tool = DynamicTool(
            name=name,
            description=description,
            code=code,
            schema=schema,
            created=now,
            modified=now,
            version=1,
            enabled=True,
            author=author
        )

        # Try to compile
        compile_err = self._compile_tool(tool)
        if compile_err:
            return {"success": False, "error": compile_err}

        # Save
        self.tools[name] = tool
        self._save_tools()

        return {
            "success": True,
            "tool": {
                "name": name,
                "description": description,
                "version": 1,
                "message": f"Tool '{name}' created and ready to use"
            }
        }

    def edit(self, name: str, description: str = None, code: str = None,
             schema: Dict = None) -> Dict:
        """Edit an existing tool."""
        if name not in self.tools:
            return {"success": False, "error": f"Tool '{name}' not found"}

        tool = self.tools[name]

        # Update fields
        if description is not None:
            tool.description = description
        if schema is not None:
            tool.schema = schema
        if code is not None:
            tool.code = code
            # Recompile
            compile_err = self._compile_tool(tool)
            if compile_err:
                return {"success": False, "error": compile_err}

        tool.modified = time.time()
        tool.version += 1

        self._save_tools()

        return {
            "success": True,
            "tool": {
                "name": name,
                "version": tool.version,
                "message": f"Tool '{name}' updated to version {tool.version}"
            }
        }

    def delete(self, name: str) -> Dict:
        """Delete a tool."""
        if name not in self.tools:
            return {"success": False, "error": f"Tool '{name}' not found"}

        del self.tools[name]
        if name in self._compiled:
            del self._compiled[name]

        self._save_tools()

        return {"success": True, "message": f"Tool '{name}' deleted"}

    def get(self, name: str) -> Optional[DynamicTool]:
        """Get a tool by name."""
        return self.tools.get(name)

    def get_code(self, name: str) -> Dict:
        """Get the source code of a tool."""
        if name not in self.tools:
            return {"success": False, "error": f"Tool '{name}' not found"}

        tool = self.tools[name]
        return {
            "success": True,
            "name": name,
            "description": tool.description,
            "code": tool.code,
            "schema": tool.schema,
            "version": tool.version,
            "created": tool.created,
            "modified": tool.modified,
            "author": tool.author
        }

    def list_tools(self) -> Dict:
        """List all dynamic tools."""
        return {
            "tools": {
                name: {
                    "description": t.description,
                    "version": t.version,
                    "enabled": t.enabled,
                    "author": t.author
                }
                for name, t in self.tools.items()
            },
            "count": len(self.tools)
        }

    def execute(self, name: str, params: Dict) -> Dict:
        """Execute a dynamic tool."""
        if name not in self.tools:
            return {"success": False, "error": f"Tool '{name}' not found"}

        tool = self.tools[name]
        if not tool.enabled:
            return {"success": False, "error": f"Tool '{name}' is disabled"}

        if name not in self._compiled:
            compile_err = self._compile_tool(tool)
            if compile_err:
                return {"success": False, "error": compile_err}

        func = self._compiled[name]

        try:
            result = func(params)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": f"Execution error: {e}"}

    def get_tool_definitions(self) -> List[Dict]:
        """Get tool definitions in the format expected by Claude."""
        definitions = []
        for name, tool in self.tools.items():
            if tool.enabled:
                definitions.append({
                    "name": name,
                    "description": tool.description,
                    "input_schema": tool.schema
                })
        return definitions

    def enable(self, name: str) -> Dict:
        """Enable a tool."""
        if name not in self.tools:
            return {"success": False, "error": f"Tool '{name}' not found"}
        self.tools[name].enabled = True
        self._save_tools()
        return {"success": True, "message": f"Tool '{name}' enabled"}

    def disable(self, name: str) -> Dict:
        """Disable a tool."""
        if name not in self.tools:
            return {"success": False, "error": f"Tool '{name}' not found"}
        self.tools[name].enabled = False
        self._save_tools()
        return {"success": True, "message": f"Tool '{name}' disabled"}


# =============================================================================
# VECTOR MEMORY - Persistent semantic memory with search
# =============================================================================

import sqlite3
import math
from collections import Counter


@dataclass
class MemoryEntry:
    """A memory entry with content and metadata."""
    id: str
    content: str
    content_type: str  # conversation, code, result, note, fact, etc.
    metadata: Dict[str, Any]
    embedding: Optional[List[float]]
    created_at: float
    accessed_at: float
    access_count: int
    importance: float  # 0-1 score for prioritization


class VectorMemory:
    """Persistent vector memory with semantic search.

    Uses SQLite with FTS5 for full-text search and TF-IDF for semantic similarity.
    No external dependencies required.

    Features:
    - Full-text search with BM25 ranking
    - TF-IDF based semantic similarity
    - Metadata filtering
    - Importance scoring and decay
    - Optional embedding API integration
    """

    MEMORY_DIR = Path(".agent_memory")
    DB_FILE = "memory.db"

    def __init__(self, embedding_fn: Optional[Callable] = None):
        """Initialize vector memory.

        Args:
            embedding_fn: Optional function that takes text and returns embedding vector.
                         If not provided, uses TF-IDF approximation.
        """
        self.embedding_fn = embedding_fn
        self._ensure_db()
        self._vocabulary: Dict[str, int] = {}  # word -> doc frequency
        self._doc_count = 0
        self._load_vocabulary()

    def _ensure_db(self):
        """Create database and tables if needed."""
        self.MEMORY_DIR.mkdir(exist_ok=True)
        self.db_path = self.MEMORY_DIR / self.DB_FILE

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # Main memory table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memories (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                content_type TEXT NOT NULL,
                metadata TEXT,
                embedding TEXT,
                tfidf_vector TEXT,
                created_at REAL NOT NULL,
                accessed_at REAL NOT NULL,
                access_count INTEGER DEFAULT 1,
                importance REAL DEFAULT 0.5
            )
        """)

        # Full-text search index
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
                id,
                content,
                content_type,
                metadata,
                content='memories',
                content_rowid='rowid'
            )
        """)

        # Triggers to keep FTS in sync
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
                INSERT INTO memories_fts(rowid, id, content, content_type, metadata)
                VALUES (new.rowid, new.id, new.content, new.content_type, new.metadata);
            END
        """)

        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, id, content, content_type, metadata)
                VALUES('delete', old.rowid, old.id, old.content, old.content_type, old.metadata);
            END
        """)

        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, id, content, content_type, metadata)
                VALUES('delete', old.rowid, old.id, old.content, old.content_type, old.metadata);
                INSERT INTO memories_fts(rowid, id, content, content_type, metadata)
                VALUES (new.rowid, new.id, new.content, new.content_type, new.metadata);
            END
        """)

        # Vocabulary table for TF-IDF
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vocabulary (
                word TEXT PRIMARY KEY,
                doc_freq INTEGER DEFAULT 1
            )
        """)

        # Statistics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        conn.commit()
        conn.close()

    def _get_conn(self) -> sqlite3.Connection:
        """Get database connection."""
        return sqlite3.connect(str(self.db_path))

    def _load_vocabulary(self):
        """Load vocabulary from database."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT word, doc_freq FROM vocabulary")
        for word, freq in cursor.fetchall():
            self._vocabulary[word] = freq

        cursor.execute("SELECT COUNT(*) FROM memories")
        self._doc_count = cursor.fetchone()[0]

        conn.close()

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into words."""
        import re
        # Simple tokenization: lowercase, split on non-alphanumeric
        text = text.lower()
        tokens = re.findall(r'\b[a-z0-9]+\b', text)
        # Remove very short and very long tokens
        tokens = [t for t in tokens if 2 <= len(t) <= 30]
        return tokens

    def _compute_tfidf(self, text: str) -> Dict[str, float]:
        """Compute TF-IDF vector for text."""
        tokens = self._tokenize(text)
        if not tokens:
            return {}

        # Term frequency
        tf = Counter(tokens)
        max_tf = max(tf.values()) if tf else 1

        # TF-IDF with smoothed IDF to handle small corpora
        tfidf = {}
        N = max(self._doc_count, 1)  # Avoid division by 0
        for word, count in tf.items():
            # Normalized TF (augmented frequency)
            word_tf = 0.5 + 0.5 * (count / max_tf)
            # Smoothed IDF: log((N + 1) / (df + 1)) + 1
            # This prevents IDF from being 0 even for common words
            doc_freq = self._vocabulary.get(word, 0)
            word_idf = math.log((N + 1) / (doc_freq + 1)) + 1
            tfidf[word] = word_tf * word_idf

        return tfidf

    def _cosine_similarity(self, vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
        """Compute cosine similarity between two sparse vectors."""
        if not vec1 or not vec2:
            return 0.0

        # Get common words
        common = set(vec1.keys()) & set(vec2.keys())
        if not common:
            return 0.0

        # Dot product
        dot = sum(vec1[w] * vec2[w] for w in common)

        # Magnitudes
        mag1 = math.sqrt(sum(v ** 2 for v in vec1.values()))
        mag2 = math.sqrt(sum(v ** 2 for v in vec2.values()))

        if mag1 == 0 or mag2 == 0:
            return 0.0

        return dot / (mag1 * mag2)

    def _update_vocabulary(self, tokens: List[str], conn: sqlite3.Connection):
        """Update vocabulary with new tokens."""
        cursor = conn.cursor()
        unique_tokens = set(tokens)

        for token in unique_tokens:
            cursor.execute("""
                INSERT INTO vocabulary (word, doc_freq) VALUES (?, 1)
                ON CONFLICT(word) DO UPDATE SET doc_freq = doc_freq + 1
            """, (token,))
            self._vocabulary[token] = self._vocabulary.get(token, 0) + 1

    def store(self, content: str, content_type: str = "note",
              metadata: Dict = None, importance: float = 0.5,
              memory_id: str = None) -> Dict:
        """Store content in memory.

        Args:
            content: Text content to store
            content_type: Type (conversation, code, result, note, fact, etc.)
            metadata: Optional metadata dict (tags, source, etc.)
            importance: 0-1 score for prioritization
            memory_id: Optional custom ID (auto-generated if not provided)

        Returns:
            {"success": bool, "id": str, ...}
        """
        if not content or not content.strip():
            return {"success": False, "error": "Content cannot be empty"}

        memory_id = memory_id or str(uuid.uuid4())[:12]
        now = time.time()
        metadata = metadata or {}

        # Compute TF-IDF vector
        tokens = self._tokenize(content)
        tfidf = self._compute_tfidf(content)

        # Get embedding if function provided
        embedding = None
        if self.embedding_fn:
            try:
                embedding = self.embedding_fn(content)
            except:
                pass

        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            # Update vocabulary
            self._update_vocabulary(tokens, conn)
            self._doc_count += 1

            # Insert memory
            cursor.execute("""
                INSERT INTO memories (id, content, content_type, metadata, embedding,
                                     tfidf_vector, created_at, accessed_at, access_count, importance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """, (
                memory_id,
                content,
                content_type,
                json.dumps(metadata),
                json.dumps(embedding) if embedding else None,
                json.dumps(tfidf),
                now,
                now,
                importance
            ))

            conn.commit()

            return {
                "success": True,
                "id": memory_id,
                "content_type": content_type,
                "tokens": len(tokens),
                "message": f"Stored memory '{memory_id}'"
            }

        except sqlite3.IntegrityError:
            return {"success": False, "error": f"Memory with ID '{memory_id}' already exists"}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def search(self, query: str, top_k: int = 10,
               content_types: List[str] = None,
               min_importance: float = 0.0,
               min_similarity: float = 0.1,
               metadata_filter: Dict = None) -> Dict:
        """Search memories semantically.

        Args:
            query: Search query
            top_k: Number of results to return
            content_types: Filter by content types
            min_importance: Minimum importance score
            min_similarity: Minimum similarity score
            metadata_filter: Filter by metadata fields

        Returns:
            {"success": bool, "results": [...], ...}
        """
        if not query or not query.strip():
            return {"success": False, "error": "Query cannot be empty"}

        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            # First, get FTS matches with BM25 ranking
            fts_query = ' OR '.join(self._tokenize(query)[:20])  # Limit query terms

            if fts_query:
                cursor.execute("""
                    SELECT m.id, m.content, m.content_type, m.metadata, m.tfidf_vector,
                           m.created_at, m.accessed_at, m.access_count, m.importance,
                           bm25(memories_fts) as fts_score
                    FROM memories m
                    JOIN memories_fts ON m.id = memories_fts.id
                    WHERE memories_fts MATCH ?
                    AND m.importance >= ?
                    ORDER BY fts_score
                    LIMIT ?
                """, (fts_query, min_importance, top_k * 3))
            else:
                # Fallback: get recent memories
                cursor.execute("""
                    SELECT id, content, content_type, metadata, tfidf_vector,
                           created_at, accessed_at, access_count, importance, 0.0 as fts_score
                    FROM memories
                    WHERE importance >= ?
                    ORDER BY accessed_at DESC
                    LIMIT ?
                """, (min_importance, top_k * 3))

            candidates = cursor.fetchall()

            # Compute TF-IDF similarity
            query_tfidf = self._compute_tfidf(query)
            results = []

            for row in candidates:
                mem_id, content, content_type, metadata_str, tfidf_str, \
                    created_at, accessed_at, access_count, importance, fts_score = row

                # Filter by content type
                if content_types and content_type not in content_types:
                    continue

                # Parse metadata and filter
                try:
                    metadata = json.loads(metadata_str) if metadata_str else {}
                except:
                    metadata = {}

                if metadata_filter:
                    match = all(metadata.get(k) == v for k, v in metadata_filter.items())
                    if not match:
                        continue

                # Compute similarity
                try:
                    mem_tfidf = json.loads(tfidf_str) if tfidf_str else {}
                except:
                    mem_tfidf = {}

                similarity = self._cosine_similarity(query_tfidf, mem_tfidf)

                if similarity < min_similarity:
                    continue

                # Combined score (similarity + importance + recency)
                recency = 1.0 / (1.0 + (time.time() - accessed_at) / 86400)  # Decay over days
                combined_score = (similarity * 0.6) + (importance * 0.2) + (recency * 0.2)

                results.append({
                    "id": mem_id,
                    "content": content[:500] + "..." if len(content) > 500 else content,
                    "full_content": content,
                    "content_type": content_type,
                    "metadata": metadata,
                    "similarity": round(similarity, 3),
                    "importance": importance,
                    "score": round(combined_score, 3),
                    "created_at": created_at,
                    "access_count": access_count
                })

            # Sort by combined score and limit
            results.sort(key=lambda x: x["score"], reverse=True)
            results = results[:top_k]

            # Update access times for returned results
            if results:
                for r in results:
                    cursor.execute("""
                        UPDATE memories SET accessed_at = ?, access_count = access_count + 1
                        WHERE id = ?
                    """, (time.time(), r["id"]))
                conn.commit()

            return {
                "success": True,
                "query": query,
                "results": results,
                "count": len(results)
            }

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def get(self, memory_id: str) -> Dict:
        """Get a specific memory by ID."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT id, content, content_type, metadata, created_at, accessed_at,
                       access_count, importance
                FROM memories WHERE id = ?
            """, (memory_id,))

            row = cursor.fetchone()
            if not row:
                return {"success": False, "error": f"Memory '{memory_id}' not found"}

            mem_id, content, content_type, metadata_str, created_at, accessed_at, \
                access_count, importance = row

            # Update access
            cursor.execute("""
                UPDATE memories SET accessed_at = ?, access_count = access_count + 1
                WHERE id = ?
            """, (time.time(), memory_id))
            conn.commit()

            return {
                "success": True,
                "memory": {
                    "id": mem_id,
                    "content": content,
                    "content_type": content_type,
                    "metadata": json.loads(metadata_str) if metadata_str else {},
                    "created_at": created_at,
                    "accessed_at": accessed_at,
                    "access_count": access_count + 1,
                    "importance": importance
                }
            }

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def update(self, memory_id: str, content: str = None,
               metadata: Dict = None, importance: float = None) -> Dict:
        """Update a memory entry."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            # Check exists
            cursor.execute("SELECT id FROM memories WHERE id = ?", (memory_id,))
            if not cursor.fetchone():
                return {"success": False, "error": f"Memory '{memory_id}' not found"}

            updates = []
            params = []

            if content is not None:
                tokens = self._tokenize(content)
                tfidf = self._compute_tfidf(content)
                updates.extend(["content = ?", "tfidf_vector = ?"])
                params.extend([content, json.dumps(tfidf)])

            if metadata is not None:
                updates.append("metadata = ?")
                params.append(json.dumps(metadata))

            if importance is not None:
                updates.append("importance = ?")
                params.append(importance)

            if not updates:
                return {"success": False, "error": "Nothing to update"}

            updates.append("accessed_at = ?")
            params.append(time.time())
            params.append(memory_id)

            cursor.execute(f"""
                UPDATE memories SET {', '.join(updates)} WHERE id = ?
            """, params)

            conn.commit()
            return {"success": True, "message": f"Updated memory '{memory_id}'"}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def delete(self, memory_id: str) -> Dict:
        """Delete a memory entry."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("DELETE FROM memories WHERE id = ?", (memory_id,))
            if cursor.rowcount == 0:
                return {"success": False, "error": f"Memory '{memory_id}' not found"}

            self._doc_count = max(0, self._doc_count - 1)
            conn.commit()
            return {"success": True, "message": f"Deleted memory '{memory_id}'"}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def list_memories(self, content_type: str = None, limit: int = 50,
                      offset: int = 0, sort_by: str = "accessed_at") -> Dict:
        """List memories with optional filtering."""
        conn = self._get_conn()
        cursor = conn.cursor()

        valid_sorts = ["created_at", "accessed_at", "importance", "access_count"]
        if sort_by not in valid_sorts:
            sort_by = "accessed_at"

        try:
            if content_type:
                cursor.execute(f"""
                    SELECT id, content, content_type, metadata, created_at, accessed_at,
                           access_count, importance
                    FROM memories
                    WHERE content_type = ?
                    ORDER BY {sort_by} DESC
                    LIMIT ? OFFSET ?
                """, (content_type, limit, offset))
            else:
                cursor.execute(f"""
                    SELECT id, content, content_type, metadata, created_at, accessed_at,
                           access_count, importance
                    FROM memories
                    ORDER BY {sort_by} DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))

            rows = cursor.fetchall()
            memories = []

            for row in rows:
                mem_id, content, ctype, metadata_str, created_at, accessed_at, \
                    access_count, importance = row
                memories.append({
                    "id": mem_id,
                    "content": content[:200] + "..." if len(content) > 200 else content,
                    "content_type": ctype,
                    "metadata": json.loads(metadata_str) if metadata_str else {},
                    "created_at": created_at,
                    "accessed_at": accessed_at,
                    "access_count": access_count,
                    "importance": importance
                })

            return {
                "success": True,
                "memories": memories,
                "count": len(memories),
                "total": self._doc_count
            }

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def stats(self) -> Dict:
        """Get memory statistics."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT COUNT(*) FROM memories")
            total = cursor.fetchone()[0]

            cursor.execute("""
                SELECT content_type, COUNT(*) as cnt
                FROM memories
                GROUP BY content_type
                ORDER BY cnt DESC
            """)
            by_type = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("SELECT COUNT(*) FROM vocabulary")
            vocab_size = cursor.fetchone()[0]

            cursor.execute("""
                SELECT AVG(importance), AVG(access_count)
                FROM memories
            """)
            avg_importance, avg_access = cursor.fetchone()

            cursor.execute("""
                SELECT SUM(LENGTH(content)) FROM memories
            """)
            total_chars = cursor.fetchone()[0] or 0

            return {
                "success": True,
                "stats": {
                    "total_memories": total,
                    "by_type": by_type,
                    "vocabulary_size": vocab_size,
                    "avg_importance": round(avg_importance or 0, 3),
                    "avg_access_count": round(avg_access or 0, 1),
                    "total_characters": total_chars,
                    "db_path": str(self.db_path)
                }
            }

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def clear(self, content_type: str = None) -> Dict:
        """Clear memories, optionally by type."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            if content_type:
                cursor.execute("DELETE FROM memories WHERE content_type = ?", (content_type,))
                deleted = cursor.rowcount
                msg = f"Cleared {deleted} memories of type '{content_type}'"
            else:
                cursor.execute("DELETE FROM memories")
                cursor.execute("DELETE FROM vocabulary")
                deleted = cursor.rowcount
                self._vocabulary = {}
                self._doc_count = 0
                msg = f"Cleared all {deleted} memories"

            conn.commit()
            return {"success": True, "message": msg, "deleted": deleted}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def add_embedding_fn(self, fn: Callable):
        """Set embedding function for enhanced semantic search."""
        self.embedding_fn = fn


@dataclass
class ToolResult:
    """Result from a tool execution."""
    tool_id: str
    tool_name: str
    success: bool
    result: Any
    error: Optional[str] = None
    execution_time: float = 0.0
    compressed: bool = False


@dataclass
class WorkingMemory:
    """Working memory for storing intermediate results without polluting context."""
    store: Dict[str, Any] = field(default_factory=dict)
    ttls: Dict[str, int] = field(default_factory=dict)  # Turns until expiry

    def set(self, key: str, value: Any, ttl: int = 10):
        """Store a value with optional TTL (in turns)."""
        self.store[key] = value
        self.ttls[key] = ttl

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value."""
        return self.store.get(key, default)

    def delete(self, key: str):
        """Delete a value."""
        self.store.pop(key, None)
        self.ttls.pop(key, None)

    def tick(self):
        """Decrease TTLs and remove expired items."""
        expired = []
        for key in self.ttls:
            self.ttls[key] -= 1
            if self.ttls[key] <= 0:
                expired.append(key)
        for key in expired:
            self.delete(key)

    def keys(self) -> List[str]:
        return list(self.store.keys())

    def summary(self) -> Dict[str, str]:
        """Get a summary of stored items."""
        return {k: f"{type(v).__name__}({len(str(v))} chars)" for k, v in self.store.items()}


class AdvancedAgent:
    """Advanced autonomous agent with meta-tools and sophisticated execution."""

    # Default MCP servers
    DEFAULT_MCP_SERVERS = {
        "parallel": {
            "url": "http://localhost:2040",
            "description": "Parallel AI MCP - web search, extraction, long-running tasks"
        },
        "jina": {
            "url": "http://localhost:8787",
            "description": "Jina MCP - document processing, embeddings, search"
        }
    }

    def __init__(self, model: str = "claude-sonnet-5", use_openrouter: bool = None):
        # Check for OpenRouter API key
        openrouter_key = os.environ.get("OPENROUTER_API_KEY")
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY")

        # Auto-detect which service to use - prefer OpenRouter when available
        if use_openrouter is None:
            use_openrouter = bool(openrouter_key)  # Use OpenRouter if key exists

        if use_openrouter and openrouter_key:
            # Use OpenRouter with Anthropic-compatible API
            self.client = anthropic.Anthropic(
                api_key=openrouter_key,
                base_url="https://openrouter.ai/api/v1"
            )
            self.using_openrouter = True
            # Map model names for OpenRouter (Claude 4.5 models)
            model_map = {
                "claude-sonnet-5": "anthropic/claude-sonnet-4",
                "claude-sonnet-5": "anthropic/claude-3.5-sonnet",
                "claude-3-opus-20240229": "anthropic/claude-3-opus",
                "claude-3-haiku-20240307": "anthropic/claude-3-haiku",
                # Claude 4.5 models
                "sonnet": "anthropic/claude-sonnet-4.5",
                "opus": "anthropic/claude-opus-4.5",
                "haiku": "anthropic/claude-haiku-4.5",
            }
            # Default to sonnet-4.5 for OpenRouter
            if model == "claude-sonnet-5":
                model = "anthropic/claude-sonnet-4.5"
            else:
                model = model_map.get(model, model if "/" in model else f"anthropic/{model}")
        else:
            self.client = anthropic.Anthropic()
            self.using_openrouter = False

        self.runtime: Optional[UnifiedRuntime] = None
        self.conversation: List[Dict] = []
        self.model = model
        self.initialized = False
        self.memory = WorkingMemory()
        self.tool_cache: Dict[str, ToolResult] = {}  # Cache recent results
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.mcp = MCPClient()
        self.async_tasks = AsyncTaskManager()
        self.dynamic_tools = DynamicToolRegistry()  # Dynamic tool creation
        self.vector_memory = VectorMemory()  # Persistent semantic memory
        self.scheduler = TaskScheduler()  # Async task scheduler
        self.async_runtime = None  # Will be initialized after self is ready

        # Auto-capture configuration
        self.auto_capture = {
            "enabled": True,
            "capture_user_facts": True,      # Capture facts/preferences from user messages
            "capture_assistant_insights": True,  # Capture discoveries/conclusions from assistant
            "capture_tool_results": True,     # Capture significant tool results
            "min_content_length": 40,         # Minimum content length to consider
            "max_content_length": 2000,       # Maximum content to store
            "dedupe_threshold": 0.7,          # Skip if similarity > this to existing memory
        }
        self._capture_keywords = {
            "facts": ["always", "never", "prefer", "like", "use", "want", "need", "important",
                     "remember", "note that", "fyi", "btw", "my name is", "i work", "i am"],
            "insights": ["discovered", "found that", "turns out", "learned", "realized",
                        "conclusion", "summary", "result", "answer", "solution"],
            "code": ["function", "class", "def ", "import ", "const ", "let ", "var "]
        }
        self._setup_mcp_servers()
        self.base_tools = self._define_base_tools()
        self.meta_tools = self._define_meta_tools()
        self.mcp_tools = self._define_mcp_tools()
        self.dynamic_tool_management = self._define_dynamic_tool_management()
        self.memory_tools = self._define_memory_tools()
        self.scheduler_tools = self._define_scheduler_tools()
        self._update_tools_list()
        # Initialize async runtime now that self is ready
        self.async_runtime = AsyncAgentRuntime(self, self.scheduler)

    def _update_tools_list(self):
        """Update the combined tools list including dynamic tools."""
        self.tools = (
            self.base_tools +
            self.meta_tools +
            self.mcp_tools +
            self.dynamic_tool_management +
            self.memory_tools +
            self.scheduler_tools +
            self.dynamic_tools.get_tool_definitions()
        )

    def _setup_mcp_servers(self):
        """Register default MCP servers."""
        for name, config in self.DEFAULT_MCP_SERVERS.items():
            self.mcp.register_server(name, config["url"], config["description"])

    def _define_base_tools(self) -> List[Dict]:
        """Define base tools available to the agent."""
        return [
            {
                "name": "get_datetime",
                "description": "Get the current date, time, and timezone information.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "format": {"type": "string", "description": "Optional strftime format"}
                    },
                    "required": []
                }
            },
            {
                "name": "run_shell",
                "description": "Run a shell command. Use for git, curl, pip, system commands, etc.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "Shell command to execute"},
                        "timeout": {"type": "integer", "description": "Timeout in seconds (default 60)"},
                        "cwd": {"type": "string", "description": "Working directory"}
                    },
                    "required": ["command"]
                }
            },
            {
                "name": "execute_python",
                "description": "Execute Python code and return the result.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Python code to execute"},
                        "capture_result": {"type": "boolean", "description": "Capture last expression as result"}
                    },
                    "required": ["code"]
                }
            },
            {
                "name": "read_file",
                "description": "Read content from a file.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path to read"},
                        "encoding": {"type": "string", "description": "File encoding (default utf-8)"},
                        "lines": {"type": "array", "items": {"type": "integer"}, "description": "[start, end] line range"}
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "write_file",
                "description": "Write content to a file.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "File path to write"},
                        "content": {"type": "string", "description": "Content to write"},
                        "append": {"type": "boolean", "description": "Append instead of overwrite"}
                    },
                    "required": ["path", "content"]
                }
            },
            {
                "name": "list_files",
                "description": "List files in a directory with optional pattern matching.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Directory path"},
                        "pattern": {"type": "string", "description": "Glob pattern"},
                        "recursive": {"type": "boolean", "description": "Search recursively"}
                    },
                    "required": []
                }
            },
            {
                "name": "http_request",
                "description": "Make an HTTP request to fetch data from URLs or APIs.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "URL to request"},
                        "method": {"type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"]},
                        "headers": {"type": "object", "description": "Request headers"},
                        "body": {"type": "string", "description": "Request body"},
                        "json_body": {"type": "object", "description": "JSON request body"}
                    },
                    "required": ["url"]
                }
            },
            {
                "name": "search_web",
                "description": "Search the web for information.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search query"},
                        "num_results": {"type": "integer", "description": "Number of results (default 5)"}
                    },
                    "required": ["query"]
                }
            }
        ]

    def _define_meta_tools(self) -> List[Dict]:
        """Define meta-tools for sophisticated control flow."""
        return [
            {
                "name": "call_tools",
                "description": """Execute multiple tools in a batch with advanced control flow.

Supports:
- Parallel execution of independent tools
- Sequential dependencies (tool B waits for tool A)
- Piping output from one tool as input to another
- Conditional execution based on previous results

Example: Run 3 tools in parallel, then pipe results to a 4th:
{
    "calls": [
        {"id": "a", "tool": "read_file", "params": {"path": "config.json"}},
        {"id": "b", "tool": "get_datetime", "params": {}},
        {"id": "c", "tool": "run_shell", "params": {"command": "whoami"}},
        {"id": "d", "tool": "execute_python", "params": {"code": "..."}, "depends_on": ["a", "b", "c"]}
    ],
    "parallel": true
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "calls": {
                            "type": "array",
                            "description": "Array of tool calls to execute",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string", "description": "Unique ID for this call (for dependencies/piping)"},
                                    "tool": {"type": "string", "description": "Tool name to call"},
                                    "params": {"type": "object", "description": "Tool parameters"},
                                    "depends_on": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "IDs of calls that must complete first"
                                    },
                                    "pipe_from": {"type": "string", "description": "ID of call to pipe output from"},
                                    "pipe_field": {"type": "string", "description": "Field from piped result to use"},
                                    "inject_as": {"type": "string", "description": "Parameter name to inject piped value as"}
                                },
                                "required": ["id", "tool"]
                            }
                        },
                        "parallel": {"type": "boolean", "description": "Run independent calls in parallel (default true)"},
                        "stop_on_error": {"type": "boolean", "description": "Stop batch on first error (default false)"},
                        "store_as": {"type": "string", "description": "Store full results in memory under this key"}
                    },
                    "required": ["calls"]
                }
            },
            {
                "name": "chain",
                "description": """Chain tools together, piping output directly to input. Results stay in memory, not conversation.

Example: Read file -> Process with Python -> Write result
{
    "steps": [
        {"tool": "read_file", "params": {"path": "data.json"}, "extract": "content"},
        {"tool": "execute_python", "params": {"code": "import json; result = json.loads(INPUT)['key']"}, "input_var": "INPUT"},
        {"tool": "write_file", "params": {"path": "output.txt"}, "inject": {"content": "RESULT"}}
    ]
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "steps": {
                            "type": "array",
                            "description": "Sequential steps to execute",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "tool": {"type": "string", "description": "Tool name"},
                                    "params": {"type": "object", "description": "Tool parameters"},
                                    "extract": {"type": "string", "description": "JSONPath to extract from result for next step"},
                                    "input_var": {"type": "string", "description": "Variable name for previous step's output in code"},
                                    "inject": {"type": "object", "description": "Map of param names to inject previous result into"}
                                },
                                "required": ["tool"]
                            }
                        },
                        "store_as": {"type": "string", "description": "Store final result in memory under this key"},
                        "return_all": {"type": "boolean", "description": "Return all step results instead of just final"}
                    },
                    "required": ["steps"]
                }
            },
            {
                "name": "parallel_map",
                "description": """Apply a tool to multiple inputs in parallel and collect results.

Example: Read multiple files in parallel
{
    "tool": "read_file",
    "inputs": [
        {"path": "file1.py"},
        {"path": "file2.py"},
        {"path": "file3.py"}
    ],
    "max_concurrent": 5
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "tool": {"type": "string", "description": "Tool to apply to each input"},
                        "inputs": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "Array of parameter objects, one per invocation"
                        },
                        "max_concurrent": {"type": "integer", "description": "Max concurrent executions (default 5)"},
                        "stop_on_error": {"type": "boolean", "description": "Stop on first error"},
                        "store_as": {"type": "string", "description": "Store results in memory"}
                    },
                    "required": ["tool", "inputs"]
                }
            },
            {
                "name": "spawn_agent",
                "description": """Spawn a sub-agent to handle a complex subtask autonomously.

The sub-agent gets its own conversation context and can make multiple tool calls.
Only the final result is returned, keeping the main context clean.

Features:
- Configurable timeout (default 300s)
- Can run blocking (wait for result) or async (returns task_id)
- Access to MCP servers if "mcp" in tools list
- Proper timeout handling during tool execution

Use for: Complex research, multi-step file operations, tasks requiring iteration.""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task": {"type": "string", "description": "Detailed task description for sub-agent"},
                        "tools": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Tool names available to sub-agent (default: all base tools). Include 'mcp' for MCP access."
                        },
                        "max_turns": {"type": "integer", "description": "Max turns for sub-agent (default 10)"},
                        "timeout": {"type": "number", "description": "Timeout in seconds (default 300). Sub-agent will stop if exceeded."},
                        "blocking": {"type": "boolean", "description": "If true (default), wait for result. If false, return task_id for async_wait."},
                        "model": {"type": "string", "description": "Model for sub-agent (default: same as parent)"},
                        "context": {"type": "object", "description": "Additional context to pass to sub-agent"}
                    },
                    "required": ["task"]
                }
            },
            {
                "name": "remember",
                "description": "Store a value in working memory for later use. Does not add to conversation context.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "Memory key"},
                        "value": {"description": "Value to store (any JSON type)"},
                        "ttl": {"type": "integer", "description": "Turns until expiry (default 10)"}
                    },
                    "required": ["key", "value"]
                }
            },
            {
                "name": "recall",
                "description": "Recall a value from working memory.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "Memory key to recall"},
                        "delete": {"type": "boolean", "description": "Delete after recalling"}
                    },
                    "required": ["key"]
                }
            },
            {
                "name": "memory_status",
                "description": "Get status of working memory - what's stored and TTLs.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "think",
                "description": """Internal reasoning step. Use this to think through complex problems without taking action.

The thinking is recorded but doesn't trigger tool execution. Use before complex tool orchestration.""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "reasoning": {"type": "string", "description": "Your reasoning/analysis"}
                    },
                    "required": ["reasoning"]
                }
            },
            {
                "name": "conditional",
                "description": """Execute a tool conditionally based on a check.

Example: Only delete file if it exists
{
    "check": {"tool": "run_shell", "params": {"command": "test -f myfile.txt && echo 'exists'"}},
    "condition": "stdout contains 'exists'",
    "if_true": {"tool": "run_shell", "params": {"command": "rm myfile.txt"}},
    "if_false": {"tool": "think", "params": {"reasoning": "File doesn't exist, nothing to delete"}}
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "check": {
                            "type": "object",
                            "description": "Tool call to check condition",
                            "properties": {
                                "tool": {"type": "string"},
                                "params": {"type": "object"}
                            },
                            "required": ["tool"]
                        },
                        "condition": {"type": "string", "description": "Condition to evaluate (e.g., 'success', 'stdout contains X', 'result.field == Y')"},
                        "if_true": {
                            "type": "object",
                            "description": "Tool call if condition is true",
                            "properties": {
                                "tool": {"type": "string"},
                                "params": {"type": "object"}
                            }
                        },
                        "if_false": {
                            "type": "object",
                            "description": "Tool call if condition is false",
                            "properties": {
                                "tool": {"type": "string"},
                                "params": {"type": "object"}
                            }
                        }
                    },
                    "required": ["check", "condition"]
                }
            }
        ]

    def _define_mcp_tools(self) -> List[Dict]:
        """Define MCP server integration tools."""
        return [
            {
                "name": "mcp_call",
                "description": """Call a tool on an MCP server.

Available servers:
- parallel (localhost:2040): Parallel AI - web search, extraction, research tasks
- jina (localhost:8787): Jina - document processing, embeddings, semantic search

Examples:
- Search: {"server": "parallel", "tool": "search", "params": {"query": "python async"}}
- Extract: {"server": "parallel", "tool": "extract", "params": {"url": "https://..."}}
- Async task: {"server": "parallel", "tool": "research", "params": {...}, "async_mode": true, "timeout": 300}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "server": {
                            "type": "string",
                            "description": "MCP server name (parallel, jina, or custom)"
                        },
                        "tool": {
                            "type": "string",
                            "description": "Tool name to call on the server"
                        },
                        "params": {
                            "type": "object",
                            "description": "Parameters for the tool"
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Timeout in seconds (default 60, max 600)"
                        },
                        "async_mode": {
                            "type": "boolean",
                            "description": "Submit as async task, returns task_id for later retrieval"
                        }
                    },
                    "required": ["server", "tool"]
                }
            },
            {
                "name": "mcp_wait",
                "description": "Wait for an async MCP task to complete.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID from async mcp_call"
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Max time to wait in seconds (default 300)"
                        },
                        "poll_interval": {
                            "type": "number",
                            "description": "How often to check status in seconds (default 2)"
                        }
                    },
                    "required": ["task_id"]
                }
            },
            {
                "name": "mcp_servers",
                "description": "List available MCP servers and their status.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "connect": {
                            "type": "string",
                            "description": "Server name to connect to and discover tools"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "async_submit",
                "description": """Submit a long-running task for background execution.

Use this for operations that may take a long time (web scraping, large file processing, etc).
Returns a task_id immediately, use async_wait to get the result.""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "tool": {
                            "type": "string",
                            "description": "Tool name to execute"
                        },
                        "params": {
                            "type": "object",
                            "description": "Tool parameters"
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Task timeout in seconds (default 300)"
                        },
                        "name": {
                            "type": "string",
                            "description": "Human-readable name for the task"
                        }
                    },
                    "required": ["tool", "params"]
                }
            },
            {
                "name": "async_wait",
                "description": "Wait for an async task to complete and get its result.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID from async_submit"
                        },
                        "timeout": {
                            "type": "number",
                            "description": "Max wait time in seconds"
                        }
                    },
                    "required": ["task_id"]
                }
            },
            {
                "name": "async_status",
                "description": "Check status of async tasks without waiting.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Specific task ID to check (optional, lists all if omitted)"
                        }
                    },
                    "required": []
                }
            }
        ]

    def _define_dynamic_tool_management(self) -> List[Dict]:
        """Define tools for creating and managing dynamic tools."""
        return [
            {
                "name": "create_tool",
                "description": """Create a new tool that becomes immediately available.

Write Python code that defines an 'execute(params)' function.
The function receives a dict of parameters and should return a result.

Available imports in tool code: json, time, Path, os, re, math, datetime, urllib, subprocess

Example - Create a tool that counts words:
{
    "name": "word_counter",
    "description": "Count words in text",
    "code": "def execute(params):\\n    text = params.get('text', '')\\n    words = text.split()\\n    return {'word_count': len(words), 'char_count': len(text)}",
    "schema": {
        "type": "object",
        "properties": {
            "text": {"type": "string", "description": "Text to count words in"}
        },
        "required": ["text"]
    }
}

Example - Create a tool that fetches JSON from URL:
{
    "name": "fetch_json",
    "description": "Fetch and parse JSON from a URL",
    "code": "def execute(params):\\n    import urllib.request\\n    url = params.get('url')\\n    with urllib.request.urlopen(url, timeout=10) as r:\\n        return json.loads(r.read().decode())",
    "schema": {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "URL to fetch JSON from"}
        },
        "required": ["url"]
    }
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Unique tool name (alphanumeric and underscores)"
                        },
                        "description": {
                            "type": "string",
                            "description": "What the tool does"
                        },
                        "code": {
                            "type": "string",
                            "description": "Python code defining execute(params) function"
                        },
                        "schema": {
                            "type": "object",
                            "description": "JSON Schema for input parameters"
                        }
                    },
                    "required": ["name", "description", "code", "schema"]
                }
            },
            {
                "name": "edit_tool",
                "description": "Edit an existing dynamic tool. Can update description, code, or schema.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name to edit"},
                        "description": {"type": "string", "description": "New description (optional)"},
                        "code": {"type": "string", "description": "New code (optional)"},
                        "schema": {"type": "object", "description": "New schema (optional)"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "delete_tool",
                "description": "Delete a dynamic tool.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name to delete"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "list_dynamic_tools",
                "description": "List all dynamically created tools.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "get_tool_code",
                "description": "Get the source code of a dynamic tool.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "enable_tool",
                "description": "Enable a disabled dynamic tool.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name to enable"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "disable_tool",
                "description": "Disable a dynamic tool without deleting it.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Tool name to disable"}
                    },
                    "required": ["name"]
                }
            }
        ]

    def _define_memory_tools(self) -> List[Dict]:
        """Define persistent vector memory tools."""
        return [
            {
                "name": "memory_store",
                "description": """Store information in persistent semantic memory.

Memory persists across sessions and can be searched semantically.
Use for: facts, code snippets, conversation highlights, research findings.

Content types: 'note', 'fact', 'code', 'conversation', 'result', 'research', 'todo'

Example:
{
    "content": "The user prefers dark mode and uses vim keybindings",
    "content_type": "fact",
    "importance": 0.8,
    "metadata": {"category": "user_preferences", "source": "conversation"}
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "content": {
                            "type": "string",
                            "description": "Content to store in memory"
                        },
                        "content_type": {
                            "type": "string",
                            "description": "Type: note, fact, code, conversation, result, research, todo",
                            "enum": ["note", "fact", "code", "conversation", "result", "research", "todo"]
                        },
                        "importance": {
                            "type": "number",
                            "description": "Importance score 0-1 (higher = more important, default 0.5)"
                        },
                        "metadata": {
                            "type": "object",
                            "description": "Optional metadata (tags, source, category, etc.)"
                        },
                        "memory_id": {
                            "type": "string",
                            "description": "Optional custom ID (auto-generated if not provided)"
                        }
                    },
                    "required": ["content"]
                }
            },
            {
                "name": "memory_search",
                "description": """Search persistent memory semantically.

Uses TF-IDF and BM25 for intelligent matching. Results are ranked by:
- Semantic similarity (60%)
- Importance score (20%)
- Recency (20%)

Example:
{
    "query": "user preferences settings",
    "content_types": ["fact", "note"],
    "top_k": 5
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query (semantic search)"
                        },
                        "top_k": {
                            "type": "integer",
                            "description": "Number of results (default 10)"
                        },
                        "content_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Filter by content types"
                        },
                        "min_importance": {
                            "type": "number",
                            "description": "Minimum importance score (0-1)"
                        },
                        "min_similarity": {
                            "type": "number",
                            "description": "Minimum similarity score (0-1, default 0.1)"
                        },
                        "metadata_filter": {
                            "type": "object",
                            "description": "Filter by metadata fields (exact match)"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "memory_get",
                "description": "Get a specific memory by its ID.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {
                            "type": "string",
                            "description": "Memory ID to retrieve"
                        }
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "memory_list",
                "description": "List memories with optional filtering and pagination.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "content_type": {
                            "type": "string",
                            "description": "Filter by content type"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max results (default 50)"
                        },
                        "offset": {
                            "type": "integer",
                            "description": "Skip first N results"
                        },
                        "sort_by": {
                            "type": "string",
                            "description": "Sort by: created_at, accessed_at, importance, access_count",
                            "enum": ["created_at", "accessed_at", "importance", "access_count"]
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "memory_update",
                "description": "Update an existing memory entry.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {
                            "type": "string",
                            "description": "Memory ID to update"
                        },
                        "content": {
                            "type": "string",
                            "description": "New content (optional)"
                        },
                        "metadata": {
                            "type": "object",
                            "description": "New metadata (optional)"
                        },
                        "importance": {
                            "type": "number",
                            "description": "New importance score (optional)"
                        }
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "memory_delete",
                "description": "Delete a memory entry by ID.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "memory_id": {
                            "type": "string",
                            "description": "Memory ID to delete"
                        }
                    },
                    "required": ["memory_id"]
                }
            },
            {
                "name": "memory_stats",
                "description": "Get statistics about the persistent memory store.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "memory_clear",
                "description": "Clear memories. Use with caution!",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "content_type": {
                            "type": "string",
                            "description": "Only clear memories of this type (optional, clears all if omitted)"
                        },
                        "confirm": {
                            "type": "boolean",
                            "description": "Must be true to confirm deletion"
                        }
                    },
                    "required": ["confirm"]
                }
            }
        ]

    def _define_scheduler_tools(self) -> List[Dict]:
        """Define async task scheduler tools."""
        return [
            {
                "name": "schedule_task",
                "description": """Schedule a task for future or recurring execution.

Task types:
- 'once': Run once after delay (default)
- 'interval': Run repeatedly (e.g., '5m', '1h', '30s')
- 'cron': Cron expression (e.g., '*/5 * * * *' = every 5 min)
- 'event': Triggered by named events

Handler can be:
- Tool name: 'get_datetime', 'execute_python', etc.
- Agent task: 'agent:analyze the current system status'

Examples:
{
    "name": "hourly_report",
    "handler": "agent:generate a status summary",
    "task_type": "interval",
    "schedule": "1h"
}
{
    "name": "on_deploy",
    "handler": "execute_python",
    "params": {"code": "print('Deployment detected!')"},
    "task_type": "event",
    "event_trigger": "deployment"
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Human-readable task name"
                        },
                        "handler": {
                            "type": "string",
                            "description": "Tool name or 'agent:task description'"
                        },
                        "task_type": {
                            "type": "string",
                            "enum": ["once", "interval", "cron", "event"],
                            "description": "Type of scheduling (default: once)"
                        },
                        "schedule": {
                            "type": "string",
                            "description": "For interval: '5m', '1h'. For cron: '*/5 * * * *'"
                        },
                        "params": {
                            "type": "object",
                            "description": "Parameters to pass to handler"
                        },
                        "delay": {
                            "type": "number",
                            "description": "Initial delay in seconds for 'once' tasks"
                        },
                        "priority": {
                            "type": "integer",
                            "description": "1-10, lower = higher priority (default 5)"
                        },
                        "max_runs": {
                            "type": "integer",
                            "description": "Max executions (null = unlimited)"
                        },
                        "event_trigger": {
                            "type": "string",
                            "description": "Event name to trigger on (for 'event' type)"
                        },
                        "metadata": {
                            "type": "object",
                            "description": "Additional task metadata"
                        }
                    },
                    "required": ["name", "handler"]
                }
            },
            {
                "name": "cancel_task",
                "description": "Cancel a scheduled task by ID.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to cancel"
                        }
                    },
                    "required": ["task_id"]
                }
            },
            {
                "name": "pause_task",
                "description": "Pause a scheduled task (can be resumed later).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to pause"
                        }
                    },
                    "required": ["task_id"]
                }
            },
            {
                "name": "resume_task",
                "description": "Resume a paused task.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to resume"
                        }
                    },
                    "required": ["task_id"]
                }
            },
            {
                "name": "list_scheduled",
                "description": "List all scheduled tasks with optional filtering.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "enum": ["pending", "running", "paused", "completed", "failed"],
                            "description": "Filter by status"
                        },
                        "task_type": {
                            "type": "string",
                            "enum": ["once", "interval", "cron", "event"],
                            "description": "Filter by task type"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_scheduled_task",
                "description": "Get details of a specific scheduled task.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to retrieve"
                        }
                    },
                    "required": ["task_id"]
                }
            },
            {
                "name": "emit_event",
                "description": """Emit a named event to trigger subscribed tasks.

Tasks scheduled with task_type='event' and matching event_trigger will run.

Example:
{
    "event_name": "deployment",
    "event_data": {"version": "1.2.0", "environment": "production"}
}""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "event_name": {
                            "type": "string",
                            "description": "Name of the event to emit"
                        },
                        "event_data": {
                            "type": "object",
                            "description": "Data to pass to triggered tasks"
                        }
                    },
                    "required": ["event_name"]
                }
            },
            {
                "name": "scheduler_history",
                "description": "Get execution history for scheduled tasks.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Filter by specific task ID (optional)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Max results (default 50)"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "scheduler_stats",
                "description": "Get scheduler statistics and status.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "run_task_now",
                "description": "Run a scheduled task immediately (in addition to its schedule).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "task_id": {
                            "type": "string",
                            "description": "Task ID to run"
                        }
                    },
                    "required": ["task_id"]
                }
            }
        ]

    def initialize(self):
        """Initialize the unified runtime and MCP connections."""
        print("\033[90mInitializing advanced agent systems...\033[0m")
        # Show which API backend is being used
        if self.using_openrouter:
            print(f"\033[90mAPI: OpenRouter ({self.model})\033[0m")
        else:
            print(f"\033[90mAPI: Anthropic ({self.model})\033[0m")
        self.runtime = UnifiedRuntime()
        result = self.runtime.initialize()
        self.initialized = True
        systems = list(result.get('systems', {}).keys())
        print(f"\033[90mSystems ready: {', '.join(systems)}\033[0m")
        print(f"\033[90mMeta-tools: call_tools, chain, parallel_map, spawn_agent, conditional\033[0m")
        print(f"\033[90mAsync tools: async_submit, async_wait, async_status\033[0m")
        print(f"\033[90mDynamic tools: create_tool, edit_tool, delete_tool, list_dynamic_tools\033[0m")
        print(f"\033[90mMemory tools: memory_store, memory_search, memory_list, memory_get, memory_stats\033[0m")
        print(f"\033[90mScheduler tools: schedule_task, emit_event, list_scheduled, run_task_now\033[0m")

        # Start async runtime
        if self.async_runtime:
            self.async_runtime.start()
            print(f"\033[90mAsync scheduler: running (background thread)\033[0m")

        # Show scheduler stats
        sched_stats = self.scheduler.stats()
        if sched_stats.get("success"):
            stats = sched_stats.get("stats", {})
            active = stats.get("active_tasks", 0)
            events = stats.get("registered_events", [])
            if active > 0 or events:
                event_info = f", events: {', '.join(events[:3])}" if events else ""
                print(f"\033[90mScheduler: {active} active tasks{event_info}\033[0m")

        # Show loaded dynamic tools
        dyn_count = len(self.dynamic_tools.tools)
        if dyn_count > 0:
            dyn_names = list(self.dynamic_tools.tools.keys())[:5]
            suffix = f"... +{dyn_count - 5} more" if dyn_count > 5 else ""
            print(f"\033[90mLoaded {dyn_count} custom tools: {', '.join(dyn_names)}{suffix}\033[0m")

        # Show persistent memory stats
        mem_stats = self.vector_memory.stats()
        if mem_stats.get("success"):
            stats = mem_stats.get("stats", {})
            mem_count = stats.get("total_memories", 0)
            if mem_count > 0:
                by_type = stats.get("by_type", {})
                type_info = ", ".join(f"{k}:{v}" for k, v in list(by_type.items())[:4])
                print(f"\033[90mPersistent memory: {mem_count} entries ({type_info})\033[0m")

        # Try to connect to MCP servers
        mcp_status = []
        for name in self.mcp.servers:
            try:
                conn_result = self.mcp.connect(name)
                if conn_result.get("success"):
                    tool_count = conn_result.get("tool_count", 0)
                    mcp_status.append(f"{name}({tool_count} tools)")
                else:
                    mcp_status.append(f"{name}(offline)")
            except:
                mcp_status.append(f"{name}(error)")

        if mcp_status:
            print(f"\033[90mMCP servers: {', '.join(mcp_status)}\033[0m")

        return result

    def _execute_base_tool(self, name: str, params: Dict) -> Dict:
        """Execute a base tool and return structured result."""
        start_time = time.time()
        try:
            if name == "get_datetime":
                from datetime import datetime
                fmt = params.get("format")
                now = datetime.now()
                result = {
                    "datetime": now.strftime(fmt) if fmt else now.isoformat(),
                    "date": now.strftime("%Y-%m-%d"),
                    "time": now.strftime("%H:%M:%S"),
                    "timezone": time.tzname[0],
                    "timestamp": now.timestamp(),
                    "weekday": now.strftime("%A")
                }
                return {"success": True, "result": result}

            elif name == "run_shell":
                import subprocess
                command = params.get("command", "")
                timeout = params.get("timeout", 60)
                cwd = params.get("cwd")
                result = subprocess.run(
                    command, shell=True, capture_output=True, text=True,
                    timeout=timeout, cwd=cwd
                )
                return {
                    "success": result.returncode == 0,
                    "result": {
                        "returncode": result.returncode,
                        "stdout": result.stdout[:10000],
                        "stderr": result.stderr[:2000] if result.stderr else None
                    }
                }

            elif name == "execute_python":
                code = params.get("code", "")
                capture = params.get("capture_result", False)
                import io, contextlib
                stdout = io.StringIO()
                exec_globals = {"__builtins__": __builtins__}
                try:
                    with contextlib.redirect_stdout(stdout):
                        if capture:
                            # Try to capture last expression
                            lines = code.strip().split('\n')
                            if lines:
                                exec('\n'.join(lines[:-1]), exec_globals)
                                try:
                                    result_val = eval(lines[-1], exec_globals)
                                    exec_globals['_result'] = result_val
                                except:
                                    exec(lines[-1], exec_globals)
                        else:
                            exec(code, exec_globals)
                    return {
                        "success": True,
                        "result": {
                            "output": stdout.getvalue(),
                            "captured": exec_globals.get('_result')
                        }
                    }
                except Exception as e:
                    return {"success": False, "error": str(e), "result": {"output": stdout.getvalue()}}

            elif name == "read_file":
                path = Path(params.get("path", ""))
                encoding = params.get("encoding", "utf-8")
                line_range = params.get("lines")
                if not path.exists():
                    return {"success": False, "error": "File not found", "result": None}
                content = path.read_text(encoding=encoding)
                if line_range:
                    lines = content.split('\n')
                    start, end = line_range[0], line_range[1] if len(line_range) > 1 else len(lines)
                    content = '\n'.join(lines[start:end])
                return {
                    "success": True,
                    "result": {"content": content[:50000], "size": len(content), "truncated": len(content) > 50000}
                }

            elif name == "write_file":
                path = Path(params.get("path", ""))
                content = params.get("content", "")
                append = params.get("append", False)
                path.parent.mkdir(parents=True, exist_ok=True)
                mode = 'a' if append else 'w'
                path.open(mode).write(content)
                return {"success": True, "result": {"path": str(path), "bytes": len(content)}}

            elif name == "list_files":
                path = Path(params.get("path", "."))
                pattern = params.get("pattern", "*")
                recursive = params.get("recursive", False)
                if not path.is_dir():
                    return {"success": False, "error": "Not a directory", "result": None}
                if recursive:
                    files = [str(f) for f in path.rglob(pattern)][:100]
                else:
                    files = [str(f) for f in path.glob(pattern)][:100]
                return {"success": True, "result": {"files": files, "count": len(files)}}

            elif name == "http_request":
                import urllib.request, urllib.error
                url = params.get("url", "")
                method = params.get("method", "GET")
                headers = params.get("headers", {})
                body = params.get("body")
                json_body = params.get("json_body")

                if json_body:
                    body = json.dumps(json_body)
                    headers["Content-Type"] = "application/json"

                req = urllib.request.Request(url, method=method, headers=headers)
                if body:
                    req.data = body.encode()

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        content = response.read().decode('utf-8', errors='replace')
                        return {
                            "success": True,
                            "result": {
                                "status": response.status,
                                "headers": dict(response.headers),
                                "body": content[:20000],
                                "truncated": len(content) > 20000
                            }
                        }
                except urllib.error.HTTPError as e:
                    return {"success": False, "error": f"HTTP {e.code}: {e.reason}", "result": {"status": e.code}}

            elif name == "search_web":
                import urllib.request, urllib.parse
                query = params.get("query", "")
                url = f"https://api.duckduckgo.com/?q={urllib.parse.quote(query)}&format=json&no_html=1"
                try:
                    with urllib.request.urlopen(url, timeout=10) as response:
                        data = json.loads(response.read().decode())
                        return {
                            "success": True,
                            "result": {
                                "abstract": data.get("Abstract", ""),
                                "answer": data.get("Answer", ""),
                                "related": [t.get("Text", "") for t in data.get("RelatedTopics", [])[:5] if isinstance(t, dict)]
                            }
                        }
                except Exception as e:
                    return {"success": False, "error": str(e), "result": None}

            else:
                return {"success": False, "error": f"Unknown base tool: {name}", "result": None}

        except Exception as e:
            return {"success": False, "error": str(e), "result": None}

    def _execute_meta_tool(self, name: str, params: Dict) -> Dict:
        """Execute a meta-tool."""

        if name == "call_tools":
            return self._execute_call_tools(params)
        elif name == "chain":
            return self._execute_chain(params)
        elif name == "parallel_map":
            return self._execute_parallel_map(params)
        elif name == "spawn_agent":
            return self._execute_spawn_agent(params)
        elif name == "remember":
            key = params.get("key")
            value = params.get("value")
            ttl = params.get("ttl", 10)
            self.memory.set(key, value, ttl)
            return {"success": True, "result": {"stored": key, "ttl": ttl}}
        elif name == "recall":
            key = params.get("key")
            delete = params.get("delete", False)
            value = self.memory.get(key)
            if delete:
                self.memory.delete(key)
            if value is None:
                return {"success": False, "error": f"Key '{key}' not found in memory", "result": None}
            return {"success": True, "result": {"key": key, "value": value}}
        elif name == "memory_status":
            return {
                "success": True,
                "result": {
                    "keys": self.memory.keys(),
                    "summary": self.memory.summary(),
                    "ttls": self.memory.ttls.copy()
                }
            }
        elif name == "think":
            reasoning = params.get("reasoning", "")
            return {"success": True, "result": {"thought": reasoning}}
        elif name == "conditional":
            return self._execute_conditional(params)
        # MCP tools
        elif name == "mcp_call":
            server = params.get("server")
            tool = params.get("tool")
            tool_params = params.get("params", {})
            timeout = min(params.get("timeout", 60), 600)  # Max 10 min
            async_mode = params.get("async_mode", False)
            return self.mcp.call_tool(server, tool, tool_params, timeout, async_mode)
        elif name == "mcp_wait":
            task_id = params.get("task_id")
            timeout = params.get("timeout", 300)
            poll_interval = params.get("poll_interval", 2)
            return self.mcp.wait_task(task_id, timeout, poll_interval)
        elif name == "mcp_servers":
            connect = params.get("connect")
            if connect:
                return self.mcp.connect(connect)
            return self.mcp.list_servers()
        # Async task tools
        elif name == "async_submit":
            tool = params.get("tool")
            tool_params = params.get("params", {})
            timeout = params.get("timeout", 300)
            task_name = params.get("name", f"async_{tool}")

            # Determine execution function
            base_tool_names = [t["name"] for t in self.base_tools]
            if tool in base_tool_names:
                func = partial(self._execute_base_tool, tool, tool_params)
            else:
                func = partial(self._execute_meta_tool, tool, tool_params)

            task_id = self.async_tasks.submit(task_name, func, timeout=timeout)
            return {
                "success": True,
                "result": {
                    "task_id": task_id,
                    "name": task_name,
                    "status": "submitted",
                    "message": "Use async_wait to get result"
                }
            }
        elif name == "async_wait":
            task_id = params.get("task_id")
            timeout = params.get("timeout")
            return self.async_tasks.wait(task_id, timeout)
        elif name == "async_status":
            task_id = params.get("task_id")
            if task_id:
                return self.async_tasks.get_status(task_id)
            return {"success": True, "result": self.async_tasks.list_tasks()}
        # Dynamic tool management
        elif name == "create_tool":
            result = self.dynamic_tools.create(
                name=params.get("name"),
                description=params.get("description"),
                code=params.get("code"),
                schema=params.get("schema", {"type": "object", "properties": {}, "required": []})
            )
            if result.get("success"):
                self._update_tools_list()  # Refresh tools list
            return result
        elif name == "edit_tool":
            result = self.dynamic_tools.edit(
                name=params.get("name"),
                description=params.get("description"),
                code=params.get("code"),
                schema=params.get("schema")
            )
            if result.get("success"):
                self._update_tools_list()
            return result
        elif name == "delete_tool":
            result = self.dynamic_tools.delete(params.get("name"))
            if result.get("success"):
                self._update_tools_list()
            return result
        elif name == "list_dynamic_tools":
            return {"success": True, "result": self.dynamic_tools.list_tools()}
        elif name == "get_tool_code":
            return self.dynamic_tools.get_code(params.get("name"))
        elif name == "enable_tool":
            result = self.dynamic_tools.enable(params.get("name"))
            if result.get("success"):
                self._update_tools_list()
            return result
        elif name == "disable_tool":
            result = self.dynamic_tools.disable(params.get("name"))
            if result.get("success"):
                self._update_tools_list()
            return result
        # Persistent memory tools
        elif name == "memory_store":
            return self.vector_memory.store(
                content=params.get("content", ""),
                content_type=params.get("content_type", "note"),
                metadata=params.get("metadata"),
                importance=params.get("importance", 0.5),
                memory_id=params.get("memory_id")
            )
        elif name == "memory_search":
            return self.vector_memory.search(
                query=params.get("query", ""),
                top_k=params.get("top_k", 10),
                content_types=params.get("content_types"),
                min_importance=params.get("min_importance", 0.0),
                min_similarity=params.get("min_similarity", 0.1),
                metadata_filter=params.get("metadata_filter")
            )
        elif name == "memory_get":
            return self.vector_memory.get(params.get("memory_id", ""))
        elif name == "memory_list":
            return self.vector_memory.list_memories(
                content_type=params.get("content_type"),
                limit=params.get("limit", 50),
                offset=params.get("offset", 0),
                sort_by=params.get("sort_by", "accessed_at")
            )
        elif name == "memory_update":
            return self.vector_memory.update(
                memory_id=params.get("memory_id", ""),
                content=params.get("content"),
                metadata=params.get("metadata"),
                importance=params.get("importance")
            )
        elif name == "memory_delete":
            return self.vector_memory.delete(params.get("memory_id", ""))
        elif name == "memory_stats":
            return self.vector_memory.stats()
        elif name == "memory_clear":
            if not params.get("confirm"):
                return {"success": False, "error": "Must set confirm=true to clear memory"}
            return self.vector_memory.clear(content_type=params.get("content_type"))
        # Scheduler tools
        elif name == "schedule_task":
            return self.scheduler.schedule_task(
                name=params.get("name", "unnamed"),
                handler=params.get("handler", ""),
                task_type=params.get("task_type", "once"),
                schedule=params.get("schedule"),
                params=params.get("params", {}),
                delay=params.get("delay", 0),
                priority=params.get("priority", 5),
                max_runs=params.get("max_runs"),
                metadata=params.get("metadata"),
                event_trigger=params.get("event_trigger")
            )
        elif name == "cancel_task":
            return self.scheduler.cancel_task(params.get("task_id", ""))
        elif name == "pause_task":
            return self.scheduler.pause_task(params.get("task_id", ""))
        elif name == "resume_task":
            return self.scheduler.resume_task(params.get("task_id", ""))
        elif name == "list_scheduled":
            return self.scheduler.list_tasks(
                status=params.get("status"),
                task_type=params.get("task_type")
            )
        elif name == "get_scheduled_task":
            return self.scheduler.get_task(params.get("task_id", ""))
        elif name == "emit_event":
            return self.scheduler.emit_event(
                event_name=params.get("event_name", ""),
                event_data=params.get("event_data")
            )
        elif name == "scheduler_history":
            return self.scheduler.get_history(
                task_id=params.get("task_id"),
                limit=params.get("limit", 50)
            )
        elif name == "scheduler_stats":
            return self.scheduler.stats()
        elif name == "run_task_now":
            task_id = params.get("task_id", "")
            if self.async_runtime and self.async_runtime.running:
                # Run async through runtime
                import asyncio
                future = asyncio.run_coroutine_threadsafe(
                    self.async_runtime.run_task_now(task_id),
                    self.async_runtime._loop
                )
                try:
                    return future.result(timeout=60)
                except Exception as e:
                    return {"success": False, "error": str(e)}
            else:
                # Run synchronously if runtime not started
                task_info = self.scheduler.get_task(task_id)
                if not task_info.get("success"):
                    return task_info
                task = task_info["task"]
                self.scheduler.mark_running(task_id)
                try:
                    result = json.loads(self.execute_tool(task["handler"], task["params"]))
                    self.scheduler.mark_completed(task_id, result=result)
                    return {"success": True, "result": result}
                except Exception as e:
                    self.scheduler.mark_completed(task_id, error=str(e))
                    return {"success": False, "error": str(e)}
        else:
            return {"success": False, "error": f"Unknown meta-tool: {name}", "result": None}

    def _execute_call_tools(self, params: Dict) -> Dict:
        """Execute multiple tools with dependencies and parallel execution."""
        calls = params.get("calls", [])
        parallel = params.get("parallel", True)
        stop_on_error = params.get("stop_on_error", False)
        store_as = params.get("store_as")

        results = {}
        completed = set()

        def can_execute(call: Dict) -> bool:
            deps = call.get("depends_on", [])
            return all(d in completed for d in deps)

        def execute_call(call: Dict) -> Tuple[str, Dict]:
            call_id = call["id"]
            tool = call["tool"]
            tool_params = dict(call.get("params", {}))

            # Handle piping
            if "pipe_from" in call and call["pipe_from"] in results:
                piped_result = results[call["pipe_from"]]
                pipe_field = call.get("pipe_field", "result")
                inject_as = call.get("inject_as", "input")

                # Extract field from piped result
                value = piped_result.get("result", {})
                if pipe_field and isinstance(value, dict):
                    value = value.get(pipe_field, value)
                tool_params[inject_as] = value

            # Execute
            if tool in [t["name"] for t in self.base_tools]:
                result = self._execute_base_tool(tool, tool_params)
            else:
                result = self._execute_meta_tool(tool, tool_params)

            return call_id, result

        # Topological execution with parallel where possible
        pending = list(calls)

        while pending:
            # Find all calls that can execute now
            ready = [c for c in pending if can_execute(c)]

            if not ready:
                # Circular dependency or error
                break

            if parallel and len(ready) > 1:
                # Execute ready calls in parallel
                futures = {self.executor.submit(execute_call, c): c for c in ready}
                for future in as_completed(futures):
                    call_id, result = future.result()
                    results[call_id] = result
                    completed.add(call_id)
                    if stop_on_error and not result.get("success"):
                        pending = []
                        break
            else:
                # Execute sequentially
                for call in ready:
                    call_id, result = execute_call(call)
                    results[call_id] = result
                    completed.add(call_id)
                    if stop_on_error and not result.get("success"):
                        break

            pending = [c for c in pending if c["id"] not in completed]

        if store_as:
            self.memory.set(store_as, results)

        return {
            "success": all(r.get("success") for r in results.values()),
            "result": {
                "completed": list(completed),
                "results": results
            }
        }

    def _execute_chain(self, params: Dict) -> Dict:
        """Execute a chain of tools, piping output to input."""
        steps = params.get("steps", [])
        store_as = params.get("store_as")
        return_all = params.get("return_all", False)

        all_results = []
        prev_result = None

        for i, step in enumerate(steps):
            tool = step["tool"]
            tool_params = dict(step.get("params", {}))

            # Inject previous result
            if prev_result is not None:
                input_var = step.get("input_var", "INPUT")
                inject_map = step.get("inject", {})

                # For execute_python, replace INPUT variable in code
                if tool == "execute_python" and "code" in tool_params:
                    if isinstance(prev_result, str):
                        tool_params["code"] = f'{input_var} = """{prev_result}"""\n' + tool_params["code"]
                    else:
                        tool_params["code"] = f'{input_var} = {json.dumps(prev_result)}\n' + tool_params["code"]

                # Inject into specific params
                for param_name, placeholder in inject_map.items():
                    if placeholder == "RESULT" or placeholder == "INPUT":
                        tool_params[param_name] = prev_result

            # Execute step
            if tool in [t["name"] for t in self.base_tools]:
                result = self._execute_base_tool(tool, tool_params)
            else:
                result = self._execute_meta_tool(tool, tool_params)

            all_results.append({"step": i, "tool": tool, "result": result})

            if not result.get("success"):
                return {
                    "success": False,
                    "error": f"Chain failed at step {i} ({tool}): {result.get('error')}",
                    "result": {"steps_completed": i, "all_results": all_results} if return_all else None
                }

            # Extract for next step
            extract = step.get("extract")
            if extract and isinstance(result.get("result"), dict):
                prev_result = result["result"].get(extract, result["result"])
            else:
                prev_result = result.get("result")

        final_result = {
            "success": True,
            "result": all_results if return_all else prev_result
        }

        if store_as:
            self.memory.set(store_as, final_result["result"])

        return final_result

    def _execute_parallel_map(self, params: Dict) -> Dict:
        """Apply a tool to multiple inputs in parallel."""
        tool = params.get("tool")
        inputs = params.get("inputs", [])
        max_concurrent = params.get("max_concurrent", 5)
        stop_on_error = params.get("stop_on_error", False)
        store_as = params.get("store_as")

        results = []

        def execute_one(tool_params: Dict, idx: int) -> Tuple[int, Dict]:
            if tool in [t["name"] for t in self.base_tools]:
                return idx, self._execute_base_tool(tool, tool_params)
            else:
                return idx, self._execute_meta_tool(tool, tool_params)

        # Execute with limited concurrency
        with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
            futures = {pool.submit(execute_one, inp, i): i for i, inp in enumerate(inputs)}
            results = [None] * len(inputs)

            for future in as_completed(futures):
                idx, result = future.result()
                results[idx] = result

                if stop_on_error and not result.get("success"):
                    pool.shutdown(wait=False)
                    break

        final = {
            "success": all(r.get("success") for r in results if r),
            "result": {
                "count": len([r for r in results if r]),
                "results": results
            }
        }

        if store_as:
            self.memory.set(store_as, final["result"])

        return final

    def _subagent_api_call(self, model: str, system_prompt: str,
                            messages: List[Dict], tools: List[Dict]) -> Dict:
        """Make an API call for sub-agent, supporting both Anthropic and OpenRouter."""
        if self.using_openrouter:
            import urllib.request
            import urllib.error

            # Convert tools to OpenAI format
            openai_tools = []
            for tool in tools:
                openai_tools.append({
                    "type": "function",
                    "function": {
                        "name": tool["name"],
                        "description": tool.get("description", ""),
                        "parameters": tool.get("input_schema", {"type": "object", "properties": {}})
                    }
                })

            # Build messages with system prompt
            api_messages = [{"role": "system", "content": system_prompt}]
            for msg in messages:
                if msg["role"] == "user":
                    content = msg["content"]
                    if isinstance(content, list):
                        # Handle tool results
                        for item in content:
                            if item.get("type") == "tool_result":
                                api_messages.append({
                                    "role": "tool",
                                    "tool_call_id": item["tool_use_id"],
                                    "content": item["content"]
                                })
                    else:
                        api_messages.append({"role": "user", "content": content})
                elif msg["role"] == "assistant":
                    content = msg["content"]
                    if isinstance(content, list):
                        text_parts = []
                        tool_calls = []
                        for item in content:
                            if item.get("type") == "text":
                                text_parts.append(item["text"])
                            elif item.get("type") == "tool_use":
                                tool_calls.append({
                                    "id": item["id"],
                                    "type": "function",
                                    "function": {
                                        "name": item["name"],
                                        "arguments": json.dumps(item["input"])
                                    }
                                })
                        assistant_msg = {"role": "assistant"}
                        if text_parts:
                            assistant_msg["content"] = "\n".join(text_parts)
                        if tool_calls:
                            assistant_msg["tool_calls"] = tool_calls
                        api_messages.append(assistant_msg)
                    else:
                        api_messages.append({"role": "assistant", "content": content})

            request_body = {
                "model": model,
                "messages": api_messages,
                "max_tokens": 2048,
                "tools": openai_tools if openai_tools else None,
                "tool_choice": "auto" if openai_tools else None
            }
            request_body = {k: v for k, v in request_body.items() if v is not None}

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY')}",
                "HTTP-Referer": "https://github.com/anthropics/claude-code",
                "X-Title": "Advanced Agent Sub-Agent"
            }

            req = urllib.request.Request(
                "https://openrouter.ai/api/v1/chat/completions",
                data=json.dumps(request_body).encode(),
                headers=headers,
                method="POST"
            )

            try:
                with urllib.request.urlopen(req, timeout=180) as response:
                    result = json.loads(response.read().decode())
            except urllib.error.HTTPError as e:
                error_body = e.read().decode() if e.fp else ""
                raise Exception(f"OpenRouter API error {e.code}: {error_body}")
            except Exception as e:
                raise Exception(f"OpenRouter request failed: {str(e)}")

            choice = result.get("choices", [{}])[0]
            message = choice.get("message", {})

            text_content = message.get("content", "") or ""
            tool_calls = []
            for tc in message.get("tool_calls", []):
                func = tc.get("function", {})
                args_str = func.get("arguments", "{}")
                try:
                    args = json.loads(args_str) if args_str else {}
                except json.JSONDecodeError:
                    args = {}
                tool_calls.append({
                    "id": tc.get("id", f"call_{len(tool_calls)}"),
                    "name": func.get("name", ""),
                    "input": args
                })

            return {"text": text_content, "tool_calls": tool_calls}

        else:
            # Direct Anthropic API
            response = self.client.messages.create(
                model=model,
                max_tokens=2048,
                system=system_prompt,
                messages=messages,
                tools=tools
            )

            text_content = ""
            tool_calls = []
            for block in response.content:
                if hasattr(block, 'text'):
                    text_content += block.text
                elif hasattr(block, 'type') and block.type == "tool_use":
                    tool_calls.append({
                        "id": block.id,
                        "name": block.name,
                        "input": block.input
                    })

            return {"text": text_content, "tool_calls": tool_calls}

    def _execute_spawn_agent(self, params: Dict) -> Dict:
        """Spawn a sub-agent for complex subtasks with timeout support."""
        task = params.get("task")
        tools_allowed = params.get("tools", [t["name"] for t in self.base_tools])
        max_turns = params.get("max_turns", 10)
        model = params.get("model", self.model)
        context = params.get("context", {})
        timeout = params.get("timeout", 300)  # Default 5 min timeout
        blocking = params.get("blocking", True)  # Wait for result by default

        def run_subagent():
            """Run the sub-agent in a thread."""
            # Filter tools for sub-agent - include MCP tools if requested
            sub_tools = [t for t in self.base_tools if t["name"] in tools_allowed]
            # Also allow mcp_call if "mcp" in tools_allowed
            if "mcp" in tools_allowed or "mcp_call" in tools_allowed:
                sub_tools.extend([t for t in self.mcp_tools if t["name"] in ["mcp_call", "mcp_wait"]])

            sub_conversation = []

            system_prompt = f"""You are a focused sub-agent executing a specific task.
Complete the task efficiently using the available tools.
You have a time limit, so work quickly and decisively.

Task: {task}

Additional context: {json.dumps(context) if context else 'None'}

IMPORTANT:
- Complete the task as quickly as possible
- If you have gathered enough information, provide your answer
- Don't make unnecessary tool calls
- When done, provide a clear summary of what you accomplished"""

            sub_conversation.append({"role": "user", "content": task})

            final_result = None
            turns_used = 0
            start_time = time.time()

            for turn in range(max_turns):
                # Check timeout
                if time.time() - start_time > timeout:
                    return {
                        "success": False,
                        "error": f"Sub-agent timed out after {timeout}s",
                        "turns_used": turns_used,
                        "partial_result": final_result
                    }

                turns_used = turn + 1

                # Use helper method that supports both APIs
                api_result = self._subagent_api_call(model, system_prompt, sub_conversation, sub_tools)
                text_content = api_result["text"]
                tool_calls = api_result["tool_calls"]

                if not tool_calls:
                    # Agent is done
                    final_result = text_content
                    break

                # Build assistant message
                content_blocks = []
                if text_content:
                    content_blocks.append({"type": "text", "text": text_content})
                for tc in tool_calls:
                    content_blocks.append({
                        "type": "tool_use",
                        "id": tc["id"],
                        "name": tc["name"],
                        "input": tc["input"]
                    })
                sub_conversation.append({"role": "assistant", "content": content_blocks})

                # Execute tools
                tool_results = []
                for tc in tool_calls:
                    # Check timeout before each tool
                    if time.time() - start_time > timeout:
                        return {
                            "success": False,
                            "error": f"Sub-agent timed out during tool execution after {timeout}s",
                            "turns_used": turns_used,
                            "partial_result": text_content
                        }

                    # Execute tool - check if it's a base tool or meta tool
                    if tc["name"] in [t["name"] for t in self.base_tools]:
                        result = self._execute_base_tool(tc["name"], tc["input"])
                    else:
                        result = self._execute_meta_tool(tc["name"], tc["input"])

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tc["id"],
                        "content": json.dumps(result)
                    })

                sub_conversation.append({"role": "user", "content": tool_results})

            return {
                "success": final_result is not None,
                "result": {
                    "agent_output": final_result,
                    "turns_used": turns_used,
                    "elapsed": time.time() - start_time
                }
            }

        # Run blocking or async
        if blocking:
            return run_subagent()
        else:
            # Submit as async task
            task_id = self.async_tasks.submit(
                f"spawn_agent:{task[:30]}",
                run_subagent,
                timeout=timeout
            )
            return {
                "success": True,
                "async": True,
                "task_id": task_id,
                "message": "Sub-agent spawned, use async_wait to get result"
            }

    def _execute_conditional(self, params: Dict) -> Dict:
        """Execute tools conditionally."""
        check = params.get("check", {})
        condition = params.get("condition", "success")
        if_true = params.get("if_true")
        if_false = params.get("if_false")

        # Execute check
        check_tool = check.get("tool")
        check_params = check.get("params", {})

        if check_tool in [t["name"] for t in self.base_tools]:
            check_result = self._execute_base_tool(check_tool, check_params)
        else:
            check_result = self._execute_meta_tool(check_tool, check_params)

        # Evaluate condition
        condition_met = False

        if condition == "success":
            condition_met = check_result.get("success", False)
        elif condition.startswith("stdout contains "):
            search_str = condition[16:].strip("'\"")
            stdout = check_result.get("result", {}).get("stdout", "")
            condition_met = search_str in stdout
        elif condition.startswith("result."):
            # Simple field check like "result.count > 0"
            parts = condition.split()
            if len(parts) >= 3:
                field_path = parts[0][7:]  # Remove "result."
                op = parts[1]
                value = parts[2]
                actual = check_result.get("result", {}).get(field_path)
                try:
                    value = int(value) if value.isdigit() else value.strip("'\"")
                    if op == "==":
                        condition_met = actual == value
                    elif op == ">":
                        condition_met = actual > value
                    elif op == "<":
                        condition_met = actual < value
                    elif op == "!=":
                        condition_met = actual != value
                except:
                    pass
        else:
            condition_met = bool(check_result.get("success"))

        # Execute branch
        branch = if_true if condition_met else if_false

        if not branch:
            return {
                "success": True,
                "result": {
                    "condition_met": condition_met,
                    "check_result": check_result,
                    "branch_taken": "true" if condition_met else "false",
                    "branch_result": None
                }
            }

        branch_tool = branch.get("tool")
        branch_params = branch.get("params", {})

        if branch_tool in [t["name"] for t in self.base_tools]:
            branch_result = self._execute_base_tool(branch_tool, branch_params)
        else:
            branch_result = self._execute_meta_tool(branch_tool, branch_params)

        return {
            "success": branch_result.get("success", False),
            "result": {
                "condition_met": condition_met,
                "check_result": check_result,
                "branch_taken": "true" if condition_met else "false",
                "branch_result": branch_result
            }
        }

    def execute_tool(self, name: str, params: Dict) -> str:
        """Execute any tool and return JSON result."""
        if not self.initialized:
            self.initialize()

        # Handle agent: prefix for spawning sub-agents
        if name.startswith("agent:"):
            task_description = name[6:]  # Remove 'agent:' prefix
            result = self._execute_spawn_agent({
                "task": task_description,
                "context": params.get("_dependency_outputs", {}),
                **{k: v for k, v in params.items() if k != "_dependency_outputs"}
            })
            return json.dumps(result, indent=2, default=str)

        # Check tool type
        base_tool_names = [t["name"] for t in self.base_tools]
        meta_tool_names = [t["name"] for t in self.meta_tools]
        mcp_tool_names = [t["name"] for t in self.mcp_tools]
        dynamic_mgmt_names = [t["name"] for t in self.dynamic_tool_management]
        memory_tool_names = [t["name"] for t in self.memory_tools]
        scheduler_tool_names = [t["name"] for t in self.scheduler_tools]

        if name in base_tool_names:
            result = self._execute_base_tool(name, params)
        elif name in meta_tool_names or name in mcp_tool_names or name in dynamic_mgmt_names or name in memory_tool_names or name in scheduler_tool_names:
            # Meta, MCP, dynamic tool management, memory, and scheduler tools handled by _execute_meta_tool
            result = self._execute_meta_tool(name, params)
        elif name in self.dynamic_tools.tools:
            # Execute dynamically created tool
            result = self.dynamic_tools.execute(name, params)
        else:
            result = {"success": False, "error": f"Unknown tool: {name}"}

        return json.dumps(result, indent=2, default=str)

    def _compress_result(self, result: str, max_len: int = 2000) -> str:
        """Compress a large result to avoid context pollution."""
        if len(result) <= max_len:
            return result

        try:
            data = json.loads(result)
            # Try to summarize
            if isinstance(data, dict) and "result" in data:
                inner = data["result"]
                if isinstance(inner, dict):
                    # Keep structure but truncate long strings
                    for k, v in inner.items():
                        if isinstance(v, str) and len(v) > 500:
                            inner[k] = v[:500] + f"... ({len(v)} chars total)"
                        elif isinstance(v, list) and len(v) > 10:
                            inner[k] = v[:10] + [f"... ({len(v)} items total)"]
                    return json.dumps(data, indent=2)
        except:
            pass

        return result[:max_len] + f"\n... (truncated, {len(result)} chars total)"

    # =========================================================================
    # AUTO-CAPTURE METHODS
    # =========================================================================

    def _should_capture(self, content: str, content_type: str) -> Tuple[bool, float, str]:
        """Determine if content should be captured to memory.

        Returns: (should_capture, importance, reason)
        """
        if not self.auto_capture.get("enabled", True):
            return False, 0.0, "auto-capture disabled"

        # Check length requirements
        min_len = self.auto_capture.get("min_content_length", 50)
        max_len = self.auto_capture.get("max_content_length", 2000)

        if len(content) < min_len:
            return False, 0.0, "too short"

        content_lower = content.lower()
        importance = 0.5
        reason = ""

        if content_type == "user_message":
            if not self.auto_capture.get("capture_user_facts", True):
                return False, 0.0, "user fact capture disabled"

            # Check for fact/preference indicators
            fact_keywords = self._capture_keywords.get("facts", [])
            matches = sum(1 for kw in fact_keywords if kw in content_lower)

            if matches == 0:
                return False, 0.0, "no fact indicators"

            importance = min(0.6 + (matches * 0.1), 0.9)
            reason = f"user fact ({matches} indicators)"

        elif content_type == "assistant_response":
            if not self.auto_capture.get("capture_assistant_insights", True):
                return False, 0.0, "assistant insight capture disabled"

            # Check for insight indicators
            insight_keywords = self._capture_keywords.get("insights", [])
            matches = sum(1 for kw in insight_keywords if kw in content_lower)

            # Also check for code patterns
            code_keywords = self._capture_keywords.get("code", [])
            code_matches = sum(1 for kw in code_keywords if kw in content)

            if matches == 0 and code_matches == 0:
                return False, 0.0, "no insight/code indicators"

            if code_matches > 0:
                importance = 0.7
                reason = f"code content ({code_matches} patterns)"
            else:
                importance = min(0.5 + (matches * 0.1), 0.8)
                reason = f"assistant insight ({matches} indicators)"

        elif content_type == "tool_result":
            if not self.auto_capture.get("capture_tool_results", True):
                return False, 0.0, "tool result capture disabled"

            # Only capture successful, non-trivial results
            try:
                data = json.loads(content) if isinstance(content, str) else content
                if not data.get("success", True):
                    return False, 0.0, "failed result"

                result = data.get("result", {})
                if isinstance(result, dict):
                    # Check for meaningful content
                    result_str = json.dumps(result)
                    if len(result_str) < min_len:
                        return False, 0.0, "result too small"
                    importance = 0.6
                    reason = "successful tool result"
                elif isinstance(result, str) and len(result) > min_len:
                    importance = 0.5
                    reason = "tool output"
                else:
                    return False, 0.0, "trivial result"
            except:
                return False, 0.0, "could not parse result"

        else:
            return False, 0.0, f"unknown content type: {content_type}"

        # Check for duplicates
        dedupe_threshold = self.auto_capture.get("dedupe_threshold", 0.7)
        search_result = self.vector_memory.search(
            query=content[:500],  # Use first 500 chars as query
            top_k=1,
            min_similarity=dedupe_threshold
        )

        if search_result.get("success") and search_result.get("count", 0) > 0:
            top_match = search_result["results"][0]
            return False, 0.0, f"duplicate (sim={top_match['similarity']:.2f})"

        return True, importance, reason

    def _capture_to_memory(self, content: str, content_type: str,
                           metadata: Dict = None, tool_name: str = None) -> Optional[str]:
        """Capture content to persistent memory if appropriate.

        Returns: memory_id if captured, None otherwise
        """
        should_capture, importance, reason = self._should_capture(content, content_type)

        if not should_capture:
            return None

        # Truncate if too long
        max_len = self.auto_capture.get("max_content_length", 2000)
        if len(content) > max_len:
            content = content[:max_len] + "... (truncated)"

        # Determine memory content type
        if content_type == "user_message":
            mem_type = "fact"
        elif content_type == "assistant_response":
            mem_type = "conversation" if "code" not in reason else "code"
        elif content_type == "tool_result":
            mem_type = "result"
        else:
            mem_type = "note"

        # Build metadata
        capture_metadata = {
            "source": "auto_capture",
            "content_type": content_type,
            "capture_reason": reason,
            "timestamp": time.time()
        }
        if tool_name:
            capture_metadata["tool_name"] = tool_name
        if metadata:
            capture_metadata.update(metadata)

        result = self.vector_memory.store(
            content=content,
            content_type=mem_type,
            importance=importance,
            metadata=capture_metadata
        )

        if result.get("success"):
            return result.get("id")
        return None

    def _extract_capturable_content(self, text: str) -> List[str]:
        """Extract individual capturable segments from longer text.

        Splits on sentence boundaries and groups related sentences.
        """
        if not text or len(text) < self.auto_capture.get("min_content_length", 50):
            return []

        # Simple sentence splitting
        import re
        sentences = re.split(r'(?<=[.!?])\s+', text)

        segments = []
        current_segment = []
        current_length = 0
        max_segment = 500

        for sentence in sentences:
            if current_length + len(sentence) > max_segment and current_segment:
                segments.append(' '.join(current_segment))
                current_segment = []
                current_length = 0

            current_segment.append(sentence)
            current_length += len(sentence)

        if current_segment:
            segments.append(' '.join(current_segment))

        return [s for s in segments if len(s) >= self.auto_capture.get("min_content_length", 50)]

    def set_auto_capture(self, enabled: bool = None, **kwargs):
        """Configure auto-capture settings.

        Args:
            enabled: Enable/disable auto-capture
            capture_user_facts: Capture user messages with facts/preferences
            capture_assistant_insights: Capture assistant discoveries
            capture_tool_results: Capture significant tool results
            min_content_length: Minimum content length
            max_content_length: Maximum content length
            dedupe_threshold: Similarity threshold for deduplication
        """
        if enabled is not None:
            self.auto_capture["enabled"] = enabled

        for key, value in kwargs.items():
            if key in self.auto_capture:
                self.auto_capture[key] = value

        return self.auto_capture.copy()

    def _openrouter_response(self, system_prompt: str) -> Tuple[str, List[Dict], str]:
        """Make a request to OpenRouter API and return (text, tool_uses, stop_reason)."""
        import urllib.request
        import urllib.error

        # Convert tools to OpenAI format
        openai_tools = []
        for tool in self.tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {"type": "object", "properties": {}})
                }
            })

        # Build messages with system prompt
        messages = [{"role": "system", "content": system_prompt}]
        for msg in self.conversation:
            if msg["role"] == "user":
                content = msg["content"]
                if isinstance(content, list):
                    # Handle tool results
                    tool_results = []
                    for item in content:
                        if item.get("type") == "tool_result":
                            tool_results.append({
                                "role": "tool",
                                "tool_call_id": item["tool_use_id"],
                                "content": item["content"]
                            })
                    messages.extend(tool_results)
                else:
                    messages.append({"role": "user", "content": content})
            elif msg["role"] == "assistant":
                content = msg["content"]
                if isinstance(content, list):
                    # Handle assistant message with tool calls
                    text_parts = []
                    tool_calls = []
                    for item in content:
                        if item.get("type") == "text":
                            text_parts.append(item["text"])
                        elif item.get("type") == "tool_use":
                            tool_calls.append({
                                "id": item["id"],
                                "type": "function",
                                "function": {
                                    "name": item["name"],
                                    "arguments": json.dumps(item["input"])
                                }
                            })
                    assistant_msg = {"role": "assistant"}
                    if text_parts:
                        assistant_msg["content"] = "\n".join(text_parts)
                    if tool_calls:
                        assistant_msg["tool_calls"] = tool_calls
                    messages.append(assistant_msg)
                else:
                    messages.append({"role": "assistant", "content": content})

        # Make request
        request_body = {
            "model": self.model,
            "messages": messages,
            "max_tokens": 4096,
            "tools": openai_tools if openai_tools else None,
            "tool_choice": "auto" if openai_tools else None
        }
        # Remove None values
        request_body = {k: v for k, v in request_body.items() if v is not None}

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY')}",
            "HTTP-Referer": "https://github.com/anthropics/claude-code",
            "X-Title": "Advanced Agent"
        }

        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(request_body).encode(),
            headers=headers,
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as response:
                result = json.loads(response.read().decode())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else ""
            raise Exception(f"OpenRouter API error {e.code}: {error_body}")

        # Parse response
        choice = result.get("choices", [{}])[0]
        message = choice.get("message", {})
        finish_reason = choice.get("finish_reason", "stop")

        response_text = message.get("content", "") or ""
        tool_uses = []

        # Print response text
        if response_text:
            print(response_text, flush=True)

        # Handle tool calls
        tool_calls = message.get("tool_calls", [])
        for tc in tool_calls:
            func = tc.get("function", {})
            tool_uses.append({
                "id": tc.get("id", f"call_{len(tool_uses)}"),
                "name": func.get("name", ""),
                "input": func.get("arguments", "{}")
            })

        # Map finish reason
        stop_reason = "end_turn" if finish_reason == "stop" else "tool_use" if finish_reason == "tool_calls" else finish_reason

        if response_text:
            print()

        return response_text, tool_uses, stop_reason

    def _stream_response(self, system_prompt: str) -> Tuple[str, List[Dict], str]:
        """Stream a response and return (text, tool_uses, stop_reason)."""
        # OpenRouter: use non-streaming for better compatibility
        if self.using_openrouter:
            return self._openrouter_response(system_prompt)

        with self.client.messages.stream(
            model=self.model,
            max_tokens=4096,
            system=system_prompt,
            messages=self.conversation,
            tools=self.tools
        ) as stream:
            response_text = ""
            tool_uses = []
            current_tool = None

            for event in stream:
                if event.type == "content_block_start":
                    if hasattr(event.content_block, 'type'):
                        if event.content_block.type == "tool_use":
                            current_tool = {
                                "id": event.content_block.id,
                                "name": event.content_block.name,
                                "input": ""
                            }

                elif event.type == "content_block_delta":
                    if hasattr(event.delta, 'text'):
                        print(event.delta.text, end="", flush=True)
                        response_text += event.delta.text
                    elif hasattr(event.delta, 'partial_json'):
                        if current_tool:
                            current_tool["input"] += event.delta.partial_json

                elif event.type == "content_block_stop":
                    if current_tool:
                        tool_uses.append(current_tool)
                        current_tool = None

            final = stream.get_final_message()
            stop_reason = final.stop_reason

            if response_text:
                print()

            return response_text, tool_uses, stop_reason

    def chat(self, user_input: str, max_turns: int = 30):
        """Process user input with multi-turn tool use."""
        self.conversation.append({"role": "user", "content": user_input})

        # Tick memory TTLs
        self.memory.tick()

        # Auto-capture: Check if user message contains facts/preferences
        if self.auto_capture.get("enabled"):
            mem_id = self._capture_to_memory(user_input, "user_message")
            if mem_id:
                print(f"\033[90m[Auto-captured user fact: {mem_id}]\033[0m")

        system_prompt = """You are an advanced autonomous AI agent with sophisticated meta-tools for complex task execution.

## Core Tools
- **get_datetime**: Current date/time
- **run_shell**: Execute shell commands (git, curl, pip, etc.)
- **execute_python**: Run Python code
- **read_file/write_file/list_files**: File operations
- **http_request**: HTTP requests to URLs/APIs
- **search_web**: Web search

## Meta-Tools (USE THESE FOR COMPLEX TASKS)

### call_tools - Batch Execution
Execute multiple tools with dependencies and parallel execution:
```json
{"calls": [
    {"id": "a", "tool": "read_file", "params": {"path": "x.txt"}},
    {"id": "b", "tool": "run_shell", "params": {"command": "date"}},
    {"id": "c", "tool": "execute_python", "params": {...}, "depends_on": ["a"], "pipe_from": "a", "pipe_field": "content", "inject_as": "data"}
], "parallel": true}
```

### chain - Direct Tool Chaining
Chain tools with output piping (doesn't pollute context):
```json
{"steps": [
    {"tool": "read_file", "params": {"path": "data.json"}, "extract": "content"},
    {"tool": "execute_python", "params": {"code": "result = len(INPUT)"}, "input_var": "INPUT"},
    {"tool": "write_file", "params": {"path": "out.txt"}, "inject": {"content": "RESULT"}}
]}
```

### parallel_map - Batch Apply
Apply a tool to multiple inputs in parallel:
```json
{"tool": "read_file", "inputs": [{"path": "a.txt"}, {"path": "b.txt"}, {"path": "c.txt"}]}
```

### spawn_agent - Sub-Agent Delegation
Spawn a sub-agent for complex subtasks (keeps main context clean):
```json
{"task": "Research and summarize the top 5 Python web frameworks", "max_turns": 10}
```

### conditional - Branching
Execute tools conditionally:
```json
{"check": {"tool": "run_shell", "params": {"command": "test -f x.txt && echo exists"}},
 "condition": "stdout contains 'exists'",
 "if_true": {"tool": "read_file", "params": {"path": "x.txt"}}}
```

### remember/recall - Working Memory
Store and retrieve values without polluting conversation:
- remember: {"key": "mydata", "value": {...}, "ttl": 10}
- recall: {"key": "mydata"}

### think - Internal Reasoning
Record reasoning without action: {"reasoning": "I should first..."}

## Persistent Memory Tools
Store and retrieve information that persists across sessions:
- **memory_store**: Store facts, code, notes with semantic indexing
- **memory_search**: Search memories semantically (TF-IDF + BM25)
- **memory_list**: List memories by type or recency
- **memory_get**: Retrieve specific memory by ID
- **memory_stats**: Show memory statistics

Important memories are auto-captured from conversations. Use memory_search to recall past context.

## Rules
1. USE META-TOOLS for multi-step tasks - they're more efficient
2. Use `chain` when piping data between tools
3. Use `parallel_map` when applying same tool to multiple inputs
4. Use `spawn_agent` for complex subtasks to keep main context clean
5. Use `remember/recall` to store intermediate results
6. Use `call_tools` with `depends_on` for complex dependency graphs
7. NEVER say "I can't" - use tools creatively to accomplish any task

Be proactive. Chain tools. Spawn sub-agents. Keep going until the task is complete."""

        print()
        turn = 0

        while turn < max_turns:
            turn += 1
            response_text, tool_uses, stop_reason = self._stream_response(system_prompt)

            if not tool_uses:
                if response_text:
                    self.conversation.append({"role": "assistant", "content": response_text})
                    # Auto-capture: Check if final response contains insights
                    if self.auto_capture.get("enabled"):
                        mem_id = self._capture_to_memory(response_text, "assistant_response")
                        if mem_id:
                            print(f"\033[90m[Auto-captured insight: {mem_id}]\033[0m")
                break

            # Build assistant message
            content_blocks = []
            if response_text:
                content_blocks.append({"type": "text", "text": response_text})

            for tool in tool_uses:
                try:
                    tool_input = json.loads(tool["input"]) if tool["input"] else {}
                except:
                    tool_input = {}
                content_blocks.append({
                    "type": "tool_use",
                    "id": tool["id"],
                    "name": tool["name"],
                    "input": tool_input
                })

            self.conversation.append({"role": "assistant", "content": content_blocks})

            # Execute tools
            tool_results = []
            for tool in tool_uses:
                try:
                    tool_input = json.loads(tool["input"]) if tool["input"] else {}
                except:
                    tool_input = {}

                is_meta = tool["name"] in [t["name"] for t in self.meta_tools]
                tool_type = "meta" if is_meta else "base"
                print(f"\033[90m[Turn {turn}: {tool_type}:{tool['name']}]\033[0m")

                result = self.execute_tool(tool["name"], tool_input)

                # Compress large results
                compressed = self._compress_result(result)
                if len(compressed) < len(result):
                    print(f"\033[90m  (result compressed: {len(result)} -> {len(compressed)} chars)\033[0m")

                result_preview = compressed[:400] + "..." if len(compressed) > 400 else compressed
                print(f"\033[90m{result_preview}\033[0m\n")

                # Auto-capture: Check if tool result is significant
                if self.auto_capture.get("enabled"):
                    mem_id = self._capture_to_memory(
                        result, "tool_result",
                        tool_name=tool["name"]
                    )
                    if mem_id:
                        print(f"\033[90m[Auto-captured {tool['name']} result: {mem_id}]\033[0m")

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool["id"],
                    "content": compressed
                })

            self.conversation.append({"role": "user", "content": tool_results})

        if turn >= max_turns:
            print(f"\033[93m[Reached max turns ({max_turns})]\033[0m")

    def run(self):
        """Main chat loop."""
        print("\033[1m" + "="*60 + "\033[0m")
        print("\033[1mADVANCED AUTONOMOUS AGENT\033[0m")
        print("\033[1m" + "="*60 + "\033[0m")
        print()
        print("Meta-tools: call_tools, chain, parallel_map, spawn_agent, conditional")
        print("Working memory: remember, recall, memory_status")
        print("Persistent memory: memory_store, memory_search, memory_list, memory_stats")
        print()
        print("Commands:")
        print("  quit       - Exit the agent")
        print("  clear      - Reset conversation and working memory")
        print("  memory     - Show working memory status")
        print("  pmemory    - Show persistent memory stats")
        print("  autocap    - Toggle auto-capture on/off")
        print()

        if not self.initialized:
            self.initialize()

        # Show auto-capture status
        if self.auto_capture.get("enabled"):
            print(f"\033[90mAuto-capture: ON (facts, insights, results)\033[0m")
        else:
            print(f"\033[90mAuto-capture: OFF\033[0m")
        print()

        while True:
            try:
                user_input = input("\033[1m>\033[0m ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ('quit', 'exit', 'q'):
                    print("Goodbye!")
                    break

                if user_input.lower() == 'clear':
                    self.conversation = []
                    self.memory = WorkingMemory()
                    print("\033[90mConversation and working memory cleared.\033[0m")
                    continue

                if user_input.lower() == 'memory':
                    print("Working memory:")
                    print(json.dumps(self.memory.summary(), indent=2))
                    continue

                if user_input.lower() == 'pmemory':
                    stats = self.vector_memory.stats()
                    if stats.get("success"):
                        print("Persistent memory:")
                        print(json.dumps(stats.get("stats", {}), indent=2))
                    continue

                if user_input.lower() == 'autocap':
                    self.auto_capture["enabled"] = not self.auto_capture.get("enabled", True)
                    status = "ON" if self.auto_capture["enabled"] else "OFF"
                    print(f"\033[90mAuto-capture: {status}\033[0m")
                    continue

                self.chat(user_input)
                print()

            except KeyboardInterrupt:
                print("\n\033[90mInterrupted. Type 'quit' to exit.\033[0m")
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"\033[91mError: {e}\033[0m")
                import traceback
                traceback.print_exc()


def main():
    agent = AdvancedAgent()
    agent.run()


if __name__ == "__main__":
    main()
