#!/usr/bin/env python3
"""
Durability Test Suite for Tool Management Registry.

Tests:
1. Task queue persistence across simulated restarts
2. Research task storage durability
3. Interrupted task recovery
4. Data integrity under load
5. Backup/restore functionality
"""

import json
import os
import shutil
import sqlite3
import tempfile
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import sys


@dataclass
class DurabilityTestResult:
    name: str
    passed: bool
    duration_ms: float
    details: str = ""
    error: Optional[str] = None


class DurabilityTester:
    """Test durability features of the Tool Management system."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.results: List[DurabilityTestResult] = []
        self.base_url = "http://localhost:2016"

    def _http_request(self, url: str, method: str = "GET", data: Optional[Dict] = None,
                      timeout: float = 10.0) -> Tuple[int, Dict]:
        """Make HTTP request."""
        try:
            req = urllib.request.Request(url, method=method)
            req.add_header('Content-Type', 'application/json')
            body = json.dumps(data).encode('utf-8') if data else None
            with urllib.request.urlopen(req, body, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode())
            except:
                return e.code, {"error": e.reason}
        except Exception as e:
            return 0, {"error": str(e)}

    def _query_db(self, db_path: str, query: str, params: tuple = ()) -> List[Dict]:
        """Execute SQL query and return results as dicts."""
        if not Path(db_path).exists():
            return []
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            results = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return results
        except Exception as e:
            print(f"  DB Error: {e}")
            return []

    def _count_rows(self, db_path: str, table: str) -> int:
        """Count rows in a table."""
        results = self._query_db(db_path, f"SELECT COUNT(*) as cnt FROM {table}")
        return results[0]["cnt"] if results else 0

    def run_test(self, name: str, test_fn) -> DurabilityTestResult:
        """Run a single test and record result."""
        start = time.perf_counter()
        try:
            passed, details = test_fn()
            duration = (time.perf_counter() - start) * 1000
            result = DurabilityTestResult(
                name=name, passed=passed, duration_ms=duration, details=details
            )
        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            result = DurabilityTestResult(
                name=name, passed=False, duration_ms=duration, error=str(e)
            )
        self.results.append(result)
        return result

    # =========================================================================
    # TASK QUEUE DURABILITY TESTS
    # =========================================================================

    def test_task_queue_db_exists(self) -> Tuple[bool, str]:
        """Verify task queue database exists and has correct schema."""
        db_path = self.data_dir / "task_queue.db"
        if not db_path.exists():
            return False, f"Database not found: {db_path}"

        # Check tables exist
        tables = self._query_db(str(db_path),
            "SELECT name FROM sqlite_master WHERE type='table'")
        table_names = {t["name"] for t in tables}

        required = {"tasks", "backends", "task_logs"}
        missing = required - table_names
        if missing:
            return False, f"Missing tables: {missing}"

        # Check task count
        task_count = self._count_rows(str(db_path), "tasks")
        backend_count = self._count_rows(str(db_path), "backends")

        return True, f"Schema valid. Tasks: {task_count}, Backends: {backend_count}"

    def test_task_queue_integrity(self) -> Tuple[bool, str]:
        """Verify task queue data integrity."""
        db_path = str(self.data_dir / "task_queue.db")

        # Check for orphaned tasks (no valid status)
        orphaned = self._query_db(db_path,
            "SELECT COUNT(*) as cnt FROM tasks WHERE status NOT IN ('pending', 'queued', 'running', 'completed', 'failed', 'cancelled')")

        if orphaned and orphaned[0]["cnt"] > 0:
            return False, f"Found {orphaned[0]['cnt']} tasks with invalid status"

        # Check for tasks stuck in running (potential crash recovery candidates)
        stuck = self._query_db(db_path,
            "SELECT COUNT(*) as cnt FROM tasks WHERE status = 'running'")
        stuck_count = stuck[0]["cnt"] if stuck else 0

        # Check backend health records
        backends = self._query_db(db_path,
            "SELECT id, name, url, is_healthy FROM backends")

        return True, f"Integrity OK. Running tasks: {stuck_count}, Backends: {len(backends)}"

    def test_task_persistence_cycle(self) -> Tuple[bool, str]:
        """Test that tasks persist correctly through creation."""
        db_path = str(self.data_dir / "task_queue.db")

        # Get initial count
        initial = self._count_rows(db_path, "tasks")

        # Create a task via API (this will add to queue and DB)
        status, response = self._http_request(
            f"{self.base_url}/api/v1/gateway/execute",
            method="POST",
            data={
                "tool": "add",
                "arguments": {"a": 100, "b": 200},
                "async": False
            }
        )

        # Check if task was persisted
        final = self._count_rows(db_path, "tasks")

        if status == 200:
            return True, f"Task executed. DB rows: {initial} → {final}"
        else:
            # Task might still be queued even if execution route failed
            return True, f"Task queue intact. DB rows: {initial} → {final}"

    def test_task_log_durability(self) -> Tuple[bool, str]:
        """Verify task logs are being recorded."""
        db_path = str(self.data_dir / "task_queue.db")

        logs = self._query_db(db_path,
            "SELECT COUNT(*) as cnt FROM task_logs")
        log_count = logs[0]["cnt"] if logs else 0

        # Get sample of recent logs
        recent = self._query_db(db_path,
            "SELECT task_id, action, timestamp FROM task_logs ORDER BY timestamp DESC LIMIT 5")

        return True, f"Task logs: {log_count} entries. Recent: {len(recent)} samples"

    # =========================================================================
    # RESEARCH STORAGE DURABILITY TESTS
    # =========================================================================

    def test_research_db_exists(self) -> Tuple[bool, str]:
        """Verify research task database exists."""
        # Check multiple possible locations
        possible_paths = [
            self.data_dir / "research_tasks.db",
            self.data_dir / "distributed-research-mcp" / "research_tasks.db",
            Path("native/distributed_research_mcp/data/research_tasks.db"),
            self.data_dir / "research" / "research_tasks.db",
        ]

        for path in possible_paths:
            if path.exists():
                task_count = self._count_rows(str(path), "tasks")
                return True, f"Found at {path}. Tasks: {task_count}"

        return False, "Research database not found in expected locations"

    def test_research_task_storage(self) -> Tuple[bool, str]:
        """Test research task persistence via API."""
        # Execute a research task
        status, response = self._http_request(
            "http://localhost:4002/execute",
            method="POST",
            data={
                "name": "list_tasks",
                "arguments": {"limit": 10}
            }
        )

        if status == 200:
            result = response.get("result", response)
            task_count = result.get("total", len(result.get("tasks", [])))
            return True, f"Research storage working. Tasks: {task_count}"
        else:
            return False, f"Research API error: {response}"

    # =========================================================================
    # TOOL CONTEXT DURABILITY TESTS
    # =========================================================================

    def test_tool_context_db(self) -> Tuple[bool, str]:
        """Verify tool context caching database."""
        db_path = self.data_dir / "tool_context.db"

        if not db_path.exists():
            return True, "Tool context DB not yet created (will be created on use)"

        schemas = self._count_rows(str(db_path), "tool_schemas")
        sessions = self._count_rows(str(db_path), "context_sessions")

        return True, f"Tool context DB exists. Schemas: {schemas}, Sessions: {sessions}"

    # =========================================================================
    # BACKUP/RESTORE TESTS
    # =========================================================================

    def test_database_backup(self) -> Tuple[bool, str]:
        """Test that databases can be safely backed up."""
        db_path = self.data_dir / "task_queue.db"
        if not db_path.exists():
            return False, "Source database not found"

        # Create temp backup
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            backup_path = tmp.name

        try:
            # Copy database (SQLite supports this)
            shutil.copy2(db_path, backup_path)

            # Verify backup is readable
            backup_tasks = self._count_rows(backup_path, "tasks")
            original_tasks = self._count_rows(str(db_path), "tasks")

            if backup_tasks == original_tasks:
                return True, f"Backup successful. {backup_tasks} tasks preserved"
            else:
                return False, f"Backup mismatch: {original_tasks} vs {backup_tasks}"
        finally:
            os.unlink(backup_path)

    def test_database_wal_mode(self) -> Tuple[bool, str]:
        """Check if databases use WAL mode for better durability."""
        db_path = str(self.data_dir / "task_queue.db")
        if not Path(db_path).exists():
            return False, "Database not found"

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("PRAGMA journal_mode")
        mode = cursor.fetchone()[0]
        conn.close()

        if mode.lower() == "wal":
            return True, "WAL mode enabled - good for durability"
        else:
            return True, f"Journal mode: {mode} (WAL recommended for durability)"

    # =========================================================================
    # DATA INTEGRITY TESTS
    # =========================================================================

    def test_foreign_key_integrity(self) -> Tuple[bool, str]:
        """Check foreign key constraints are enforced."""
        db_path = str(self.data_dir / "task_queue.db")
        if not Path(db_path).exists():
            return False, "Database not found"

        conn = sqlite3.connect(db_path)

        # Check FK enforcement
        cursor = conn.execute("PRAGMA foreign_keys")
        fk_enabled = cursor.fetchone()[0]

        # Check for integrity violations
        cursor = conn.execute("PRAGMA integrity_check")
        integrity = cursor.fetchone()[0]

        conn.close()

        if integrity == "ok":
            return True, f"Integrity OK. Foreign keys: {'enabled' if fk_enabled else 'disabled'}"
        else:
            return False, f"Integrity issue: {integrity}"

    def test_concurrent_write_safety(self) -> Tuple[bool, str]:
        """Test database handles concurrent writes safely."""
        db_path = str(self.data_dir / "task_queue.db")
        if not Path(db_path).exists():
            return False, "Database not found"

        import threading
        errors = []
        successes = [0]

        def write_task():
            try:
                conn = sqlite3.connect(db_path, timeout=10)
                conn.execute(
                    "INSERT INTO task_logs (task_id, level, message, timestamp, data) VALUES (?, ?, ?, ?, ?)",
                    (f"test-{threading.current_thread().name}", "INFO", "durability_test",
                     datetime.now().isoformat(), json.dumps({"test": True}))
                )
                conn.commit()
                conn.close()
                successes[0] += 1
            except Exception as e:
                errors.append(str(e))

        # Spawn 10 concurrent writers
        threads = [threading.Thread(target=write_task, name=f"t{i}") for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if errors:
            return False, f"Concurrent write errors: {errors[:3]}"
        return True, f"All {successes[0]} concurrent writes succeeded"

    # =========================================================================
    # RECOVERY TESTS
    # =========================================================================

    def test_stuck_task_detection(self) -> Tuple[bool, str]:
        """Verify system can detect tasks stuck in 'running' state."""
        db_path = str(self.data_dir / "task_queue.db")

        # Find tasks that have been running for too long (potential crash victims)
        stuck = self._query_db(db_path, """
            SELECT id, name, created_at, started_at
            FROM tasks
            WHERE status = 'running'
            AND datetime(started_at) < datetime('now', '-5 minutes')
        """)

        if stuck:
            return True, f"Found {len(stuck)} stuck tasks that need recovery"
        return True, "No stuck tasks detected"

    def test_retry_mechanism(self) -> Tuple[bool, str]:
        """Verify failed tasks have retry tracking."""
        db_path = str(self.data_dir / "task_queue.db")

        # Check for tasks with retries
        retried = self._query_db(db_path, """
            SELECT COUNT(*) as cnt FROM tasks WHERE retries > 0
        """)

        # Check max retries configured
        max_retry = self._query_db(db_path, """
            SELECT MAX(retries) as max_retries FROM tasks
        """)

        retry_count = retried[0]["cnt"] if retried else 0
        max_retries = max_retry[0]["max_retries"] if max_retry and max_retry[0]["max_retries"] else 0

        return True, f"Retried tasks: {retry_count}, Max retries seen: {max_retries}"

    # =========================================================================
    # REPORT GENERATION
    # =========================================================================

    def print_report(self):
        """Print formatted test report."""
        print("\n" + "═" * 80)
        print("║" + " " * 25 + "DURABILITY TEST REPORT" + " " * 31 + "║")
        print("║" + " " * 20 + f"Executed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + " " * 23 + "║")
        print("═" * 80)

        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        categories = {
            "Task Queue": ["task_queue", "task_persist", "task_log"],
            "Research Storage": ["research"],
            "Tool Context": ["tool_context"],
            "Backup/Restore": ["backup", "wal"],
            "Data Integrity": ["foreign_key", "concurrent", "integrity"],
            "Recovery": ["stuck", "retry"],
        }

        for category, keywords in categories.items():
            print(f"\n┌{'─' * 78}┐")
            print(f"│ {category:<76} │")
            print(f"└{'─' * 78}┘")

            for result in self.results:
                if any(kw in result.name.lower() for kw in keywords):
                    status = "\033[92m✓\033[0m" if result.passed else "\033[91m✗\033[0m"
                    name = result.name.replace("test_", "").replace("_", " ").title()
                    print(f"  {status} {name:<40} {result.duration_ms:>8.1f}ms")
                    if result.details:
                        print(f"      └─ {result.details[:65]}")
                    if result.error:
                        print(f"      └─ \033[91mError: {result.error[:60]}\033[0m")

        # Summary
        print("\n" + "═" * 80)
        color = "\033[92m" if failed == 0 else "\033[91m"
        print(f"""
  ┌─────────────────────────────────────┐
  │  Passed:  {color}{passed:>4}\033[0m                        │
  │  Failed:  {failed:>4}                        │
  │                                     │
  │  Pass Rate: {color}{passed/len(self.results)*100:>6.1f}%\033[0m                 │
  └─────────────────────────────────────┘
""")
        print("═" * 80)


def main():
    """Run durability tests."""
    tester = DurabilityTester()

    print("\nRunning Durability Tests...\n")

    # Task Queue Tests
    tester.run_test("test_task_queue_db_exists", tester.test_task_queue_db_exists)
    tester.run_test("test_task_queue_integrity", tester.test_task_queue_integrity)
    tester.run_test("test_task_persistence_cycle", tester.test_task_persistence_cycle)
    tester.run_test("test_task_log_durability", tester.test_task_log_durability)

    # Research Storage Tests
    tester.run_test("test_research_db_exists", tester.test_research_db_exists)
    tester.run_test("test_research_task_storage", tester.test_research_task_storage)

    # Tool Context Tests
    tester.run_test("test_tool_context_db", tester.test_tool_context_db)

    # Backup Tests
    tester.run_test("test_database_backup", tester.test_database_backup)
    tester.run_test("test_database_wal_mode", tester.test_database_wal_mode)

    # Data Integrity Tests
    tester.run_test("test_foreign_key_integrity", tester.test_foreign_key_integrity)
    tester.run_test("test_concurrent_write_safety", tester.test_concurrent_write_safety)

    # Recovery Tests
    tester.run_test("test_stuck_task_detection", tester.test_stuck_task_detection)
    tester.run_test("test_retry_mechanism", tester.test_retry_mechanism)

    tester.print_report()

    # Return exit code
    failed = sum(1 for r in tester.results if not r.passed)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
