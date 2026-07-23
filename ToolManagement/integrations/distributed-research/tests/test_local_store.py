"""Tests for local SQLite storage (local_store.py)."""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from parallel_websearch_server.local_store import LocalStore, get_local_store


@pytest.fixture
def temp_store():
    """Create a temporary LocalStore for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = LocalStore(sqlite_path=str(db_path))
        store.connect()
        yield store
        store.close()


class TestLocalStoreTaskRuns:
    """Tests for task run storage."""

    def test_save_and_get_task_run(self, temp_store):
        """Test saving and retrieving a task run."""
        temp_store.save_task_run(
            run_id="trun_test123",
            processor="lite",
            status="queued",
            input_text="What is 2+2?",
            metadata={"test": True}
        )

        result = temp_store.get_task_run("trun_test123")
        assert result is not None
        assert result["run_id"] == "trun_test123"
        assert result["processor"] == "lite"
        assert result["status"] == "queued"
        assert result["input_text"] == "What is 2+2?"

    def test_update_task_run_status(self, temp_store):
        """Test updating a task run's status."""
        temp_store.save_task_run(
            run_id="trun_update",
            processor="core",
            status="queued"
        )

        # Update status
        temp_store.save_task_run(
            run_id="trun_update",
            processor="core",
            status="running"
        )

        result = temp_store.get_task_run("trun_update")
        assert result["status"] == "running"

    def test_save_task_run_with_output(self, temp_store):
        """Test saving a task run with output."""
        temp_store.save_task_run(
            run_id="trun_output",
            processor="lite",
            status="completed",
            input_text="Calculate 5+3",
            output_text="8"
        )

        result = temp_store.get_task_run("trun_output")
        assert result["output_text"] == "8"
        assert result["status"] == "completed"

    def test_list_task_runs_with_filters(self, temp_store):
        """Test listing task runs with status and processor filters."""
        # Create multiple runs
        for i in range(5):
            status = "completed" if i < 3 else "failed"
            processor = "lite" if i % 2 == 0 else "core"
            temp_store.save_task_run(
                run_id=f"trun_{i}",
                processor=processor,
                status=status
            )

        # Filter by status
        completed = temp_store.list_task_runs(status="completed")
        assert len(completed) == 3

        # Filter by processor
        lite_runs = temp_store.list_task_runs(processor="lite")
        assert len(lite_runs) == 3

        # Filter by both
        completed_lite = temp_store.list_task_runs(status="completed", processor="lite")
        assert len(completed_lite) == 2

    def test_get_recent_task_ids(self, temp_store):
        """Test getting recent task IDs."""
        for i in range(10):
            temp_store.save_task_run(
                run_id=f"trun_{i:02d}",
                processor="lite",
                status="queued"
            )

        recent_ids = temp_store.get_recent_task_ids(limit=5)
        assert len(recent_ids) == 5
        # Most recent should be last created
        assert recent_ids[0] == "trun_09"


class TestLocalStoreTaskGroups:
    """Tests for task group storage."""

    def test_save_and_get_task_group(self, temp_store):
        """Test saving and retrieving a task group."""
        temp_store.save_task_group(
            taskgroup_id="tgrp_test123",
            status="active",
            metadata={"experiment": "test1"},
            num_task_runs=5,
            completed_runs=2,
            failed_runs=0
        )

        result = temp_store.get_task_group("tgrp_test123")
        assert result is not None
        assert result["taskgroup_id"] == "tgrp_test123"
        assert result["status"] == "active"
        assert result["num_task_runs"] == 5
        assert result["completed_runs"] == 2

    def test_update_task_group_counts(self, temp_store):
        """Test updating task group run counts (delta sync)."""
        temp_store.save_task_group(
            taskgroup_id="tgrp_delta",
            status="active",
            num_task_runs=0,
            completed_runs=0,
            failed_runs=0
        )

        # Simulate delta update
        temp_store.save_task_group(
            taskgroup_id="tgrp_delta",
            status="active",
            num_task_runs=5,
            completed_runs=3,
            failed_runs=1
        )

        result = temp_store.get_task_group("tgrp_delta")
        assert result["num_task_runs"] == 5
        assert result["completed_runs"] == 3
        assert result["failed_runs"] == 1

    def test_list_task_groups(self, temp_store):
        """Test listing task groups."""
        for i in range(5):
            temp_store.save_task_group(
                taskgroup_id=f"tgrp_{i}",
                status="active" if i < 3 else "completed"
            )

        groups = temp_store.list_task_groups(limit=10)
        assert len(groups) == 5


class TestLocalStoreEvents:
    """Tests for event storage (30-day rolling)."""

    def test_save_and_list_events(self, temp_store):
        """Test saving and listing events."""
        temp_store.save_event(
            event_id="evt_1",
            event_type="task_run.created",
            data={"run_id": "trun_123"},
            resource_id="trun_123",
            resource_type="task_run"
        )

        events = temp_store.list_events(limit=10)
        assert len(events) == 1
        assert events[0]["event_type"] == "task_run.created"
        assert events[0]["resource_id"] == "trun_123"

    def test_list_events_by_type(self, temp_store):
        """Test filtering events by type."""
        for i in range(5):
            event_type = "task_run.created" if i < 3 else "task_group.created"
            temp_store.save_event(
                event_id=f"evt_{i}",
                event_type=event_type,
                data={"index": i}
            )

        task_run_events = temp_store.list_events(event_type="task_run.created")
        assert len(task_run_events) == 3

    def test_list_events_by_resource(self, temp_store):
        """Test filtering events by resource ID."""
        for i in range(5):
            resource_id = "trun_123" if i < 2 else f"trun_{i}"
            temp_store.save_event(
                event_id=f"evt_{i}",
                event_type="task_run.status",
                data={"status": "running"},
                resource_id=resource_id
            )

        events = temp_store.list_events(resource_id="trun_123")
        assert len(events) == 2

    def test_get_event_count(self, temp_store):
        """Test getting event counts."""
        for i in range(10):
            temp_store.save_event(
                event_id=f"evt_{i}",
                event_type="test.event",
                data={}
            )

        count = temp_store.get_event_count()
        assert count == 10

    def test_cleanup_old_events(self, temp_store):
        """Test cleaning up old events."""
        # Insert an old event manually
        conn = temp_store._get_conn()
        old_date = (datetime.utcnow() - timedelta(days=35)).isoformat()
        conn.execute(
            """INSERT INTO events (event_id, event_type, data, created_at)
               VALUES (?, ?, ?, ?)""",
            ("old_evt", "test.old", "{}", old_date)
        )
        conn.commit()

        # Insert a recent event
        temp_store.save_event(
            event_id="new_evt",
            event_type="test.new",
            data={}
        )

        # Cleanup events older than 30 days
        deleted = temp_store.cleanup_old_events(days=30)
        assert deleted == 1

        # Verify only new event remains
        count = temp_store.get_event_count()
        assert count == 1


class TestLocalStoreSearch:
    """Tests for search request storage."""

    def test_save_and_list_searches(self, temp_store):
        """Test saving and listing search requests."""
        temp_store.save_search_request(
            request_id="srch_123",
            objective="Find Python tutorials",
            results=[{"title": "Python 101", "url": "https://example.com"}],
            results_count=1
        )

        searches = temp_store.list_search_requests(limit=10)
        assert len(searches) == 1
        assert searches[0]["objective"] == "Find Python tutorials"


class TestLocalStoreStats:
    """Tests for storage statistics."""

    def test_get_stats(self, temp_store):
        """Test getting storage statistics."""
        # Add some data
        temp_store.save_task_run(run_id="t1", processor="lite", status="completed")
        temp_store.save_task_run(run_id="t2", processor="lite", status="failed")
        temp_store.save_task_group(taskgroup_id="g1", status="active")

        stats = temp_store.get_stats()
        # get_stats returns table names with 'count' sub-dict
        assert "task_runs" in stats
        assert stats["task_runs"]["count"] == 2
        # Status breakdown is in separate key
        assert "task_status_breakdown" in stats
        assert stats["task_status_breakdown"]["completed"] == 1
        assert stats["task_status_breakdown"]["failed"] == 1
