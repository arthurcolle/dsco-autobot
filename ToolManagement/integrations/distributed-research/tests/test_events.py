"""Tests for event system (events.py)."""

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import pytest_asyncio

from parallel_websearch_server.events import (
    Event,
    EventBus,
    EventType,
    emit_error,
    emit_findall_event,
    emit_search_event,
    emit_task_group_created,
    emit_task_group_updated,
    emit_task_run_created,
    emit_task_run_status,
    emit_warning,
    get_event_bus,
)


class TestEvent:
    """Tests for Event dataclass."""

    def test_event_creation(self):
        """Test creating an event."""
        event = Event(
            type=EventType.TASK_RUN_CREATED,
            data={"run_id": "trun_123"},
            resource_id="trun_123",
            resource_type="task_run"
        )

        assert event.type == EventType.TASK_RUN_CREATED
        assert event.data["run_id"] == "trun_123"
        assert event.resource_id == "trun_123"
        assert event.event_id is not None

    def test_event_to_sse(self):
        """Test converting event to SSE format."""
        event = Event(
            type=EventType.TASK_RUN_COMPLETED,
            data={"status": "completed"},
            resource_id="trun_456"
        )

        sse = event.to_sse()
        assert "id: " in sse
        assert "event: task_run.completed" in sse
        assert "data: " in sse
        assert sse.endswith("\n\n")

    def test_event_to_dict(self):
        """Test converting event to dictionary."""
        event = Event(
            type=EventType.SEARCH_COMPLETED,
            data={"results_count": 10}
        )

        d = event.to_dict()
        assert d["type"] == "search.completed"
        assert d["data"]["results_count"] == 10
        assert "timestamp" in d
        assert "event_id" in d


class TestEventBus:
    """Tests for EventBus singleton."""

    @pytest.fixture
    def fresh_event_bus(self):
        """Create a fresh EventBus instance."""
        # Reset singleton for testing
        EventBus._instance = None
        bus = EventBus()
        yield bus
        EventBus._instance = None

    def test_singleton_pattern(self, fresh_event_bus):
        """Test EventBus singleton pattern."""
        bus1 = get_event_bus()
        bus2 = get_event_bus()
        assert bus1 is bus2

    @pytest.mark.asyncio
    async def test_publish_stores_in_history(self, fresh_event_bus):
        """Test that published events are stored in history."""
        event = Event(
            type=EventType.TASK_GROUP_CREATED,
            data={"group_id": "tgrp_123"}
        )

        await fresh_event_bus.publish(event)

        history = fresh_event_bus.get_recent_events(limit=10)
        assert len(history) == 1
        assert history[0]["type"] == "task_group.created"

    def test_publish_sync_stores_in_history(self, fresh_event_bus):
        """Test synchronous publish stores events."""
        with patch('parallel_websearch_server.local_store.get_local_store') as mock_store:
            mock_store.return_value = MagicMock()

            event = Event(
                type=EventType.TASK_RUN_QUEUED,
                data={"run_id": "trun_sync"}
            )

            fresh_event_bus.publish_sync(event)

            assert fresh_event_bus._stats["events_published"] == 1
            history = fresh_event_bus.get_recent_events(limit=10)
            assert len(history) == 1

    def test_get_stats(self, fresh_event_bus):
        """Test getting event bus statistics."""
        stats = fresh_event_bus.get_stats()

        assert "events_published" in stats
        assert "events_delivered" in stats
        assert "subscribers_total" in stats
        assert "subscribers_active" in stats
        assert "history_size" in stats
        assert "history_limit" in stats

    def test_get_recent_events_with_type_filter(self, fresh_event_bus):
        """Test filtering recent events by type."""
        with patch('parallel_websearch_server.local_store.get_local_store') as mock_store:
            mock_store.return_value = MagicMock()

            # Add events of different types
            fresh_event_bus.publish_sync(Event(
                type=EventType.TASK_RUN_CREATED,
                data={}
            ))
            fresh_event_bus.publish_sync(Event(
                type=EventType.TASK_GROUP_CREATED,
                data={}
            ))
            fresh_event_bus.publish_sync(Event(
                type=EventType.TASK_RUN_COMPLETED,
                data={}
            ))

            # Filter by type
            task_run_events = fresh_event_bus.get_recent_events(
                limit=10,
                event_types=["task_run.created", "task_run.completed"]
            )
            assert len(task_run_events) == 2

    def test_history_limit_enforced(self, fresh_event_bus):
        """Test that history respects the limit."""
        fresh_event_bus._history_limit = 5

        with patch('parallel_websearch_server.local_store.get_local_store') as mock_store:
            mock_store.return_value = MagicMock()

            # Add more events than limit
            for i in range(10):
                fresh_event_bus.publish_sync(Event(
                    type=EventType.HEARTBEAT,
                    data={"index": i}
                ))

            assert len(fresh_event_bus._event_history) == 5
            # Most recent should be kept
            assert fresh_event_bus._event_history[-1].data["index"] == 9


class TestEmitFunctions:
    """Tests for convenience emit functions."""

    @pytest.fixture
    def mock_bus(self):
        """Mock the event bus for testing emit functions."""
        EventBus._instance = None
        bus = EventBus()
        # Set the singleton instance so get_event_bus() returns our bus
        EventBus._instance = bus
        # Mock at the source module where it's defined (since it's imported inside publish_sync)
        patcher = patch('parallel_websearch_server.local_store.get_local_store')
        mock_store_func = patcher.start()
        mock_store_func.return_value = MagicMock()
        yield bus
        patcher.stop()
        EventBus._instance = None

    def test_emit_task_group_created(self, mock_bus):
        """Test emitting task group created event."""
        emit_task_group_created("tgrp_new", {"experiment": "test"})

        events = mock_bus.get_recent_events(limit=1)
        assert len(events) == 1
        assert events[0]["type"] == "task_group.created"
        assert events[0]["resource_id"] == "tgrp_new"

    def test_emit_task_group_updated(self, mock_bus):
        """Test emitting task group updated event."""
        emit_task_group_updated("tgrp_upd", "active", {"completed": 5})

        events = mock_bus.get_recent_events(limit=1)
        assert len(events) == 1
        assert events[0]["type"] == "task_group.updated"
        assert events[0]["data"]["status"] == "active"

    def test_emit_task_run_created(self, mock_bus):
        """Test emitting task run created event."""
        emit_task_run_created("trun_new", "lite", "tgrp_123", "What is 2+2?")

        events = mock_bus.get_recent_events(limit=1)
        assert len(events) == 1
        assert events[0]["type"] == "task_run.created"
        assert events[0]["data"]["processor"] == "lite"

    def test_emit_task_run_status(self, mock_bus):
        """Test emitting task run status events."""
        for status in ["queued", "running", "completed", "failed"]:
            emit_task_run_status(f"trun_{status}", status)

        events = mock_bus.get_recent_events(limit=10)
        assert len(events) == 4

    def test_emit_search_event(self, mock_bus):
        """Test emitting search events."""
        emit_search_event("started", "Find Python tutorials")
        emit_search_event("completed", "Find Python tutorials", results_count=10)

        events = mock_bus.get_recent_events(limit=10)
        assert len(events) == 2
        assert events[0]["type"] == "search.started"
        assert events[1]["type"] == "search.completed"

    def test_emit_findall_event(self, mock_bus):
        """Test emitting findall events."""
        emit_findall_event("created", "fa_123", "Find all companies")
        emit_findall_event("progress", "fa_123", candidates_found=50, candidates_matched=10)
        emit_findall_event("completed", "fa_123", candidates_found=100, candidates_matched=25)

        events = mock_bus.get_recent_events(limit=10)
        assert len(events) == 3
        assert events[0]["type"] == "findall.created"
        assert events[2]["type"] == "findall.completed"

    def test_emit_error(self, mock_bus):
        """Test emitting error events."""
        emit_error("Something went wrong", {"code": 500})

        events = mock_bus.get_recent_events(limit=1)
        assert len(events) == 1
        assert events[0]["type"] == "system.error"
        assert events[0]["data"]["message"] == "Something went wrong"

    def test_emit_warning(self, mock_bus):
        """Test emitting warning events."""
        emit_warning("Rate limit approaching", {"remaining": 10})

        events = mock_bus.get_recent_events(limit=1)
        assert len(events) == 1
        assert events[0]["type"] == "system.warning"


class TestEventTypes:
    """Tests for EventType enum."""

    def test_all_event_types_have_values(self):
        """Test that all event types have string values."""
        for event_type in EventType:
            assert isinstance(event_type.value, str)
            # Most have format: category.action, but HEARTBEAT is just "heartbeat"
            if event_type != EventType.HEARTBEAT:
                assert "." in event_type.value  # Format: category.action

    def test_event_type_categories(self):
        """Test event type categories."""
        categories = {
            "task_group": ["created", "updated", "completed"],
            "task_run": ["created", "queued", "running", "completed", "failed", "cancelled"],
            "search": ["started", "completed", "failed"],
            "extract": ["started", "completed", "failed"],
            "findall": ["created", "running", "progress", "completed", "failed"],
            "system": ["error", "warning"],
        }

        for category, actions in categories.items():
            for action in actions:
                event_type_str = f"{category}.{action}"
                matching = [et for et in EventType if et.value == event_type_str]
                assert len(matching) == 1, f"Missing event type: {event_type_str}"
