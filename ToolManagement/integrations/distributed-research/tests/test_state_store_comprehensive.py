"""Comprehensive tests for ContextStateStore covering concurrency, errors, and edge cases."""

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import aiosqlite

from parallel_websearch_server.state_store import ContextStateStore


class TestStateStoreInit:
    """Test store initialization and schema management."""

    @pytest.mark.asyncio
    async def test_creates_database_and_tables(self, temp_db_path):
        """Test that the store creates DB file and tables on init."""
        assert not temp_db_path.exists()

        store = ContextStateStore(str(temp_db_path))
        await store.initialize()

        assert temp_db_path.exists()

        # Verify tables exist
        async with aiosqlite.connect(str(temp_db_path)) as db:
            cursor = await db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = {row[0] for row in await cursor.fetchall()}

        assert "contexts" in tables
        assert "interactions" in tables
        await store.close()

    @pytest.mark.asyncio
    async def test_creates_parent_directories(self, tmp_path):
        """Test that missing parent directories are created."""
        nested_path = tmp_path / "nested" / "dirs" / "cache.db"
        store = ContextStateStore(str(nested_path))
        await store.initialize()

        assert nested_path.exists()
        await store.close()

    @pytest.mark.asyncio
    async def test_enables_wal_mode(self, temp_db_path):
        """Test that WAL mode is enabled for better concurrency."""
        store = ContextStateStore(str(temp_db_path))
        await store.initialize()

        async with aiosqlite.connect(str(temp_db_path)) as db:
            cursor = await db.execute("PRAGMA journal_mode")
            mode = (await cursor.fetchone())[0]

        assert mode.upper() == "WAL"
        await store.close()

    @pytest.mark.asyncio
    async def test_handles_readonly_directory(self, tmp_path):
        """Test error handling when directory is not writable."""
        db_path = tmp_path / "readonly" / "cache.db"
        db_path.parent.mkdir()
        db_path.parent.chmod(0o444)  # Read-only

        store = ContextStateStore(str(db_path))

        with pytest.raises(Exception):  # Should raise permission error
            await store.initialize()

        # Cleanup
        db_path.parent.chmod(0o755)


class TestContextOperations:
    """Test context CRUD operations."""

    @pytest.mark.asyncio
    async def test_create_context_with_auto_id(self, state_store):
        """Test creating context with auto-generated ID."""
        context = await state_store.create_context(
            name="Test Context",
            metadata={"key": "value"},
            state={"counter": 0}
        )

        assert context["context_id"]  # Should have an ID
        assert context["name"] == "Test Context"
        assert context["metadata"]["key"] == "value"
        assert context["state"]["counter"] == 0
        assert context["created_at"]
        assert context["updated_at"]

    @pytest.mark.asyncio
    async def test_create_context_with_custom_id(self, state_store):
        """Test creating context with user-provided ID."""
        context = await state_store.create_context(
            context_id="custom-123",
            name="Custom ID Context"
        )

        assert context["context_id"] == "custom-123"

    @pytest.mark.asyncio
    async def test_create_context_duplicate_id_ignored(self, state_store):
        """Test that duplicate context IDs are handled gracefully."""
        await state_store.create_context(context_id="dup-1", name="First")
        context2 = await state_store.create_context(context_id="dup-1", name="Second")

        # Should return existing context (upsert behavior)
        assert context2["name"] == "First"

    @pytest.mark.asyncio
    async def test_get_context_not_found(self, state_store):
        """Test getting non-existent context returns None."""
        result = await state_store.get_context("does-not-exist")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_context_merge_semantics(self, state_store):
        """Test that update merges JSON fields correctly."""
        await state_store.create_context(
            context_id="merge-test",
            metadata={"a": 1, "b": {"nested": "value"}},
            state={"x": "old", "y": [1, 2]}
        )

        updated = await state_store.update_context(
            "merge-test",
            metadata={"b": {"nested": "updated", "new_key": "added"}, "c": 3},
            state={"y": [3, 4], "z": "new"}
        )

        # Metadata should deep merge
        assert updated["metadata"]["a"] == 1  # Original preserved
        assert updated["metadata"]["b"]["nested"] == "updated"  # Updated
        assert updated["metadata"]["b"]["new_key"] == "added"  # Added
        assert updated["metadata"]["c"] == 3  # New top-level

        # State should deep merge
        assert updated["state"]["x"] == "old"  # Original preserved
        assert updated["state"]["y"] == [3, 4]  # Replaced array
        assert updated["state"]["z"] == "new"  # Added

    @pytest.mark.asyncio
    async def test_update_context_replace_semantics(self, state_store):
        """Test that replace flags completely override fields."""
        await state_store.create_context(
            context_id="replace-test",
            metadata={"old": "data"},
            state={"old": "state"}
        )

        updated = await state_store.update_context(
            "replace-test",
            metadata={"new": "metadata"},
            state={"new": "state"},
            replace_metadata=True,
            replace_state=True
        )

        assert updated["metadata"] == {"new": "metadata"}
        assert updated["state"] == {"new": "state"}

    @pytest.mark.asyncio
    async def test_update_nonexistent_context(self, state_store):
        """Test updating non-existent context returns None."""
        result = await state_store.update_context(
            "does-not-exist",
            metadata={"test": "data"}
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_list_contexts_pagination(self, state_store):
        """Test listing contexts with limit and ordering."""
        # Create multiple contexts
        for i in range(5):
            await state_store.create_context(
                context_id=f"ctx-{i}",
                name=f"Context {i}"
            )
            await asyncio.sleep(0.01)  # Ensure different timestamps

        # Test limit
        contexts = await state_store.list_contexts(limit=3)
        assert len(contexts) == 3

        # Should be ordered by updated_at DESC (most recent first)
        assert contexts[0]["context_id"] == "ctx-4"
        assert contexts[1]["context_id"] == "ctx-3"
        assert contexts[2]["context_id"] == "ctx-2"

    @pytest.mark.asyncio
    async def test_context_with_null_metadata_and_state(self, state_store):
        """Test that contexts can have null metadata/state."""
        context = await state_store.create_context(
            context_id="null-fields",
            name="Minimal"
            # No metadata or state
        )

        assert context["metadata"] is None or context["metadata"] == {}
        assert context["state"] is None or context["state"] == {}

        # Can still update
        updated = await state_store.update_context(
            "null-fields",
            metadata={"now": "has", "data": True}
        )
        assert updated["metadata"]["now"] == "has"


class TestInteractionOperations:
    """Test interaction recording and retrieval."""

    @pytest.mark.asyncio
    async def test_record_interaction_basic(self, state_store):
        """Test recording a basic interaction."""
        await state_store.create_context(context_id="ctx-1")

        interaction_id = await state_store.record_interaction(
            context_id="ctx-1",
            interaction_type="search",
            status="completed",
            input_payload={"query": "test"},
            output_payload={"results": 5},
            summary="Found 5 results"
        )

        assert interaction_id > 0

        interactions = await state_store.list_interactions("ctx-1")
        assert len(interactions) == 1
        assert interactions[0]["interaction_type"] == "search"
        assert interactions[0]["status"] == "completed"
        assert interactions[0]["summary"] == "Found 5 results"

    @pytest.mark.asyncio
    async def test_record_interaction_without_context(self, state_store):
        """Test recording interaction without existing context auto-creates it."""
        interaction_id = await state_store.record_interaction(
            context_id="auto-created",
            interaction_type="task",
            status="running"
        )

        assert interaction_id > 0

        # Context should have been created
        context = await state_store.get_context("auto-created")
        assert context is not None

    @pytest.mark.asyncio
    async def test_interaction_with_remote_run_id(self, state_store):
        """Test interactions with remote_run_id for updates."""
        await state_store.create_context(context_id="ctx-run")

        await state_store.record_interaction(
            context_id="ctx-run",
            interaction_type="task.run",
            status="queued",
            remote_run_id="remote-123"
        )

        # Update by remote_run_id
        await state_store.update_run_status(
            "remote-123",
            status="completed",
            output_payload={"final": "result"},
            summary="Task completed successfully"
        )

        interactions = await state_store.list_interactions("ctx-run")
        assert interactions[0]["status"] == "completed"
        assert interactions[0]["summary"] == "Task completed successfully"
        assert interactions[0]["output_payload"]["final"] == "result"

    @pytest.mark.asyncio
    async def test_update_run_without_existing_interaction(self, state_store):
        """Test that updating non-existent run is a no-op."""
        # Should not raise, just do nothing
        await state_store.update_run_status(
            "non-existent-run",
            status="failed",
            summary="This should be ignored"
        )

        # No error, no crash

    @pytest.mark.asyncio
    async def test_list_interactions_ordering_and_limit(self, state_store):
        """Test interaction listing respects limit and ordering."""
        await state_store.create_context(context_id="ctx-order")

        # Create interactions with slight delays
        for i in range(5):
            await state_store.record_interaction(
                context_id="ctx-order",
                interaction_type=f"type-{i}",
                status="completed",
                summary=f"Interaction {i}"
            )
            await asyncio.sleep(0.01)

        # Get limited recent interactions
        interactions = await state_store.list_interactions("ctx-order", limit=3)

        assert len(interactions) == 3
        # Most recent first
        assert interactions[0]["summary"] == "Interaction 4"
        assert interactions[1]["summary"] == "Interaction 3"
        assert interactions[2]["summary"] == "Interaction 2"

    @pytest.mark.asyncio
    async def test_interaction_json_serialization(self, state_store):
        """Test that complex JSON payloads are properly stored and retrieved."""
        complex_payload = {
            "nested": {
                "deep": {
                    "value": True,
                    "numbers": [1, 2.5, -3],
                    "null_field": None
                }
            },
            "unicode": "Test 🚀 emoji",
            "datetime": "2024-01-01T00:00:00Z"
        }

        await state_store.create_context(context_id="ctx-json")
        await state_store.record_interaction(
            context_id="ctx-json",
            interaction_type="test",
            status="completed",
            input_payload=complex_payload,
            output_payload={"response": complex_payload}
        )

        interactions = await state_store.list_interactions("ctx-json")
        retrieved_input = interactions[0]["input_payload"]
        retrieved_output = interactions[0]["output_payload"]

        assert retrieved_input["nested"]["deep"]["value"] is True
        assert retrieved_input["unicode"] == "Test 🚀 emoji"
        assert retrieved_output["response"]["nested"]["deep"]["numbers"] == [1, 2.5, -3]


class TestConcurrency:
    """Test concurrent operations."""

    @pytest.mark.asyncio
    async def test_concurrent_context_creation(self, state_store):
        """Test that multiple contexts can be created concurrently."""

        async def create_context(i: int):
            return await state_store.create_context(
                context_id=f"concurrent-{i}",
                name=f"Context {i}",
                metadata={"index": i}
            )

        # Create 10 contexts concurrently
        tasks = [create_context(i) for i in range(10)]
        contexts = await asyncio.gather(*tasks)

        assert len(contexts) == 10
        for i, ctx in enumerate(contexts):
            assert ctx["metadata"]["index"] == i

    @pytest.mark.asyncio
    async def test_concurrent_interaction_recording(self, state_store):
        """Test recording interactions from multiple async tasks."""
        await state_store.create_context(context_id="ctx-concurrent")

        async def record_interaction(i: int):
            return await state_store.record_interaction(
                context_id="ctx-concurrent",
                interaction_type="concurrent",
                status="completed",
                summary=f"Task {i}"
            )

        # Record 20 interactions concurrently
        tasks = [record_interaction(i) for i in range(20)]
        interaction_ids = await asyncio.gather(*tasks)

        assert len(set(interaction_ids)) == 20  # All unique IDs

        interactions = await state_store.list_interactions("ctx-concurrent", limit=100)
        assert len(interactions) == 20

    @pytest.mark.asyncio
    async def test_concurrent_updates_to_same_context(self, state_store):
        """Test that concurrent updates to same context don't corrupt data."""
        await state_store.create_context(
            context_id="ctx-race",
            state={"counter": 0, "values": []}
        )

        async def increment_counter(value: int):
            # Get current state
            ctx = await state_store.get_context("ctx-race")
            current = ctx["state"]["counter"]

            # Simulate processing delay
            await asyncio.sleep(0.001)

            # Update with incremented value
            return await state_store.update_context(
                "ctx-race",
                state={"counter": current + value}
            )

        # Run 5 concurrent increments
        tasks = [increment_counter(1) for _ in range(5)]
        await asyncio.gather(*tasks)

        final = await state_store.get_context("ctx-race")

        # Due to race conditions without locking, final counter
        # might not be 5, but should be between 1 and 5
        assert 1 <= final["state"]["counter"] <= 5

    @pytest.mark.asyncio
    async def test_concurrent_run_status_updates(self, state_store):
        """Test concurrent updates to different run statuses."""
        await state_store.create_context(context_id="ctx-runs")

        # Create multiple runs
        run_ids = []
        for i in range(5):
            await state_store.record_interaction(
                context_id="ctx-runs",
                interaction_type="task.run",
                status="running",
                remote_run_id=f"run-{i}"
            )
            run_ids.append(f"run-{i}")

        async def update_run(run_id: str, status: str):
            await state_store.update_run_status(
                run_id,
                status=status,
                summary=f"{run_id} is {status}"
            )

        # Update all runs concurrently
        tasks = [
            update_run(run_id, "completed" if i % 2 == 0 else "failed")
            for i, run_id in enumerate(run_ids)
        ]
        await asyncio.gather(*tasks)

        interactions = await state_store.list_interactions("ctx-runs", limit=10)
        completed = [i for i in interactions if i["status"] == "completed"]
        failed = [i for i in interactions if i["status"] == "failed"]

        assert len(completed) == 3  # runs 0, 2, 4
        assert len(failed) == 2  # runs 1, 3


class TestErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_malformed_json_in_database(self, temp_db_path):
        """Test handling of corrupted JSON in database."""
        store = ContextStateStore(str(temp_db_path))
        await store.initialize()

        # Manually insert bad JSON
        async with aiosqlite.connect(str(temp_db_path)) as db:
            await db.execute(
                """
                INSERT INTO contexts (context_id, name, metadata, state)
                VALUES (?, ?, ?, ?)
                """,
                ("bad-json", "Test", "{not valid json}", '{"valid": "json"}')
            )
            await db.commit()

        # Should handle gracefully
        context = await store.get_context("bad-json")
        assert context is not None
        # Metadata should be None or empty due to parse error
        assert context["metadata"] is None or context["metadata"] == {}
        # State should still work
        assert context["state"]["valid"] == "json"

        await store.close()

    @pytest.mark.asyncio
    async def test_extremely_long_context_id(self, state_store):
        """Test handling of very long context IDs."""
        long_id = "x" * 1000  # 1000 character ID

        context = await state_store.create_context(
            context_id=long_id,
            name="Long ID Test"
        )

        assert context["context_id"] == long_id

        retrieved = await state_store.get_context(long_id)
        assert retrieved["name"] == "Long ID Test"

    @pytest.mark.asyncio
    async def test_special_characters_in_fields(self, state_store):
        """Test handling of special characters in various fields."""
        special_chars = "'; DROP TABLE contexts; --"

        context = await state_store.create_context(
            context_id=special_chars,
            name=special_chars,
            metadata={"sql_injection": special_chars},
            description=special_chars
        )

        # Should handle SQL injection attempts safely
        assert context["context_id"] == special_chars
        assert context["metadata"]["sql_injection"] == special_chars

    @pytest.mark.asyncio
    async def test_unicode_and_emojis(self, state_store):
        """Test Unicode and emoji support."""
        await state_store.create_context(context_id="ctx-unicode")

        await state_store.record_interaction(
            context_id="ctx-unicode",
            interaction_type="测试",  # Chinese
            status="完成",
            summary="Test with emojis 🚀🎉 and Arabic نص عربي"
        )

        interactions = await state_store.list_interactions("ctx-unicode")
        assert interactions[0]["interaction_type"] == "测试"
        assert "🚀" in interactions[0]["summary"]

    @pytest.mark.asyncio
    async def test_database_locked_retry(self, state_store, monkeypatch):
        """Test that database lock errors are retried."""
        original_execute = state_store._conn.execute
        call_count = 0

        async def flaky_execute(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Simulate database locked error on first call
                raise aiosqlite.OperationalError("database is locked")
            return await original_execute(*args, **kwargs)

        monkeypatch.setattr(state_store._conn, "execute", flaky_execute)

        # Should succeed after retry
        context = await state_store.create_context(
            context_id="retry-test",
            name="Should work on retry"
        )

        assert context["name"] == "Should work on retry"
        assert call_count == 2  # First attempt failed, second succeeded

    @pytest.mark.asyncio
    async def test_close_and_reopen(self, temp_db_path):
        """Test closing and reopening the store maintains data."""
        store1 = ContextStateStore(str(temp_db_path))
        await store1.initialize()

        await store1.create_context(
            context_id="persist-test",
            metadata={"value": 42}
        )

        await store1.close()

        # Open new connection
        store2 = ContextStateStore(str(temp_db_path))
        await store2.initialize()

        context = await store2.get_context("persist-test")
        assert context["metadata"]["value"] == 42

        await store2.close()