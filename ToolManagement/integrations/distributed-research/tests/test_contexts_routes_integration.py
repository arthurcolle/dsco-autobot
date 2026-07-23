"""Integration tests for context routes with real workflows."""

import asyncio
from datetime import datetime

import httpx
import pytest

from parallel_websearch_server.models import ProcessorType, TaskRunStatus
from tests.fixtures import SearchResponseFactory, TaskRunFactory


class TestContextLifecycle:
    """Test complete context lifecycle through various operations."""

    @pytest.mark.asyncio
    async def test_context_creation_and_retrieval(self, test_client):
        """Test creating and retrieving a context."""
        # Create context
        create_response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "name": "Research Project Alpha",
                "description": "Tracking research on quantum computing",
                "metadata": {
                    "owner": "researcher-123",
                    "tags": ["quantum", "computing", "physics"]
                },
                "state": {
                    "phase": "initial",
                    "progress": 0
                }
            }
        )

        assert create_response.status_code == 201
        context = create_response.json()
        context_id = context["context_id"]
        assert context["name"] == "Research Project Alpha"

        # Retrieve context
        get_response = await test_client.get(f"/v1alpha/contexts/{context_id}")
        assert get_response.status_code == 200

        data = get_response.json()
        assert data["context"]["context_id"] == context_id
        assert data["context"]["metadata"]["owner"] == "researcher-123"
        assert data["history"] == []  # No interactions yet

    @pytest.mark.asyncio
    async def test_context_with_search_and_task_workflow(self, test_client, mock_transport):
        """Test a complete workflow: create context, run search, create task, check history."""

        # 1. Create context
        create_response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "context_id": "workflow-ctx",
                "name": "Complete Workflow Test",
                "metadata": {"workflow": "search-and-analyze"}
            }
        )
        assert create_response.status_code == 201

        # 2. Run search with context
        mock_transport.add_response(
            "/v1beta/search",
            httpx.Response(
                200,
                json=SearchResponseFactory.create_search_response(results_count=5)
            )
        )

        search_response = await test_client.post(
            "/v1beta/search?context_id=workflow-ctx",
            json={"objective": "Find information on AI safety"}
        )
        assert search_response.status_code == 200

        # 3. Create task run with context
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(
                    run_id="workflow-task-1",
                    status=TaskRunStatus.QUEUED
                )
            )
        )

        task_response = await test_client.post(
            "/v1/tasks/runs?context_id=workflow-ctx",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Analyze the search results"
            }
        )
        assert task_response.status_code == 200

        # 4. Check context history
        history_response = await test_client.get(
            "/v1alpha/contexts/workflow-ctx?history_limit=10"
        )
        assert history_response.status_code == 200

        data = history_response.json()
        history = data["history"]

        # Should have 2 interactions
        assert len(history) == 2

        # Most recent first (task, then search)
        assert history[0]["interaction_type"] == "task.run"
        assert history[0]["remote_run_id"] == "workflow-task-1"

        assert history[1]["interaction_type"] == "search"
        assert "5 results" in history[1]["summary"]

    @pytest.mark.asyncio
    async def test_context_update_merge_behavior(self, test_client):
        """Test context update with merge vs replace semantics."""

        # Create context
        create_response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "context_id": "update-test",
                "metadata": {"a": 1, "b": {"nested": "value"}},
                "state": {"items": [1, 2], "status": "active"}
            }
        )
        assert create_response.status_code == 201

        # Merge update (default)
        merge_response = await test_client.patch(
            "/v1alpha/contexts/update-test",
            json={
                "metadata": {"b": {"nested": "updated", "new": "field"}, "c": 3},
                "state": {"items": [3, 4, 5], "phase": "processing"}
            }
        )
        assert merge_response.status_code == 200

        merged = merge_response.json()
        # Check deep merge
        assert merged["metadata"]["a"] == 1  # Original preserved
        assert merged["metadata"]["b"]["nested"] == "updated"
        assert merged["metadata"]["b"]["new"] == "field"
        assert merged["metadata"]["c"] == 3

        assert merged["state"]["items"] == [3, 4, 5]  # Array replaced
        assert merged["state"]["status"] == "active"  # Original preserved
        assert merged["state"]["phase"] == "processing"  # New field

        # Replace update
        replace_response = await test_client.patch(
            "/v1alpha/contexts/update-test?replace_metadata=true&replace_state=true",
            json={
                "metadata": {"completely": "new"},
                "state": {"fresh": "state"}
            }
        )
        assert replace_response.status_code == 200

        replaced = replace_response.json()
        assert replaced["metadata"] == {"completely": "new"}
        assert replaced["state"] == {"fresh": "state"}

    @pytest.mark.asyncio
    async def test_context_not_found(self, test_client):
        """Test operations on non-existent context."""

        # Get non-existent
        response = await test_client.get("/v1alpha/contexts/does-not-exist")
        assert response.status_code == 404

        # Update non-existent
        response = await test_client.patch(
            "/v1alpha/contexts/does-not-exist",
            json={"metadata": {"test": "data"}}
        )
        assert response.status_code == 404

        # Get history for non-existent
        response = await test_client.get("/v1alpha/contexts/does-not-exist/history")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_context_listing(self, test_client):
        """Test listing contexts with pagination."""

        # Create multiple contexts
        for i in range(5):
            await test_client.post(
                "/v1alpha/contexts",
                json={
                    "context_id": f"list-ctx-{i}",
                    "name": f"Context {i}",
                    "metadata": {"index": i}
                }
            )
            await asyncio.sleep(0.01)  # Ensure different timestamps

        # List with default limit
        list_response = await test_client.get("/v1alpha/contexts")
        assert list_response.status_code == 200

        contexts = list_response.json()
        assert len(contexts) <= 20  # Default limit

        # List with custom limit
        limited_response = await test_client.get("/v1alpha/contexts?limit=3")
        assert limited_response.status_code == 200

        limited = limited_response.json()
        assert len(limited) == 3

        # Should be ordered by updated_at DESC
        # Most recently created should be first
        assert limited[0]["context_id"] == "list-ctx-4"

    @pytest.mark.asyncio
    async def test_context_history_pagination(self, test_client, mock_transport, state_store):
        """Test interaction history pagination."""

        context_id = "history-pagination-ctx"

        # Create context
        await test_client.post(
            "/v1alpha/contexts",
            json={"context_id": context_id, "name": "History Test"}
        )

        # Create many interactions directly
        for i in range(15):
            await state_store.record_interaction(
                context_id=context_id,
                interaction_type="test",
                status="completed",
                summary=f"Interaction {i}",
                input_payload={"index": i}
            )
            await asyncio.sleep(0.001)

        # Get limited history via context detail
        detail_response = await test_client.get(
            f"/v1alpha/contexts/{context_id}?history_limit=5"
        )
        assert detail_response.status_code == 200
        data = detail_response.json()
        assert len(data["history"]) == 5

        # Get full history via dedicated endpoint
        full_history_response = await test_client.get(
            f"/v1alpha/contexts/{context_id}/history"
        )
        assert full_history_response.status_code == 200
        full_history = full_history_response.json()
        assert len(full_history) == 15

        # With limit
        limited_history_response = await test_client.get(
            f"/v1alpha/contexts/{context_id}/history?limit=10"
        )
        assert limited_history_response.status_code == 200
        limited_history = limited_history_response.json()
        assert len(limited_history) == 10

        # Should be in reverse chronological order
        assert limited_history[0]["summary"] == "Interaction 14"
        assert limited_history[1]["summary"] == "Interaction 13"


class TestContextWithManagedTasks:
    """Test context integration with managed task runs."""

    @pytest.mark.asyncio
    async def test_managed_task_updates_context(self, test_client, mock_transport):
        """Test that managed task run updates context throughout lifecycle."""

        # Create context
        await test_client.post(
            "/v1alpha/contexts",
            json={"context_id": "managed-ctx", "name": "Managed Task Context"}
        )

        run_id = "managed-in-ctx"

        # Setup mocks for managed run
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
            )
        )

        # Status progression
        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.RUNNING)
            )
        )
        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.COMPLETED)
            )
        )

        # Final result
        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}/result",
            httpx.Response(
                200,
                json=TaskRunFactory.create_result(
                    run_id=run_id,
                    content="Analysis complete with 3 key findings"
                )
            )
        )

        # Run managed task
        managed_response = await test_client.post(
            "/v1/tasks/runs/managed?context_id=managed-ctx&poll_interval_seconds=0.01",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Analyze complex topic"
            }
        )
        assert managed_response.status_code == 200

        # Check context history shows final state
        history_response = await test_client.get("/v1alpha/contexts/managed-ctx")
        assert history_response.status_code == 200

        history = history_response.json()["history"]
        assert len(history) == 1
        assert history[0]["status"] == TaskRunStatus.COMPLETED.value
        assert "3 key findings" in history[0]["summary"]


class TestConcurrentContextOperations:
    """Test concurrent operations on contexts."""

    @pytest.mark.asyncio
    async def test_concurrent_updates_same_context(self, test_client):
        """Test concurrent updates to the same context."""

        # Create context
        await test_client.post(
            "/v1alpha/contexts",
            json={
                "context_id": "concurrent-update",
                "state": {"counter": 0}
            }
        )

        async def update_context(value: int):
            return await test_client.patch(
                "/v1alpha/contexts/concurrent-update",
                json={"state": {"values": [value]}}
            )

        # Concurrent updates
        tasks = [update_context(i) for i in range(5)]
        responses = await asyncio.gather(*tasks)

        for response in responses:
            assert response.status_code == 200

        # Final state should have merged all updates
        final_response = await test_client.get("/v1alpha/contexts/concurrent-update")
        final_state = final_response.json()["context"]["state"]

        # Due to merge behavior, should have counter and values
        assert "counter" in final_state
        assert "values" in final_state

    @pytest.mark.asyncio
    async def test_concurrent_context_creation(self, test_client):
        """Test creating multiple contexts concurrently."""

        async def create_context(i: int):
            return await test_client.post(
                "/v1alpha/contexts",
                json={
                    "name": f"Concurrent {i}",
                    "metadata": {"index": i}
                }
            )

        tasks = [create_context(i) for i in range(10)]
        responses = await asyncio.gather(*tasks)

        created_ids = []
        for response in responses:
            assert response.status_code == 201
            created_ids.append(response.json()["context_id"])

        # All IDs should be unique
        assert len(set(created_ids)) == 10


class TestContextValidation:
    """Test context input validation and error handling."""

    @pytest.mark.asyncio
    async def test_invalid_context_creation(self, test_client):
        """Test creating context with invalid data."""

        # Empty name (if required)
        response = await test_client.post(
            "/v1alpha/contexts",
            json={"name": ""}
        )
        # May or may not be invalid depending on schema
        assert response.status_code in [201, 422]

        # Invalid JSON in metadata
        response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "name": "Test",
                "metadata": "not a dict"  # Should be object/dict
            }
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_extremely_large_context_data(self, test_client):
        """Test context with very large metadata/state."""

        large_data = {"key" + str(i): "value" * 100 for i in range(1000)}

        response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "name": "Large Context",
                "metadata": large_data,
                "state": large_data
            }
        )

        # Should handle large payloads
        assert response.status_code == 201

        # Can retrieve
        context_id = response.json()["context_id"]
        get_response = await test_client.get(f"/v1alpha/contexts/{context_id}")
        assert get_response.status_code == 200

    @pytest.mark.asyncio
    async def test_special_characters_in_context_id(self, test_client):
        """Test context IDs with special characters."""

        special_ids = [
            "ctx-with-dashes",
            "ctx_with_underscores",
            "ctx.with.dots",
            "ctx:with:colons",
            "ctx/with/slashes",
            "ctx with spaces",
            "ctx-123-números",
            "ctx-中文-unicode"
        ]

        for ctx_id in special_ids:
            response = await test_client.post(
                "/v1alpha/contexts",
                json={
                    "context_id": ctx_id,
                    "name": f"Test {ctx_id}"
                }
            )

            # Should handle or reject gracefully
            assert response.status_code in [201, 422]

            if response.status_code == 201:
                # Should be retrievable
                get_response = await test_client.get(f"/v1alpha/contexts/{ctx_id}")
                assert get_response.status_code == 200