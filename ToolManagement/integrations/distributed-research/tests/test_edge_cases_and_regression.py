"""Edge cases, regression tests, and utility function tests."""

import json
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from parallel_websearch_server.models import (
    ProcessorType,
    TaskRun,
    TaskRunJsonOutput,
    TaskRunResult,
    TaskRunStatus,
    TaskRunTextOutput,
)
from parallel_websearch_server.routes.tasks import (
    _normalize_error_detail,
    _prepare_output_summary,
    _summarize_task_run,
)
from parallel_websearch_server.routes.search import (
    _summarize_search_results,
    _summarize_extract_results,
)
from tests.fixtures import SearchResponseFactory, TaskRunFactory


class TestUtilityFunctions:
    """Test utility and helper functions used across routes."""

    def test_normalize_error_detail_dict(self):
        """Test normalizing dict error details."""
        error = {"error": "Something failed", "code": 500}
        result = _normalize_error_detail(error)
        assert result == error

    def test_normalize_error_detail_string(self):
        """Test normalizing string error details."""
        error = "Simple error message"
        result = _normalize_error_detail(error)
        assert result == {"error": error}

    def test_normalize_error_detail_other_types(self):
        """Test normalizing other types."""
        result = _normalize_error_detail(None)
        assert result == {"error": None}

        result = _normalize_error_detail(123)
        assert result == {"error": 123}

    def test_summarize_search_results_basic(self):
        """Test search results summarization."""
        results = SearchResponseFactory.create_search_response(results_count=5)
        summary = _summarize_search_results(results)

        assert "5 results" in summary.lower()
        assert "result 1" in summary or "5" in summary

    def test_summarize_search_results_empty(self):
        """Test summarization with no results."""
        results = {"results": []}
        summary = _summarize_search_results(results)

        assert "no results" in summary.lower() or "0" in summary

    def test_summarize_extract_results_basic(self):
        """Test extract results summarization."""
        results = SearchResponseFactory.create_extract_response(
            ["https://example.com", "https://test.org"]
        )
        summary = _summarize_extract_results(results)

        assert "2" in summary or "items" in summary.lower()

    def test_summarize_extract_results_empty(self):
        """Test extract summarization with no items."""
        results = {"items": []}
        summary = _summarize_extract_results(results)

        assert "no" in summary.lower() or "0" in summary

    def test_summarize_task_run_queued(self):
        """Test task run summarization for queued status."""
        run_dict = TaskRunFactory.create(status=TaskRunStatus.QUEUED)
        run = TaskRun(**run_dict)
        summary = _summarize_task_run(run)

        assert "queued" in summary.lower() or "created" in summary.lower()

    def test_summarize_task_run_completed(self):
        """Test task run summarization for completed status."""
        run_dict = TaskRunFactory.create(status=TaskRunStatus.COMPLETED)
        run = TaskRun(**run_dict)
        summary = _summarize_task_run(run)

        assert "completed" in summary.lower()

    def test_summarize_task_run_failed(self):
        """Test task run summarization for failed status."""
        run_dict = TaskRunFactory.create(status=TaskRunStatus.FAILED)
        run = TaskRun(**run_dict)
        summary = _summarize_task_run(run)

        assert "failed" in summary.lower()

    def test_prepare_output_summary_text(self):
        """Test output preparation with text output."""
        result_dict = TaskRunFactory.create_result(
            output_type="text",
            content="This is a long analysis that should be summarized. " * 10
        )

        # Manually construct TaskRunResult since we're testing the function
        run_data = result_dict["run"]
        output_data = result_dict["output"]

        run = TaskRun(**run_data)
        output = TaskRunTextOutput(**output_data)
        result = TaskRunResult(run=run, output=output)

        output_text, output_json, summary = _prepare_output_summary(result)

        assert output_text.startswith("This is a long analysis")
        assert output_json is None
        assert "completed" in summary.lower() or len(summary) > 0

    def test_prepare_output_summary_json(self):
        """Test output preparation with JSON output."""
        result_dict = TaskRunFactory.create_result(
            output_type="json",
            content={
                "findings": ["finding 1", "finding 2"],
                "confidence": 0.95
            }
        )

        run_data = result_dict["run"]
        output_data = result_dict["output"]

        run = TaskRun(**run_data)
        output = TaskRunJsonOutput(**output_data)
        result = TaskRunResult(run=run, output=output)

        output_text, output_json, summary = _prepare_output_summary(result)

        assert output_text is None
        assert output_json["confidence"] == 0.95
        assert len(summary) > 0

    def test_prepare_output_summary_no_output(self):
        """Test output preparation with no output."""
        run_dict = TaskRunFactory.create(status=TaskRunStatus.FAILED)
        run = TaskRun(**run_dict)
        result = TaskRunResult(run=run, output=None)

        output_text, output_json, summary = _prepare_output_summary(result)

        assert output_text is None
        assert output_json is None
        assert "no output" in summary.lower() or "failed" in summary.lower()


class TestHTTPErrorHandling:
    """Test HTTP error handling across routes."""

    @pytest.mark.asyncio
    async def test_404_handling(self, test_client):
        """Test 404 responses for non-existent resources."""
        response = await test_client.get("/v1/does-not-exist")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_method_not_allowed(self, test_client):
        """Test method not allowed responses."""
        # Try GET on POST-only endpoint
        response = await test_client.get("/v1beta/search")
        assert response.status_code == 405

    @pytest.mark.asyncio
    async def test_malformed_json_request(self, test_client):
        """Test handling of malformed JSON in request body."""
        response = await test_client.post(
            "/v1beta/search",
            content=b"{invalid json",
            headers={"content-type": "application/json"}
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_request_body(self, test_client):
        """Test handling of empty request body where body is required."""
        response = await test_client.post(
            "/v1beta/search",
            json={}
        )
        assert response.status_code == 422  # Missing required fields


class TestContextAutoCreation:
    """Test automatic context creation when context_id is provided."""

    @pytest.mark.asyncio
    async def test_search_auto_creates_context(self, test_client, mock_transport, state_store):
        """Test that search auto-creates context if it doesn't exist."""
        mock_transport.add_response(
            "/v1beta/search",
            httpx.Response(
                200,
                json=SearchResponseFactory.create_search_response(results_count=1)
            )
        )

        response = await test_client.post(
            "/v1beta/search?context_id=auto-created-ctx",
            json={"objective": "test query"}
        )

        assert response.status_code == 200

        # Context should exist now
        context = await state_store.get_context("auto-created-ctx")
        assert context is not None

    @pytest.mark.asyncio
    async def test_task_auto_creates_context(self, test_client, mock_transport, state_store):
        """Test that task runs auto-create context if it doesn't exist."""
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id="auto-ctx-run")
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs?context_id=auto-task-ctx",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "test input"
            }
        )

        assert response.status_code == 200

        # Context should exist
        context = await state_store.get_context("auto-task-ctx")
        assert context is not None


class TestSerializationEdgeCases:
    """Test serialization edge cases and datetime handling."""

    @pytest.mark.asyncio
    async def test_datetime_serialization_in_context(self, test_client):
        """Test that datetime fields are properly serialized."""
        response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "name": "Datetime Test",
                "metadata": {
                    "created_at": "2024-01-01T00:00:00Z",
                    "scheduled_for": "2024-12-31T23:59:59Z"
                }
            }
        )

        assert response.status_code == 201
        context = response.json()

        # Should be able to retrieve without serialization errors
        get_response = await test_client.get(f"/v1alpha/contexts/{context['context_id']}")
        assert get_response.status_code == 200

    @pytest.mark.asyncio
    async def test_nested_json_serialization(self, test_client, mock_transport, state_store):
        """Test deeply nested JSON structures in interactions."""
        deeply_nested = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "level5": {
                                "data": "deep value",
                                "array": [1, 2, 3],
                                "mixed": [{"a": 1}, {"b": 2}]
                            }
                        }
                    }
                }
            }
        }

        # Record directly to test serialization
        await state_store.record_interaction(
            context_id="nested-json-ctx",
            interaction_type="test",
            status="completed",
            input_payload=deeply_nested,
            output_payload=deeply_nested,
            summary="Nested JSON test"
        )

        # Should be able to retrieve
        interactions = await state_store.list_interactions("nested-json-ctx")
        assert interactions[0]["input_payload"]["level1"]["level2"]["level3"]["level4"]["level5"]["data"] == "deep value"


class TestRaceConditions:
    """Test potential race conditions and timing issues."""

    @pytest.mark.asyncio
    async def test_rapid_status_updates_same_run(self, state_store):
        """Test rapid status updates to same run don't corrupt data."""
        import asyncio

        # Create initial interaction
        await state_store.record_interaction(
            context_id="race-ctx",
            interaction_type="task.run",
            status="queued",
            remote_run_id="race-run",
            summary="Initial"
        )

        # Rapid concurrent updates
        async def update_status(status: str):
            await state_store.update_run_status(
                "race-run",
                status=status,
                summary=f"Status: {status}"
            )

        statuses = ["running", "processing", "finalizing", "completed"]
        tasks = [update_status(s) for s in statuses]
        await asyncio.gather(*tasks)

        # Final status should be one of the updates
        interactions = await state_store.list_interactions("race-ctx")
        assert interactions[0]["status"] in statuses

    @pytest.mark.asyncio
    async def test_context_read_during_write(self, test_client, mock_transport):
        """Test reading context while it's being written to."""
        import asyncio

        # Create context
        await test_client.post(
            "/v1alpha/contexts",
            json={"context_id": "read-write-race", "name": "Race Test"}
        )

        # Mock for search
        mock_transport.add_response(
            "/v1beta/search",
            httpx.Response(
                200,
                json=SearchResponseFactory.create_search_response(results_count=1)
            )
        )

        async def write_context():
            """Write to context via search."""
            return await test_client.post(
                "/v1beta/search?context_id=read-write-race",
                json={"objective": "concurrent test"}
            )

        async def read_context():
            """Read context details."""
            await asyncio.sleep(0.001)  # Small delay
            return await test_client.get("/v1alpha/contexts/read-write-race")

        # Execute concurrently
        write_task = asyncio.create_task(write_context())
        read_task = asyncio.create_task(read_context())

        write_response, read_response = await asyncio.gather(write_task, read_task)

        # Both should succeed
        assert write_response.status_code == 200
        assert read_response.status_code == 200


class TestResourceLimits:
    """Test behavior at resource limits."""

    @pytest.mark.asyncio
    async def test_many_contexts(self, test_client):
        """Test creating many contexts."""
        context_ids = []

        # Create 50 contexts
        for i in range(50):
            response = await test_client.post(
                "/v1alpha/contexts",
                json={
                    "name": f"Context {i}",
                    "metadata": {"index": i}
                }
            )
            assert response.status_code == 201
            context_ids.append(response.json()["context_id"])

        # Should be able to list and retrieve all
        list_response = await test_client.get("/v1alpha/contexts?limit=100")
        assert list_response.status_code == 200
        contexts = list_response.json()
        assert len(contexts) >= 50

    @pytest.mark.asyncio
    async def test_many_interactions_per_context(self, state_store):
        """Test a single context with many interactions."""
        context_id = "many-interactions-ctx"

        # Create 100 interactions
        for i in range(100):
            await state_store.record_interaction(
                context_id=context_id,
                interaction_type="test",
                status="completed",
                summary=f"Interaction {i}",
                input_payload={"index": i}
            )

        # Should be able to retrieve all
        interactions = await state_store.list_interactions(context_id, limit=200)
        assert len(interactions) == 100

    @pytest.mark.asyncio
    async def test_extremely_long_summary(self, state_store):
        """Test interaction with very long summary text."""
        long_summary = "x" * 10000  # 10KB summary

        await state_store.record_interaction(
            context_id="long-summary-ctx",
            interaction_type="test",
            status="completed",
            summary=long_summary
        )

        interactions = await state_store.list_interactions("long-summary-ctx")
        assert len(interactions[0]["summary"]) == 10000


class TestErrorRecovery:
    """Test error recovery and resilience."""

    @pytest.mark.asyncio
    async def test_partial_interaction_recording(self, state_store):
        """Test that partial interaction data is still recorded."""
        # Record with minimal data
        interaction_id = await state_store.record_interaction(
            context_id="minimal-ctx",
            interaction_type="test",
            status="completed"
            # No input_payload, output_payload, or summary
        )

        assert interaction_id > 0

        interactions = await state_store.list_interactions("minimal-ctx")
        assert len(interactions) == 1
        assert interactions[0]["interaction_type"] == "test"

    @pytest.mark.asyncio
    async def test_context_update_after_error(self, test_client, mock_transport, state_store):
        """Test that context can be updated after an error interaction."""
        # First, record a failed interaction
        await state_store.record_interaction(
            context_id="recovery-ctx",
            interaction_type="search",
            status="failed",
            summary="Failed attempt"
        )

        # Now successfully update the context
        update_response = await test_client.patch(
            "/v1alpha/contexts/recovery-ctx",
            json={
                "state": {"recovered": True, "retry_count": 1}
            }
        )

        assert update_response.status_code == 200

        # Should have both the error and the update
        history_response = await test_client.get("/v1alpha/contexts/recovery-ctx")
        data = history_response.json()

        assert data["context"]["state"]["recovered"] is True
        assert len(data["history"]) == 1  # The failed interaction


class TestBackwardsCompatibility:
    """Test backwards compatibility scenarios."""

    @pytest.mark.asyncio
    async def test_search_without_context_id(self, test_client, mock_transport):
        """Test that search still works without context_id (backwards compatible)."""
        mock_transport.add_response(
            "/v1beta/search",
            httpx.Response(
                200,
                json=SearchResponseFactory.create_search_response(results_count=2)
            )
        )

        response = await test_client.post(
            "/v1beta/search",  # No context_id query param
            json={"objective": "test without context"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2

    @pytest.mark.asyncio
    async def test_task_without_context_id(self, test_client, mock_transport):
        """Test that task runs work without context_id."""
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id="no-ctx-run")
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs",  # No context_id
            json={
                "processor": ProcessorType.LITE.value,
                "input": "test without context"
            }
        )

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_managed_run_without_context_id(self, test_client, mock_transport):
        """Test that managed runs work without context_id."""
        run_id = "no-ctx-managed"

        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
            )
        )

        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.COMPLETED)
            )
        )

        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}/result",
            httpx.Response(
                200,
                json=TaskRunFactory.create_result(run_id=run_id, content="Done")
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs/managed?poll_interval_seconds=0.01",  # No context_id
            json={
                "processor": ProcessorType.LITE.value,
                "input": "managed without context"
            }
        )

        assert response.status_code == 200


class TestDocumentationExamples:
    """Test that examples from documentation actually work."""

    @pytest.mark.asyncio
    async def test_readme_context_creation_example(self, test_client):
        """Test context creation example from README."""
        # Example from README
        response = await test_client.post(
            "/v1alpha/contexts",
            json={
                "name": "clinical-trials-tracker",
                "metadata": {"owner": "agent-7"}
            }
        )

        assert response.status_code == 201
        assert response.json()["name"] == "clinical-trials-tracker"

    @pytest.mark.asyncio
    async def test_readme_search_with_context_example(self, test_client, mock_transport):
        """Test search with context example from README."""
        # First create context
        await test_client.post(
            "/v1alpha/contexts",
            json={
                "context_id": "clinical-trials-tracker",
                "name": "Clinical Trials Tracker"
            }
        )

        # Mock search response
        mock_transport.add_response(
            "/v1beta/search",
            httpx.Response(
                200,
                json=SearchResponseFactory.create_search_response(results_count=5)
            )
        )

        # Example from README
        response = await test_client.post(
            "/v1beta/search?context_id=clinical-trials-tracker",
            json={
                "objective": "latest phase 3 oncology trials",
                "max_results": 5
            }
        )

        assert response.status_code == 200
        assert len(response.json()["results"]) == 5

    @pytest.mark.asyncio
    async def test_readme_managed_task_example(self, test_client, mock_transport):
        """Test managed task example from README."""
        run_id = "fda-approval-run"

        # Setup mocks
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
            )
        )

        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.COMPLETED)
            )
        )

        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}/result",
            httpx.Response(
                200,
                json=TaskRunFactory.create_result(
                    run_id=run_id,
                    content="3 recent FDA oncology approvals mapped"
                )
            )
        )

        # Example from README
        response = await test_client.post(
            "/v1/tasks/runs/managed?context_id=clinical-trials-tracker&poll_interval_seconds=0.01",
            json={
                "processor": "lite",
                "input": "Map the 3 most recent FDA oncology approvals"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["final_status"] == TaskRunStatus.COMPLETED.value