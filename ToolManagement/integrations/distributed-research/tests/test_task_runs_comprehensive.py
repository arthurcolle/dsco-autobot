"""Comprehensive tests for task runs API including managed runs and SSE."""

import asyncio
import json
from datetime import datetime, timedelta

import httpx
import pytest

from parallel_websearch_server.models import ProcessorType, TaskRunStatus
from tests.fixtures import TaskRunFactory, assert_interaction_recorded


class TestCreateTaskRun:
    """Test POST /v1/tasks/runs endpoint."""

    @pytest.mark.asyncio
    async def test_create_task_run_success(self, test_client, mock_transport):
        """Test successful task run creation."""
        mock_response = httpx.Response(
            200,
            json=TaskRunFactory.create(
                run_id="new-run-123",
                status=TaskRunStatus.QUEUED
            ),
        )
        mock_transport.add_response("/v1/tasks/runs", mock_response)

        response = await test_client.post(
            "/v1/tasks/runs",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Analyze this text"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "new-run-123"
        assert data["status"] == TaskRunStatus.QUEUED.value

    @pytest.mark.asyncio
    async def test_create_task_run_with_context(self, test_client, mock_transport, state_store):
        """Test task run creation with context caching."""
        mock_response = httpx.Response(
            200,
            json=TaskRunFactory.create(run_id="ctx-run-456"),
        )
        mock_transport.add_response("/v1/tasks/runs", mock_response)

        response = await test_client.post(
            "/v1/tasks/runs?context_id=task-ctx-1",
            json={
                "processor": ProcessorType.PRO.value,
                "input": {"text": "Extract key points"}
            }
        )

        assert response.status_code == 200

        # Check interaction was recorded
        interactions = await state_store.list_interactions("task-ctx-1")
        interaction = assert_interaction_recorded(
            interactions,
            interaction_type="task.run",
            status=TaskRunStatus.QUEUED.value,
            summary_contains="Task run created"
        )

        assert interaction["remote_run_id"] == "ctx-run-456"

    @pytest.mark.asyncio
    async def test_create_task_run_validation_error(self, test_client):
        """Test task run creation with invalid payload."""
        response = await test_client.post(
            "/v1/tasks/runs",
            json={
                "invalid_field": "bad data"
                # Missing required processor and input
            }
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_create_task_run_upstream_error(self, test_client, mock_transport, state_store):
        """Test handling of upstream errors during creation."""
        mock_response = httpx.Response(
            503,
            json={"error": "Service temporarily unavailable"},
        )
        mock_transport.add_response("/v1/tasks/runs", mock_response)

        response = await test_client.post(
            "/v1/tasks/runs?context_id=task-error-ctx",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Test input"
            }
        )

        assert response.status_code == 503

        # Error should be cached
        interactions = await state_store.list_interactions("task-error-ctx")
        assert interactions[0]["status"] == TaskRunStatus.FAILED.value

    @pytest.mark.asyncio
    async def test_create_task_run_with_options(self, test_client, mock_transport):
        """Test task run creation with all optional parameters."""
        mock_response = httpx.Response(
            200,
            json=TaskRunFactory.create(),
        )
        mock_transport.add_response("/v1/tasks/runs", mock_response)

        response = await test_client.post(
            "/v1/tasks/runs",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Complex task",
                "options": {
                    "max_depth": 3,
                    "include_sources": True,
                    "language": "en"
                },
                "metadata": {
                    "user_id": "user-123",
                    "session": "sess-456"
                }
            }
        )

        assert response.status_code == 200

        # Verify options were sent
        request = mock_transport.requests[0]
        request_body = json.loads(request.content)
        assert request_body["options"]["max_depth"] == 3


class TestGetTaskRun:
    """Test GET /v1/tasks/runs/{run_id} endpoint."""

    @pytest.mark.asyncio
    async def test_get_task_run_success(self, test_client, mock_transport):
        """Test getting task run status."""
        mock_response = httpx.Response(
            200,
            json=TaskRunFactory.create(
                run_id="get-run-123",
                status=TaskRunStatus.RUNNING
            ),
        )
        mock_transport.add_response("/v1/tasks/runs/get-run-123", mock_response)

        response = await test_client.get("/v1/tasks/runs/get-run-123")

        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "get-run-123"
        assert data["status"] == TaskRunStatus.RUNNING.value

    @pytest.mark.asyncio
    async def test_get_task_run_not_found(self, test_client, mock_transport):
        """Test getting non-existent task run."""
        mock_response = httpx.Response(
            404,
            json={"error": "Task run not found"},
        )
        mock_transport.add_response("/v1/tasks/runs/missing", mock_response)

        response = await test_client.get("/v1/tasks/runs/missing")
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_get_task_run_status_progression(self, test_client, mock_transport, state_store):
        """Test tracking status progression with context."""
        # First, record initial interaction
        await state_store.record_interaction(
            context_id="status-ctx",
            interaction_type="task.run",
            status=TaskRunStatus.QUEUED.value,
            remote_run_id="status-run-123",
            summary="Task queued"
        )

        # Simulate status progression
        statuses = [
            TaskRunStatus.RUNNING,
            TaskRunStatus.PROCESSING,
            TaskRunStatus.COMPLETED
        ]

        for status in statuses:
            mock_response = httpx.Response(
                200,
                json=TaskRunFactory.create(
                    run_id="status-run-123",
                    status=status
                ),
            )
            mock_transport.add_response("/v1/tasks/runs/status-run-123", mock_response)

            response = await test_client.get("/v1/tasks/runs/status-run-123")
            assert response.status_code == 200
            assert response.json()["status"] == status.value


class TestGetTaskRunResult:
    """Test GET /v1/tasks/runs/{run_id}/result endpoint."""

    @pytest.mark.asyncio
    async def test_get_result_text_output(self, test_client, mock_transport, state_store):
        """Test getting task result with text output."""
        # Setup initial run in context
        await state_store.record_interaction(
            context_id="result-ctx",
            interaction_type="task.run",
            status=TaskRunStatus.RUNNING.value,
            remote_run_id="result-run-123",
            summary="Task running"
        )

        mock_response = httpx.Response(
            200,
            json=TaskRunFactory.create_result(
                run_id="result-run-123",
                status=TaskRunStatus.COMPLETED,
                output_type="text",
                content="This is the final analysis result."
            ),
        )
        mock_transport.add_response("/v1/tasks/runs/result-run-123/result", mock_response)

        response = await test_client.get("/v1/tasks/runs/result-run-123/result")

        assert response.status_code == 200
        data = response.json()
        assert data["run"]["status"] == TaskRunStatus.COMPLETED.value
        assert data["output"]["type"] == "text"
        assert "final analysis" in data["output"]["content"]

        # Check status was updated in context
        interactions = await state_store.list_interactions("result-ctx")
        assert interactions[0]["status"] == TaskRunStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_get_result_json_output(self, test_client, mock_transport):
        """Test getting task result with JSON output."""
        mock_response = httpx.Response(
            200,
            json=TaskRunFactory.create_result(
                run_id="json-run-123",
                output_type="json",
                content={
                    "summary": "Analysis complete",
                    "findings": ["Finding 1", "Finding 2"],
                    "confidence": 0.95
                }
            ),
        )
        mock_transport.add_response("/v1/tasks/runs/json-run-123/result", mock_response)

        response = await test_client.get("/v1/tasks/runs/json-run-123/result")

        assert response.status_code == 200
        data = response.json()
        assert data["output"]["type"] == "json"
        assert data["output"]["content"]["confidence"] == 0.95

    @pytest.mark.asyncio
    async def test_get_result_no_output(self, test_client, mock_transport):
        """Test task result with no output (e.g., failed task)."""
        mock_response = httpx.Response(
            200,
            json={
                "run": TaskRunFactory.create(
                    run_id="failed-run-123",
                    status=TaskRunStatus.FAILED
                ),
                "output": None
            },
        )
        mock_transport.add_response("/v1/tasks/runs/failed-run-123/result", mock_response)

        response = await test_client.get("/v1/tasks/runs/failed-run-123/result")

        assert response.status_code == 200
        data = response.json()
        assert data["run"]["status"] == TaskRunStatus.FAILED.value
        assert data["output"] is None

    @pytest.mark.asyncio
    async def test_get_result_still_running(self, test_client, mock_transport):
        """Test getting result for still-running task."""
        mock_response = httpx.Response(
            200,
            json={
                "run": TaskRunFactory.create(
                    run_id="running-run-123",
                    status=TaskRunStatus.RUNNING
                ),
                "output": None,
                "message": "Task is still running"
            },
        )
        mock_transport.add_response("/v1/tasks/runs/running-run-123/result", mock_response)

        response = await test_client.get("/v1/tasks/runs/running-run-123/result")

        assert response.status_code == 200
        data = response.json()
        assert data["run"]["status"] == TaskRunStatus.RUNNING.value


class TestManagedTaskRun:
    """Test POST /v1/tasks/runs/managed endpoint."""

    @pytest.mark.asyncio
    async def test_managed_run_success_flow(self, test_client, mock_transport, state_store):
        """Test successful managed task run from creation to completion."""
        run_id = "managed-123"

        # Mock creation response
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
            )
        )

        # Mock status check responses (simulating progression)
        statuses = [
            TaskRunStatus.RUNNING,
            TaskRunStatus.PROCESSING,
            TaskRunStatus.COMPLETED
        ]
        for status in statuses:
            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=run_id, status=status)
                )
            )

        # Mock final result
        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}/result",
            httpx.Response(
                200,
                json=TaskRunFactory.create_result(
                    run_id=run_id,
                    status=TaskRunStatus.COMPLETED,
                    content="Managed task completed successfully"
                )
            )
        )

        # Execute managed run
        response = await test_client.post(
            "/v1/tasks/runs/managed?context_id=managed-ctx&poll_interval_seconds=0.01&timeout_seconds=10",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Managed research task"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["run"]["run_id"] == run_id
        assert data["final_status"] == TaskRunStatus.COMPLETED.value
        assert data["output_text"] == "Managed task completed successfully"
        assert len(data["status_updates"]) > 0

        # Check context interaction
        interactions = await state_store.list_interactions("managed-ctx")
        assert interactions[0]["status"] == TaskRunStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_managed_run_timeout(self, test_client, mock_transport, state_store):
        """Test managed task run timeout."""
        run_id = "timeout-run"

        # Mock creation
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
            )
        )

        # Keep returning RUNNING status to trigger timeout
        for _ in range(10):
            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.RUNNING)
                )
            )

        response = await test_client.post(
            "/v1/tasks/runs/managed?context_id=timeout-ctx&poll_interval_seconds=0.01&timeout_seconds=0.05",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Will timeout"
            }
        )

        assert response.status_code == 504
        error = response.json()
        assert "timeout" in error["detail"]["error"].lower()

        # Timeout should be recorded
        interactions = await state_store.list_interactions("timeout-ctx")
        assert interactions[0]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_managed_run_failure_during_execution(self, test_client, mock_transport, state_store):
        """Test managed run that fails during execution."""
        run_id = "failed-managed"

        # Mock creation
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
            )
        )

        # Progress then fail
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
                json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.FAILED)
            )
        )

        # Mock failed result
        mock_transport.add_response(
            f"/v1/tasks/runs/{run_id}/result",
            httpx.Response(
                200,
                json={
                    "run": TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.FAILED),
                    "output": None,
                    "error": "Task processing failed"
                }
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs/managed?context_id=failed-ctx&poll_interval_seconds=0.01",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Will fail"
            }
        )

        assert response.status_code == 200  # Managed endpoint returns 200 even for failed tasks
        data = response.json()
        assert data["final_status"] == TaskRunStatus.FAILED.value
        assert data["output_text"] is None or data["output_text"] == ""

    @pytest.mark.asyncio
    async def test_managed_run_invalid_processor(self, test_client):
        """Test managed run with invalid processor."""
        response = await test_client.post(
            "/v1/tasks/runs/managed",
            json={
                "processor": "invalid-processor",
                "input": "Test"
            }
        )

        assert response.status_code == 422


class TestTaskRunEvents:
    """Test SSE events endpoint for task runs."""

    @pytest.mark.asyncio
    async def test_events_streaming(self, test_client, mock_transport):
        """Test SSE event streaming for task run."""
        # Create SSE response with events
        events = [
            'data: {"event": "status", "data": {"status": "running"}}\n\n',
            'data: {"event": "progress", "data": {"percent": 50}}\n\n',
            'data: {"event": "status", "data": {"status": "completed"}}\n\n',
        ]

        # Mock SSE stream response
        async def stream_generator():
            for event in events:
                yield event.encode()

        mock_response = httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=b"".join([e.encode() for e in events])
        )
        mock_transport.add_response("/v1/tasks/runs/sse-run/events", mock_response)

        response = await test_client.get("/v1/tasks/runs/sse-run/events")

        # Note: Testing SSE with TestClient is limited
        # In real usage, would need to properly consume the stream
        assert response.status_code == 200


class TestConcurrentTaskRuns:
    """Test concurrent task run operations."""

    @pytest.mark.asyncio
    async def test_concurrent_task_creation(self, test_client, mock_transport, state_store):
        """Test creating multiple task runs concurrently."""
        # Program mock responses
        for i in range(5):
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=f"concurrent-{i}")
                )
            )

        async def create_task(i: int):
            return await test_client.post(
                f"/v1/tasks/runs?context_id=concurrent-tasks-ctx",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Task {i}"
                }
            )

        # Create tasks concurrently
        tasks = [create_task(i) for i in range(5)]
        responses = await asyncio.gather(*tasks)

        for response in responses:
            assert response.status_code == 200

        # All should be recorded in context
        interactions = await state_store.list_interactions("concurrent-tasks-ctx")
        assert len(interactions) == 5

    @pytest.mark.asyncio
    async def test_concurrent_managed_runs(self, test_client, mock_transport):
        """Test multiple managed runs executing concurrently."""
        # Setup mocks for 2 concurrent managed runs
        for run_num in range(2):
            run_id = f"parallel-{run_num}"

            # Creation
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
                )
            )

            # Status checks
            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.COMPLETED)
                )
            )

            # Result
            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}/result",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create_result(
                        run_id=run_id,
                        content=f"Result for run {run_num}"
                    )
                )
            )

        async def run_managed(num: int):
            return await test_client.post(
                f"/v1/tasks/runs/managed?poll_interval_seconds=0.01",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Parallel task {num}"
                }
            )

        # Execute in parallel
        tasks = [run_managed(i) for i in range(2)]
        responses = await asyncio.gather(*tasks)

        for i, response in enumerate(responses):
            assert response.status_code == 200
            data = response.json()
            assert f"Result for run {i}" in data["output_text"]


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_extremely_long_input(self, test_client, mock_transport):
        """Test task run with very large input."""
        large_input = "x" * 100000  # 100KB of text

        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id="large-input-run")
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs",
            json={
                "processor": ProcessorType.LITE.value,
                "input": large_input
            }
        )

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_special_characters_in_run_id(self, test_client, mock_transport):
        """Test handling of special characters in run ID."""
        special_id = "run-with-special-!@#$%^&*()-chars"

        mock_transport.add_response(
            f"/v1/tasks/runs/{special_id}",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id=special_id)
            )
        )

        response = await test_client.get(f"/v1/tasks/runs/{special_id}")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_network_interruption_during_managed_run(self, test_client, mock_transport):
        """Test network error during managed run polling."""
        # Mock creation success
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id="network-fail", status=TaskRunStatus.QUEUED)
            )
        )

        # First status check succeeds
        mock_transport.add_response(
            "/v1/tasks/runs/network-fail",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id="network-fail", status=TaskRunStatus.RUNNING)
            )
        )

        # Then network error
        mock_transport.set_error(httpx.NetworkError("Connection lost"))

        response = await test_client.post(
            "/v1/tasks/runs/managed?poll_interval_seconds=0.01&timeout_seconds=1",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Network test"
            }
        )

        assert response.status_code in [502, 504]  # Gateway error or timeout