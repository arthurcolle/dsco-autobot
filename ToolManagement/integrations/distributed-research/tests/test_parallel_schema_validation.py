"""Comprehensive parallel tests for schema validation and edge cases in manage_task_run.

This test suite focuses on the validation errors seen in production:
- Auto schema not supported for certain processors
- Missing required fields
- Invalid schema structures
- High concurrency with mixed valid/invalid requests
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, List

import httpx
import pytest

from parallel_websearch_server.models import ProcessorType, TaskRunStatus
from tests.fixtures import TaskRunFactory


class TestSchemaValidationErrors:
    """Test schema validation edge cases that cause 422 errors."""

    @pytest.mark.asyncio
    async def test_auto_schema_with_base_processor(self, test_client, mock_transport):
        """Test that AutoSchema with 'base' processor returns validation error."""
        # Mock upstream API rejecting AutoSchema for 'base' processor
        mock_response = httpx.Response(
            422,
            json={
                "detail": {
                    "type": "error",
                    "error": {
                        "ref_id": "5c9597f-1b23-472e-9360-5c35bdc91c7f",
                        "message": "Auto schema is not supported for processor base.",
                        "detail": {}
                    }
                }
            },
        )
        mock_transport.add_response("/v1/tasks/runs", mock_response)

        response = await test_client.post(
            "/v1/tasks/runs/managed",
            json={
                "processor": ProcessorType.BASE.value,
                "input": "Test input",
                "task_spec": {
                    "output_schema": {
                        "type": "auto"
                    }
                }
            }
        )

        assert response.status_code == 422
        error_detail = response.json()
        assert "auto schema" in error_detail["detail"]["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_missing_required_fields(self, test_client):
        """Test validation error when required fields are missing."""
        response = await test_client.post(
            "/v1/tasks/runs/managed",
            json={
                # Missing 'processor' and 'input'
                "task_spec": {
                    "output_schema": {
                        "type": "json"
                    }
                }
            }
        )

        assert response.status_code == 422
        error = response.json()
        assert "detail" in error

    @pytest.mark.asyncio
    async def test_invalid_processor_type(self, test_client):
        """Test validation error with invalid processor type."""
        response = await test_client.post(
            "/v1/tasks/runs/managed",
            json={
                "processor": "invalid-processor-type",
                "input": "Test input"
            }
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_malformed_json_schema(self, test_client, mock_transport):
        """Test malformed JsonSchema structure."""
        mock_response = httpx.Response(
            422,
            json={
                "detail": {
                    "type": "validation_error",
                    "errors": [{
                        "type": "missing",
                        "loc": ["body", "task_spec", "output_schema", "JsonSchema", "json_schema"],
                        "msg": "Field required",
                        "input": {
                            "type": "object",
                            "properties": "invalid"  # Should be dict, not string
                        }
                    }]
                }
            },
        )
        mock_transport.add_response("/v1/tasks/runs", mock_response)

        response = await test_client.post(
            "/v1/tasks/runs/managed",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Test",
                "task_spec": {
                    "output_schema": {
                        "type": "json",
                        "properties": "invalid"  # Wrong type
                    }
                }
            }
        )

        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_empty_input(self, test_client):
        """Test validation with empty input string."""
        response = await test_client.post(
            "/v1/tasks/runs/managed",
            json={
                "processor": ProcessorType.LITE.value,
                "input": ""  # Empty input
            }
        )

        # Should either succeed (if API allows) or return 422
        assert response.status_code in [200, 422]

    @pytest.mark.asyncio
    async def test_null_task_spec(self, test_client, mock_transport):
        """Test with null task_spec (should be valid since it's optional)."""
        mock_transport.add_response(
            "/v1/tasks/runs",
            httpx.Response(
                200,
                json=TaskRunFactory.create(run_id="null-spec-run")
            )
        )

        # Status check for managed run
        mock_transport.add_response(
            "/v1/tasks/runs/null-spec-run",
            httpx.Response(
                200,
                json=TaskRunFactory.create(
                    run_id="null-spec-run",
                    status=TaskRunStatus.COMPLETED
                )
            )
        )

        # Result
        mock_transport.add_response(
            "/v1/tasks/runs/null-spec-run/result",
            httpx.Response(
                200,
                json=TaskRunFactory.create_result(
                    run_id="null-spec-run",
                    content="Result without spec"
                )
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Test without task_spec",
                "task_spec": None
            }
        )

        assert response.status_code == 200


class TestParallelSchemaValidation:
    """Test parallel requests with mixed schema validations."""

    @pytest.mark.asyncio
    async def test_parallel_mixed_valid_invalid_schemas(self, test_client, mock_transport):
        """Test parallel requests with mix of valid and invalid schemas."""
        # Setup mocks for valid requests (LITE processor)
        for i in range(3):
            run_id = f"valid-{i}"
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
                    json=TaskRunFactory.create_result(run_id=run_id, content=f"Result {i}")
                )
            )

        # Setup mocks for invalid requests (BASE processor with AutoSchema)
        for i in range(2):
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    422,
                    json={
                        "detail": {
                            "type": "error",
                            "error": {
                                "message": "Auto schema is not supported for processor base.",
                            }
                        }
                    }
                )
            )

        async def send_valid_request(i: int):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Valid request {i}",
                    "task_spec": {
                        "output_schema": {
                            "type": "text"
                        }
                    }
                }
            )

        async def send_invalid_request(i: int):
            return await test_client.post(
                "/v1/tasks/runs/managed",
                json={
                    "processor": ProcessorType.BASE.value,
                    "input": f"Invalid request {i}",
                    "task_spec": {
                        "output_schema": {
                            "type": "auto"
                        }
                    }
                }
            )

        # Send all requests in parallel
        valid_tasks = [send_valid_request(i) for i in range(3)]
        invalid_tasks = [send_invalid_request(i) for i in range(2)]

        all_responses = await asyncio.gather(
            *valid_tasks,
            *invalid_tasks,
            return_exceptions=False
        )

        # Check valid responses succeeded
        for i in range(3):
            assert all_responses[i].status_code == 200
            data = all_responses[i].json()
            assert data["final_status"] == TaskRunStatus.COMPLETED.value

        # Check invalid responses failed with 422
        for i in range(3, 5):
            assert all_responses[i].status_code == 422

    @pytest.mark.asyncio
    async def test_parallel_different_schema_types(self, test_client, mock_transport):
        """Test parallel requests with different output schema types."""
        schema_types = [
            {"type": "text", "description": "Plain text output"},
            {"type": "json", "properties": {"result": {"type": "string"}}, "required": ["result"]},
            {"type": "text"},
        ]

        # Setup mocks
        for i, schema in enumerate(schema_types):
            run_id = f"schema-type-{i}"
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

            # Create result based on schema type
            if schema["type"] == "json":
                result = TaskRunFactory.create_result(
                    run_id=run_id,
                    output_type="json",
                    content={"result": f"JSON output {i}"}
                )
            else:
                result = TaskRunFactory.create_result(
                    run_id=run_id,
                    output_type="text",
                    content=f"Text output {i}"
                )

            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}/result",
                httpx.Response(200, json=result)
            )

        async def create_task_with_schema(i: int, schema: Dict):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Task with schema {i}",
                    "task_spec": {
                        "output_schema": schema
                    }
                }
            )

        tasks = [create_task_with_schema(i, schema) for i, schema in enumerate(schema_types)]
        responses = await asyncio.gather(*tasks)

        for i, response in enumerate(responses):
            assert response.status_code == 200
            data = response.json()
            assert data["final_status"] == TaskRunStatus.COMPLETED.value

    @pytest.mark.asyncio
    async def test_parallel_processor_types(self, test_client, mock_transport):
        """Test parallel requests across all valid processor types."""
        processors = [
            ProcessorType.LITE,
            ProcessorType.CORE,
            ProcessorType.PRO,
        ]

        # Setup mocks for each processor
        for i, processor in enumerate(processors):
            run_id = f"processor-{processor.value}-{i}"
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(
                        run_id=run_id,
                        status=TaskRunStatus.QUEUED,
                        processor=processor
                    )
                )
            )
            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(
                        run_id=run_id,
                        status=TaskRunStatus.COMPLETED,
                        processor=processor
                    )
                )
            )
            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}/result",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create_result(
                        run_id=run_id,
                        content=f"Result from {processor.value}"
                    )
                )
            )

        async def create_with_processor(processor: ProcessorType):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
                json={
                    "processor": processor.value,
                    "input": f"Task for {processor.value}",
                    "task_spec": {
                        "output_schema": {"type": "text"}
                    }
                }
            )

        tasks = [create_with_processor(p) for p in processors]
        responses = await asyncio.gather(*tasks)

        for response in responses:
            assert response.status_code == 200
            assert response.json()["final_status"] == TaskRunStatus.COMPLETED.value


class TestHighConcurrencyStress:
    """Stress tests with high concurrency."""

    @pytest.mark.asyncio
    async def test_high_concurrency_managed_runs(self, test_client, mock_transport):
        """Test 20 concurrent managed runs."""
        num_tasks = 20

        # Setup mocks
        for i in range(num_tasks):
            run_id = f"stress-{i}"

            # Creation
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
                )
            )

            # Status check
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
                        content=f"Stress test result {i}"
                    )
                )
            )

        async def create_managed_run(i: int):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01&timeout_seconds=10",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Stress test task {i}"
                }
            )

        # Create all tasks concurrently
        tasks = [create_managed_run(i) for i in range(num_tasks)]
        start_time = asyncio.get_event_loop().time()
        responses = await asyncio.gather(*tasks)
        elapsed = asyncio.get_event_loop().time() - start_time

        # All should succeed
        for i, response in enumerate(responses):
            assert response.status_code == 200, f"Task {i} failed"
            data = response.json()
            assert data["final_status"] == TaskRunStatus.COMPLETED.value

        print(f"\n✓ Completed {num_tasks} concurrent managed runs in {elapsed:.2f}s")

    @pytest.mark.asyncio
    async def test_high_concurrency_with_failures(self, test_client, mock_transport):
        """Test 30 concurrent requests with mix of success and failures."""
        num_success = 20
        num_failures = 10
        total = num_success + num_failures

        # Setup successful requests
        for i in range(num_success):
            run_id = f"success-{i}"
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
                    json=TaskRunFactory.create_result(run_id=run_id, content=f"Success {i}")
                )
            )

        # Setup failure requests (validation errors)
        for i in range(num_failures):
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    422,
                    json={
                        "detail": {
                            "type": "validation_error",
                            "error": {"message": f"Validation error {i}"}
                        }
                    }
                )
            )

        async def create_request(i: int):
            if i < num_success:
                # Valid request
                return await test_client.post(
                    "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
                    json={
                        "processor": ProcessorType.LITE.value,
                        "input": f"Valid task {i}"
                    }
                )
            else:
                # Invalid request (will trigger validation error)
                return await test_client.post(
                    "/v1/tasks/runs/managed",
                    json={
                        "processor": ProcessorType.BASE.value,
                        "input": f"Invalid task {i}",
                        "task_spec": {
                            "output_schema": {"type": "auto"}
                        }
                    }
                )

        tasks = [create_request(i) for i in range(total)]
        responses = await asyncio.gather(*tasks, return_exceptions=False)

        success_count = sum(1 for r in responses if r.status_code == 200)
        failure_count = sum(1 for r in responses if r.status_code == 422)

        assert success_count == num_success
        assert failure_count == num_failures

        print(f"\n✓ Handled {total} concurrent requests: {success_count} success, {failure_count} failures")

    @pytest.mark.asyncio
    async def test_concurrent_requests_with_timeouts(self, test_client, mock_transport):
        """Test concurrent requests where some timeout."""
        num_complete = 5
        num_timeout = 5

        # Setup requests that complete
        for i in range(num_complete):
            run_id = f"complete-{i}"
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
                    json=TaskRunFactory.create_result(run_id=run_id, content=f"Done {i}")
                )
            )

        # Setup requests that timeout (stay in RUNNING state)
        for i in range(num_timeout):
            run_id = f"timeout-{i}"
            mock_transport.add_response(
                "/v1/tasks/runs",
                httpx.Response(
                    200,
                    json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.QUEUED)
                )
            )
            # Keep returning RUNNING to trigger timeout
            for _ in range(20):
                mock_transport.add_response(
                    f"/v1/tasks/runs/{run_id}",
                    httpx.Response(
                        200,
                        json=TaskRunFactory.create(run_id=run_id, status=TaskRunStatus.RUNNING)
                    )
                )

        async def create_completing_task(i: int):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01&timeout_seconds=5",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Will complete {i}"
                }
            )

        async def create_timing_out_task(i: int):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01&timeout_seconds=0.05",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Will timeout {i}"
                }
            )

        complete_tasks = [create_completing_task(i) for i in range(num_complete)]
        timeout_tasks = [create_timing_out_task(i) for i in range(num_timeout)]

        all_responses = await asyncio.gather(
            *complete_tasks,
            *timeout_tasks,
            return_exceptions=False
        )

        # Check completing tasks
        for i in range(num_complete):
            assert all_responses[i].status_code == 200
            assert all_responses[i].json()["final_status"] == TaskRunStatus.COMPLETED.value

        # Check timeout tasks
        for i in range(num_complete, num_complete + num_timeout):
            assert all_responses[i].status_code == 504  # Gateway timeout


class TestComplexSchemaStructures:
    """Test complex and nested schema structures."""

    @pytest.mark.asyncio
    async def test_deeply_nested_json_schema(self, test_client, mock_transport):
        """Test task with deeply nested JSON schema."""
        complex_schema = {
            "type": "json",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "profile": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "metadata": {
                                    "type": "object",
                                    "properties": {
                                        "tags": {"type": "array"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "required": ["user"]
        }

        run_id = "nested-schema"
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
                    output_type="json",
                    content={
                        "user": {
                            "profile": {
                                "name": "Test User",
                                "metadata": {"tags": ["tag1", "tag2"]}
                            }
                        }
                    }
                )
            )
        )

        response = await test_client.post(
            "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
            json={
                "processor": ProcessorType.LITE.value,
                "input": "Extract user data",
                "task_spec": {
                    "output_schema": complex_schema
                }
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["output_json"]["user"]["profile"]["name"] == "Test User"

    @pytest.mark.asyncio
    async def test_parallel_complex_schemas(self, test_client, mock_transport):
        """Test multiple concurrent requests with different complex schemas."""
        schemas = [
            {
                "type": "json",
                "properties": {
                    "items": {"type": "array"},
                    "count": {"type": "integer"}
                },
                "required": ["items", "count"]
            },
            {
                "type": "json",
                "properties": {
                    "summary": {"type": "string"},
                    "details": {
                        "type": "object",
                        "properties": {
                            "score": {"type": "number"}
                        }
                    }
                }
            },
            {
                "type": "text",
                "description": "Plain text summary"
            }
        ]

        # Setup mocks
        for i, schema in enumerate(schemas):
            run_id = f"complex-{i}"
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

            # Create appropriate output based on schema
            if schema["type"] == "json":
                if "items" in schema.get("properties", {}):
                    content = {"items": ["item1", "item2"], "count": 2}
                else:
                    content = {"summary": "Summary", "details": {"score": 0.95}}

                result = TaskRunFactory.create_result(
                    run_id=run_id,
                    output_type="json",
                    content=content
                )
            else:
                result = TaskRunFactory.create_result(
                    run_id=run_id,
                    output_type="text",
                    content="Plain text result"
                )

            mock_transport.add_response(
                f"/v1/tasks/runs/{run_id}/result",
                httpx.Response(200, json=result)
            )

        async def create_with_complex_schema(i: int, schema: Dict):
            return await test_client.post(
                "/v1/tasks/runs/managed?poll_interval_seconds=0.01",
                json={
                    "processor": ProcessorType.LITE.value,
                    "input": f"Complex task {i}",
                    "task_spec": {
                        "output_schema": schema
                    }
                }
            )

        tasks = [create_with_complex_schema(i, schema) for i, schema in enumerate(schemas)]
        responses = await asyncio.gather(*tasks)

        for response in responses:
            assert response.status_code == 200
            data = response.json()
            assert data["final_status"] == TaskRunStatus.COMPLETED.value
