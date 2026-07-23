"""Shared test fixtures and helpers for comprehensive testing."""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Request, Response

from parallel_websearch_server.app import create_app
from parallel_websearch_server.config import Settings
from parallel_websearch_server.models import (
    ProcessorType,
    TaskRun,
    TaskRunStatus,
)
from parallel_websearch_server.state_store import ContextStateStore, get_state_store


class MockHTTPTransport(httpx.AsyncHTTPTransport):
    """Mock HTTP transport that can be programmed with responses."""

    def __init__(self):
        self.responses: Dict[str, List[Response]] = {}
        self.requests: List[Request] = []
        self.error_on_next = None
        super().__init__()

    def add_response(self, pattern: str, response: Response):
        """Add a response for URLs matching pattern."""
        if pattern not in self.responses:
            self.responses[pattern] = []
        self.responses[pattern].append(response)

    def set_error(self, error: Exception):
        """Make the next request raise an error."""
        self.error_on_next = error

    async def handle_async_request(self, request: Request) -> Response:
        """Handle async request by returning mocked response."""
        self.requests.append(request)

        if self.error_on_next:
            err = self.error_on_next
            self.error_on_next = None
            raise err

        url = str(request.url)

        # Find matching pattern
        for pattern, responses in self.responses.items():
            if pattern in url:
                if responses:
                    return responses.pop(0)

        # Default 404 response
        return Response(
            status_code=404,
            headers={"content-type": "application/json"},
            json={"error": f"No mock response for {url}"},
        )

    async def aclose(self):
        """Satisfy httpx interface expectations without extra cleanup."""
        self.responses.clear()
        self.requests.clear()


class TaskRunFactory:
    """Factory for creating test TaskRun objects."""

    @staticmethod
    def create(
        run_id: str = "run-test-123",
        status: TaskRunStatus = TaskRunStatus.QUEUED,
        processor: ProcessorType = ProcessorType.LITE,
        **kwargs
    ) -> Dict[str, Any]:
        """Create a TaskRun dict for API responses."""
        now = datetime.utcnow().isoformat() + "Z"
        data = {
            "run_id": run_id,
            "status": status.value,
            "created_at": now,
            "modified_at": now,
            "processor": processor.value,
            "metadata": kwargs.get("metadata", {}),
        }
        data.update(kwargs)
        return data

    @staticmethod
    def create_result(
        run_id: str = "run-test-123",
        status: TaskRunStatus = TaskRunStatus.COMPLETED,
        output_type: str = "text",
        content: Any = "Test output content",
    ) -> Dict[str, Any]:
        """Create a TaskRunResult dict."""
        run_data = TaskRunFactory.create(run_id=run_id, status=status)

        if output_type == "text":
            output = {"type": "text", "content": content}
        elif output_type == "json":
            output = {"type": "json", "content": content if isinstance(content, dict) else {"result": content}}
        else:
            output = None

        return {
            "run": run_data,
            "output": output,
        }


class SearchResponseFactory:
    """Factory for creating search/extract responses."""

    @staticmethod
    def create_search_response(results_count: int = 3) -> Dict[str, Any]:
        """Create a search response with mock results."""
        results = []
        for i in range(results_count):
            results.append({
                "title": f"Result {i+1}",
                "url": f"https://example.com/page{i+1}",
                "excerpts": [f"This is excerpt {i+1}"],
                "relevance_score": 0.9 - (i * 0.1),
            })

        return {
            "results": results,
            "metadata": {
                "total_results": results_count,
                "search_time_ms": 123,
            }
        }

    @staticmethod
    def create_extract_response(urls: List[str]) -> Dict[str, Any]:
        """Create an extract response."""
        items = []
        for url in urls:
            items.append({
                "url": url,
                "title": f"Page at {url}",
                "content": f"Extracted content from {url}",
                "metadata": {"extraction_time_ms": 50}
            })

        return {
            "items": items,
            "metadata": {
                "total_extracted": len(urls),
                "extraction_time_ms": 150,
            }
        }


@pytest_asyncio.fixture
async def temp_db_path():
    """Provide a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test_cache.db"


@pytest_asyncio.fixture
async def state_store(temp_db_path):
    """Create an isolated state store for testing."""
    store = ContextStateStore(str(temp_db_path))
    await store.initialize()
    try:
        yield store
    finally:
        await store.close()


@pytest_asyncio.fixture
async def mock_transport():
    """Provide a mock HTTP transport."""
    return MockHTTPTransport()


@pytest_asyncio.fixture
async def test_settings(temp_db_path):
    """Create test settings."""
    return Settings(
        cache_db_path=str(temp_db_path),
        api_key="test-api-key",
        api_base_url="https://api.parallel.ai",
    )


@pytest_asyncio.fixture
async def app_with_mocked_httpx(test_settings, mock_transport, state_store):
    """Create the real FastAPI app with mocked httpx client."""

    http_client = httpx.AsyncClient(
        transport=mock_transport,
        base_url=test_settings.api_base_url,
    )

    # Create app and inject our mocked dependencies directly since httpx.ASGITransport
    # no longer runs lifespan hooks automatically (removed in httpx 0.28+).
    app = FastAPI()
    app.state.httpx_client = http_client
    app.state.state_store = state_store

    # Import and register all routers (mimicking real app.py)
    from parallel_websearch_server.routes import (
        contexts,
        search,
        tasks,
    )

    app.include_router(search.router)
    app.include_router(tasks.router)
    app.include_router(contexts.router)

    # Override dependencies to use our test fixtures
    def get_test_client():
        return http_client

    def get_test_store():
        return app.state.state_store

    from parallel_websearch_server.dependencies import get_http_client
    app.dependency_overrides[get_http_client] = get_test_client
    app.dependency_overrides[get_state_store] = get_test_store

    try:
        yield app
    finally:
        await http_client.aclose()


@pytest_asyncio.fixture
async def test_client(app_with_mocked_httpx):
    """Create an async test client for the app."""
    transport = ASGITransport(app=app_with_mocked_httpx)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


def assert_interaction_recorded(
    interactions: List[Dict],
    interaction_type: str,
    status: str,
    summary_contains: Optional[str] = None
):
    """Helper to assert an interaction was recorded correctly."""
    matching = [
        i for i in interactions
        if i["interaction_type"] == interaction_type and i["status"] == status
    ]
    assert matching, f"No interaction found with type={interaction_type}, status={status}"

    if summary_contains:
        assert any(
            summary_contains in i.get("summary", "") for i in matching
        ), f"No interaction summary contains '{summary_contains}'"

    return matching[0]


async def wait_for_condition(
    check_func,
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str = "Condition not met"
):
    """Wait for a condition to become true."""
    import asyncio
    import time

    start = time.time()
    while time.time() - start < timeout:
        if await check_func():
            return
        await asyncio.sleep(interval)

    raise TimeoutError(f"{message} after {timeout}s")
