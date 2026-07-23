"""Comprehensive tests for search and extract routes with real HTTP flows."""

import asyncio
from typing import Dict

import httpx
import pytest

from tests.fixtures import (
    SearchResponseFactory,
    assert_interaction_recorded,
    wait_for_condition,
)


class TestSearchEndpoint:
    """Test /v1beta/search endpoint with real HTTP flows."""

    @pytest.mark.asyncio
    async def test_search_success_basic(self, test_client, mock_transport, state_store):
        """Test successful search request and response."""
        # Program mock response
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_search_response(results_count=3),
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        # Make request
        response = await test_client.post(
            "/v1beta/search",
            json={"objective": "find information about Python"}
        )

        assert response.status_code == 200
        data = response.json()

        assert "results" in data
        assert len(data["results"]) == 3
        assert data["results"][0]["title"] == "Result 1"

        # Verify HTTP call was made
        assert len(mock_transport.requests) == 1
        assert "objective" in mock_transport.requests[0].content.decode()

    @pytest.mark.asyncio
    async def test_search_with_context_caching(self, test_client, mock_transport, state_store):
        """Test search with context_id parameter records interaction."""
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_search_response(results_count=2),
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        response = await test_client.post(
            "/v1beta/search?context_id=search-ctx-1",
            json={
                "objective": "machine learning algorithms",
                "max_results": 5
            }
        )

        assert response.status_code == 200

        # Check interaction was recorded
        interactions = await state_store.list_interactions("search-ctx-1")
        interaction = assert_interaction_recorded(
            interactions,
            interaction_type="search",
            status="completed",
            summary_contains="2 results"
        )

        # Verify payloads were stored
        assert interaction["input_payload"]["objective"] == "machine learning algorithms"
        assert len(interaction["output_payload"]["results"]) == 2

    @pytest.mark.asyncio
    async def test_search_upstream_error_cached(self, test_client, mock_transport, state_store):
        """Test that upstream errors are properly cached and propagated."""
        mock_response = httpx.Response(
            500,
            json={"error": "Internal server error", "details": "Database unavailable"},
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        response = await test_client.post(
            "/v1beta/search?context_id=search-error-ctx",
            json={"objective": "test query"}
        )

        assert response.status_code == 500
        error_data = response.json()
        assert "error" in error_data

        # Error should be cached
        interactions = await state_store.list_interactions("search-error-ctx")
        assert_interaction_recorded(
            interactions,
            interaction_type="search",
            status="failed",
            summary_contains="Search request failed"
        )

    @pytest.mark.asyncio
    async def test_search_validation_error(self, test_client):
        """Test request validation errors."""
        response = await test_client.post(
            "/v1beta/search",
            json={"wrong_field": "no objective"}
        )

        assert response.status_code == 422
        error = response.json()
        assert "detail" in error
        # Should indicate missing required field

    @pytest.mark.asyncio
    async def test_search_network_timeout(self, test_client, mock_transport, state_store):
        """Test handling of network timeouts."""
        # Make mock transport raise timeout error
        mock_transport.set_error(httpx.TimeoutException("Request timed out"))

        response = await test_client.post(
            "/v1beta/search?context_id=timeout-ctx",
            json={"objective": "slow query"}
        )

        assert response.status_code == 504

        # Timeout should be recorded
        interactions = await state_store.list_interactions("timeout-ctx")
        assert len(interactions) > 0
        assert interactions[0]["status"] == "failed"

    @pytest.mark.asyncio
    async def test_search_rate_limit_handling(self, test_client, mock_transport):
        """Test 429 rate limit response handling."""
        mock_response = httpx.Response(
            429,
            json={"error": "Rate limit exceeded"},
            headers={"Retry-After": "60"},
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        response = await test_client.post(
            "/v1beta/search",
            json={"objective": "rate limited query"}
        )

        assert response.status_code == 429

    @pytest.mark.asyncio
    async def test_search_with_all_parameters(self, test_client, mock_transport):
        """Test search with all optional parameters."""
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_search_response(results_count=10),
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        response = await test_client.post(
            "/v1beta/search",
            json={
                "objective": "comprehensive search",
                "max_results": 10,
                "include_raw_content": True,
                "date_filter": "2024-01-01",
                "domain_filter": ["example.com", "test.org"],
                "exclude_domains": ["spam.com"]
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 10

        # Verify all parameters were sent
        request = mock_transport.requests[0]
        request_body = request.content.decode()
        assert "include_raw_content" in request_body
        assert "date_filter" in request_body

    @pytest.mark.asyncio
    async def test_concurrent_searches_different_contexts(self, test_client, mock_transport, state_store):
        """Test multiple concurrent searches with different contexts."""
        # Program multiple responses
        for i in range(5):
            mock_response = httpx.Response(
                200,
                json=SearchResponseFactory.create_search_response(results_count=i+1),
            )
            mock_transport.add_response("/v1beta/search", mock_response)

        async def do_search(ctx_id: str, query: str):
            return await test_client.post(
                f"/v1beta/search?context_id={ctx_id}",
                json={"objective": query}
            )

        # Execute concurrent searches
        tasks = [
            do_search(f"concurrent-ctx-{i}", f"query {i}")
            for i in range(5)
        ]
        responses = await asyncio.gather(*tasks)

        # All should succeed
        for response in responses:
            assert response.status_code == 200

        # Each context should have its interaction
        for i in range(5):
            interactions = await state_store.list_interactions(f"concurrent-ctx-{i}")
            assert len(interactions) == 1
            assert interactions[0]["input_payload"]["objective"] == f"query {i}"

    @pytest.mark.asyncio
    async def test_search_empty_results_handling(self, test_client, mock_transport):
        """Test handling of empty search results."""
        mock_response = httpx.Response(
            200,
            json={"results": [], "metadata": {"total_results": 0}},
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        response = await test_client.post(
            "/v1beta/search",
            json={"objective": "obscure query with no results"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["results"] == []

    @pytest.mark.asyncio
    async def test_search_malformed_upstream_response(self, test_client, mock_transport, state_store):
        """Test handling of malformed upstream responses."""
        mock_response = httpx.Response(
            200,
            content=b"not json at all",  # Invalid JSON
        )
        mock_transport.add_response("/v1beta/search", mock_response)

        response = await test_client.post(
            "/v1beta/search?context_id=malformed-ctx",
            json={"objective": "test"}
        )

        assert response.status_code == 502  # Bad gateway

        # Should still record the failure
        interactions = await state_store.list_interactions("malformed-ctx")
        assert interactions[0]["status"] == "failed"


class TestExtractEndpoint:
    """Test /v1beta/extract endpoint with real HTTP flows."""

    @pytest.mark.asyncio
    async def test_extract_success_basic(self, test_client, mock_transport):
        """Test successful extract request."""
        urls = ["https://example.com", "https://test.org"]
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_extract_response(urls),
        )
        mock_transport.add_response("/v1beta/extract", mock_response)

        response = await test_client.post(
            "/v1beta/extract",
            json={
                "urls": urls,
                "objective": "Extract main content"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert len(data["items"]) == 2
        assert data["items"][0]["url"] == urls[0]

    @pytest.mark.asyncio
    async def test_extract_with_context_caching(self, test_client, mock_transport, state_store):
        """Test extract with context caching."""
        urls = ["https://example.com/page1"]
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_extract_response(urls),
        )
        mock_transport.add_response("/v1beta/extract", mock_response)

        response = await test_client.post(
            "/v1beta/extract?context_id=extract-ctx-1",
            json={
                "urls": urls,
                "objective": "Extract article text",
                "include_raw_html": False
            }
        )

        assert response.status_code == 200

        # Check interaction
        interactions = await state_store.list_interactions("extract-ctx-1")
        interaction = assert_interaction_recorded(
            interactions,
            interaction_type="extract",
            status="completed",
            summary_contains="1 items"
        )

        assert interaction["input_payload"]["urls"] == urls
        assert len(interaction["output_payload"]["items"]) == 1

    @pytest.mark.asyncio
    async def test_extract_invalid_urls(self, test_client):
        """Test extract with invalid URLs."""
        response = await test_client.post(
            "/v1beta/extract",
            json={
                "urls": ["not-a-url", "also/not/valid"],
                "objective": "Extract"
            }
        )

        # Should either validate URLs or pass through to upstream
        # Depends on implementation
        assert response.status_code in [422, 400]

    @pytest.mark.asyncio
    async def test_extract_empty_urls_list(self, test_client):
        """Test extract with empty URLs list."""
        response = await test_client.post(
            "/v1beta/extract",
            json={
                "urls": [],
                "objective": "Extract from nothing"
            }
        )

        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_extract_duplicate_urls(self, test_client, mock_transport):
        """Test extract with duplicate URLs."""
        urls = ["https://example.com", "https://example.com", "https://test.org"]
        # Mock should return unique URLs only
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_extract_response(
                ["https://example.com", "https://test.org"]
            ),
        )
        mock_transport.add_response("/v1beta/extract", mock_response)

        response = await test_client.post(
            "/v1beta/extract",
            json={"urls": urls, "objective": "Extract"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2

    @pytest.mark.asyncio
    async def test_extract_partial_failures(self, test_client, mock_transport):
        """Test extract where some URLs fail but others succeed."""
        urls = ["https://success.com", "https://failure.com", "https://success2.com"]

        # Response indicates partial success
        response_data = {
            "items": [
                {
                    "url": "https://success.com",
                    "title": "Success Page",
                    "content": "Content extracted"
                },
                {
                    "url": "https://failure.com",
                    "error": "Failed to fetch: 404 Not Found"
                },
                {
                    "url": "https://success2.com",
                    "title": "Another Success",
                    "content": "More content"
                }
            ],
            "metadata": {
                "total_requested": 3,
                "total_extracted": 2,
                "failed_urls": 1
            }
        }

        mock_response = httpx.Response(200, json=response_data)
        mock_transport.add_response("/v1beta/extract", mock_response)

        response = await test_client.post(
            "/v1beta/extract",
            json={"urls": urls, "objective": "Extract all"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 3
        assert data["metadata"]["failed_urls"] == 1

    @pytest.mark.asyncio
    async def test_extract_with_extraction_options(self, test_client, mock_transport):
        """Test extract with various extraction options."""
        urls = ["https://example.com"]
        mock_response = httpx.Response(
            200,
            json=SearchResponseFactory.create_extract_response(urls),
        )
        mock_transport.add_response("/v1beta/extract", mock_response)

        response = await test_client.post(
            "/v1beta/extract",
            json={
                "urls": urls,
                "objective": "Extract with options",
                "include_raw_html": True,
                "include_metadata": True,
                "max_content_length": 5000,
                "extraction_schema": {
                    "title": "string",
                    "author": "string",
                    "date": "string"
                }
            }
        )

        assert response.status_code == 200

        # Verify options were passed
        request = mock_transport.requests[0]
        request_body = request.content.decode()
        assert "include_raw_html" in request_body
        assert "extraction_schema" in request_body

    @pytest.mark.asyncio
    async def test_concurrent_extracts_same_context(self, test_client, mock_transport, state_store):
        """Test concurrent extract requests on same context."""
        # Program responses
        for i in range(3):
            urls = [f"https://example.com/page{i}"]
            mock_response = httpx.Response(
                200,
                json=SearchResponseFactory.create_extract_response(urls),
            )
            mock_transport.add_response("/v1beta/extract", mock_response)

        async def do_extract(page_num: int):
            return await test_client.post(
                "/v1beta/extract?context_id=shared-extract-ctx",
                json={
                    "urls": [f"https://example.com/page{page_num}"],
                    "objective": f"Extract page {page_num}"
                }
            )

        # Execute concurrently
        tasks = [do_extract(i) for i in range(3)]
        responses = await asyncio.gather(*tasks)

        for response in responses:
            assert response.status_code == 200

        # Should have 3 interactions in shared context
        interactions = await state_store.list_interactions("shared-extract-ctx")
        assert len(interactions) == 3

        # Each should have different input
        objectives = {i["input_payload"]["objective"] for i in interactions}
        assert len(objectives) == 3