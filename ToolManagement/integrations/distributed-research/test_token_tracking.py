#!/usr/bin/env python3
"""Test script for token tracking functionality."""

from parallel_websearch_server.token_tracking import (
    get_token_counter,
    count_request_response_tokens,
    TokenModel,
    SizeBucket
)

def test_basic_token_counting():
    """Test basic token counting."""
    print("=== Testing Token Counting ===\n")

    counter = get_token_counter()

    # Test 1: Short text
    short_text = "Hello, world!"
    tokens = counter.count_tokens(short_text, TokenModel.CLAUDE)
    print(f"Test 1 - Short text: '{short_text}'")
    print(f"  Tokens: {tokens}")
    print(f"  Expected: ~3-5 tokens\n")

    # Test 2: Medium text
    medium_text = """
    This is a medium-length text that should contain roughly 50-100 tokens.
    We're testing the token counting functionality to ensure it works correctly
    with different lengths of input text. This helps us track context window usage.
    """
    tokens = counter.count_tokens(medium_text, TokenModel.CLAUDE)
    print(f"Test 2 - Medium text ({len(medium_text)} chars)")
    print(f"  Tokens: {tokens}")
    print(f"  Expected: ~50-80 tokens\n")

    # Test 3: JSON data
    json_data = {
        "objective": "Find the latest developments in quantum computing",
        "max_results": 10,
        "metadata": {
            "tags": ["quantum", "computing", "research"],
            "priority": "high"
        }
    }
    tokens = counter.count_json_tokens(json_data, TokenModel.CLAUDE)
    print(f"Test 3 - JSON data")
    print(f"  Tokens: {tokens}")
    print(f"  Expected: ~30-50 tokens\n")

    # Test 4: Size categorization
    print("Test 4 - Size bucket categorization:")
    test_counts = [100, 750, 3000, 15000, 75000, 150000]
    for count in test_counts:
        bucket = counter.categorize_size(count)
        print(f"  {count:>7} tokens -> {bucket.value}")
    print()

    # Test 5: Request/Response pair
    request_body = {
        "processor": "lite",
        "input": "Explain quantum entanglement in simple terms",
        "task_spec": {"output_schema": {"type": "text"}}
    }
    response_body = {
        "task_run_id": "trun_abc123",
        "status": "completed",
        "output": """Quantum entanglement is a phenomenon where two particles become
        connected in such a way that the state of one particle instantly affects the
        state of the other, regardless of the distance between them. When particles
        are entangled, measuring one particle immediately determines the state of
        the other particle."""
    }

    metrics = count_request_response_tokens(request_body, response_body, TokenModel.CLAUDE)
    print("Test 5 - Request/Response pair:")
    print(f"  Request tokens: {metrics.request_tokens}")
    print(f"  Response tokens: {metrics.response_tokens}")
    print(f"  Total tokens: {metrics.total_tokens}")
    print(f"  Request bytes: {metrics.request_bytes}")
    print(f"  Response bytes: {metrics.response_bytes}")
    print(f"  Total bytes: {metrics.total_bytes}")
    print(f"  Size bucket: {metrics.size_bucket.value}")
    print(f"  Model: {metrics.model.value}")
    print()

    print("=== All Tests Passed! ===")

if __name__ == "__main__":
    test_basic_token_counting()
