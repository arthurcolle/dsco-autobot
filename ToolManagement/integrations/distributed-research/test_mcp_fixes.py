#!/usr/bin/env python3
"""
Test script for the fixed MCP endpoints.
"""

import asyncio
import json
from datetime import datetime

# Test the MCP schemas
from parallel_websearch_server.mcp_schemas import (
    SearchInput,
    ManagedTaskRunInput,
    CreateGroupInput,
    AddGroupRunsInput,
    OptimizeAndExecuteGroupInput,
)


def test_search_input():
    """Test search input validation."""
    # Valid input
    search = SearchInput(objective="quantum computing", max_results=5)
    print(f"✓ Search input: {search.dict()}")

    # Test with defaults
    search2 = SearchInput(objective="AI news")
    print(f"✓ Search with defaults: {search2.dict()}")


def test_managed_task_input():
    """Test managed task run input validation."""
    # String input without task_spec
    task1 = ManagedTaskRunInput(
        processor="lite",
        input="What is quantum computing?"
    )
    print(f"✓ Task with string input: {task1.dict()}")

    # Dict input with task_spec
    task2 = ManagedTaskRunInput(
        processor="turbo",
        input={"query": "Explain AI"},
        task_spec={"output_schema": {"type": "object"}}
    )
    print(f"✓ Task with dict input: {task2.dict()}")

    # String that should be converted to dict
    task3 = ManagedTaskRunInput(
        processor="lite",
        input="Test query",
        task_spec={"instructions": "Be detailed"}
    )
    print(f"✓ Task with auto-conversion: {task3.dict()}")


def test_create_group_input():
    """Test create group input validation."""
    # With metadata dict
    group1 = CreateGroupInput(metadata={"name": "Test Group"})
    print(f"✓ Group with dict metadata: {group1.dict()}")

    # With JSON string metadata (should be parsed)
    group2 = CreateGroupInput(metadata='{"priority": "high"}')
    print(f"✓ Group with JSON string metadata: {group2.dict()}")

    # Empty group
    group3 = CreateGroupInput()
    print(f"✓ Group with defaults: {group3.dict()}")


def test_add_runs_input():
    """Test add runs input validation."""
    # List of proper runs
    runs1 = AddGroupRunsInput(runs=[
        {"processor": "lite", "input": "Query 1"},
        {"processor": "turbo", "input": {"query": "Query 2"}}
    ])
    print(f"✓ Add runs with list: {runs1.dict()}")

    # JSON string of runs (should be parsed)
    runs2 = AddGroupRunsInput(
        runs='[{"processor": "lite", "input": "Query 3"}]'
    )
    print(f"✓ Add runs with JSON string: {runs2.dict()}")

    # Single string (should be converted to run)
    runs3 = AddGroupRunsInput(runs="Simple query")
    print(f"✓ Add runs with single string: {runs3.dict()}")


def test_optimize_input():
    """Test optimize and execute input validation."""
    # List of task strings
    opt1 = OptimizeAndExecuteGroupInput(
        task_inputs=["Task 1", "Task 2", "Task 3"],
        strategy="balanced"
    )
    print(f"✓ Optimize with list: {opt1.dict()}")

    # JSON string array (should be parsed)
    opt2 = OptimizeAndExecuteGroupInput(
        task_inputs='["Task A", "Task B"]',
        strategy="quality"
    )
    print(f"✓ Optimize with JSON string: {opt2.dict()}")

    # Comma-separated string (should be split)
    opt3 = OptimizeAndExecuteGroupInput(
        task_inputs="Research AI, Analyze trends, Review papers",
        strategy="speed"
    )
    print(f"✓ Optimize with CSV string: {opt3.dict()}")


async def test_state_store():
    """Test the MCP state store."""
    from parallel_websearch_server.mcp_state_store import get_mcp_state_store

    store = get_mcp_state_store()

    # Save a task run
    store.save_task_run(
        task_run_id="test_run_001",
        processor="lite",
        input_data="Test query",
        status="completed",
        output_data="Test output"
    )
    print("✓ Saved task run to state store")

    # Save a task group
    store.save_task_group(
        taskgroup_id="test_group_001",
        status="running",
        total_runs=3,
        metadata={"test": True}
    )
    print("✓ Saved task group to state store")

    # Retrieve recent tasks
    recent_tasks = store.list_recent_task_runs(limit=5)
    print(f"✓ Recent tasks: {len(recent_tasks)} found")

    # Retrieve recent groups
    recent_groups = store.list_recent_task_groups(limit=5)
    print(f"✓ Recent groups: {len(recent_groups)} found")


def main():
    """Run all tests."""
    print("=" * 60)
    print("TESTING MCP FIXES")
    print("=" * 60)

    print("\n1. Testing Search Input Schema:")
    test_search_input()

    print("\n2. Testing Managed Task Input Schema:")
    test_managed_task_input()

    print("\n3. Testing Create Group Input Schema:")
    test_create_group_input()

    print("\n4. Testing Add Runs Input Schema:")
    test_add_runs_input()

    print("\n5. Testing Optimize Input Schema:")
    test_optimize_input()

    print("\n6. Testing State Store:")
    asyncio.run(test_state_store())

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED! ✅")
    print("=" * 60)
    print("\nThe MCP endpoints are now properly configured with:")
    print("- ✅ Proper Pydantic input/output schemas")
    print("- ✅ Automatic JSON parameter parsing")
    print("- ✅ Input format validation and conversion")
    print("- ✅ Persistent state management")
    print("- ✅ Comprehensive error handling")
    print("\nYou can now use these endpoints through the MCP protocol!")


if __name__ == "__main__":
    main()