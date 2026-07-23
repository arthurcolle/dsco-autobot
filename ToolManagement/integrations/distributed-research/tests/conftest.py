"""Pytest configuration and shared fixtures."""

import sys
from pathlib import Path

import pytest
import pytest_asyncio

# Add project root to path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import all fixtures from fixtures module to make them available to all tests
from tests.fixtures import (
    MockHTTPTransport,
    SearchResponseFactory,
    TaskRunFactory,
    app_with_mocked_httpx,
    assert_interaction_recorded,
    mock_transport,
    state_store,
    temp_db_path,
    test_client,
    test_settings,
    wait_for_condition,
)

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


# Set default asyncio mode to auto
@pytest.fixture(scope="session")
def event_loop_policy():
    """Set event loop policy for async tests."""
    import asyncio
    return asyncio.get_event_loop_policy()
