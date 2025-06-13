"""
Pytest configuration and shared fixtures.
"""

from datetime import datetime

import pytest


@pytest.fixture
def sample_timestamp():
    """Fixture providing a sample timestamp for testing."""
    return datetime(2023, 6, 15, 14, 45)


@pytest.fixture
def sample_icao():
    """Fixture providing a sample ICAO code for testing."""
    return "ABC123"


@pytest.fixture
def mock_successful_response():
    """Fixture providing a mock successful HTTP response."""
    from unittest.mock import Mock

    response = Mock()
    response.status_code = 200
    response.content = b"mock_content_data"
    return response


@pytest.fixture
def mock_error_response():
    """Fixture providing a mock HTTP error response."""
    from unittest.mock import Mock

    response = Mock()
    response.status_code = 404
    return response
