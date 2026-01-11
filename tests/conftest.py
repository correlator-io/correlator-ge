"""Shared test fixtures for correlator-ge.

This module provides reusable pytest fixtures for integration testing,
including mock HTTP response fixtures for Correlator API testing.

Fixtures:
    Mock HTTP Fixtures:
        - mock_correlator_success: Mock Correlator returning 200 OK
        - mock_correlator_partial_success: Mock returning 207 Multi-Status
        - mock_correlator_validation_error: Mock returning 422 Unprocessable Entity
        - mock_correlator_server_error: Mock returning 500 Internal Server Error

    CLI Fixtures:
        - runner: Click CliRunner for CLI testing
"""

from collections.abc import Iterator
from typing import Any

import pytest
import responses  # type: ignore[import-not-found]
from click.testing import CliRunner

# =============================================================================
# Constants
# =============================================================================

# Default mock Correlator endpoint used in integration tests
MOCK_CORRELATOR_ENDPOINT = "http://localhost:8080/api/v1/lineage/events"


# =============================================================================
# CLI Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create Click test runner for CLI testing.

    Returns:
        CliRunner instance for CLI testing.
    """
    return CliRunner()


# =============================================================================
# Mock HTTP Response Fixtures
# =============================================================================


def _success_response_body(received: int = 1) -> dict[str, Any]:
    """Generate a success response body matching Correlator API format.

    Args:
        received: Number of events received.

    Returns:
        Dict matching Correlator's success response format.
    """
    return {
        "status": "success",
        "summary": {
            "received": received,
            "successful": received,
            "failed": 0,
            "retriable": 0,
            "non_retriable": 0,
        },
        "failed_events": [],
        "correlation_id": "test-correlation-id-123",
        "timestamp": "2024-01-01T12:00:00Z",
    }


def _partial_success_response_body(
    received: int = 10, failed: int = 2
) -> dict[str, Any]:
    """Generate a 207 partial success response body.

    Args:
        received: Total events received.
        failed: Number of events that failed.

    Returns:
        Dict matching Correlator's partial success response format.
    """
    return {
        "status": "partial_success",
        "summary": {
            "received": received,
            "successful": received - failed,
            "failed": failed,
            "retriable": 0,
            "non_retriable": failed,
        },
        "failed_events": [
            {
                "index": i,
                "reason": f"Validation error for event {i}",
                "retriable": False,
            }
            for i in range(failed)
        ],
        "correlation_id": "test-correlation-id-456",
        "timestamp": "2024-01-01T12:00:00Z",
    }


def _validation_error_response_body() -> dict[str, Any]:
    """Generate a 422 validation error response body.

    Returns:
        Dict matching Correlator's validation error response format.
    """
    return {
        "status": "error",
        "summary": {
            "received": 1,
            "successful": 0,
            "failed": 1,
            "retriable": 0,
            "non_retriable": 1,
        },
        "failed_events": [
            {
                "index": 0,
                "reason": "eventTime is required and cannot be zero value",
                "retriable": False,
            }
        ],
        "correlation_id": "test-correlation-id-789",
        "timestamp": "2024-01-01T12:00:00Z",
    }


@pytest.fixture
def mock_correlator_success() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 200 OK success response.

    Uses the responses library to mock HTTP POST requests to the
    Correlator lineage events endpoint.

    Yields:
        responses.RequestsMock context with configured mock.

    Example:
        def test_with_mock(mock_correlator_success):
            # HTTP POST to MOCK_CORRELATOR_ENDPOINT will return 200 OK
            response = requests.post(MOCK_CORRELATOR_ENDPOINT, json=[...])
            assert response.status_code == 200
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json=_success_response_body(),
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_correlator_partial_success() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 207 Multi-Status partial success.

    This fixture simulates the scenario where some events succeed
    and others fail validation.

    Yields:
        responses.RequestsMock context with configured mock.
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json=_partial_success_response_body(received=10, failed=2),
            status=207,
        )
        yield rsps


@pytest.fixture
def mock_correlator_validation_error() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 422 Unprocessable Entity.

    This fixture simulates validation errors where all events fail.

    Yields:
        responses.RequestsMock context with configured mock.
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json=_validation_error_response_body(),
            status=422,
        )
        yield rsps


@pytest.fixture
def mock_correlator_server_error() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server returning 500 Internal Server Error.

    This fixture simulates server-side errors.

    Yields:
        responses.RequestsMock context with configured mock.
    """
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            MOCK_CORRELATOR_ENDPOINT,
            json={"error": "Internal server error"},
            status=500,
        )
        yield rsps


@pytest.fixture
def mock_correlator_dynamic() -> Iterator[responses.RequestsMock]:
    """Mock Correlator server with dynamic response based on request.

    This fixture provides a RequestsMock that can be configured by the test
    to add custom responses. Useful for tests that need to inspect the
    request body or customize responses.

    Yields:
        responses.RequestsMock context for custom configuration.

    Example:
        def test_custom_response(mock_correlator_dynamic):
            def callback(request):
                events = json.loads(request.body)
                return (200, {}, json.dumps(_success_response_body(len(events))))

            mock_correlator_dynamic.add_callback(
                responses.POST,
                MOCK_CORRELATOR_ENDPOINT,
                callback=callback
            )
    """
    with responses.RequestsMock() as rsps:
        yield rsps
