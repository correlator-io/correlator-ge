"""Integration test fixtures for correlator-ge.

These fixtures support integration tests against a real Correlator backend.

Environment Variables:
    CORRELATOR_URL: Correlator API endpoint (default: http://localhost:8080)
    CORRELATOR_DB_URL: PostgreSQL connection string for DB verification (optional)
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import requests

from ge_correlator.action import CorrelatorValidationAction
from tests.helpers.fixture_helpers import (
    create_checkpoint_result,
    load_fixture,
)

if TYPE_CHECKING:
    from types import SimpleNamespace

# Environment configuration
CORRELATOR_URL = os.environ.get("CORRELATOR_URL", "http://localhost:8080")
CORRELATOR_DB_URL = os.environ.get("CORRELATOR_DB_URL")
TEST_NAMESPACE_PREFIX = "great_expectations://integration-test"


@pytest.fixture(scope="module")
def correlator_available() -> bool:
    """Check if Correlator backend is reachable.

    Module-scoped to avoid repeated health checks.
    """
    try:
        response = requests.get(f"{CORRELATOR_URL}/ping", timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


@pytest.fixture
def skip_if_correlator_unavailable(correlator_available: bool) -> None:
    """Skip test if Correlator is not running."""
    if not correlator_available:
        pytest.skip(f"Correlator not available at {CORRELATOR_URL}")


@pytest.fixture
def unique_run_id() -> str:
    """Generate unique UUID for test isolation.

    Each test gets a unique run ID to prevent data collision
    across parallel test runs. This is used directly as the
    OpenLineage runId (must be UUID format).
    """
    return str(uuid4())


@pytest.fixture
def unique_run_name(unique_run_id: str) -> str:
    """Generate prefixed run name for GE fixtures.

    More readable in logs/database than raw UUID.
    Example: "test-run-abc123..." vs "abc123..."
    """
    return f"test-run-{unique_run_id}"


@pytest.fixture
def test_namespace(unique_run_id: str) -> str:
    """Generate unique namespace per test to prevent data collision."""
    return f"{TEST_NAMESPACE_PREFIX}-{unique_run_id}"


@pytest.fixture
def sample_checkpoint_success(unique_run_name: str) -> SimpleNamespace:
    """Checkpoint with successful validation (real GE data).

    Uses checkpoint_result_success.json with run_name override
    for test isolation.
    """
    fixture_data = load_fixture("checkpoint_result_success.json")
    # Override run_name for test isolation (prefixed for readability)
    fixture_data["run_id"]["run_name"] = unique_run_name
    return create_checkpoint_result(fixture_data)


@pytest.fixture
def sample_checkpoint_failure(unique_run_name: str) -> SimpleNamespace:
    """Checkpoint with failed validation (real GE data).

    Uses checkpoint_result_failure.json with run_name override
    for test isolation.
    """
    fixture_data = load_fixture("checkpoint_result_failure.json")
    fixture_data["run_id"]["run_name"] = unique_run_name
    return create_checkpoint_result(fixture_data)


@pytest.fixture
def sample_checkpoint_multiple(unique_run_name: str) -> SimpleNamespace:
    """Checkpoint with multiple validations (mixed results).

    Uses checkpoint_result_multiple.json with run_name override
    for test isolation.
    """
    fixture_data = load_fixture("checkpoint_result_multiple.json")
    fixture_data["run_id"]["run_name"] = unique_run_name
    return create_checkpoint_result(fixture_data)


@pytest.fixture
def configured_action(test_namespace: str) -> CorrelatorValidationAction:
    """Create action configured for integration testing.

    Uses real CORRELATOR_URL endpoint with unique namespace
    per test for isolation.
    """
    return CorrelatorValidationAction(
        correlator_endpoint=f"{CORRELATOR_URL}/api/v1/lineage/events",
        job_namespace=test_namespace,
        timeout=30,
    )


@pytest.fixture
def db_connection():
    """Create psycopg2 connection for database verification.

    Skips tests if CORRELATOR_DB_URL not set (DB verification is optional).

    Yields:
        psycopg2 connection object

    Note:
        Connection is automatically closed after test.
    """
    if not CORRELATOR_DB_URL:
        pytest.skip("CORRELATOR_DB_URL not set - skipping DB verification")

    psycopg2 = pytest.importorskip("psycopg2")

    conn = psycopg2.connect(CORRELATOR_DB_URL)
    try:
        yield conn
    finally:
        conn.close()


def query_job_runs(
    connection: Any,
    namespace: str,
    job_name_pattern: str | None = None,
) -> list[dict[str, Any]]:
    """Query job_runs table for verification.

    Args:
        connection: psycopg2 connection
        namespace: Job namespace to filter by
        job_name_pattern: Optional LIKE pattern for job name

    Returns:
        List of job_run records as dicts
    """
    with connection.cursor() as cursor:
        if job_name_pattern:
            cursor.execute(
                """
                SELECT job_run_id, job_name, job_namespace, run_id, current_state, created_at
                FROM job_runs
                WHERE job_namespace = %s AND job_name LIKE %s
                ORDER BY created_at DESC
                """,
                (namespace, job_name_pattern),
            )
        else:
            cursor.execute(
                """
                SELECT job_run_id, job_name, job_namespace, run_id, current_state, created_at
                FROM job_runs
                WHERE job_namespace = %s
                ORDER BY created_at DESC
                """,
                (namespace,),
            )

        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


@pytest.fixture(scope="module", autouse=True)
def cleanup_test_data() -> None:
    """Clean up test data from database after all tests.

    Cleanup only runs when CLEANUP_TEST_DATA=true is set.
    Usually test data is left for inspection after integration tests.

    Cleans up tables in dependency order:
    1. test_results - validation results linked to job runs
    2. lineage_event_idempotency - lineage event deduplication records
    3. lineage_edges - job run lineage connections
    4. job_runs - the job runs themselves
    5. datasets - dataset records matching test namespace

    Usage:
        CLEANUP_TEST_DATA=true CORRELATOR_DB_URL=postgres://... make run test integration
    """
    yield  # Run tests first

    # Check env vars at runtime (not import time)
    if os.environ.get("CLEANUP_TEST_DATA", "false").lower() != "true":
        return  # Cleanup disabled

    # Read DB URL at runtime (module-level CORRELATOR_DB_URL is read at import time)
    db_url = os.environ.get("CORRELATOR_DB_URL")
    if not db_url:
        print("\nWarning: CLEANUP_TEST_DATA=true but CORRELATOR_DB_URL not set")
        return

    try:
        import psycopg2  # noqa: PLC0415 - lazy import for optional dependency

        conn = psycopg2.connect(db_url)
        try:
            with conn.cursor() as cursor:
                namespace_pattern = f"{TEST_NAMESPACE_PREFIX}%"
                total_deleted = 0

                # 1. Delete test_results for matching job_runs
                cursor.execute(
                    """
                    DELETE FROM test_results
                    WHERE job_run_id IN (
                        SELECT job_run_id FROM job_runs
                        WHERE job_namespace LIKE %s
                    )
                    """,
                    (namespace_pattern,),
                )
                total_deleted += cursor.rowcount

                # 2. Delete lineage_event_idempotency based on job_namespace in JSON metadata
                cursor.execute(
                    """
                    DELETE FROM lineage_event_idempotency
                    WHERE event_metadata->>'job_namespace' LIKE %s
                    """,
                    (namespace_pattern,),
                )
                total_deleted += cursor.rowcount

                # 3. Delete lineage_edges for matching job_runs
                cursor.execute(
                    """
                    DELETE FROM lineage_edges
                    WHERE job_run_id IN (
                        SELECT job_run_id FROM job_runs
                        WHERE job_namespace LIKE %s
                    )
                    """,
                    (namespace_pattern,),
                )
                total_deleted += cursor.rowcount

                # 4. Delete job_runs
                cursor.execute(
                    """
                    DELETE FROM job_runs
                    WHERE job_namespace LIKE %s
                    """,
                    (namespace_pattern,),
                )
                total_deleted += cursor.rowcount

                # 5. Delete datasets matching test namespace
                cursor.execute(
                    """
                    DELETE FROM datasets
                    WHERE namespace LIKE %s
                    """,
                    ("taxi_datasource%",),
                )
                total_deleted += cursor.rowcount

                conn.commit()
                if total_deleted > 0:
                    print(f"\nCleaned up {total_deleted} integration test records")
        finally:
            conn.close()
    except Exception as e:
        print(f"\nWarning: Failed to cleanup test data: {e}")
