"""Integration tests for correlator-ge against real Correlator backend.

These tests validate the full roundtrip:
    GE CheckpointResult → CorrelatorValidationAction → Correlator API → Database

Requirements:
    1. Correlator running at CORRELATOR_URL (default: http://localhost:8080)
    2. PostgreSQL accessible at CORRELATOR_DB_URL (optional - for DB verification)

Run with:
    pytest tests/integration/ -m integration -v
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest
import requests

from ge_correlator.action import CorrelatorValidationAction

if TYPE_CHECKING:
    from types import SimpleNamespace

# Import fixtures from conftest
from tests.integration.conftest import (
    CORRELATOR_URL,
    query_job_runs,
)


@pytest.mark.integration
class TestCorrelatorIntegration:
    """Integration tests against real Correlator backend."""

    def test_success_validation_emits_start_complete(
        self,
        skip_if_correlator_unavailable: None,
        configured_action: CorrelatorValidationAction,
        sample_checkpoint_success: SimpleNamespace,
        db_connection,
        test_namespace: str,
    ) -> None:
        """Successful validation emits START + COMPLETE events.

        Verifies:
            - Action executes without error
            - Two events emitted (START + COMPLETE)
            - Events stored in database with correct state
        """
        # Act
        result = configured_action.run(sample_checkpoint_success)

        # Assert action succeeded
        assert result["success"] is True
        assert result.get("events_emitted") == 2  # START + COMPLETE

        # Verify in database
        job_runs = query_job_runs(db_connection, test_namespace)
        assert len(job_runs) >= 1, "Expected at least one job run in database"

        # Check we have both START and terminal state
        states = {run["current_state"] for run in job_runs}
        assert "COMPLETE" in states or "complete" in states.lower() if states else False

    def test_failed_validation_emits_start_fail(
        self,
        skip_if_correlator_unavailable: None,
        configured_action: CorrelatorValidationAction,
        sample_checkpoint_failure: SimpleNamespace,
        db_connection,
        test_namespace: str,
    ) -> None:
        """Failed validation emits START + FAIL events.

        Verifies:
            - Action executes without error
            - Two events emitted (START + FAIL)
            - FAIL state recorded in database
        """
        # Act
        result = configured_action.run(sample_checkpoint_failure)

        # Assert action succeeded (fire-and-forget)
        assert result["success"] is True
        assert result.get("events_emitted") == 2  # START + FAIL

        # Verify in database
        job_runs = query_job_runs(db_connection, test_namespace)
        assert len(job_runs) >= 1, "Expected at least one job run in database"

        # Check for FAIL state
        states = [
            run["current_state"].upper() if run["current_state"] else ""
            for run in job_runs
        ]
        assert "FAIL" in states, f"Expected FAIL state, got: {states}"

    def test_multiple_validations_emit_multiple_pairs(
        self,
        skip_if_correlator_unavailable: None,
        configured_action: CorrelatorValidationAction,
        sample_checkpoint_multiple: SimpleNamespace,
        db_connection,
        test_namespace: str,
    ) -> None:
        """N validations emit 2N events (START + END for each).

        The multiple fixture contains 2 validations, so we expect 4 events.
        """
        # Act
        result = configured_action.run(sample_checkpoint_multiple)

        # Assert action succeeded
        assert result["success"] is True
        assert result.get("events_emitted") == 4  # 2 validations x 2 events each

        # Verify in database - should have multiple job runs
        job_runs = query_job_runs(db_connection, test_namespace)
        assert len(job_runs) >= 2, f"Expected at least 2 job runs, got {len(job_runs)}"

    def test_fire_and_forget_on_unreachable_correlator(
        self,
        sample_checkpoint_success: SimpleNamespace,
    ) -> None:
        """Action succeeds even when Correlator is unreachable.

        Fire-and-forget pattern: emission failures don't fail the checkpoint.
        """
        # Create action with unreachable endpoint
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:59999/api/v1/lineage/events",
            job_namespace="great_expectations://unreachable-test",
            timeout=2,  # Short timeout
        )

        # Act - should not raise
        result = action.run(sample_checkpoint_success)

        # Assert action reports success (fire-and-forget)
        assert result["success"] is True
        # Should have error message indicating failure
        assert "error" in result

    def test_api_response_format(
        self,
        skip_if_correlator_unavailable: None,
        unique_run_id: str,
        test_namespace: str,
    ) -> None:
        """Direct API call returns OpenLineage-compliant response.

        Validates the response format matches plugin-developer-guide.md spec:
            - status: "success" | "partial_success" | "error"
            - summary: { received, successful, failed, retriable, non_retriable }
            - failed_events: []
            - correlation_id: UUID
            - timestamp: ISO 8601
        """
        # Build minimal valid event
        events = [
            {
                "eventTime": datetime.now(timezone.utc).isoformat(),
                "eventType": "START",
                "producer": "https://github.com/correlator-io/correlator-ge/test",
                "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json",
                "run": {"runId": unique_run_id},
                "job": {"namespace": test_namespace, "name": "test_api_format"},
            }
        ]

        # Act
        response = requests.post(
            f"{CORRELATOR_URL}/api/v1/lineage/events",
            json=events,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        # Assert response format
        assert response.status_code in (
            200,
            207,
        ), f"Unexpected status: {response.status_code}"

        body = response.json()

        # Required fields per plugin-developer-guide.md
        assert "status" in body
        assert body["status"] in ("success", "partial_success", "error")

        assert "summary" in body
        summary = body["summary"]
        assert "received" in summary
        assert "successful" in summary
        assert "failed" in summary

        assert "failed_events" in body
        assert isinstance(body["failed_events"], list)

        assert "correlation_id" in body
        assert "timestamp" in body

    def test_emit_on_success_skips_failed_checkpoint(
        self,
        skip_if_correlator_unavailable: None,
        sample_checkpoint_failure: SimpleNamespace,
        test_namespace: str,
    ) -> None:
        """emit_on='success' skips emission for failed checkpoints."""
        action = CorrelatorValidationAction(
            correlator_endpoint=f"{CORRELATOR_URL}/api/v1/lineage/events",
            job_namespace=test_namespace,
            emit_on="success",
            timeout=30,
        )

        # Act
        result = action.run(sample_checkpoint_failure)

        # Assert - action succeeds but skips emission
        assert result["success"] is True
        assert result.get("message") == "Skipped - emit_on condition not met"
        assert "events_emitted" not in result

    def test_emit_on_failure_skips_successful_checkpoint(
        self,
        skip_if_correlator_unavailable: None,
        sample_checkpoint_success: SimpleNamespace,
        test_namespace: str,
    ) -> None:
        """emit_on='failure' skips emission for successful checkpoints."""
        action = CorrelatorValidationAction(
            correlator_endpoint=f"{CORRELATOR_URL}/api/v1/lineage/events",
            job_namespace=test_namespace,
            emit_on="failure",
            timeout=30,
        )

        # Act
        result = action.run(sample_checkpoint_success)

        # Assert - action succeeds but skips emission
        assert result["success"] is True
        assert result.get("message") == "Skipped - emit_on condition not met"
        assert "events_emitted" not in result


@pytest.mark.integration
class TestIdempotency:
    """Test Correlator's idempotency guarantees."""

    def test_duplicate_event_idempotency(
        self,
        skip_if_correlator_unavailable: None,
        configured_action: CorrelatorValidationAction,
        sample_checkpoint_success: SimpleNamespace,
        db_connection,
        test_namespace: str,
    ) -> None:
        """Duplicate events are handled idempotently.

        Sending the same checkpoint result twice should not create
        duplicate records in the database.
        """
        # Act - send same events twice
        result1 = configured_action.run(sample_checkpoint_success)
        result2 = configured_action.run(sample_checkpoint_success)

        # Assert both succeed
        assert result1["success"] is True
        assert result2["success"] is True

        # Query database - should have same number of runs
        # (Correlator deduplicates based on job+run+eventType+eventTime)
        job_runs = query_job_runs(db_connection, test_namespace)

        # We expect only 1 unique job run (not 2)
        # Note: This depends on Correlator's idempotency implementation
        unique_run_ids = {run["run_id"] for run in job_runs}
        assert (
            len(unique_run_ids) == 1
        ), f"Expected 1 unique run_id due to idempotency, got {len(unique_run_ids)}"
