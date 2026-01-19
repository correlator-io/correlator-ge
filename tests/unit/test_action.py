"""Tests for CorrelatorValidationAction.

This module contains tests for the GE ValidationAction that emits
OpenLineage events to Correlator.

Test Coverage:
- Class initialization and Pydantic field validation
- _should_emit() logic (all/success/failure modes)
- run() method: event building and emission
- Fire-and-forget error handling
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests
import responses

from ge_correlator.action import CorrelatorValidationAction

# =============================================================================
# Test Fixtures - Mock GE Objects
# =============================================================================


def create_mock_checkpoint_result(
    success: bool = True,
    checkpoint_name: str = "my_checkpoint",
    run_name: str = "test-run-123",
    run_time: datetime | None = None,
    validation_results: list[tuple[Any, Any]] | None = None,
) -> MagicMock:
    """Create a mock CheckpointResult for testing.

    Args:
        success: Overall checkpoint success status.
        checkpoint_name: Name of the checkpoint.
        run_name: Run identifier.
        run_time: Run start time.
        validation_results: List of (validation_id, validation_result) tuples.

    Returns:
        Mock CheckpointResult object.
    """
    if run_time is None:
        run_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    checkpoint_result = MagicMock()
    checkpoint_result.success = success

    # Checkpoint config
    checkpoint_result.checkpoint_config = MagicMock()
    checkpoint_result.checkpoint_config.name = checkpoint_name

    # Run ID
    checkpoint_result.run_id = MagicMock()
    checkpoint_result.run_id.run_name = run_name
    checkpoint_result.run_id.run_time = run_time

    # Run results - dict of validation_id -> {"validation_result": ..., "actions_results": ...}
    # This matches the real GE CheckpointResult structure
    if validation_results is None:
        validation_id = create_mock_validation_id()
        validation_result = create_mock_validation_result(success=success)
        validation_results = [(validation_id, validation_result)]

    checkpoint_result.run_results = {
        vid: {"validation_result": vr, "actions_results": {}}
        for vid, vr in validation_results
    }

    return checkpoint_result


def create_mock_validation_id(
    suite_name: str = "my_suite",
) -> MagicMock:
    """Create a mock ValidationResultIdentifier."""
    validation_id = MagicMock()
    validation_id.expectation_suite_identifier = MagicMock()
    validation_id.expectation_suite_identifier.name = suite_name
    return validation_id


def create_mock_validation_result(
    success: bool = True,
    datasource_name: str = "postgres_prod",
    data_asset_name: str = "public.users",
    expectations: list[dict[str, Any]] | None = None,
) -> MagicMock:
    """Create a mock ExpectationSuiteValidationResult."""
    validation_result = MagicMock()
    validation_result.success = success

    # Meta with batch_spec
    validation_result.meta = {
        "batch_spec": {
            "datasource_name": datasource_name,
            "data_asset_name": data_asset_name,
        }
    }

    # Statistics
    validation_result.statistics = {
        "evaluated_expectations": 5,
        "successful_expectations": 4 if success else 2,
        "unsuccessful_expectations": 1 if success else 3,
    }

    # Expectation results
    if expectations is None:
        expectations = [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "user_id",
                "success": True,
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "column": "email",
                "success": success,
            },
        ]

    results = []
    for exp in expectations:
        result = MagicMock()
        result.success = exp.get("success", True)
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = exp.get(
            "expectation_type", "unknown"
        )
        result.expectation_config.kwargs = {"column": exp.get("column")}
        results.append(result)

    validation_result.results = results

    return validation_result


# =============================================================================
# A. Class Initialization Tests
# =============================================================================


@pytest.mark.unit
class TestCorrelatorValidationActionInit:
    """Tests for CorrelatorValidationAction initialization."""

    def test_requires_correlator_endpoint(self) -> None:
        """correlator_endpoint is required."""
        with pytest.raises((TypeError, ValueError)):
            CorrelatorValidationAction()  # type: ignore[call-arg]

    def test_initializes_with_required_fields(self) -> None:
        """Action initializes with required correlator_endpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        assert (
            action.correlator_endpoint == "http://localhost:8080/api/v1/lineage/events"
        )

    def test_api_key_defaults_to_none(self) -> None:
        """api_key defaults to None."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        assert action.api_key is None

    def test_api_key_can_be_set(self) -> None:
        """api_key can be provided."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            api_key="secret-key",
        )
        assert action.api_key == "secret-key"

    def test_emit_on_defaults_to_all(self) -> None:
        """emit_on defaults to 'all'."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        assert action.emit_on == "all"

    def test_emit_on_accepts_success(self) -> None:
        """emit_on accepts 'success' value."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="success",
        )
        assert action.emit_on == "success"

    def test_emit_on_accepts_failure(self) -> None:
        """emit_on accepts 'failure' value."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="failure",
        )
        assert action.emit_on == "failure"

    def test_job_namespace_defaults_to_great_expectations(self) -> None:
        """job_namespace defaults to 'great_expectations://default'."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        assert action.job_namespace == "great_expectations://default"

    def test_job_namespace_can_be_customized(self) -> None:
        """job_namespace can be customized."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            job_namespace="my_org.data_quality",
        )
        assert action.job_namespace == "my_org.data_quality"

    def test_timeout_defaults_to_30(self) -> None:
        """timeout defaults to 30 seconds."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        assert action.timeout == 30

    def test_timeout_can_be_customized(self) -> None:
        """timeout can be customized."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            timeout=60,
        )
        assert action.timeout == 60

    def test_type_discriminator_is_correlator(self) -> None:
        """type discriminator is set to 'correlator'."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        assert action.type == "correlator"


# =============================================================================
# B. _should_emit() Tests
# =============================================================================


@pytest.mark.unit
class TestShouldEmit:
    """Tests for _should_emit() method."""

    def test_emit_on_all_returns_true_for_success(self) -> None:
        """emit_on='all' returns True for successful checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="all",
        )
        checkpoint_result = create_mock_checkpoint_result(success=True)
        assert action._should_emit(checkpoint_result) is True

    def test_emit_on_all_returns_true_for_failure(self) -> None:
        """emit_on='all' returns True for failed checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="all",
        )
        checkpoint_result = create_mock_checkpoint_result(success=False)
        assert action._should_emit(checkpoint_result) is True

    def test_emit_on_success_returns_true_for_success(self) -> None:
        """emit_on='success' returns True for successful checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="success",
        )
        checkpoint_result = create_mock_checkpoint_result(success=True)
        assert action._should_emit(checkpoint_result) is True

    def test_emit_on_success_returns_false_for_failure(self) -> None:
        """emit_on='success' returns False for failed checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="success",
        )
        checkpoint_result = create_mock_checkpoint_result(success=False)
        assert action._should_emit(checkpoint_result) is False

    def test_emit_on_failure_returns_true_for_failure(self) -> None:
        """emit_on='failure' returns True for failed checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="failure",
        )
        checkpoint_result = create_mock_checkpoint_result(success=False)
        assert action._should_emit(checkpoint_result) is True

    def test_emit_on_failure_returns_false_for_success(self) -> None:
        """emit_on='failure' returns False for successful checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="failure",
        )
        checkpoint_result = create_mock_checkpoint_result(success=True)
        assert action._should_emit(checkpoint_result) is False


# =============================================================================
# C. _build_events() Tests
# =============================================================================


@pytest.mark.unit
class TestBuildEvents:
    """Tests for _build_events() method."""

    def test_builds_start_and_complete_events_for_success(self) -> None:
        """Builds START and COMPLETE events for successful validation."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result(success=True)

        events = action._build_events(checkpoint_result)

        assert len(events) == 2
        assert events[0].eventType.name == "START"
        assert events[1].eventType.name == "COMPLETE"

    def test_builds_start_and_fail_events_for_failure(self) -> None:
        """Builds START and FAIL events for failed validation."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result(success=False)

        events = action._build_events(checkpoint_result)

        assert len(events) == 2
        assert events[0].eventType.name == "START"
        assert events[1].eventType.name == "FAIL"

    def test_events_have_correct_job_name(self) -> None:
        """Events have job name in format checkpoint.suite."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result(
            checkpoint_name="daily_validation",
        )
        # Set suite name on the validation_id
        validation_id = next(iter(checkpoint_result.run_results.keys()))
        validation_id.expectation_suite_identifier.name = "users_suite"

        events = action._build_events(checkpoint_result)

        assert events[0].job.name == "daily_validation.users_suite"
        assert events[1].job.name == "daily_validation.users_suite"

    def test_events_have_correct_job_namespace(self) -> None:
        """Events have configured job namespace."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            job_namespace="my_org.data_quality",
        )
        checkpoint_result = create_mock_checkpoint_result()

        events = action._build_events(checkpoint_result)

        assert events[0].job.namespace == "my_org.data_quality"
        assert events[1].job.namespace == "my_org.data_quality"

    def test_events_have_same_run_id(self) -> None:
        """START and COMPLETE/FAIL events have same run ID (UUID format)."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result(run_name="my-run-456")

        events = action._build_events(checkpoint_result)

        # Both events should have the same run ID
        assert events[0].run.runId == events[1].run.runId
        # Run ID should be a valid UUID (generated from run_name)
        uuid.UUID(events[0].run.runId)  # Validates UUID format

    def test_start_event_has_run_time_as_event_time(self) -> None:
        """START event uses run_time as eventTime (ISO format string)."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        run_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        checkpoint_result = create_mock_checkpoint_result(run_time=run_time)

        events = action._build_events(checkpoint_result)

        # OpenLineage expects eventTime as ISO format string
        assert events[0].eventTime == run_time.isoformat()

    def test_complete_event_has_input_datasets(self) -> None:
        """COMPLETE event includes input datasets with facets."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        events = action._build_events(checkpoint_result)

        # COMPLETE event should have inputs
        complete_event = events[1]
        assert complete_event.inputs is not None
        assert len(complete_event.inputs) == 1
        assert complete_event.inputs[0].namespace == "postgres_prod"
        assert complete_event.inputs[0].name == "public.users"

    def test_input_datasets_have_data_quality_facets(self) -> None:
        """Input datasets include dataQualityAssertions facet."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        events = action._build_events(checkpoint_result)

        complete_event = events[1]
        input_dataset = complete_event.inputs[0]
        assert input_dataset.inputFacets is not None
        assert "dataQualityAssertions" in input_dataset.inputFacets

    def test_builds_events_for_multiple_validations(self) -> None:
        """Builds events for each validation in checkpoint."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )

        # Create checkpoint with 2 validations
        validation_id_1 = create_mock_validation_id(suite_name="suite_1")
        validation_result_1 = create_mock_validation_result(
            datasource_name="ds1", data_asset_name="table1"
        )
        validation_id_2 = create_mock_validation_id(suite_name="suite_2")
        validation_result_2 = create_mock_validation_result(
            datasource_name="ds2", data_asset_name="table2"
        )

        checkpoint_result = create_mock_checkpoint_result(
            validation_results=[
                (validation_id_1, validation_result_1),
                (validation_id_2, validation_result_2),
            ]
        )

        events = action._build_events(checkpoint_result)

        # 2 validations x 2 events each = 4 events
        assert len(events) == 4

    def test_events_have_producer_url(self) -> None:
        """Events include producer URL."""
        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        events = action._build_events(checkpoint_result)

        assert "correlator-ge" in events[0].producer
        assert "correlator-ge" in events[1].producer


# =============================================================================
# D. run() Method Tests
# =============================================================================


@pytest.mark.unit
class TestRunMethod:
    """Tests for run() method."""

    @responses.activate
    def test_run_emits_events_to_correlator(self) -> None:
        """run() emits events to Correlator endpoint."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"status": "success"},
            status=200,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        action.run(checkpoint_result=checkpoint_result)

        assert len(responses.calls) == 1

    @responses.activate
    def test_run_returns_success_dict(self) -> None:
        """run() returns dict with class name and success status."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"status": "success"},
            status=200,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        result = action.run(checkpoint_result=checkpoint_result)

        assert result["class"] == "CorrelatorValidationAction"
        assert result["success"] is True

    @responses.activate
    def test_run_includes_api_key_header(self) -> None:
        """run() includes X-API-Key header when api_key is set."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"status": "success"},
            status=200,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            api_key="my-secret-key",
        )
        checkpoint_result = create_mock_checkpoint_result()

        action.run(checkpoint_result=checkpoint_result)

        assert responses.calls[0].request.headers["X-API-Key"] == "my-secret-key"

    @responses.activate
    def test_run_skips_emission_when_should_emit_false(self) -> None:
        """run() skips event emission when _should_emit returns False."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"status": "success"},
            status=200,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            emit_on="failure",  # Only emit on failure
        )
        checkpoint_result = create_mock_checkpoint_result(success=True)  # Success

        result = action.run(checkpoint_result=checkpoint_result)

        # Should not make HTTP call
        assert len(responses.calls) == 0
        assert result["success"] is True
        assert "skipped" in result.get("message", "").lower()


# =============================================================================
# E. Fire-and-Forget Error Handling Tests
# =============================================================================


@pytest.mark.unit
class TestFireAndForgetErrorHandling:
    """Tests for fire-and-forget error handling."""

    @responses.activate
    def test_run_catches_connection_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run() catches connection errors and logs warning."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            body=requests.exceptions.ConnectionError("Connection refused"),
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        # Should not raise - fire-and-forget
        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True  # Action reports success despite emission fail
        assert "error" in result
        assert "connection" in caplog.text.lower() or "error" in caplog.text.lower()

    @responses.activate
    def test_run_catches_timeout_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """run() catches timeout errors and logs warning."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            body=requests.exceptions.Timeout("Connection timed out"),
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        # Should not raise - fire-and-forget
        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True
        assert "error" in result

    @responses.activate
    def test_run_catches_4xx_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """run() catches 4xx errors and logs warning."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"error": "Invalid request"},
            status=400,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        # Should not raise - fire-and-forget
        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True
        assert "error" in result

    @responses.activate
    def test_run_catches_5xx_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """run() catches 5xx errors and logs warning."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"error": "Internal server error"},
            status=500,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        # Should not raise - fire-and-forget
        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True
        assert "error" in result

    def test_run_catches_unexpected_exception(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run() catches unexpected exceptions and logs error."""
        caplog.set_level(logging.WARNING)

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )

        # Create a checkpoint result that will cause an exception
        checkpoint_result = MagicMock()
        checkpoint_result.success = True
        checkpoint_result.run_results = None  # This will cause an error

        # Should not raise - fire-and-forget
        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True
        assert "error" in result

    @responses.activate
    def test_run_logs_warning_on_emission_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run() logs warning when event emission fails."""
        caplog.set_level(logging.WARNING)

        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"error": "Server error"},
            status=500,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )
        checkpoint_result = create_mock_checkpoint_result()

        action.run(checkpoint_result=checkpoint_result)

        # Should log a warning about the failure
        assert any(
            "correlator" in record.message.lower() or "error" in record.message.lower()
            for record in caplog.records
            if record.levelno >= logging.WARNING
        )


# =============================================================================
# F. Integration-style Tests
# =============================================================================


@pytest.mark.unit
class TestIntegration:
    """Integration-style tests for full action workflow."""

    @responses.activate
    def test_full_success_workflow(self) -> None:
        """Full workflow: successful checkpoint emits START + COMPLETE."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"status": "success", "summary": {"received": 2, "successful": 2}},
            status=200,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
            job_namespace="my_company.data_quality",
        )

        checkpoint_result = create_mock_checkpoint_result(
            success=True,
            checkpoint_name="daily_users_check",
            run_name="run-2024-01-15",
        )

        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True
        assert len(responses.calls) == 1

        # Verify emitted events
        sent_body = json.loads(responses.calls[0].request.body)
        assert len(sent_body) == 2
        assert sent_body[0]["eventType"] == "START"
        assert sent_body[1]["eventType"] == "COMPLETE"
        assert sent_body[0]["job"]["namespace"] == "my_company.data_quality"

    @responses.activate
    def test_full_failure_workflow(self) -> None:
        """Full workflow: failed checkpoint emits START + FAIL."""
        responses.add(
            responses.POST,
            "http://localhost:8080/api/v1/lineage/events",
            json={"status": "success"},
            status=200,
        )

        action = CorrelatorValidationAction(
            correlator_endpoint="http://localhost:8080/api/v1/lineage/events",
        )

        checkpoint_result = create_mock_checkpoint_result(success=False)

        result = action.run(checkpoint_result=checkpoint_result)

        assert result["success"] is True

        sent_body = json.loads(responses.calls[0].request.body)
        assert sent_body[0]["eventType"] == "START"
        assert sent_body[1]["eventType"] == "FAIL"
