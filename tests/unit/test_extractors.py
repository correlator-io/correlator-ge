"""Tests for extractors module.

This module tests the metadata extractors that pull information from
GE CheckpointResult and ExpectationSuiteValidationResult objects.

Test Coverage:
    - extract_job_name(): Checkpoint + suite name extraction
    - extract_run_id(): Run ID extraction or generation
    - extract_run_time(): Run time extraction
    - extract_datasets(): Dataset extraction from batch_spec
    - extract_data_quality_facets(): Facet extraction from results
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from ge_correlator.extractors import (
    extract_data_quality_facets,
    extract_datasets,
    extract_job_name,
    extract_run_id,
    extract_run_time,
)

# Test producer URL
TEST_PRODUCER = "https://github.com/correlator-io/correlator-ge/test"

# =============================================================================
# Mock Object Factories
# =============================================================================


def create_mock_checkpoint_config(name: str = "test_checkpoint") -> MagicMock:
    """Create mock CheckpointConfig."""
    config = MagicMock()
    config.name = name
    return config


def create_mock_run_id(
    run_name: str = "test-run-123",
    run_time: datetime | None = None,
) -> MagicMock:
    """Create mock RunIdentifier."""
    run_id = MagicMock()
    run_id.run_name = run_name
    run_id.run_time = run_time or datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    return run_id


def create_mock_expectation_suite_identifier(
    suite_name: str = "test_suite",
) -> MagicMock:
    """Create mock ExpectationSuiteIdentifier."""
    suite_id = MagicMock()
    suite_id.name = suite_name
    suite_id.expectation_suite_name = suite_name
    return suite_id


def create_mock_validation_id(suite_name: str = "test_suite") -> MagicMock:
    """Create mock ValidationResultIdentifier."""
    validation_id = MagicMock()
    validation_id.expectation_suite_identifier = (
        create_mock_expectation_suite_identifier(suite_name)
    )
    return validation_id


def create_mock_checkpoint_result(
    checkpoint_name: str = "test_checkpoint",
    run_name: str = "test-run-123",
    run_time: datetime | None = None,
) -> MagicMock:
    """Create mock CheckpointResult."""
    result = MagicMock()
    result.checkpoint_config = create_mock_checkpoint_config(checkpoint_name)
    result.run_id = create_mock_run_id(run_name, run_time)
    result.success = True
    return result


def create_mock_expectation_result(
    expectation_type: str = "expect_column_values_to_not_be_null",
    success: bool = True,
    column: str | None = "user_id",
) -> MagicMock:
    """Create mock ExpectationValidationResult."""
    result = MagicMock()
    result.success = success

    config = MagicMock()
    config.expectation_type = expectation_type
    config.kwargs = {"column": column} if column else {}
    result.expectation_config = config

    return result


def create_mock_validation_result(
    success: bool = True,
    datasource_name: str = "postgres_prod",
    data_asset_name: str = "public.users",
    expectations: list[dict[str, Any]] | None = None,
) -> MagicMock:
    """Create mock ExpectationSuiteValidationResult."""
    result = MagicMock()
    result.success = success

    # Meta with batch_spec
    result.meta = {
        "batch_spec": {
            "datasource_name": datasource_name,
            "data_asset_name": data_asset_name,
        },
        "expectation_suite_name": "test_suite",
    }

    # Statistics
    result.statistics = {
        "evaluated_expectations": 5,
        "successful_expectations": 4,
        "unsuccessful_expectations": 1,
        "success_percent": 80.0,
    }

    # Results (per-expectation)
    if expectations is None:
        expectations = [
            {
                "type": "expect_column_values_to_not_be_null",
                "success": True,
                "column": "user_id",
            },
            {
                "type": "expect_column_values_to_be_unique",
                "success": True,
                "column": "email",
            },
            {
                "type": "expect_column_values_to_match_regex",
                "success": False,
                "column": "phone",
            },
        ]

    result.results = [
        create_mock_expectation_result(
            expectation_type=exp["type"],
            success=exp["success"],
            column=exp.get("column"),
        )
        for exp in expectations
    ]

    return result


# =============================================================================
# A. extract_job_name() Tests
# =============================================================================


@pytest.mark.unit
class TestExtractJobName:
    """Tests for extract_job_name() function."""

    def test_extracts_checkpoint_and_suite_name(self) -> None:
        """Extracts job name as checkpoint.suite format."""
        checkpoint_result = create_mock_checkpoint_result(
            checkpoint_name="daily_validation"
        )
        validation_id = create_mock_validation_id(suite_name="users_suite")

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "daily_validation.users_suite"

    def test_handles_special_characters_in_names(self) -> None:
        """Handles special characters in checkpoint/suite names."""
        checkpoint_result = create_mock_checkpoint_result(
            checkpoint_name="prod-db_check"
        )
        validation_id = create_mock_validation_id(suite_name="users.v2_suite")

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "prod-db_check.users.v2_suite"

    def test_handles_missing_checkpoint_config(self) -> None:
        """Returns unknown_checkpoint when config is missing."""
        checkpoint_result = MagicMock()
        checkpoint_result.checkpoint_config = None
        validation_id = create_mock_validation_id(suite_name="my_suite")

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "unknown_checkpoint.my_suite"

    def test_handles_missing_checkpoint_name(self) -> None:
        """Returns unknown_checkpoint when config.name is None."""
        checkpoint_result = MagicMock()
        checkpoint_result.checkpoint_config = MagicMock()
        checkpoint_result.checkpoint_config.name = None
        validation_id = create_mock_validation_id(suite_name="my_suite")

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "unknown_checkpoint.my_suite"

    def test_handles_missing_suite_identifier(self) -> None:
        """Returns unknown_suite when suite identifier is missing."""
        checkpoint_result = create_mock_checkpoint_result(
            checkpoint_name="my_checkpoint"
        )
        validation_id = MagicMock()
        validation_id.expectation_suite_identifier = None

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "my_checkpoint.unknown_suite"

    def test_handles_missing_suite_name(self) -> None:
        """Returns unknown_suite when suite name is None."""
        checkpoint_result = create_mock_checkpoint_result(
            checkpoint_name="my_checkpoint"
        )
        validation_id = MagicMock()
        validation_id.expectation_suite_identifier = MagicMock()
        validation_id.expectation_suite_identifier.name = None
        validation_id.expectation_suite_identifier.expectation_suite_name = None

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "my_checkpoint.unknown_suite"

    def test_prefers_name_over_expectation_suite_name(self) -> None:
        """Uses .name attribute if available, falls back to .expectation_suite_name."""
        checkpoint_result = create_mock_checkpoint_result(
            checkpoint_name="my_checkpoint"
        )
        validation_id = MagicMock()
        suite_id = MagicMock()
        suite_id.name = "primary_name"
        suite_id.expectation_suite_name = "fallback_name"
        validation_id.expectation_suite_identifier = suite_id

        job_name = extract_job_name(checkpoint_result, validation_id)

        assert job_name == "my_checkpoint.primary_name"


# =============================================================================
# B. extract_run_id() Tests
# =============================================================================


@pytest.mark.unit
class TestExtractRunId:
    """Tests for extract_run_id() function."""

    def test_generates_valid_uuid7(self) -> None:
        """Generates valid UUID7 per OpenLineage recommendation."""
        create_mock_checkpoint_result(run_name="manual-2024-01-15")

        run_id = extract_run_id()

        # Verify it's a valid UUID string (OpenLineage requirement)
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id

        # Verify it's UUID version 7 (time-ordered)
        assert parsed.version == 7

    def test_generates_unique_uuid_each_call(self) -> None:
        """Each call generates a unique UUID (not deterministic)."""
        create_mock_checkpoint_result(run_name="same-run-name")

        run_id_1 = extract_run_id()
        run_id_2 = extract_run_id()

        # UUIDs should be different (not deterministic)
        assert run_id_1 != run_id_2

        # Both should be valid UUID7
        assert uuid.UUID(run_id_1).version == 7
        assert uuid.UUID(run_id_2).version == 7

    def test_generates_uuid_when_run_id_missing(self) -> None:
        """Generates UUID7 even when run_id is not available."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = None

        run_id = extract_run_id()

        # Verify it's a valid UUID7 string
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id
        assert parsed.version == 7

    def test_generates_uuid_when_run_name_missing(self) -> None:
        """Generates UUID7 when run_id.run_name is None."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = MagicMock()
        checkpoint_result.run_id.run_name = None

        run_id = extract_run_id()

        # Verify it's a valid UUID7 string
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id
        assert parsed.version == 7


# =============================================================================
# C. extract_run_time() Tests
# =============================================================================


@pytest.mark.unit
class TestExtractRunTime:
    """Tests for extract_run_time() function."""

    def test_extracts_run_time_from_run_id(self) -> None:
        """Extracts run_time from checkpoint's run_id."""
        expected_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        checkpoint_result = create_mock_checkpoint_result(run_time=expected_time)

        run_time = extract_run_time(checkpoint_result)

        assert run_time == expected_time

    def test_returns_current_time_when_run_id_missing(self) -> None:
        """Returns current UTC time when run_id is missing."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = None

        before = datetime.now(timezone.utc)
        run_time = extract_run_time(checkpoint_result)
        after = datetime.now(timezone.utc)

        assert before <= run_time <= after

    def test_returns_current_time_when_run_time_missing(self) -> None:
        """Returns current UTC time when run_id.run_time is None."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = MagicMock()
        checkpoint_result.run_id.run_time = None

        before = datetime.now(timezone.utc)
        run_time = extract_run_time(checkpoint_result)
        after = datetime.now(timezone.utc)

        assert before <= run_time <= after


# =============================================================================
# D. extract_datasets() Tests
# =============================================================================


@pytest.mark.unit
class TestExtractDatasets:
    """Tests for extract_datasets() function."""

    def test_extracts_dataset_from_batch_spec(self) -> None:
        """Extracts namespace and name from batch_spec."""
        validation_result = create_mock_validation_result(
            datasource_name="postgres_prod",
            data_asset_name="public.users",
        )

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["namespace"] == "postgres_prod"
        assert datasets[0]["name"] == "public.users"

    def test_returns_empty_list_when_no_meta(self) -> None:
        """Returns empty list when meta is not available."""
        validation_result = MagicMock()
        validation_result.meta = None

        datasets = extract_datasets(validation_result)

        assert datasets == []

    def test_returns_empty_list_when_batch_spec_empty(self) -> None:
        """Returns empty list when batch_spec has no datasource info."""
        validation_result = MagicMock()
        validation_result.meta = {"batch_spec": {}}

        datasets = extract_datasets(validation_result)

        assert datasets == []

    def test_uses_active_batch_definition_as_fallback(self) -> None:
        """Falls back to active_batch_definition when batch_spec incomplete."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {},
            "active_batch_definition": {
                "datasource_name": "snowflake_prod",
                "data_asset_name": "analytics.orders",
            },
        }

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["namespace"] == "snowflake_prod"
        assert datasets[0]["name"] == "analytics.orders"

    def test_batch_spec_takes_precedence_over_active_batch_definition(self) -> None:
        """batch_spec values take precedence over active_batch_definition."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "primary_ds",
                "data_asset_name": "primary_asset",
            },
            "active_batch_definition": {
                "datasource_name": "fallback_ds",
                "data_asset_name": "fallback_asset",
            },
        }

        datasets = extract_datasets(validation_result)

        assert datasets[0]["namespace"] == "primary_ds"
        assert datasets[0]["name"] == "primary_asset"

    def test_partial_info_uses_unknown_for_missing_name(self) -> None:
        """Uses 'unknown' for name when only datasource is available."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "my_datasource",
            },
        }

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["namespace"] == "my_datasource"
        assert datasets[0]["name"] == "unknown"

    def test_handles_complex_data_asset_names(self) -> None:
        """Handles complex data asset names with special characters."""
        validation_result = create_mock_validation_result(
            datasource_name="bigquery://project-123",
            data_asset_name="dataset.table$20240115",
        )

        datasets = extract_datasets(validation_result)

        assert datasets[0]["namespace"] == "bigquery://project-123"
        assert datasets[0]["name"] == "dataset.table$20240115"

    def test_includes_schema_from_batch_spec(self) -> None:
        """Includes schema_name prefix when available in batch_spec."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "demo_postgres",
                "data_asset_name": "customers",
                "schema_name": "marts",
            },
        }

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["namespace"] == "demo_postgres"
        assert datasets[0]["name"] == "marts.customers"

    def test_includes_schema_from_active_batch_definition(self) -> None:
        """Falls back to schema_name from active_batch_definition."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "demo_postgres",
                "data_asset_name": "orders",
            },
            "active_batch_definition": {
                "schema_name": "staging",
            },
        }

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["name"] == "staging.orders"

    def test_no_schema_prefix_when_schema_missing(self) -> None:
        """Does not add schema prefix when schema_name not available."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "demo_postgres",
                "data_asset_name": "customers",
            },
        }

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["name"] == "customers"

    def test_no_schema_prefix_when_schema_empty(self) -> None:
        """Does not add schema prefix when schema_name is empty string."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "demo_postgres",
                "data_asset_name": "customers",
                "schema_name": "",
            },
        }

        datasets = extract_datasets(validation_result)

        assert len(datasets) == 1
        assert datasets[0]["name"] == "customers"

    def test_batch_spec_schema_takes_precedence(self) -> None:
        """batch_spec schema_name takes precedence over active_batch_definition."""
        validation_result = MagicMock()
        validation_result.meta = {
            "batch_spec": {
                "datasource_name": "demo_postgres",
                "data_asset_name": "customers",
                "schema_name": "marts",
            },
            "active_batch_definition": {
                "schema_name": "staging",
            },
        }

        datasets = extract_datasets(validation_result)

        assert datasets[0]["name"] == "marts.customers"


# =============================================================================
# E. extract_data_quality_facets() Tests
# =============================================================================


@pytest.mark.unit
class TestExtractDataQualityFacets:
    """Tests for extract_data_quality_facets() function."""

    def test_raises_valueerror_when_producer_missing(self) -> None:
        """Raises ValueError when producer is not provided."""
        validation_result = create_mock_validation_result()

        with pytest.raises(ValueError, match="producer is required"):
            extract_data_quality_facets(validation_result, producer="")

    def test_raises_valueerror_when_producer_none(self) -> None:
        """Raises ValueError when producer is None."""
        validation_result = create_mock_validation_result()

        with pytest.raises(ValueError, match="producer is required"):
            extract_data_quality_facets(validation_result, producer=None)  # type: ignore[arg-type]

    def test_extracts_statistics_to_data_quality_facet(self) -> None:
        """Extracts statistics into dataQualityMetrics facet."""
        validation_result = create_mock_validation_result()

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert "dataQualityMetrics" in facets
        # NOTE: rowCount intentionally omitted - GE doesn't provide actual row counts
        assert "_producer" in facets["dataQualityMetrics"]
        assert "_schemaURL" in facets["dataQualityMetrics"]

    def test_extracts_assertions_from_results(self) -> None:
        """Extracts per-expectation assertions."""
        validation_result = create_mock_validation_result(
            expectations=[
                {
                    "type": "expect_column_values_to_not_be_null",
                    "success": True,
                    "column": "id",
                },
                {
                    "type": "expect_column_values_to_be_unique",
                    "success": False,
                    "column": "email",
                },
            ]
        )

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert "dataQualityAssertions" in facets
        assertions = facets["dataQualityAssertions"]["assertions"]
        assert len(assertions) == 2
        assert assertions[0]["assertion"] == "expect_column_values_to_not_be_null"
        assert assertions[0]["success"] is True
        assert assertions[0]["column"] == "id"
        assert assertions[1]["assertion"] == "expect_column_values_to_be_unique"
        assert assertions[1]["success"] is False
        assert assertions[1]["column"] == "email"

    def test_handles_expectations_without_column(self) -> None:
        """Handles expectations that don't have a column parameter."""
        validation_result = MagicMock()
        validation_result.statistics = {"evaluated_expectations": 1}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_table_row_count_to_be_between"
        )
        result.expectation_config.kwargs = {"min_value": 100, "max_value": 1000}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert len(assertions) == 1
        assert assertions[0]["assertion"] == "expect_table_row_count_to_be_between"
        assert assertions[0]["success"] is True
        assert "column" not in assertions[0]

    def test_handles_empty_results(self) -> None:
        """Returns empty assertions when no results available."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        validation_result.results = []

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert facets["dataQualityAssertions"]["assertions"] == []
        assert "dataQualityMetrics" in facets

    def test_handles_missing_statistics(self) -> None:
        """Handles missing statistics gracefully."""
        validation_result = MagicMock()
        validation_result.statistics = None
        validation_result.results = []

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert "dataQualityMetrics" in facets

    def test_handles_missing_expectation_config(self) -> None:
        """Handles results without expectation_config."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = None
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert len(assertions) == 1
        assert assertions[0]["assertion"] == "unknown"
        assert assertions[0]["success"] is True

    def test_uses_custom_producer_when_provided(self) -> None:
        """Uses custom producer URL when provided."""
        validation_result = create_mock_validation_result()
        custom_producer = "https://example.com/my-producer/v1.0.0"

        facets = extract_data_quality_facets(
            validation_result, producer=custom_producer
        )

        assert facets["dataQualityMetrics"]["_producer"] == custom_producer
        assert facets["dataQualityAssertions"]["_producer"] == custom_producer

    def test_schema_urls_are_valid_openlineage_urls(self) -> None:
        """Schema URLs point to valid OpenLineage spec URLs."""
        validation_result = create_mock_validation_result()

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert "openlineage.io/spec" in facets["dataQualityMetrics"]["_schemaURL"]
        assert "openlineage.io/spec" in facets["dataQualityAssertions"]["_schemaURL"]

    def test_handles_type_attribute_instead_of_expectation_type(self) -> None:
        """Handles config.type when expectation_type is not available."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock(spec=["type", "kwargs"])
        result.expectation_config.expectation_type = None
        result.expectation_config.type = "expect_column_to_exist"
        result.expectation_config.kwargs = {"column": "test_col"}
        # Remove expectation_type attribute
        del result.expectation_config.expectation_type
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert assertions[0]["assertion"] == "expect_column_to_exist"

    def test_message_populated_for_failed_assertion_with_unexpected_count(self) -> None:
        """Message populated for failed assertion with unexpected_count."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_column_values_to_not_be_null"
        )
        result.expectation_config.kwargs = {"column": "email"}
        result.result = {
            "unexpected_count": 5,
            "unexpected_percent": 0.33,
            "element_count": 1500,
        }
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert "message" in assertions[0]
        assert "5" in assertions[0]["message"]
        assert "0.33" in assertions[0]["message"]

    def test_message_populated_for_failed_assertion_with_observed_value(self) -> None:
        """Message populated for failed assertion with observed_value only."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_table_row_count_to_be_between"
        )
        result.expectation_config.kwargs = {}
        result.result = {"observed_value": 0}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert "message" in assertions[0]
        assert "0" in assertions[0]["message"]

    def test_message_populated_for_exception(self) -> None:
        """Message populated from exception_info when exception raised."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_to_exist"
        result.expectation_config.kwargs = {"column": "missing_col"}
        result.result = {}
        result.exception_info = {
            "raised_exception": True,
            "exception_message": "Column 'missing_col' not found in table",
        }
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert "message" in assertions[0]
        assert "Column 'missing_col' not found" in assertions[0]["message"]

    def test_no_message_for_successful_assertion(self) -> None:
        """No message populated for successful assertions."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_column_values_to_not_be_null"
        )
        result.expectation_config.kwargs = {"column": "email"}
        result.result = {"unexpected_count": 0, "element_count": 1500}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert "message" not in assertions[0]

    def test_message_fallback_when_result_empty(self) -> None:
        """Fallback message when result dict is empty and no exception."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_to_exist"
        result.expectation_config.kwargs = {}
        result.result = {}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert "message" in assertions[0]
        assert assertions[0]["message"] == "Validation failed"

    def test_duration_added_when_provided(self) -> None:
        """Duration added to assertions when duration_ms_per_expectation > 0."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_to_exist"
        result.expectation_config.kwargs = {"column": "id"}
        result.result = {}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(
            validation_result,
            producer=TEST_PRODUCER,
            duration_ms_per_expectation=42,
        )

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert assertions[0]["durationMs"] == 42

    def test_no_duration_when_zero(self) -> None:
        """No durationMs field when duration_ms_per_expectation is 0."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_to_exist"
        result.expectation_config.kwargs = {}
        result.result = {}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(
            validation_result,
            producer=TEST_PRODUCER,
            duration_ms_per_expectation=0,
        )

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert "durationMs" not in assertions[0]

    def test_duration_distributed_across_expectations(self) -> None:
        """Same duration applied to each expectation."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result1 = MagicMock()
        result1.success = True
        result1.expectation_config = MagicMock()
        result1.expectation_config.expectation_type = "expect_column_to_exist"
        result1.expectation_config.kwargs = {"column": "id"}
        result1.result = {}
        result1.exception_info = {"raised_exception": False}

        result2 = MagicMock()
        result2.success = True
        result2.expectation_config = MagicMock()
        result2.expectation_config.expectation_type = "expect_column_to_exist"
        result2.expectation_config.kwargs = {"column": "name"}
        result2.result = {}
        result2.exception_info = {"raised_exception": False}

        validation_result.results = [result1, result2]

        facets = extract_data_quality_facets(
            validation_result,
            producer=TEST_PRODUCER,
            duration_ms_per_expectation=100,
        )

        assertions = facets["dataQualityAssertions"]["assertions"]
        assert len(assertions) == 2
        assert assertions[0]["durationMs"] == 100
        assert assertions[1]["durationMs"] == 100

    def test_row_count_extracted_from_row_count_expectation(self) -> None:
        """rowCount populated from expect_table_row_count_* expectations."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_table_row_count_to_be_between"
        )
        result.expectation_config.kwargs = {}
        result.result = {"observed_value": 1500}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert facets["dataQualityMetrics"]["rowCount"] == 1500

    def test_null_count_extracted_from_not_null_expectation(self) -> None:
        """columnMetrics.nullCount populated from expect_column_values_to_not_be_null."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_column_values_to_not_be_null"
        )
        result.expectation_config.kwargs = {"column": "email"}
        result.result = {"unexpected_count": 5, "element_count": 1500}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert facets["dataQualityMetrics"]["columnMetrics"]["email"]["nullCount"] == 5

    def test_be_null_expectation_excluded_from_null_count(self) -> None:
        """expect_column_values_to_be_null does NOT populate nullCount.

        This expectation has inverted semantics: unexpected_count represents
        non-null values, not null values. Including it would report incorrect
        nullCount metrics.
        """
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_values_to_be_null"
        result.expectation_config.kwargs = {"column": "deleted_at"}
        # unexpected_count=10 means 10 NON-NULL values, not 10 nulls
        result.result = {"unexpected_count": 10, "element_count": 100}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        # Should NOT have nullCount for this column (inverted semantics)
        assert "deleted_at" not in facets["dataQualityMetrics"]["columnMetrics"]

    def test_distinct_count_extracted_from_unique_count_expectation(self) -> None:
        """columnMetrics.distinctCount populated from unique value count expectations."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = (
            "expect_column_unique_value_count_to_be_between"
        )
        result.expectation_config.kwargs = {"column": "status"}
        result.result = {"observed_value": 5}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert (
            facets["dataQualityMetrics"]["columnMetrics"]["status"]["distinctCount"]
            == 5
        )

    def test_multiple_column_metrics_aggregated(self) -> None:
        """Multiple column metrics aggregated correctly."""
        validation_result = MagicMock()
        validation_result.statistics = {}

        result1 = MagicMock()
        result1.success = False
        result1.expectation_config = MagicMock()
        result1.expectation_config.expectation_type = (
            "expect_column_values_to_not_be_null"
        )
        result1.expectation_config.kwargs = {"column": "email"}
        result1.result = {"unexpected_count": 5}
        result1.exception_info = {"raised_exception": False}

        result2 = MagicMock()
        result2.success = False
        result2.expectation_config = MagicMock()
        result2.expectation_config.expectation_type = (
            "expect_column_values_to_not_be_null"
        )
        result2.expectation_config.kwargs = {"column": "phone"}
        result2.result = {"unexpected_count": 10}
        result2.exception_info = {"raised_exception": False}

        validation_result.results = [result1, result2]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        column_metrics = facets["dataQualityMetrics"]["columnMetrics"]
        assert column_metrics["email"]["nullCount"] == 5
        assert column_metrics["phone"]["nullCount"] == 10

    def test_no_row_count_when_no_row_count_expectation(self) -> None:
        """No rowCount in metrics when no row count expectation present."""
        validation_result = MagicMock()
        validation_result.statistics = {}
        result = MagicMock()
        result.success = True
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_to_exist"
        result.expectation_config.kwargs = {"column": "id"}
        result.result = {}
        result.exception_info = {"raised_exception": False}
        validation_result.results = [result]

        facets = extract_data_quality_facets(validation_result, producer=TEST_PRODUCER)

        assert "rowCount" not in facets["dataQualityMetrics"]
