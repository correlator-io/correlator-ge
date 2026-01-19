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

    def test_extracts_run_name_and_generates_deterministic_uuid(self) -> None:
        """Generates deterministic UUID from checkpoint's run_name."""
        checkpoint_result = create_mock_checkpoint_result(run_name="manual-2024-01-15")

        run_id = extract_run_id(checkpoint_result)

        # Verify it's a valid UUID string (OpenLineage requirement)
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id

        # Verify it's deterministic (same input = same UUID)
        checkpoint_result_2 = create_mock_checkpoint_result(
            run_name="manual-2024-01-15"
        )
        run_id_2 = extract_run_id(checkpoint_result_2)
        assert run_id == run_id_2

    def test_generates_uuid_when_run_id_missing(self) -> None:
        """Generates UUID when run_id is not available."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = None

        run_id = extract_run_id(checkpoint_result)

        # Verify it's a valid UUID string
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id

    def test_generates_uuid_when_run_name_missing(self) -> None:
        """Generates UUID when run_id.run_name is None."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = MagicMock()
        checkpoint_result.run_id.run_name = None

        run_id = extract_run_id(checkpoint_result)

        # Verify it's a valid UUID string
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id

    def test_generates_uuid_when_run_name_empty(self) -> None:
        """Generates UUID when run_id.run_name is empty string."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = MagicMock()
        checkpoint_result.run_id.run_name = ""

        run_id = extract_run_id(checkpoint_result)

        # Verify it's a valid UUID string
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id

    def test_converts_run_name_to_uuid_deterministically(self) -> None:
        """Converts non-string run_name to deterministic UUID."""
        checkpoint_result = MagicMock()
        checkpoint_result.run_id = MagicMock()
        checkpoint_result.run_id.run_name = 12345  # Integer

        run_id = extract_run_id(checkpoint_result)

        # Verify it's a valid UUID string
        parsed = uuid.UUID(run_id)
        assert str(parsed) == run_id

        # Verify it's deterministic
        checkpoint_result_2 = MagicMock()
        checkpoint_result_2.run_id = MagicMock()
        checkpoint_result_2.run_id.run_name = 12345
        run_id_2 = extract_run_id(checkpoint_result_2)
        assert run_id == run_id_2


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
