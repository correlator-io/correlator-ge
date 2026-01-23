"""Tests for fixture_helpers module.

Verifies that fixture loading and CheckpointResult reconstruction work correctly.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from ge_correlator.extractors import extract_datasets
from tests.helpers.fixture_helpers import (
    create_checkpoint_result,
    load_failure_fixture,
    load_fixture,
    load_multiple_fixture,
    load_success_fixture,
)


@pytest.mark.unit
class TestLoadFixture:
    """Tests for load_fixture function."""

    def test_loads_success_fixture(self) -> None:
        """Loads checkpoint_result_success.json successfully."""
        data = load_fixture("checkpoint_result_success.json")
        assert isinstance(data, dict)
        assert data["success"] is True
        assert "_metadata" in data

    def test_loads_failure_fixture(self) -> None:
        """Loads checkpoint_result_failure.json successfully."""
        data = load_fixture("checkpoint_result_failure.json")
        assert isinstance(data, dict)
        assert data["success"] is False

    def test_loads_multiple_fixture(self) -> None:
        """Loads checkpoint_result_multiple.json successfully."""
        data = load_fixture("checkpoint_result_multiple.json")
        assert isinstance(data, dict)
        assert "run_results" in data

    def test_raises_for_missing_fixture(self) -> None:
        """Raises FileNotFoundError for non-existent fixture."""
        with pytest.raises(FileNotFoundError):
            load_fixture("nonexistent.json")


@pytest.mark.unit
class TestCreateCheckpointResult:
    """Tests for create_checkpoint_result function."""

    def test_creates_object_with_success_attribute(self) -> None:
        """Created object has success attribute."""
        data = load_fixture("checkpoint_result_success.json")
        result = create_checkpoint_result(data)
        assert result.success is True

    def test_creates_object_with_checkpoint_config_name(self) -> None:
        """Created object has checkpoint_config.name attribute."""
        data = load_fixture("checkpoint_result_success.json")
        result = create_checkpoint_result(data)
        assert hasattr(result, "checkpoint_config")
        assert hasattr(result.checkpoint_config, "name")
        assert result.checkpoint_config.name == "success_checkpoint"

    def test_creates_object_with_run_id_run_name(self) -> None:
        """Created object has run_id.run_name attribute."""
        data = load_fixture("checkpoint_result_success.json")
        result = create_checkpoint_result(data)
        assert hasattr(result, "run_id")
        assert hasattr(result.run_id, "run_name")
        # run_name can be None in ephemeral context

    def test_creates_object_with_run_id_run_time(self) -> None:
        """Created object has run_id.run_time as datetime."""
        data = load_fixture("checkpoint_result_success.json")
        result = create_checkpoint_result(data)
        assert hasattr(result.run_id, "run_time")
        assert isinstance(result.run_id.run_time, datetime)

    def test_creates_run_results_as_dict(self) -> None:
        """run_results is a dict that can be iterated with .items()."""
        data = load_fixture("checkpoint_result_success.json")
        result = create_checkpoint_result(data)
        assert isinstance(result.run_results, dict)
        assert len(result.run_results) > 0

        # Verify we can iterate like action.py does
        for validation_id, validation_result in result.run_results.items():
            assert hasattr(validation_id, "expectation_suite_identifier")
            assert hasattr(validation_result, "success")


@pytest.mark.unit
class TestValidationIdStructure:
    """Tests for validation_id structure in run_results."""

    def test_validation_id_has_expectation_suite_identifier(self) -> None:
        """validation_id has expectation_suite_identifier with name."""
        result = load_success_fixture()
        validation_id = next(iter(result.run_results.keys()))

        assert hasattr(validation_id, "expectation_suite_identifier")
        assert hasattr(validation_id.expectation_suite_identifier, "name")

    def test_validation_id_has_fallback_expectation_suite_name(self) -> None:
        """validation_id.expectation_suite_identifier has fallback attribute."""
        result = load_success_fixture()
        validation_id = next(iter(result.run_results.keys()))

        # extractors.py checks this as fallback
        assert hasattr(
            validation_id.expectation_suite_identifier, "expectation_suite_name"
        )


@pytest.mark.unit
class TestValidationResultStructure:
    """Tests for validation_result structure in run_results."""

    def test_validation_result_has_success(self) -> None:
        """validation_result has success attribute."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))
        assert hasattr(validation_result, "success")
        assert isinstance(validation_result.success, bool)

    def test_validation_result_meta_is_dict(self) -> None:
        """validation_result.meta is a dict (for .get() access)."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))

        assert hasattr(validation_result, "meta")
        assert isinstance(validation_result.meta, dict)

    def test_validation_result_meta_supports_get(self) -> None:
        """validation_result.meta supports .get() for batch_spec."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))

        # This is how extractors.py accesses batch_spec
        batch_spec = validation_result.meta.get("batch_spec", {})
        assert isinstance(batch_spec, dict)

    def test_validation_result_has_results_list(self) -> None:
        """validation_result.results is a list of expectation results."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))

        assert hasattr(validation_result, "results")
        assert isinstance(validation_result.results, list)
        assert len(validation_result.results) > 0

    def test_validation_result_has_statistics(self) -> None:
        """validation_result.statistics is a dict."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))

        assert hasattr(validation_result, "statistics")
        assert isinstance(validation_result.statistics, dict)


@pytest.mark.unit
class TestExpectationResultStructure:
    """Tests for individual expectation results."""

    def test_expectation_result_has_success(self) -> None:
        """expectation result has success attribute."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))
        expectation_result = validation_result.results[0]

        assert hasattr(expectation_result, "success")

    def test_expectation_result_has_expectation_config(self) -> None:
        """expectation result has expectation_config with type and kwargs."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))
        expectation_result = validation_result.results[0]

        assert hasattr(expectation_result, "expectation_config")
        config = expectation_result.expectation_config

        assert hasattr(config, "expectation_type")
        assert hasattr(config, "kwargs")

    def test_expectation_config_has_to_json_dict(self) -> None:
        """expectation_config has to_json_dict() method."""
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))
        expectation_result = validation_result.results[0]
        config = expectation_result.expectation_config

        assert hasattr(config, "to_json_dict")
        json_dict = config.to_json_dict()
        assert isinstance(json_dict, dict)
        assert "kwargs" in json_dict


@pytest.mark.unit
class TestBatchSpecOverride:
    """Tests for batch_spec_override parameter."""

    def test_override_applies_to_meta(self) -> None:
        """batch_spec_override replaces batch_spec in meta."""
        data = load_fixture("checkpoint_result_success.json")
        override = {
            "datasource_name": "custom_datasource",
            "data_asset_name": "custom_table",
        }

        result = create_checkpoint_result(data, batch_spec_override=override)
        validation_result = next(iter(result.run_results.values()))

        batch_spec = validation_result.meta.get("batch_spec", {})
        assert batch_spec["datasource_name"] == "custom_datasource"
        assert batch_spec["data_asset_name"] == "custom_table"

    def test_no_override_keeps_original(self) -> None:
        """Without override, original batch_spec is preserved."""
        data = load_fixture("checkpoint_result_success.json")
        result = create_checkpoint_result(data)
        validation_result = next(iter(result.run_results.values()))

        # Should have original meta from fixture
        assert (
            "batch_spec" in validation_result.meta
            or "active_batch_definition" in validation_result.meta
        )


@pytest.mark.unit
class TestBatchSpecExtractionPath:
    """Tests verifying batch_spec_override works with extractors.

    The fixtures have null batch_spec (GE ephemeral context behavior),
    so extractors.py falls back to active_batch_definition. These tests
    verify the PRIMARY extraction path (batch_spec) works correctly
    when batch_spec is populated.
    """

    def test_extract_datasets_uses_batch_spec_when_populated(self) -> None:
        """extract_datasets uses batch_spec over active_batch_definition."""
        data = load_fixture("checkpoint_result_success.json")
        # Override with different values than active_batch_definition
        override = {
            "datasource_name": "postgres_prod",
            "data_asset_name": "public.orders",
        }

        result = create_checkpoint_result(data, batch_spec_override=override)
        validation_result = next(iter(result.run_results.values()))

        datasets = extract_datasets(validation_result)

        # Should use batch_spec values, NOT active_batch_definition
        assert len(datasets) == 1
        assert datasets[0]["namespace"] == "postgres_prod"
        assert datasets[0]["name"] == "public.orders"

    def test_extract_datasets_fallback_without_override(self) -> None:
        """extract_datasets falls back to active_batch_definition when batch_spec is null."""
        # Load fixture without override - batch_spec has null values
        result = load_success_fixture()
        validation_result = next(iter(result.run_results.values()))

        datasets = extract_datasets(validation_result)

        # Should fall back to active_batch_definition values
        assert len(datasets) == 1
        assert datasets[0]["namespace"] == "taxi_datasource"
        assert datasets[0]["name"] == "taxi_clean"

    def test_batch_spec_takes_precedence_over_active_batch_definition(self) -> None:
        """Verifies batch_spec is checked first, active_batch_definition second."""
        data = load_fixture("checkpoint_result_success.json")
        # Use completely different values to prove batch_spec wins
        override = {
            "datasource_name": "snowflake_warehouse",
            "data_asset_name": "analytics.fact_sales",
        }

        result = create_checkpoint_result(data, batch_spec_override=override)
        validation_result = next(iter(result.run_results.values()))

        # Verify active_batch_definition still exists with original values
        active_def = validation_result.meta.get("active_batch_definition", {})
        assert active_def.get("datasource_name") == "taxi_datasource"

        # But extract_datasets should use batch_spec
        datasets = extract_datasets(validation_result)
        assert datasets[0]["namespace"] == "snowflake_warehouse"
        assert datasets[0]["name"] == "analytics.fact_sales"


@pytest.mark.unit
class TestConvenienceFunctions:
    """Tests for convenience loader functions."""

    def test_load_success_fixture_returns_namespace(self) -> None:
        """load_success_fixture returns reconstructed object."""
        result = load_success_fixture()
        assert result.success is True
        assert hasattr(result, "run_results")

    def test_load_failure_fixture_returns_namespace(self) -> None:
        """load_failure_fixture returns reconstructed object."""
        result = load_failure_fixture()
        assert result.success is False

    def test_load_multiple_fixture_has_multiple_results(self) -> None:
        """load_multiple_fixture has multiple validation results."""
        result = load_multiple_fixture()
        assert len(result.run_results) == 2
