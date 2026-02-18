"""Metadata extractors for Great Expectations objects.

This module extracts metadata from GE CheckpointResult and
ExpectationSuiteValidationResult objects for OpenLineage event construction.

Functions:
    extract_job_name: Build job name from checkpoint and suite names
    extract_run_id: Get or generate unique run identifier
    extract_run_time: Extract run start time from checkpoint
    extract_datasets: Extract dataset information from validation result
    extract_data_quality_facets: Map GE expectations to OpenLineage facets

Requirements:
    - Great Expectations >= 1.3.0
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from great_expectations.checkpoint import CheckpointResult
from great_expectations.core import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from uuid_extensions import uuid7 as uuid7_lib  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


# Expectation patterns that produce metrics
ROW_COUNT_EXPECTATIONS = {
    "expect_table_row_count_to_be_between",
    "expect_table_row_count_to_equal",
    "expect_table_row_count_to_equal_other_table",
}

NULL_COUNT_EXPECTATIONS = {
    "expect_column_values_to_not_be_null",
    # Note: expect_column_values_to_be_null is excluded because its unexpected_count
    # represents non-null values, not null values (inverted semantics)
}

DISTINCT_COUNT_EXPECTATIONS = {
    "expect_column_distinct_values_to_be_in_set",
    "expect_column_distinct_values_to_contain_set",
    "expect_column_distinct_values_to_equal_set",
    "expect_column_unique_value_count_to_be_between",
}


def _extract_metrics_from_results(
    results: list[ExpectationValidationResult],
) -> tuple[int | None, dict[str, dict[str, int]]]:
    """Extract rowCount and columnMetrics from expectation results.

    Parses expectation results to extract actual observed values for
    OpenLineage DataQualityMetrics facet.

    Args:
        results: List of GE ExpectationValidationResult objects.

    Returns:
        Tuple of (rowCount, columnMetrics dict).
        rowCount is None if no row count expectation found.
        columnMetrics maps column names to metric dicts with nullCount/distinctCount.
    """
    row_count: int | None = None
    column_metrics: dict[str, dict[str, int]] = {}

    for result in results:
        config = result.expectation_config
        if not config:
            continue

        # Get expectation type
        expectation_type = getattr(config, "expectation_type", None) or getattr(
            config, "type", None
        )
        if not expectation_type:
            continue

        result_dict = getattr(result, "result", {}) or {}
        kwargs = getattr(config, "kwargs", {}) or {}
        column = kwargs.get("column")

        # Extract rowCount from row count expectations
        if expectation_type in ROW_COUNT_EXPECTATIONS:
            observed = result_dict.get("observed_value")
            if isinstance(observed, int):
                row_count = observed

        # Extract nullCount - unexpected_count IS the null count for not-null expectations
        elif expectation_type in NULL_COUNT_EXPECTATIONS and column:
            unexpected_count = result_dict.get("unexpected_count")
            if unexpected_count is not None:
                column_metrics.setdefault(column, {})["nullCount"] = int(
                    unexpected_count
                )

        # Extract distinctCount from distinct value expectations
        elif expectation_type in DISTINCT_COUNT_EXPECTATIONS and column:
            observed = result_dict.get("observed_value")
            if isinstance(observed, int):
                column_metrics.setdefault(column, {})["distinctCount"] = observed

    return row_count, column_metrics


def _build_assertion_message(result: ExpectationValidationResult) -> str:
    """Build GE-native message from expectation result.

    Only populates message for failed assertions. Returns empty string
    for successful assertions.

    Message format follows GE native language:
    - Exception: "{exception_message}"
    - Unexpected values: "{unexpected_count} unexpected values found, {percent}% of {total} total"
    - Observed value: "Observed value: {observed_value}"
    - Fallback: "Validation failed"

    Args:
        result: GE ExpectationValidationResult object.

    Returns:
        Human-readable message string, or empty string for success.
    """
    # Don't add message for successful assertions
    if result.success:
        return ""

    # Check for exception first
    exception_info = getattr(result, "exception_info", {}) or {}
    if exception_info.get("raised_exception"):
        return str(
            exception_info.get("exception_message", "Validation failed with exception")
        )

    # Extract from result dict
    result_dict = getattr(result, "result", {}) or {}

    # Check for unexpected_count (null checks, uniqueness checks, etc.)
    unexpected_count = result_dict.get("unexpected_count")
    if unexpected_count is not None:
        unexpected_percent = result_dict.get("unexpected_percent", 0)
        element_count = result_dict.get("element_count", 0)
        return (
            f"{unexpected_count} unexpected values found, "
            f"{unexpected_percent:.2f}% of {element_count} total"
        )

    # Check for observed_value (row count, distinct count, etc.)
    observed_value = result_dict.get("observed_value")
    if observed_value is not None:
        return f"Observed value: {observed_value}"

    # Fallback for failed assertions without detailed result
    return "Validation failed"


def extract_job_name(
    checkpoint_result: CheckpointResult,
    validation_id: ValidationResultIdentifier,
) -> str:
    """Extract job name from checkpoint and validation.

    Format: {checkpoint_name}.{expectation_suite_name}

    Args:
        checkpoint_result: GE CheckpointResult object.
        validation_id: ValidationResultIdentifier for the specific validation.

    Returns:
        Job name in format "checkpoint_name.suite_name".

    Example:
        >>> job_name = extract_job_name(checkpoint_result, validation_id)
        >>> # Returns: "daily_validation.users_suite"
    """
    # Extract checkpoint name from config
    checkpoint_name = "unknown_checkpoint"
    if checkpoint_result.checkpoint_config and checkpoint_result.checkpoint_config.name:
        checkpoint_name = checkpoint_result.checkpoint_config.name

    # Extract suite name from validation_id
    suite_name = "unknown_suite"
    suite_id = validation_id.expectation_suite_identifier
    if suite_id:
        # Try 'name' attribute first (GE 1.3+), fallback to 'expectation_suite_name'
        if hasattr(suite_id, "name") and suite_id.name:
            suite_name = suite_id.name
        elif (
            hasattr(suite_id, "expectation_suite_name")
            and suite_id.expectation_suite_name
        ):
            suite_name = suite_id.expectation_suite_name

    return f"{checkpoint_name}.{suite_name}"


def extract_run_id() -> str:
    """Generate a unique run ID as UUID7.

    OpenLineage recommends UUID7 (time-ordered) for runId because it provides
    better database indexing and chronological sorting.

    Returns:
        UUID7 string for OpenLineage runId.

    Example:
        >>> run_id = extract_run_id()
        >>> # Returns: "01234567-89ab-7def-8123-456789abcdef"
    """
    return str(uuid7_lib())


def extract_run_time(checkpoint_result: CheckpointResult) -> datetime:
    """Extract run start time from checkpoint.

    Uses GE's run_id.run_time if available, otherwise returns current UTC time.

    Args:
        checkpoint_result: GE CheckpointResult object.

    Returns:
        Run start time as datetime (UTC).

    Example:
        >>> run_time = extract_run_time(checkpoint_result)
        >>> # Returns: datetime(2024, 1, 15, 10, 30, 0)
    """
    if checkpoint_result.run_id and checkpoint_result.run_id.run_time:
        run_time = checkpoint_result.run_id.run_time
        # Ensure we return a datetime object
        if isinstance(run_time, datetime):
            return run_time

    # Return current UTC time if run_time not available
    return datetime.now(timezone.utc)


def extract_datasets(
    validation_result: ExpectationSuiteValidationResult,
) -> list[dict[str, Any]]:
    """Extract dataset information from validation result.

    Extracts datasource_name and data_asset_name from batch_spec metadata.
    Returns list of OpenLineage dataset dictionaries.

    Args:
        validation_result: GE ExpectationSuiteValidationResult object.

    Returns:
        List of dataset dicts with 'namespace' and 'name' keys.

    Example:
        >>> datasets = extract_datasets(validation_result)
        >>> # Returns: [{"namespace": "postgres_prod", "name": "public.users"}]
    """
    datasets: list[dict[str, Any]] = []

    # Get meta dict from validation result (meta can be dict, object, or None)
    meta: dict[str, Any] = {}
    if validation_result.meta:
        # meta can be ExpectationSuiteValidationResultMeta or dict
        if isinstance(validation_result.meta, dict):
            meta = dict(validation_result.meta)
        else:
            # ExpectationSuiteValidationResultMeta - convert to dict via vars()
            # Note: type: ignore needed because mypy incorrectly infers unreachable
            # due to GE's complex union type for meta field
            meta = dict(vars(validation_result.meta))  # type: ignore[unreachable]

    # Extract from batch_spec
    batch_spec = meta.get("batch_spec", {})
    datasource_name = batch_spec.get("datasource_name")
    data_asset_name = batch_spec.get("data_asset_name")
    schema_name = batch_spec.get("schema_name")

    # Also check active_batch_definition (GE 1.x) for fallbacks
    batch_definition = meta.get("active_batch_definition", {})
    if not datasource_name:
        datasource_name = batch_definition.get("datasource_name")
    if not data_asset_name:
        data_asset_name = batch_definition.get("data_asset_name")
    if not schema_name:
        schema_name = batch_definition.get("schema_name")

    # Build dataset name with schema prefix if available
    # This ensures dataset URN matches dbt pattern: schema.table
    dataset_name = data_asset_name
    if schema_name and data_asset_name:
        dataset_name = f"{schema_name}.{data_asset_name}"

    # Build dataset if we have valid info
    # NOTE: Namespace Limitation
    # GE's datasource_name is a logical name (e.g., "postgres_prod"), not a connection
    # URI (e.g., "postgresql://prod-db:5432/mydb"). This differs from how dbt and Airflow
    # emit namespaces, which may prevent cross-tool correlation in Correlator.
    #
    # Mitigation: Correlator platform will implement namespace aliasing to map
    # logical names to canonical URIs. See: tech-debt document "Namespace
    # Aliasing for Cross-Tool Correlation"
    #
    # Extracting connection URIs from GE's datasource config was considered but deferred
    # due to complexity (varies by datasource type) vs. value for alpha.
    if datasource_name and dataset_name:
        datasets.append(
            {
                "namespace": datasource_name,
                "name": dataset_name,
            }
        )
    elif datasource_name:
        # Partial info - use datasource as namespace, unknown as name
        datasets.append(
            {
                "namespace": datasource_name,
                "name": "unknown",
            }
        )

    return datasets


def extract_data_quality_facets(
    validation_result: ExpectationSuiteValidationResult,
    producer: str,
    duration_ms_per_expectation: int = 0,
) -> dict[str, Any]:
    """Extract data quality metrics as OpenLineage facets.

    Maps GE expectations to OpenLineage DataQualityAssertions facet.
    Extracts statistics and per-expectation pass/fail results.

    Args:
        validation_result: GE ExpectationSuiteValidationResult object.
        producer: Producer URL for facet metadata (required).
            Should be the PRODUCER constant from ge_correlator.action.
        duration_ms_per_expectation: Estimated duration per expectation in ms.
            GE doesn't track per-expectation timing, so this is typically
            the total validation duration divided by number of expectations.

    Returns:
        Dict with dataQuality and dataQualityAssertions facets.

    Raises:
        ValueError: If producer is empty or None.

    Example:
        >>> from ge_correlator.action import PRODUCER
        >>> facets = extract_data_quality_facets(validation_result, producer=PRODUCER)
        >>> facets["dataQualityAssertions"]["assertions"]
        [{"assertion": "expect_column_values_to_not_be_null", "success": True, ...}]
    """
    if not producer:
        raise ValueError(
            "producer is required for extract_data_quality_facets(). "
            "Pass PRODUCER from ge_correlator.action."
        )

    facets: dict[str, Any] = {}

    results: list[ExpectationValidationResult] = validation_result.results or []

    # Extract metrics from results (rowCount, columnMetrics)
    row_count, column_metrics = _extract_metrics_from_results(results)

    # Build DataQualityMetrics facet with actual observed values
    data_quality_metrics: dict[str, Any] = {
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
        "columnMetrics": column_metrics,
    }
    if row_count is not None:
        data_quality_metrics["rowCount"] = row_count
    facets["dataQualityMetrics"] = data_quality_metrics

    # Build DataQualityAssertions facet from results
    assertions: list[dict[str, Any]] = []

    for result in results:
        assertion: dict[str, Any] = {
            "assertion": "unknown",
            "success": bool(result.success) if result.success is not None else False,
        }

        # Extract expectation type and column from config
        config = result.expectation_config
        if config:
            # Get expectation type (try expectation_type first, then type)
            if hasattr(config, "expectation_type") and config.expectation_type:
                assertion["assertion"] = config.expectation_type
            elif hasattr(config, "type") and config.type:
                assertion["assertion"] = config.type

            # Get column name if applicable
            # Note: kwargs access varies by GE version, keep defensive pattern
            kwargs: dict[str, Any] = {}
            if hasattr(config, "kwargs") and config.kwargs:
                kwargs = dict(config.kwargs) if isinstance(config.kwargs, dict) else {}
            elif hasattr(config, "to_json_dict"):
                # GE 1.x may use different structure
                try:
                    json_dict = config.to_json_dict()
                    raw_kwargs = json_dict.get("kwargs", {})
                    kwargs = dict(raw_kwargs) if isinstance(raw_kwargs, dict) else {}
                except Exception:  # nosec B110
                    # Fallback silently - kwargs extraction is best-effort
                    pass

            column = kwargs.get("column")
            if column:
                assertion["column"] = column

        # Add message for failed assertions
        message = _build_assertion_message(result)
        if message:
            assertion["message"] = message

        # Add duration if provided
        if duration_ms_per_expectation > 0:
            assertion["durationMs"] = duration_ms_per_expectation

        assertions.append(assertion)

    facets["dataQualityAssertions"] = {
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
        "assertions": assertions,
    }

    return facets
