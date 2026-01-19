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

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def extract_job_name(
    checkpoint_result: Any,
    validation_id: Any,
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
    if hasattr(checkpoint_result, "checkpoint_config"):
        config = checkpoint_result.checkpoint_config
        if hasattr(config, "name") and config.name:
            checkpoint_name = config.name

    # Extract suite name from validation_id
    suite_name = "unknown_suite"
    if hasattr(validation_id, "expectation_suite_identifier"):
        suite_id = validation_id.expectation_suite_identifier
        if hasattr(suite_id, "name") and suite_id.name:
            suite_name = suite_id.name
        elif (
            hasattr(suite_id, "expectation_suite_name")
            and suite_id.expectation_suite_name
        ):
            suite_name = suite_id.expectation_suite_name

    return f"{checkpoint_name}.{suite_name}"


def extract_run_id(checkpoint_result: Any) -> str:
    """Extract or generate a unique run ID as UUID.

    OpenLineage requires runId to be a valid UUID. If GE provides a run_name,
    we generate a deterministic UUID from it using uuid5 with a namespace.
    This ensures the same run_name always produces the same UUID for correlation.

    Args:
        checkpoint_result: GE CheckpointResult object.

    Returns:
        UUID string for OpenLineage runId.

    Example:
        >>> run_id = extract_run_id(checkpoint_result)
        >>> # Returns: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    """
    # Namespace UUID for GE runs (deterministic generation)
    GE_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

    if hasattr(checkpoint_result, "run_id"):
        run_id = checkpoint_result.run_id
        if hasattr(run_id, "run_name") and run_id.run_name:
            # Generate deterministic UUID from run_name for consistency
            return str(uuid.uuid5(GE_NAMESPACE, str(run_id.run_name)))

    # Generate random UUID if run_id not available
    return str(uuid.uuid4())


def extract_run_time(checkpoint_result: Any) -> datetime:
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
    if hasattr(checkpoint_result, "run_id"):
        run_id = checkpoint_result.run_id
        if hasattr(run_id, "run_time") and run_id.run_time:
            run_time = run_id.run_time
            # Ensure we return a datetime object
            if isinstance(run_time, datetime):
                return run_time

    # Return current UTC time if run_time not available
    return datetime.now(timezone.utc)


def extract_datasets(
    validation_result: Any,
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

    # Get meta dict from validation result
    meta: dict[str, Any] = {}
    if hasattr(validation_result, "meta") and validation_result.meta:
        meta = validation_result.meta

    # Extract from batch_spec
    batch_spec = meta.get("batch_spec", {})
    datasource_name = batch_spec.get("datasource_name")
    data_asset_name = batch_spec.get("data_asset_name")

    # Also check active_batch_definition (GE 1.x)
    if not datasource_name or not data_asset_name:
        batch_definition = meta.get("active_batch_definition", {})
        if not datasource_name:
            datasource_name = batch_definition.get("datasource_name")
        if not data_asset_name:
            data_asset_name = batch_definition.get("data_asset_name")

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
    if datasource_name and data_asset_name:
        datasets.append(
            {
                "namespace": datasource_name,
                "name": data_asset_name,
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
    validation_result: Any,
    producer: str,
) -> dict[str, Any]:
    """Extract data quality metrics as OpenLineage facets.

    Maps GE expectations to OpenLineage DataQualityAssertions facet.
    Extracts statistics and per-expectation pass/fail results.

    Args:
        validation_result: GE ExpectationSuiteValidationResult object.
        producer: Producer URL for facet metadata (required).
            Should be the PRODUCER constant from ge_correlator.action.

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

    # Build DataQualityMetrics facet
    # NOTE: rowCount is intentionally omitted. The OpenLineage spec defines rowCount
    # as "number of rows in the dataset", but GE validation results don't provide
    # actual row counts - only expectation counts. Using evaluated_expectations
    # for rowCount would be semantically incorrect. If GE exposes row count via
    # batch_markers or action_context in the future, we can add it here.
    facets["dataQualityMetrics"] = {
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
        "columnMetrics": {},
    }

    # Build DataQualityAssertions facet from results
    assertions: list[dict[str, Any]] = []

    results: list[Any] = []
    if hasattr(validation_result, "results") and validation_result.results:
        results = validation_result.results

    for result in results:
        assertion: dict[str, Any] = {
            "assertion": "unknown",
            "success": False,
        }

        # Extract success status
        if hasattr(result, "success"):
            assertion["success"] = bool(result.success)

        # Extract expectation type and column
        if hasattr(result, "expectation_config"):
            config = result.expectation_config

            # Get expectation type
            if hasattr(config, "expectation_type"):
                assertion["assertion"] = config.expectation_type
            elif hasattr(config, "type"):
                assertion["assertion"] = config.type

            # Get column name if applicable
            kwargs = {}
            if hasattr(config, "kwargs") and config.kwargs:
                kwargs = config.kwargs
            elif hasattr(config, "to_json_dict"):
                # GE 1.x may use different structure
                try:
                    json_dict = config.to_json_dict()
                    kwargs = json_dict.get("kwargs", {})
                except Exception:  # nosec B110
                    # Fallback silently - kwargs extraction is best-effort
                    pass

            column = kwargs.get("column")
            if column:
                assertion["column"] = column

        assertions.append(assertion)

    facets["dataQualityAssertions"] = {
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
        "assertions": assertions,
    }

    return facets
