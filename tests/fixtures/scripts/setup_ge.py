#!/usr/bin/env python3
"""Set up Great Expectations context for fixture generation.

Creates an ephemeral GE context with:
- PandasDatasource pointing to sample data
- Expectation suite with taxi data expectations
- Checkpoint for running validations

Run:
    python setup_ge.py

This script is idempotent - safe to run multiple times.

Requirements:
    - great_expectations >= 1.3.0
    - pandas
"""

from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeNull,
)


def create_context() -> Any:
    """Create an ephemeral GE context.

    Returns:
        Ephemeral DataContext configured for testing.
    """
    return gx.get_context(mode="ephemeral")


def add_datasource(context: Any, data_dir: Path) -> None:
    """Add PandasDatasource for CSV files.

    Args:
        context: GE DataContext.
        data_dir: Path to directory containing CSV files.
    """
    # Add Pandas datasource
    datasource = context.data_sources.add_pandas(name="taxi_datasource")

    # Add data assets for each CSV file
    datasource.add_csv_asset(
        name="taxi_clean",
        filepath_or_buffer=str(data_dir / "taxi_clean.csv"),
    )
    datasource.add_csv_asset(
        name="taxi_dirty",
        filepath_or_buffer=str(data_dir / "taxi_dirty.csv"),
    )

    print("Added datasource 'taxi_datasource' with assets: taxi_clean, taxi_dirty")


def create_expectation_suite(context: Any) -> ExpectationSuite:
    """Create expectation suite for taxi data validation.

    Expectations:
    - vendor_id: not null, in set [1, 2]
    - passenger_count: not null, between 1 and 6
    - fare_amount: between 0 and 500
    - trip_distance: >= 0

    Args:
        context: GE DataContext.

    Returns:
        Created ExpectationSuite.
    """
    suite = context.suites.add(ExpectationSuite(name="taxi_expectations"))

    # vendor_id expectations
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="vendor_id"))
    suite.add_expectation(ExpectColumnValuesToBeInSet(column="vendor_id", value_set=[1, 2]))

    # passenger_count expectations
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="passenger_count"))
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=1,
            max_value=6,
        )
    )

    # fare_amount expectations
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="fare_amount",
            min_value=0,
            max_value=500,
        )
    )

    # trip_distance expectations
    suite.add_expectation(
        ExpectColumnValuesToBeBetween(
            column="trip_distance",
            min_value=0,
        )
    )

    print(f"Created suite 'taxi_expectations' with {len(suite.expectations)} expectations")
    return suite


def create_validation_definitions(context: Any) -> tuple:
    """Create validation definitions for clean and dirty data.

    Args:
        context: GE DataContext.

    Returns:
        Tuple of (clean_validation, dirty_validation) definitions.
    """
    suite = context.suites.get("taxi_expectations")
    datasource = context.data_sources.get("taxi_datasource")

    # Create batch definitions
    clean_asset = datasource.get_asset("taxi_clean")
    dirty_asset = datasource.get_asset("taxi_dirty")

    clean_batch = clean_asset.add_batch_definition_whole_dataframe("full_clean")
    dirty_batch = dirty_asset.add_batch_definition_whole_dataframe("full_dirty")

    # Create validation definitions
    clean_validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="validate_taxi_clean",
            data=clean_batch,
            suite=suite,
        )
    )

    dirty_validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="validate_taxi_dirty",
            data=dirty_batch,
            suite=suite,
        )
    )

    print("Created validation definitions: validate_taxi_clean, validate_taxi_dirty")
    return clean_validation, dirty_validation


def create_checkpoints(
    context: Any,
    clean_validation: Any,
    dirty_validation: Any,
) -> tuple:
    """Create checkpoints for running validations.

    Args:
        context: GE DataContext.
        clean_validation: Validation definition for clean data.
        dirty_validation: Validation definition for dirty data.

    Returns:
        Tuple of (success_checkpoint, failure_checkpoint, multiple_checkpoint).
    """
    # Checkpoint for success scenario (clean data)
    success_checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="success_checkpoint",
            validation_definitions=[clean_validation],
        )
    )

    # Checkpoint for failure scenario (dirty data)
    failure_checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="failure_checkpoint",
            validation_definitions=[dirty_validation],
        )
    )

    # Checkpoint for multiple validations (both)
    multiple_checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="multiple_checkpoint",
            validation_definitions=[clean_validation, dirty_validation],
        )
    )

    print("Created checkpoints: success_checkpoint, failure_checkpoint, multiple_checkpoint")
    return success_checkpoint, failure_checkpoint, multiple_checkpoint


def setup_ge() -> Any:
    """Set up complete GE context for fixture generation.

    Returns:
        Configured DataContext.
    """
    print("Setting up Great Expectations context...")

    # Resolve data directory in ge_sample_project (sibling to scripts/)
    data_dir = Path(__file__).parent.parent / "ge_sample_project" / "data"
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Data directory not found: {data_dir}\n"
            "Run generate_sample_data.py first."
        )

    # Create context and configure
    context = create_context()
    add_datasource(context, data_dir)
    create_expectation_suite(context)
    clean_val, dirty_val = create_validation_definitions(context)
    create_checkpoints(context, clean_val, dirty_val)

    print("\nGE context setup complete!")
    return context


if __name__ == "__main__":
    setup_ge()
