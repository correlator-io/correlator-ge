"""Great Expectations validation action for correlator-ge.

This module provides functions that hook into Great Expectations checkpoint
validation and emit OpenLineage events to Correlator.

The action captures:
    - Validation start events (on_validation_start)
    - Validation success events (on_validation_success)
    - Validation failure events (on_validation_failed)

Usage:
    Configure in great_expectations.yml checkpoint config.
    See docs/CONFIGURATION.md for details.

Note:
    This is a skeleton implementation. The actual action architecture
    (class vs functions, GE action registration) will be determined
    after Task 2.2 research is complete.
"""

from typing import Any


def on_validation_start(
    checkpoint_name: Any,
    validation_config: Any,
) -> None:
    """Handle validation start event.

    Emits an OpenLineage START event when a checkpoint validation begins.

    Args:
        checkpoint_name: Name of the GE checkpoint being executed.
        validation_config: Configuration for the validation run.

    Raises:
        NotImplementedError: Skeleton implementation - not yet functional.
    """
    raise NotImplementedError(
        "on_validation_start() is not yet implemented. "
        "This is a skeleton release."
    )


def on_validation_success(
    checkpoint_name: Any,
    validation_result: Any,
) -> None:
    """Handle validation success event.

    Emits an OpenLineage COMPLETE event when validation passes.

    Args:
        checkpoint_name: Name of the GE checkpoint that was executed.
        validation_result: The validation result object from GE.

    Raises:
        NotImplementedError: Skeleton implementation - not yet functional.
    """
    raise NotImplementedError(
        "on_validation_success() is not yet implemented. "
        "This is a skeleton release."
    )


def on_validation_failed(
    checkpoint_name: Any,
    validation_result: Any,
) -> None:
    """Handle validation failure event.

    Emits an OpenLineage FAIL event when validation fails.

    Args:
        checkpoint_name: Name of the GE checkpoint that was executed.
        validation_result: The validation result object from GE.

    Raises:
        NotImplementedError: Skeleton implementation - not yet functional.
    """
    raise NotImplementedError(
        "on_validation_failed() is not yet implemented. "
        "This is a skeleton release."
    )
