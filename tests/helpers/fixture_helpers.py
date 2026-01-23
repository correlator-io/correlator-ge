"""Helpers for loading and reconstructing GE checkpoint fixtures.

This module provides utilities to:
1. Load JSON fixtures from tests/fixtures/
2. Reconstruct CheckpointResult-like objects for testing action.py and extractors.py

The reconstructed objects support the access patterns used in production code:
- Attribute access: checkpoint_result.success, checkpoint_result.run_id.run_name
- Dict access: validation_result.meta.get("batch_spec", {})
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def load_fixture(filename: str) -> dict[str, Any]:
    """Load a JSON fixture file.

    Args:
        filename: Name of the fixture file (e.g., "checkpoint_result_success.json")

    Returns:
        Parsed JSON as a dict.

    Raises:
        FileNotFoundError: If fixture file doesn't exist.
    """
    filepath = FIXTURES_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Fixture not found: {filepath}")

    with open(filepath) as f:
        return json.load(f)


def create_checkpoint_result(
    fixture_data: dict[str, Any],
    *,
    batch_spec_override: dict[str, Any] | None = None,
) -> SimpleNamespace:
    """Reconstruct a CheckpointResult-like object from fixture data.

    Creates an object that matches the access patterns in action.py and extractors.py:
    - checkpoint_result.success (bool)
    - checkpoint_result.checkpoint_config.name (str)
    - checkpoint_result.run_id.run_name (str | None)
    - checkpoint_result.run_id.run_time (datetime)
    - checkpoint_result.run_results (dict)

    Args:
        fixture_data: Parsed JSON fixture data.
        batch_spec_override: Optional override for batch_spec in all validation results.
            Useful for testing dataset extraction with specific datasource/asset names.

    Returns:
        SimpleNamespace mimicking CheckpointResult structure.
    """
    # Build run_id
    run_id_data = fixture_data.get("run_id", {})
    run_id = SimpleNamespace(
        run_name=run_id_data.get("run_name"),
        run_time=_parse_datetime(run_id_data.get("run_time")),
    )

    # Build checkpoint_config
    checkpoint_config_data = fixture_data.get("checkpoint_config", {})
    checkpoint_config = SimpleNamespace(
        name=checkpoint_config_data.get("name", "unknown_checkpoint"),
    )

    # Build run_results dict
    run_results = {}
    for _key, run_result_data in fixture_data.get("run_results", {}).items():
        validation_id = _create_validation_id(run_result_data.get("validation_id", {}))
        validation_result = _create_validation_result(
            run_result_data.get("validation_result", {}),
            batch_spec_override=batch_spec_override,
        )
        run_results[validation_id] = validation_result

    return SimpleNamespace(
        success=fixture_data.get("success", True),
        run_id=run_id,
        checkpoint_config=checkpoint_config,
        run_results=run_results,
    )


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO format datetime string."""
    if value is None:
        return None
    try:
        # Handle timezone-aware ISO format
        return datetime.fromisoformat(value)
    except ValueError:
        # Fallback for edge cases
        return datetime.now(timezone.utc)


def _create_validation_id(data: dict[str, Any]) -> _ValidationId:
    """Create a ValidationResultIdentifier-like object."""
    suite_data = data.get("expectation_suite_identifier", {})
    run_id_data = data.get("run_id", {})

    return _ValidationId(
        suite_name=suite_data.get("name"),
        run_name=run_id_data.get("run_name"),
        run_time=_parse_datetime(run_id_data.get("run_time")),
        batch_identifier=data.get("batch_identifier"),
    )


def _create_validation_result(
    data: dict[str, Any],
    *,
    batch_spec_override: dict[str, Any] | None = None,
) -> SimpleNamespace:
    """Create an ExpectationSuiteValidationResult-like object.

    The `meta` attribute is kept as a dict (not SimpleNamespace) because
    extractors.py uses `.get()` on it: `validation_result.meta.get("batch_spec", {})`
    """
    # Build meta as a dict (extractors.py uses .get() on it)
    meta_data = data.get("meta", {})
    meta = dict(meta_data)  # Copy to avoid mutation

    # Apply batch_spec override if provided
    if batch_spec_override is not None:
        meta["batch_spec"] = batch_spec_override

    # Build results list
    results = [_create_expectation_result(r) for r in data.get("results", [])]

    # Build statistics
    stats_data = data.get("statistics", {})
    statistics = {
        "evaluated_expectations": stats_data.get("evaluated_expectations", 0),
        "successful_expectations": stats_data.get("successful_expectations", 0),
        "unsuccessful_expectations": stats_data.get("unsuccessful_expectations", 0),
        "success_percent": stats_data.get("success_percent", 0.0),
    }

    return SimpleNamespace(
        success=data.get("success", True),
        meta=meta,  # Dict, not SimpleNamespace
        results=results,
        statistics=statistics,
    )


def _create_expectation_result(data: dict[str, Any]) -> SimpleNamespace:
    """Create an ExpectationValidationResult-like object."""
    config_data = data.get("expectation_config", {})
    result_data = data.get("result", {})

    # Build expectation_config with to_json_dict() method
    expectation_config = _ExpectationConfig(
        expectation_type=config_data.get("expectation_type", "unknown"),
        type=config_data.get("expectation_type", "unknown"),  # Fallback
        kwargs=config_data.get("kwargs", {}),
    )

    return SimpleNamespace(
        success=data.get("success", True),
        expectation_config=expectation_config,
        result=SimpleNamespace(
            element_count=result_data.get("element_count"),
            unexpected_count=result_data.get("unexpected_count"),
            unexpected_percent=result_data.get("unexpected_percent"),
            partial_unexpected_list=result_data.get("partial_unexpected_list", []),
        ),
    )


class _ValidationId:
    """Hashable validation identifier for use as dict key.

    Mimics GE's ValidationResultIdentifier with attribute access patterns
    used in extractors.py.
    """

    def __init__(
        self,
        suite_name: str | None,
        run_name: str | None,
        run_time: datetime | None,
        batch_identifier: str | None,
    ) -> None:
        self._suite_name = suite_name
        self._run_name = run_name
        self._run_time = run_time
        self._batch_identifier = batch_identifier

        # Create nested objects for attribute access
        self.expectation_suite_identifier = SimpleNamespace(
            name=suite_name,
            expectation_suite_name=suite_name,  # Fallback attribute
        )
        self.run_id = SimpleNamespace(
            run_name=run_name,
            run_time=run_time,
        )
        self.batch_identifier = batch_identifier

    def __hash__(self) -> int:
        return hash((self._suite_name, self._run_name, self._batch_identifier))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ValidationId):
            return False
        return (
            self._suite_name == other._suite_name
            and self._run_name == other._run_name
            and self._batch_identifier == other._batch_identifier
        )

    def __repr__(self) -> str:
        return f"ValidationId({self._suite_name}::{self._run_name}::{self._batch_identifier})"


class _ExpectationConfig:
    """Expectation config with to_json_dict() method.

    extractors.py tries to call to_json_dict() as a fallback for getting kwargs.
    """

    def __init__(
        self,
        expectation_type: str,
        type: str,
        kwargs: dict[str, Any],
    ) -> None:
        self.expectation_type = expectation_type
        self.type = type
        self.kwargs = kwargs

    def to_json_dict(self) -> dict[str, Any]:
        """Return JSON-serializable dict representation."""
        return {
            "expectation_type": self.expectation_type,
            "kwargs": self.kwargs,
        }


# Convenience functions for common fixtures


def load_success_fixture() -> SimpleNamespace:
    """Load and reconstruct the success checkpoint fixture."""
    data = load_fixture("checkpoint_result_success.json")
    return create_checkpoint_result(data)


def load_failure_fixture() -> SimpleNamespace:
    """Load and reconstruct the failure checkpoint fixture."""
    data = load_fixture("checkpoint_result_failure.json")
    return create_checkpoint_result(data)


def load_multiple_fixture() -> SimpleNamespace:
    """Load and reconstruct the multiple validations checkpoint fixture."""
    data = load_fixture("checkpoint_result_multiple.json")
    return create_checkpoint_result(data)
