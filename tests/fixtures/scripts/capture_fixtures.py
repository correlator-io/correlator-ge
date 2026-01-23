#!/usr/bin/env python3
"""Capture GE checkpoint results as JSON fixtures.

Runs checkpoints and serializes CheckpointResult objects to JSON files
for use in integration tests.

Run:
    python capture_fixtures.py

Output:
    ../checkpoint_result_success.json   - All expectations pass
    ../checkpoint_result_failure.json   - Multiple expectation failures
    ../checkpoint_result_multiple.json  - Two validations, mixed results

Requirements:
    - great_expectations >= 1.3.0
    - Run generate_sample_data.py first
    - Run setup_ge.py conceptually (this script sets up its own context)
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import great_expectations
from great_expectations.core import RunIdentifier

# Add scripts directory to path for local imports
sys.path.insert(0, str(Path(__file__).parent))

from setup_ge import setup_ge


def serialize_checkpoint_result(result: Any, checkpoint_name: str) -> dict[str, Any]:
    """Serialize CheckpointResult to a JSON-serializable dict.

    Captures all fields accessed by action.py and extractors.py:
    - success: overall checkpoint success
    - run_id.run_name: for deterministic UUID generation
    - run_id.run_time: for event timestamps
    - checkpoint_config.name: for job name extraction
    - run_results: dict of validation_id → run_result

    Args:
        result: GE CheckpointResult object.
        checkpoint_name: Name of the checkpoint for metadata.

    Returns:
        Dict ready for JSON serialization.
    """
    serialized: dict[str, Any] = {
        "_metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "checkpoint_name": checkpoint_name,
            "ge_version": _get_ge_version(),
        },
        "success": result.success,
        "run_id": _serialize_run_id(result.run_id),
        "checkpoint_config": _serialize_checkpoint_config(result, checkpoint_name),
        "run_results": _serialize_run_results(result.run_results),
    }
    return serialized


def _get_ge_version() -> str:
    """Get Great Expectations version."""
    return great_expectations.__version__


def _serialize_run_id(run_id: Any) -> dict[str, Any]:
    """Serialize RunIdentifier."""
    run_name = None
    if hasattr(run_id, "run_name") and run_id.run_name is not None:
        run_name = str(run_id.run_name)

    run_time = None
    if hasattr(run_id, "run_time") and run_id.run_time is not None:
        run_time = run_id.run_time.isoformat()

    return {
        "run_name": run_name,
        "run_time": run_time,
    }


def _serialize_checkpoint_config(result: Any, checkpoint_name: str) -> dict[str, Any]:
    """Serialize checkpoint config."""
    config: dict[str, Any] = {"name": checkpoint_name}

    if hasattr(result, "checkpoint_config"):
        cfg = result.checkpoint_config
        if hasattr(cfg, "name") and cfg.name:
            config["name"] = cfg.name

    return config


def _serialize_run_results(run_results: dict[Any, Any]) -> dict[str, Any]:
    """Serialize run_results dict.

    GE 1.x structure: {validation_id: ExpectationSuiteValidationResult}
    Values are validation results directly (not wrapped in dict).
    """
    serialized: dict[str, Any] = {}

    for validation_id, validation_result in run_results.items():
        vid_str = _serialize_validation_id(validation_id)
        serialized[vid_str] = {
            "validation_id": _serialize_validation_id_full(validation_id),
            "validation_result": _serialize_validation_result(validation_result),
        }

    return serialized


def _serialize_validation_id(validation_id: Any) -> str:
    """Serialize validation_id to unique string key."""
    parts = []

    if hasattr(validation_id, "expectation_suite_identifier"):
        suite_id = validation_id.expectation_suite_identifier
        if hasattr(suite_id, "name"):
            parts.append(suite_id.name)
        elif hasattr(suite_id, "expectation_suite_name"):
            parts.append(suite_id.expectation_suite_name)

    if hasattr(validation_id, "run_id"):
        run_id = validation_id.run_id
        if hasattr(run_id, "run_name"):
            parts.append(str(run_id.run_name))

    # Include batch_identifier for uniqueness when multiple validations share suite/run
    if hasattr(validation_id, "batch_identifier") and validation_id.batch_identifier:
        parts.append(str(validation_id.batch_identifier))

    return "::".join(parts) if parts else str(validation_id)


def _serialize_validation_id_full(validation_id: Any) -> dict[str, Any]:
    """Serialize validation_id with full structure for reconstruction."""
    result: dict[str, Any] = {}

    if hasattr(validation_id, "expectation_suite_identifier"):
        suite_id = validation_id.expectation_suite_identifier
        result["expectation_suite_identifier"] = {
            "name": getattr(suite_id, "name", None) or getattr(suite_id, "expectation_suite_name", None),
        }

    if hasattr(validation_id, "run_id"):
        result["run_id"] = _serialize_run_id(validation_id.run_id)

    if hasattr(validation_id, "batch_identifier"):
        result["batch_identifier"] = str(validation_id.batch_identifier)

    return result


def _serialize_validation_result(validation_result: Any) -> dict[str, Any]:
    """Serialize ExpectationSuiteValidationResult."""
    serialized: dict[str, Any] = {
        "success": validation_result.success,
        "results": [],
        "meta": {},
        "statistics": {},
    }

    # Serialize results (list of ExpectationValidationResult)
    if hasattr(validation_result, "results") and validation_result.results:
        for result in validation_result.results:
            serialized["results"].append(_serialize_expectation_result(result))

    # Serialize meta (includes batch_spec)
    if hasattr(validation_result, "meta") and validation_result.meta:
        meta = validation_result.meta
        serialized["meta"] = _serialize_meta(meta)

    # Serialize statistics
    if hasattr(validation_result, "statistics") and validation_result.statistics:
        stats = validation_result.statistics
        serialized["statistics"] = {
            "evaluated_expectations": getattr(stats, "evaluated_expectations", 0),
            "successful_expectations": getattr(stats, "successful_expectations", 0),
            "unsuccessful_expectations": getattr(stats, "unsuccessful_expectations", 0),
            "success_percent": getattr(stats, "success_percent", 0.0),
        }

    return serialized


def _serialize_meta(meta: Any) -> dict[str, Any]:
    """Serialize validation result meta dict."""
    serialized: dict[str, Any] = {}

    # Handle dict-like meta
    if isinstance(meta, dict):
        for key, value in meta.items():
            if key == "batch_spec":
                serialized["batch_spec"] = _serialize_batch_spec(value)
            elif key == "active_batch_definition":
                serialized["active_batch_definition"] = _serialize_batch_definition(value)
            else:
                # Try to serialize, fall back to string
                try:
                    json.dumps(value)
                    serialized[key] = value
                except (TypeError, ValueError):
                    serialized[key] = str(value)

    return serialized


def _serialize_batch_spec(batch_spec: Any) -> dict[str, Any]:
    """Serialize batch_spec from meta."""
    if isinstance(batch_spec, dict):
        return {
            "datasource_name": batch_spec.get("datasource_name"),
            "data_asset_name": batch_spec.get("data_asset_name"),
            "batch_identifiers": batch_spec.get("batch_identifiers", {}),
        }

    # Handle object-like batch_spec
    return {
        "datasource_name": getattr(batch_spec, "datasource_name", None),
        "data_asset_name": getattr(batch_spec, "data_asset_name", None),
        "batch_identifiers": getattr(batch_spec, "batch_identifiers", {}),
    }


def _serialize_batch_definition(batch_def: Any) -> dict[str, Any]:
    """Serialize active_batch_definition from meta."""
    if isinstance(batch_def, dict):
        return {
            "datasource_name": batch_def.get("datasource_name"),
            "data_asset_name": batch_def.get("data_asset_name"),
        }

    return {
        "datasource_name": getattr(batch_def, "datasource_name", None),
        "data_asset_name": getattr(batch_def, "data_asset_name", None),
    }


def _serialize_expectation_result(result: Any) -> dict[str, Any]:
    """Serialize ExpectationValidationResult."""
    serialized: dict[str, Any] = {
        "success": bool(result.success),  # Ensure native Python bool
        "expectation_config": {},
        "result": {},
    }

    # Serialize expectation_config
    if hasattr(result, "expectation_config"):
        config = result.expectation_config
        serialized["expectation_config"] = {
            "expectation_type": getattr(config, "expectation_type", None) or getattr(config, "type", "unknown"),
            "kwargs": _get_config_kwargs(config),
        }

    # Serialize result details
    if hasattr(result, "result") and result.result:
        res = result.result
        serialized["result"] = {
            "element_count": getattr(res, "element_count", None),
            "unexpected_count": getattr(res, "unexpected_count", None),
            "unexpected_percent": getattr(res, "unexpected_percent", None),
            "partial_unexpected_list": getattr(res, "partial_unexpected_list", []),
        }

    return serialized


def _get_config_kwargs(config: Any) -> dict[str, Any]:
    """Extract kwargs from expectation config."""
    kwargs: dict[str, Any] = {}

    if hasattr(config, "kwargs") and config.kwargs:
        kwargs = dict(config.kwargs)
    elif hasattr(config, "to_json_dict"):
        try:
            json_dict = config.to_json_dict()
            kwargs = json_dict.get("kwargs", {})
        except Exception:
            pass

    # Ensure JSON serializable
    return {k: v for k, v in kwargs.items() if _is_json_serializable(v)}


def _is_json_serializable(value: Any) -> bool:
    """Check if value is JSON serializable."""
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def capture_fixtures() -> None:
    """Capture all fixture files."""
    print("Capturing GE checkpoint fixtures...")

    # Set up GE context
    context = setup_ge()
    output_dir = Path(__file__).parent.parent  # tests/fixtures/

    # Capture success fixture
    # Use explicit run_id with run_name for deterministic UUID generation in tests
    print("\n--- Running success_checkpoint (clean data) ---")
    success_checkpoint = context.checkpoints.get("success_checkpoint")
    success_result = success_checkpoint.run(
        run_id=RunIdentifier(run_name="test-success-run")
    )
    success_fixture = serialize_checkpoint_result(success_result, "success_checkpoint")
    _write_fixture(output_dir / "checkpoint_result_success.json", success_fixture)

    # Capture failure fixture
    print("\n--- Running failure_checkpoint (dirty data) ---")
    failure_checkpoint = context.checkpoints.get("failure_checkpoint")
    failure_result = failure_checkpoint.run(
        run_id=RunIdentifier(run_name="test-failure-run")
    )
    failure_fixture = serialize_checkpoint_result(failure_result, "failure_checkpoint")
    _write_fixture(output_dir / "checkpoint_result_failure.json", failure_fixture)

    # Capture multiple fixture
    print("\n--- Running multiple_checkpoint (both) ---")
    multiple_checkpoint = context.checkpoints.get("multiple_checkpoint")
    multiple_result = multiple_checkpoint.run(
        run_id=RunIdentifier(run_name="test-multiple-run")
    )
    multiple_fixture = serialize_checkpoint_result(multiple_result, "multiple_checkpoint")
    _write_fixture(output_dir / "checkpoint_result_multiple.json", multiple_fixture)

    print("\nFixtures captured successfully!")
    print(f"Output directory: {output_dir}")


def _write_fixture(path: Path, data: dict[str, Any]) -> None:
    """Write fixture to JSON file."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Wrote: {path}")


if __name__ == "__main__":
    capture_fixtures()
