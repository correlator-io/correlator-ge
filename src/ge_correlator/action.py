"""Great Expectations validation action for correlator-ge.

This module provides the CorrelatorValidationAction class that hooks into
Great Expectations checkpoint validation and emits OpenLineage events to Correlator.

The action:
    - Captures validation results from GE checkpoints
    - Builds OpenLineage START and COMPLETE/FAIL events
    - Emits events to Correlator in a single batch
    - Uses fire-and-forget pattern (errors don't fail checkpoint)

Usage:
    Configure via Python API (recommended) or checkpoint YAML.
    See docs/CONFIGURATION.md for details.

Requirements:
    - Great Expectations >= 1.3.0
"""

import logging
from datetime import datetime, timezone
from typing import Any, Literal, Optional
from uuid import NAMESPACE_URL, uuid5

import great_expectations
from great_expectations.checkpoint import ValidationAction
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    Run,
    RunEvent,
    RunState,
)

from ge_correlator import __version__
from ge_correlator.emitter import emit_events
from ge_correlator.extractors import (
    extract_data_quality_facets,
    extract_datasets,
    extract_job_name,
    extract_run_id,
    extract_run_time,
)

# GE version check - fail fast with clear error
_GE_MIN_VERSION = (1, 3, 0)
_ge_version_str = great_expectations.__version__
try:
    _ge_version = tuple(map(int, _ge_version_str.split(".")[:3]))
except ValueError:
    # Handle pre-release versions like "1.3.0rc1" by extracting numeric parts
    import re

    _version_parts = re.findall(r"\d+", _ge_version_str)[:3]
    _ge_version = tuple(map(int, _version_parts)) if _version_parts else (0, 0, 0)

if _ge_version < _GE_MIN_VERSION:
    raise ImportError(
        f"correlator-ge requires Great Expectations >= 1.3.0, "
        f"found {_ge_version_str}. "
        f"Please upgrade: pip install 'great_expectations>=1.3.0'"
    )

logger = logging.getLogger(__name__)

# Producer URL for OpenLineage events
PRODUCER = f"https://github.com/correlator-io/correlator-ge/{__version__}"


class CorrelatorValidationAction(ValidationAction):
    """Emit OpenLineage events to Correlator after GE validation.

    This action captures checkpoint validation results and emits them as
    OpenLineage events for automated incident correlation.

    Supported in Great Expectations >= 1.3.0.

    Attributes:
        correlator_endpoint: Full URL to Correlator's lineage endpoint.
        api_key: Optional API key for X-API-Key header.
        emit_on: When to emit events - "all", "success", or "failure".
        job_namespace: Namespace for OpenLineage job (default: great_expectations://default).
        timeout: HTTP request timeout in seconds.

    Example:
        Python API (recommended):
        ```python
        from ge_correlator.action import CorrelatorValidationAction

        checkpoint = Checkpoint(
            name="daily_validation",
            validation_definitions=[my_validation_definition],
            actions=[
                CorrelatorValidationAction(
                    correlator_endpoint="http://correlator:8080/api/v1/lineage/events",
                    api_key=os.environ.get("CORRELATOR_API_KEY"),
                    emit_on="all",
                ),
            ],
        )
        ```

        YAML config (legacy):
        ```yaml
        action_list:
          - name: correlator_emit
            action:
              class_name: CorrelatorValidationAction
              module_name: ge_correlator.action
              correlator_endpoint: ${CORRELATOR_ENDPOINT}
              api_key: ${CORRELATOR_API_KEY}
        ```
    """

    # Pydantic discriminator for GE action type registry
    type: Literal["correlator"] = "correlator"

    # Required by ValidationAction base class
    name: str = "correlator_action"

    # Configuration fields (Pydantic field declarations)
    correlator_endpoint: str
    api_key: Optional[str] = None
    emit_on: Literal["all", "success", "failure"] = "all"
    job_namespace: str = "great_expectations://default"
    timeout: int = 30

    def run(
        self,
        checkpoint_result: Any,
        action_context: Optional[Any] = None,  # noqa: ARG002
    ) -> dict[str, Any]:
        """Execute the action after checkpoint validation.

        Builds OpenLineage events from checkpoint results and emits them
        to Correlator. Uses fire-and-forget pattern - errors are logged
        but don't fail the checkpoint.

        Args:
            checkpoint_result: GE CheckpointResult object.
            action_context: Optional action context (unused).

        Returns:
            Dict with class name, success status, and optional message/error.
        """
        try:
            if not self._should_emit(checkpoint_result):
                logger.info(
                    f"Correlator action skipped: "
                    f"emit_on={self.emit_on}, checkpoint success={checkpoint_result.success}"
                )
                return {
                    "class": self.__class__.__name__,
                    "success": True,
                    "message": "Skipped - emit_on condition not met",
                }

            events = self._build_events(checkpoint_result)

            emit_events(
                events=events,  # type: ignore[arg-type]
                endpoint=self.correlator_endpoint,
                api_key=self.api_key,
                timeout=self.timeout,
            )

            logger.info(f"Correlator action emitted {len(events)} events")
            return {
                "class": self.__class__.__name__,
                "success": True,
                "events_emitted": len(events),
            }

        except Exception as e:
            # Fire-and-forget: log error but don't fail checkpoint
            logger.warning(f"Correlator action failed to emit events: {e}")
            return {
                "class": self.__class__.__name__,
                "success": True,  # Action "succeeds" even if emission fails
                "error": str(e),
            }

    def _should_emit(self, checkpoint_result: Any) -> bool:
        """Check if events should be emitted based on emit_on config.

        Args:
            checkpoint_result: GE CheckpointResult object.

        Returns:
            True if events should be emitted, False otherwise.
        """
        if self.emit_on == "all":
            return True
        if self.emit_on == "success":
            return checkpoint_result.success is True
        # emit_on == "failure"
        return checkpoint_result.success is False

    def _build_events(self, checkpoint_result: Any) -> list[RunEvent]:
        """Build OpenLineage events from checkpoint result.

        Creates START and COMPLETE/FAIL events for each validation in the
        checkpoint. Events are batched together for efficient emission.

        Args:
            checkpoint_result: GE CheckpointResult object.

        Returns:
            List of RunEvent objects ready for emission.
        """
        events: list[RunEvent] = []

        # Get run metadata
        run_id = extract_run_id(checkpoint_result)
        run_time = extract_run_time(checkpoint_result)

        # Process each validation result
        # GE 1.x: run_results maps validation_id -> ExpectationSuiteValidationResult directly
        for validation_id, validation_result in checkpoint_result.run_results.items():

            # Extract job name
            job_name = extract_job_name(checkpoint_result, validation_id)

            # Generate unique run_id per validation to avoid duplicate START events
            # Each validation in a checkpoint gets its own OpenLineage run
            validation_run_id = str(uuid5(NAMESPACE_URL, f"{run_id}:{validation_id}"))

            # Build Job and Run objects
            # Note: type: ignore needed because openlineage-python lacks type stubs
            job = Job(namespace=self.job_namespace, name=job_name)  # type: ignore[call-arg]
            run = Run(runId=validation_run_id)  # type: ignore[call-arg]

            # Create START event (uses run_time as event time)
            # Note: OpenLineage expects eventTime as ISO format string
            start_event = RunEvent(  # type: ignore[call-arg]
                eventType=RunState.START,
                eventTime=run_time.isoformat(),
                run=run,
                job=job,
                producer=PRODUCER,
            )
            events.append(start_event)

            # Determine event type based on validation success
            if validation_result.success:
                event_type = RunState.COMPLETE
            else:
                event_type = RunState.FAIL

            # Extract datasets and facets for COMPLETE/FAIL event
            datasets = extract_datasets(validation_result)
            facets = extract_data_quality_facets(validation_result, producer=PRODUCER)

            # Build input datasets with facets
            inputs: list[InputDataset] = []
            for dataset in datasets:
                input_dataset = InputDataset(  # type: ignore[call-arg]
                    namespace=dataset["namespace"],
                    name=dataset["name"],
                    inputFacets=facets,
                )
                inputs.append(input_dataset)

            # Create COMPLETE/FAIL event (uses current time as event time)
            end_event = RunEvent(  # type: ignore[call-arg]
                eventType=event_type,
                eventTime=datetime.now(timezone.utc).isoformat(),
                run=run,
                job=job,
                inputs=inputs if inputs else None,
                producer=PRODUCER,
            )
            events.append(end_event)

        return events
