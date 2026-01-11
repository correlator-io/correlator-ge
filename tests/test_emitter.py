"""Tests for OpenLineage event emitter module.

This module contains tests for constructing OpenLineage events
and emitting them to Correlator.

Test Coverage:
- emit_events(): Send events to OpenLineage backend
- create_run_event(): Build OpenLineage RunEvent

Note:
    This is a skeleton test file. Full tests will be added after
    Task 2.3 (Validation action implementation) is complete.
"""

import pytest

from ge_correlator.emitter import create_run_event, emit_events

# =============================================================================
# A. emit_events() Tests - Skeleton
# =============================================================================


@pytest.mark.unit
class TestEmitEvents:
    """Tests for emit_events() function.

    Note: These are skeleton tests. Full tests will be added
    when emit_events() is implemented.
    """

    def test_emit_events_raises_not_implemented(self) -> None:
        """Test that emit_events() raises NotImplementedError in skeleton.

        This test verifies the skeleton behavior. It will be replaced
        with actual tests when emit_events() is implemented.
        """
        with pytest.raises(NotImplementedError, match="emit_events"):
            emit_events(endpoint="http://localhost:5000", events=[])

    def test_emit_events_with_api_key_raises_not_implemented(self) -> None:
        """Test that emit_events() with api_key raises NotImplementedError.

        This test verifies the skeleton behavior with optional api_key parameter.
        """
        with pytest.raises(NotImplementedError, match="emit_events"):
            emit_events(
                endpoint="http://localhost:5000",
                events=[],
                api_key="test-key",
            )


# =============================================================================
# B. create_run_event() Tests - Skeleton
# =============================================================================


@pytest.mark.unit
class TestCreateRunEvent:
    """Tests for create_run_event() function.

    Note: These are skeleton tests. Full tests will be added
    when create_run_event() is implemented.
    """

    def test_create_run_event_raises_not_implemented(self) -> None:
        """Test that create_run_event() raises NotImplementedError in skeleton.

        This test verifies the skeleton behavior. It will be replaced
        with actual tests when create_run_event() is implemented.
        """
        with pytest.raises(NotImplementedError, match="create_run_event"):
            create_run_event(
                event_type="START",
                job_namespace="great_expectations",
                job_name="my_checkpoint.my_suite",
                run_id="test-run-id",
            )

    def test_create_run_event_with_inputs_raises_not_implemented(self) -> None:
        """Test that create_run_event() with inputs raises NotImplementedError.

        This test verifies the skeleton behavior with optional inputs parameter.
        """
        with pytest.raises(NotImplementedError, match="create_run_event"):
            create_run_event(
                event_type="COMPLETE",
                job_namespace="great_expectations",
                job_name="my_checkpoint.my_suite",
                run_id="test-run-id",
                inputs=[{"namespace": "db", "name": "source_table"}],
            )
