"""Tests for Great Expectations validation action module.

This module contains tests for the validation action functions
that emit OpenLineage events to Correlator.

Test Coverage:
- on_validation_start(): Emits START event
- on_validation_success(): Emits COMPLETE event
- on_validation_failed(): Emits FAIL event

Note:
    This is a skeleton test file. Full tests will be added after
    Task 2.3 (Validation action implementation) is complete.
"""

import pytest

from ge_correlator.action import (
    on_validation_failed,
    on_validation_start,
    on_validation_success,
)

# =============================================================================
# A. on_validation_start() Tests - Skeleton
# =============================================================================


@pytest.mark.unit
class TestOnValidationStart:
    """Tests for on_validation_start() function.

    Note: These are skeleton tests. Full tests will be added
    when on_validation_start() is implemented.
    """

    def test_raises_not_implemented(self) -> None:
        """Test that on_validation_start() raises NotImplementedError.

        This test verifies the skeleton behavior. It will be replaced
        with actual tests when on_validation_start() is implemented.
        """
        with pytest.raises(NotImplementedError, match="on_validation_start"):
            on_validation_start(None, None)


# =============================================================================
# B. on_validation_success() Tests - Skeleton
# =============================================================================


@pytest.mark.unit
class TestOnValidationSuccess:
    """Tests for on_validation_success() function.

    Note: These are skeleton tests. Full tests will be added
    when on_validation_success() is implemented.
    """

    def test_raises_not_implemented(self) -> None:
        """Test that on_validation_success() raises NotImplementedError.

        This test verifies the skeleton behavior. It will be replaced
        with actual tests when on_validation_success() is implemented.
        """
        with pytest.raises(NotImplementedError, match="on_validation_success"):
            on_validation_success(None, None)


# =============================================================================
# C. on_validation_failed() Tests - Skeleton
# =============================================================================


@pytest.mark.unit
class TestOnValidationFailed:
    """Tests for on_validation_failed() function.

    Note: These are skeleton tests. Full tests will be added
    when on_validation_failed() is implemented.
    """

    def test_raises_not_implemented(self) -> None:
        """Test that on_validation_failed() raises NotImplementedError.

        This test verifies the skeleton behavior. It will be replaced
        with actual tests when on_validation_failed() is implemented.
        """
        with pytest.raises(NotImplementedError, match="on_validation_failed"):
            on_validation_failed(None, None)
