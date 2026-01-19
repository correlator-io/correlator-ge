"""correlator-ge: Great Expectations validation action for Correlator.

This package provides a Great Expectations ValidationAction that captures
checkpoint validation results and emits OpenLineage events for automated
incident correlation.

Key Features:
    - Capture GE validation results (pass/fail)
    - Construct OpenLineage events with data quality assertions
    - Emit events to Correlator or any OpenLineage-compatible backend
    - Zero-friction integration with existing GE checkpoints

Usage:
    ```python
    from ge_correlator import CorrelatorValidationAction

    checkpoint = Checkpoint(
        name="daily_validation",
        validation_definitions=[my_validation_definition],
        actions=[
            CorrelatorValidationAction(
                correlator_endpoint="http://correlator:8080/api/v1/lineage/events",
                api_key=os.environ.get("CORRELATOR_API_KEY"),
            ),
        ],
    )
    ```

Requirements:
    - Great Expectations >= 1.3.0

For detailed documentation, see:
    https://github.com/correlator-io/correlator-ge
"""

from importlib.metadata import PackageNotFoundError, version

__version__: str

try:
    __version__ = version("correlator-ge")
except PackageNotFoundError:
    # Package not installed (development mode without editable install)
    __version__ = "0.0.0+dev"


__author__ = "Emmanuel King Kasulani"
__email__ = "kasulani@gmail.com"
__license__ = "Apache-2.0"


# Public API exports
__all__ = [
    "CorrelatorValidationAction",
    "__version__",
    "emit_events",
]

from .action import CorrelatorValidationAction
from .emitter import emit_events
