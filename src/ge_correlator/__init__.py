"""correlator-ge: Emit Great Expectations validation events as OpenLineage events.

This package provides a Great Expectations validation action that captures
checkpoint validation results and emits OpenLineage events for automated
incident correlation.

Key Features:
    - Capture GE validation results (pass/fail/warning)
    - Construct OpenLineage events with data quality assertions
    - Emit events to Correlator or any OpenLineage-compatible backend
    - Zero-friction integration with existing GE checkpoints

Usage:
    $ ge-correlator --version
    $ ge-correlator config

Architecture:
    - action: Checkpoint validation action
    - emitter: Construct and emit OpenLineage events
    - config: Configuration file loading utilities
    - cli: Command-line interface

For detailed documentation, see:
    https://github.com/correlator-io/correlator-ge

Note:
    This is a skeleton implementation. Full functionality will be added
    after Task 2.2 research is complete.
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
    "__version__",
    "create_run_event",
    "emit_events",
    "flatten_config",
    "load_yaml_config",
    "on_validation_failed",
    "on_validation_start",
    "on_validation_success",
]

from .action import (
    on_validation_failed,
    on_validation_start,
    on_validation_success,
)
from .config import flatten_config, load_yaml_config
from .emitter import create_run_event, emit_events
