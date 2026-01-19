"""Command-line interface for correlator-ge.

This module provides a minimal CLI entry point using Click framework.
The primary interface is the CorrelatorValidationAction class configured
via Python API or GE checkpoint YAML - not CLI commands.

Usage:
    $ ge-correlator --version
    $ ge-correlator --help
"""

import click

from . import __version__


@click.group()
@click.version_option(version=__version__, prog_name="ge-correlator")
def cli() -> None:
    """ge-correlator: Emit Great Expectations events as OpenLineage events.

    This package provides CorrelatorValidationAction - a GE checkpoint action
    that emits OpenLineage events to Correlator for incident correlation.

    Configuration is done via Python API (recommended) or checkpoint YAML.
    See https://github.com/correlator-io/correlator-ge for documentation.
    """
    pass


if __name__ == "__main__":
    cli()
