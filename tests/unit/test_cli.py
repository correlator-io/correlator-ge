"""Tests for CLI module.

This module tests the minimal correlator-ge CLI (--version and --help).
The primary interface is CorrelatorValidationAction configured via Python API
or GE checkpoint YAML - not CLI commands.
"""

import pytest
from click.testing import CliRunner

from ge_correlator import __version__
from ge_correlator.cli import cli


@pytest.mark.unit
class TestCLI:
    """Tests for CLI commands."""

    def test_cli_version_option(self, runner: CliRunner) -> None:
        """Test that --version option shows correct version."""
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert __version__ in result.output
        assert "ge-correlator" in result.output

    def test_cli_help_option(self, runner: CliRunner) -> None:
        """Test that --help option shows help text."""
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "ge-correlator" in result.output
        assert "OpenLineage" in result.output
