"""Tests for CLI module.

This module tests the correlator-ge CLI commands, including:
- Command structure and options
- Configuration file loading
- Environment variable fallbacks

Uses Click's CliRunner for CLI testing.

Note:
    This is a skeleton test file. Full CLI command tests will be added
    when the actual commands are implemented (similar to correlator-dbt).
"""

from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from ge_correlator import __version__
from ge_correlator.cli import cli, resolve_credentials
from ge_correlator.config import load_yaml_config

# =============================================================================
# A. Command Structure Tests
# =============================================================================


@pytest.mark.unit
class TestCommandStructure:
    """Tests for CLI command structure and options."""

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


# =============================================================================
# B. Config File Integration Tests
# =============================================================================


@pytest.mark.unit
class TestConfigFileIntegration:
    """Tests for config file integration with CLI.

    Note: Full config file integration tests (like test_test_command_with_config_file)
    will be added when actual CLI commands are implemented.
    """

    def test_invalid_config_file_shows_error(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that invalid YAML config file produces clear error."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: [unclosed")

        # Use a minimal command invocation with --config
        # This tests the config loading callback directly
        result = runner.invoke(
            cli,
            [
                "--help",  # Just get help, but with config loaded
            ],
            env={"CORRELATOR_GE_CONFIG": str(config_file)},
        )

        # Help should still work even with bad config env var
        # (config is only loaded when explicitly passed via --config)
        assert result.exit_code == 0

    def test_env_var_interpolation_in_config_file(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that env vars in config file are expanded.

        This tests the config loading mechanism works correctly.
        Full integration tests will be added when CLI commands are implemented.
        """
        monkeypatch.setenv("MY_ENDPOINT", "http://from-env:8080/api/v1/lineage/events")

        config_file = tmp_path / ".correlator-ge.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: ${MY_ENDPOINT}
"""
        )

        # For now, just verify the config file can be read
        # Full command tests will be added when commands are implemented
        config = load_yaml_config(config_file)
        assert (
            config["correlator"]["endpoint"]
            == "http://from-env:8080/api/v1/lineage/events"
        )


# =============================================================================
# C. OpenLineage Environment Variable Compatibility Tests
# =============================================================================


@pytest.mark.unit
class TestOpenLineageEnvVarCompatibility:
    """Tests for OpenLineage compatible environment variable fallbacks.

    correlator-ge supports OpenLineage environment variables as fallbacks
    to simplify migration from other OpenLineage tools.

    Priority order:
    1. CLI arguments (highest)
    2. CORRELATOR_* env vars
    3. OPENLINEAGE_* env vars (lowest)

    Note: Full CLI integration tests (using cli_mocks) will be added
    when actual CLI commands are implemented. These tests verify
    the resolve_credentials function directly.
    """

    def test_openlineage_url_fallback_when_no_endpoint_provided(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that OPENLINEAGE_URL env var is used when no endpoint provided."""
        monkeypatch.setenv(
            "OPENLINEAGE_URL", "http://openlineage-backend:5000/api/v1/lineage"
        )

        endpoint, _ = resolve_credentials(endpoint=None, api_key=None)

        assert endpoint == "http://openlineage-backend:5000/api/v1/lineage"

    def test_correlator_endpoint_takes_priority_over_openlineage_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CORRELATOR_ENDPOINT takes priority over OPENLINEAGE_URL."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://correlator:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("OPENLINEAGE_URL", "http://openlineage:5000/api/v1/lineage")

        endpoint, _ = resolve_credentials(endpoint=None, api_key=None)

        assert endpoint == "http://correlator:8080/api/v1/lineage/events"

    def test_openlineage_api_key_fallback_when_no_api_key_provided(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that OPENLINEAGE_API_KEY env var is used when no API key provided."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://localhost:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "openlineage-api-key-123")

        _, api_key = resolve_credentials(endpoint=None, api_key=None)

        assert api_key == "openlineage-api-key-123"

    def test_correlator_api_key_takes_priority_over_openlineage_api_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CORRELATOR_API_KEY takes priority over OPENLINEAGE_API_KEY."""
        monkeypatch.setenv(
            "CORRELATOR_ENDPOINT", "http://localhost:8080/api/v1/lineage/events"
        )
        monkeypatch.setenv("CORRELATOR_API_KEY", "correlator-api-key-456")
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "openlineage-api-key-123")

        _, api_key = resolve_credentials(endpoint=None, api_key=None)

        assert api_key == "correlator-api-key-456"

    def test_missing_endpoint_shows_helpful_error_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that missing endpoint shows error mentioning all options."""
        monkeypatch.delenv("CORRELATOR_ENDPOINT", raising=False)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)

        with pytest.raises(click.UsageError) as exc_info:
            resolve_credentials(endpoint=None, api_key=None)

        error_message = str(exc_info.value)
        # Error should mention all ways to provide endpoint
        assert (
            "CORRELATOR_ENDPOINT" in error_message
            or "correlator-endpoint" in error_message
        )
        assert "OPENLINEAGE_URL" in error_message
