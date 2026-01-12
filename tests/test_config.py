"""Tests for configuration management module.

This module tests the configuration management functionality including:
- YAML config file loading and discovery
- Environment variable interpolation in YAML
- Priority order: CLI args > env vars > config file > defaults
- Edge cases and error handling

Uses pytest fixtures and tmp_path for isolated file system testing.
"""

from pathlib import Path
from typing import Any

import pytest

from ge_correlator.config import (
    _interpolate_env_vars,
    flatten_config,
    load_yaml_config,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def sample_config_content() -> str:
    """Sample YAML config file content with all fields."""
    return """\
correlator:
  endpoint: http://localhost:8080/api/v1/lineage/events
  namespace: test-namespace
  api_key: test-api-key
"""


@pytest.fixture
def partial_config_content() -> str:
    """YAML config with only some fields set."""
    return """\
correlator:
  endpoint: http://localhost:8080/api/v1/lineage/events
"""


@pytest.fixture
def config_with_env_vars() -> str:
    """YAML config with environment variable references."""
    return """\
correlator:
  endpoint: ${CORRELATOR_ENDPOINT}
  api_key: ${CORRELATOR_API_KEY}
  namespace: production
"""


@pytest.fixture
def nested_config_dict() -> dict[str, Any]:
    """Nested config dict as parsed from YAML."""
    return {
        "correlator": {
            "endpoint": "http://localhost:8080/api/v1/lineage/events",
            "namespace": "production",
            "api_key": "secret-key",
        },
    }


# =============================================================================
# A. Config File Discovery Tests
# =============================================================================


@pytest.mark.unit
class TestConfigFileDiscovery:
    """Tests for config file discovery and loading."""

    def test_load_config_from_explicit_path(
        self, tmp_path: Path, sample_config_content: str
    ):
        """Config file loaded when explicit path is provided."""
        config_file = tmp_path / "custom-config.yml"
        config_file.write_text(sample_config_content)

        config = load_yaml_config(config_file)

        assert (
            config["correlator"]["endpoint"]
            == "http://localhost:8080/api/v1/lineage/events"
        )
        assert config["correlator"]["namespace"] == "test-namespace"

    def test_load_config_from_default_location(
        self,
        tmp_path: Path,
        sample_config_content: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Config file at .correlator-ge.yml is auto-discovered in cwd."""
        config_file = tmp_path / ".correlator-ge.yml"
        config_file.write_text(sample_config_content)

        # Change to tmp_path directory
        monkeypatch.chdir(tmp_path)

        config = load_yaml_config()

        assert (
            config["correlator"]["endpoint"]
            == "http://localhost:8080/api/v1/lineage/events"
        )

    def test_load_config_from_yaml_extension(
        self,
        tmp_path: Path,
        sample_config_content: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Config file at .correlator-ge.yaml is auto-discovered in cwd."""
        config_file = tmp_path / ".correlator-ge.yaml"
        config_file.write_text(sample_config_content)

        # Change to tmp_path directory
        monkeypatch.chdir(tmp_path)

        config = load_yaml_config()

        assert (
            config["correlator"]["endpoint"]
            == "http://localhost:8080/api/v1/lineage/events"
        )

    def test_yml_takes_precedence_over_yaml(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """.yml file takes precedence over .yaml when both exist."""
        # Create both files with different content
        yml_file = tmp_path / ".correlator-ge.yml"
        yml_file.write_text("correlator:\n  endpoint: http://from-yml")

        yaml_file = tmp_path / ".correlator-ge.yaml"
        yaml_file.write_text("correlator:\n  endpoint: http://from-yaml")

        monkeypatch.chdir(tmp_path)

        config = load_yaml_config()

        # .yml should win
        assert config["correlator"]["endpoint"] == "http://from-yml"

    def test_config_file_not_found_returns_empty_dict(self, tmp_path: Path):
        """Missing config file returns empty dict without error."""
        non_existent = tmp_path / "does-not-exist.yml"

        config = load_yaml_config(non_existent)

        assert config == {}

    def test_config_file_invalid_yaml_raises_error(self, tmp_path: Path):
        """Invalid YAML produces clear error message."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: content: [unclosed")

        with pytest.raises(ValueError, match="Invalid YAML"):
            load_yaml_config(config_file)


# =============================================================================
# B. Config File Parsing Tests
# =============================================================================


@pytest.mark.unit
class TestConfigFileParsing:
    """Tests for YAML config file parsing."""

    def test_parse_yaml_config_all_fields(
        self, tmp_path: Path, sample_config_content: str
    ):
        """All fields from YAML are correctly parsed."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(sample_config_content)

        config = load_yaml_config(config_file)

        assert (
            config["correlator"]["endpoint"]
            == "http://localhost:8080/api/v1/lineage/events"
        )
        assert config["correlator"]["namespace"] == "test-namespace"
        assert config["correlator"]["api_key"] == "test-api-key"

    def test_parse_yaml_config_partial_fields(
        self, tmp_path: Path, partial_config_content: str
    ):
        """Partial config with missing sections is handled gracefully."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(partial_config_content)

        config = load_yaml_config(config_file)

        assert (
            config["correlator"]["endpoint"]
            == "http://localhost:8080/api/v1/lineage/events"
        )
        # namespace not in config
        assert "namespace" not in config["correlator"]

    def test_flatten_config_nested_structure(self, nested_config_dict: dict[str, Any]):
        """Nested YAML structure is flattened to config field names."""
        flat = flatten_config(nested_config_dict)

        assert (
            flat["correlator_endpoint"] == "http://localhost:8080/api/v1/lineage/events"
        )
        assert flat["openlineage_namespace"] == "production"
        assert flat["correlator_api_key"] == "secret-key"

    def test_env_var_interpolation_in_yaml(
        self, tmp_path: Path, config_with_env_vars: str, monkeypatch: pytest.MonkeyPatch
    ):
        """Environment variables in YAML values are expanded."""
        monkeypatch.setenv("CORRELATOR_ENDPOINT", "http://env-endpoint:8080")
        monkeypatch.setenv("CORRELATOR_API_KEY", "env-secret-key")

        config_file = tmp_path / "config.yml"
        config_file.write_text(config_with_env_vars)

        config = load_yaml_config(config_file)

        assert config["correlator"]["endpoint"] == "http://env-endpoint:8080"
        assert config["correlator"]["api_key"] == "env-secret-key"
        assert config["correlator"]["namespace"] == "production"  # No interpolation


# =============================================================================
# C. Environment Variable Interpolation Tests
# =============================================================================


@pytest.mark.unit
class TestEnvVarInterpolation:
    """Tests for environment variable interpolation in strings."""

    def test_interpolate_single_env_var(self, monkeypatch: pytest.MonkeyPatch):
        """Single ${VAR} pattern is replaced."""
        monkeypatch.setenv("MY_VAR", "my-value")

        result = _interpolate_env_vars("prefix-${MY_VAR}-suffix")

        assert result == "prefix-my-value-suffix"

    def test_interpolate_multiple_env_vars(self, monkeypatch: pytest.MonkeyPatch):
        """Multiple ${VAR} patterns are replaced."""
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "8080")

        result = _interpolate_env_vars("http://${HOST}:${PORT}/api")

        assert result == "http://localhost:8080/api"

    def test_interpolate_missing_env_var_empty_string(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Missing environment variable is replaced with empty string."""
        monkeypatch.delenv("UNDEFINED_VAR", raising=False)

        result = _interpolate_env_vars("prefix-${UNDEFINED_VAR}-suffix")

        assert result == "prefix--suffix"

    def test_interpolate_no_env_vars_unchanged(self):
        """String without ${VAR} patterns is returned unchanged."""
        result = _interpolate_env_vars("http://localhost:8080/api")

        assert result == "http://localhost:8080/api"

    def test_interpolate_whole_value_is_env_var(self, monkeypatch: pytest.MonkeyPatch):
        """Value that is entirely ${VAR} is fully replaced."""
        monkeypatch.setenv("FULL_VALUE", "complete-replacement")

        result = _interpolate_env_vars("${FULL_VALUE}")

        assert result == "complete-replacement"


# =============================================================================
# D. Flatten Config Tests
# =============================================================================


@pytest.mark.unit
class TestFlattenConfig:
    """Tests for flattening nested config to flat dict."""

    def test_flatten_empty_config(self):
        """Empty config returns empty dict."""
        result = flatten_config({})

        assert result == {}

    def test_flatten_partial_config(self):
        """Config with only some sections is partially flattened."""
        nested = {
            "correlator": {
                "endpoint": "http://localhost:8080",
            }
        }

        result = flatten_config(nested)

        assert result == {"correlator_endpoint": "http://localhost:8080"}

    def test_flatten_maps_namespace_to_openlineage_namespace(self):
        """correlator.namespace maps to openlineage_namespace field."""
        nested = {
            "correlator": {
                "namespace": "production",
            }
        }

        result = flatten_config(nested)

        assert result["openlineage_namespace"] == "production"

    def test_flatten_ignores_unknown_sections(self):
        """Unknown top-level sections are ignored."""
        nested = {
            "correlator": {"endpoint": "http://localhost"},
            "unknown_section": {"foo": "bar"},
        }

        result = flatten_config(nested)

        assert "unknown_section" not in result
        assert "foo" not in result


# =============================================================================
# E. Edge Cases
# =============================================================================


@pytest.mark.unit
class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_config_file(self, tmp_path: Path):
        """Empty config file returns empty dict."""
        config_file = tmp_path / "empty.yml"
        config_file.write_text("")

        config = load_yaml_config(config_file)

        assert config == {} or config is None

    def test_config_file_with_only_comments(self, tmp_path: Path):
        """Config file with only comments returns empty dict."""
        config_file = tmp_path / "comments.yml"
        config_file.write_text(
            """\
# This is a comment
# Another comment
"""
        )

        config = load_yaml_config(config_file)

        assert config == {} or config is None

    def test_unknown_fields_ignored(self, tmp_path: Path):
        """Unknown fields in config file don't cause errors."""
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            """\
correlator:
  endpoint: http://localhost:8080
  unknown_field: should_be_ignored
unknown_section:
  another_unknown: value
"""
        )

        config = load_yaml_config(config_file)
        flat = flatten_config(config)

        assert flat["correlator_endpoint"] == "http://localhost:8080"
        assert "unknown_field" not in flat
        assert "unknown_section" not in flat

    def test_config_file_yaml_extension_variations(self, tmp_path: Path):
        """Both .yml and .yaml extensions are valid."""
        config_yml = tmp_path / "config.yml"
        config_yaml = tmp_path / "config.yaml"

        content = """\
correlator:
  endpoint: http://localhost:8080
"""
        config_yml.write_text(content)
        config_yaml.write_text(content)

        # Both should load successfully
        result_yml = load_yaml_config(config_yml)
        result_yaml = load_yaml_config(config_yaml)

        assert result_yml["correlator"]["endpoint"] == "http://localhost:8080"
        assert result_yaml["correlator"]["endpoint"] == "http://localhost:8080"
