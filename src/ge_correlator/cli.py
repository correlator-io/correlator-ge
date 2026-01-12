"""Command-line interface for correlator-ge.

This module provides the CLI entry point using Click framework.
The CLI is minimal for an action-based plugin - commands will be added
after Task 2.2 research identifies concrete use cases.

Usage:
    $ ge-correlator --version
    $ ge-correlator --help

Note:
    This is a skeleton implementation. The shared utility functions
    (load_config_callback, resolve_credentials, etc.) are kept for consistency
    with dbt-correlator and will be used by action/emitter modules.
"""

import os
from pathlib import Path
from typing import Any, Optional

import click

from . import __version__
from .config import CONFIG_TO_CLI_MAPPING, flatten_config, load_yaml_config


def load_config_callback(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Load config file and set defaults for unset options.

    This callback loads configuration from YAML file and sets up Click's
    default_map so that config file values are used as defaults. This enables
    the priority order: CLI args > env vars > config file > defaults.

    Args:
        ctx: Click context object.
        param: Click parameter (the --config option).
        value: Path to config file (or None for auto-discovery).

    Returns:
        The config file path for reference.

    Raises:
        click.BadParameter: If config file exists but is invalid YAML.
        click.BadParameter: If explicitly specified config file doesn't exist.
    """
    config_path = Path(value) if value else None

    # If explicit path provided and file doesn't exist, fail early
    if config_path is not None and not config_path.exists():
        raise click.BadParameter(
            f"Config file not found: {config_path}",
            param=param,
            param_hint="--config",
        )

    try:
        yaml_config = load_yaml_config(config_path)
    except ValueError as e:
        raise click.BadParameter(str(e), param=param, param_hint="--config") from e

    if yaml_config:
        # Flatten nested YAML to match Click option names
        flat_config = flatten_config(yaml_config)

        # Map flat config keys to Click option names using shared mapping
        default_map: dict[str, Any] = {}
        for config_key, click_key in CONFIG_TO_CLI_MAPPING.items():
            if config_key in flat_config:
                default_map[click_key] = flat_config[config_key]

        # Set default_map on context for this command
        # Merge existing default_map with config file values
        if ctx.default_map:
            merged = dict(ctx.default_map)
            merged.update(default_map)
            ctx.default_map = merged
        else:
            ctx.default_map = default_map

    return value


def get_endpoint_with_fallback() -> Optional[str]:
    """Get endpoint from env vars with dbt-ol compatible fallback.

    Priority: CORRELATOR_ENDPOINT > OPENLINEAGE_URL

    Returns:
        Endpoint URL or None if neither env var is set.
    """
    return os.environ.get("CORRELATOR_ENDPOINT") or os.environ.get("OPENLINEAGE_URL")


def get_api_key_with_fallback() -> Optional[str]:
    """Get API key from env vars with dbt-ol compatible fallback.

    Priority: CORRELATOR_API_KEY > OPENLINEAGE_API_KEY

    Returns:
        API key or None if neither env var is set.
    """
    return os.environ.get("CORRELATOR_API_KEY") or os.environ.get("OPENLINEAGE_API_KEY")


def resolve_credentials(
    endpoint: Optional[str],
    api_key: Optional[str],
) -> tuple[str, Optional[str]]:
    """Resolve endpoint and API key with dbt-ol compatible fallbacks.

    Applies environment variable fallbacks and validates that endpoint is set.
    This consolidates the credential resolution logic used by all CLI commands.

    Priority for endpoint: CLI arg > CORRELATOR_ENDPOINT > OPENLINEAGE_URL
    Priority for API key: CLI arg > CORRELATOR_API_KEY > OPENLINEAGE_API_KEY

    Args:
        endpoint: Endpoint from CLI option (maybe None).
        api_key: API key from CLI option (maybe None).

    Returns:
        Tuple of (resolved_endpoint, resolved_api_key).

    Raises:
        click.UsageError: If no endpoint is configured.

    Example:
        >>> endpoint, api_key = resolve_credentials(None, None)
        # Uses CORRELATOR_ENDPOINT or OPENLINEAGE_URL from environment
    """
    resolved_endpoint = endpoint or get_endpoint_with_fallback()

    if not resolved_endpoint:
        raise click.UsageError(
            "Missing --correlator-endpoint. "
            "Set via CLI, CORRELATOR_ENDPOINT, or OPENLINEAGE_URL env var."
        )

    resolved_api_key = api_key or get_api_key_with_fallback()

    return resolved_endpoint, resolved_api_key


@click.group()
@click.version_option(version=__version__, prog_name="ge-correlator")
def cli() -> None:
    """ge-correlator: Emit Great Expectations events as OpenLineage events.

    Automatically connects GE checkpoint validations to incident correlation
    through OpenLineage events. Works with Correlator or any OpenLineage-
    compatible backend.

    For more information:
        https://github.com/correlator-io/correlator-ge
    """
    pass


if __name__ == "__main__":
    cli()
