# Configuration Guide

This document explains how `correlator-ge` configuration works, including the priority order and how to troubleshoot
configuration issues.

> **Note:** This is a skeleton release. Full validation action configuration will be documented after Task 2.2 research
> is complete.

## Overview

`correlator-ge` integrates with Great Expectations checkpoints to emit OpenLineage events during validation runs.

| Event Type | Description                  | Facets Emitted                         |
|------------|------------------------------|----------------------------------------|
| `START`    | Checkpoint validation begins | Run metadata                           |
| `COMPLETE` | Validation passes            | DataQualityAssertions facet            |
| `FAIL`     | Validation fails             | DataQualityAssertions facet + failures |

## Configuration Sources

`correlator-ge` accepts configuration from four sources (in priority order):

| Priority    | Source                | Example                            |
|-------------|-----------------------|------------------------------------|
| 1 (highest) | CLI arguments         | `--correlator-endpoint http://...` |
| 2           | Environment variables | `CORRELATOR_ENDPOINT=http://...`   |
| 3           | Config file           | `.correlator-ge.yml`               |
| 4 (lowest)  | Default values        | Hardcoded in code                  |

**Rule: The most specific source wins.**

## Config File Format

Create `.correlator-ge.yml` in your project root:

```yaml
# .correlator-ge.yml
correlator:
  endpoint: http://localhost:8080/api/v1/lineage/events
  namespace: great_expectations
  api_key: ${CORRELATOR_API_KEY}  # Environment variable interpolation
```

### Minimal Example

Only `correlator.endpoint` is required:

```yaml
correlator:
  endpoint: http://localhost:8080/api/v1/lineage/events
```

### Environment Variable Interpolation

Use `${VAR_NAME}` syntax to reference environment variables:

```yaml
correlator:
  api_key: ${CORRELATOR_API_KEY}
  endpoint: ${CORRELATOR_ENDPOINT}
```

**Note:** Missing environment variables are replaced with empty string.

## Config File Discovery

When no `--config` flag is provided, `correlator-ge` searches for config files in this order:

1. `.correlator-ge.yml` in current working directory
2. `.correlator-ge.yaml` in current working directory
3. `.correlator-ge.yml` in home directory (`~/.correlator-ge.yml`)
4. `.correlator-ge.yaml` in home directory

The first file found is used. `.yml` takes precedence over `.yaml`.

## Priority Order Explained

### Why This Order?

```
CLI args   → "I want THIS right now, just for this run"
    ↓
Env vars   → "Use this for my shell session / CI pipeline"
    ↓
Config file → "Project-level defaults for the team"
    ↓
Defaults   → "Sensible fallback if nothing else is set"
```

### Environment Variable Fallback Chain

For OpenLineage compatibility, some options support fallback environment variables:

```
Endpoint: CLI arg → CORRELATOR_ENDPOINT → OPENLINEAGE_URL → Config file → Error
API Key:  CLI arg → CORRELATOR_API_KEY  → OPENLINEAGE_API_KEY → Config file → None
```

This allows `correlator-ge` to work alongside other OpenLineage tools.

### Real-World Example

Your team has a shared `.correlator-ge.yml`:

```yaml
correlator:
  endpoint: http://prod-correlator:8080
  namespace: great_expectations
```

**Local development override:**

```bash
# Override just for this run
ge-correlator --correlator-endpoint http://localhost:8080
```

**CI pipeline override:**

```bash
# Set via env var for all commands in the pipeline
export CORRELATOR_ENDPOINT=http://staging-correlator:8080
```

**Using OpenLineage environment variables:**

```bash
# Works with existing OpenLineage setup
export OPENLINEAGE_URL=http://localhost:8080/api/v1/lineage/events
export OPENLINEAGE_NAMESPACE=great_expectations
```

## Configuration Options Reference

| Config File Path       | CLI Option                | Environment Variable                          | Default              |
|------------------------|---------------------------|-----------------------------------------------|----------------------|
| `correlator.endpoint`  | `--correlator-endpoint`   | `CORRELATOR_ENDPOINT` or `OPENLINEAGE_URL`    | (required)           |
| `correlator.namespace` | `--openlineage-namespace` | `OPENLINEAGE_NAMESPACE`                       | `great_expectations` |
| `correlator.api_key`   | `--correlator-api-key`    | `CORRELATOR_API_KEY` or `OPENLINEAGE_API_KEY` | `None`               |

### Option Details

**`--correlator-endpoint`** (required)
OpenLineage API endpoint URL. Works with Correlator or any OpenLineage-compatible backend.

**`--openlineage-namespace`**
Job namespace for OpenLineage events. This identifies the job, not the dataset.

**`--correlator-api-key`**
API key for authentication. Use environment variables for secrets.

## Great Expectations Integration

> **Note:** Checkpoint action configuration will be finalized after Task 2.2 research.

### Planned Checkpoint Configuration

```yaml
# great_expectations/checkpoints/my_checkpoint.yml
name: my_checkpoint
action_list:
  - name: correlator_lineage
    action:
      class_name: CorrelatorValidationAction
      module_name: ge_correlator.action
```

### Environment Variables

```bash
# Required
export CORRELATOR_ENDPOINT=http://localhost:8080/api/v1/lineage/events

# Optional
export CORRELATOR_API_KEY=your-api-key
export OPENLINEAGE_NAMESPACE=great_expectations
```

## Running Alongside Other OpenLineage Tools

If you want to try `correlator-ge` without changing your existing OpenLineage setup, you can run both tools side by
side.

### Why This Works

| Tool            | Primary Env Vars                            |
|-----------------|---------------------------------------------|
| Other OL tools  | `OPENLINEAGE_URL`, `OPENLINEAGE_API_KEY`    |
| `correlator-ge` | `CORRELATOR_ENDPOINT`, `CORRELATOR_API_KEY` |

Since these are different variable names, both tools can coexist with independent configurations.

### Side-by-Side Setup

```bash
# Your existing OpenLineage setup (unchanged)
export OPENLINEAGE_URL=http://prod-openlineage:8080/api/v1/lineage
export OPENLINEAGE_API_KEY=your-prod-key

# Add correlator-ge pointing to Correlator
export CORRELATOR_ENDPOINT=http://correlator:8080/api/v1/lineage/events
export CORRELATOR_API_KEY=your-correlator-key

# Both tools work independently
```

## How It Works Internally

Understanding the implementation helps with troubleshooting.

### Click's Priority Mechanism

The CLI uses [Click](https://click.palletsprojects.com/), which has built-in support for:

- CLI arguments (highest priority)
- Environment variables (via `envvar=` parameter)
- Default values (via `default=` parameter)

Config file support is added via Click's `default_map` mechanism.

### The Flow

```
User runs: ge-correlator --version

┌─────────────────────────────────────────────────────────────┐
│ 1. --config callback runs first (is_eager=True)             │
│    └─→ Loads .correlator-ge.yml (if exists)                 │
│    └─→ Sets ctx.default_map = {"correlator_endpoint": ...}  │
│                                                             │
│ 2. Each option is processed by Click:                       │
│    └─→ CLI arg provided?    → Use it, stop                  │
│    └─→ Env var set?         → Use it, stop                  │
│    └─→ In ctx.default_map?  → Use it, stop (config!)        │
│    └─→ Use default= value   → Fallback                      │
│                                                             │
│ 3. For endpoint/api_key, additional fallback check:         │
│    └─→ resolve_credentials() checks OPENLINEAGE_* vars      │
└─────────────────────────────────────────────────────────────┘
```

### Key Code Locations

| File        | Location                            | Purpose                              |
|-------------|-------------------------------------|--------------------------------------|
| `cli.py`    | `load_config_callback()`            | Loads config file into `default_map` |
| `cli.py`    | `resolve_credentials()`             | Handles OL env var fallbacks         |
| `cli.py`    | `@click.option(..., is_eager=True)` | Ensures config loads first           |
| `cli.py`    | `@click.option(..., envvar=...)`    | Enables env var support              |
| `config.py` | `CONFIG_TO_CLI_MAPPING`             | Maps config keys to CLI option names |
| `config.py` | `load_yaml_config()`                | File discovery and YAML parsing      |

## Troubleshooting

### Common Issues

| Symptom               | Likely Cause                  | Solution                                   |
|-----------------------|-------------------------------|--------------------------------------------|
| Config file ignored   | File not in expected location | Check cwd, use `--config` explicitly       |
| Env var not working   | Wrong variable name           | Check `CORRELATOR_ENDPOINT` (exact case)   |
| CLI doesn't override  | Typo in option name           | Check `--correlator-endpoint` spelling     |
| `${VAR}` not expanded | Env var not set               | Export the variable first                  |
| Wrong file loaded     | Multiple config files exist   | Use `--config` to specify exact file       |
| OL vars not working   | Correlator vars take priority | Unset `CORRELATOR_*` vars if using OL vars |

### Verify Configuration

To see what configuration will be used:

```bash
# Check if config file exists
ls -la .correlator-ge.yml

# Check environment variables
env | grep -E 'CORRELATOR|OPENLINEAGE'

# Run CLI to verify
ge-correlator --version
ge-correlator --help
```

## Best Practices

1. **Commit config file to repo** - Use `.correlator-ge.yml` for team defaults
2. **Use env vars for secrets** - Never commit `api_key` values, use `${CORRELATOR_API_KEY}`
3. **Use CLI for one-off overrides** - Don't modify config file for temporary changes
4. **Use `.yml` extension** - It takes precedence and is more common
5. **Prefer Correlator env vars** - Use `CORRELATOR_*` over `OPENLINEAGE_*` for clarity
