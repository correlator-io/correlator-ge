# Configuration Guide

This document explains how to configure `ge-correlator` to emit OpenLineage events from Great Expectations checkpoints.

## Overview

`ge-correlator` provides `CorrelatorValidationAction` - a GE checkpoint action that emits OpenLineage events after
validation completes.

| Event Type | Description       | Facets Emitted        |
|------------|-------------------|-----------------------|
| `START`    | Validation begins | Run metadata          |
| `COMPLETE` | Validation passes | DataQualityAssertions |
| `FAIL`     | Validation fails  | DataQualityAssertions |

---

## Configuration Methods

### Python API (Recommended)

The recommended way to configure `ge-correlator`:

```python
import os
from great_expectations.checkpoint import Checkpoint
from ge_correlator import CorrelatorValidationAction

# Create checkpoint with Correlator action
checkpoint = Checkpoint(
    name="daily_validation",
    validation_definitions=[my_validation_definition],
    actions=[
        CorrelatorValidationAction(
            correlator_endpoint="http://correlator:8080/api/v1/lineage/events",
            api_key=os.environ.get("CORRELATOR_API_KEY"),
            emit_on="all",
            job_namespace="great_expectations://prod",
            timeout=30,
        ),
    ],
)

# Run checkpoint
result = checkpoint.run()
```

### Checkpoint YAML (Legacy)

For YAML-based checkpoint configuration:

```yaml
# great_expectations/checkpoints/my_checkpoint.yml
name: my_checkpoint
validation_definitions:
  - my_validation_definition
actions:
  - name: correlator_emit
    action:
      class_name: CorrelatorValidationAction
      module_name: ge_correlator.action
      correlator_endpoint: ${CORRELATOR_ENDPOINT}
      api_key: ${CORRELATOR_API_KEY}
      emit_on: all
      job_namespace: great_expectations://prod
      timeout: 30
```

Environment variables in `${VAR}` format are expanded by GE.

---

## Configuration Options

| Option                | Type  | Default                          | Description                             |
|-----------------------|-------|----------------------------------|-----------------------------------------|
| `correlator_endpoint` | `str` | (required)                       | Full URL to Correlator lineage endpoint |
| `api_key`             | `str` | `None`                           | API key for `X-API-Key` header          |
| `emit_on`             | `str` | `"all"`                          | When to emit events                     |
| `job_namespace`       | `str` | `"great_expectations://default"` | OpenLineage job namespace               |
| `timeout`             | `int` | `30`                             | HTTP request timeout in seconds         |

### correlator_endpoint (required)

The full URL to your Correlator or OpenLineage-compatible backend:

```python
# Correlator
correlator_endpoint = "http://correlator:8080/api/v1/lineage/events"

# Generic OpenLineage backend
correlator_endpoint = "http://openlineage-backend:5000/api/v1/lineage"
```

### api_key

Optional API key sent in the `X-API-Key` header:

```python
# From environment variable (recommended)
api_key = os.environ.get("CORRELATOR_API_KEY")

# Direct value (not recommended for production)
api_key = "your-secret-key"
```

### emit_on

Controls when events are emitted:

| Value       | Behavior                                        |
|-------------|-------------------------------------------------|
| `"all"`     | Emit for both successful and failed validations |
| `"success"` | Only emit when checkpoint passes                |
| `"failure"` | Only emit when checkpoint fails                 |

```python
# Alert-only mode - only emit on failures
emit_on = "failure"
```

### job_namespace

OpenLineage job namespace. Use URI format for Correlator compatibility:

```python
# Default
job_namespace = "great_expectations://default"

# Environment-specific
job_namespace = "great_expectations://prod"
```

### timeout

HTTP request timeout in seconds:

```python
# Default (30 seconds)
timeout = 30

# Longer timeout for slow networks
timeout = 60
```

---

## Environment Variables

While configuration is done via action fields, you can use environment variables in your code:

```python
import os

CorrelatorValidationAction(
    correlator_endpoint=os.environ["CORRELATOR_ENDPOINT"],
    api_key=os.environ.get("CORRELATOR_API_KEY"),  # Optional
)
```

Or in YAML using GE's variable substitution:

```yaml
actions:
  - name: correlator_emit
    action:
      class_name: CorrelatorValidationAction
      module_name: ge_correlator.action
      correlator_endpoint: ${CORRELATOR_ENDPOINT}
      api_key: ${CORRELATOR_API_KEY}
```

---

## Multiple Checkpoints

Each checkpoint can have its own configuration:

```python
# Development checkpoint - emit all events
dev_checkpoint = Checkpoint(
    name="dev_validation",
    validation_definitions=[...],
    actions=[
        CorrelatorValidationAction(
            correlator_endpoint="http://dev-correlator:8080/api/v1/lineage/events",
            emit_on="all",
        ),
    ],
)

# Production checkpoint - only emit failures
prod_checkpoint = Checkpoint(
    name="prod_validation",
    validation_definitions=[...],
    actions=[
        CorrelatorValidationAction(
            correlator_endpoint="http://prod-correlator:8080/api/v1/lineage/events",
            api_key=os.environ["CORRELATOR_API_KEY"],
            emit_on="failure",
        ),
    ],
)
```

---

## Error Handling

The action uses a **fire-and-forget** pattern:

- Emission errors are logged as warnings
- Errors don't fail your checkpoint
- The action always returns `success: True`

This ensures lineage emission doesn't block your data validation workflows.

```python
# This will complete even if Correlator is unreachable
result = checkpoint.run()
# result.success reflects validation outcome, not emission
```

To see emission errors, enable logging:

```python
import logging

logging.getLogger("ge_correlator").setLevel(logging.WARNING)
```

---

## Troubleshooting

### GE Version Check

`ge-correlator` requires GE >= 1.3.0. If you see:

```
ImportError: correlator-ge requires Great Expectations >= 1.3.0, found 0.18.0
```

Upgrade Great Expectations:

```bash
pip install 'great_expectations>=1.3.0'
```

### Connection Errors

If events aren't being emitted, check:

1. **Endpoint URL** - Ensure it's reachable from your environment
2. **API Key** - Verify the key is correct if authentication is required
3. **Logs** - Enable logging to see emission errors

```python
import logging

logging.basicConfig(level=logging.DEBUG)
```

### Verify Configuration

```python
from ge_correlator import CorrelatorValidationAction

action = CorrelatorValidationAction(
    correlator_endpoint="http://correlator:8080/api/v1/lineage/events",
)

print(f"Endpoint: {action.correlator_endpoint}")
print(f"Emit on: {action.emit_on}")
print(f"Namespace: {action.job_namespace}")
print(f"Timeout: {action.timeout}")
```

---

## Best Practices

1. **Use environment variables for secrets** - Never hardcode API keys
2. **Use Python API for new projects** - More flexibility and type safety
3. **Use `emit_on="failure"` in production** - Reduces noise for passing validations
4. **Set appropriate timeout** - Increase for slow networks
5. **Use URI-style namespaces** - Better Correlator compatibility
