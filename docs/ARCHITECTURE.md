# Architecture

## Overview

`ge-correlator` is a Great Expectations plugin that emits OpenLineage events to Correlator for automated incident
correlation. It provides a custom `ValidationAction` that hooks into GE checkpoint execution.

## Data Flow

```
GE Checkpoint.run()
        │
        ▼
┌───────────────────────────────┐
│  CorrelatorValidationAction   │
│  └─ run(checkpoint_result)    │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│  _build_events()              │
│  ├─ extract_run_id()          │
│  ├─ extract_run_time()        │
│  ├─ extract_job_name()        │
│  ├─ extract_datasets()        │
│  └─ extract_data_quality_*()  │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│  emit_events()                │
│  └─ HTTP POST (batch)         │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│  Correlator Backend           │
│  /api/v1/lineage/events       │
└───────────────────────────────┘
```

## Components

### CorrelatorValidationAction (`action.py`)

The core component - a GE `ValidationAction` subclass that emits OpenLineage events after checkpoint validation:

```python
from great_expectations.checkpoint import ValidationAction


class CorrelatorValidationAction(ValidationAction):
    type: Literal["correlator"] = "correlator"

    # Pydantic config fields
    correlator_endpoint: str
    api_key: Optional[str] = None
    emit_on: Literal["all", "success", "failure"] = "all"
    job_namespace: str = "great_expectations://default"
    timeout: int = 30

    def run(self, checkpoint_result, action_context=None) -> dict:
        """Execute after checkpoint validation completes."""
```

**Key methods:**

| Method            | Purpose                                            |
|-------------------|----------------------------------------------------|
| `run()`           | Main entry point - builds and emits events         |
| `_should_emit()`  | Checks `emit_on` config against checkpoint success |
| `_build_events()` | Creates START and COMPLETE/FAIL RunEvent objects   |

**Design decisions:**

- **Direct `ValidationAction` subclass** - Full control, no coupling to OpenLineage's GE integration
- **Pydantic config fields** - Native GE configuration pattern
- **Fire-and-forget** - Errors logged but don't fail checkpoint

### Emitter (`emitter.py`)

Handles HTTP communication with Correlator:

```python
def emit_events(
        events: list[Event],
        endpoint: str,
        api_key: Optional[str] = None,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
) -> None:
```

**Features:**

- Single batch POST for efficiency
- Handles 200/204/207/4xx/5xx responses
- Dependency injection for `requests.Session` (testability)
- Detailed error logging with response bodies

### Extractors (`extractors.py`)

Extracts metadata from GE objects for OpenLineage event construction:

| Function                        | Purpose                                          |
|---------------------------------|--------------------------------------------------|
| `extract_job_name()`            | Returns `{checkpoint_name}.{suite_name}`         |
| `extract_run_id()`              | Generates deterministic UUID from GE run_name    |
| `extract_run_time()`            | Extracts `run_id.run_time` for START event       |
| `extract_datasets()`            | Extracts datasource/asset from batch_spec        |
| `extract_data_quality_facets()` | Maps GE expectations to OL DataQualityAssertions |

### CLI (`cli.py`)

Minimal CLI for version and help:

```bash
ge-correlator --version
ge-correlator --help
```

The primary interface is `CorrelatorValidationAction` configured via Python API or checkpoint YAML - not CLI commands.

## Event Lifecycle

For each validation in a checkpoint, two events are emitted:

### 1. START Event

```json
{
  "eventType": "START",
  "eventTime": "2024-01-15T10:30:00Z",
  // run_id.run_time
  "producer": "https://github.com/correlator-io/correlator-ge/0.0.1",
  "run": {
    "runId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  },
  "job": {
    "namespace": "great_expectations://default",
    "name": "daily_validation.users_suite"
  }
}
```

### 2. COMPLETE/FAIL Event

```json
{
  "eventType": "COMPLETE",
  // or "FAIL"
  "eventTime": "2024-01-15T10:30:05Z",
  // current time
  "producer": "https://github.com/correlator-io/correlator-ge/0.0.1",
  "run": {
    "runId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  },
  "job": {
    "namespace": "great_expectations://default",
    "name": "daily_validation.users_suite"
  },
  "inputs": [
    {
      "namespace": "postgres_prod",
      "name": "public.users",
      "inputFacets": {
        "dataQualityMetrics": {
          "_producer": "...",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
          "columnMetrics": {}
        },
        "dataQualityAssertions": {
          "_producer": "...",
          "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
          "assertions": [
            {
              "assertion": "expect_column_values_to_not_be_null",
              "success": true,
              "column": "user_id"
            },
            {
              "assertion": "expect_column_values_to_be_unique",
              "success": false,
              "column": "email"
            }
          ]
        }
      }
    }
  ]
}
```

## OpenLineage Compliance

Events follow the [OpenLineage spec](https://openlineage.io/docs/spec/):

| Field           | Source                                 | Notes                                                      |
|-----------------|----------------------------------------|------------------------------------------------------------|
| `eventType`     | Validation success                     | `START`, `COMPLETE`, or `FAIL`                             |
| `eventTime`     | START: `run_id.run_time`, END: `now()` | ISO 8601 format                                            |
| `producer`      | Package URL                            | `https://github.com/correlator-io/correlator-ge/{version}` |
| `run.runId`     | Deterministic UUID from `run_name`     | Ensures correlation                                        |
| `job.namespace` | Config field                           | Default: `great_expectations://default`                    |
| `job.name`      | Extracted                              | `{checkpoint_name}.{suite_name}`                           |
| `inputs`        | Batch spec                             | `InputDataset` with data quality facets                    |

### Data Quality Facets

Two facets are included in COMPLETE/FAIL events:

1. **DataQualityMetrics** - Summary metrics (extensible)
2. **DataQualityAssertions** - Per-expectation pass/fail results

## Integration Points

### Great Expectations

- **Version**: >= 1.3.0 (custom actions restored)
- **Hook**: `ValidationAction.run()` called after checkpoint validation
- **Config**: Pydantic fields in action class

### Correlator

- **Endpoint**: `POST /api/v1/lineage/events`
- **Format**: Array of OpenLineage events
- **Auth**: `X-API-Key` header (optional)
- **Responses**: 200/204 success, 207 partial, 4xx/5xx error

### OpenLineage

- **SDK**: `openlineage-python >= 1.0.0`
- **Events**: `RunEvent` from `openlineage.client.event_v2`
- **Facets**: `DataQualityMetrics`, `DataQualityAssertions`

## Fire-and-Forget Pattern

Lineage emission follows a strict fire-and-forget pattern:

1. **Emitter** raises exceptions on any error (connection, timeout, validation)
2. **Transport** catches ALL exceptions and logs them
3. **Result**: Airflow task execution is NEVER affected by lineage failures

This design ensures observability doesn't impact reliability.

---

*Architecture last updated: January 19, 2026*