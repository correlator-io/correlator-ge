# Architecture

> **Note:** This is a placeholder document. Full architecture documentation will be added after Task 2.2 (GE checkpoint research) is complete.

## Overview

`correlator-ge` is a Great Expectations plugin that emits OpenLineage events to Correlator for automated incident correlation.

## Components

### Validation Action (`action.py`)

The validation action hooks into GE checkpoint execution:

- `on_validation_start()` - Emits START event when checkpoint begins
- `on_validation_success()` - Emits COMPLETE event when validation passes
- `on_validation_failed()` - Emits FAIL event when validation fails

### Emitter (`emitter.py`)

Constructs and emits OpenLineage events to Correlator:

- `create_run_event()` - Creates OpenLineage RunEvent
- `emit_events()` - Sends events via HTTP POST

### Configuration (`config.py`)

Handles configuration loading:

- YAML config file (`.correlator-ge.yml`)
- Environment variable interpolation
- Priority: CLI args > env vars > config file > defaults

### CLI (`cli.py`)

Minimal CLI for configuration and debugging:

- `ge-correlator --version` - Show version
- `ge-correlator --help` - Show help

## Data Flow

```
GE Checkpoint Execution
        │
        ▼
┌───────────────────┐
│ Validation Action │
│   (lifecycle)     │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│  Event Emitter    │
│  (OpenLineage)    │
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   Correlator      │
│   (backend)       │
└───────────────────┘
```

## OpenLineage Event Structure

Events follow the OpenLineage v1.0 specification:

```json
{
  "eventTime": "2024-01-01T12:00:00Z",
  "eventType": "START|COMPLETE|FAIL",
  "producer": "https://github.com/correlator-io/correlator-ge/{version}",
  "schemaURL": "https://openlineage.io/spec/1-0-0/OpenLineage.json",
  "run": {
    "runId": "{checkpoint_run_id}"
  },
  "job": {
    "namespace": "great_expectations",
    "name": "{checkpoint_name}.{suite_name}"
  },
  "inputs": [...],
  "outputs": [...]
}
```

## Integration Points

- **Great Expectations**: Checkpoint validation actions
- **Correlator**: HTTP POST to `/api/v1/lineage/events`
- **OpenLineage**: Standard event format with DataQualityAssertions facets

---

*Full documentation will be added in Task 2.3 (Validation action implementation).*
