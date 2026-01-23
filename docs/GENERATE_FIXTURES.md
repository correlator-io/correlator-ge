# Test Fixture Generation Guide

This guide walks through generating and maintaining test fixtures for `ge-correlator` integration tests.

---

## Prerequisites

- Python 3.9+
- Great Expectations >= 1.3.0
- pandas

## Overview

Integration tests use serialized `CheckpointResult` fixtures instead of running actual GE validations.
This approach:

- **Avoids GE runtime overhead** - Tests run in milliseconds
- **Provides controlled scenarios** - Clean data (pass), dirty data (fail), mixed results
- **Enables CI/CD testing** - No GE installation required in CI runners
- **Maintains reproducibility** - Deterministic test data

---

## 1. Fixture Structure

Each fixture matches the GE 1.x `CheckpointResult` structure:

```json
{
  "_metadata": {
    "generated_at": "ISO timestamp",
    "checkpoint_name": "string",
    "ge_version": "1.x.x"
  },
  "success": true/false,
  "run_id": {
    "run_name": "string or null",
    "run_time": "ISO timestamp"
  },
  "checkpoint_config": {
    "name": "string"
  },
  "run_results": {
    "validation_key": {
      "validation_id": {
        "expectation_suite_identifier": {
          "name": "string"
        },
        "run_id": {
          "run_name": "string",
          "run_time": "ISO timestamp"
        },
        "batch_identifier": "string"
      },
      "validation_result": {
        "success": true/false,
        "results": [
          ...
        ],
        "meta": {
          "batch_spec": {
            "datasource_name": "...",
            "data_asset_name": "..."
          },
          "active_batch_definition": {
            "datasource_name": "...",
            "data_asset_name": "..."
          }
        },
        "statistics": {
          "evaluated_expectations": 0,
          "successful_expectations": 0,
          "unsuccessful_expectations": 0
        }
      }
    }
  }
}
```

---

## 2. Sample Data Design

> **Note:** This is **simplified test data**, not the official GE tutorial dataset
> (`yellow_tripdata_sample_2019-01.csv`). We use a minimal 5-column, 10-row dataset
> designed specifically to exercise our expectations with controlled pass/fail scenarios.

The generator creates minimal taxi-style data designed to exercise specific expectations.

### taxi_clean.csv (10 rows, all valid)

| Column          | Valid Values |
|-----------------|--------------|
| vendor_id       | 1 or 2       |
| passenger_count | 1-6          |
| fare_amount     | 0-500        |
| trip_distance   | >= 0         |

### taxi_dirty.csv (10 rows with violations)

| Column          | Violations                        |
|-----------------|-----------------------------------|
| vendor_id       | NULL values, value 3 (not in set) |
| passenger_count | NULL, 0, 10 (out of range)        |
| fare_amount     | Negative, > 500                   |
| trip_distance   | Negative values                   |

### Expectations Tested

| Expectation                           | Column          | Constraint |
|---------------------------------------|-----------------|------------|
| `expect_column_values_to_not_be_null` | vendor_id       | NOT NULL   |
| `expect_column_values_to_be_in_set`   | vendor_id       | IN (1, 2)  |
| `expect_column_values_to_not_be_null` | passenger_count | NOT NULL   |
| `expect_column_values_to_be_between`  | passenger_count | [1, 6]     |
| `expect_column_values_to_be_between`  | fare_amount     | [0, 500]   |
| `expect_column_values_to_be_between`  | trip_distance   | >= 0       |

---

## 3. Regenerating Fixtures

At the time of creation of this document, **GE Version 1.10.0** was used to generate the fixtures.

Run these commands from the repository root when:

- Upgrading Great Expectations version
- Modifying expectations or sample data
- Fixtures become corrupted or outdated

### Step 1: Generate Sample Data

```bash
cd tests/fixtures/scripts
python generate_sample_data.py
```

This creates:

- `ge_sample_project/data/taxi_clean.csv`
- `ge_sample_project/data/taxi_dirty.csv`

### Step 2: Capture Fixtures

```bash
python capture_fixtures.py
```

This runs GE checkpoints and serializes results to:

- `../checkpoint_result_success.json`
- `../checkpoint_result_failure.json`
- `../checkpoint_result_multiple.json`

### Verification

```bash
# Check fixture files exist
ls -la tests/fixtures/checkpoint_result_*.json

# Verify JSON structure
python -c "import json; json.load(open('tests/fixtures/checkpoint_result_success.json'))"

# Run unit tests for fixture helpers
make run test unit
```

---

## 4. Fixture Helpers

The `tests/helpers/fixture_helpers.py` module provides utilities for loading fixtures:

```python
from tests.helpers.fixture_helpers import (
    load_fixture,
    create_checkpoint_result,
    load_success_fixture,
    load_failure_fixture,
    load_multiple_fixture,
)

# Load raw JSON
data = load_fixture("checkpoint_result_success.json")

# Create CheckpointResult-like object
checkpoint = create_checkpoint_result(data)

# Convenience functions
success = load_success_fixture()
failure = load_failure_fixture()
multiple = load_multiple_fixture()
```

### Overriding batch_spec

For tests requiring specific dataset metadata:

```python
checkpoint = create_checkpoint_result(
    data,
    batch_spec_override={
        "datasource_name": "my_datasource",
        "data_asset_name": "my_table",
    },
)
```

---

## 5. Integration Test Fixtures

The `tests/integration/conftest.py` provides pytest fixtures for integration tests:

```python
@pytest.fixture
def sample_checkpoint_success(unique_run_name):
    """Checkpoint with all expectations passing."""
    fixture_data = load_fixture("checkpoint_result_success.json")
    fixture_data["run_id"]["run_name"] = unique_run_name
    return create_checkpoint_result(fixture_data)


@pytest.fixture
def sample_checkpoint_failure(unique_run_name):
    """Checkpoint with multiple failures."""
    ...


@pytest.fixture
def sample_checkpoint_multiple(unique_run_name):
    """Checkpoint with 2 validations (1 pass, 1 fail)."""
    ...
```

---

## Troubleshooting

### Problem: Fixtures fail to load

```
FileNotFoundError: Fixture not found: .../checkpoint_result_success.json
```

**Solution:** Regenerate fixtures:

```bash
cd tests/fixtures/scripts
python generate_sample_data.py
python capture_fixtures.py
```

### Problem: GE version mismatch

```
ImportError: correlator-ge requires Great Expectations >= 1.3.0
```

**Solution:** Upgrade GE and regenerate:

```bash
pip install "great_expectations>=1.3.0"
cd tests/fixtures/scripts
python capture_fixtures.py
```

### Problem: Tests fail with attribute errors

```
AttributeError: 'SimpleNamespace' object has no attribute 'meta'
```

**Solution:** The fixture structure may have changed. Check `fixture_helpers.py`
matches the current fixture JSON structure.

---

## What's Committed vs Gitignored

| Path                       | Committed | Purpose           |
|----------------------------|-----------|-------------------|
| `scripts/`                 | Yes       | Generator code    |
| `checkpoint_result_*.json` | Yes       | Test fixtures     |
| `README.md`                | Yes       | Documentation     |
| `ge_sample_project/`       | No        | Runtime artifacts |

---

## References

- [Great Expectations Checkpoints](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint)
- [correlator-ge Integration Tests](../tests/integration/)
- [Plugin Developer Guide](../notes/plugin-developer-guide.md)

---

*Last updated: January 20, 2026*