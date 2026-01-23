# Integration Tests

Integration tests validate the full roundtrip from Great Expectations checkpoint results through the `ge-correlator`
plugin to the Correlator backend.

## Prerequisites

1. **Correlator backend** running locally (default: `http://localhost:8080`)
2. **PostgreSQL** accessible for database verification

## Environment Variables

| Variable            | Required | Default                 | Description                                      |
|---------------------|----------|-------------------------|--------------------------------------------------|
| `CORRELATOR_URL`    | No       | `http://localhost:8080` | Correlator API endpoint                          |
| `CORRELATOR_DB_URL` | No       | -                       | PostgreSQL connection string for DB verification |
| `CLEANUP_TEST_DATA` | No       | `false`                 | Set to `true` to clean up test data after tests  |

## Running Tests

```bash
# Set environment database variable
CORRELATOR_DB_URL=postgres://user:pass@localhost:5432/correlator

# Basic run (requires Correlator running)
make run test integration

# With cleanup after tests
CLEANUP_TEST_DATA=true make run test integration

# Run specific test with verbose output
pytest tests/integration/ -m integration -k "test_name" -v -s
```

## Test Coverage

### Event Emission

- **Successful validation** emits START + COMPLETE events
- **Failed validation** emits START + FAIL events
- **Multiple validations** in a single checkpoint emit event pairs for each validation
- **Conditional emission** (`emit_on` config) correctly skips events based on validation outcome

### Fire-and-Forget Behavior

- Plugin succeeds even when Correlator is unreachable
- Emission failures don't break GE checkpoint execution

### API Contract

- Events conform to OpenLineage specification
- Correlator response format matches plugin-developer-guide spec
- Response includes status, summary, correlation_id, and timestamp

### Idempotency

- Duplicate events are handled idempotently by Correlator
- Re-running the same checkpoint doesn't create duplicate records

## Test Isolation

Each test run uses:

- **Unique namespace**: `great_expectations://integration-test-{uuid}`
- **Unique run IDs**: Prevents collision across parallel test runs
- **Prefixed run names**: `test-run-{uuid}` for readable logs

## Database Cleanup

When `CLEANUP_TEST_DATA=true`, the following tables are cleaned after tests:

1. `test_results` - validation results linked to job runs
2. `lineage_event_idempotency` - event deduplication records
3. `lineage_edges` - job run lineage connections
4. `job_runs` - the job runs themselves
5. `datasets` - dataset records from test fixtures

Cleanup uses namespace pattern matching to only remove test data.

## Fixtures

Tests use serialized `CheckpointResult` fixtures from `tests/fixtures/`:

- `checkpoint_result_success.json` - all expectations pass
- `checkpoint_result_failure.json` - multiple expectation failures
- `checkpoint_result_multiple.json` - mixed results (1 pass + 1 fail)

See [docs/GENERATE_FIXTURES.md](../../docs/GENERATE_FIXTURES.md) for fixture generation details.
