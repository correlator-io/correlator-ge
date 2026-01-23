# Test Fixtures

JSON fixtures generated from Great Expectations 1.10.0 checkpoint runs.

## Directory Structure

```
tests/fixtures/
├── scripts/                        # Generator scripts (committed)
│   ├── generate_sample_data.py     # Creates taxi_clean.csv and taxi_dirty.csv
│   ├── setup_ge.py                 # Configures ephemeral GE context
│   └── capture_fixtures.py         # Runs checkpoints, serializes to JSON
├── ge_sample_project/              # Runtime artifacts (gitignored)
│   └── data/                       # Generated CSV files
├── checkpoint_result_success.json  # All expectations pass
├── checkpoint_result_failure.json  # Multiple expectation failures
├── checkpoint_result_multiple.json # 2 validations: 1 pass + 1 fail
└── README.md                       # This file
```

## Regenerating Fixtures

```bash
cd tests/fixtures/scripts

# 1. Generate sample data
python generate_sample_data.py

# 2. Capture fixtures
python capture_fixtures.py
```

## More Information

See [docs/GENERATE_FIXTURES.md](../../docs/GENERATE_FIXTURES.md) for:

- Fixture JSON structure and design rationale
- Sample data specifications (taxi_clean.csv vs taxi_dirty.csv)
- Fixture helper utilities (`tests/helpers/fixture_helpers.py`)
- Troubleshooting guide