# 🔗 ge-correlator

**Connect Great Expectations validations to incident correlation**

[![PyPI version](https://img.shields.io/pypi/v/correlator-ge.svg)](https://pypi.org/project/correlator-ge/)
[![codecov](https://codecov.io/gh/correlator-io/correlator-ge/graph/badge.svg?token=lyZvWgRrNk)](https://codecov.io/gh/correlator-io/correlator-ge)
[![Python Version](https://img.shields.io/pypi/pyversions/correlator-ge.svg)](https://pypi.org/project/correlator-ge/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

---

## What It Does

Links Great Expectations validation results to data pipeline incidents:

- Connects validation failures to upstream job runs that caused data issues
- Provides navigation from data quality alerts to root cause
- Integrates with your existing OpenLineage infrastructure
- Works alongside your current GE checkpoint workflows

---

## Quick Start

### installation
```bash
pip install correlator-ge
```

### Configuration

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
            emit_on="all",  # "all", "success", or "failure"
        ),
    ],
)

# Run checkpoint - events are automatically emitted
result = checkpoint.run()
```

Your validation results are now being correlated with data lineage.

---

## How It Works

`correlator-ge` hooks into Great Expectations checkpoint execution and emits OpenLineage events:

1. **START** - Emits validation start event when checkpoint runs
2. **Validate** - GE runs your expectation suites
3. **Parse** - Extracts validation results and data quality metrics
4. **Emit** - Sends events with DataQualityAssertions facets
5. **COMPLETE/FAIL** - Emits completion event based on validation outcome

Events are emitted in a single batch for efficiency. The action uses a fire-and-forget pattern - emission errors are
logged but don't fail your checkpoint.

See [Architecture](docs/ARCHITECTURE.md) for technical details.

---

## Why It Matters

**The Problem:** When data quality checks fail, teams need to trace back through pipeline runs and lineage graphs to
find what upstream job introduced the bad data.

**What You Get:** `ge-correlator` automatically connects your validation failures to their upstream causes, making it
easier to identify which job run introduced the data quality issue.

**Key Benefits:**

- **Faster triage**: Validation failures linked to upstream job runs
- **Context in one place**: Data quality results correlated with lineage
- **Standard integration**: Uses OpenLineage DataQualityAssertions facets
- **Non-invasive setup**: Adds to existing checkpoint configuration
- **Fire-and-forget**: Emission errors don't fail your checkpoints

**Built on Standards:** Uses OpenLineage, the industry standard for data lineage. No vendor lock-in, no proprietary
formats.

---

## Versioning

This package follows [Semantic Versioning](https://semver.org/) with the following guidelines:

- **0.x.y versions** (e.g., 0.1.0, 0.2.0) indicate **initial development phase**:
    - The API is not yet stable and may change between minor versions
    - Features may be added, modified, or removed without major version changes
    - Not recommended for production-critical systems without pinned versions

- **1.0.0 and above** will indicate a **stable API** with semantic versioning guarantees:
    - MAJOR version for incompatible API changes
    - MINOR version for backwards-compatible functionality additions
    - PATCH version for backwards-compatible bug fixes

The current version is in early development stage, so expect possible API changes until the 1.0.0 release.

---

## Documentation

**For detailed usage, configuration, and development:**

- **Configuration**: [docs/CONFIGURATION.md](docs/CONFIGURATION.md) - Action options, environment variables
- **Architecture**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - Internal design, OpenLineage events
- **Development**: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) - Development setup, testing
- **Contributing**: [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) - Contribution guidelines
- **Deployment**: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) - Release process, PyPI publishing

---

## Requirements

- **Great Expectations >= 1.3.0** (required - custom actions restored in 1.3.0)
- **Python >= 3.9**

---

## Links

- **Correlator**: https://github.com/correlator-io/correlator
- **OpenLineage**: https://openlineage.io/
- **Great Expectations**: https://greatexpectations.io/
- **Issues**: https://github.com/correlator-io/correlator-ge/issues
- **Discussions**: https://github.com/correlator-io/correlator/discussions

---

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.
