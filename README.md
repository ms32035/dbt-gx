# dbt-gx

[![PyPI](https://img.shields.io/pypi/v/dbt-gx)](https://pypi.org/project/dbt-gx/)
[![Python](https://img.shields.io/pypi/pyversions/dbt-gx)](https://pypi.org/project/dbt-gx/)
[![CI](https://github.com/ms32035/dbt-gx/actions/workflows/ci.yml/badge.svg)](https://github.com/ms32035/dbt-gx/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Run your dbt tests powered by Great Expectations** — richer assertions, structured JSON results, and auto-generated data docs, without changing a single line of your dbt project.

dbt-gx bridges [dbt](https://www.getdbt.com/) and [Great Expectations](https://greatexpectations.io/). It reads your compiled dbt manifest, converts your existing tests into GX expectations, executes them against your warehouse, and writes machine-readable results you can feed into dashboards, alerts, or CI gates.

### Why dbt-gx?

- **Zero setup overhead** — no separate GX project, no config files to maintain; point it at your dbt project and go
- **Drop-in compatibility** — `not_null`, `unique`, and popular `dbt_utils` tests convert automatically with no changes to your models
- **Structured output** — JSON results per suite and per expectation, ready for downstream processing or CI failure thresholds
- **Fully extensible** — map any GX expectation with a one-line YAML entry, or plug in a custom Python function for logic that goes beyond simple checks
- **Optional data docs** — generate a browsable HTML validation report alongside your dbt docs

## Installation

```bash
# Install core package (PostgreSQL support included)
pip install dbt-gx

# With Snowflake support
pip install "dbt-gx[snowflake]"
```

After installation, the `dbt-gx` command is available in your shell:

```bash
dbt-gx --help
```

## Usage

### CLI Usage

1. Add configuration to your dbt project:

Create a `dbt_gx_config.yml` file in your dbt project root:

```yaml
project_dir: .
profiles_dir: ~/.dbt
target: dev
vars: {}
```

2. (Optional) Create a custom test conversion configuration file:

The package comes with built-in test conversions for common dbt tests. If you need to add or override test conversions, create a `test_mappings.yml` file:

```yaml
test_mappings:
  # Override a default test conversion
  unique:
    expectation: expect_column_values_to_be_unique
    params:
      column: "{column_name}"
      ignore_null: true  # Add additional parameter

  # Add a custom test conversion
  custom_test:
    expectation: custom_expectation
    params:
      column: "{column_name}"
    function: path.to.custom_function
```

3. Run tests:

```bash
# Use only default test conversions
dbt-gx test --config dbt_gx_config.yml

# Use default test conversions and your custom ones
dbt-gx test --config dbt_gx_config.yml --test-conversion-config test_mappings.yml
```
## Configuration

### Built-in Test Conversions

The package includes default test conversions for:

- Standard dbt tests:
  - `unique`
  - `not_null`
  - `accepted_values`
  - `relationships`

- dbt_utils tests:
  - `dbt_utils.at_least_one`
  - `dbt_utils.equal_rowcount`
  - `dbt_utils.fewer_rows_than`
  - `dbt_utils.not_constant`

- dbt_expectations tests:
  - `dbt_expectations.expect_column_values_to_be_between`
  - `dbt_expectations.expect_column_values_to_match_regex`
  - `dbt_expectations.expect_column_values_to_be_in_type_list`

### Custom Test Conversions

You can override default test conversions or add new ones in your custom configuration file. Each test configuration can include:

- `expectation`: The Great Expectations expectation name
- `params`: Parameters to pass to the expectation
- `function`: (Optional) Path to a custom conversion function

### Custom Test Functions

You can create custom test conversion functions:

```python
def custom_function(test_config: dict, context: dict) -> dict:
    """
    Custom test conversion function.

    Args:
        test_config: The dbt test configuration
        context: Context information including column name, table name, etc.

    Returns:
        dict: Great Expectations expectation configuration
    """
    return {
        "expectation_type": "custom_expectation",
        "kwargs": {
            "column": context["column_name"],
            # Add custom parameters
        }
    }
```

## Supported databases

| Database   | Install extra      |
|------------|--------------------|
| PostgreSQL | *(included)*       |
| Snowflake  | `dbt-gx[snowflake]` |

## Development

```bash
pip install -e ".[dev]"
pre-commit install
```

### Unit tests (no external services)

```bash
pytest tests/test_scanner.py tests/test_converter.py tests/test_config.py tests/test_runner.py
```

### Integration tests (requires Docker)

Start the Postgres database (exposed on port **5433**), compile the test dbt project, then run dbt-gx against it:

```bash
docker compose up -d

cd tests/resources/dbt_projects/jaffle-shop
dbt deps
dbt seed --profiles-dir ../../dbt_profiles --target dev
dbt run  --profiles-dir ../../dbt_profiles --target dev
cd ../../../..

dbt-gx --project-dir tests/resources/dbt_projects/jaffle-shop \
  test \
  --profiles-dir tests/resources/dbt_profiles \
  --target dev
```

Results are written to `tests/resources/dbt_projects/jaffle-shop/target/dbt_gx/test_results.json`.
