# dbt-gx

A tool to execute dbt tests using Great Expectations as the execution engine.

## Features

- Execute dbt tests using Great Expectations
- Support for standard dbt tests and popular packages (dbt_utils, dbt_expectations)
- Built-in default test conversions for common dbt tests
- Configurable test conversion between dbt and Great Expectations
- Support for both table and column level tests
- Custom test conversion functions
- Integration with dbt project configuration
- Apache Airflow operator for orchestration

## Installation

```bash
# Install core package
pip install dbt-gx

# Install with Airflow support
pip install dbt-gx[airflow]
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

The package comes with built-in test conversions for common dbt tests. If you need to add or override test conversions, create a `test_conversions.yml` file:

```yaml
test_conversions:
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
dbt-gx run-tests --config dbt_gx_config.yml

# Use default test conversions and your custom ones
dbt-gx run-tests --config dbt_gx_config.yml --test-conversion-config test_conversions.yml
```

### Airflow Integration

The package provides an Airflow operator for running dbt tests with Great Expectations. Here's an example DAG:

```python
from datetime import datetime, timedelta
from airflow import DAG
from dbt_gx.operators.dbt_gx_operator import DbtGxOperator

with DAG(
    "dbt_gx_example",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
) as dag:
    run_dbt_tests = DbtGxOperator(
        task_id="run_dbt_tests",
        project_dir="/path/to/dbt/project",
        config_path="/path/to/dbt_gx_config.yml",
        test_conversion_config_path="/path/to/custom_test_conversions.yml",  # Optional
        profiles_dir="~/.dbt",
        target="dev",
        vars={"env": "dev"},
        output_path="/path/to/output/test_results.json",
        fail_on_error=True,
    )
```

The operator supports the following parameters:
- `project_dir`: dbt project directory
- `config_path`: Path to dbt-gx configuration file
- `test_conversion_config_path`: Optional path to additional test conversion configuration file
- `profiles_dir`: Optional dbt profiles directory
- `target`: Optional dbt target name
- `vars`: Optional dictionary of variables to pass to dbt
- `output_path`: Optional path to output file for test results
- `fail_on_error`: Whether to fail the task if any test fails (default: True)

All paths in the operator can use Airflow templating.

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

## Development

1. Clone the repository
2. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
3. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

### Running tests

```
cd tests/resources/dbt_projects/jaffle-shop
dbt deps
dbt seed --profiles-dir ../../dbt_profiles
dbt run --profiles-dir ../../dbt_profiles

```
