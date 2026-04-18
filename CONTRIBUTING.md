# Contributing

## Development setup

```bash
git clone https://github.com/ms32035/dbt-gx.git
cd dbt-gx
pip install -e ".[dev]"
pre-commit install
```

## Running tests

Unit tests run without any external services:

```bash
pytest
```

Integration tests against a real Postgres database require Docker (exposed on port **5433**):

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

## Code style

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [mypy](https://mypy.readthedocs.io/) in strict mode for type checking.

```bash
ruff check .
ruff format .
mypy dbt_gx
```

All checks are also enforced via pre-commit hooks (`pre-commit run --all-files`).

## Adding a new database adapter

1. Create `dbt_gx/connections/<type>.py` with a class named `<Type>Connection` that inherits from `Connection`.
2. Implement the `datasource()` classmethod returning a GX `Datasource`.
3. Declare `direct_params`, `mapped_params`, and `query_params` as `ClassVar` attributes to control which dbt profile keys are forwarded to the connection.

## Submitting changes

1. Fork the repository and create a feature branch.
2. Ensure all pre-commit checks pass.
3. Open a pull request with a clear description of the change and why it is needed.
