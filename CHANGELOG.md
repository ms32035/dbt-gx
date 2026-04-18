# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-01-01

### Added
- Initial release
- Execute dbt tests using Great Expectations as the validation engine
- Built-in conversions for `not_null`, `unique`, `dbt_utils.at_least_one`, `dbt_utils.unique_combination_of_columns`
- Support for custom test conversion functions via YAML configuration
- PostgreSQL and Snowflake database adapters
- CLI commands: `dbt-gx test` and `dbt-gx ls`
- Ephemeral Great Expectations context (no persisted GX configuration required)
- Optional data docs generation
- Apache Airflow operator support
