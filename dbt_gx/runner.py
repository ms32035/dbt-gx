"""Test runner module for executing Great Expectations tests."""

import importlib
from typing import Any, cast

from great_expectations import get_context
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.datasource.fluent import SQLDatasource

from dbt_gx.converter import TestConverter
from dbt_gx.models.dbt_base import DbtModel, DbtProject
from dbt_gx.models.dbt_gx_config import DbtGxConfig, create_data_context_config


class TestRunner:
    """Runs Great Expectations tests."""

    def __init__(
        self,
        config: DbtGxConfig,
        target_config: dict[str, Any],
    ):
        """Initialize the test runner.

        Args:
            config: dbt-gx configuration.
            target_config: Target configuration from dbt profile.
        """
        # Create context with config
        context_config = create_data_context_config(config)
        self.context = get_context(project_config=context_config, mode="ephemeral")
        self.config = config
        self.target_config = target_config

        # Initialize converter
        self.converter = TestConverter(config)

        # Dictionary to store datasources by database+schema
        self.datasources: dict[tuple[str, str], SQLDatasource] = {}

    def _get_datasource_key(self, model: DbtModel) -> tuple[str, str]:
        """Get the key for datasource lookup based on model's database and schema.

        Args:
            model: The dbt model.

        Returns:
            Tuple of (database, schema) to use as key.
        """
        return (
            model.database or self.target_config.get("database", ""),
            model.schema or self.target_config.get("schema", ""),
        )

    def _get_or_create_datasource(self, model: DbtModel) -> SQLDatasource:
        """Get existing datasource or create a new one for the model.

        Args:
            model: The dbt model to get/create datasource for.

        Returns:
            SQLDatasource instance for the model.
        """
        key = self._get_datasource_key(model)

        if key not in self.datasources:
            # Create new datasource config with model's database and schema
            datasource_config = self.target_config.copy()
            if model.database:
                datasource_config["database"] = model.database
            if model.schema:
                datasource_config["schema"] = model.schema

            # Create the datasource
            target_type = cast(str, datasource_config.get("type"))
            conn_module = importlib.import_module(f"dbt_gx.connections.{target_type}")
            conn_class = getattr(conn_module, target_type.capitalize() + "Connection")
            datasource = conn_class.datasource(datasource_config, model)
            self.datasources[key] = datasource
            self.context.add_datasource(datasource=datasource)

        return self.datasources[key]

    def run_model(
        self,
        model: DbtModel,
    ) -> dict[str, Any]:
        """Run tests for a single dbt model using Great Expectations.

        Args:
            model: The dbt model to run tests for.

        Returns:
            Test results from Great Expectations.
        """
        # Get appropriate datasource for this model
        datasource = self._get_or_create_datasource(model)

        # Convert model tests to expectations
        ge_batch = self.converter.convert_model(model)

        # Create a new expectation suite for this model
        suite = ExpectationSuite(
            name=f"{model.name}_suite",
            expectations=[],
            meta={
                "notes": {
                    "format": "markdown",
                    "content": [f"Suite created by dbt-gx for model {model.name}"],
                }
            },
        )
        self.context.suites.add(suite)

        # Create data asset for the model if it doesn't exist
        if model.name not in datasource.assets:
            datasource.add_table_asset(
                name=model.name,
                table_name=model.name,
            )

        # Get validator for the model
        validator = self.context.get_validator(
            datasource_name=datasource.name,
            data_asset_name=model.name,
            expectation_suite_name=suite.name,
            batch_request=None,  # Use default batch request
        )

        # Add expectations
        for expectation in ge_batch.expectations:
            validator.expect_configured(expectation)

        # Run validation
        results = validator.validate()
        return cast(dict[str, Any], results.to_json_dict())

    def run_project(
        self,
        project: DbtProject,
    ) -> dict[str, dict[str, Any]]:
        """Run tests for all models in a dbt project using Great Expectations.

        Args:
            project: The dbt project containing models to test.

        Returns:
            Dictionary mapping model names to their test results.
        """
        results = {}
        for model in project.models:
            if model.tests:  # Only run tests for models that have tests
                results[model.name] = self.run_model(model)

        return results
