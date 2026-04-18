"""Test runner module for executing Great Expectations tests."""

import importlib
from datetime import datetime
from typing import cast

from great_expectations import Checkpoint, ValidationDefinition, get_context
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource

from dbt_gx.converter import TestConverter
from dbt_gx.models.dbt_base import DbtModel, DbtProject
from dbt_gx.models.dbt_gx_config import create_data_context_config
from dbt_gx.models.dbt_gx_runtime_env import DbtGxRuntimeEnv
from dbt_gx.models.run_result import RunResult


class TestRunner:
    """Runs Great Expectations tests."""

    def __init__(
        self,
        runtime_env: DbtGxRuntimeEnv,
    ):
        """Initialize the test runner.

        Args:
            runtime_env: Runtime environment configuration containing dbt-gx configuration
                       and dbt profile configuration.
        """
        # Create context with config
        context_config = create_data_context_config()
        runtime_env.dbt_profile_config.load_target()
        self.runtime_env = runtime_env
        self.context = get_context(project_config=context_config, mode="ephemeral")

        self.checkpoint = Checkpoint(
            validation_definitions=[],
            name="dbt-gx_checkpoint",
        )

        # Initialize converter
        self.converter = TestConverter(runtime_env.dbt_gx_config)

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
            model.database or self.runtime_env.dbt_profile_config.target_config.get("database", ""),
            model.schema or self.runtime_env.dbt_profile_config.target_config.get("schema", ""),
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
            datasource_config = self.runtime_env.dbt_profile_config.target_config.copy()
            if model.database:
                datasource_config["database"] = model.database
            if model.schema:
                datasource_config["schema"] = model.schema

            # Create the datasource
            target_type = cast(str, datasource_config.get("type"))
            if not target_type:
                raise ValueError("Database type ('type') is not set in the dbt profile target configuration")
            try:
                conn_module = importlib.import_module(f"dbt_gx.connections.{target_type}")
                conn_class = getattr(conn_module, target_type.capitalize() + "Connection")
            except (ImportError, AttributeError) as e:
                raise ValueError(
                    f"Could not load connection adapter for database type '{target_type}': {e}. "
                    f"Ensure a module exists at dbt_gx/connections/{target_type}.py."
                ) from e
            datasource = conn_class.datasource(datasource_config, model)
            self.datasources[key] = datasource
            self.context.add_datasource(datasource=datasource)

        return self.datasources[key]

    def add_model(
        self,
        model: DbtModel,
    ) -> None:
        """Queue a dbt model's tests to be run by Great Expectations.

        Args:
            model: The dbt model to add.
        """
        # Get appropriate datasource for this model
        datasource = self._get_or_create_datasource(model)

        # Convert model tests to expectations
        ge_batch = self.converter.convert_model(model)

        # Create a new expectation suite for this model
        suite = ExpectationSuite(
            name=model.name,
            expectations=[],
            meta={
                "notes": {
                    "format": "markdown",
                    "content": [f"Suite created by dbt-gx for model {model.name}"],
                }
            },
        )
        for exp in ge_batch.expectations:
            suite.add_expectation(exp)

        self.context.suites.add(suite)

        # Create data asset for the model if it doesn't exist
        if model.name not in datasource.assets:
            asset = datasource.add_table_asset(
                name=model.name,
                table_name=model.name,
            )
            batch_definition = asset.add_batch_definition(name="dbt-gx")
        else:
            asset = datasource.get_asset(model.name)
            batch_definition = asset.get_batch_definition("dbt-gx")

        validation_definition = ValidationDefinition(
            name=asset.name,
            data=batch_definition,
            suite=suite,
        )
        self.context.validation_definitions.add(validation_definition)
        self.checkpoint.validation_definitions.append(validation_definition)

    def add_project(
        self,
        project: DbtProject,
    ) -> None:
        """Queue all models in a dbt project whose tests should be run.

        Args:
            project: The dbt project containing models to add.
        """

        for model in project.models:
            if model.tests:  # Only run tests for models that have tests
                self.add_model(model)

    def run(self) -> RunResult:
        results = self.checkpoint.run(run_id=RunIdentifier(self.runtime_env.run_name))
        end_time = datetime.now().astimezone()
        if self.runtime_env.dbt_gx_config.generate_docs:
            self.context.add_data_docs_site(site_name="dbt_gx", site_config=self.runtime_env.site_config)  # type: ignore[arg-type]
            self.context.build_data_docs(["dbt_gx"])
        return RunResult(
            run=results.run_id.to_json_dict(),
            results=[r.to_json_dict() for r in results.run_results.values()],  # type: ignore[no-untyped-call]
            end_time=end_time,
        )
