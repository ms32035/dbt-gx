"""Core module for running dbt tests with Great Expectations."""

from typing import TYPE_CHECKING

from dbt_gx.models.dbt_gx_config import DbtGxConfig, create_default_config, load_config
from dbt_gx.models.dbt_gx_runtime_env import GbtGxRuntimeEnv
from dbt_gx.runner import TestRunner
from dbt_gx.scanner import DbtProjectScanner

if TYPE_CHECKING:
    from dbt_gx.models.run_result import RunResult


class DbtGxRunner:
    """Main class for running dbt tests with Great Expectations."""

    def __init__(
        self,
        runtime_env: GbtGxRuntimeEnv,
    ) -> None:
        """Initialize the runner.

        Args:
            runtime_env: Runtime environment configuration containing project directory,
                        dbt profile configuration, and dbt-gx configuration.
        """
        self.scanner = DbtProjectScanner(project_dir=runtime_env.project_dir)

        self.runner = TestRunner(
            runtime_env=runtime_env,
        )

    def run(self) -> "RunResult":
        """Run tests for all models in the project.

        Returns:
            Dictionary mapping model names to their test results.
        """
        # Scan project for models and tests
        project = self.scanner.scan_project()
        self.runner.add_project(project)

        # Run tests for all models
        return self.runner.run()


__all__ = [
    "DbtGxConfig",
    "DbtGxRunner",
    "DbtProjectScanner",
    "TestRunner",
    "create_default_config",
    "load_config",
]
