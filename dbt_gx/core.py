"""Core module for running dbt tests with Great Expectations."""

from pathlib import Path
from typing import Any

from dbt_gx.models.dbt_gx_config import DbtGxConfig, create_default_config, load_config
from dbt_gx.models.dbt_profile import DbtProfileConfig
from dbt_gx.runner import TestRunner
from dbt_gx.scanner import DbtProjectScanner


class DbtGxRunner:
    """Main class for running dbt tests with Great Expectations."""

    def __init__(
        self,
        project_dir: Path,
        config: DbtGxConfig,
        profile_config: DbtProfileConfig,
    ) -> None:
        """Initialize the runner.

        Args:
            project_dir: Path to the dbt project directory.
            config: dbt-gx configuration.
            profile_config: dbt profile configuration.
        """
        self.scanner = DbtProjectScanner(project_dir=project_dir)
        self.runner = TestRunner(
            config=config,
            target_config=profile_config.load_target(),
        )

    def run(self) -> dict[str, dict[str, Any]]:
        """Run tests for all models in the project.

        Returns:
            Dictionary mapping model names to their test results.
        """
        # Scan project for models and tests
        project = self.scanner.scan_project()

        # Run tests for all models
        return self.runner.run_project(project)


__all__ = [
    "DbtGxConfig",
    "DbtGxRunner",
    "DbtProjectScanner",
    "TestRunner",
    "create_default_config",
    "load_config",
]
