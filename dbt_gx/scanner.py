"""Scanner for dbt projects."""

import json
from pathlib import Path
from typing import Any, cast

from dbt_gx.models.dbt_base import DbtColumnTest, DbtModel, DbtProject, DbtTableTest, DbtTest


class DbtProjectScanner:
    """Scanner for dbt projects."""

    def __init__(self, project_dir: Path) -> None:
        """Initialize scanner.

        Args:
            project_dir: Path to dbt project directory.
        """
        self.project_dir = project_dir

    def scan_models(self) -> list[DbtModel]:
        """Scan project for models and their tests.

        Returns:
            List of models with their tests.
        """
        manifest = self._load_manifest()
        return self._extract_models(manifest)

    def scan_project(self) -> DbtProject:
        """Scan project for models and tests.

        Returns:
            Project with models and tests.
        """
        manifest = self._load_manifest()
        models = self._extract_models(manifest)
        return DbtProject(models=models)

    def _load_manifest(self) -> dict[str, Any]:
        """Load manifest.json from the target directory.

        Returns:
            Manifest data as dictionary.

        Raises:
            FileNotFoundError: If manifest.json is not found.
        """
        manifest_path = self.project_dir / "target" / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"Could not find manifest.json at {manifest_path}. Please run `dbt compile` or `dbt run` first."
            )

        with manifest_path.open() as f:
            return cast(dict[str, Any], json.load(f))

    def _extract_models(self, manifest: dict[str, Any]) -> list[DbtModel]:
        """Extract models and their tests from manifest.

        Args:
            manifest: dbt manifest data.

        Returns:
            List of models with their tests.
        """
        models: list[DbtModel] = []
        model_tests: dict[str, list[DbtTest]] = {}

        # First pass: collect all tests and group by model
        for node_key, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") != "test":
                continue

            test_metadata = node.get("test_metadata")
            if not test_metadata:
                continue

            # Get the model this test is attached to
            attached_node = node.get("attached_node")
            if not attached_node:
                continue

            if attached_node not in model_tests:
                model_tests[attached_node] = []

            test: DbtTest
            if "column_name" in test_metadata:
                test = DbtColumnTest(
                    name=node_key,
                    test_type=test_metadata["name"],
                    namespace=test_metadata.get("namespace", ""),
                    column_name=test_metadata["column_name"],
                    kwargs=test_metadata.get("kwargs", {}),
                )
            else:
                test = DbtTableTest(
                    name=node_key,
                    test_type=test_metadata["name"],
                    namespace=test_metadata.get("namespace", ""),
                    kwargs=test_metadata.get("kwargs", {}),
                )
            model_tests[attached_node].append(test)

        # Second pass: create models with their tests
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") != "model":
                continue

            model = DbtModel(
                name=node["name"],
                unique_id=node["unique_id"],
                database=node.get("database"),
                schema=node.get("schema"),
                tests=model_tests.get(node["unique_id"], []),
            )
            models.append(model)

        return models
