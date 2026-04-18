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

        try:
            with manifest_path.open() as f:
                return cast(dict[str, Any], json.load(f))
        except json.JSONDecodeError as e:
            raise ValueError(f"manifest.json at {manifest_path} contains invalid JSON: {e}") from e

    def _process_test_node(self, node: dict[str, Any], model_tests: dict[str, list[DbtTest]]) -> None:
        """Process a test node from the manifest.

        Args:
            node: The test node from the manifest.
            model_tests: Dictionary mapping model IDs to their tests.
        """
        test_metadata = node.get("test_metadata")
        if not test_metadata:
            return

        # Get the model this test is attached to
        attached_node = node.get("attached_node")
        if not attached_node:
            return

        if attached_node not in model_tests:
            model_tests[attached_node] = []

        test: DbtTest
        if "column_name" in test_metadata.get("kwargs", {}):
            test = DbtColumnTest(
                name="test." + ".".join(node["fqn"]),
                test_type=test_metadata["name"],
                namespace=test_metadata.get("namespace", None),
                column_name=test_metadata["kwargs"]["column_name"],
                kwargs=test_metadata.get("kwargs", {}),
            )
        else:
            test = DbtTableTest(
                name="test." + ".".join(node["fqn"]),
                test_type=test_metadata["name"],
                namespace=test_metadata.get("namespace", None),
                kwargs=test_metadata.get("kwargs", {}),
            )
        model_tests[attached_node].append(test)

    def _process_test_config(self, test_config: Any) -> tuple[str, str | None, dict[str, Any]]:
        """Process a test configuration into test type, namespace, and kwargs.

        Args:
            test_config: The test configuration to process.

        Returns:
            Tuple of (test_type, namespace, kwargs).

        Raises:
            ValueError: If the test configuration is invalid.
        """
        if isinstance(test_config, str):
            test_type, namespace = self._process_name(test_config)
            kwargs = {}
        elif isinstance(test_config, dict):
            # Handle case where test config is a dict with a single key-value pair
            # e.g., {"dbt_utils.unique_combination_of_columns": {"combination_of_columns": [...]}}
            if len(test_config) == 1:
                test_type, test_params = next(iter(test_config.items()))
                namespace = None
                if "." in test_type:
                    namespace, test_type = test_type.split(".", 1)
                kwargs = test_params
            else:
                # Handle case where test config has explicit fields
                test_type = test_config["name"]
                namespace = test_config.get("namespace", None)
                kwargs = test_config.get("kwargs", {})
        else:
            raise ValueError(f"Invalid test config: {test_config}")

        return test_type, namespace, kwargs

    def _process_model_node(self, node: dict[str, Any], model_tests: dict[str, list[DbtTest]]) -> DbtModel:
        """Process a model node from the manifest.

        Args:
            node: The model node from the manifest.
            model_tests: Dictionary mapping model IDs to their tests.

        Returns:
            The processed DbtModel instance.
        """
        model = DbtModel(
            name=node["name"],
            unique_id=node["unique_id"],
            database=node.get("database"),
            schema=node.get("schema"),
            tests=model_tests.get(node["unique_id"], []),
        )

        # Process data tests from meta
        node_meta = node.get("meta", {}) or {}
        dbt_gx_meta = node_meta.get("dbt_gx", {}) or {}
        data_tests = dbt_gx_meta.get("data_tests", []) or []

        for test_config in data_tests:
            test_type, namespace, kwargs = self._process_test_config(test_config)

            test = DbtTableTest(
                name=".".join(["test", model.full_name, test_type]),
                test_type=test_type,
                namespace=namespace,
                kwargs=kwargs,
            )
            model.tests.append(test)

        return model

    def _extract_models(self, manifest: dict[str, Any]) -> list[DbtModel]:
        """Extract models and their tests from manifest.

        Args:
            manifest: dbt manifest data.

        Returns:
            List of models with their tests.
        """
        models: list[DbtModel] = []
        model_tests: dict[str, list[DbtTest]] = {}

        # First pass: collect all tests
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") == "test":
                self._process_test_node(node, model_tests)

        # Second pass: create models with their tests
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type") == "model":
                model = self._process_model_node(node, model_tests)
                models.append(model)

        return models

    def _process_name(self, name: str) -> tuple[str, str | None]:
        """Process a name into a tuple of test type and namespace.

        Args:
            name: The name to process, optionally in "namespace.test_type" format.

        Returns:
            Tuple of (test_type, namespace).
        """
        if "." in name:
            namespace, test_type = name.split(".", 1)
            return test_type, namespace
        return name, None
