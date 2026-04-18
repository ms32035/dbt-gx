"""Shared test fixtures."""

import json
from pathlib import Path
from typing import Any

import pytest

from dbt_gx.models.dbt_base import DbtColumnTest, DbtModel, DbtProject
from dbt_gx.models.dbt_gx_config import DbtGxConfig
from dbt_gx.models.dbt_gx_runtime_env import DbtGxRuntimeEnv
from tests.helpers import TestProfileConfig


@pytest.fixture
def test_profile_config() -> TestProfileConfig:
    return TestProfileConfig(target_config={"type": "postgres", "database": "test", "schema": "public"})


@pytest.fixture
def orders_model() -> DbtModel:
    return DbtModel(
        name="orders",
        unique_id="model.test_project.orders",
        database=None,
        schema=None,
        tests=[
            DbtColumnTest(
                name="test.test_project.not_null_orders_id",
                test_type="not_null",
                column_name="id",
            ),
            DbtColumnTest(
                name="test.test_project.unique_orders_id",
                test_type="unique",
                column_name="id",
            ),
        ],
    )


@pytest.fixture
def customers_model() -> DbtModel:
    return DbtModel(
        name="customers",
        unique_id="model.test_project.customers",
        database=None,
        schema=None,
        tests=[
            DbtColumnTest(
                name="test.test_project.unique_customers_id",
                test_type="unique",
                column_name="id",
            ),
        ],
    )


@pytest.fixture
def minimal_manifest_dir(tmp_path: Path) -> Path:
    """Write a minimal dbt manifest.json and return the project directory."""
    manifest: dict[str, Any] = {
        "nodes": {
            "model.test_project.orders": {
                "resource_type": "model",
                "name": "orders",
                "unique_id": "model.test_project.orders",
                "database": None,
                "schema": "main",
                "fqn": ["test_project", "orders"],
                "meta": {},
            },
            "test.test_project.not_null_orders_id": {
                "resource_type": "test",
                "name": "not_null_orders_id",
                "unique_id": "test.test_project.not_null_orders_id",
                "fqn": ["test_project", "not_null_orders_id"],
                "attached_node": "model.test_project.orders",
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "id", "model": "ref('orders')"},
                },
            },
            "test.test_project.unique_orders_id": {
                "resource_type": "test",
                "name": "unique_orders_id",
                "unique_id": "test.test_project.unique_orders_id",
                "fqn": ["test_project", "unique_orders_id"],
                "attached_node": "model.test_project.orders",
                "test_metadata": {
                    "name": "unique",
                    "kwargs": {"column_name": "id", "model": "ref('orders')"},
                },
            },
        }
    }
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path


@pytest.fixture
def default_gx_config() -> DbtGxConfig:
    return DbtGxConfig(generate_docs=False)


@pytest.fixture
def runtime_env(
    tmp_path: Path,
    test_profile_config: TestProfileConfig,
    default_gx_config: DbtGxConfig,
) -> DbtGxRuntimeEnv:
    return DbtGxRuntimeEnv(
        project_dir=tmp_path,
        dbt_profile_config=test_profile_config,
        dbt_gx_config=default_gx_config,
    )


@pytest.fixture
def passing_project(orders_model: DbtModel) -> DbtProject:
    return DbtProject(models=[orders_model])
