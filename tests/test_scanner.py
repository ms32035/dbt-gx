"""Tests for DbtProjectScanner."""

import json
import re
from pathlib import Path
from typing import Any

import pytest

from dbt_gx.models.dbt_base import DbtColumnTest, DbtTableTest
from dbt_gx.scanner import DbtProjectScanner


def test_scan_project_extracts_models(minimal_manifest_dir: Path) -> None:
    scanner = DbtProjectScanner(minimal_manifest_dir)
    project = scanner.scan_project()
    assert len(project.models) == 1
    assert project.models[0].name == "orders"


def test_scan_project_extracts_column_tests(minimal_manifest_dir: Path) -> None:
    scanner = DbtProjectScanner(minimal_manifest_dir)
    project = scanner.scan_project()
    model = project.models[0]
    assert len(model.tests) == 2
    test_types = {t.test_type for t in model.tests}
    assert test_types == {"not_null", "unique"}
    column_tests = [t for t in model.tests if isinstance(t, DbtColumnTest)]
    assert all(ct.column_name == "id" for ct in column_tests)


def test_scan_project_missing_manifest(tmp_path: Path) -> None:
    scanner = DbtProjectScanner(tmp_path)
    with pytest.raises(FileNotFoundError, match=re.escape("manifest.json")):
        scanner.scan_project()


def test_scan_project_invalid_manifest_json(tmp_path: Path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text("{invalid json")
    scanner = DbtProjectScanner(tmp_path)
    with pytest.raises(ValueError, match="invalid JSON"):
        scanner.scan_project()


def test_scan_project_empty_manifest(tmp_path: Path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps({"nodes": {}}))
    scanner = DbtProjectScanner(tmp_path)
    project = scanner.scan_project()
    assert project.models == []


def test_scan_project_model_without_tests(tmp_path: Path) -> None:
    manifest: dict[str, dict[str, Any]] = {
        "nodes": {
            "model.proj.orders": {
                "resource_type": "model",
                "name": "orders",
                "unique_id": "model.proj.orders",
                "database": None,
                "schema": "main",
                "fqn": ["proj", "orders"],
                "meta": {},
            }
        }
    }
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    scanner = DbtProjectScanner(tmp_path)
    project = scanner.scan_project()
    assert len(project.models) == 1
    assert project.models[0].tests == []


def test_scan_project_table_level_test(tmp_path: Path) -> None:
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "resource_type": "model",
                "name": "orders",
                "unique_id": "model.proj.orders",
                "database": None,
                "schema": "main",
                "fqn": ["proj", "orders"],
                "meta": {},
            },
            "test.proj.at_least_one_orders_id": {
                "resource_type": "test",
                "name": "at_least_one_orders_id",
                "unique_id": "test.proj.at_least_one_orders_id",
                "fqn": ["proj", "at_least_one_orders_id"],
                "attached_node": "model.proj.orders",
                "test_metadata": {
                    "name": "at_least_one",
                    "namespace": "dbt_utils",
                    "kwargs": {"column_name": "id", "model": "ref('orders')"},
                },
            },
        }
    }
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    scanner = DbtProjectScanner(tmp_path)
    project = scanner.scan_project()
    model = project.models[0]
    assert len(model.tests) == 1
    test = model.tests[0]
    assert test.test_type == "at_least_one"
    assert test.namespace == "dbt_utils"
    assert isinstance(test, DbtColumnTest)


def test_scan_project_meta_data_tests(tmp_path: Path) -> None:
    manifest = {
        "nodes": {
            "model.proj.orders": {
                "resource_type": "model",
                "name": "orders",
                "unique_id": "model.proj.orders",
                "database": None,
                "schema": "main",
                "fqn": ["proj", "orders"],
                "meta": {
                    "dbt_gx": {
                        "data_tests": ["row_count_positive"],
                    }
                },
            }
        }
    }
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    (target_dir / "manifest.json").write_text(json.dumps(manifest))
    scanner = DbtProjectScanner(tmp_path)
    project = scanner.scan_project()
    model = project.models[0]
    assert len(model.tests) == 1
    assert model.tests[0].test_type == "row_count_positive"
    assert isinstance(model.tests[0], DbtTableTest)


def test_process_test_config_invalid(tmp_path: Path) -> None:
    scanner = DbtProjectScanner(tmp_path)
    with pytest.raises(ValueError, match="Invalid test config"):
        scanner._process_test_config(123)
