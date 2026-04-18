"""Tests for TestRunner.

Unit tests mock GX internals and run without any external services.
Integration tests (marked `integration`) require the Docker Postgres container
described in docker-compose.yaml to be running.
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from dbt_gx.models.dbt_base import DbtColumnTest, DbtModel, DbtProject
from dbt_gx.models.dbt_gx_config import DbtGxConfig
from dbt_gx.models.dbt_gx_runtime_env import DbtGxRuntimeEnv
from dbt_gx.models.dbt_profile import DbtProfileConfig
from dbt_gx.models.run_result import RunResult
from dbt_gx.runner import TestRunner
from tests.helpers import TestProfileConfig

_PG_DSN = "host=localhost port=5433 dbname=postgres user=postgres password=dbt-gx123"


def _postgres_available() -> bool:
    try:
        conn = psycopg.connect(_PG_DSN, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _postgres_available(), reason="Postgres not available (start docker compose)"
)


# ---------------------------------------------------------------------------
# Unit tests (no external services)
# ---------------------------------------------------------------------------


def test_run_result_run_stats() -> None:
    result = RunResult(
        run={"run_name": "test"},
        results=[
            {
                "success": True,
                "results": [
                    {"success": True},
                    {"success": True},
                ],
            },
            {
                "success": False,
                "results": [
                    {"success": False},
                ],
            },
        ],
        end_time=datetime.now(tz=UTC),
    )
    stats = result.run_stats()
    assert stats["suites_total"] == 2
    assert stats["suites_success"] == 1
    assert stats["expectations_total"] == 3
    assert stats["expectations_success"] == 2


@patch("dbt_gx.runner.get_context")
@patch("dbt_gx.runner.Checkpoint")
@patch.object(TestRunner, "add_model")
def test_runner_skips_models_without_tests(
    mock_add_model: MagicMock,
    mock_checkpoint_cls: MagicMock,
    mock_get_context: MagicMock,
    runtime_env: DbtGxRuntimeEnv,
) -> None:
    runner = TestRunner(runtime_env=runtime_env)
    empty_model = DbtModel(name="empty", unique_id="model.proj.empty", database=None, schema=None, tests=[])
    runner.add_project(DbtProject(models=[empty_model]))
    mock_add_model.assert_not_called()


@patch("dbt_gx.runner.get_context")
@patch("dbt_gx.runner.Checkpoint")
def test_runner_unsupported_db_type(
    mock_checkpoint_cls: MagicMock,
    mock_get_context: MagicMock,
    tmp_path: Path,
    default_gx_config: DbtGxConfig,
) -> None:
    profile_config = TestProfileConfig(target_config={"type": "bigquery"})
    env = DbtGxRuntimeEnv(
        project_dir=tmp_path,
        dbt_profile_config=profile_config,
        dbt_gx_config=default_gx_config,
    )
    model = DbtModel(
        name="orders",
        unique_id="model.proj.orders",
        database=None,
        schema=None,
        tests=[
            DbtColumnTest(name="test.proj.not_null_id", test_type="not_null", column_name="id"),
        ],
    )
    runner = TestRunner(runtime_env=env)
    with pytest.raises(ValueError, match="Could not load connection adapter"):
        runner.add_project(DbtProject(models=[model]))


# ---------------------------------------------------------------------------
# Integration tests (require Docker Postgres)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_tables() -> None:
    """Create test tables in Postgres for integration tests."""
    with psycopg.connect(_PG_DSN) as conn:
        conn.execute("DROP TABLE IF EXISTS dbt_gx_test_orders")
        conn.execute("DROP TABLE IF EXISTS dbt_gx_test_customers")
        conn.execute(
            """
            CREATE TABLE dbt_gx_test_orders (
                id INTEGER,
                amount FLOAT NOT NULL,
                status VARCHAR
            )
            """
        )
        conn.execute(
            "INSERT INTO dbt_gx_test_orders VALUES (1, 10.0, 'completed'), (2, 20.0, 'pending'), (3, 30.0, 'completed')"
        )
        # Intentional: duplicate id (for failing test)
        conn.execute(
            """
            CREATE TABLE dbt_gx_test_customers (
                id INTEGER,
                name VARCHAR,
                email VARCHAR
            )
            """
        )
        conn.execute("INSERT INTO dbt_gx_test_customers VALUES (1, 'Alice', 'alice@example.com'), (1, 'Bob', NULL)")
        conn.commit()
    yield
    with psycopg.connect(_PG_DSN) as conn:
        conn.execute("DROP TABLE IF EXISTS dbt_gx_test_orders")
        conn.execute("DROP TABLE IF EXISTS dbt_gx_test_customers")
        conn.commit()


@pytest.fixture
def postgres_profile_config() -> DbtProfileConfig:
    return DbtProfileConfig(
        profiles_dir=Path("tests/resources/dbt_profiles"),
    )


@pytest.fixture
def postgres_runtime_env(
    tmp_path: Path,
    postgres_profile_config: DbtProfileConfig,
) -> DbtGxRuntimeEnv:
    return DbtGxRuntimeEnv(
        project_dir=tmp_path,
        dbt_profile_config=postgres_profile_config,
        dbt_gx_config=DbtGxConfig(generate_docs=False),
    )


@requires_postgres
def test_runner_passing_expectations(pg_tables: None, postgres_runtime_env: DbtGxRuntimeEnv) -> None:
    model = DbtModel(
        name="dbt_gx_test_orders",
        unique_id="model.proj.dbt_gx_test_orders",
        database=None,
        schema=None,
        tests=[
            DbtColumnTest(name="t1", test_type="not_null", column_name="id"),
            DbtColumnTest(name="t2", test_type="unique", column_name="id"),
        ],
    )
    runner = TestRunner(runtime_env=postgres_runtime_env)
    runner.add_project(DbtProject(models=[model]))
    result = runner.run()

    stats = result.run_stats()
    assert stats["suites_total"] == 1
    assert stats["suites_success"] == 1
    assert stats["expectations_total"] == 2
    assert stats["expectations_success"] == 2


@requires_postgres
def test_runner_failing_expectations(pg_tables: None, postgres_runtime_env: DbtGxRuntimeEnv) -> None:
    model = DbtModel(
        name="dbt_gx_test_customers",
        unique_id="model.proj.dbt_gx_test_customers",
        database=None,
        schema=None,
        tests=[
            DbtColumnTest(name="t1", test_type="unique", column_name="id"),
        ],
    )
    runner = TestRunner(runtime_env=postgres_runtime_env)
    runner.add_project(DbtProject(models=[model]))
    result = runner.run()

    stats = result.run_stats()
    assert stats["suites_total"] == 1
    assert stats["suites_success"] == 0
    assert stats["expectations_success"] == 0
