import json
from pathlib import Path

import click

from dbt_gx.converter import TestConverter
from dbt_gx.core import DbtGxRunner, create_default_config, load_config
from dbt_gx.models.dbt_gx_runtime_env import GbtGxRuntimeEnv
from dbt_gx.models.dbt_profile import DbtProfileConfig
from dbt_gx.scanner import DbtProjectScanner


def test_command(
    project_dir: Path,
    config: Path | None = None,
    output: Path = Path("test_results.json"),
    profile_name: str = "default",
    target: str | None = None,
    profiles_dir: Path | None = None,
    run_name: str = "dbt-gx",
) -> None:
    """Run dbt tests using Great Expectations.

    Args:
        project_dir: Path to the dbt project directory.
        config: Optional path to dbt-gx configuration file. If not provided, default configuration will be used.
        output: Path to output file for test results.
        profile_name: Name of the dbt profile to use.
        target: Target name to use from the profile.
        profiles_dir: Path to dbt profiles directory.
        run_name: Name for this test run.
    """
    # Load configurations
    if config:
        click.echo(f"Loading configuration from {config}...")
        config_obj = load_config(config)
    else:
        click.echo("No configuration file provided, using default configuration...")
        config_obj = create_default_config()

    profile_config = DbtProfileConfig(
        profile_name=profile_name,
        target_name=target,
        profiles_dir=profiles_dir,
    )

    # Create runtime environment and initialize runner
    runtime_env = GbtGxRuntimeEnv(
        project_dir=project_dir,
        dbt_gx_config=config_obj,
        dbt_profile_config=profile_config,
        output=output,
        run_name=run_name,
    )

    runner = DbtGxRunner(runtime_env=runtime_env)
    results = runner.run()

    # Save results
    output = project_dir / "target" / "dbt_gx" / output
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(results, f, indent=2)

    click.echo(f"Test results saved to {output}")


def ls_command(
    project_dir: Path,
    config: Path | None = None,
) -> None:
    """Run dbt tests using Great Expectations.

    Args:
        project_dir: Path to the dbt project directory.
        config: Optional path to dbt-gx configuration file. If not provided, default configuration will be used.
    """
    # Load configurations
    if config:
        click.echo(f"Loading configuration from {config}...")
        config_obj = load_config(config)
    else:
        click.echo("No configuration file provided, using default configuration...")
        config_obj = create_default_config()

    scanner = DbtProjectScanner(project_dir=project_dir)

    project = scanner.scan_project()

    converter = TestConverter(config_obj)

    for model in project.models:
        click.echo(f"Model: {model.name}")
        for test in model.tests:
            test_conversion = converter.get_test_conversion(test.test_type, test.namespace)
            if test_conversion:
                click.echo(f"  ✓ Test: {test.name} -> {test_conversion.expectation_class}")
            else:
                click.secho(f"  ⚠️ Test: {test.name} -> No conversion found", fg="yellow")
