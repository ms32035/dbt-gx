import json
from pathlib import Path

import click

from dbt_gx.core import DbtGxRunner, create_default_config, load_config
from dbt_gx.models.dbt_profile import DbtProfileConfig


def test_command(
    project_dir: Path,
    config: Path | None = None,
    output: Path = Path("test_results.json"),
    profile_name: str = "default",
    target: str | None = None,
    profiles_dir: Path | None = None,
) -> None:
    """Run dbt tests using Great Expectations.

    Args:
        project_dir: Path to the dbt project directory.
        config: Optional path to dbt-gx configuration file. If not provided, default configuration will be used.
        output: Path to output file for test results.
        profile_name: Name of the dbt profile to use.
        target: Target name to use from the profile.
        profiles_dir: Path to dbt profiles directory.
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

    # Initialize and run tests
    runner = DbtGxRunner(
        project_dir=project_dir,
        config=config_obj,
        profile_config=profile_config,
    )
    results = runner.run()

    # Save results
    output = project_dir / "target" / "dbt_gx" / output
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(results, f, indent=2)

    click.echo(f"Test results saved to {output}")
