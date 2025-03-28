"""Command-line interface for dbt-gx."""

from pathlib import Path

import click


class Context:
    """CLI context object."""

    def __init__(self, project_dir: Path, config: Path | None):
        """Initialize context with project directory and config path."""
        self.project_dir = project_dir
        self.config = config


@click.group()
@click.option(
    "--project-dir",
    default=".",
    help="dbt project directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    "--config",
    required=False,
    help="Path to dbt-gx configuration file (optional)",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
)
@click.pass_context
def cli(ctx: click.Context, project_dir: Path, config: Path | None) -> None:
    """Execute dbt tests using Great Expectations as the execution engine."""
    ctx.obj = Context(project_dir, config)


@cli.command()
@click.pass_context
def ls(ctx: click.Context) -> None:
    """List available dbt tests."""
    click.echo("Listing dbt tests...")
    try:
        from dbt_gx.command import ls_command

        ls_command(
            ctx.obj.project_dir,
            ctx.obj.config,
        )
    except Exception as e:
        click.echo(f"Error listing tests: {e!s}", err=True)
        raise click.Abort() from e


@cli.command()
@click.option(
    "--output",
    default="test_results.json",
    help="Path to output file for test results",
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--profile-name",
    required=True,
    help="dbt profile name to use",
    type=str,
    default="default",
)
@click.option(
    "--target",
    required=False,
    help="dbt target name to use (defaults to profile's default target)",
    type=str,
)
@click.option(
    "--profiles-dir",
    required=False,
    help="Path to dbt profiles directory (defaults to ~/.dbt)",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path),
)
@click.pass_context
def test(
    ctx: click.Context,
    output: Path,
    profile_name: str,
    target: str | None,
    profiles_dir: Path | None,
) -> None:
    """Run dbt tests using Great Expectations."""
    click.echo("Running dbt tests...")
    try:
        from dbt_gx.command import test_command

        test_command(
            ctx.obj.project_dir,
            ctx.obj.config,
            output,
            profile_name,
            target,
            profiles_dir,
        )
    except Exception as e:
        click.echo(f"Error running tests: {e!s}", err=True)
        raise click.Abort() from e
