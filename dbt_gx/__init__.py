"""dbt-gx - Execute dbt tests using Great Expectations as the execution engine."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dbt-gx")
except PackageNotFoundError:
    __version__ = "0.0.0"
