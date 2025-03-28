"""Base models for dbt tests."""

from dataclasses import dataclass, field
from typing import Any


@dataclass(kw_only=True)
class DbtTest:
    """Base class for dbt test configurations."""

    name: str
    namespace: str
    test_type: str
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class DbtModel:
    """Represents a dbt model."""

    name: str
    unique_id: str
    database: str | None = None
    schema: str | None = None
    tests: list[DbtTest] = field(default_factory=list)

    @property
    def full_schema(self) -> str | None:
        """Get the full schema name for the model.

        Returns:
            The full schema name, including database and schema, or None if schema is not set.
        """
        if not self.schema:
            return None
        return f"{self.database}.{self.schema}" if self.database else self.schema


@dataclass(kw_only=True)
class DbtProject:
    """Represents a dbt project."""

    models: list[DbtModel] = field(default_factory=list)


@dataclass(kw_only=True)
class DbtTableTest(DbtTest):
    """Represents a table-level dbt test configuration."""


@dataclass(kw_only=True)
class DbtColumnTest(DbtTableTest):
    """Represents a column-level dbt test configuration."""

    column_name: str
