"""Base test conversion models for dbt-gx."""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class TestConversionParams:
    """Parameters for test conversion."""

    column: str | None = None
    value: Any | None = None
    kwargs: dict[str, Any] = field(default_factory=dict)
    kwargs_mapping: dict[str, str] = field(default_factory=dict)

    def dict(self, exclude_none: bool = False) -> dict[str, Any]:
        """Convert to dictionary.

        Args:
            exclude_none: Whether to exclude None values from the output.

        Returns:
            Dictionary representation of the parameters.
        """
        data = asdict(self)
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data


@dataclass
class TestConversion:
    """Configuration for converting a dbt test to a Great Expectations expectation."""

    expectation_type: str
    params: TestConversionParams = field(default_factory=TestConversionParams)
    function: str | None = None
