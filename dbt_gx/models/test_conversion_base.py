"""Base test conversion models for dbt-gx."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TestConversionParams:
    """Parameters for test conversion."""

    column: str | None = None
    # Additional parameters for the expectation
    kwargs: dict[str, Any] = field(default_factory=dict)

    # Mapping of parameter kwargs between dbt and Great Expectations
    # Key: dbt parameter name, Value: Great Expectations parameter name
    kwargs_mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class TestConversion:
    """Configuration for converting a dbt test to a Great Expectations expectation."""

    expectation_class: str
    params: TestConversionParams = field(default_factory=TestConversionParams)
    function: str | None = None
