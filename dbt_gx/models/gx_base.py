from dataclasses import dataclass, field

from great_expectations.expectations import Expectation

from dbt_gx.models.dbt_base import DbtModel


@dataclass(kw_only=True)
class GxBatch:
    """Base class for dbt test configurations."""

    model: DbtModel
    expectations: list[Expectation] = field(default_factory=list)
