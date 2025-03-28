from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import Datasource

    from dbt_gx.models.dbt_base import DbtModel


class Connection:
    direct_params: ClassVar[tuple[str, ...]] = tuple()
    mapped_params: ClassVar[dict[str, str]] = {}
    query_params: ClassVar[dict[str, str]] = {}

    @classmethod
    def params(cls, dbt_params: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
        new_params: dict[str, Any] = {}
        query_params: dict[str, str] = {}
        for key, value in dbt_params.items():
            if key in cls.mapped_params:
                new_params[cls.mapped_params[key]] = value
            elif key in cls.direct_params:
                new_params[key] = value
            elif key in cls.query_params:
                new_params[key] = value

        return new_params, query_params

    @classmethod
    def datasource(cls, target_config: dict[str, Any], model: "DbtModel") -> "Datasource":
        raise NotImplementedError
