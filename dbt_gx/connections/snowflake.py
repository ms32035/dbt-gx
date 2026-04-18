from typing import Any, ClassVar

from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.snowflake_datasource import SnowflakeDatasource
from snowflake.sqlalchemy import URL

from dbt_gx.models.dbt_base import DbtModel

from . import Connection


class SnowflakeConnection(Connection):
    direct_params: ClassVar[tuple[str, ...]] = (
        "account",
        "user",
        "password",
        "database",
        "warehouse",
        "schema",
        "role",
        "authenticator",
    )

    @classmethod
    def datasource(cls, target_config: dict[str, Any], model: DbtModel) -> Datasource:
        params, _ = cls.params(target_config)
        return SnowflakeDatasource(
            name=model.full_schema,
            connection_string=URL(
                **params,
            ),
        )
