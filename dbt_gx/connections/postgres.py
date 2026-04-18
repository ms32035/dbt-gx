from typing import Any, ClassVar

from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent.postgres_datasource import PostgresDatasource
from sqlalchemy.engine.url import URL

from dbt_gx.models.dbt_base import DbtModel

from . import Connection


class PostgresConnection(Connection):
    direct_params: ClassVar[tuple[str, ...]] = ("password", "database", "port", "host")
    mapped_params: ClassVar[dict[str, str]] = {"user": "username"}

    @classmethod
    def datasource(cls, target_config: dict[str, Any], model: DbtModel) -> Datasource:
        params, query_params = cls.params(target_config)
        return PostgresDatasource(
            name=model.full_schema or model.name,
            connection_string=URL.create(
                drivername="postgresql+psycopg",
                query=query_params,
                **params,
                # hide_password=False is intentional: GX's PostgresDatasource stores this as a
                # pydantic PostgresDsn field which masks the password in __repr__ and error messages.
                # Passing hide_password=True would store "***" as the literal password, breaking auth.
            ).render_as_string(hide_password=False),
        )
