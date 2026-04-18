"""Shared test helpers."""

from dbt_gx.models.dbt_profile import DbtProfileConfig


class TestProfileConfig(DbtProfileConfig):
    """DbtProfileConfig subclass that skips loading from profiles.yml."""

    def load_target(self) -> None:
        pass
