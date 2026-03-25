"""Configuration model for dbt-gx."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dbt_gx.models.default_test_conversions import default_conversion_factory
from dbt_gx.models.test_conversion_base import TestConversion, TestConversionParams


@dataclass
class DbtGxConfig:
    """Configuration for dbt-gx."""

    test_mappings: dict[str, TestConversion] = field(default_factory=default_conversion_factory)
    generate_docs: bool = True

    def merge_with(self, other: "DbtGxConfig") -> "DbtGxConfig":
        """Merge this configuration with another.

        Args:
            other: Other configuration to merge with.

        Returns:
            New configuration with merged test mappings.
        """
        merged_mappings = self.test_mappings.copy()
        merged_mappings.update(other.test_mappings)
        return DbtGxConfig(test_mappings=merged_mappings)


def create_default_config() -> DbtGxConfig:
    """Create a default configuration.

    Returns:
        Default configuration with sensible defaults.
    """
    return DbtGxConfig()


def load_config(config_path: Path) -> DbtGxConfig:
    """Load configuration from a YAML file.

    Args:
        config_path: Path to the configuration file.

    Returns:
        Loaded configuration.
    """
    with config_path.open() as f:
        config_dict = yaml.safe_load(f)

    generate_docs = config_dict.get("generate_docs", True)

    # Handle test mappings if present
    test_mappings = default_conversion_factory()
    if "test_mappings" in config_dict:
        for test_name, test_config in config_dict["test_mappings"].items():
            params = TestConversionParams(**test_config.get("params", {}))
            test_mappings[test_name] = TestConversion(
                expectation_class=test_config["expectation"],
                params=params,
                function=test_config.get("function"),
            )

    return DbtGxConfig(test_mappings=test_mappings, generate_docs=generate_docs)


def create_data_context_config() -> dict[str, Any]:
    """Create Great Expectations data context configuration.

    Args:
        config: dbt-gx configuration.

    Returns:
        Data context configuration.
    """
    return {
        "config_version": 3.0,
        "analytics_enabled": False,
        "stores": {
            "expectations_store": {
                "class_name": "InMemoryStoreBackend",
            },
            "validations_store": {"class_name": "InMemoryStoreBackend"},
            "validation_results_store": {"class_name": "InMemoryStoreBackend"},
            "checkpoint_store": {"class_name": "InMemoryStoreBackend"},
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "validation_results_store_name": "validation_results_store",
        "checkpoint_store_name": "checkpoint_store",
        "data_docs_sites": {},
    }
