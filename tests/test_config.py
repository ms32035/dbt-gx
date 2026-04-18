"""Tests for configuration loading."""

from pathlib import Path

import pytest

from dbt_gx.models.dbt_gx_config import (
    DbtGxConfig,
    create_default_config,
    load_config,
)
from dbt_gx.models.default_test_conversions import DEFAULT_TEST_CONVERSIONS


def test_create_default_config_has_default_mappings() -> None:
    config = create_default_config()
    assert "not_null" in config.test_mappings
    assert "unique" in config.test_mappings
    assert "dbt_utils.at_least_one" in config.test_mappings


def test_default_config_generate_docs_true() -> None:
    config = create_default_config()
    assert config.generate_docs is True


def test_load_config_basic(tmp_path: Path) -> None:
    config_file = tmp_path / "dbt_gx.yml"
    config_file.write_text(
        "generate_docs: false\ntest_mappings:\n  my_custom_test:\n    expectation: ExpectColumnValuesToNotBeNull\n"
    )
    config = load_config(config_file)
    assert config.generate_docs is False
    assert "my_custom_test" in config.test_mappings
    # Default mappings are preserved
    assert "not_null" in config.test_mappings


def test_load_config_invalid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "dbt_gx.yml"
    config_file.write_text("key: [\ninvalid")
    with pytest.raises(ValueError, match="invalid YAML"):
        load_config(config_file)


def test_load_config_empty_file(tmp_path: Path) -> None:
    config_file = tmp_path / "dbt_gx.yml"
    config_file.write_text("")
    with pytest.raises(ValueError, match="empty"):
        load_config(config_file)


def test_load_config_missing_expectation_field(tmp_path: Path) -> None:
    config_file = tmp_path / "dbt_gx.yml"
    config_file.write_text("test_mappings:\n  bad_test:\n    params: {}\n")
    with pytest.raises(ValueError, match="missing required field 'expectation'"):
        load_config(config_file)


def test_load_config_with_params(tmp_path: Path) -> None:
    config_file = tmp_path / "dbt_gx.yml"
    config_file.write_text(
        "test_mappings:\n"
        "  row_count_positive:\n"
        "    expectation: ExpectTableRowCountToBeBetween\n"
        "    params:\n"
        "      kwargs:\n"
        "        min_value: 1\n"
    )
    config = load_config(config_file)
    conversion = config.test_mappings["row_count_positive"]
    assert conversion.expectation_class == "ExpectTableRowCountToBeBetween"
    assert conversion.params.kwargs == {"min_value": 1}


def test_merge_with_overrides_mappings() -> None:
    base = create_default_config()
    other = DbtGxConfig(
        test_mappings={"not_null": base.test_mappings["unique"]},  # override not_null with unique's conversion
        generate_docs=False,
    )
    merged = base.merge_with(other)
    assert merged.test_mappings["not_null"].expectation_class == "ExpectColumnValuesToBeUnique"
    assert "dbt_utils.at_least_one" in merged.test_mappings


def test_default_test_conversions_use_pascal_case() -> None:
    for name, conversion in DEFAULT_TEST_CONVERSIONS.items():
        assert conversion.expectation_class[0].isupper(), (
            f"Conversion '{name}' has non-PascalCase expectation_class: {conversion.expectation_class}"
        )
