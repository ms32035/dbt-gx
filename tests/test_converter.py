"""Tests for TestConverter."""

import pytest
from great_expectations import expectations

from dbt_gx.converter import TestConverter
from dbt_gx.models.dbt_base import DbtColumnTest, DbtModel, DbtTableTest
from dbt_gx.models.dbt_gx_config import DbtGxConfig
from dbt_gx.models.test_conversion_base import TestConversion, TestConversionParams


@pytest.fixture
def converter(default_gx_config: DbtGxConfig) -> TestConverter:
    return TestConverter(default_gx_config)


@pytest.fixture
def simple_model() -> DbtModel:
    return DbtModel(name="orders", unique_id="model.proj.orders", database=None, schema=None)


def test_get_test_conversion_known_test(converter: TestConverter) -> None:
    conversion = converter.get_test_conversion("not_null", None)
    assert conversion is not None
    assert conversion.expectation_class == "ExpectColumnValuesToNotBeNull"


def test_get_test_conversion_namespaced(converter: TestConverter) -> None:
    conversion = converter.get_test_conversion("at_least_one", "dbt_utils")
    assert conversion is not None
    assert conversion.expectation_class == "ExpectColumnUniqueValueCountToBeBetween"


def test_get_test_conversion_unknown_returns_none(converter: TestConverter) -> None:
    assert converter.get_test_conversion("nonexistent_test", None) is None


def test_convert_test_not_null(converter: TestConverter, simple_model: DbtModel) -> None:
    test = DbtColumnTest(
        name="test.proj.not_null_orders_id",
        test_type="not_null",
        column_name="id",
    )
    result = converter.convert_test(simple_model, test)
    assert isinstance(result, expectations.ExpectColumnValuesToNotBeNull)


def test_convert_test_unique(converter: TestConverter, simple_model: DbtModel) -> None:
    test = DbtColumnTest(
        name="test.proj.unique_orders_id",
        test_type="unique",
        column_name="id",
    )
    result = converter.convert_test(simple_model, test)
    assert isinstance(result, expectations.ExpectColumnValuesToBeUnique)


def test_convert_test_unknown_returns_none(converter: TestConverter, simple_model: DbtModel) -> None:
    test = DbtColumnTest(
        name="test.proj.unknown_orders_id",
        test_type="totally_unknown",
        column_name="id",
    )
    assert converter.convert_test(simple_model, test) is None


def test_convert_test_kwargs_mapping(simple_model: DbtModel) -> None:
    config = DbtGxConfig(
        generate_docs=False,
        test_mappings={
            "unique_combination_of_columns": TestConversion(
                expectation_class="ExpectCompoundColumnsToBeUnique",
                params=TestConversionParams(kwargs_mapping={"combination_of_columns": "column_list"}),
            )
        },
    )
    converter = TestConverter(config)
    test = DbtTableTest(
        name="test.proj.unique_combo",
        test_type="unique_combination_of_columns",
        kwargs={"combination_of_columns": ["id", "status"]},
    )
    result = converter.convert_test(simple_model, test)
    assert isinstance(result, expectations.ExpectCompoundColumnsToBeUnique)


def test_convert_test_bad_custom_function(converter: TestConverter, simple_model: DbtModel) -> None:
    config = DbtGxConfig(
        generate_docs=False,
        test_mappings={
            "broken": TestConversion(
                expectation_class="ExpectColumnValuesToNotBeNull",
                function="nonexistent.module.function",
            )
        },
    )
    c = TestConverter(config)
    test = DbtColumnTest(name="test.proj.broken", test_type="broken", column_name="id")
    with pytest.raises(ValueError, match="Cannot import custom function"):
        c.convert_test(simple_model, test)


def test_convert_model_produces_gx_batch(converter: TestConverter) -> None:
    model = DbtModel(
        name="orders",
        unique_id="model.proj.orders",
        database=None,
        schema=None,
        tests=[
            DbtColumnTest(name="test.proj.not_null_id", test_type="not_null", column_name="id"),
            DbtColumnTest(name="test.proj.unique_id", test_type="unique", column_name="id"),
            DbtColumnTest(name="test.proj.unknown_id", test_type="unknown_test", column_name="id"),
        ],
    )
    batch = converter.convert_model(model)
    assert batch.model is model
    # unknown_test has no conversion, so only 2 expectations
    assert len(batch.expectations) == 2
