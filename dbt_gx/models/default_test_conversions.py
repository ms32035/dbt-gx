"""Default test conversions for dbt-gx."""

from dbt_gx.models.test_conversion_base import TestConversion, TestConversionParams

# Default test conversions mapping dbt test names to Great Expectations expectations
DEFAULT_TEST_CONVERSIONS = {
    "not_null": TestConversion(
        expectation_class="ExpectColumnValuesToNotBeNull",
    ),
    "unique": TestConversion(
        expectation_class="ExpectColumnValuesToBeUnique",
    ),
    "dbt_utils.at_least_one": TestConversion(
        expectation_class="ExpectColumnUniqueValueCountToBeBetween",
        params=TestConversionParams(kwargs={"min_value": 1}),
    ),
    "dbt_utils.unique_combination_of_columns": TestConversion(
        expectation_class="ExpectCompoundColumnsToBeUnique",
        params=TestConversionParams(kwargs_mapping={"combination_of_columns": "column_list"}),
    ),
}


def default_convertion_factory() -> dict[str, TestConversion]:
    return DEFAULT_TEST_CONVERSIONS
