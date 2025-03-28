"""Default test conversions for dbt-gx."""

from dbt_gx.models.test_conversion_base import TestConversion, TestConversionParams

# Default test conversions mapping dbt test names to Great Expectations expectations
DEFAULT_TEST_CONVERSIONS = {
    "not_null": TestConversion(
        expectation_type="expect_column_values_to_not_be_null",
    ),
    "unique": TestConversion(
        expectation_type="expect_column_values_to_be_unique",
    ),
    "accepted_values": TestConversion(
        expectation_type="expect_column_values_to_be_in_set",
        params=TestConversionParams(value="{values}", kwargs_mapping={"values": "value_set"}),
    ),
    "relationships": TestConversion(
        expectation_type="expect_column_values_to_be_in_set",
        params=TestConversionParams(value="{to}", kwargs={"parse_strings_as_datetimes": True}),
    ),
    "positive_value": TestConversion(
        expectation_type="expect_column_values_to_be_between",
        params=TestConversionParams(kwargs={"min_value": 0, "strict_min": True}),
    ),
}


def default_convertion_factory() -> dict[str, TestConversion]:
    return DEFAULT_TEST_CONVERSIONS
