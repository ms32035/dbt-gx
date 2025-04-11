"""Test converter module for converting dbt tests to Great Expectations expectations."""

import importlib
from typing import Any

from great_expectations import expectations

from dbt_gx.models.dbt_base import DbtColumnTest, DbtModel, DbtTest
from dbt_gx.models.dbt_gx_config import DbtGxConfig
from dbt_gx.models.gx_base import GxBatch
from dbt_gx.models.test_conversion_base import TestConversion


def import_function(function_path: str) -> Any:
    """Import a function from a module path.

    Args:
        function_path: Full path to the function (e.g., 'module.submodule.function')

    Returns:
        The imported function.

    Raises:
        ImportError: If the function cannot be imported.
    """
    module_path, function_name = function_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, function_name)


class TestConverter:
    """Converts dbt tests to Great Expectations expectations."""

    def __init__(self, config: DbtGxConfig):
        """Initialize the test converter.

        Args:
            config: dbt-gx configuration.
        """
        self.config = config

    def get_test_conversion(self, test_type: str, namespace: str | None) -> TestConversion | None:
        """Get test conversion configuration for a test.

        Args:
            test_type: Name of the test.

        Returns:
            Test conversion configuration if found, None otherwise.
        """
        if namespace is None:
            return self.config.test_mappings.get(test_type)
        return self.config.test_mappings.get(f"{namespace}.{test_type}")

    def _get_context(self, test: DbtTest) -> dict[str, str]:
        """Get the context dictionary for parameter formatting.

        Args:
            test: The test being converted.

        Returns:
            Dictionary containing context values for parameter formatting.
        """
        context = {}
        if isinstance(test, DbtColumnTest):
            context["column_name"] = test.column_name
        return context

    def convert_test(self, model: DbtModel, test: DbtTest) -> expectations.Expectation | None:
        """Convert a dbt test to a Great Expectations expectation.

        Args:
            test: The test to convert.

        Returns:
            Great Expectations expectation instance if conversion is possible,
            None otherwise.
        """
        conversion = self.get_test_conversion(test.test_type, test.namespace)
        if not conversion:
            return None

        context = self._get_context(test)

        if conversion.function:
            converter_func = import_function(conversion.function)
            result = converter_func(test.kwargs, context)
            if isinstance(result, dict):
                expectation_class = getattr(expectations, result["expectation_type"])
                return expectation_class(**result["kwargs"])
            return result

        # Format parameter values with context
        params = {}

        for key, value in conversion.params.kwargs_mapping.items():
            params[value] = test.kwargs[key]

        check_type = "table"

        if isinstance(test, DbtColumnTest):
            params["column"] = test.column_name
            check_type = "column"

        # Add any additional parameters from test_config
        if "kwargs" in test.kwargs:
            params.update(test.kwargs["kwargs"])

        # Get the expectation class and create an instance
        expectation_class = getattr(expectations, conversion.expectation_class)
        return expectation_class(**params, meta={"model": model.meta, "check_type": check_type})

    def convert_model(self, model: DbtModel) -> GxBatch:
        """Convert a dbt model and its tests to a Great Expectations batch.

        Args:
            model: The dbt model to convert.

        Returns:
            GxBatch containing the model and its converted expectations.
        """
        converted_expectations = []

        for test in model.tests:
            expectation = self.convert_test(model, test)
            if expectation:
                converted_expectations.append(expectation)

        return GxBatch(model=model, expectations=converted_expectations)
