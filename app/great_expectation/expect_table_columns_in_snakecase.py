import re

# This class defines a Metric to support \
# your Expectation
# For most Expectations, the main business logic \
# for calculation will live here.
# To learn about the relationship between Metrics\
# and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics
from typing import Any, Dict, Optional, Tuple

from great_expectations.core import ExpectationConfiguration

# This giant block of imports should be something simpler, such as:
# from great_expectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics.metric_provider import (
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.expectations.util import (
    render_evaluation_parameter_string,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import substitute_none_for_missing
from great_expectations.validator.validation_graph import MetricConfiguration

TEXT_SNAKE_CASE_PATTERN = re.compile(r"^[a-z0-9]+([_a-z0-9]+)*$")


# This class defines the Metric, a class used by the \
# Expectation to compute important data for validating itself
class TableSnakecaseColumnNames(TableMetricProvider):

    # This is a built in metric - you do not have to implement\
    # it yourself. If you would like to use
    # a metric that does not yet exist, you can use the \
    # template below to implement it!
    metric_name = "table.snakecase_column_names"

    # Below are metric computations for different dialects \
    # (Pandas, SqlAlchemy, Spark)
    # They can be used to compute the table data you will need \
    # to validate your Expectation
    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        columns = metrics.get("table.columns")
        # For each column, testing if alphabetical and returning\
        # number of alphabetical columns
        snake_case_columns = len(
            list(
                filter(
                    lambda column: not TEXT_SNAKE_CASE_PATTERN.match(column)
                    if isinstance(column, str)
                    else False,
                    columns,
                )
            )
        )
        return snake_case_columns

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectTableColumnsInSnakecase(TableExpectation):
    """TODO: add a docstring here"""

    # These examples will be shown in the public gallery, \
    # and also executed as unit tests for your Expectation
    # examples = [
    #     {
    #         "data": {
    #             "column_One": [3, 5, 7],
    #             "column_@wo": [True, False, True],
    #             "column_3": ["a", "b", "c"],
    #             "columnFOUR": [None, 2, None],
    #         },
    #         "tests": [
    #             {
    #                 "title": "snakecase_column_names",
    #                 "exact_match_out": False,
    #                 "include_in_gallery": True,
    #                 "in": {},
    #                 "out": {
    #                     "success": False,
    #                     "observed_value": 3,
    #                 },
    #             },
    #         ],
    #     }
    # ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": [],
        "contributors": [],
        "package": "experimental_expectations",
    }
    metric_dependencies = ("table.snakecase_column_names",)
    success_keys = ()

    default_kwarg_values = {
        # "user_input": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ):
        """
        Validates that a configuration has been set, and sets a \
            configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for \
            the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that \
                    will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. \
                Otherwise, raises an exception
        """

        #     # Setting up a configuration
        # try:
        #     assert "user_input" in configuration.kwargs
        # # "user_input is required"
        #     assert isinstance(
        #         configuration.kwargs["user_input"], str
        #     ), "user_input must be a string"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))
        super().validate_configuration(configuration)
        return True

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["value"])
        template_str = "Must have Snakecase Column Names"
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    # This method will utilize the computed metric to validate that your \
    # Expectation about the Table is true
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        actual_column_count = metrics.get("table.snakecase_column_names")
        return {
            "success": actual_column_count == 0,
            "result": {"observed_value": actual_column_count},
        }


# if __name__ == "__main__":
#     diagnostics = ExpectSnakecaseColumnNames().run_diagnostics()
#     print(json.dumps(diagnostics, indent=2))
