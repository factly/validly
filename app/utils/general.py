import asyncio

import great_expectations as ge

from app.expectations.custom_expectations import GenericCustomExpectations


async def duplicates_expectation_suite(dataset, result_format):
    ge_pandas_dataset = ge.from_pandas(
        dataset
    )  # , dataset_class = GenericCustomExpectations
    expectation = ge_pandas_dataset.expect_compound_columns_to_be_unique(
        column_list=ge_pandas_dataset.columns, result_format=result_format
    )
    return expectation


async def leading_trailing_whitespace_expectation_suite(
    dataset, result_format
):
    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = ge_pandas_dataset.expect_column_values_to_not_have_leading_or_trailing_whitespace(
        column_list=list(ge_pandas_dataset.columns),
        result_format=result_format,
    )
    return expectation


async def multispaces_between_text_expectation_suite(dataset, result_format):
    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = (
        ge_pandas_dataset.expect_columns_values_to_not_have_multispaces(
            column_list=list(ge_pandas_dataset.columns),
            result_format=result_format,
        )
    )
    return expectation


async def special_character_expectation_suite(dataset, result_format):
    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = (
        ge_pandas_dataset.expect_column_values_to_not_have_special_character(
            column_list=list(ge_pandas_dataset.columns),
            result_format=result_format,
        )
    )
    return expectation


async def bracket_values_expectation_suite(dataset, result_format):
    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = ge_pandas_dataset.expect_column_values_to_not_have_brackets(
        column_list=list(ge_pandas_dataset.columns),
        result_format=result_format,
    )
    return expectation


async def general_table_expectation_suite(dataset, result_format):
    expectations = await asyncio.gather(
        duplicates_expectation_suite(dataset, result_format),
        leading_trailing_whitespace_expectation_suite(dataset, result_format),
        multispaces_between_text_expectation_suite(dataset, result_format),
        bracket_values_expectation_suite(dataset, result_format),
        special_character_expectation_suite(dataset, result_format),
    )
    expectations = [expectation.to_json_dict() for expectation in expectations]
    return {"table_expectations": expectations}
