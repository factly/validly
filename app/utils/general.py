import asyncio
from collections import ChainMap

import great_expectations as ge

from app.core.config import CustomExpectationsSettings, Settings
from app.expectations.custom_expectations import GenericCustomExpectations

settings = Settings()
custom_settings = CustomExpectationsSettings()


async def duplicates_expectation_suite(dataset, result_format):
    ge_pandas_dataset = ge.from_pandas(
        dataset
    )  # , dataset_class = GenericCustomExpectations
    expectation = ge_pandas_dataset.expect_compound_columns_to_be_unique(
        column_list=ge_pandas_dataset.columns, result_format=result_format
    )
    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": custom_settings.DUPLICATE_ROWS_EXPECTATION_NAME,
        "expectation_error_message": custom_settings.DUPLICATE_ROWS_EXPECTATION_ERR_MSG,
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


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
    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": custom_settings.LEADING_TRAILING_WHITE_SPACE_EXPECTATION_NAME,
        "expectation_error_message": custom_settings.LEADING_TRAILING_WHITESPACE_EXPECTATION_ERR_MSG,
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


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
    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": custom_settings.MULTIPLE_SPACE_EXPECTATION_NAME,
        "expectation_error_message": custom_settings.MULTIPLE_SPACE_EXPECTATION_ERR_MSG,
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


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
    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": custom_settings.SPECIAL_CHARACTER_EXPECTATION_NAME,
        "expectation_error_message": custom_settings.SPECIAL_CHARACTER_EXPECTATION_ERR_MSG,
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


async def bracket_values_expectation_suite(dataset, result_format):
    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = ge_pandas_dataset.expect_column_values_to_not_have_brackets(
        column_list=list(ge_pandas_dataset.columns),
        result_format=result_format,
    )
    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": custom_settings.BRACKETS_EXPECTATION_NAME,
        "expectation_error_message": custom_settings.BRACKETS_EXPECTATION_ERR_MSG,
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


async def general_table_expectation_suite(dataset, result_format):
    expectations = await asyncio.gather(
        duplicates_expectation_suite(dataset, result_format),
        leading_trailing_whitespace_expectation_suite(dataset, result_format),
        multispaces_between_text_expectation_suite(dataset, result_format),
        bracket_values_expectation_suite(dataset, result_format),
        special_character_expectation_suite(dataset, result_format),
    )
    expectations = ChainMap(*expectations)
    return expectations
