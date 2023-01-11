import asyncio
from collections import ChainMap

import great_expectations as ge

from app.core.config import (
    CustomExpectationsSettings,
    MetadataSettings,
    Settings,
)
from app.expectations.custom_expectations import GenericCustomExpectations
from app.models.enums import ExpectationResultType
from app.utils.column_mapping import find_metadata_columns

settings = Settings()
custom_settings = CustomExpectationsSettings()
metadata_custom_settings = MetadataSettings()


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


async def null_not_in_columns(dataset, result_format, column, column_type):
    # !This function can be a bottleneck later as it holds to many specific tasks,
    # !such as applying on individual column, modifying expectation report
    if column_type == "numeric":
        expectation_name = custom_settings.NULL_NUMERIC_VALUE_NAME.format(
            column=column
        )
        expectation_error_message = custom_settings.NULL_NUMERIC_VALUE_MSG
    else:
        expectation_name = custom_settings.NULL_CATEGORICAL_VALUE_NAME.format(
            column=column
        )
        expectation_error_message = custom_settings.NULL_CATEGORICAL_VALUE_MSG

    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = ge_pandas_dataset.expect_column_values_to_not_be_null(
        column=column,
        catch_exceptions=True,
        result_format=result_format,
    )
    # expectation = ge_pandas_dataset.expect_column_values_to_not_be_null(
    #     column=column, result_format=result_format, catch_exceptions=True
    # )

    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": expectation_name,
        "expectation_error_message": expectation_error_message,
    }

    if "unexpected_index_list" in expectation_dict["result"]:
        expectation_dict["result"]["unexpected_list"] = (
            dataset.iloc[
                expectation_dict["result"]["unexpected_index_list"], :
            ]
            .fillna("")
            .to_dict(orient="records")
        )
    # partial unexpected index list is not made with this expectation but is required by the studio
    if (
        "partial_unexpected_index_list" not in expectation_dict["result"]
        and result_format == ExpectationResultType.COMPLETE
    ):
        expectation_dict["result"][
            "partial_unexpected_index_list"
        ] = expectation_dict["result"]["unexpected_index_list"][
            : max(20, len(expectation_dict["result"]["unexpected_index_list"]))
        ]
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


async def observation_more_than_thresh_expectation_suite(
    dataset, result_format
):
    ge_pandas_dataset = ge.from_pandas(
        dataset, dataset_class=GenericCustomExpectations
    )
    expectation = (
        ge_pandas_dataset.expect_multicolumn_dataset_to_have_more_than_x_rows(
            column_list=dataset.columns.tolist(),
            result_format=result_format,
        )
    )
    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": custom_settings.OBSERVATIONS_MORE_THAN_THRESH_NAME,
        "expectation_error_message": custom_settings.OBSERVATIONS_MORE_THAN_THRESH_MSG.format(
            thresh=custom_settings.MINIMUM_DATASET_OBSERVATION_THRESH
        ),
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


async def general_table_expectation_suite(dataset, result_format):
    """Chaining all general expectaion suites for Datasets

    Chianmap required general expectation suites(ex: duplicates, whitespaces,
    not null) and running all of them asynchronously

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations of all Expectation suites running
    """

    numeric_columns = dataset.select_dtypes(
        include=custom_settings.NUMERIC_COLUMNS_TYPES
    ).columns.tolist()
    numeric_columns = [
        numeric_column
        for numeric_column in numeric_columns
        if numeric_column not in custom_settings.UNIT_NOTE_COLUMNS
    ]

    expectations = await asyncio.gather(
        duplicates_expectation_suite(dataset, result_format),
        leading_trailing_whitespace_expectation_suite(dataset, result_format),
        multispaces_between_text_expectation_suite(dataset, result_format),
        bracket_values_expectation_suite(dataset, result_format),
        special_character_expectation_suite(dataset, result_format),
        # null_not_in_columns(dataset, result_format, "price"),
        *[
            null_not_in_columns(dataset, result_format, col, "numeric")
            for col in numeric_columns
        ],
        observation_more_than_thresh_expectation_suite(dataset, result_format),
    )
    expectations = ChainMap(*expectations)
    return expectations


async def general_metadata_expectation_suite(dataset, result_format):
    """Chaining all general expectaion suites for Metadata

    Chianmap required general expectation suites(ex: duplicates, whitespaces,
    not null) and running all of them asynchronously

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations of all Expectation suites running
    """

    categorical_columns_dict = await find_metadata_columns(
        set(dataset.columns)
    )
    categorical_columns = [
        each_columns_value[0]
        for each_columns_value in categorical_columns_dict.values()
    ]

    categorical_columns = [
        categorical_column
        for categorical_column in categorical_columns
        if categorical_column not in custom_settings.UNIT_NOTE_COLUMNS
    ]

    numeric_columns = dataset.select_dtypes(
        include=custom_settings.NUMERIC_COLUMNS_TYPES
    ).columns.tolist()
    numeric_columns = [
        numeric_column
        for numeric_column in numeric_columns
        if numeric_column not in custom_settings.UNIT_NOTE_COLUMNS
    ]

    expectations = await asyncio.gather(
        duplicates_expectation_suite(dataset, result_format),
        leading_trailing_whitespace_expectation_suite(dataset, result_format),
        multispaces_between_text_expectation_suite(dataset, result_format),
        *[
            null_not_in_columns(dataset, result_format, col, "category")
            for col in categorical_columns
        ],
    )
    expectations = ChainMap(*expectations)
    return expectations
