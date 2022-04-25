import asyncio
from collections import ChainMap

import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.core.config import DateTimeSettings, Settings
from app.utils.column_mapping import find_datetime_columns
from app.utils.common import (
    modify_values_to_match_regex,
    modify_values_to_match_strftime_format,
    read_dataset,
)

settings = Settings()
datetime_settings = DateTimeSettings()


async def modify_calendar_year_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = datetime_settings.CALENDAR_YEAR_EXPECTATION

    changed_config = {
        "expect_column_values_to_match_regex": {
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_match_regex(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def calendar_year_expectation_suite(dataset, result_format):
    # get all date time columns present inside datasets
    results = {}
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    for each_column in datetime_columns["calender_year"]:
        expectation_suite = await modify_calendar_year_expectation_suite(
            each_column,
            result_format,
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        validation = ge_pandas_dataset.validate()
        validation = ge_pandas_dataset.validate()
        validation_ui_name = (
            validation["results"][0]["expectation_config"]["meta"][
                "expectation_name"
            ]
            + " - "
            + validation["results"][0]["expectation_config"]["_kwargs"][
                "column"
            ]
        )
        results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_non_calendar_year_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = datetime_settings.NON_CALENDAR_YEAR_EXPECTATION

    changed_config = {
        "expect_column_values_to_match_regex": {
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_match_regex(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def non_calendar_year_expectation_suite(dataset, result_format):
    # get all date time columns present inside datasets
    results = {}
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    for each_column in datetime_columns["non_calendar_year"]:
        expectation_suite = await modify_non_calendar_year_expectation_suite(
            each_column,
            result_format,
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        validation = ge_pandas_dataset.validate()
        validation_ui_name = (
            validation["results"][0]["expectation_config"]["meta"][
                "expectation_name"
            ]
            + " - "
            + validation["results"][0]["expectation_config"]["_kwargs"][
                "column"
            ]
        )
        results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_quarter_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = datetime_settings.QUARTER_EXPECTATION

    changed_config = {
        "expect_column_values_to_match_regex": {
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_match_regex(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def quarter_expectation_suite(dataset, result_format):
    # get all date time columns present inside datasets
    results = {}
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    for each_column in datetime_columns["quarter"]:
        expectation_suite = await modify_quarter_expectation_suite(
            each_column, result_format
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        validation = ge_pandas_dataset.validate()
        validation_ui_name = (
            validation["results"][0]["expectation_config"]["meta"][
                "expectation_name"
            ]
            + " - "
            + validation["results"][0]["expectation_config"]["_kwargs"][
                "column"
            ]
        )
        results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_month_expectation_suite(column_name: str, result_format: str):
    default_expectation_suite = datetime_settings.MONTH_EXPECTATION

    changed_config = {
        "expect_column_values_to_match_strftime_format": {
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_match_strftime_format(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def month_expectation_suite(dataset, result_format):
    # get all date time columns present inside datasets
    results = {}
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    for each_column in datetime_columns["month"]:
        expectation_suite = await modify_month_expectation_suite(
            each_column, result_format
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        validation = ge_pandas_dataset.validate()
        validation_ui_name = (
            validation["results"][0]["expectation_config"]["meta"][
                "expectation_name"
            ]
            + " - "
            + validation["results"][0]["expectation_config"]["_kwargs"][
                "column"
            ]
        )
        results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_date_expectation_suite(column_name: str, result_format: str):
    default_expectation_suite = datetime_settings.DATE_EXPECTATION

    changed_config = {
        "expect_column_values_to_match_strftime_format": {
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_match_strftime_format(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def date_expectation_suite(dataset, result_format):
    # get all date time columns present inside datasets
    results = {}
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    for each_column in datetime_columns["date"]:
        expectation_suite = await modify_date_expectation_suite(
            each_column, result_format
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        validation = ge_pandas_dataset.validate()
        validation_ui_name = (
            validation["results"][0]["expectation_config"]["meta"][
                "expectation_name"
            ]
            + " - "
            + validation["results"][0]["expectation_config"]["_kwargs"][
                "column"
            ]
        )
        results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def datetime_expectation_suite(dataset, result_format):
    if isinstance(dataset, str):
        dataset = await read_dataset(dataset)

    expectations = await asyncio.gather(
        calendar_year_expectation_suite(dataset, result_format),
        non_calendar_year_expectation_suite(dataset, result_format),
        quarter_expectation_suite(dataset, result_format),
        month_expectation_suite(dataset, result_format),
        date_expectation_suite(dataset, result_format),
    )
    expectations = ChainMap(*expectations)
    return jsonable_encoder(expectations)


async def datetime_expectation_suites(s3_files_key, result_type):
    expectation = await asyncio.gather(
        *[
            datetime_expectation_suite(
                f"http://{settings.S3_ENDPOINT}/{settings.S3_BUCKET}/{s3_file_key}",
                result_type,
            )
            for s3_file_key in s3_files_key
        ]
    )
    expectation = ChainMap(*expectation)
    return jsonable_encoder(expectation)
