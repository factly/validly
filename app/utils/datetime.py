import great_expectations as ge

from app.core.config import DateTimeSettings
from app.utils.column_mapping import find_datetime_columns
from app.utils.common import (
    modify_values_to_match_regex,
    modify_values_to_match_strftime_format,
)

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
        results[each_column] = ge_pandas_dataset.validate()

    return results


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
        results[each_column] = ge_pandas_dataset.validate()

    return results


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
        results[each_column] = ge_pandas_dataset.validate()

    return results


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
        results[each_column] = ge_pandas_dataset.validate()

    return results


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
        results[each_column] = ge_pandas_dataset.validate()

    return results


async def datetime_expectation_suite(dataset, result_format):
    results = []
    results.append(
        await calendar_year_expectation_suite(
            dataset,
            result_format,
        )
    )
    results.append(
        await non_calendar_year_expectation_suite(dataset, result_format)
    )
    results.append(
        await quarter_expectation_suite(
            dataset,
            result_format,
        )
    )
    results.append(
        await month_expectation_suite(
            dataset,
            result_format,
        )
    )
    results.append(
        await date_expectation_suite(
            dataset,
            result_format,
        )
    )
    return [result for result in results if result]
