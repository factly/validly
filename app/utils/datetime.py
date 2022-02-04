from app.core.config import DateTimeSettings
from app.utils.column_mapping import find_datetime_columns

datetime_settings = DateTimeSettings()


async def calender_year_proper_format(dataset, result_format):
    results = {}
    # get all date time columns present inside datasets
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    # get all those columns corresponding to calender year
    for each_column in datetime_columns["calender_year"]:
        expectation = dataset.expect_column_values_to_match_strftime_format(
            column=each_column,
            strftime_format=datetime_settings.CALENDAR_YEAR_PATTERN,
            result_format=result_format,
            include_config=True,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results


async def non_calender_year_proper_format(dataset, result_format):
    results = {}
    # get all date time columns present inside datasets
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    # get all those columns corresponding to non-calender year\
    # (Fiscal Year, Academic_year)
    for each_column in datetime_columns["non_calendar_year"]:
        expectation = dataset.expect_column_values_to_match_regex(
            column=each_column,
            regex=datetime_settings.NON_CALENDAR_YEAR_PATTERN,
            result_format=result_format,
            include_config=True,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results


async def quarter_proper_format(dataset, result_format):
    results = {}
    # get all date time columns present inside datasets
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    # get all those columns corresponding to date
    for each_column in datetime_columns["quarter"]:
        expectation = dataset.expect_column_values_to_match_regex(
            column=each_column,
            regex=datetime_settings.QUARTER_FORMAT,
            result_format=result_format,
            include_config=False,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results


async def month_proper_format(dataset, result_format):
    results = {}
    # get all date time columns present inside datasets
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    # get all those columns corresponding to date
    for each_column in datetime_columns["month"]:
        expectation = dataset.expect_column_values_to_match_strftime_format(
            column=each_column,
            strftime_format=datetime_settings.MONTH_FORMAT,
            result_format=result_format,
            include_config=False,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results


async def date_proper_format(dataset, result_format):
    results = {}
    # get all date time columns present inside datasets
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    # get all those columns corresponding to date
    for each_column in datetime_columns["date"]:
        expectation = dataset.expect_column_values_to_match_strftime_format(
            column=each_column,
            strftime_format=datetime_settings.DATE_FORMAT,
            result_format=result_format,
            include_config=True,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results
