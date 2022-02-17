from fastapi import APIRouter

from app.core.config import DateTimeSettings, Settings
from app.models.enums import ExpectationResultType
from app.utils.common import read_pandas_dataset
from app.utils.datetime import (
    calendar_year_expectation_suite,
    date_expectation_suite,
    datetime_expectation_suite,
    month_expectation_suite,
    non_calendar_year_expectation_suite,
    quarter_expectation_suite,
)

settings = Settings()
datetime_settings = DateTimeSettings()
datetime_router = router = APIRouter()


@router.get(
    "/calendar-year/expectations",
    # response_model=Dict[str, DateStrftimePattern],
    # response_model_exclude_none=True,
    summary="Suitability of calender year columns",
)
async def execute_calendar_year_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_COUNTRY,
):
    dataset = await read_pandas_dataset(source)
    expectation = await calendar_year_expectation_suite(dataset, result_type)
    return expectation


@router.get(
    "/non-calendar-year/expectations",
    # response_model=Dict[str, RegexPatternExpectation],
    # response_model_exclude_none=True,
    summary="Suitability of non-calender year columns",
)
async def execute_non_calendar_year_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_pandas_dataset(source)
    expectation = await non_calendar_year_expectation_suite(
        dataset, result_type
    )
    return expectation


@router.get(
    "/quarter/expectations",
    # response_model=Dict[str, RegexPatternExpectation],
    # response_model_exclude_none=True,
    summary="Suitable Expectations for quarter columns",
)
async def execute_quarter_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_QUARTER,
):
    dataset = await read_pandas_dataset(source)
    expectation = await quarter_expectation_suite(dataset, result_type)
    return expectation


@router.get(
    "/month/expectations",
    # response_model=Dict[str, DateStrftimePattern],
    # response_model_exclude_none=True,
    summary="Suitable Expectations for month columns",
)
async def execute_month_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_MONTH,
):
    dataset = await read_pandas_dataset(source)
    expectation = await month_expectation_suite(dataset, result_type)
    return expectation


@router.get(
    "/date/expectations",
    # response_model=Dict[str, DateStrftimePattern],
    # response_model_exclude_none=True,
    summary="Suitable Expectations for date columns",
)
async def execute_date_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_DATE,
):
    dataset = await read_pandas_dataset(source)
    expectation = await date_expectation_suite(dataset, result_type)
    return expectation


@router.get(
    "/expectations",
    summary="Expectations on all date columns",
)
async def execute_datetime_expectation_columns(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_DATE,
):
    dataset = await read_pandas_dataset(source)
    expectation = await datetime_expectation_suite(dataset, result_type)
    return expectation
