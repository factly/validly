from typing import Dict

from fastapi import APIRouter

from app.core.config import DateTimeSettings, Settings
from app.models.date_strftime_pattern import DateStrftimePattern
from app.models.enums import ExpectationResultType
from app.models.regex_pattern import RegexPatternExpectation
from app.utils.common import read_dataset
from app.utils.datetime import (
    calender_year_proper_format,
    date_proper_format,
    month_proper_format,
    non_calender_year_proper_format,
    quarter_proper_format,
)

settings = Settings()
datetime_settings = DateTimeSettings()
datetime_router = router = APIRouter()


@router.get(
    "/expect-calender-year-proper-format",
    response_model=Dict[str, DateStrftimePattern],
    response_model_exclude_none=True,
    summary="Expected to have proper format for calender year",
)
async def expect_calender_year_proper_format(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_COUNTRY,
):
    # functionality to check various year columns
    dataset = await read_dataset(source)
    expectation = await calender_year_proper_format(dataset, result_type)
    return expectation


@router.get(
    "/expect_non_calender_year_proper_format",
    response_model=Dict[str, RegexPatternExpectation],
    response_model_exclude_none=True,
    summary="Expected to have proper format for non-calender year",
)
async def expect_non_calender_year_proper_format(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    # functionality to check various year columns
    dataset = await read_dataset(source)
    expectation = await non_calender_year_proper_format(dataset, result_type)
    return expectation


@router.get(
    "/expect_quarter_in_proper_format",
    response_model=Dict[str, RegexPatternExpectation],
    response_model_exclude_none=True,
    summary="Expected to have proper format for quarter",
)
async def expect_quarter_in_proper_format(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await quarter_proper_format(dataset, result_type)
    return expectation


@router.get(
    "/expect_month_proper_format",
    response_model=Dict[str, DateStrftimePattern],
    response_model_exclude_none=True,
)
async def expect_month_proper_format(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await month_proper_format(dataset, result_type)
    return expectation


@router.get(
    "/expect_date_proper_format",
    response_model=Dict[str, DateStrftimePattern],
    response_model_exclude_none=True,
)
async def expect_date_proper_format(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await date_proper_format(dataset, result_type)
    return expectation
