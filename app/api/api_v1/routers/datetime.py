import logging
from typing import Dict, Union

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.logger import logger

from app.core.config import DateTimeSettings, Settings
from app.models.date_strftime_pattern import DateStrftimePattern
from app.models.enums import ExpectationResultType
from app.models.regex_pattern import RegexPatternExpectation
from app.utils.common import read_pandas_dataset
from app.utils.datetime import (
    calendar_year_expectation_suite,
    date_expectation_suite,
    datetime_expectation_suites,
    month_expectation_suite,
    non_calendar_year_expectation_suite,
    quarter_expectation_suite,
)
from app.utils.minio_transfer import (
    get_files_inside_folder,
    upload_local_file_to_bucket,
)

logging.basicConfig(level=logging.INFO)

settings = Settings()
datetime_settings = DateTimeSettings()
datetime_router = router = APIRouter()


@router.get(
    "/calendar-year/expectations",
    response_model=Dict[str, RegexPatternExpectation],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
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
    response_model=Dict[str, RegexPatternExpectation],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
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
    response_model=Dict[str, RegexPatternExpectation],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
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
    response_model=Dict[str, DateStrftimePattern],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
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
    response_model=Dict[str, DateStrftimePattern],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
    summary="Suitable Expectations for date columns",
)
async def execute_date_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_DATE,
):
    dataset = await read_pandas_dataset(source)
    expectation = await date_expectation_suite(dataset, result_type)
    return expectation


@router.post(
    "/expectations",
    response_model=Dict[
        str, Union[RegexPatternExpectation, DateStrftimePattern]
    ],
    response_model_exclude_none=False,
    summary="Expectations on all date columns",
)
async def execute_datetime_expectation_columns(
    result_type: ExpectationResultType,
    dataset: UploadFile = File(...),
):
    try:
        logger.info(f"dataset: {dataset}")
        # upload dataset to minio
        s3_folder = await upload_local_file_to_bucket(dataset)
    except Exception as e:
        logger.exception(f"error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not upload file to minio",
        )
    else:
        s3_files_key = await get_files_inside_folder(s3_folder)
        expectation = await datetime_expectation_suites(
            s3_files_key, result_type
        )
        return expectation
