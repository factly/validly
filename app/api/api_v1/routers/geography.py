import logging
from typing import Dict

from fastapi import APIRouter, File, HTTPException, UploadFile, status
from fastapi.logger import logger

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.models.expect_column_values_to_be_in_set import ColumnValuesToBeInSet
from app.utils.common import read_dataset
from app.utils.geography import (
    country_expectation_suite,
    geography_expectation_suites,
    state_expectation_suite,
)
from app.utils.minio_transfer import (
    get_files_inside_folder,
    upload_local_file_to_bucket,
)

logging.basicConfig(level=logging.INFO)

settings = Settings()
geographic_router = router = APIRouter()


@router.get(
    "/country/expectations",
    response_model=Dict[str, ColumnValuesToBeInSet],
    response_model_exclude_none=True,
    summary="Expected to have Proper Naming Convention for Countries",
)
async def execute_country_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_COUNTRY,
):
    dataset = await read_dataset(source)
    expectation = await country_expectation_suite(dataset, result_type)
    return expectation


@router.get(
    "/state/expectations",
    response_model=Dict[str, ColumnValuesToBeInSet],
    response_model_exclude_none=True,
    summary="Expected to have Proper Naming Convention for State",
)
async def execute_state_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_STATE,
):
    dataset = await read_dataset(source)
    expectation = await state_expectation_suite(dataset, result_type)
    return expectation


@router.post(
    "/expectations",
    response_model=Dict[str, ColumnValuesToBeInSet],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
    summary="Expected to have Proper Naming Convention for Geography Columns",
)
async def execute_geography_expectation_suite(
    result_type: ExpectationResultType,
    datasets: UploadFile = File(...),
):
    try:
        logger.info(f"dataset: {datasets}")
        # upload dataset to minio
        s3_folder = await upload_local_file_to_bucket(datasets)
    except Exception as e:
        logger.exception(f"error: {e}")
        HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not upload file to minio",
        )
    else:
        s3_files_key = await get_files_inside_folder(s3_folder)
        expectation = await geography_expectation_suites(
            s3_files_key, result_type
        )
        return expectation
