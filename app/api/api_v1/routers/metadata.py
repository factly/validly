import logging

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from fastapi.logger import logger

from app.models.enums import ExpectationResultType
from app.models.metadata_gsheet import MetadataGsheetRequest
from app.utils.common import read_dataset
from app.utils.gsheets import get_records_from_gsheets
from app.utils.metadata import metadata_expectation_suite

logging.basicConfig(level=logging.INFO)

metadata_router = router = APIRouter()


@router.post("/expectations/metadata/file")
async def execute_metadata_expectation_from_file(
    result_type: ExpectationResultType = Form(
        ExpectationResultType.SUMMARY,
        description="Level of Details for a Expectation result",
    ),
    file: UploadFile = File(...),
):

    # read the dataset from uploaded CSV file
    logger.info(f"dataset: {file.filename}")
    dataset = await read_dataset(file, is_file=True)
    # df = pd.read_csv(datasets.file)

    # # metadata expectation
    # expectation = await metadata_expectation_suite(
    #     df, result_type, dataset_name=datasets.filename
    # )

    # metadata expectation
    expectation = await metadata_expectation_suite(
        dataset, result_type, dataset_name=file.filename
    )

    return expectation


@router.post("/expectations/metadata/gsheet")
async def execute_metadata_expectation_gsheet(request: MetadataGsheetRequest):
    """Validating meta-data directly present in Google Sheets

    Args:
        request (MetadataGsheetRequest): Takes input such as sheetID, worksheet_name

    Raises:
        HTTPException: If there is no data available in the selected worksheet

    Returns:
        Json: Validation output from validly
    """
    dataset_meta_data = get_records_from_gsheets(
        sheet_id=request.sheet_id,
        worksheet=request.worksheet,
    )
    if not dataset_meta_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sheet {request.sheet_id} not found in {request.worksheet}",
        )

    df = pd.DataFrame(dataset_meta_data)

    # metadata expectation
    dataset_name = (
        f"{request.sheet_id}-{request.worksheet}"
        if request.worksheet
        else f"{request.sheet_id}"
    )
    expectation = await metadata_expectation_suite(
        df, request.result_type, dataset_name=dataset_name
    )
    return expectation
