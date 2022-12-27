import logging
from fastapi import APIRouter
from app.models.enums import ExpectationResultType
from fastapi import APIRouter, File, HTTPException, UploadFile, status, Form
from fastapi.logger import logger
import pandas as pd
from app.utils.metadata import metadata_expectation_suite

logging.basicConfig(level=logging.INFO)

metadata_router = router = APIRouter()


@router.post(
    "/expectations/metadata"
)
async def execute_sector_expectation(
    result_type: ExpectationResultType = Form(
        ExpectationResultType.SUMMARY,
        description="Level of Deatils for a Expectation result",
    ),
    datasets: UploadFile = File(...),
):

    # read the dataset from uploaded CSV file
    logger.info(f"dataset: {datasets.filename}")
    df = pd.read_csv(datasets.file)

    # metadata expectatation
    expectation = await metadata_expectation_suite(df, result_type)
    
    return expectation