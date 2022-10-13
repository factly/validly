import logging
from typing import List

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.logger import logger
from fastapi.templating import Jinja2Templates

from app.core.config import Settings

# from app.models.date_strftime_pattern import DateStrftimePattern
from app.models.enums import ExpectationResultFormat, ExpectationResultType
from aiohttp import ClientSession

# from app.models.expect_column_values_to_be_in_set import ColumnValuesToBeInSet
# from app.models.general import GeneralTableExpectation
# from app.models.regex_list_pattern import RegexMatchList
# from app.models.regex_pattern import RegexPatternExpectation
from app.utils.dataset import (
    datasets_expectation,
    datasets_expectation_from_url,
)
from app.utils.minio_transfer import (
    get_files_inside_folder,
    upload_local_file_to_bucket,
)

logging.basicConfig(level=logging.INFO)
templates = Jinja2Templates(directory="templates")

dataset_router = router = APIRouter()
settings = Settings()


@router.get(
    "/expectation/datasets/",
)
async def execute_dataset_expectation(request: Request):
    return templates.TemplateResponse(
        "base.html", context={"request": request}
    )


@router.post(
    "/expectation/datasets/",
    # response_model=Dict[
    #     str,
    #     Dict[
    #         str,
    #         Union[
    #             List[GeneralTableExpectation],
    #             RegexPatternExpectation,
    #             RegexMatchList,
    #             ColumnValuesToBeInSet,
    #             DateStrftimePattern,
    #         ],
    #     ],
    # ],
    # response_model_exclude_none=True,
    # response_model_exclude_unset=True,
    summary="Execute all possible expectation to a dataset",
)
async def execute_dataset_expectation_post(
    request: Request,
    format: ExpectationResultFormat = Query(
        default=ExpectationResultFormat.JSON,
        description="Provide Expectation output in desired format",
    ),
    result_type: ExpectationResultType = Form(
        ExpectationResultType.SUMMARY,
        description="Level of Deatils for a Expectation result",
    ),
    datasets: List[UploadFile] = File(
        ..., description="Dataset file to be evaluated"
    ),
):
    try:
        logger.info(f"dataset: {datasets}")
        # upload dataset to minio
        s3_folder = await upload_local_file_to_bucket(datasets)
    except Exception as e:
        logger.exception(f"error: {e}")
        HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not upload file to temporary minio bucket",
        )
    else:
        s3_files_key = await get_files_inside_folder(s3_folder)
        expectations = await datasets_expectation(s3_files_key, result_type)
        if format is ExpectationResultFormat.JSON:
            return expectations
        else:
            return templates.TemplateResponse(
                "base.html",
                context={"request": request, "expectations": expectations},
            )


@router.post(
    "/expectation/datasets/urls/",
)
async def execute_dataset_expectation_post_from_url(
    request: Request,
    urls: List[str],
    format: ExpectationResultFormat = Query(
        default=ExpectationResultFormat.JSON,
        description="Provide Expectation output in desired format",
    ),
    result_type: ExpectationResultType = Query(ExpectationResultType.SUMMARY),
):
    session = ClientSession()
    try:
        expectations = await datasets_expectation_from_url(urls, result_type, session = session)
    except Exception as e:
        logger.exception(f"error: {e}")
        HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not run Expectation",
        )
    else:
        if format is ExpectationResultFormat.JSON:
            return expectations
        else:
            return templates.TemplateResponse(
                "base.html",
                context={"request": request, "expectations": expectations},
            )
    finally:
        await session.close()


@router.post(
    "/expectation/datasets/uppy/",
)
async def execute_dataset_expectation_post_from_uppy(
    request: Request,
    format: ExpectationResultFormat = Query(
        default=ExpectationResultFormat.JSON,
        description="Provide Expectation output in desired format",
    ),
):
    # File should be passed by uppy in request form
    form = await request.form()
    file = form["file"]

    # If file is not present in request form, then quit the operation
    if not file:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file found in Request",
        )

    try:
        # upload dataset to minio
        s3_folder = await upload_local_file_to_bucket(file)
    except Exception as e:
        logger.exception(f"Error on saving file temporary to Minio: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error on saving file temporary to Minio: {e}",
        )
    else:
        # Get all files inside the folder in Minio Bucket
        s3_files_key = await get_files_inside_folder(s3_folder)

        # TODO : Later on, use ENUM to get the result type
        expectations = await datasets_expectation(s3_files_key, "SUMMARY")

        if format is ExpectationResultFormat.JSON:
            return expectations
        else:
            return templates.TemplateResponse(
                "base.html",
                context={"request": request, "expectations": expectations},
            )
