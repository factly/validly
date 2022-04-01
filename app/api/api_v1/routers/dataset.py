import logging
from typing import Dict, List, Union

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from fastapi.logger import logger
from fastapi.templating import Jinja2Templates

from app.core.config import Settings
from app.models.date_strftime_pattern import DateStrftimePattern
from app.models.enums import ExpectationResultType
from app.models.expect_column_values_to_be_in_set import ColumnValuesToBeInSet
from app.models.general import GeneralTableExpectation
from app.models.regex_list_pattern import RegexMatchList
from app.models.regex_pattern import RegexPatternExpectation
from app.utils.dataset import datasets_expectation
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
async def execute_dataset_expectation_get(request: Request):
    # with open('app/test/response_1648555775750.json') as json_file:
    #     expectations = json.load(json_file)
    # return templates.TemplateResponse("dataset-form.html",
    # context={"request": request, "expectations": expectations})
    return templates.TemplateResponse(
        "dataset-form.html", context={"request": request}
    )


@router.post(
    "/expectation/datasets/",
    response_model=Dict[
        str,
        Dict[
            str,
            Union[
                List[GeneralTableExpectation],
                RegexPatternExpectation,
                RegexMatchList,
                ColumnValuesToBeInSet,
                DateStrftimePattern,
            ],
        ],
    ],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
    summary="Execute all possible expectation to a dataset",
)
async def execute_dataset_expectation_post(
    request: Request,
    result_type: ExpectationResultType = Form(...),
    datasets: List[UploadFile] = File(...),
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
        expectations = await datasets_expectation(s3_files_key, result_type)
        return templates.TemplateResponse(
            "base.html",
            context={"request": request, "expectations": expectations},
        )
        # return expectations

    # return {"filename": dataset.filename, "content": dataset.content_type}     pass


# @router.get(
#     "/expectation/datasets",
#     response_model=Dict[
#         str,
#         Dict[
#             Any,
#             Union[
#                 List[GeneralTableExpectation],
#                 RegexPatternExpectation,
#                 RegexMatchList,
#                 ColumnValuesToBeInSet,
#                 DateStrftimePattern,
#             ],
#         ],
#     ],
#     response_model_exclude_none=True,
#     response_model_exclude_unset=True,
#     summary="Expectation for all datasets in a folder",
# )
# async def execute_datasets_expectation(
#     result_type: ExpectationResultType,
#     dataset_folder: str = settings.EXAMPLE_FOLDER,
# ):
#     try:
#         expectations = await datasets_expectation(dataset_folder, result_type)
#     except PathError as pe:
#         raise HTTPException(
#             status_code=status.HTTP_406_NOT_ACCEPTABLE,
#             detail=jsonable_encoder(
#                 {"error": str(pe), "dataset_folder": f"{dataset_folder}"}
#             ),
#         )
#     else:
#         return expectations
