from typing import Any, Dict, List, Union

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from pydantic import PathError

from app.core.config import Settings
from app.models.date_strftime_pattern import DateStrftimePattern
from app.models.enums import ExpectationResultType
from app.models.expect_column_values_to_be_in_set import ColumnValuesToBeInSet
from app.models.general import GeneralTableExpectation
from app.models.regex_list_pattern import RegexMatchList
from app.models.regex_pattern import RegexPatternExpectation
from app.utils.dataset import dataset_expectation, datasets_expectation

dataset_router = router = APIRouter()
settings = Settings()


@router.get(
    "/expectation/datasets/{dataset:path}",
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
    summary="Execute all possible expectation to datasets",
)
async def execute_dataset_expectation(
    result_type: ExpectationResultType,
    dataset: str = Query(settings.EXAMPLE_URL),
):

    expectations = await dataset_expectation(dataset, result_type)
    return expectations


@router.get(
    "/expectation/datasets",
    response_model=Dict[
        str,
        Dict[
            Any,
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
    summary="Expectation for all datasets in a folder",
)
async def execute_datasets_expectation(
    result_type: ExpectationResultType,
    dataset_folder: str = settings.EXAMPLE_FOLDER,
):
    try:
        expectations = await datasets_expectation(dataset_folder, result_type)
    except PathError as pe:
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail=jsonable_encoder(
                {"error": str(pe), "dataset_folder": f"{dataset_folder}"}
            ),
        )
    else:
        return expectations
