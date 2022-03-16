from typing import Dict

from fastapi import APIRouter

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.models.regex_list_pattern import RegexMatchList
from app.utils.common import read_dataset
from app.utils.unit import unit_expectation_suite

settings = Settings()

unit_router = router = APIRouter()


@router.get(
    "/expectations",
    summary="Expected to have proper format for writing units",
    response_model=Dict[str, RegexMatchList],
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
)
async def execute_unit_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await unit_expectation_suite(dataset, result_type)
    return expectation
