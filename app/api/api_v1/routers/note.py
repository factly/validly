from typing import Dict

from fastapi import APIRouter

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.models.regex_list_pattern import RegexMatchList
from app.utils.common import read_dataset
from app.utils.note import note_proper_format

settings = Settings()

note_router = router = APIRouter()


@router.get(
    "/expect_note_in_proper_format",
    summary="Expected to have note column in proper format",
    response_model=Dict[str, RegexMatchList],
    response_model_exclude_none=True,
)
async def expect_note_in_proper_format(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await note_proper_format(dataset, result_type)
    return expectation
