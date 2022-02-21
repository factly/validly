import asyncio
from collections import ChainMap

from fastapi import APIRouter

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.utils.common import read_dataset
from app.utils.datetime import datetime_expectation_suite
from app.utils.general import general_table_expectation_suite
from app.utils.geography import geography_expectation_suite
from app.utils.note import note_expectation_suite
from app.utils.unit import unit_expectation_suite

dataset_router = router = APIRouter()
settings = Settings()


@router.get(
    "/expectation",
    summary="Execute all possible expectation to datasets",
)
async def execute_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectations = await asyncio.gather(
        datetime_expectation_suite(dataset, result_type),
        geography_expectation_suite(dataset, result_type),
        note_expectation_suite(dataset, result_type),
        unit_expectation_suite(dataset, result_type),
        general_table_expectation_suite(dataset, result_type),
    )
    expectations = ChainMap(*expectations)
    return expectations
