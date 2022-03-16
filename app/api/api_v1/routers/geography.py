from typing import Dict

from fastapi import APIRouter

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.models.expect_column_values_to_be_in_set import ColumnValuesToBeInSet
from app.utils.common import read_dataset
from app.utils.geography import (
    country_expectation_suite,
    geography_expectation_suite,
    state_expectation_suite,
)

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


@router.get(
    "/expectations",
    summary="Expected to have Proper Naming Convention for Geography Columns",
)
async def execute_geography_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_COUNTRY,
):
    dataset = await read_dataset(source)
    expectation = await geography_expectation_suite(dataset, result_type)
    return expectation
