from typing import Dict

from fastapi import APIRouter

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.models.expect_column_values_to_be_in_set import ColumnValuesToBeInSet
from app.utils.common import read_dataset
from app.utils.geography import standard_country_names, standard_state_names

settings = Settings()
geographic_router = router = APIRouter()


@router.get(
    "/expect_countries_in_standard_name",
    response_model=Dict[str, ColumnValuesToBeInSet],
    response_model_exclude_none=True,
    summary="Expected to have Proper Naming Convention for States",
)
async def expect_countries_in_standard_name(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_COUNTRY,
):
    dataset = await read_dataset(source)
    expectation = await standard_country_names(dataset, result_type)
    return expectation


@router.get(
    "/expect_states_in_standard_name",
    response_model=Dict[str, ColumnValuesToBeInSet],
    response_model_exclude_none=True,
    summary="Expected to have Proper Naming Convention for Countries",
)
async def expect_states_in_standard_name(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_STATE,
):
    dataset = await read_dataset(source)
    expectation = await standard_state_names(dataset, result_type)
    return expectation