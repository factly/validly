from fastapi import APIRouter

from app.core.config import Settings
from app.utils.column_mapping import column_expectations_suit
from app.utils.common import read_dataset

settings = Settings()
column_router = router = APIRouter()


@column_router.get(
    "/expect_column_names_in_snake_casing",
    summary="Expect column names to be in snake_casing",
)
async def expect_column_names_in_snake_casing(
    source: str = settings.EXAMPLE_URL,
    result_format: str = "snake_casing",
):
    dataset = await read_dataset(source)
    expectation = await column_expectations_suit(dataset, result_format)
    return expectation
