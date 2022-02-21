from fastapi import APIRouter

from app.core.config import Settings
from app.models.enums import ExpectationResultType
from app.utils.common import read_dataset
from app.utils.general import (
    bracket_values_expectatio_suite,
    duplicates_expectation_suite,
    general_table_expectation_suite,
    leading_trailing_whitespace_expectation_suite,
    multispaces_between_text_expectation_suite,
    special_character_expectation_suite,
)

settings = Settings()

general_router = router = APIRouter()


@router.get(
    "/duplicates/expectations",
    summary="Check if dataset has duplicate entries",
)
async def execute_duplicates_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL_COUNTRY,
):
    dataset = await read_dataset(source)
    expectation = await duplicates_expectation_suite(dataset, result_type)
    return expectation.to_json_dict()


@router.get(
    "/leading-trailing-whitespace/expectations",
    summary="Check if dataset has leading and trailing whitespace",
)
async def execute_leading_trailing_whitespace_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await leading_trailing_whitespace_expectation_suite(
        dataset, result_type
    )
    return expectation.to_json_dict()


@router.get(
    "/multispace-whitespace/expectations",
    summary="Check if dataset has multispaces whitespaces",
)
async def execute_multispace_between_text_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await multispaces_between_text_expectation_suite(
        dataset, result_type
    )
    return expectation.to_json_dict()


@router.get(
    "/special-character/expectations",
    summary="Check if dataset has special characters present in column values",
)
async def execute_special_character_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await special_character_expectation_suite(
        dataset, result_type
    )
    return expectation.to_json_dict()


@router.get(
    "/bracket-present/expectations",
    summary="Check if dataset has brackets present",
)
async def execute_bracket_values_expectatio_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await bracket_values_expectatio_suite(dataset, result_type)
    return expectation.to_json_dict()


@router.get(
    "/expectations",
    summary="Run expectation on table",
)
async def execute_general_table_expectation_suite(
    result_type: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectation = await general_table_expectation_suite(dataset, result_type)
    return expectation
