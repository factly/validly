import asyncio
from collections import ChainMap

from fastapi import APIRouter

from app.core.config import DateTimeSettings, Settings
from app.models.column_mapping import (
    AllMappedColumns,
    DateTimeColumns,
    GeographyColumns,
    MetadataColumns,
    NoteColumns,
    TagsColumns,
    UnitColumns,
)
from app.models.enums import ExpectationResultType
from app.utils.column_mapping import (
    find_datetime_columns,
    find_geography_columns,
    find_mapped_columns,
    find_metadata_columns,
    find_note_columns,
    find_tags_columns,
    find_unit_columns,
)
from app.utils.common import read_dataset
from app.utils.datetime import datetime_expectation_suite
from app.utils.general import general_table_expectation_suite
from app.utils.geography import geography_expectation_suite
from app.utils.metadata import metadata_expectation_suite
from app.utils.note import note_expectation_suite
from app.utils.tags import tags_expectation_suite
from app.utils.unit import unit_expectation_suite

settings = Settings()
datetime_settings = DateTimeSettings()
column_mapper_router = router = APIRouter()


@router.get(
    "",
    response_model=AllMappedColumns,
    response_model_exclude_none=True,
)
async def get_mapped_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    mapped_columns = await find_mapped_columns(set(dataset.columns))
    return mapped_columns


@router.get(
    "/datetime",
    response_model=DateTimeColumns,
    response_model_exclude_none=True,
)
async def get_datetime_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    datetime_columns = await find_datetime_columns(set(dataset.columns))
    return datetime_columns


@router.get(
    "/geography",
    response_model=GeographyColumns,
    response_model_exclude_none=True,
)
async def get_geography_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    geography_columns = await find_geography_columns(set(dataset.columns))
    return geography_columns


@router.get(
    "/unit",
    response_model=UnitColumns,
    response_model_exclude_none=True,
)
async def get_unit_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    unit_columns = await find_unit_columns(set(dataset.columns))
    return unit_columns


@router.get(
    "/note",
    response_model=NoteColumns,
    response_model_exclude_none=True,
)
async def get_note_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    note_columns = await find_note_columns(set(dataset.columns))
    return note_columns


@router.get(
    "/metadata",
    response_model=MetadataColumns,
    response_model_exclude_none=True,
)
async def get_metadata_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    metadata_columns = await find_metadata_columns(set(dataset.columns))
    return metadata_columns


@router.get(
    "/tags",
    response_model=TagsColumns,
    response_model_exclude_none=True,
)
async def get_tags_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    tags_columns = await find_tags_columns(set(dataset.columns))
    return tags_columns


@router.get(
    "/expectations",
    # response_model=ObjectColumns,
    # response_model_exclude_none=True,
    summary="Run expectation on mapped columns",
)
async def execute_column_expectations(
    result_format: ExpectationResultType,
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    expectations = await asyncio.gather(
        datetime_expectation_suite(dataset, result_format),
        geography_expectation_suite(dataset, result_format),
        unit_expectation_suite(dataset, result_format),
        note_expectation_suite(dataset, result_format),
        general_table_expectation_suite(dataset, result_format),
        metadata_expectation_suite(dataset, result_format),
        tags_expectation_suite(dataset, result_format),
    )
    expectations = ChainMap(*expectations)
    return expectations
