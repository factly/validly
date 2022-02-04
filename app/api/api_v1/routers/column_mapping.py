from fastapi import APIRouter

from app.core.config import DateTimeSettings, Settings
from app.models.column_mapping import (
    AllMappedColumns,
    DateTimeColumns,
    GeographyColumns,
    NoteColumns,
    UnitColumns,
    ObjectColumns
)
from app.utils.column_mapping import (
    find_datetime_columns,
    find_geography_columns,
    find_mapped_columns,
    find_note_columns,
    find_object_columns,
    find_unit_columns,
)
from app.utils.common import read_dataset

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
    "/object",
    response_model=ObjectColumns,
    response_model_exclude_none= True,
    summary= "Provide column with str type values"
)
async def get_object_columns(
    source: str = settings.EXAMPLE_URL,
):
    dataset = await read_dataset(source)
    object_columns = await find_object_columns(dataset)
    return object_columns
