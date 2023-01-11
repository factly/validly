from __future__ import annotations

from typing import List

from pydantic import BaseModel


class DateTimeColumns(BaseModel):
    non_calendar_year: List[str]
    calender_year: List[str]
    quarter: List[str]
    month: List[str]
    date: List[str]


class GeographyColumns(BaseModel):
    country: List[str]
    state: List[str]


class UnitColumns(BaseModel):
    unit: List[str]


class NoteColumns(BaseModel):
    note: List[str]


class MetadataColumns(BaseModel):
    sector: List[str]
    organization: List[str]
    short_form: List[str]


class TagsColumns(BaseModel):
    tags: List[str]


class AllMappedColumns(BaseModel):
    non_calendar_year: List[str]
    calender_year: List[str]
    quarter: List[str]
    month: List[str]
    date: List[str]
    country: List[str]
    state: List[str]
    unit: List[str]
    note: List[str]
    tags: List[str]
    unmapped: List[str]


class ObjectColumns(BaseModel):
    object_columns: List[str]
