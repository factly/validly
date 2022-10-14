import re
from itertools import chain

from app.core.config import (
    AirlineSettings,
    DateTimeSettings,
    GeographySettings,
    NoteSettings,
    UnitSettings,
)

datetime_settings = DateTimeSettings()
geography_settings = GeographySettings()
unit_settings = UnitSettings()
note_settings = NoteSettings()
airline_settings = AirlineSettings()


def extract_pattern_from_columns(
    columns: set,
    pattern,
):
    matched_columns = set(filter(pattern.match, columns))
    return matched_columns, columns.difference(matched_columns)


async def find_datetime_columns(columns: set):
    non_cal_year_pattern = re.compile(
        r".*({}|{})".format(
            datetime_settings.FISCAL_YEAR_KEYWORD,
            datetime_settings.ACADEMIC_YEAR_KEYWORD,
        )
    )
    cal_year_pattern = re.compile(
        r".*({})".format(datetime_settings.CALENDAR_YEAR_KEYWORD)
    )
    quarter_pattern = re.compile(
        r".*({})".format(datetime_settings.QUARTER_KEYWORD)
    )
    month_pattern = re.compile(
        r".*({})".format(datetime_settings.MONTH_KEYWORD)
    )
    date_pattern = re.compile(r".*({})".format(datetime_settings.DATE_KEYWORD))

    fiscal_year_columns, columns = extract_pattern_from_columns(
        columns, non_cal_year_pattern
    )
    year_columns, columns = extract_pattern_from_columns(
        columns, cal_year_pattern
    )
    quarter_columns, columns = extract_pattern_from_columns(
        columns, quarter_pattern
    )
    month_columns, columns = extract_pattern_from_columns(
        columns, month_pattern
    )
    date_columns, columns = extract_pattern_from_columns(columns, date_pattern)

    return {
        "non_calendar_year": fiscal_year_columns,
        "calender_year": year_columns,
        "quarter": quarter_columns,
        "month": month_columns,
        "date": date_columns,
    }


async def find_geography_columns(columns: set):
    country_pattern = re.compile(
        r".*({})".format(geography_settings.COUNTRY_KEYWORD)
    )
    state_pattern = re.compile(
        r".*({})".format(geography_settings.STATE_KEYWORD)
    )
    city_pattern = re.compile(
        r".*({})".format(geography_settings.CITY_KEYWORD)
    )

    country_column, columns = extract_pattern_from_columns(
        columns, country_pattern
    )
    state_columns, columns = extract_pattern_from_columns(
        columns, state_pattern
    )
    city_columns, columns = extract_pattern_from_columns(columns, city_pattern)

    return {
        "country": country_column,
        "state": state_columns,
        "city": city_columns,
    }


async def find_airline_name_columns(columns: set):
    airline_name_pattern = re.compile(
        r".*({})".format(airline_settings.AIRLINE_NAME_KEYWORD)
    )
    airline_name, _ = extract_pattern_from_columns(
        columns, airline_name_pattern
    )
    return {"airline_name": airline_name}


async def find_unit_columns(columns: set):
    unit_pattern = re.compile(r"({})".format(unit_settings.UNIT_KEYWORD))
    unit_column, _ = extract_pattern_from_columns(columns, unit_pattern)
    return {
        "unit": unit_column,
    }


async def find_note_columns(columns: set):
    note_pattern = re.compile(r"({})".format(note_settings.NOTE_KEYWORD))
    note_column, _ = extract_pattern_from_columns(columns, note_pattern)
    return {
        "note": note_column,
    }


async def find_object_columns(dataset):
    object_columns = list(dataset.select_dtypes(include=["object"]).columns)
    return {"object_columns": object_columns}


async def find_mapped_columns(columns):
    datetime_columns = await find_datetime_columns(columns)
    geography_columns = await find_geography_columns(columns)
    unit_columns = await find_unit_columns(columns)
    note_columns = await find_note_columns(columns)
    airline_name_columns = await find_airline_name_columns(columns)
    mapped_columns = {
        **datetime_columns,
        **geography_columns,
        **unit_columns,
        **note_columns,
        **airline_name_columns,
    }
    not_mapped_columns = list(
        set(columns).difference(
            list(chain.from_iterable(mapped_columns.values()))
        )
    )
    return {**mapped_columns, "unmapped": not_mapped_columns}
