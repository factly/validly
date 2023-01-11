import re
from itertools import chain
from typing import Dict

from app.core.config import (
    AirlineSettings,
    DateTimeSettings,
    GeographySettings,
    MetadataSettings,
    NoteSettings,
    TagsSettings,
    UnitSettings,
)

datetime_settings = DateTimeSettings()
geography_settings = GeographySettings()
unit_settings = UnitSettings()
note_settings = NoteSettings()
airline_settings = AirlineSettings()
metadata_settings = MetadataSettings()
tags_settings = TagsSettings()


def extract_pattern_from_columns(
    columns: set[str],
    pattern,
) -> Dict[str, set[str]]:
    """Match regex pattern against columns to extract column names from

    _extended_summary_

    Args:
        columns (set[str]): All column names from the dataframe to extract
        pattern (_type_): Regex pattern for particular columns

    Returns:
        Dict[str, set[str]]: Map column names according to their category types
    """
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


# async def find_tags_columns(columns: set):
#     tags_pattern = re.compile(r"({})".format(tags_settings.TAGS_KEYWORD))
#     tags_column, _ = extract_pattern_from_columns(columns, tags_pattern)
#     return {
#         "tags": tags_column,
#     }


async def find_object_columns(dataset):
    object_columns = list(dataset.select_dtypes(include=["object"]).columns)
    return {"object_columns": object_columns}


async def find_metadata_columns(columns: set):
    sector_pattern = re.compile(
        r".*({}).*".format(metadata_settings.SECTOR_KEYWORD)
    )
    organization_pattern = re.compile(
        r".*({}).*".format(metadata_settings.ORGANIZATION_KEYWORD)
    )
    short_form_pattern = re.compile(
        r".*({}).*".format(metadata_settings.SHORT_FORM_KEYWORD)
    )
    description_pattern = re.compile(
        r".*({}).*".format(metadata_settings.DESCRIPTION_KEYWORD)
    )
    tags_pattern = re.compile(r".*({}).*".format(tags_settings.TAGS_KEYWORD))
    temporal_coverage_pattern = re.compile(
        r".*({}).*".format(metadata_settings.TEMPORAL_COVERAGE_KEYWORD)
    )
    dataset_name_for_factly_pattern = re.compile(
        r".*({}).*".format(metadata_settings.DATASET_NAME_FOR_FACTLY_KEYWORD)
    )
    granularity_pattern = re.compile(
        r".*({}).*".format(metadata_settings.GRANULARITY_KEYWORD)
    )
    time_saved_in_hours_pattern = re.compile(
        r".*({}).*".format(metadata_settings.TIME_SAVED_IN_HOURS_KEYWORD)
    )
    file_path_pattern = re.compile(
        r".*({}).*".format(metadata_settings.FILE_PATH_KEYWORD)
    )
    frequency_of_update_pattern = re.compile(
        r".*({}).*".format(metadata_settings.FREQUENCY_OF_UPDATE_KEYWORD)
    )
    source_link_pattern = re.compile(
        r".*({}).*".format(metadata_settings.SOURCE_LINK_KEYWORD)
    )
    spacial_coverage_pattern = re.compile(
        r".*({}).*".format(metadata_settings.SPACIAL_COVERAGE_KEYWORD)
    )
    variable_measured_pattern = re.compile(
        r".*({}).*".format(metadata_settings.VARIABLE_MEASURED_KEYWORD)
    )
    data_next_update_pattern = re.compile(
        r".*({}).*".format(metadata_settings.DATA_NEXT_UPDATE_KEYWORD)
    )
    source_pattern = re.compile(
        r".*({}).*".format(metadata_settings.SOURCE_KEYWORD)
    )

    sector_column, columns = extract_pattern_from_columns(
        columns, sector_pattern
    )
    organization_column, columns = extract_pattern_from_columns(
        columns, organization_pattern
    )
    short_form_column, columns = extract_pattern_from_columns(
        columns, short_form_pattern
    )
    description_column, columns = extract_pattern_from_columns(
        columns, description_pattern
    )
    tags_column, columns = extract_pattern_from_columns(columns, tags_pattern)
    temporal_coverage_column, columns = extract_pattern_from_columns(
        columns, temporal_coverage_pattern
    )
    dataset_name_for_factly_column, columns = extract_pattern_from_columns(
        columns, dataset_name_for_factly_pattern
    )
    granularity_column, columns = extract_pattern_from_columns(
        columns, granularity_pattern
    )
    time_saved_in_hours_column, columns = extract_pattern_from_columns(
        columns, time_saved_in_hours_pattern
    )
    file_path_column, columns = extract_pattern_from_columns(
        columns, file_path_pattern
    )
    frequency_of_update_column, columns = extract_pattern_from_columns(
        columns, frequency_of_update_pattern
    )
    source_link_column, columns = extract_pattern_from_columns(
        columns, source_link_pattern
    )
    spacial_coverage_column, columns = extract_pattern_from_columns(
        columns, spacial_coverage_pattern
    )
    variable_measured_column, columns = extract_pattern_from_columns(
        columns, variable_measured_pattern
    )
    data_next_update_column, columns = extract_pattern_from_columns(
        columns, data_next_update_pattern
    )
    source_column, columns = extract_pattern_from_columns(
        columns, source_pattern
    )

    return {
        "sector": list(sector_column),
        "organization": list(organization_column),
        "short_form": list(short_form_column),
        "description": list(description_column),
        "tags": list(tags_column),
        "temporal_coverage": list(temporal_coverage_column),
        "dataset_name_for_factly": list(dataset_name_for_factly_column),
        "granularity": list(granularity_column),
        "time_saved_in_hours": list(time_saved_in_hours_column),
        "file_path": list(file_path_column),
        "frequency_of_update": list(frequency_of_update_column),
        "source_link": list(source_link_column),
        # "archive":  list(archive_column),
        "spacial_coverage": list(spacial_coverage_column),
        "variable_measured": list(variable_measured_column),
        "data_next_update": list(data_next_update_column),
        "source": list(source_column),
    }


async def find_mapped_columns(columns):
    datetime_columns = await find_datetime_columns(columns)
    geography_columns = await find_geography_columns(columns)
    unit_columns = await find_unit_columns(columns)
    note_columns = await find_note_columns(columns)
    airline_name_columns = await find_airline_name_columns(columns)
    metadata_columns = await find_metadata_columns(columns)
    mapped_columns = {
        **datetime_columns,
        **geography_columns,
        **unit_columns,
        **note_columns,
        **airline_name_columns,
        **metadata_columns,
    }
    not_mapped_columns = list(
        set(columns).difference(
            list(chain.from_iterable(mapped_columns.values()))
        )
    )
    return {**mapped_columns, "unmapped": not_mapped_columns}
