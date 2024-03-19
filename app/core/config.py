import re
from pathlib import Path
from typing import Dict, List

from pydantic import BaseSettings

FILE_PATH = Path(__file__).resolve()
APP_DIR = FILE_PATH.parents[1]
CORE_FOLDER = FILE_PATH.parents[0]


class Settings(BaseSettings):

    PROJECT_NAME: str = "Validly Service"
    API_V1_STR: str = "/api/v1"
    MODE: str = "development"
    DOCS_URL: str = "/api/docs"
    EXAMPLE_FOLDER: str = "/Users/somitragupta/factly/news-room-datasets"
    EXAMPLE_URL: str = (
        "/Users/somitragupta/factly/factly-datasets/projects/rbi/\
data/processed/1_timeseries/5_handbook-of-statistics-on-the-indian-economy/\
hbs-mb-scb-select-aggregates-weekly/output.csv"
    )
    EXAMPLE_URL_COUNTRY: str = """https://storage.factly.org/mande/\
edu-ministry/data/processed/statistics/1_AISHE_report/19_enrolment_foreign/output.csv"""
    EXAMPLE_URL_STATE: str = """https://storage.factly.org/mande/edu-ministry/data/\
processed/statistics/1_AISHE_report/1_universities_count_by_state/output.csv"""
    EXAMPLE_URL_QUARTER: str = """/Users/somitragupta/factly/factly-datasets/\
projects/rbi/data/processed/1_timeseries/9_quarterly-bsr-1-outstanding\
-credit-of-scheduled-commercial-banks/qbsr1-outcredit-acctype/output.csv"""
    EXAMPLE_URL_MONTH: str = """/Users/somitragupta/factly/factly-datasets/\
projects/rbi/data/processed/1_timeseries/5_handbook-of-statistics-\
on-the-indian-economy/hbs-es-exhange-rate-inr-high-low-monthly/output.csv"""
    EXAMPLE_URL_DATE: str = """/Users/somitragupta/factly/factly-datasets\
/projects/rbi/data/processed/1_timeseries/5_handbook-of-statistics-on\
-the-indian-economy/hbs-mb-scb-select-aggregates-weekly/output.csv"""

    # CORS PARAMS
    CORS_ORIGINS: list = ["*"]
    CORS_METHODS: list = ["*"]
    CORS_HEADERS: list = ["*"]

    # S3 (MINIO) PARAMS
    S3_ENDPOINT: str = ...
    S3_BUCKET: str = ...
    S3_KEY: str = ...
    S3_SECRET: str = ...
    S3_SECURE: bool = ...
    S3_REGION: str = ...

    DATA_CLEANING_GUIDE_LINK: str = "https://wp.me/ad1WQ9-dvg"

    # Source Dataset Storage Params
    S3_SOURCE_ACCESS_KEY: str = ...
    S3_SOURCE_SECRET_KEY: str = ...
    S3_SOURCE_ENDPOINT_URL: str = ...
    S3_SOURCE_RESOURCE: str = "s3"

    # GOOGLE CONFIGURATION
    SERVICE_ACCOUNT_CONF: Dict[str, str] = {"<CHANGE_ME>": "<CHANGE_ME>"}
    GSHEET_SCOPES: List[str] = ["https://www.googleapis.com/auth/spreadsheets"]

    class Config:
        env_file = ".env"


class DateTimeSettings(BaseSettings):

    CALENDAR_YEAR_KEYWORD = "year"
    FISCAL_YEAR_KEYWORD = "fiscal_year"
    ACADEMIC_YEAR_KEYWORD = "academic_year"
    QUARTER_KEYWORD = "quarter"
    MONTH_KEYWORD = "month"
    DATE_KEYWORD = "date"
    CALENDAR_YEAR_PATTERN = "%Y"
    NON_CALENDAR_YEAR_PATTERN = r"^\d{4}-\d{2}$"
    QUARTER_FORMAT = r"Q[1-4]"
    MONTH_FORMAT = "%B"
    DATE_FORMAT = "%d-%m-%Y"
    CALENDAR_YEAR_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "calendar_year",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "year",
                    "regex": "^\\d{4}$",
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Calender Year Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectaion_error_message": "Calendar Year should be represented as YYYY",
                },
            }
        ],
    }
    NON_CALENDAR_YEAR_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "non_calendar_year",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "fiscal_year",
                    "regex": "^\\d{4}-\\d{2}$",
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Non Calender Year Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectaion_error_message": "Non-Calendar Year such as a financial year or an academic year should be represented as YYYY-YY",
                },
            }
        ],
    }
    QUARTER_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "quarter_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "quarter",
                    "regex": "^Q[1-4]$",
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Quarter Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectaion_error_message": "Quarter should be represented as Q1,Q2,Q3,Q4",
                },
            }
        ],
    }
    MONTH_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "month_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_strftime_format",
                "kwargs": {
                    "column": "month",
                    "strftime_format": "%B",
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Month Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectaion_error_message": "Month should be complete name with Title case such as January",
                },
            }
        ],
    }
    DATE_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "date_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_strftime_format",
                "kwargs": {
                    "column": "date",
                    "strftime_format": "%d-%m-%Y",
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Date Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Date should be represented as DD-MM-YYYY",
                },
            }
        ],
    }


class GeographySettings(BaseSettings):

    COUNTRY_KEYWORD = "country"
    STATE_KEYWORD = "state"
    CITY_KEYWORD = "city"
    COUNTRY_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "country_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "country",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Country Name",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Country Name should be from the Data Dictionary",
                },
            }
        ],
    }
    STATE_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "date_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "state",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "State Name",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "State Name should be from the Data Dictionary",
                },
            }
        ],
    }
    CITY_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "city_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "city",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "City Name",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "City Name should be from the Data Dictionary",
                },
            }
        ],
    }


class AirlineSettings(BaseSettings):

    AIRLINE_NAME_KEYWORD: str = "airline"
    AIRLINE_NAME_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "airline_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "airline",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Airline Name",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Airline Name should be from the Data Dictionary",
                },
            }
        ],
    }


class UnitSettings(BaseSettings):

    UNIT_KEYWORD = "unit"
    # UNIT_PATTERN = [r",?.+?in[^,]+[,]?"]
    UNIT_PATTERN = [r"(\s?([a-z_0-9]+\sin\s[\w -]+)+[,]?)"]
    UNIT_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "unit_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex_list",
                "kwargs": {
                    "column": "unit",
                    # "regex_list": [",?.+?in[^,]+[,]?"],
                    "regex_list": [r"(\s?([a-z_0-9]+\sin\s[\w -]+)+[,]?)"],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Unit Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Unit should be in proper format as 'column_name in unit'",
                },
            }
        ],
    }


class NoteSettings(BaseSettings):

    NOTE_KEYWORD = "note"
    NOTE_PATTERN = [r",?.+?:[^,]+[,]?"]
    NOTE_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "unit_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex_list",
                "kwargs": {
                    "column": "note",
                    "regex_list": [",?.+?:[^,]+[,]?"],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Note Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Note should be in proper format as 'column_name:note'",
                },
            }
        ],
    }


class CustomExpectationsSettings(BaseSettings):

    NULL_DATETIME_VALUE_NAME: str = "Null date values Flag - {column}"
    NULL_DATETIME_VALUE_MSG: str = (
        "Null values should not be permitted for datetime values"
    )

    NUMERIC_COLUMNS_TYPES = ["float64", "int64"]
    NUMERIC_VALUES_PATTERN = re.compile(r"^-?\d+(\.\d{1,2})?$")
    NUMERIC_EXPECTATION_NAME: str = (
        "Numeric values in specific pattern - {column}"
    )
    NUMERIC_EXPECTATION_ERR_MSG: str = (
        "Numeric values should be in proper format both integer and float(roundoff to two decimal places)"
    )

    NEGATIVE_NUMERIC_VALUES_PATTERN = re.compile(r"^-\d+(\.\d{1,})?$")
    NEGATIVE_NUMERIC_EXPECTATION_NAME: str = (
        "Negative Numeric values Flag - {column}"
    )
    NEGATIVE_NUMERIC_EXPECTATION_ERR_MSG: str = (
        "Flag Numeric values that are negative"
    )

    COLUMN_NAMES_PATTERN = re.compile(r"^[a-z]+(?:_[a-z]+)*$")
    COLUMN_NAMES_EXPECTATION_NAME: str = "Column names in specific pattern"
    COLUMN_NAMES_EXPECTATION_ERR_MSG: str = (
        "Column names should be in lower case and separated by underscore - {column}"
    )

    TRAIL_OR_LEAD_WHITESPACE_PATTERN = re.compile(r"^\s+.*|.*\s+$")
    LEADING_TRAILING_WHITE_SPACE_EXPECTATION_NAME: str = (
        "No leading or trailing whitespaces"
    )
    LEADING_TRAILING_WHITESPACE_EXPECTATION_ERR_MSG: str = (
        "There should be no leading and trailing white spaces"
    )

    MULTIPLE_BLANKSPACE_PATTERN = re.compile(r".+?[\w]+.+?(\s{2,}).+[\w]+.+")
    MULTIPLE_SPACE_EXPECTATION_NAME: str = "No multiple whitespaces"
    MULTIPLE_SPACE_EXPECTATION_ERR_MSG: str = (
        "There should be no multiple whitespaces"
    )

    SPECIAL_CHARACTER_PATTERN = re.compile(r".*?[^\)\w\s\.]$")
    SPECIAL_CHARACTER_EXPECTATION_NAME: str = (
        "No special characters in Columns"
    )
    SPECIAL_CHARACTER_EXPECTATION_ERR_MSG: str = (
        "There should be no special character in the category name and measured value, like Telangana** , and any additional information  should be captured in notes instead of using a special character"
    )

    BRACKET_PATTERN = re.compile(r".*([\[\(].+?[\)\]]).*")
    BRACKETS_EXPECTATION_NAME: str = "No unnecessary brackets in Categories"
    BRACKETS_EXPECTATION_ERR_MSG: str = (
        "Categories should not have unnecessary brackets"
    )

    DUPLICATE_ROWS_EXPECTATION_NAME: str = "No Duplicate Rows"
    DUPLICATE_ROWS_EXPECTATION_ERR_MSG: str = (
        "There should be no duplicate rows in the dataset"
    )

    UNIT_NOTE_COLUMNS = ("unit", "note")
    NULL_NUMERIC_VALUE_NAME: str = "Null numeric values Flag - {column}"
    NULL_NUMERIC_VALUE_MSG: str = "Null Numeric values should be cross-checked"

    NULL_CATEGORICAL_VALUE_NAME: str = "Null values Flag - {column}"
    NULL_CATEGORICAL_VALUE_MSG: str = (
        "Null values should not present in this column"
    )

    MINIMUM_DATASET_OBSERVATION_THRESH: int = 10
    OBSERVATIONS_MORE_THAN_THRESH_NAME: str = "Minimum required observation"
    OBSERVATIONS_MORE_THAN_THRESH_MSG: str = (
        "Generally the datasets must be more a threshold number of observation ({thresh})"
    )


class MetadataSettings(BaseSettings):

    SECTOR_KEYWORD = "sector"
    ORGANIZATION_KEYWORD = "organization"
    SHORT_FORM_KEYWORD = "short_form"

    DESCRIPTION_KEYWORD = "description"
    DATASET_NAME_FOR_FACTLY_KEYWORD = "dataset_name_for_factly"
    GRANULARITY_KEYWORD = "granularity"
    TIME_SAVED_IN_HOURS_KEYWORD = "time_saved_in_hours"
    FILE_PATH_KEYWORD = "file_path"
    FREQUENCY_OF_UPDATE_KEYWORD = "frequency_of_update"
    SOURCE_LINK_KEYWORD = "source_link"
    # ARCHIVE_KEYWORD = "archive"
    TEMPORAL_COVERAGE_KEYWORD = "temporal_coverage"
    SPACIAL_COVERAGE_KEYWORD = "spatial_coverage"
    VARIABLE_MEASURED_KEYWORD = "variable_measured"
    DATA_NEXT_UPDATE_KEYWORD = "data_next_update"
    SOURCE_KEYWORD = "source"
    SECTOR_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "sector_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "sector",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Sector Name in set of values",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Sector Name should be from the Data Dictionary",
                },
            }
        ],
    }

    ORGANIZATION_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "organization_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "organization",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Organization Name in set of values",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Organization Name should be from the Data Dictionary",
                },
            }
        ],
    }

    SHORT_FORM_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "short_form_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "short_form",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Short Form in set of values",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Short Form should be from the Data Dictionary",
                },
            }
        ],
    }

    FREQUENCY_OF_UPDATE_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "frequency_of_update_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "frequency_of_update",
                    "value_set": [],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Frequency of Update in set of values",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Frequency of Update should be from the Data Dictionary",
                },
            }
        ],
    }

    FILE_PATH_PATTERN = [r"(s3:\/\/[a-z0-9\/._]*)"]
    FILE_PATH_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "file_path_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex_list",
                "kwargs": {
                    "column": "file_path",
                    # "regex_list": [",?.+?in[^,]+[,]?"],
                    "regex_list": [r"(s3:\/\/[a-z0-9\/._]*)"],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "File Path in proper format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "File Path should be of specific pattern",
                },
            }
        ],
    }

    DATA_NEXT_UPDATE_PATTERN = [r"(\d{1,2}-\d{1,2}-\d{4})"]
    DATA_NEXT_UPDATE_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "data_next_update_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex_list",
                "kwargs": {
                    "column": "data_next_update",
                    # "regex_list": [",?.+?in[^,]+[,]?"],
                    "regex_list": [r"(\d{1,2}-\d{1,2}-\d{4})"],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Data Next Update in proper format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Data Next Update should be of specific pattern",
                },
            }
        ],
    }

    DESCRIPTION_NAME: str = "Description"
    DESCRIPTION_ERROR_MSG: str = (
        "Description should be in the range of 50 to 5000"
    )

    TIME_SAVED_IN_HOURS_NAME: str = "Null values in columns - {column}"
    TIME_SAVED_IN_HOURS_MSG: str = (
        "Null values should not present in these columns"
    )
    # TIME_SAVED_IN_HOURS_EXPECTATION = {
    #     "data_asset_type": None,
    #     "expectation_suite_name": "time_saved_in_hours_expectation_suite",
    #     "expectations": [
    #         {
    #             "expectation_type": "expect_column_values_to_be_in_set",
    #             "kwargs": {
    #                 "column": "time_saved_in_hours",
    #                 "value_set": [],
    #                 "result_format": "SUMMARY",
    #             },
    #             "meta": {
    #                 "expectation_name": "Time Saved In Hours",
    #                 "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
    #                 "expectation_error_message": "Time Saved in Hours should be from the range of 2 to 6 hours",
    #             },
    #         }
    #     ],
    # }


class TagsSettings(BaseSettings):

    TAGS_KEYWORD = "tags"
    TAGS_PATTERN = [r"([a-z 0-9,]*)"]
    TAGS_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "tags_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex_list",
                "kwargs": {
                    "column": "tags",
                    # "regex_list": [",?.+?in[^,]+[,]?"],
                    "regex_list": [r"([a-z 0-9,]*)"],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Tags in proper format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Tags should be in proper format as 'valueone, value-two'",
                },
            }
        ],
    }
