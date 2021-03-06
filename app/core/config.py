import re
from pathlib import Path

from pydantic import BaseSettings

APP_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):

    PROJECT_NAME: str = "Validly Service"
    API_V1_STR: str = "/api/v1"
    MODE: str = "development"
    DOCS_URL: str = "/api/docs"
    EXAMPLE_FOLDER: str = "/Users/somitragupta/factly/news-room-datasets"
    EXAMPLE_URL: str = "/Users/somitragupta/factly/factly-datasets/projects/rbi/\
data/processed/1_timeseries/5_handbook-of-statistics-on-the-indian-economy/\
hbs-mb-scb-select-aggregates-weekly/output.csv"
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
    S3_ENDPOINT: str = "localhost:9000"
    S3_BUCKET: str = "validly"
    S3_KEY: str = "minio"
    S3_SECRET: str = "password"
    S3_SECURE: bool = False

    DATA_CLEANING_GUIDE_LINK: str = "https://wp.me/ad1WQ9-dvg"

    # Source Dataset Storage Params
    S3_SOURCE_ACCESS_KEY: str = ...
    S3_SOURCE_SECRET_KEY: str = ...
    S3_SOURCE_ENDPOINT_URL: str = ...
    S3_SOURCE_RESOURCE: str = "s3"

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


class UnitSettings(BaseSettings):

    UNIT_KEYWORD = "unit"
    UNIT_PATTERN = [r",?.+?in[^,]+[,]?"]
    UNIT_EXPECTATION = {
        "data_asset_type": None,
        "expectation_suite_name": "unit_expectation_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_match_regex_list",
                "kwargs": {
                    "column": "unit",
                    "regex_list": [",?.+?in[^,]+[,]?"],
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
                    "column": "unit",
                    "regex_list": [",?.+?:[^,]+[,]?"],
                    "result_format": "SUMMARY",
                },
                "meta": {
                    "expectation_name": "Unit Format",
                    "cleaning_pdf_link": "https://wp.me/ad1WQ9-dvg",
                    "expectation_error_message": "Note should be in proper format as 'column_name:note'",
                },
            }
        ],
    }


class CustomExpectationsSettings(BaseSettings):

    TRAIL_OR_LEAD_WHITESPACE_PATTERN = re.compile(r"^\s+.*|.*\s+$")
    MULTIPLE_BLANKSPACE_PATTERN = re.compile(r".+?[\w]+.+?(\s{2,}).+[\w]+.+")
    SPECIAL_CHARACTER_PATTERN = re.compile(r".*?[^\)\w\s\.]$")
    BRACKET_PATTERN = re.compile(r".*([\[\(].+?[\)\]]).*")

    LEADING_TRAILING_WHITE_SPACE_EXPECTATION_NAME: str = (
        "No leading or traling whitespaces"
    )
    MULTIPLE_SPACE_EXPECTATION_NAME: str = "No multiple whitespaces"
    SPECIAL_CHARACTER_EXPECTATION_NAME: str = (
        "No special characters in Columns"
    )
    BRACKETS_EXPECTATION_NAME: str = "No unnecessary brackets in Categories"
    DUPLICATE_ROWS_EXPECTATION_NAME: str = "No Duplicate Rows"

    LEADING_TRAILING_WHITESPACE_EXPECTATION_ERR_MSG: str = (
        "There should be no leading and trailing white spaces"
    )
    MULTIPLE_SPACE_EXPECTATION_ERR_MSG: str = (
        "There should be no multiple whitespaces"
    )
    SPECIAL_CHARACTER_EXPECTATION_ERR_MSG: str = "There should be no special character in the category name and measured value, like Telangana** , and any additional information  should be captured in notes instead of using a special character"
    BRACKETS_EXPECTATION_ERR_MSG: str = (
        "Categories should not have unnecessary brackets"
    )
    DUPLICATE_ROWS_EXPECTATION_ERR_MSG: str = (
        "There should be no duplicate rows in the dataset"
    )
