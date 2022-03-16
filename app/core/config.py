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
    EXAMPLE_URL: str = """https://storage.factly.org/mande/mospi/\
data/processed/2018/agriculture/mospi_8_3_consolidated.csv"""
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
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
                "meta": {},
            }
        ],
    }


class CustomExpectationsSettings(BaseSettings):

    TRAIL_OR_LEAD_WHITESPACE_PATTERN = re.compile(r"^\s+.*|.*\s+$")
    MULTIPLE_BLANKSPACE_PATTERN = re.compile(r".+?[\w]+.+?(\s{2,}).+[\w]+.+")
    SPECIAL_CHARACTER_PATTERN = re.compile(r".*?[^\)\w\s\.]$")
    BRACKET_PATTERN = re.compile(r".*([\[\(].+?[\)\]]).*")
