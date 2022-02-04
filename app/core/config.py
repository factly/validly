from pathlib import Path

from pydantic import BaseSettings

APP_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):

    PROJECT_NAME: str = "Validly Service"
    API_V1_STR: str = "/api/v1"
    MODE: str = "development"
    DOCS_URL: str = "/api/docs"
    EXAMPLE_URL: str = "https://storage.factly.org/mande/mospi/data/processed/2018/agriculture/mospi_8_3_consolidated.csv"
    EXAMPLE_URL_COUNTRY: str = "https://storage.factly.org/mande/edu-ministry/data/processed/statistics/1_AISHE_report/19_enrolment_foreign/output.csv"
    EXAMPLE_URL_STATE: str = "https://storage.factly.org/mande/edu-ministry/data/processed/statistics/1_AISHE_report/1_universities_count_by_state/output.csv"

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


class GeographySettings(BaseSettings):

    COUNTRY_KEYWORD = "country"
    STATE_KEYWORD = "state"


class UnitSettings(BaseSettings):

    UNIT_KEYWORD = "unit"
    UNIT_PATTERN = [r",?.+?in[^,]+[,]?"]


class NoteSettings(BaseSettings):

    NOTE_KEYWORD = "note"
    NOTE_PATTERN = [r",?.+?:[^,]+[,]?"]
