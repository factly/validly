from pathlib import Path

import great_expectations as ge

from app.core.config import NoteSettings
from app.utils.column_mapping import find_note_columns
from app.utils.common import modify_values_to_match_regex_list

PROJECT_DIR = Path(__file__).resolve().parents[2]

note_settings = NoteSettings()


async def modify_note_expectation_suite(column_name: str, result_format: str):
    default_expectation_suite = note_settings.NOTE_EXPECTATION

    changed_config = {
        "expect_column_values_to_match_regex_list": {
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_match_regex_list(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def note_expectation_suite(dataset, result_format):
    results = {}
    note_columns = await find_note_columns(set(dataset.columns))
    for each_column in note_columns["note"]:
        expectation_suite = await modify_note_expectation_suite(
            each_column, result_format
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        results[each_column] = ge_pandas_dataset.validate()

    return results
