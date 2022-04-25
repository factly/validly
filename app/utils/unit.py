from pathlib import Path

import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.core.config import UnitSettings
from app.utils.column_mapping import find_unit_columns
from app.utils.common import modify_values_to_match_regex_list

PROJECT_DIR = Path(__file__).resolve().parents[2]

unit_settings = UnitSettings()


async def unit_proper_format(dataset, result_format):
    results = {}
    # get all unit columns present inside datasets
    unit_columns = await find_unit_columns(set(dataset.columns))
    # get all those columns corresponding to unit
    for each_column in unit_columns["unit"]:
        expectation = dataset.expect_column_values_to_match_regex_list(
            column=each_column,
            regex_list=unit_settings.UNIT_PATTERN,
            match_on="any",
            result_format=result_format,
            include_config=True,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results


async def modify_unit_expectation_suite(column_name: str, result_format: str):
    default_expectation_suite = unit_settings.UNIT_EXPECTATION

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


async def unit_expectation_suite(dataset, result_format):
    results = {}
    unit_columns = await find_unit_columns(set(dataset.columns))
    for each_column in unit_columns["unit"]:
        expectation_suite = await modify_unit_expectation_suite(
            each_column, result_format
        )
        # convert pandas dataset to great_expectations dataset
        ge_pandas_dataset = ge.from_pandas(
            dataset, expectation_suite=expectation_suite
        )
        results[each_column] = ge_pandas_dataset.validate()

    return jsonable_encoder(results)
