from pathlib import Path

import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.core.config import TagsSettings
# from app.utils.column_mapping import find_tags_columns
from app.utils.column_mapping import find_metadata_columns
from app.utils.common import modify_values_to_match_regex_list

PROJECT_DIR = Path(__file__).resolve().parents[2]

tags_settings = TagsSettings()


async def modify_tags_expectation_suite(column_name: str, result_format: str):
    default_expectation_suite = tags_settings.TAGS_EXPECTATION

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


async def tags_expectation_suite(dataset, result_format: str):
    """Expectation to check Tag values format

    Expectation is on whether Tag values format follows the expected regex pattern.

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    tags_column = mapped_columns["tags"][0]

    # for each_column in tags_columns["tags"]:
    expectation_suite = await modify_tags_expectation_suite(
        tags_column, result_format
    )
    # convert pandas dataset to great_expectations dataset
    ge_pandas_dataset = ge.from_pandas(
        dataset, expectation_suite=expectation_suite
    )
    validation = ge_pandas_dataset.validate()
    validation_ui_name = (
        validation["results"][0]["expectation_config"]["meta"][
            "expectation_name"
        ]
        + " - "
        + validation["results"][0]["expectation_config"]["_kwargs"][
            "column"
        ]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)
