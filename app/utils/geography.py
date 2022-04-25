import asyncio
from collections import ChainMap

import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.core.config import APP_DIR, GeographySettings, Settings
from app.utils.column_mapping import find_geography_columns
from app.utils.common import (
    modify_values_to_be_in_set,
    read_dataset,
    read_pandas_dataset,
)

settings = Settings()
geograhy_setting = GeographySettings()


async def modify_state_expectation_suite(column_name: str, result_format: str):
    default_expectation_suite = geograhy_setting.STATE_EXPECTATION

    state_dataset = await read_pandas_dataset(APP_DIR / "core" / "state.csv")
    state_list = state_dataset["state"].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": state_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def state_expectation_suite(dataset, result_format):
    results = {}
    geography_columns = await find_geography_columns(set(dataset.columns))
    for each_column in geography_columns["state"]:
        expectation_suite = await modify_state_expectation_suite(
            each_column, result_format
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


async def modify_country_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = geograhy_setting.COUNTRY_EXPECTATION

    country_dataset = await read_pandas_dataset(
        APP_DIR / "core" / "country.csv"
    )
    country_list = country_dataset["country"].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": country_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def country_expectation_suite(dataset, result_format):
    results = {}
    geography_columns = await find_geography_columns(set(dataset.columns))
    for each_column in geography_columns["country"]:
        expectation_suite = await modify_country_expectation_suite(
            each_column, result_format
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


async def geography_expectation_suite(dataset, result_format):
    if isinstance(dataset, str):
        dataset = await read_dataset(dataset)

    expectations = await asyncio.gather(
        country_expectation_suite(dataset, result_format),
        state_expectation_suite(dataset, result_format),
    )
    expectations = ChainMap(*expectations)
    return expectations


async def geography_expectation_suites(s3_files_key, result_type):
    expectation = await asyncio.gather(
        *[
            geography_expectation_suite(
                f"http://{settings.S3_ENDPOINT}/{settings.S3_BUCKET}/{s3_file_key}",
                result_type,
            )
            for s3_file_key in s3_files_key
        ]
    )
    expectation = ChainMap(*expectation)
    return jsonable_encoder(expectation)
