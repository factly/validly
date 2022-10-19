import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.core.config import APP_DIR, AirlineSettings, Settings
from app.utils.column_mapping import find_airline_name_columns
from app.utils.common import modify_values_to_be_in_set, read_pandas_dataset

settings = Settings()
airline_settings = AirlineSettings()


async def modify_airline_name_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = airline_settings.AIRLINE_NAME_EXPECTATION

    airline_names_dataset = await read_pandas_dataset(
        APP_DIR / "core" / "airline_names.csv"
    )
    airline_names_list = airline_names_dataset["airline_names"].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": airline_names_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def airline_name_expectation_suite(dataset, result_format):
    results = {}
    airline_name_columns = await find_airline_name_columns(
        set(dataset.columns)
    )
    for each_column in airline_name_columns["airline_name"]:
        expectation_suite = await modify_airline_name_expectation_suite(
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
