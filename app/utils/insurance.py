import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.core.config import APP_DIR, InsuranceCompanySettings, Settings
from app.utils.column_mapping import find_insurance_company_columns
from app.utils.common import modify_values_to_be_in_set, read_pandas_dataset

settings = Settings()
insurance_company_settings = InsuranceCompanySettings()


async def modify_insurance_company_name_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = (
        insurance_company_settings.INSURANCE_COMPANY_NAME_EXPECTATION
    )

    insurance_company_names_dataset = await read_pandas_dataset(
        APP_DIR / "core" / "insurance_companies.csv"
    )
    insurance_company_names_list = insurance_company_names_dataset[
        "insurance_companies"
    ].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": insurance_company_names_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def insurance_company_name_expectation_suite(dataset, result_format):
    results = {}
    insurance_company_name_columns = await find_insurance_company_columns(
        set(dataset.columns)
    )
    for each_column in insurance_company_name_columns["insurance_name"]:
        expectation_suite = (
            await modify_insurance_company_name_expectation_suite(
                each_column, result_format
            )
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
