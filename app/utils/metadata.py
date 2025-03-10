import asyncio
from collections import ChainMap

import great_expectations as ge
from fastapi.encoders import jsonable_encoder

from app.api.api_v1.routers.dictionary import standard_data_values
from app.core.config import MetadataSettings, Settings
from app.utils.column_mapping import find_metadata_columns
from app.utils.common import (
    modify_values_length_to_be_between,
    modify_values_to_be_in_set,
    modify_values_to_match_regex_list,
    read_dataset,
)
from app.utils.general import general_metadata_expectation_suite
from app.utils.tags import tags_expectation_suite
from app.utils.unit import unit_expectation_suite

settings = Settings()
meta_data_setting = MetadataSettings()


async def check_column_order(dataset):
    results = {}
    settings.METADATA_COLUMN_ORDER_STRING.split(",")
    column_order_list = settings.METADATA_COLUMN_ORDER_STRING.split(",")
    validation = dataset.expect_table_columns_to_match_ordered_list(
        column_order_list
    )
    results["Expect Table Columns To Match The Given List"] = validation
    return jsonable_encoder(results)


async def modify_dataset_name_for_factly_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = (
        meta_data_setting.DATASET_NAME_FOR_FACTLY_EXPECTATION
    )
    changed_config = {
        "expect_column_value_lengths_to_be_between": {
            "min_value": 5,
            "max_value": 200,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_length_to_be_between(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def dataset_name_for_factly_expectation_suite(dataset, result_format):
    """Expectation to check description in specific range

    Expectation is on whether description lies in the range of 50 to 5000 characters
    Flag if its outside the range.

    Args:
        dataset (Data-frame): Read metadata csv using Pandas Data-frame
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    sector_column = mapped_columns["dataset_name_for_factly"][0]

    expectation_suite = await modify_dataset_name_for_factly_expectation_suite(
        sector_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_description_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = meta_data_setting.DESCRIPTION_EXPECTATION
    changed_config = {
        "expect_column_value_lengths_to_be_between": {
            "min_value": 50,
            "max_value": 5000,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_length_to_be_between(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def description_expectation_suite(dataset, result_format):
    """Expectation to check description in specific range

    Expectation is on whether description lies in the range of 50 to 5000 characters
    Flag if its outside the range.

    Args:
        dataset (Data-frame): Read metadata csv using Pandas Data-frame
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    sector_column = mapped_columns["description"][0]

    expectation_suite = await modify_description_expectation_suite(
        sector_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_sector_expectation_suite(
    column_name: str, result_format: str
):
    """
    Summary: Modify the default sector expectation suite using
        sector.csv file in app.core

    Args:
        column_name (str): _description_
        result_format (str): _description_

    Returns:
        _type_: _description_
    """

    default_expectation_suite = meta_data_setting.SECTOR_EXPECTATION

    sector_dataset = standard_data_values[["sectors"]].dropna().copy()
    sector_list = sector_dataset["sectors"].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": sector_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def sector_expectation_suite(dataset, result_format):
    """Expectation to check if Sector values are in sector.csv

    Expectation is on whether every value present in sector column of metadata
    csv is in sector.csv file or not

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    sector_column = mapped_columns["sector"][0]

    expectation_suite = await modify_sector_expectation_suite(
        sector_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_organization_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = meta_data_setting.ORGANIZATION_EXPECTATION

    organization_dataset = (
        standard_data_values[["organisation"]].dropna().copy()
    )
    organization_list = organization_dataset["organisation"].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": organization_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def organization_expectation_suite(dataset, result_format):
    """Expectation to check if Organization values are in organization.csv

    Expectation is on whether every value present in organization column of metadata
    csv is in organization.csv file or not

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    organization_column = mapped_columns["organization"][0]

    expectation_suite = await modify_organization_expectation_suite(
        organization_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation
    return jsonable_encoder(results)


async def modify_short_form_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = meta_data_setting.SHORT_FORM_EXPECTATION

    # NOTE: Modify the short_form_expectation_suite to use short_form
    short_form_dataset = {"short_form": ""}
    short_form_list = short_form_dataset["short_form"]

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": short_form_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def modify_frequency_of_update_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = (
        meta_data_setting.FREQUENCY_OF_UPDATE_EXPECTATION
    )

    frequency_of_update_dataset = (
        standard_data_values[["frequency_of_update"]].dropna().copy()
    )
    frequency_of_update_list = frequency_of_update_dataset[
        "frequency_of_update"
    ].tolist()

    changed_config = {
        "expect_column_values_to_be_in_set": {
            "value_set": frequency_of_update_list,
            "column": column_name,
            "result_format": result_format,
        }
    }
    changed_expectation_suite = await modify_values_to_be_in_set(
        changed_config, default_expectation_suite
    )
    return changed_expectation_suite


async def frequency_of_update_expectation_suite(dataset, result_format):
    """Expectation to check if Frequency of Update values are in frequency_of_update.csv

    Expectation is on whether every value present in Frequency of update column of metadata
    csv is in frequency_of_update.csv file or not

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    frequency_of_update_column = mapped_columns["frequency_of_update"][0]

    expectation_suite = await modify_frequency_of_update_expectation_suite(
        frequency_of_update_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_file_path_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = meta_data_setting.FILE_PATH_EXPECTATION

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


async def file_path_expectation_suite(dataset, result_format: str):
    """Expectation to check File Path values format

    Expectation is on whether file path values format follows the expected regex pattern.

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    file_path_column = mapped_columns["file_path"][0]

    # for each_column in tags_columns["tags"]:
    expectation_suite = await modify_file_path_expectation_suite(
        file_path_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def modify_data_next_update_expectation_suite(
    column_name: str, result_format: str
):
    default_expectation_suite = meta_data_setting.DATA_NEXT_UPDATE_EXPECTATION

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


async def data_next_update_expectation_suite(dataset, result_format: str):
    """Expectation to check Data Next Update values format

    Expectation is on whether Data Next Update values format follows the expected regex pattern.

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    results = {}
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    data_next_update_column = mapped_columns["data_next_update"][0]

    # for each_column in tags_columns["tags"]:
    expectation_suite = await modify_data_next_update_expectation_suite(
        data_next_update_column, result_format
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
        + validation["results"][0]["expectation_config"]["_kwargs"]["column"]
    )
    results[validation_ui_name] = validation

    return jsonable_encoder(results)


async def time_saved_in_hours_expectation_suite(dataset, result_format):
    """Expectation to check Time saved in Hours values in specific range

    Expectation is on whether Time saved in Hours values lies in the range of 2 to 6 hours
    Flag if its outside the range.

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations
    """
    mapped_columns = await find_metadata_columns(set(dataset.columns))
    time_saved_in_hours_column = mapped_columns["time_saved_in_hours"][0]
    expectation_name = meta_data_setting.TIME_SAVED_IN_HOURS_NAME.format(
        column=time_saved_in_hours_column
    )
    expectation_error_message = meta_data_setting.TIME_SAVED_IN_HOURS_MSG

    ge_pandas_dataset = ge.from_pandas(dataset)

    expectation = ge_pandas_dataset.expect_column_values_to_be_between(
        column=time_saved_in_hours_column,
        min_value=2,
        max_value=6,
        catch_exceptions=True,
        result_format=result_format,
    )

    expectation_dict = expectation.to_json_dict()
    expectation_dict["expectation_config"]["meta"] = {
        "cleaning_pdf_link": settings.DATA_CLEANING_GUIDE_LINK,
        "expectation_name": expectation_name,
        "expectation_error_message": expectation_error_message,
    }
    response = {
        expectation_dict["expectation_config"]["meta"][
            "expectation_name"
        ]: expectation_dict
    }
    return response


async def metadata_expectation_suite(
    dataset, result_format, dataset_name: str
):
    """Chaining all expectation suites

    Chianmap every expectation suite and running all of them asynchronously

    Args:
        dataset (Dataframe): Read metadata csv using Pandas Dataframe
        result_format (str): SUMMARY

    Returns:
        Dict: Dictionary of Expectations of all Expectation suites running
    """
    if isinstance(dataset, str):
        dataset = await read_dataset(dataset)
    # Dataset modification for sector expectation suite
    dataset_sector = dataset.copy()
    # explode the dataset based on sector column
    dataset_sector["sectors"] = dataset_sector["sectors"].apply(
        lambda x: x.split(",")
    )
    dataset_sector = dataset_sector.explode("sectors").reset_index(drop=True)
    dataset_sector["sectors"] = dataset_sector["sectors"].str.strip()

    expectations = await asyncio.gather(
        check_column_order(dataset),
        sector_expectation_suite(dataset_sector, result_format),
        organization_expectation_suite(dataset, result_format),
        description_expectation_suite(dataset, result_format),
        dataset_name_for_factly_expectation_suite(dataset, result_format),
        unit_expectation_suite(dataset, result_format),
        tags_expectation_suite(dataset, result_format),
        frequency_of_update_expectation_suite(dataset, result_format),
        file_path_expectation_suite(dataset, result_format),
        data_next_update_expectation_suite(dataset, result_format),
        time_saved_in_hours_expectation_suite(dataset, result_format),
        general_metadata_expectation_suite(dataset, result_format),
    )
    expectations = ChainMap(*expectations)
    return {dataset_name: jsonable_encoder(expectations)}
