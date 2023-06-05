import logging
import re
from io import BytesIO
from typing import Union

import great_expectations as ge
import pandas as pd
from charset_normalizer import from_bytes
from fastapi.logger import logger

from app.core.config import APP_DIR, GeographySettings

logging.basicConfig(level=logging.INFO)
geographic_settings = GeographySettings()


async def get_file(session, url):
    async with session.get(url) as response:
        return await response.read()


def get_encoding(obj):
    encoding = from_bytes(obj).best().encoding
    return encoding


async def read_dataset(
    source: str,
    s3_client=None,
    bucket_name: Union[str, None] = None,
    is_file: bool = False,
    **kwargs,
) -> ge.dataset.pandas_dataset.PandasDataset:
    if s3_client:
        # dataset should be downloaded from s3 storage
        try:
            response = s3_client.get_object(bucket_name, source)
            dataset = ge.read_csv(BytesIO(response.data))
            logger.info(f"Dataset read from : {source}")
        except UnicodeDecodeError:
            encoding = get_encoding(obj=response.data)
            dataset = ge.read_csv(BytesIO(response.data), encoding=encoding)
            logger.info(f"Dataset read from : {source} with non-utf8 encoding")
        except Exception as e:
            logger.info(f"Error reading Dataset from : {source}: {e}")
        finally:
            response.close()
            response.release_conn()
    elif is_file:
        try:
            file = source.file.read()
            dataset = ge.read_csv(BytesIO(file))
            logger.info(f"Dataset read from : {source.filename}")
        except UnicodeDecodeError:
            encoding = get_encoding(obj=file)
            dataset = ge.read_csv(BytesIO(file), encoding=encoding)
            logger.info(
                f"Dataset read from : {source.filename} with non-utf8 encoding"
            )
        except Exception as e:
            logger.info(f"Error reading Dataset from : {source.filename}: {e}")
    else:
        session = kwargs.pop("session")
        try:
            dataset = ge.read_csv(source, **kwargs)
            logger.info(f"Dataset read from : {source}")
        except UnicodeDecodeError:
            file = await get_file(url=source, session=session)
            encoding = get_encoding(obj=file)
            dataset = ge.read_csv(BytesIO(file), encoding=encoding)
            logger.info(f"Dataset read from : {source} with non-utf8 encoding")
        except Exception as e:
            logger.info(f"Error reading Dataset from : {source}: {e}")
    return dataset


async def read_pandas_dataset(source: str, **kwargs):
    dataset = pd.read_csv(source, **kwargs)
    return dataset


async def load_values_to_be_in_set(domain: str):
    # this function is used to load csv files, consisting values
    # for states or country that are required to be in specific set
    set_values_file = APP_DIR / "core" / f"{domain}.csv"
    set_values = pd.read_csv(set_values_file)[f"{domain}"].unique()
    return set_values


async def modify_column_names_to_expectation_suite(
    expectation_suite: dict, expectation_config: dict
):
    modified_expectations = []
    for expectation in expectation_suite["expectations"]:
        expectation["kwargs"].update(expectation_config)
        modified_expectations.append(expectation)
    expectation_suite["expectations"] = modified_expectations
    return expectation_suite


async def modify_default_expectation_suite(
    expectation_suite: dict, expectation_config: dict
):
    modified_expectation = []
    for expectation in expectation_suite["expectations"]:
        if expectation["expectation_type"] in expectation_config.keys():
            expectation["kwargs"].update(
                expectation_config[expectation["expectation_type"]]
            )
        modified_expectation.append(expectation)
    expectation_suite["expectations"] = modified_expectation
    return expectation_suite


async def modify_values_to_be_in_set(
    changed_config: dict, default_config: str
):
    for expectation in default_config["expectations"]:
        if (
            expectation["expectation_type"]
            == "expect_column_values_to_be_in_set"
        ):
            expectation["kwargs"].update(
                changed_config["expect_column_values_to_be_in_set"]
            )
    return default_config


async def modify_values_to_match_regex_list(
    changed_config: dict, default_config: str
):
    for expectation in default_config["expectations"]:
        if (
            expectation["expectation_type"]
            == "expect_column_values_to_match_regex_list"
        ):
            expectation["kwargs"].update(
                changed_config["expect_column_values_to_match_regex_list"]
            )
    return default_config


async def modify_values_to_match_regex(
    changed_config: dict, default_config: str
):
    for expectation in default_config["expectations"]:
        if (
            expectation["expectation_type"]
            == "expect_column_values_to_match_regex"
        ):
            expectation["kwargs"].update(
                changed_config["expect_column_values_to_match_regex"]
            )
    return default_config


async def modify_values_to_match_strftime_format(
    changed_config: dict, default_config: str
):
    for expectation in default_config["expectations"]:
        if (
            expectation["expectation_type"]
            == "expect_column_values_to_match_strftime_format"
        ):
            expectation["kwargs"].update(
                changed_config["expect_column_values_to_match_strftime_format"]
            )
    return default_config


def slugify(text: str):
    text = text.lower()
    text = re.sub(r"[^/.a-z0-9_-]", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text
