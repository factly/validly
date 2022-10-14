import asyncio
import logging
from collections import ChainMap

from fastapi.encoders import jsonable_encoder
from minio import Minio

from app.core.config import Settings
from app.utils.airline import airline_name_expectation_suite
from app.utils.common import read_dataset
from app.utils.datetime import datetime_expectation_suite
from app.utils.general import general_table_expectation_suite
from app.utils.geography import geography_expectation_suite
from app.utils.note import note_expectation_suite
from app.utils.unit import unit_expectation_suite

settings = Settings()

logging.basicConfig(level=logging.INFO)


async def dataset_expectation(
    dataset_path, result_type, s3_client, bucket_name
):
    dataset = await read_dataset(dataset_path, s3_client, bucket_name)
    expectation = await asyncio.gather(
        datetime_expectation_suite(dataset, result_type),
        geography_expectation_suite(dataset, result_type),
        airline_name_expectation_suite(dataset, result_type),
        note_expectation_suite(dataset, result_type),
        unit_expectation_suite(dataset, result_type),
        general_table_expectation_suite(dataset, result_type),
    )
    expectation = ChainMap(*expectation)
    return {dataset_path: jsonable_encoder(expectation)}


# async def datasets_expectation(dataset_folder_path, result_type):

#     # Currently dataset_folder_path is in local machine
#     if not Path(dataset_folder_path).exists():
#         raise PathError

#     expectations = await asyncio.gather(
#         *[
#             dataset_expectation(dataset_path, result_type)
#             for dataset_path in Path(dataset_folder_path).glob("**/*.csv")
#         ]
#     )
#     expectations = ChainMap(*expectations)
#     return jsonable_encoder(expectations)


async def datasets_expectation(s3_files_key, result_type):
    client = Minio(
        endpoint=settings.S3_ENDPOINT,
        access_key=settings.S3_KEY,
        secret_key=settings.S3_SECRET,
        secure=settings.S3_SECURE,
        region=settings.S3_REGION,
    )
    expectations = await asyncio.gather(
        *[
            dataset_expectation(
                s3_file_key, result_type, client, settings.S3_BUCKET
            )
            for s3_file_key in s3_files_key
        ]
    )
    expectations = ChainMap(*expectations)
    return jsonable_encoder(expectations)


async def datasets_expectation_from_url(urls, result_type):
    expectations = await asyncio.gather(
        *[
            dataset_expectation(
                url, result_type, s3_client=None, bucket_name=None
            )
            for url in urls
        ]
    )
    expectations = ChainMap(*expectations)
    return jsonable_encoder(expectations)
