import asyncio
from typing import List
from urllib.parse import urlparse

import boto3
from fastapi.encoders import jsonable_encoder
from fastapi.logger import logger

from app.core.config import Settings

settings = Settings()


def get_s3_resource(
    s3_access_key: str, s3_secret_key: str, s3_endpoint_url: str, resource: str
):
    try:
        session = boto3.Session(
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        )

        s3_resource = session.resource(resource, endpoint_url=s3_endpoint_url)
    except Exception as e:
        raise ValueError(f"Error connecting to S3: {e}")
    else:
        return s3_resource


async def check_file_metadata(session, file_key: str):

    # get bucket and file path according to the file key
    file_parts = urlparse(file_key)
    bucket, obj_key = file_parts.netloc, file_parts.path.lstrip("/")
    obj = session.ObjectSummary(bucket, obj_key)
    logger.info(f"Checking for key: {file_key}")

    # get the metadata
    try:
        metadata = {"key": file_key, "size": obj.size, "exist": True}
    except Exception as e:
        logger.error(f"Error getting metadata for {file_key} : {e}")
        metadata = {"key": file_key, "size": None, "exist": False}
    finally:
        return jsonable_encoder(metadata)


async def check_files_metadata(session, file_keys: List[str]):
    files_metadata = await asyncio.gather(
        *[check_file_metadata(session, file_key) for file_key in file_keys]
    )
    return files_metadata
