import asyncio
import logging
import uuid

from fastapi.logger import logger
from minio import Minio

from app.core.config import Settings
from app.utils.common import slugify

logging.basicConfig(level=logging.INFO)


settings = Settings()


async def _upload_file_to_minio_folder(
    folder_name: str, upload_file_object, bucket_name: str, minio_client
):
    # get the filename
    filename = upload_file_object.filename
    logger.info(f"File name : {filename}")
    try:
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=f"{folder_name}/{slugify(filename)}",
            data=upload_file_object.file,
            length=-1,
            content_type=upload_file_object.content_type,
            part_size=10 * 1024 * 1024,
        )
    except Exception as e:
        logger.error(f"Could not upload {filename} to minio: {e}")
        return False
    else:
        logger.info(f"Uploaded {filename} to minio")
        return True


async def upload_local_file_to_bucket(upload_file_objects):
    # Create client with access and secret key.
    if not isinstance(upload_file_objects, list):
        upload_file_objects = [upload_file_objects]

    try:
        client = Minio(
            endpoint=settings.S3_ENDPOINT,
            access_key=settings.S3_KEY,
            secret_key=settings.S3_SECRET,
            secure=settings.S3_SECURE,
            region=settings.S3_REGION,
        )
        logger.info(
            f"Bucket Exists : {client.bucket_exists(settings.S3_BUCKET)}"
        )
        logger.info("Created Minio Client")
    except Exception as e:
        logger.exception(f"Could not create Minio Client: {e}")

    if not client.bucket_exists(settings.S3_BUCKET):
        client.make_bucket(settings.S3_BUCKET)

    # we will save each transaction of files in separate folders
    folder_name = str(uuid.uuid4())
    logger.info(f"Folder name :{folder_name}")

    await asyncio.gather(
        *[
            _upload_file_to_minio_folder(
                folder_name, upload_file_object, settings.S3_BUCKET, client
            )
            for upload_file_object in upload_file_objects
        ]
    )

    return folder_name


async def get_files_inside_folder(folder_name: str):
    # Create client with access and secret key.
    # provide urls for all the files
    try:
        client = Minio(
            endpoint=settings.S3_ENDPOINT,
            access_key=settings.S3_KEY,
            secret_key=settings.S3_SECRET,
            secure=settings.S3_SECURE,
            region=settings.S3_REGION,
        )
    except Exception as e:
        logger.exception(f"Could not create Minio Client : {e}")

    # try to get all files inside folder
    try:
        file_keys = [
            file.object_name
            for file in client.list_objects(
                settings.S3_BUCKET, prefix=folder_name, recursive=True
            )
        ]
    except Exception as e:
        # raise exception if error happened
        raise Exception(f"Could not get files inside folder: {e}")
    else:
        return file_keys
