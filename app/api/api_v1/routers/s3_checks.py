from typing import List, Union

from fastapi import APIRouter, Form, HTTPException, status
from fastapi.logger import logger

from app.core.config import Settings
from app.models.s3_checks import s3FileCheck, s3FileCheckResponse
from app.utils.s3_checks import get_s3_resource

s3_router = router = APIRouter()
settings = Settings()


@router.post("/files/key/", response_model=s3FileCheck)
async def check_if_file_exist_in_bucket(
    file_key: str = Form(...),
    bucket: str = Form(...),
    s3_access_key: Union[str, None] = Form(
        None,
        description="S3 access key. If None then take the default one from env variables",
    ),
    s3_secret_key: Union[str, None] = Form(
        None,
        description="S3 secret key. If None then take the default one from env variables",
    ),
    s3_endpoint_url: Union[str, None] = Form(
        None,
        description="S3 endpoint url key. If None then take the default one from env variables",
    ),
    resource: Union[str, None] = Form(
        None,
        description="S3 resource. If None then take the default one from env variables",
    ),
):
    """
    Check if file exist in bucket
    """
    s3_access_key = (
        settings.S3_SOURCE_ACCESS_KEY
        if s3_access_key is None
        else s3_access_key
    )
    s3_secret_key = (
        settings.S3_SOURCE_SECRET_KEY
        if s3_secret_key is None
        else s3_secret_key
    )
    s3_endpoint_url = (
        settings.S3_SOURCE_ENDPOINT_URL
        if s3_endpoint_url is None
        else s3_endpoint_url
    )
    resource = settings.S3_SOURCE_RESOURCE if resource is None else resource
    try:
        s3_resource = get_s3_resource(
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint_url=s3_endpoint_url,
            resource=resource,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error connecting to S3: {e}",
        )
    else:
        is_exist = False
        bucket = s3_resource.Bucket(bucket)
        if bucket in s3_resource.buckets.all():
            objs = list(bucket.objects.filter(Prefix=file_key))
            logger.info(f"Checking if file exist in bucket: {file_key}")
            if len(objs) == 1 and objs[0].key == file_key:
                is_exist = True
            return {
                "file_key": file_key,
                "is_exists": is_exist,
            }
        else:
            logger.info(f"Bucket {bucket} does not exist")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Bucket does not exist",
            )


@router.post("/files", response_model=s3FileCheckResponse)
async def check_if_files_exist_in_bucket(
    file_keys: List[str] = Form(...),
    bucket: str = Form(...),
    s3_access_key: Union[str, None] = Form(
        None,
        description="S3 access key. If None then take the default one from env variables",
    ),
    s3_secret_key: Union[str, None] = Form(
        None,
        description="S3 secret key. If None then take the default one from env variables",
    ),
    s3_endpoint_url: Union[str, None] = Form(
        None,
        description="S3 endpoint url . If None then take the default one from env variables",
    ),
    resource: Union[str, None] = Form(
        None,
        description="S3 resource. If None then take the default one from env variables",
    ),
):
    s3_access_key = (
        settings.S3_SOURCE_ACCESS_KEY
        if s3_access_key is None
        else s3_access_key
    )
    s3_secret_key = (
        settings.S3_SOURCE_SECRET_KEY
        if s3_secret_key is None
        else s3_secret_key
    )
    s3_endpoint_url = (
        settings.S3_SOURCE_ENDPOINT_URL
        if s3_endpoint_url is None
        else s3_endpoint_url
    )
    resource = settings.S3_SOURCE_RESOURCE if resource is None else resource
    try:
        s3_resource = get_s3_resource(
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_endpoint_url=s3_endpoint_url,
            resource=resource,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error connecting to S3: {e}",
        )
    else:
        # TODO : Check how Form is combining all the strings inside list
        if len(file_keys) == 1:
            file_keys = [key for key in file_keys[0].split(",")]
        file_keys_set = set(file_keys)

        bucket = s3_resource.Bucket(bucket)

        if bucket in s3_resource.buckets.all():
            logger.debug("Bucket exists: {}".format(bucket))

            # objs = {obj.key for obj in bucket.objects.all()}
            all_s3_objects = [
                {"key": obj.key, "size": obj.size}
                for obj in bucket.objects.all()
            ]

            existing_keys = file_keys_set.intersection(
                [obj["key"] for obj in all_s3_objects]
            )
            non_existing_keys = set(file_keys).difference(existing_keys)
            existing_file_details = [
                obj for obj in all_s3_objects if obj["key"] in existing_keys
            ]

            return {
                "exists": existing_file_details,
                "non_exists": list(non_existing_keys),
            }

        else:
            logger.info(f"Bucket {bucket} does not exist")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Bucket does not exist",
            )
