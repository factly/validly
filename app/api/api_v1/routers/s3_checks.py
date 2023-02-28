from typing import List, Union

from fastapi import APIRouter, Form, HTTPException, status

from app.core.config import Settings
from app.utils.s3_checks import (
    check_file_metadata,
    check_files_metadata,
    get_s3_resource,
)

s3_router = router = APIRouter()
settings = Settings()


@router.post(
    "/files/key/",
)
async def check_if_file_exist_in_bucket(
    file_key: str = Form(...),
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
        file_metadata = await check_file_metadata(s3_resource, file_key)
        return file_metadata


@router.post(
    "/files",
)
async def check_if_files_exist_in_bucket(
    file_keys: List[str] = Form(...),
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

        files_metadata = await check_files_metadata(
            session=s3_resource, file_keys=file_keys_set
        )

        return files_metadata
