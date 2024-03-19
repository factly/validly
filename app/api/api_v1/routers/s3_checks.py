from fastapi import APIRouter, Body, HTTPException, status

from app.core.config import Settings
from app.models.s3_checks import s3FileKeyCheckRequest, s3FileKeysCheckRequest
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
    request: s3FileKeyCheckRequest = Body(
        None, examples=s3FileKeyCheckRequest.Config.schema_extra["examples"]
    )
):
    """
    Check if file exist in bucket
    """
    s3_access_key = (
        settings.S3_SOURCE_ACCESS_KEY
        if request.s3_access_key is None
        else request.s3_access_key
    )
    s3_secret_key = (
        settings.S3_SOURCE_SECRET_KEY
        if request.s3_secret_key is None
        else request.s3_secret_key
    )
    s3_endpoint_url = (
        settings.S3_SOURCE_ENDPOINT_URL
        if request.s3_endpoint_url is None
        else request.s3_endpoint_url
    )
    resource = (
        settings.S3_SOURCE_RESOURCE
        if request.resource is None
        else request.resource
    )
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
        file_metadata = await check_file_metadata(
            s3_resource, request.file_key
        )
        return file_metadata


@router.post(
    "/files",
)
async def check_if_files_exist_in_bucket(
    request: s3FileKeysCheckRequest = Body(
        None, examples=s3FileKeysCheckRequest.Config.schema_extra["examples"]
    )
):
    s3_access_key = (
        settings.S3_SOURCE_ACCESS_KEY
        if request.s3_access_key is None
        else request.s3_access_key
    )
    s3_secret_key = (
        settings.S3_SOURCE_SECRET_KEY
        if request.s3_secret_key is None
        else request.s3_secret_key
    )
    s3_endpoint_url = (
        settings.S3_SOURCE_ENDPOINT_URL
        if request.s3_endpoint_url is None
        else request.s3_endpoint_url
    )
    resource = (
        settings.S3_SOURCE_RESOURCE
        if request.resource is None
        else request.resource
    )
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
        files_metadata = await check_files_metadata(
            session=s3_resource, file_keys=request.file_keys
        )

        return files_metadata
