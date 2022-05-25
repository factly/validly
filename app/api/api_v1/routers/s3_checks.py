from typing import List

from fastapi import APIRouter, Depends, Form, HTTPException, Path, status
from fastapi.logger import logger

from app.models.s3_checks import s3FileCheck, s3FileCheckResponse
from app.utils.s3_checks import get_s3_resource

s3_router = router = APIRouter()


@router.post("/files/{file_key:path}", response_model=s3FileCheck)
async def check_if_file_exist_in_bucket(
    file_key: str = Path(...),
    bucket: str = Form(...),
    s3_resource=Depends(get_s3_resource),
):
    """
    Check if file exist in bucket
    """
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
    s3_resource=Depends(get_s3_resource),
):

    # TODO : Check how Form is combining all the strings inside list
    if len(file_keys) == 1:
        file_keys = [key for key in file_keys[0].split(",")]

    bucket = s3_resource.Bucket(bucket)

    if bucket in s3_resource.buckets.all():
        logger.debug("Bucket exists: {}".format(bucket))

        objs = {obj.key for obj in bucket.objects.all()}
        existing_files = objs.intersection(file_keys)
        non_existing_files = set(file_keys) - existing_files

        return {
            "exists": list(existing_files),
            "non_exists": list(non_existing_files),
        }

    else:
        logger.info(f"Bucket {bucket} does not exist")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Bucket does not exist",
        )
