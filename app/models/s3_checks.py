from typing import List, Union

from pydantic import BaseModel


class s3FileCheck(BaseModel):
    file_key: str
    is_exists: bool


class ObjectDetail(BaseModel):
    key: str
    size: int


class s3FileCheckResponse(BaseModel):
    exists: List[ObjectDetail]
    non_exists: List[str]


class s3FileKeyCheckRequest(BaseModel):
    file_key: str
    s3_access_key: Union[str, None] = None
    s3_secret_key: Union[str, None] = None
    s3_endpoint_url: Union[str, None] = None
    resource: Union[str, None] = None

    class Config:
        schema_extra = {
            "examples": {
                "file": {
                    "summary": "s3://roapitest/processed/wheat/2015/output.csv",
                    "description": "Provide file key to check if it exists in S3",
                    "value": {
                        "file_key": "file",
                        "s3_access_key": None,
                        "s3_secret_key": None,
                        "s3_endpoint_url": None,
                        "resource": None,
                    },
                },
            }
        }


class s3FileKeysCheckRequest(BaseModel):
    file_keys: List[str]
    s3_access_key: Union[str, None] = None
    s3_secret_key: Union[str, None] = None
    s3_endpoint_url: Union[str, None] = None
    resource: Union[str, None] = None

    class Config:
        schema_extra = {
            "examples": {
                "files": {
                    "summary": "Files",
                    "description": "Provide file keys to check if it exists in S3",
                    "value": {
                        "file_keys": [
                            "s3://roapitest/processed/wheat/2015/output.csv",
                            "s3://roapitest/processed/wheat/2016/output.csv",
                        ],
                        "s3_access_key": None,
                        "s3_secret_key": None,
                        "s3_endpoint_url": None,
                        "resource": None,
                    },
                },
            }
        }
