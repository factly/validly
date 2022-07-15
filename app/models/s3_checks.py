from typing import List

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
