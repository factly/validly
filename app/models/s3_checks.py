from typing import List

from pydantic import BaseModel


class s3FileCheck(BaseModel):
    file_key: str
    is_exists: bool


class s3FileCheckResponse(BaseModel):
    exists: List[str]
    non_exists: List[str]
