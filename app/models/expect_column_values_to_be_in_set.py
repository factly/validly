from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class PartialUnexpectedCount(BaseModel):
    value: str
    count: int


class Result(BaseModel):
    element_count: Optional[int]
    missing_count: Optional[int]
    missing_percent: Optional[int]
    unexpected_count: Optional[int]
    unexpected_percent: Optional[int]
    unexpected_percent_total: Optional[int]
    unexpected_percent_nonmissing: Optional[int]
    partial_unexpected_list: Optional[List[str]]
    partial_unexpected_index_list: Optional[List[int]]
    partial_unexpected_counts: Optional[List[PartialUnexpectedCount]]
    unexpected_list: Optional[List[str]]
    unexpected_index_list: Optional[List[int]]


class ExceptionInfo(BaseModel):
    raised_exception: bool
    exception_message: Optional[Any]
    exception_traceback: Optional[Any]


class ColumnValuesToBeInSet(BaseModel):
    success: bool
    expectation_config: Optional[Any]
    result: Optional[Result]
    meta: Optional[Dict[str, Any]]
    exception_info: Optional[ExceptionInfo]
