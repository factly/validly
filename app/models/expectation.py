from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class _Kwargs(BaseModel):
    column: str
    regex: str
    result_format: str


class ExpectationConfig(BaseModel):
    _expectation_type: str
    _kwargs: _Kwargs
    _raw_kwargs: Any
    meta: Dict[str, Any]
    success_on_last_run: Any
    _ge_cloud_id: Any
    _expectation_context: Any


class PartialUnexpectedCount(BaseModel):
    value: int
    count: int


class Result(BaseModel):
    element_count: int
    missing_count: int
    missing_percent: int
    unexpected_count: int
    unexpected_percent: int
    unexpected_percent_total: int
    unexpected_percent_nonmissing: int
    partial_unexpected_list: List[int]
    partial_unexpected_index_list: List[int]
    partial_unexpected_counts: List[PartialUnexpectedCount]
    unexpected_list: List[int]
    unexpected_index_list: List[int]


class ExceptionInfo(BaseModel):
    raised_exception: bool
    exception_traceback: Any
    exception_message: Any


class Expectation(BaseModel):
    success: bool
    expectation_config: Optional[ExpectationConfig]
    result: Optional[Result]
    meta: Optional[Dict[str, Any]]
    exception_info: Optional[ExceptionInfo]
