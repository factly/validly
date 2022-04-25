from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class ExceptionInfo(BaseModel):
    raised_exception: Optional[bool]
    exception_traceback: Optional[Any]
    exception_message: Optional[Any]


class Details(BaseModel):
    partial_unexpected_counts_error: Optional[str]


class Result(BaseModel):
    element_count: Optional[int]
    missing_count: Optional[int]
    missing_percent: Optional[int]
    unexpected_count: Optional[int]
    unexpected_percent: Optional[float]
    unexpected_percent_total: Optional[float]
    unexpected_percent_nonmissing: Optional[float]
    partial_unexpected_list: Optional[List[Dict[str, Any]]]
    details: Optional[Details]
    _partial_unexpected_index_list: Optional[List[int]]
    partial_unexpected_counts: Optional[List]


class Kwargs(BaseModel):
    column_list: Optional[List[str]]
    result_format: Optional[str]


class ExpectationConfig(BaseModel):
    _meta: Optional[Dict[str, Any]]
    expectation_type: Optional[str]
    _kwargs: Optional[Kwargs]


class GeneralTableExpectation(BaseModel):
    success: bool
    result: Result
    exception_info: ExceptionInfo
    _meta: Dict[str, Any]
    expectation_config: ExpectationConfig

    class Config:
        underscore_attrs_are_private = True
