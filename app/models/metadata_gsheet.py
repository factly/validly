from typing import Union

from pydantic import BaseModel

from app.models.enums import ExpectationResultType


class MetadataGsheetRequest(BaseModel):
    sheet_id: str
    worksheet: Union[str, None] = None
    result_type: ExpectationResultType = ExpectationResultType.SUMMARY.value
