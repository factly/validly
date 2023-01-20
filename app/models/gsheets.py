from pydantic import BaseModel


class GsheetRequest(BaseModel):
    sheet_id: str
    worksheet: str


class GsheetSaveRequest(GsheetRequest):
    entity: str
