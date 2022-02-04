from fastapi import FastAPI

from app.api.api_v1.routers.column import column_router
from app.api.api_v1.routers.column_mapping import column_mapper_router
from app.api.api_v1.routers.datetime import datetime_router
from app.api.api_v1.routers.geography import geographic_router
from app.api.api_v1.routers.note import note_router
from app.api.api_v1.routers.unit import unit_router
from app.core.config import Settings

settings = Settings()

app = FastAPI(title=settings.PROJECT_NAME, docs_url=settings.DOCS_URL)


@app.get(settings.API_V1_STR)
async def root():
    return {"message": "Server is up"}


app.include_router(
    column_mapper_router, prefix="/columns", tags=["Columns Mapped"]
)

app.include_router(datetime_router, prefix="/columns/datetime", tags=["Date & Time"])

app.include_router(geographic_router, prefix="/columns/geography", tags=["Geography"])

app.include_router(unit_router, prefix="/columns/unit", tags=["Unit Column"])

app.include_router(note_router, prefix="/columns/note", tags=["Note Column"])

# app.include_router(column_router, prefix="/column", tags=["Column"])
