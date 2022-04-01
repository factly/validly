from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.api_v1.routers.column_mapping import column_mapper_router
from app.api.api_v1.routers.dataset import dataset_router
from app.api.api_v1.routers.datetime import datetime_router
from app.api.api_v1.routers.documentation import documentation_router
from app.api.api_v1.routers.general import general_router
from app.api.api_v1.routers.geography import geographic_router
from app.api.api_v1.routers.note import note_router
from app.api.api_v1.routers.unit import unit_router
from app.core.config import Settings

settings = Settings()

app = FastAPI(title=settings.PROJECT_NAME, docs_url=settings.DOCS_URL)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get(settings.API_V1_STR)
async def home(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})
    # return {"message": "Server is up"}


app.include_router(dataset_router, prefix="", tags=["Compare Datasets"])

app.include_router(
    column_mapper_router, prefix="/columns", tags=["Columns Mapped"]
)

app.include_router(
    datetime_router, prefix="/columns/datetime", tags=["Date & Time Column"]
)

app.include_router(
    geographic_router, prefix="/columns/geography", tags=["Geography Column"]
)

app.include_router(unit_router, prefix="/columns/unit", tags=["Unit Column"])

app.include_router(note_router, prefix="/columns/note", tags=["Note Column"])

app.include_router(general_router, prefix="/table", tags=["Table"])

app.include_router(documentation_router, prefix="", tags=["Documentation"])

# app.include_router(column_router, prefix="/column", tags=["Column"])
