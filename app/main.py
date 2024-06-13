from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.api_v1.routers.dataset import dataset_router
from app.api.api_v1.routers.dictionary import dictionary_router
from app.api.api_v1.routers.docs import docs_router
from app.api.api_v1.routers.metadata import metadata_router
from app.api.api_v1.routers.s3_checks import s3_router
from app.core.config import Settings

settings = Settings()

app = FastAPI(title=settings.PROJECT_NAME, docs_url=settings.DOCS_URL)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_methods=settings.CORS_METHODS,
    allow_headers=settings.CORS_HEADERS,
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get(settings.API_V1_STR)
async def home(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})


app.include_router(dataset_router, prefix="", tags=["Compare Datasets"])
app.include_router(s3_router, prefix="/s3", tags=["S3 Checks"])
app.include_router(metadata_router, prefix="", tags=["Metadata"])
app.include_router(docs_router, prefix="/docs", tags=["Documentation"])
app.include_router(
    dictionary_router, prefix="/dictionary", tags=["Dictionary"]
)
