import logging

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO)
templates = Jinja2Templates(directory="templates")

documentation_router = router = APIRouter()


@router.get("/expectation/documentation/")
async def provide_documentation(request: Request):
    return templates.TemplateResponse(
        "docs/docs-base.html", context={"request": request}
    )
