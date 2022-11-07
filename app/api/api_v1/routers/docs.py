from fastapi import APIRouter

from app.core.docs import AllExpectationDocs

docs_router = router = APIRouter()


@router.get(
    "/expectations/",
    summary="Provide all the expectation details required for the Documentation page.",
)
async def all_api_documentation():
    return AllExpectationDocs().dict()
