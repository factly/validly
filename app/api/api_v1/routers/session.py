from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse
import httpx
from typing import Optional

session_router = router = APIRouter()


@router.get("/whoami")
async def get_session(authorization: Optional[str] = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header is missing")

    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://develop-xtjn2g.zitadel.cloud/oidc/v1/userinfo",
            headers={"Authorization": authorization}
        )

    if response.status_code == 200:
        return JSONResponse(content=response.json(), status_code=200)
    else:
        raise HTTPException(status_code=401, detail="Invalid or expired session")