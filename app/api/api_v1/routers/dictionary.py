import pandas as pd
from fastapi import APIRouter, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from app.core.config import CORE_FOLDER, Settings
from app.models.gsheets import GsheetSaveRequest
from app.utils.gsheets import get_records_from_gsheets

settings = Settings()

dictionary_router = router = APIRouter()


@router.get("/", summary="Get all Saved Entities csv file name")
async def get_entity_names():
    # List down all the csv files present in the config folder
    return [
        csv_file.name.replace(".csv", "")
        for csv_file in CORE_FOLDER.glob("**/*.csv")
    ]


@router.get(
    "/{entity}",
    summary="Get data about Saved Entity csv file",
    response_class=JSONResponse,
)
async def get_entity_data(entity: str):
    entity_df = pd.read_csv(CORE_FOLDER / f"{entity}.csv")
    # to avoid json conversion error
    entity_df = entity_df.fillna("")

    # convert to json
    json_compatible_item_data = jsonable_encoder(
        entity_df.to_dict(orient="records")
    )
    return JSONResponse(content=json_compatible_item_data)


@router.put(
    "/",
    summary="Update an Entity csv file and save it to config",
)
async def update_entity(request: GsheetSaveRequest):
    # looking for destination that needs to be updated
    destination_file = CORE_FOLDER / f"{request.entity}.csv"

    # only proceed if file is existing as we don not want to create new files
    if not destination_file.is_file():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provided entity does not exist",
        )

    # get dataset/tags from the data dictionary google sheet
    dataset_meta_data = get_records_from_gsheets(
        sheet_id=request.sheet_id,
        worksheet=request.worksheet,
    )

    # read the file in pandas and replace it to config
    entity_df = pd.DataFrame(dataset_meta_data)

    # Save the dataset inside config folder
    entity_df.to_csv(CORE_FOLDER / f"{request.entity}.csv", index=False)

    return {"entity": request.entity, "action": "UPDATE", "status": "SUCCEED"}
