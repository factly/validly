import io

import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from app.core.config import CORE_FOLDER, Settings
from app.models.gsheets import GsheetSaveRequest
from app.utils.gsheets import get_records_from_gsheets

settings = Settings()

dictionary_router = router = APIRouter()

# reading sheet name from env
google_spread_sheet_sheet_name = settings.GOOGLE_SPREAD_SHEET_SHEET_NAME
google_sheet_id = settings.GOOGLE_SHEET_ID

g_sheet_session = requests.Session()
common_g_sheet_link_format = "https://docs.google.com/spreadsheets/d/"
g_sheet_id = f"{google_sheet_id}"
download_sheet_name = (
    f"/gviz/tq?tqx=out:csv&sheet={google_spread_sheet_sheet_name}"
)
url_name = common_g_sheet_link_format + g_sheet_id + download_sheet_name
g_sheet_response = g_sheet_session.get(url_name)
g_sheet_bytes_data = g_sheet_response.content
data = pd.read_csv(io.StringIO(g_sheet_bytes_data.decode("utf-8")))

standard_data_values = data.copy()
standard_data_values.rename(
    columns={
        "country_standard_name": "country",
        "unique_standard_airline_name": "airline",
        "standard_disease_name": "diseases",
        "psu_companies": "psu",
        "standard_district_name": "district",
        "standard_states": "state",
        "insurance_standard_names": "insurance_companies",
    },
    inplace=True,
)


@router.get("/", summary="Get all Saved Entities csv file name")
async def get_entity_names():
    # List down all the csv files present in the config folder
    return data.columns.tolist()


@router.get(
    "/{entity}",
    summary="Get data about Saved Entity csv file",
    response_class=JSONResponse,
)
async def get_entity_data(entity: str):
    entity_df = data[[entity]].dropna()
    # to avoid json conversion error
    # entity_df = entity_df.fillna("")

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
