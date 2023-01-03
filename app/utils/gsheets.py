from typing import Dict, List, Union

import gspread
from google.oauth2 import service_account

from app.core.config import Settings

settings = Settings()


def get_records_from_gsheets(
    sheet_id: str, worksheet: Union[None, str] = None
) -> List[Dict[str, str]]:
    """Get records from a google worksheet

    Args:
        sheet_id (str): ID of the worksheet , present in URL
        worksheet (Union[None, str], optional): Specific worksheet name. Defaults to None.

    Returns:
        List[Dict[str, str]]: List of records
    """ """"""
    credentials = service_account.Credentials.from_service_account_info(
        settings.SERVICE_ACCOUNT_CONF, scopes=settings.GSHEET_SCOPES
    )

    # authorize the clientsheet
    client = gspread.authorize(credentials)

    # get the instance of the Spreadsheet
    sheet = client.open_by_key(sheet_id)

    # get the very first worksheet present if no tab is mentioned
    # tab is worksheet window inside Gsheets
    if not worksheet:

        # for no specific name of worksheet provided
        # get the very first sheet
        tab = sheet.get_worksheet(0)
    else:
        tab = sheet.worksheet(worksheet)

    links_metadata = tab.get_all_records()
    return links_metadata
