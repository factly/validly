import great_expectations as ge
import pandas as pd

from app.core.config import APP_DIR, GeographySettings

geographic_settings = GeographySettings()


async def read_dataset(source: str):
    dataset = ge.read_csv(source)

    return dataset


async def load_values_to_be_in_set(domain: str):
    # this function is used to load csv files, consiting values
    # for states or country that are required to be in specific set
    set_values_file = APP_DIR / "core" / f"{domain}.csv"
    set_values = pd.read_csv(set_values_file)[f"{domain}"].unique()
    return set_values
