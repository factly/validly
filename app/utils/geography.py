from app.utils.column_mapping import find_geography_columns
from app.utils.common import load_values_to_be_in_set


async def standard_state_names(dataset, result_format):
    results = {}
    # get all geography columns present inside datasets
    geography_columns = await find_geography_columns(set(dataset.columns))
    # get all allowable values for state
    allowable_values = await load_values_to_be_in_set("state")
    # get all state column within geography columns
    for each_column in geography_columns["state"]:
        expectation = dataset.expect_column_values_to_be_in_set(
            column=each_column,
            value_set=allowable_values,
            result_format=result_format,
            include_config=False,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results


async def standard_country_names(dataset, result_format):
    results = {}
    # get all geography columns present inside datasets
    geography_columns = await find_geography_columns(set(dataset.columns))
    # get all allowable values for state
    allowable_values = await load_values_to_be_in_set("country")
    # get all state column within geography columns
    for each_column in geography_columns["country"]:
        expectation = dataset.expect_column_values_to_be_in_set(
            column=each_column,
            value_set=allowable_values,
            result_format=result_format,
            include_config=False,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results
