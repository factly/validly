from app.core.config import UnitSettings
from app.utils.column_mapping import find_unit_columns

unit_settings = UnitSettings()


async def unit_proper_format(dataset, result_format):
    results = {}
    # get all unit columns present inside datasets
    unit_columns = await find_unit_columns(set(dataset.columns))
    # get all those columns corresponding to unit
    for each_column in unit_columns["unit"]:
        expectation = dataset.expect_column_values_to_match_regex_list(
            column=each_column,
            regex_list=unit_settings.UNIT_PATTERN,
            match_on="any",
            result_format=result_format,
            include_config=True,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results
