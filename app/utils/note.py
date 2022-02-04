from app.core.config import NoteSettings
from app.utils.column_mapping import find_note_columns

note_settings = NoteSettings()


async def note_proper_format(dataset, result_format):
    results = {}
    # get all note columns present inside datasets
    note_columns = await find_note_columns(set(dataset.columns))
    # get all those columns corresponding to note
    for each_column in note_columns["note"]:
        print(each_column)
        expectation = dataset.expect_column_values_to_match_regex_list(
            column=each_column,
            regex_list=note_settings.NOTE_PATTERN,
            match_on="any",
            result_format=result_format,
            include_config=True,
            catch_exceptions=True,
        )
        results[each_column] = expectation
    return results
