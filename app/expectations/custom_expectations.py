from datetime import date

from great_expectations.dataset import MetaPandasDataset, PandasDataset

from app.core.config import CustomExpectationsSettings

custom_expectation_settings = CustomExpectationsSettings()

CURRENT_YEAR = str(date.today().year)


class GenericCustomExpectations(PandasDataset):
    _data_asset_type = "GenericCustomExpectations"

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_column_values_to_not_have_leading_or_trailing_whitespace(
        self,
        column_list,
        pattern=custom_expectation_settings.TRAIL_OR_LEAD_WHITESPACE_PATTERN,
        meta={
            "expectation_name": "No leading or traling whitespaces",
        },
    ):
        return column_list.applymap(
            lambda x: not pattern.match(x) if isinstance(x, str) else True
        ).all(axis=1)

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_columns_values_to_not_have_multispaces(
        self,
        column_list,
        pattern=custom_expectation_settings.MULTIPLE_BLANKSPACE_PATTERN,
        meta={
            "expectation_name": "No multiple whitespaces",
        },
    ):
        return column_list.applymap(
            lambda x: not pattern.match(x) if isinstance(x, str) else True
        ).all(axis=1)

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_column_values_to_not_have_special_character(
        self,
        column_list,
        pattern=custom_expectation_settings.SPECIAL_CHARACTER_PATTERN,
        meta={
            "expectation_name": "No special characters in Table values",
        },
        include_meta=True,
    ):
        return column_list.applymap(
            lambda x: not pattern.match(x) if isinstance(x, str) else True
        ).all(axis=1)

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_column_values_to_not_have_brackets(
        self,
        column,
        pattern=custom_expectation_settings.BRACKET_PATTERN,
        meta={
            "expectation_name": "No unnecessary brackets in Categories",
        },
    ):
        return column.applymap(
            lambda x: not pattern.match(x) if isinstance(x, str) else True
        ).all(axis=1)
