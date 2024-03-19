import logging
from datetime import date

import numpy as np
import pandas as pd
from great_expectations.dataset import MetaPandasDataset, PandasDataset

from app.core.config import CustomExpectationsSettings

custom_expectation_settings = CustomExpectationsSettings()

CURRENT_YEAR = str(date.today().year)
logging.basicConfig(level=logging.INFO)


class GenericCustomExpectations(PandasDataset):
    _data_asset_type = "GenericCustomExpectations"

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_column_values_to_not_have_leading_or_trailing_whitespace(
        self,
        column_list,
        pattern=custom_expectation_settings.TRAIL_OR_LEAD_WHITESPACE_PATTERN,
        meta={
            "expectation_name": "No leading or trailing whitespaces",
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

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_multicolumn_dataset_to_have_more_than_x_rows(self, column_list):
        length = column_list.shape[0]
        return np.repeat(
            np.array(
                (
                    column_list.index
                    >= custom_expectation_settings.MINIMUM_DATASET_OBSERVATION_THRESH
                ).any()
            ),
            length,
        )

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_numerical_values_to_be_in_specific_pattern(
        self,
        column_list,
        pattern=custom_expectation_settings.NUMERIC_VALUES_PATTERN,
        meta={
            "expectation_name": "Numeric values in specific pattern",
        },
        include_meta=True,
    ):
        bool_list = column_list.applymap(
            lambda x: True if pattern.match(str(x)) else False
        )
        return bool_list[bool_list.columns[0]]

    @MetaPandasDataset.multicolumn_map_expectation
    def flag_negative_numerical_values(
        self,
        column_list,
        pattern=custom_expectation_settings.NEGATIVE_NUMERIC_VALUES_PATTERN,
        meta={
            "expectation_name": "Negative Numeric values Flag",
        },
        include_meta=True,
    ):
        bool_list = column_list.applymap(
            lambda x: False if pattern.match(str(x)) else True
        )
        return bool_list[bool_list.columns[0]]

    @MetaPandasDataset.multicolumn_map_expectation
    def expect_column_names_to_be_in_specific_pattern(
        self,
        column_list,
        pattern=custom_expectation_settings.COLUMN_NAMES_PATTERN,
        meta={
            "expectation_name": "Values in specific pattern",
        },
        include_meta=True,
        find_columns=False,
    ):
        boolean_list = pd.Series(column_list.columns).apply(
            lambda x: True if pattern.match(str(x)) else False
        )
        # improper_column_list = [
        #     column
        #     for column, boolean in zip(column_list.columns, boolean_list)
        #     if not boolean
        # ]
        # logging.info(boolean_list.all())

        return boolean_list.all()
