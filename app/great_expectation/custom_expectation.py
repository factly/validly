import re

from great_expectations.data_asset import DataAsset
from great_expectations.dataset import PandasDataset

TEXT_SNAKE_CASE_PATTERN = re.compile(r"^[a-z0-9]+([_a-z0-9]+)*$")


class CustomPandasDataset(PandasDataset):

    _data_asset_type = "CustomPandasDataset"

    @DataAsset.expectation([])
    def expect_column_in_snake_case(self) -> dict:

        non_snake_case_columns = list(
            filter(
                lambda column: not TEXT_SNAKE_CASE_PATTERN.match(column)
                if isinstance(column, str)
                else False,
                self.columns,
            )
        )
        success = len(non_snake_case_columns) == 0

        result = {
            "unexpected_columns": len(non_snake_case_columns),
            "unexpected_columns_list": non_snake_case_columns,
        }

        return {"success": success, "result": result}
