from great_expectations.execution_engine.pandas_execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.validator.validator import Validator

from app.great_expectation.expect_table_columns_in_snakecase import (
    ExpectTableColumnsInSnakecase,
)
from app.utils.column_mapping import find_datetime_columns
from app.great_expectation.custom_expectation import CustomPandasDataset
import great_expectations as ge

async def columns_in_snake_casing(pandas_dataset):
    # # validator = Validator(execution_engine=PandasExecutionEngine(), batches=[pandas_dataset])
    # # expectation = validator.validate_expectation("expect_snakecase_column_names")
    pandas_dataset = ge.from_pandas(pandas_dataset, dataset_class=CustomPandasDataset)
    expectation = pandas_dataset.expect_column_in_snake_case()
    return expectation
