import great_expectations as ge

from app.great_expectation.custom_expectation import CustomPandasDataset


async def columns_in_snake_casing(pandas_dataset):
    pandas_dataset = ge.from_pandas(
        pandas_dataset, dataset_class=CustomPandasDataset
    )
    expectation = pandas_dataset.expect_column_in_snake_case()
    return expectation
