from unittest.mock import patch
from simpletl.destinations.delta import DeltaTableDestination
import polars as pl

def test_delta_table_destination():
    df = pl.DataFrame({
        'column1': [10, 20, 5],
        'column2': ['foo', 'bar', 'baz']
    })

    destination = DeltaTableDestination(bucket_url='my-bucket', prefix='data/sales_pipeline')

    with patch('deltalake.writer.write_deltalake') as mock_write_deltalake:
        destination.write_data(df)
        mock_write_deltalake.assert_called_once_with(
            'my-bucket/data/sales_pipeline',
            df.to_pandas(),
            mode='append'
        )