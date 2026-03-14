import pytest
from simpletl.transformations.transformation import filter_rows, select_columns, rename_columns
import polars as pl

def test_filter_rows():
    df = pl.DataFrame({
        'column1': [10, 20, 5],
        'column2': ['foo', 'bar', 'baz']
    })
    filtered_df = filter_rows(df, 'column1 > 10')
    assert filtered_df.shape == (1, 2)
    assert filtered_df['column1'].to_list() == [20]
    assert filtered_df['column2'].to_list() == ['bar']

def test_select_columns():
    df = pl.DataFrame({
        'column1': [10, 20, 5],
        'column2': ['foo', 'bar', 'baz']
    })
    selected_df = select_columns(df, ['column1'])
    assert selected_df.shape == (3, 1)
    assert selected_df['column1'].to_list() == [10, 20, 5]

def test_rename_columns():
    df = pl.DataFrame({
        'column1': [10, 20, 5],
        'column2': ['foo', 'bar', 'baz']
    })
    renamed_df = rename_columns(df, {'column1': 'new_column1'})
    assert renamed_df.columns == ['new_column1', 'column2']
    assert renamed_df['new_column1'].to_list() == [10, 20, 5]