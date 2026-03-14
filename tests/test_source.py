from simpletl.sources import CsvSource
import polars as pl

def test_csv_source():
    csv_data = '''column1,column2
10,foo
20,bar
5,baz'''
    with open('test_sales.csv', 'w') as file:
        file.write(csv_data)

    source = CsvSource(sep=',', path='test_sales.csv')
    df = source.get_data()

    expected_df = pl.DataFrame({
        'column1': [10, 20, 5],
        'column2': ['foo', 'bar', 'baz']
    })

    assert df.equals(expected_df)
