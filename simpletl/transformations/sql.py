import polars as pl

from simpletl.abstract import Transformation

class SqlQueryTransformation(Transformation):
    
    def __init__(self, config):
        self.query = config.get("query")
        
    def transform_data(self, df: pl.DataFrame):
        return df.sql(self.query)