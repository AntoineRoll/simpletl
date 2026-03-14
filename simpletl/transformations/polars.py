from simpletl.abstract import Transformation
import polars as pl

class CollectLazyDataframeTransformation(Transformation):
    def __init__(self, config: dict):
        self.config = config
        
    def __name__(self):
        return "collect_lazy_dataframe"
        
    def transform_data(self, df: pl.LazyFrame):
        return df.collect()