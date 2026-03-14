import polars as pl

from simpletl.abstract import Source


class DeltaSource(Source):
    def __init__(self, config: dict):
        self.path = config.get("path")
        
    def read_data(self) -> pl.LazyFrame:

        return pl.scan_delta(self.path)