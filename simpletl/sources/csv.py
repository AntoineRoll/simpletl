import polars as pl

from simpletl.abstract import Source


class CsvSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("url")
        self.separator = config.get("separator", ",")


        if self.url is None: raise ValueError("URL is required for CSV source under `source.url`")

    def read_data(self) -> pl.DataFrame:
        return pl.read_csv(self.url, separator=self.separator, infer_schema_length=None)