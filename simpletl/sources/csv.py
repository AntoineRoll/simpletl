import polars as pl

from simpletl.abstract import Source


class CsvSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")
        
        if self.url is None: raise ValueError("URL is required for CSV source under `source.url`")

    def read_data(self, source_config: dict) -> pl.DataFrame:
        url = source_config.get("url")
        
        separator = source_config.get("separator", ",")

        return pl.read_csv(url, separator=separator, infer_schema_length=None)