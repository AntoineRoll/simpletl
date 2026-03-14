from abc import ABC, abstractmethod
import polars as pl
import requests
import os

from simpletl.abstract import Destination

def urljoin(*args):
    return "/".join(
        elem
        .removeprefix("/")
        .removesuffix("/")
        for elem in args
    )


class ParquetFileDestination(Destination):
    def __init__(self, config: dict):
        self.file_path = config.get("file_path")
        self.extra_parquet_args = config.get("parquet_args", {})

    def write_data(self, df: pl.DataFrame) -> str:
        df.write_parquet(self.file_path, **self.extra_parquet_args)

        return self.file_path

