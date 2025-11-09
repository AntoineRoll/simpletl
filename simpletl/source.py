import requests
import polars as pl
from abc import ABC, abstractmethod


class Source(ABC):
    @abstractmethod
    def get_data(self, source_config: dict) -> pl.DataFrame:
        raise NotImplementedError


class WebJsonSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")
        self.format = config.get("source", {}).get("format")

    def get_data(self, source_config: dict) -> pl.DataFrame:
        url = source_config.get("url")

        response = requests.get(url)
        response.raise_for_status()

        raw_data = response.content

        return pl.read_json(raw_data)


class CsvSource(Source):
    def __init__(self, sep: str = ",", url: str = None, path: str = None):
        self.sep = sep
        self.url = url
        self.path = path

    def get_data(self) -> pl.DataFrame:
        if self.url:
            return pl.read_csv(self.url, sep=self.sep)
        elif self.path:
            return pl.read_csv(self.path, sep=self.sep)
        else:
            raise ValueError("Either URL or path must be provided")
