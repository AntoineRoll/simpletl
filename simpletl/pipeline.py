from typing import List, Callable
import polars as pl
from .source import Source
from .destination import Destination
import logging


class Pipeline:
    def __init__(
        self,
        config: dict,
        source: Source,
        transformations: List[Callable[[pl.DataFrame], pl.DataFrame]] = None,
        destinations: List[Destination] = None,
    ):
        self.config = config
        self.source = source
        self.transformations = transformations or []
        self.destinations = destinations or []
        
        self._validate_config()
        
    def _validate_config(self):
        assert "id" in self.config

    def run(self):
        df = self.source.get_data(self.config.get("source", {}))
        logging.info("Source data with %s rows and %s columns loaded", len(df), len(df.columns))

        for transformation in self.transformations:
            df = transformation(df)

        for destination in self.destinations:
            destination.write_data(self.config.get("destination", {}), df)
            
        return df
