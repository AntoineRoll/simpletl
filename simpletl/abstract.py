from abc import ABC, abstractmethod
import polars as pl
import logging

class Destination(ABC):
    @abstractmethod
    def __init__(self, config: dict):
        raise NotImplementedError
    
    @abstractmethod
    def write_data(self, df: pl.DataFrame | pl.LazyFrame):
        pass
    
class Transformation(ABC):
    @abstractmethod
    def __init__(self, config: dict):
        raise NotImplementedError
    
    def __name__(self):
        logging.warning("__name__ method should be overridden in transformations classes.")
        return self.__class__.__name__

    @abstractmethod
    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        pass
    
class Source(ABC):
    @abstractmethod
    def __init__(self, config: dict):
        raise NotImplementedError

    @abstractmethod
    def read_data(self) -> pl.DataFrame:
        raise NotImplementedError
    
