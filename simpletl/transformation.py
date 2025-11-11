from typing import Callable, List
import polars as pl
from abc import ABC, abstractmethod

TransformationFunc = Callable[[pl.DataFrame], pl.DataFrame]


class Transformation(ABC):
    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        pass


def extract_features_from_geojson(df: pl.DataFrame) -> pl.DataFrame:
    return pl.DataFrame(
        (
            df["features"]
            .explode()
            .struct.field("properties")
            .alias("feature_properties"),
            df["features"]
            .explode()
            .struct.field("geometry")
            .struct.field("type")
            .alias("geometry_type"),
            df["features"]
            .explode()
            .struct.field("geometry")
            .struct.field("coordinates")
            .alias("geometry_coordinates"),
        )
    )


def filter_rows(df: pl.DataFrame, condition: str) -> pl.DataFrame:
    # Use polars expressions to construct the filter condition
    expr = pl.col(condition.split(" ")[0]).gt(int(condition.split(" ")[2]))
    return df.filter(expr)


def select_columns(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
    return df.select(columns)


def rename_columns(df: pl.DataFrame, mapping: dict) -> pl.DataFrame:
    return df.rename(mapping)
