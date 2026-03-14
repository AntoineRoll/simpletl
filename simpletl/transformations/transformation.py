from typing import Callable, List
import polars as pl
from abc import ABC, abstractmethod
import json

from simpletl.abstract import Transformation

def lists_to_json(s: pl.Series) -> str:
    return json.dumps(s.to_list())

def extract_features_from_geojson(df: pl.DataFrame, unnest_properties: bool=True) -> pl.DataFrame:
    if unnest_properties:
        feature_properties = (
            df["properties"]
            .struct.unnest()
        ).get_columns()
    else:
        feature_properties = df["properties"]
    return pl.DataFrame(
        (
            *feature_properties,
            df["geometry"]
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


def collect_lazy_dataframe(df: pl.LazyFrame) -> pl.DataFrame:
    return df.collect(engine="streaming")

def sql_transformation(sql_query: str) -> Callable[[pl.DataFrame | pl.LazyFrame], pl.DataFrame | pl.LazyFrame]:
    return lambda x: x.sql(sql_query)


def _fix_schema_with_empty_structs(
    schema: pl.Schema, 
    replace_with={"dummy": pl.Int8()}
    ) -> pl.Schema:
    """
    Fix a polars schema by replacing empty structs with structs containing a dummy field.
    
    Args:
        schema: Input polars schema
        replace_with: Struct to replace Empty Structs
        
    Returns:
        Fixed polars schema with empty structs replaced by structs with dummy fields
    """
    
    def _fix_dtype(dtype: pl.DataType) -> pl.DataType:
        if not isinstance(dtype, pl.Struct):
            return dtype
        
        dtype: pl.Struct
        
        if len(dtype.fields) == 0:
            return pl.Struct(replace_with)
        
        return pl.Struct(
            {
                field.name: _fix_dtype(field.dtype)
                for field in dtype.fields
            }
        )
        
    fixed_fields = []
    for field_name, field_dtype in schema.items():
        fixed_fields.append((field_name, _fix_dtype(field_dtype)))
            
    
    return pl.Schema(fixed_fields)

def fill_empty_structs_in_dataframe(df: pl.DataFrame):
    
    return df.match_to_schema(
        schema=_fix_schema_with_empty_structs(df.collect_schema()),
        missing_struct_fields='insert'
    )