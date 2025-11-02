import requests
import polars as pl
from abc import ABC, abstractmethod
import json
from io import StringIO 


class Source(ABC):
    @abstractmethod
    def get_data(self, source_config: dict) -> pl.DataFrame:
        raise NotImplementedError


class WebFileSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")
        self.format = config.get("source", {}).get("format")
    
    def get_data(self, source_config: dict) -> pl.DataFrame:

        url = source_config.get("url")
        format = source_config.get("format")
        
        
        response = requests.get(url)
        response.raise_for_status()
        
        raw_data = response.text
        
        if format=="geojson":
            return self._load_geojson_features(raw_data)
    
    
    def _load_geojson_features(self, geojson_str: str):
        
        geojson_features = json.loads(geojson_str)["features"]
        print(geojson_features[0])
        
        geojson_data = [
            {
                **elem["properties"],
                "geometry": 
                    [elem["geometry"]["coordinates"]] 
                    if elem["geometry"]["type"]=="Polygon"
                    else elem["geometry"]["coordinates"],
                "geometry_type": elem["geometry"]["type"]
            }
            for elem in geojson_features
        ]
        
        return pl.DataFrame(
            geojson_data
        )
        

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
