import json
import requests

from simpletl.abstract import Source
import polars as pl
import geopandas as gpd


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
    
class GeojsonSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")
        self.format = config.get("source", {}).get("format")

    def read_data(self) -> pl.DataFrame:
        url = self.url

        response = requests.get(url)
        response.raise_for_status()

        raw_data = response.content
        
        # Parse JSON properly to preserve geometry structure
        geojson_data = json.loads(raw_data.decode('utf-8'))
        
        # Handle different geometry types properly
        features = []
        for feature in geojson_data.get('features', []):
            # Preserve the original geometry structure
            feature_dict = {
                'type': feature.get('type'),
                'properties': feature.get('properties', {}),
                'geometry': json.dumps(feature.get('geometry'))  # Keep as JSON string to preserve structure
            }
            features.append(feature_dict)
        
        # Create DataFrame with proper schema that preserves geometry structure
        return pl.DataFrame(features)


