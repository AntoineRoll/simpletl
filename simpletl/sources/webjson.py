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
    
class WebGeojsonSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")
        self.format = config.get("source", {}).get("format")

    def get_data(self, source_config: dict) -> pl.DataFrame:
        url = source_config.get("url")

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


class WebShapefileSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")
        self.format = config.get("source", {}).get("format")

    def read_data(self, source_config: dict) -> pl.DataFrame:
        url = source_config.get("url")
        
        gdf = gpd.read_file(url)
        gdf = gdf.to_crs(epsg=4326)  # Reproject to WGS84
        
        # Convert any columns with geometry type to string
        for col in gdf.columns:
            if gdf[col].dtype == "geometry":
                gdf[col+"_geojson"] = gdf[col].__geo_interface__["features"]
                gdf[col+"_geojson"] = gdf[col+"_geojson"].apply(lambda x: json.dumps(x))
                gdf[col] = gdf[col].to_wkt()
        
        df = pl.from_pandas(gdf)

        return df