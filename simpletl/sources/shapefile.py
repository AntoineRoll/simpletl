from abstract import Source
import polars as pl
import geopandas as gpd
import json

class ShapefileSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")

    def read_data(self) -> pl.DataFrame:
        url = self.url
        
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