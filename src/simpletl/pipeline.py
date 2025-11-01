from .source.data_fetcher import DataFetcher
from .transformation.transformation_engine import TransformationEngine
from .destination.data_loader import DataLoader
import logging
import yaml
import polars as pl

logging.basicConfig(level=logging.INFO)

class Pipeline:
    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        self.sources = config.get('sources', [])
        self.transformations = config.get('transformations', [])
        self.destinations = config.get('destinations', [])

    def run(self):
        logging.info("Starting ETL pipeline")

        # Step 1: Fetch data from sources
        data_fetcher = DataFetcher()
        fetched_data = []
        for source in self.sources:
            data_type = source.get('type')
            if data_type.lower() == 'csv':
                data = data_fetcher.fetch_data(source['url'], data_type)
                fetched_data.append(pl.read_csv(pl.StringCacheConfig(use=True, capacity=100_000).parse_bytes(data)))
            elif data_type.lower() in ['json', 'geojson']:
                data = data_fetcher.fetch_data(source['url'], data_type)
                fetched_data.append(pl.read_json(data))
            else:
                logging.error(f"Unsupported data type: {data_type}")
                raise ValueError(f"Unsupported data type: {data_type}")

        # Step 2: Apply transformations
        transformation_engine = TransformationEngine()
        transformed_data = []
        for data, transformation in zip(fetched_data, self.transformations):
            transformed_df = data
            for step in transformation['steps']:
                transformed_df = transformation_engine.apply_step(transformed_df, step)
            transformed_data.append(transformed_df)

        # Step 3: Load data to destinations
        data_loader = DataLoader()
        for data, destination in zip(transformed_data, self.destinations):
            data_loader.load_data(data, destination['s3_bucket'], destination['s3_prefix'])

        logging.info("ETL pipeline completed successfully")

# Example usage:
# if __name__ == "__main__":
#     pipeline = Pipeline("config.yaml")
#     pipeline.run()