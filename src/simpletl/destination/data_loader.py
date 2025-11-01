import polars as pl
import boto3
from typing import List, Dict
import deltalake as dl
import logging
from deltalake.writer import write_deltalake
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO)

class DataLoader:
    def __init__(self, s3_client=None):
        if s3_client is None:
            self.s3_client = boto3.client('s3')
        else:
            self.s3_client = s3_client

    def load_data(self, data, s3_bucket, s3_prefix):
        logging.info(f"Loading data to S3 bucket: {s3_bucket}, prefix: {s3_prefix}")
        s3_path = f"s3://{s3_bucket}/{s3_prefix}/"

        # Convert data to Polars DataFrame
        if isinstance(data, list):
            data = pl.DataFrame(data)
        elif isinstance(data, tuple):
            data = pl.DataFrame({"value": data})
        elif not isinstance(data, pl.DataFrame):
            raise ValueError("Data must be a Polars DataFrame, list, or tuple")

        # Save to Delta Table on S3
        try:
            write_deltalake(
                data,
                s3_path,
                mode="overwrite",
                storage_options={"client_kwargs": {"endpoint_url": "http://your-s3-custom-endpoint"}}
            )
            logging.info(f"Data successfully loaded to Delta Table at {s3_path}")
        except Exception as e:
            logging.error(f"Failed to load data to Delta Table at {s3_path}. Error: {e}")
            raise e