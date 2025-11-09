from abc import ABC, abstractmethod
import polars as pl


class Destination(ABC):
    @abstractmethod
    def write_data(self, destination_config: dict, df: pl.DataFrame):
        pass


class ParquetFileDestination(Destination):
    def __init__(self):
        self.file_path = None

    def write_data(self, destination_config: dict, df: pl.DataFrame):
        config = destination_config.get("parquet")

        self.file_path = config.get("file_path")

        extra_parquet_args = config.get("parquet_args", {})

        df.write_parquet(self.file_path, **extra_parquet_args)

        return self.file_path


class DeltaTableDestination(Destination):
    def __init__(self, bucket_url: str | None = None, prefix: str | None = None):
        self.bucket_url = bucket_url
        self.prefix = prefix

        if self.bucket_url is None or self.prefix is None:
            self.table_path = None
        else:
            self.table_path = f"s3://{self.bucket_url}/{self.prefix}"

    def write_data(self, destination_config: dict, df: pl.DataFrame):
        config = destination_config.get("delta")
        mode = config.get("mode") or "overwrite"
        overwrite_schema = config.get("overwrite_schema") or False

        if config.get("bucket_url") and config.get("prefix"):
            self.table_path = f"s3://{config.get('bucket_url')}/{config.get('prefix')}"
            # self.table_path = f"{config.get('bucket_url')}/{config.get('prefix')}"

        import os

        df.write_delta(
            self.table_path,
            mode=mode,
            overwrite_schema=overwrite_schema,
            storage_options={
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            },
        )

        return self.table_path
