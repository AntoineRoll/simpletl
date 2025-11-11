from typing import List, Callable
import polars as pl
from .source import Source
from .destination import Destination
import logging
import json
import os
from datetime import datetime, timezone


class PipelineMetadata:
    def __init__(self, pipeline_id: str, metadata_folder: str = "./simpletl_pipelines"):
        self.pipeline_id = pipeline_id
        self.metadata_folder = metadata_folder
        self.metadata = {}
        self._load_or_init_metadata()

    @property
    def _metadata_path(self):
        return os.path.join(self.metadata_folder, f"{self.pipeline_id}.json")

    def _load_or_init_metadata(self):
        if os.path.exists(self._metadata_path):
            with open(self._metadata_path) as f:
                self.metadata = json.load(f)

            current_pipeline = self._serialize()

            previous_pipeline: dict = self.metadata.get("pipelines")[-1]
            previous_version = previous_pipeline.copy().pop("version", 1)

            if not current_pipeline == previous_pipeline:
                # Pipeline has changed, registering a new version
                new_version = previous_version + 1
                self.metadata["pipeline_version"] = new_version
                self.metadata["pipelines"] += [
                    {**current_pipeline, "version": new_version}
                ]

        else:
            self.metadata = {
                "pipelines": [{**self._serialize(), "version": 1}],
                "pipeline_version": 1,
                "runs": [],
            }

    def _save_metadata(self):
        # Creating folder if not already exists
        os.makedirs(self.metadata_folder, exist_ok=True)
        with open(self._metadata_path, "w") as f:
            json.dump(self.metadata, f, indent=2)

    def log_run_start(self):
        run_info = {
            "start_time": datetime.now(timezone.utc).isoformat(),
            "data": {},
            "pipeline_version": self.metadata.get("pipeline_version"),
            "status": "success",  # Success unless specified otherwise
            "error": None,
        }
        self.metadata["runs"].append(run_info)
        return run_info

    def log_run_end(self, run_info, additional_info=None):
        run_info["end_time"] = datetime.now(timezone.utc).isoformat()

        if additional_info:
            run_info["data"].update(additional_info)

    def get_metadata(self):
        return self.metadata


class Pipeline(PipelineMetadata):
    def __init__(
        self,
        config: dict,
        source: Source,
        transformations: List[Callable[[pl.DataFrame], pl.DataFrame]] = None,
        destinations: List[Destination] = None,
        store_metadata: bool = False,
    ):
        self.config = config
        self.source = source
        self.transformations = transformations or []
        self.destinations = destinations or []
        self.store_metadata = store_metadata

        self._validate_config()

        pipeline_id = self.config.get("id")
        super().__init__(pipeline_id)

    def _serialize(self):
        return {
            "name": self.__class__.__name__,
            "source": self.source.__class__.__name__,
            "transformations": [
                transform.__name__ for transform in self.transformations
            ],
            "destinations": [dest.__class__.__name__ for dest in self.destinations],
            "config": self.config,
        }

    def _validate_config(self):
        assert "id" in self.config, "Field `id` is required in pipeline config."

    def run(self) -> pl.DataFrame:
        run_info = self.log_run_start() if self.store_metadata else None

        try:
            df = self.process_data()
            if self.store_metadata:
                run_info["data"]["rows"] = len(df)
                run_info["data"]["columns"] = len(df.columns)
        except Exception as e:
            if self.store_metadata:
                run_info["status"] = "failed"
                run_info["error"] = str(e)
            raise e
        finally:
            if self.store_metadata:
                self.log_run_end(run_info)
                self._save_metadata()

        return df

    def process_data(self):
        df = self.source.get_data(self.config.get("source", {}))
        logging.info(
            "Source data with %s rows and %s columns loaded", len(df), len(df.columns)
        )

        for transformation in self.transformations:
            df = transformation(df)
            logging.info(
                "Data transformed with %s resulting in %s rows and %s columns.",
                transformation.__name__,
                len(df),
                len(df.columns),
            )

        for destination in self.destinations:
            destination.write_data(self.config.get("destination", {}), df)

        return df
