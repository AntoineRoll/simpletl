
from abc import ABC, abstractmethod
import polars as pl
import requests
import os

from simpletl.abstract import Destination

def urljoin(*args):
    return "/".join(
        elem
        .removeprefix("/")
        .removesuffix("/")
        for elem in args
    )

class DeltaTableDestination(Destination):
    def __init__(self, config: dict):
        self.config = config
        self.bucket_url = self.config.get("bucket_url")
        self.prefix = self.config.get("prefix")
        self.mode = self.config.get("mode", "overwrite")
        self.overwrite_schema = self.config.get("overwrite_schema", False)
        
        self.table_config = self.config.get("table", {})

        if self.bucket_url is None or self.prefix is None:
            self.table_path = None
        else:
            self.table_path = f"s3://{self.bucket_url}/{self.prefix}"
            
    def _create_or_update_table(
        self, 
        catalog_name: str, 
        schema_name: str, 
        table_name: str,
        external_location: str,
        columns: list[dict[str, str]] | None = None,
        comment: str | None = None,
        columns_in_table_description: bool = True,
    ):
        """Create or update a table in unity catalog using the Unity Catalog Table API

        Args:
            catalog_name (str): Name of the catalog
            schema_name (str): Name of the schema
            table_name (str): Name of table
            external_location (str): External location of the table
            columns (list[dict[str, str]], optional): List of column definitions with name and type
            comment (str, optional): Description of the table
        """
        try:
            # Get Unity Catalog API endpoint and token from environment
            uc_endpoint = os.getenv("UC_ENDPOINT")
            uc_token = os.getenv("UC_TOKEN", None)
            
            if not uc_endpoint:
                raise ValueError("UC_ENDPOINT must be set")
            
            headers = {
                "Content-Type": "application/json"
            }
            if uc_endpoint:
                headers["Authorization"] = f"Bearer {uc_token}"
            
            # Create new table
            table_definition = {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "name": table_name,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": external_location
            }
            
            # Add columns if provided
            if columns:
                table_definition["columns"] = [
                    {
                        "name": col["name"],
                        "type_name": col["type"],
                        "type_text": col["type"],
                        "position": pos,
                        "nullable": col.get("nullable", True),
                        "type_interval_type": "string",
                        "type_json": "string",
                        "comment": col.get("description")
                    } for pos, col in enumerate(columns)
                ]

            # Add comment if provided
            if comment:
                if columns_in_table_description:
                    comment += "\n\nColumns:\n"
                    comment += "\n".join(
                        f"\t -{col.get('name')} ({col.get('type')}: {col.get('description')})"
                        for col in columns
                    )
                table_definition["comment"] = comment
                    
            
            # Construct the full URL for the Tables API
            api_url = urljoin(uc_endpoint, "tables")
                        
            # Construct the URL to get table details
            table_url = urljoin(api_url, "/", f"{catalog_name}.{schema_name}.{table_name}")
            
            response = requests.get(table_url, headers=headers)
            table_exists = response.status_code == 200
            
            if table_exists:
                print("Table already exists. Checking for metadata update...")
                
                # Get existing table details
                existing_table: dict = response.json()
                existing_table_definition = existing_table.fromkeys(
                    ["catalog_name", "schema_name", "name", "table_type", "data_source_format", "columns", "comment"]
                )
                
                if existing_table_definition != table_definition:
                    # Delete existing table
                    delete_response = requests.delete(table_url, headers=headers)
                    if delete_response.status_code not in [200, 204]:
                        raise Exception(f"Warning: Failed to delete existing table: {delete_response.text}")
                    
                    # Recreate table
                    create_response = requests.post(api_url, json=table_definition, headers=headers)
                    if create_response.status_code not in [200, 204]:
                        raise Exception(f"Warning: Failed to recreate table: {create_response.text}")
            
            else:
                response = requests.post(
                    api_url,
                    headers=headers,
                    json=table_definition
                )
                
                if response.status_code not in [200, 201]:
                    raise Exception(f"Failed to create table: {response.text}")
                
        except ImportError:
            # Fallback for environments without requests library
            print("Warning: requests library not available. Skipping table creation.")
        # except Exception as e:
        #     print(f"Error creating/updating table via Unity Catalog API: {str(e)}")
            
            
    def write_data(self, df: pl.DataFrame):
            
        table_config = self.table_config
                    
        if table_config:
            self._create_or_update_table(
                catalog_name=table_config.get("catalog"),
                schema_name=table_config.get("schema"),
                table_name=table_config.get("name"),
                comment=table_config.get("description"),
                columns=table_config.get("columns"),
                external_location=self.table_path
            )

        if isinstance(df, pl.DataFrame):
            df.write_delta(
                self.table_path,
                mode=self.mode,
                overwrite_schema=self.overwrite_schema,
                storage_options={
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                    "AWS_REGION": "gra",
                    "AWS_COPY_IF_NOT_EXISTS": os.getenv("AWS_COPY_IF_NOT_EXISTS"),
                    "retry_timeout": "600s",
                    "retries": "0",
                },
            )
        else:
            raise Exception("Unsupport DataFrame type: ", type(df))

        return self.table_path
