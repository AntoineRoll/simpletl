from simpletl.destinations.delta import DeltaTableDestination
from simpletl.destinations.parquet import ParquetFileDestination


destinations = {
    "delta": DeltaTableDestination,
    "parquet": ParquetFileDestination,
}