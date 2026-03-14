from .sql import SqlQueryTransformation
from .polars import CollectLazyDataframeTransformation

transformations = {
    "sql_query": SqlQueryTransformation,
    "collect_lazy_dataframe": CollectLazyDataframeTransformation
}