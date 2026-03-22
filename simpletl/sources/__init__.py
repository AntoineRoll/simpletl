from .csv import CsvSource
from .geojson import WebJsonSource
from .geojson import GeojsonSource
from .shapefile import ShapefileSource
from .xml import LargeXmlSource
from .delta import DeltaSource

sources = {
    "csv": CsvSource,
    "webjson": WebJsonSource,
    "geojson": GeojsonSource,
    "shapefile": ShapefileSource,
    "xml": LargeXmlSource,
    "delta": DeltaSource
}