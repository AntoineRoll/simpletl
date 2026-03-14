from .csv import CsvSource
from .webjson import WebJsonSource
from .webjson import WebGeojsonSource
from .webjson import WebShapefileSource
from .xml import LargeXmlSource
from .delta import DeltaSource

sources = {
    "csv": CsvSource,
    "webjson": WebJsonSource,
    "webgeojson": WebGeojsonSource,
    "shapefile": WebShapefileSource,
    "xml": LargeXmlSource,
    "delta": DeltaSource
}