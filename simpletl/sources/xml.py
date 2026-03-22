from simpletl.abstract import Source
from lxml import etree
import json
import tempfile
import bz2
import requests
import logging
import polars as pl
from gc import collect as gc_collect


def download_large_file(url, destination, chunk_size=65536):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size):
                destination.write(chunk)
        print("File downloaded successfully!")
    except requests.exceptions.RequestException as e:
        print("Error downloading the file:", e)


def elem2dict(node, attributes=True):
    """
    Convert an lxml.etree node tree into a dict.
    """
    result = {}
    if attributes:
        for item in node.attrib.items():
            key, result[key] = item

    for element in node.iterchildren():
        # Remove namespace prefix
        key = element.tag.split("}")[1] if "}" in element.tag else element.tag

        # Process element as tree element if the inner XML contains non-whitespace content
        if element.text and element.text.strip():
            value = element.text
        else:
            value = elem2dict(element)
        if key in result:
            if type(result[key]) is list:
                result[key].append(value)
            else:
                result[key] = [result[key], value]
        else:
            result[key] = value
    return result


def dump_xml_to_ndjson(filename: str) -> str:
    """
    Dump an XML file to an ndjson temporary file

    Args:
        filename (str): filename of the xml file to parse

    Returns:
        str: Name of the temporary file
    """
    first_line = True

    with (
        bz2.open(filename, "rb") as input_file,
        tempfile.NamedTemporaryFile(
            mode="w", delete=False, encoding="utf-8", suffix=".ndjson"
        ) as output_file,
    ):
        n_elem_parsed = 0
        # Use iterparse to process the XML incrementally
        for _, elem in etree.iterparse(
            input_file, events=("end",), tag="{*}page", encoding="utf-8"
        ):
            if not first_line:
                output_file.write("\n")
            output_file.write((json.dumps(elem2dict(elem))))
            first_line = False

            elem.clear()

            n_elem_parsed += 1
            if n_elem_parsed % 100_000 == 0:
                print(f"{n_elem_parsed:_d} elements currently parsed.")
                gc_collect()
        print(f"{n_elem_parsed:_d} total elements parsed.")

    return output_file.name


class LargeXmlSource(Source):
    def __init__(self, config: dict):
        self.url = config.get("source", {}).get("url")

    def read_data(self,):
        
        logging.info("Getting data from %s", self.url)

        # Download bz2 or xml file to a temporary file
        with tempfile.NamedTemporaryFile() as xml_file:
            download_large_file(self.url, xml_file)
            logging.info("Downloaded file from %s to %s", self.url, xml_file.name)

            # Dump to ndjson for lazy transformations later
            ndjson_filename = dump_xml_to_ndjson(xml_file.name)

        return pl.scan_ndjson(ndjson_filename)
