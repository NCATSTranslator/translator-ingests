"""
Open Health Data - Carolina ingest parser
(adapted a bit from COHD and HMBD ingests)
"""
from typing import Any, Iterable
from pathlib import Path

from zipfile import ZipFile
import csv

from biolink_model.datamodel.pydanticmodel_v2 import MolecularEntity

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing
)

import koza
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    get_biolink_model_toolkit
)

bmt = get_biolink_model_toolkit()


def get_latest_version() -> str:
    return "2026-06-29"  # Temporary placeholder version


_ohdc_nodes: dict[str, NamedThing] = {}

@koza.transform()
def transform_ohdc_ingest(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    """
    Given that OHD@Carolina is a zip archive wrapping a csv file,
    that we process as a streaming knowledge source design pattern.
    """
    # We actually ignore the input data Iterable,
    # assuming that Koza didn't bother pre-processing
    # the downloaded OHD@Carolina file, leaving it to us here
    if koza_transform.input_files_dir is None:
        raise ValueError("input_files_dir must be set for OHD@Carolina ingest")

    ohdc_data_archive_path: Path = koza_transform.input_files_dir / "unc_omop_2018_2022_kg.zip"

    with ZipFile(ohdc_data_archive_path) as zf:
        # open the OHD@Carolina CSV file
        with open('unc_omop_2018_2022_kg.csv', newline="") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                # TODO: process each row of the CSV file
                yield KnowledgeGraph(nodes=[], edges=[])
