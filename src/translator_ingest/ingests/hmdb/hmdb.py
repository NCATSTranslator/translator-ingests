"""
Human Metabolome Database (HMDB) data ingest script.
Derived from https://github.com/RobokopU24/ORION/blob/master/parsers/hmdb/src/loadHMDB.py
with modifications as required from other KPs identified by the Phase 2 ingest survey
"""
from typing import Any, Iterable
from pathlib import Path
import re

import requests

from bs4 import BeautifulSoup
from zipfile import ZipFile
import xml.etree.cElementTree as E_Tree

import koza

from biolink_model.datamodel.pydanticmodel_v2 import MolecularEntity

from koza.model.graphs import KnowledgeGraph

from translator_ingest.ingests.hmdb.hmdb_ingest_utils import (
    read_xml_file,
    get_genes,
    get_diseases,
    get_pathways
)

def get_latest_version() -> str:
    """
    Gets the version of the HMDB data.
    :return: Str, version in format yyyy-mm-dd
    """
    # Original version code from RENCI Orion code "get_latest_source_version" method.
    # Needs to be updated to use the new HMDB download page.
    #
    # this grabs the html from the download page and searches for the Current Version on it
    # html_page: requests.Response = requests.get('https://hmdb.ca/downloads')
    # html_page.raise_for_status()
    #
    # resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')
    # search_text = 'Current Version '
    # div_tag = resp.find(name='a', string=re.compile('Current Version'))
    # if div_tag:
    #     latest_version = div_tag.text.split(search_text)[1].strip('() ')
    #     return latest_version
    # else:
    #     raise Exception("Version could not be determined from html parsing for HMDB.")

    # Returning hard coded version for now,
    # pending repair of the above dynamic version discovery code.
    return "2021-11-17"

@koza.on_data_begin()
def on_begin_ingest_by_record(koza_transform: koza.KozaTransform) -> None:

    # koza.transform_metadata is a dictionary that can be used to save arbitrary metadata, the contents of  which will
    # be copied to metadata output files. transform_metadata persists across all tagged transforms for a source.
    koza_transform.transform_metadata["ingest_by_record"] = {
        "records_input": 0,
        "records_skipped": 0,
    }

def count(koza_transform: koza.KozaTransform, tag: str):
    koza_transform.transform_metadata["ingest_by_record"][tag] += 1

def count_record(koza_transform: koza.KozaTransform):
    count(koza_transform, "records_input")

def count_skipped(koza_transform: koza.KozaTransform, msg: str):
    count(koza_transform, "records_skipped")
    koza_transform.log(msg=msg, level="DEBUG")


@koza.transform()
def transform_ingest_all_streaming(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    """
    Given that HMDB is a zip archive wrapping an XML file, through which we iterate over metabolites,
    we rather might sd well process this a streaming knowledge source design pattern.
    """
    # We actually ignore the input data Iterable,
    # assuming that Koza didn't bother pre-processing
    # the downloaded HMDB file, leaving it to us here
    if koza_transform.input_files_dir is None:
        raise ValueError("input_files_dir must be set for HMDB ingest")

    hmdb_data_archive_path: Path = koza_transform.input_files_dir / "hmdb_metabolites.zip"

    with ZipFile(hmdb_data_archive_path) as zf:
        # open the hmdb xml file
        with zf.open('hmdb_metabolites.xml', 'r') as fp:
            # loop through, filtering for relevant elements
            for record in read_xml_file(koza_transform, fp, 'metabolite'):

                count_record(koza_transform)

                # convert the xml text into an object
                el: E_Tree.Element = E_Tree.fromstring(record)

                # get the metabolite element
                metabolite_accession: E_Tree.Element = el.find('accession')

                # did we get a good value?
                if metabolite_accession is not None and metabolite_accession.text is not None:
                    # create a valid curie for the metabolite id
                    metabolite_id = f"HMDB:{metabolite_accession.text}"

                    # get the metabolite name element
                    metabolite_name: E_Tree.Element = el.find('name')

                    # did we get a good value?
                    if metabolite_name is not None and metabolite_name.text is not None:

                        # get the nodes and edges for the pathways
                        pathways = get_pathways(koza_transform, el, metabolite_id)

                        # get nodes and edges for the diseases
                        diseases = get_diseases(koza_transform, el, metabolite_id)

                        # get the nodes and edges for genes
                        genes = get_genes(koza_transform, el, metabolite_id)

                        # did we get something created?
                        if pathways or diseases or genes:
                            nodes: list = []
                            edges: list = []

                            nodes.extend([entry[0] for entry in pathways])
                            edges.extend([entry[1] for entry in pathways])
                            nodes.extend([entry[0] for entry in diseases])
                            edges.extend([entry[1] for entry in diseases])
                            nodes.extend([entry[0] for entry in genes])
                            edges.extend([entry[1] for entry in genes])

                            # create the common metabolite node and add it to the list
                            nodes.append(
                                MolecularEntity(
                                    id=metabolite_id,
                                    name=metabolite_name.text
                                            .encode('ascii',errors='ignore')
                                            .decode(encoding="utf-8")
                                )
                            )
                            # send the metabolite-specific subgraph out to the data ingest stream
                            yield KnowledgeGraph(nodes=nodes, edges=edges)

                        else:
                            count_skipped(
                                koza_transform,
                                msg=f'Metabolite {metabolite_id} record skipped '+
                                    'due to no pathway, disease or gene data.'
                            )
                    else:
                        count_skipped(
                            koza_transform,
                            msg=f'Metabolite {metabolite_id} record skipped due to invalid metabolite name.'
                        )
                else:
                    count_skipped(
                        koza_transform,
                        msg=f'Record skipped due to invalid metabolite id: {record}'
                    )
