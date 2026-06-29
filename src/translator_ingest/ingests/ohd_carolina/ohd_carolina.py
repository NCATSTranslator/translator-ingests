"""
Open Health Data - Carolina ingest parser
(adapted from COHD ingestion)
"""
from typing import Optional, Any

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Study,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from bmt.pydantic import get_node_class
from translator_ingest.util.transform_utils import entity_id

import koza
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    get_biolink_model_toolkit,
    knowledge_sources_from_trapi
)

from translator_ingest.ingests.cohd.cohd_util import (
    parse_node_properties,
    get_cohd_supporting_study
)

bmt = get_biolink_model_toolkit()


def get_latest_version() -> str:
    return "2026-06-29"  # Temporary placeholder version


_cohd_nodes: dict[str, NamedThing] = {}

@koza.transform()
def transform_hmdb_ingest(
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

                count_input(koza_transform)

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
                                tag='Missing pathway, disease and gene data'
                            )
                    else:
                        count_skipped(
                            koza_transform,
                            tag='Invalid metabolite name'
                        )
                else:
                    count_skipped(
                        koza_transform,
                        tag='Invalid metabolite ID'
                    )
