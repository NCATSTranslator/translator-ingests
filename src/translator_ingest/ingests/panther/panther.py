"""
Ingest of Reference Genome Orthologs from Panther
This code is inspired and adapted from various sources:

1. Monarch Database Ingest: https://github.com/monarch-initiative/pantherdb-orthologs-ingest
2. Biothings MyGenes Ingest: https://github.com/biothings/mygene.info/tree/7181b3d46aa76d3e234cdbe212192b2cabd325ed/src/plugins/pantherdb
3. Automat-Panther TRAPI service: https://robokop.renci.org/api-docs/docs/automat/panther
"""
from typing import Any, Optional
from loguru import logger
import re
import requests

from bs4 import BeautifulSoup

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneHomologyAssociation,
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from translator_ingest.util.biolink import (
    entity_id,
    build_association_knowledge_sources
)

import koza
from koza.model.graphs import KnowledgeGraph

# Custom pantherdb specific function, and constants respectively
from translator_ingest.ingests.panther.panther_orthologs_utils import (
    # make_ncbi_taxon_gene_map,
    # NCBI_MAP_FILE_PATH,
    parse_gene_info,
    panther_taxon_map,
    # relevant_ncbi_cols,
    # relevant_ncbi_taxons,
    db_to_curie_map
)

# Hacky cache for the latest version number
panther_data_version: Optional[str] = None

def get_latest_version() -> str:
    """
    Code cannibalized from https://github.com/RobokopU24/ORION/blob/master/parsers/panther/src/loadPanther.py
    :return: String representing the Panther version number
    """
    global panther_data_version

    if panther_data_version is not None:
        return panther_data_version

    # init the return
    ret_val: str = 'Not found'

    # load the web page for Panther sequence data which has file names with an embedded version number
    html_page: requests.Response = requests.get(
        'http://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/')

    # get the html into a parsable object
    resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

    # set the search text
    search_text = 'PTHR*'

    # find the version tag
    a_tag: BeautifulSoup.Tag = resp.find('a', string=re.compile(search_text))

    # Check if the tag was found?
    if a_tag is not None:
        # strip off the search text
        val = a_tag.text.split(search_text[:-1])[1].strip()

        # get the actual version number
        ret_val = val.split('_')[0]

        # save the version for data gathering later
        panther_data_version = ret_val

    # return to the caller
    return ret_val


@koza.on_data_begin(tag="gene_orthology")
def on_data_begin_gene_orthology(koza_transform: koza.KozaTransform) -> None:
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza_transform.log("Starting Panther Gene Orthology processing")
    koza_transform.log(f"Version: {get_latest_version()}")

    # TODO: ported from the Monarch ingest... not sure if and
    #       how these are to be used, so we ignore them for now.
    # koza_transform.state["species_pair_min"] = {}
    # koza_transform.state["species_pair_max"] = {}
    # koza_transform.state["species_pair_stats"] = {}

    # RMB: 29-Sept-2025: we skip NCBI Gene lookup for now
    # koza_transform.extra_fields["ntg_map"] = make_ncbi_taxon_gene_map(
    #     gene_info_file=NCBI_MAP_FILE_PATH,
    #     relevant_columns=relevant_ncbi_cols,
    #     taxon_catalog=relevant_ncbi_taxons
    # )

@koza.on_data_end(tag="gene_orthology")
def on_data_end_gene_orthology(koza_transform: koza.KozaTransform) -> None:
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza_transform.log("Panther Gene Orthology processing complete")


@koza.transform_record(tag="gene_orthology")
def transform_gene_orthology(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform a Panther protein orthology relationship entry into a
    Biolink Model-compliant gene to gene orthology knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: koza.model.graphs.KnowledgeGraph wrapping nodes (NamedThing) and edges (Association)
    """
    try:
        # Parse the gene information for both species and format gene id to curie:gene_id
        # (Gene and Ortholog columns are formatted the same, but for different species/gene info)
        species_a, gene_a_id = parse_gene_info(
            record["Gene"],
            panther_taxon_map,
            db_to_curie_map,
            # koza_transform.extra_fields["ntg_map"]  # we skip NCBI Gene lookup for now
        )
        species_b, gene_b_id = parse_gene_info(
            record["Ortholog"],
            panther_taxon_map,
            db_to_curie_map,
            # koza_transform.extra_fields["ntg_map"]  # we skip NCBI Gene lookup for now
        )

        # Only consume species we are interested in (i.e.,
        # those that are in our NCBI Taxon catalog)
        if (not species_a) or (not species_b):
            return None

        # Format our species names to NCBI Taxon IDs
        ncbitaxon_a = "NCBITaxon:{}".format(panther_taxon_map[species_a])
        ncbitaxon_b = "NCBITaxon:{}".format(panther_taxon_map[species_b])

        gene_a = Gene(id=gene_a_id, in_taxon=[ncbitaxon_a],**{})
        gene_b = Gene(id=gene_b_id, in_taxon=[ncbitaxon_b],**{})

        # Our ortholog identifier (panther protein family name), and predicate
        panther_ortholog_id = record["Panther Ortholog ID"]
        orthology_evidence = ["PANTHER.FAMILY:{}".format(panther_ortholog_id)]

        # Generate our association object
        association = GeneToGeneHomologyAssociation(
            id=entity_id(),
            subject=gene_a.id,
            object=gene_b.id,
            predicate="biolink:orthologous_to",
            has_evidence=orthology_evidence,
            sources=build_association_knowledge_sources(primary="infores:panther"),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.not_provided
        )
        return KnowledgeGraph(nodes=[gene_a, gene_b], edges=[association])

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_record_gene_to_disease() - record: '{str(record)}' " +
            f"with {type(e)} exception: "+str(e)
        )
        return None


@koza.on_data_begin(tag="gene_annotation")
def on_data_begin_gene_annotation(koza_transform: koza.KozaTransform) -> None:
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza_transform.log("Starting Panther Gene Orthology processing")
    koza_transform.log(f"Version: {get_latest_version()}")


@koza.on_data_end(tag="gene_annotation")
def on_data_end_gene_annotation(koza_transform: koza.KozaTransform) -> None:
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza_transform.log("Panther Gene Orthology processing complete")


@koza.transform_record(tag="gene_annotation")
def transform_gene_annotation(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform a Panther protein orthology relationship entry into a
    Biolink Model-compliant gene to gene orthology knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: koza.model.graphs.KnowledgeGraph wrapping nodes (NamedThing) and edges (Association)
    """
    return None