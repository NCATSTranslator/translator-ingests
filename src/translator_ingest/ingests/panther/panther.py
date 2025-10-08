"""
Ingest of Reference Genome Orthologs from Panther
"""
from typing import Any
from loguru import logger

# Imports that called by code - commented out - for screen-scraping
# inside and early implementation of get_latest_version() for Panther
# import requests
# from bs4 import BeautifulSoup

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneHomologyAssociation,
    GeneToGeneFamilyAssociation,
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum, GeneFamily
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


def get_latest_version() -> str:
    #
    # TODO: Panther has a "are you human" web sentry blocking simple screen scraping, so
    #       this method is currently only an incompletely implemented inert stub.
    #
    # Panther doesn't provide a great programmatic way to determine the latest version,
    # but it does have a Data Status page with a version on it, formatted somewhat as follows:
    #      <td align="right" class="formLabel">
    #          Current Release: <a href="/news/news20240620.jsp"><b>PANTHER 19.0</b></a>&nbsp;&nbsp;|
    #          ... other stuff...
    #      </td>
    # Thus, we attempt to fetch the HTML and parse it to determine the current version.
    #
    #
    # html_page: requests.Response = requests.get('https://www.pantherdb.org/data/')
    # resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')
    # td_elements: BeautifulSoup.Tag = resp.find_all('td')
    # version_element = None
    # for element in td_elements:
    #     if "Current Release" in element.text:
    #         version_element = element
    #         break
    # if version_element is not None:
    #     # Version tagging is something like "Current Release: <a href="/news/news20240620.jsp"><b>PANTHER 19.0</b>"
    #     return version_element.text.split('PANTHER ')[1].split("</b>")[0]
    # else:
    #     raise RuntimeError('Could not determine latest version for Panther. Version block not found in HTML.')
    return "19.0"  # Hard-coded release as of September 25, 2025


@koza.on_data_begin()
def on_data_begin_panther(koza_transform: koza.KozaTransform) -> None:
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

@koza.on_data_end()
def on_data_end_panther(koza_transform: koza.KozaTransform):
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza_transform.log("Panther Gene Orthology processing complete")

# Ingests must implement a function decorated with @koza.transform() OR @koza.transform_record() (not both).
# These functions should contain the core data transformation logic generating and returning NamedThings (nodes) and
# Associations (edges) from source data.
#
# The transform_record function takes the KozaTransform and a single record, a dictionary typically corresponding to a
# row in a source data file, and returns a tuple of NamedThings and Associations. Any number of NamedThings and/or
# Associations can be returned.
@koza.transform_record()
def transform_gene_to_gene_orthology(
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
        gene_family_id = "PANTHER.FAMILY:{}".format(panther_ortholog_id)
        orthology_evidence = [gene_family_id]

        gene_family = GeneFamily(id=gene_family_id, **{})

        # Generate our association objects
        orthology_relationship = GeneToGeneHomologyAssociation(
            id=entity_id(),
            subject=gene_a.id,
            object=gene_b.id,
            predicate="biolink:orthologous_to",
            has_evidence=orthology_evidence,
            sources=build_association_knowledge_sources(primary="infores:panther"),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_validation_of_automated_agent
        )
        gene_a_family_relationship = GeneToGeneFamilyAssociation(
            id=entity_id(),
            subject=gene_a.id,
            object=gene_family.id,
            predicate="biolink:member_of",
            sources=build_association_knowledge_sources(primary="infores:panther"),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_validation_of_automated_agent
        )
        gene_b_family_relationship = GeneToGeneFamilyAssociation(
            id=entity_id(),
            subject=gene_b.id,
            object=gene_family.id,
            predicate="biolink:member_of",
            sources=build_association_knowledge_sources(primary="infores:panther"),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_validation_of_automated_agent
        )
        return KnowledgeGraph(
            nodes=[
                gene_a,
                gene_b
            ],
            edges=[
                orthology_relationship,
                gene_a_family_relationship,
                gene_b_family_relationship
            ]
        )

    except Exception as e:
        # Catch and report all errors here with messages
        logger.warning(
            f"transform_record_gene_to_disease() - record: '{str(record)}' " +
            f"with {type(e)} exception: "+str(e)
        )
        return None