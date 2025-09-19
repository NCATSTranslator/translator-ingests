import uuid
import koza
from typing import Any, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneHomologyAssociation,
    Gene,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association
)
from translator_ingest.util.biolink import (
    entity_id,
    build_association_knowledge_sources
)
from koza.model.graphs import KnowledgeGraph

# Custom pantherdb specific function, and constants respectively
from panther_orthologs_utils import (
    make_ncbi_taxon_gene_map,
    parse_gene_info,
    panther_taxon_map,
    relevant_ncbi_cols,
    relevant_ncbi_taxons,
    db_to_curie_map
)


# Retrieve and return a string representing the latest version of the source data.
# If a source does not implement versioning, we need to do it. For static datasets assign a version string
# corresponding to the current version. For sources that are updated regularly, use file modification dates if
# possible or the current date. Versions should (ideally) be sortable (ie YYYY-MM-DD) and should contain no spaces.
def get_latest_version() -> str:
    return "v1"


# Functions decorated with @koza.on_data_begin() or @koza.on_data_end() are optional. If implemented they will be called
# before and/or after transform or transform_record.
@koza.on_data_begin()
def on_data_begin_panther(koza_transform: koza.KozaTransform) -> None:
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza_transform.log("Starting Panther Gene Orthology processing")
    koza_transform.log(f"Version: {get_latest_version()}")

    # TODO: ported from the Monarch ingest... not sure if and how these are to be used
    # koza_transform.state["species_pair_max"] = {}
    # koza_transform.state["species_pair_stats"] = {}

    koza_transform.extra_fields["tx_gmap"] = make_ncbi_taxon_gene_map(
        gene_info_file="./data/ncbi/gene_info.gz",
        relevant_columns=relevant_ncbi_cols,
        taxon_catalog=relevant_ncbi_taxons
    )

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
def transform_ingest_by_record(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform an 'AllOrthologs.tar.gz' data entry into a
    (Pydantic encapsulated) Biolink knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: koza.model.graphs.KnowledgeGraph wrapping nodes (NamedThing) and edges (Association)
    """
    # Parse the gene information for both species and format gene id to curie:gene_id
    # (Gene and Ortholog columns are formatted the same, but for different species/gene info)
    species_a, gene_a_id = parse_gene_info(
        record["Gene"],
        panther_taxon_map,
        db_to_curie_map,
        koza_transform.extra_fields["tx_gmap"]
    )
    species_b, gene_b_id = parse_gene_info(
        record["Ortholog"],
        panther_taxon_map,
        db_to_curie_map,
        koza_transform.extra_fields["tx_gmap"]
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
