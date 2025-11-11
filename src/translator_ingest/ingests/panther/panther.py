"""
Ingest of Reference Genome Orthologs from Panther
"""
from typing import Any
import requests
import re
from loguru import logger

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

from translator_ingest.ingests.panther.panther_orthologs_utils import (
    parse_gene_info,
    panther_taxon_map,
    db_to_curie_map
)


def get_latest_version() -> str:
    try:
        response = requests.get(
            "https://data.pantherdb.org/ftp/ortholog/current_release/README"
        )
        response.raise_for_status()
        text = response.text
        match = re.search(pattern=r"version:\s*v\.(\d+\.\d+)", string=text)
        if match:
            version = match.group(1)
            print(f"Extracted version: {version}")
            return version
        else:
            raise RuntimeError("Version match not found.")

    except (requests.RequestException, RuntimeError) as e:
        print(f"Could not determine latest version for Panther: {e}")
        return "unknown"

@koza.on_data_begin()
def on_data_begin_panther(koza_transform: koza.KozaTransform) -> None:
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza_transform.log("Starting Panther Gene Orthology processing")
    koza_transform.log(f"Version: {get_latest_version()}")

@koza.on_data_end()
def on_data_end_panther(koza_transform: koza.KozaTransform):
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza_transform.log("Panther Gene Orthology processing complete")


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
            db_to_curie_map
        )
        species_b, gene_b_id = parse_gene_info(
            record["Ortholog"],
            panther_taxon_map,
            db_to_curie_map
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
                gene_b,
                gene_family
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