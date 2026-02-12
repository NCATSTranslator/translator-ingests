"""
Ingest of Reference Genome Orthologs from Panther
"""
from typing import Any, Iterable
from pathlib import Path
import requests
import re

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneHomologyAssociation,
    GeneToGeneFamilyAssociation,
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum, GeneFamily
)
from bmt.pydantic import entity_id, build_association_knowledge_sources

import koza
from koza.model.graphs import KnowledgeGraph

from translator_ingest.ingests.panther.panther_orthologs_utils import (
    extract_panther_data_polars,
    GENE_A_ID_COL,
    GENE_B_ID_COL,
    NCBITAXON_A_COL,
    NCBITAXON_B_COL,
    GENE_FAMILY_ID_COL,
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
def on_begin_panther_ingest(koza_transform: koza.KozaTransform) -> None:
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza_transform.log("Starting Panther Gene Orthology processing")
    koza_transform.log(f"Version: {get_latest_version()}")

@koza.on_data_end()
def on_end_panther_ingest(koza_transform: koza.KozaTransform):
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza_transform.log("Panther Gene Orthology processing complete")


@koza.prepare_data()
def prepare_panther_data(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[dict[str, Any]] | None:
    """
    Pre-process Panther RefGenomeOrthologs data using polars.

    Bypasses the Koza input reader to directly read the tar.gz archive,
    filter by target species, and resolve gene CURIEs vectorized with polars.

    :param koza_transform: The koza.KozaTransform context of the data processing.
    :param data: Iterable[dict[str, Any]], @koza.prepare_data() specified input data
                 parameter is not used by the pipeline, hence ignored.
    :return: Iterable[dict[str, Any]], pre-processed records with resolved gene CURIEs
             and taxon IDs ready for record-by-record transform_record processing.
    """
    data_archive_path: Path = koza_transform.input_files_dir / "RefGenomeOrthologs.tar.gz"
    df = extract_panther_data_polars(data_archive_path)
    return df.to_dicts()


@koza.transform_record()
def transform_gene_to_gene_orthology(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform a pre-processed Panther orthology record into a
    Biolink Model-compliant gene to gene orthology knowledge graph statement.

    Records are expected to contain pre-processed fields from prepare_panther_data:
    gene_a_id, gene_b_id, ncbitaxon_a, ncbitaxon_b, gene_family_id.

    :param koza_transform: KozaTransform object
    :param record: Dict contents of a single pre-processed input data record
    :return: koza.model.graphs.KnowledgeGraph wrapping nodes (NamedThing) and edges (Association)
    """
    gene_a_id = record[GENE_A_ID_COL]
    gene_b_id = record[GENE_B_ID_COL]
    ncbitaxon_a = record[NCBITAXON_A_COL]
    ncbitaxon_b = record[NCBITAXON_B_COL]
    gene_family_id = record[GENE_FAMILY_ID_COL]

    gene_a = Gene(id=gene_a_id, in_taxon=[ncbitaxon_a])
    gene_b = Gene(id=gene_b_id, in_taxon=[ncbitaxon_b])

    orthology_evidence = [gene_family_id]
    gene_family = GeneFamily(id=gene_family_id)

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
