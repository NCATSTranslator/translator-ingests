from typing import Any

import requests
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Cell,
    AnatomicalEntity,
    Gene,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from translator_ingest.util.biolink import INFORES_BGEE, entity_id, build_association_knowledge_sources

from bs4 import BeautifulSoup
from koza.model.graphs import KnowledgeGraph

BIOLINK_EXPRESSED_IN = "biolink:expressed_in"

def get_latest_version() -> str:
    return "bgee_v15_0"

@koza.on_data_begin(tag="bgee_expressed_in")
def on_data_begin_bgee(koza_transform: koza.KozaTransform) -> None:
    """
    Called before processing begins.
    Can be used for setup or validation of input files.
    """
    koza_transform.log("Starting Bgee processing")
    koza_transform.log(f"Version: {get_latest_version()}")

@koza.on_data_end(tag="bgee_expressed_in")
def on_data_end_bgee(koza_transform: koza.KozaTransform):
    """
    Called after all data has been processed.
    Used for logging summary statistics.
    """
    koza_transform.log("Bgee processing complete")

@koza.transform_record(tag="bgee_expressed_in")
def transform_bgee_expressed_in(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform a Bgee genetic expression relationship entry into a
    Biolink Model-compliant anotomical entity/cell to gene knowledge graph statement.

    :param koza_transform: KozaTransform object (unused in this implementation)
    :param record: Dict contents of a single input data record
    :return: koza.model.graphs.KnowledgeGraph wrapping nodes (NamedThing) and edges (Association)
    """
    gene_id = f"Ensembl:{record['Gene ID']}"
    anatomical_id = record['Anatomical entity ID']
    

    gene_node = Gene(id=gene_id,**{})
    entity_node = None
    if(anatomical_id.startswith("CL:")):
        entity_node = Cell(id=anatomical_id,**{})
    elif(anatomical_id.startswith("UBERON:")):
        entity_node = AnatomicalEntity(id=anatomical_id,**{})
    else:
        raise ValueError(f"In Bgee Ingest; 'Anatomical entity ID' {anatomical_id} does not start with 'CL:' or 'UBERON:'.")
    # Generate our association objects
    association = Association(
        id=entity_id(),
        subject=gene_id,
        predicate=BIOLINK_EXPRESSED_IN,
        object=anatomical_id,
        sources=build_association_knowledge_sources(primary=INFORES_BGEE),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.automated_agent,
    )
    return KnowledgeGraph(nodes=[gene_node,entity_node], edges=[association])
