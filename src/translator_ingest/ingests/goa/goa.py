import logging
import uuid
from typing import Iterable, Dict, List

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    GeneToGoTermAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    Association,
)

# Constants
INFORES_GOA = "infores:goa"
INFORES_BIOLINK = "infores:biolink"

# Mappings
ASPECT_TO_PREDICATE = {
    "P": "biolink:participates_in",
    "F": "biolink:enables",
    "C": "biolink:located_in",
}

ASPECT_TO_ASSOCIATION = {
    "P": GeneToGoTermAssociation,
    "F": GeneToGoTermAssociation,
    "C": GeneToGoTermAssociation,
}

GOA_EVIDENCE_CODE_TO_KL_AT = {
    "EXP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IDA": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IPI": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IMP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IGI": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IEP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HTP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HDA": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HMP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HGI": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HEP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IBA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IBD": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IKR": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IRD": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISS": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISO": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISM": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IGC": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "RCA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "TAS": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "NAS": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IC": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ND": (KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided),
    "IEA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.automated_agent),
}

def transform_record(record: Dict) -> (Iterable[NamedThing], Iterable[Association]):
    """
    Transforms a single GAF record into Biolink nodes and edges.
    """
    nodes: List[NamedThing] = []
    associations: List[Association] = []

    # parse gene and taxon info
    db_object_id = record["DB_Object_ID"]
    go_id = record["GO_ID"]
    aspect = record["Aspect"]
    db_object_symbol = record["DB_Object_Symbol"]
    qualifier = record["Qualifier"]
    publications = record["DB_Reference"].split("|")
    evidence_code = record["Evidence_Code"]
    taxon = record["Taxon"]
    creation_date = record["Date"]

    # create nodes for gene and go term
    gene = Gene(
        id=f"UniProtKB:{db_object_id}",
        name=db_object_symbol,
        category=["biolink:Gene"],
        in_taxon=[taxon.replace("taxon:", "NCBITaxon:")],
    )
    nodes.append(gene)

    go_term = NamedThing(id=go_id, category=["biolink:NamedThing"])
    nodes.append(go_term)

    # skip if aspect is missing or not relevant
    predicate = ASPECT_TO_PREDICATE.get(aspect)
    if not predicate:
        logging.warning(f"Unknown aspect '{aspect}' for record: {record}")
        return [], []

    # get eco evidence code
    knowledge_level, agent_type = GOA_EVIDENCE_CODE_TO_KL_AT.get(
        evidence_code, (KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided)
    )

    # fallback if eco term missing
    if not knowledge_level or not agent_type:
        logging.warning(f"Unknown evidence code '{evidence_code}' for record: {record}")
        knowledge_level = KnowledgeLevelEnum.not_provided
        agent_type = AgentTypeEnum.not_provided

    # get predicate from qualifier or aspect
    if "NOT" in qualifier:
        predicate = "biolink:negatively_regulates"

    # skip if predicate missing
    if not predicate:
        logging.warning(f"Missing predicate for record: {record}")
        return [], []

    # format knowledge source
    primary_source = INFORES_GOA
    aggregator_sources = [INFORES_BIOLINK]

    # format publications
    publications_list = [f"PMID:{p}" for p in publications if p]

    # create association edge (see note above for in_taxon)
    association = GeneToGoTermAssociation(
        id=str(uuid.uuid4()),
        subject=gene.id,
        predicate=predicate,
        object=go_term.id,
        negated="NOT" in qualifier,
        has_evidence=[f"ECO:{evidence_code}"],
        publications=publications_list,
        primary_knowledge_source=primary_source,
        aggregator_knowledge_source=aggregator_sources,
        knowledge_level=knowledge_level,
        agent_type=agent_type,
        # in_taxon=[taxon.replace("taxon:", "NCBITaxon:")],
        # NOTE: Although the Biolink Model spec (and some documentation) suggests that `in_taxon` can be used on associations,
        # in the current local biolink-model.yaml, and in the `biolink-model.datamodel.pydanticmodel_v2`  `GeneToGoTermAssociation` (and its parent classes) do NOT include the
        # 'thing with taxon' mixin. Only classes with this mixin are allowed to have the `in_taxon` slot.
        # Therefore, per the actual YAML and the generated Python Pydantic models, `in_taxon` is NOT allowed on edges/associations.
        # So I use `in_taxon` on nodes (e.g., Gene, GeneProduct). This is why it is commented out here.
        # The taxon of the association can be inferred from the subject node's in_taxon property.
    )
    associations.append(association)

    return nodes, associations