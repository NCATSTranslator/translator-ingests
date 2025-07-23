import logging
import uuid
from typing import Iterable, Dict, List, Optional, Type, Tuple

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    GeneToGoTermAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    # OntologyClass, while OntologyClass would be semantically more appropriate, Koza's KGX converter only supports NamedThing and Association entities for proper serialization.
    Association,
)

# Constants
INFORES_GOA = "infores:goa"
INFORES_BIOLINK = "infores:biolink"

# Mappings
# Note: Biolink pydantic model doesn't expose predicate constants programmatically from the YAML slots section,
# so we use hardcoded mappings. This could be enhanced if biolink-model adds predicate registry in future versions.
# The YAML file contains predicate definitions like "participates in:", "enables:", "located in:" in the slots section,
# but these are not exposed as constants in the generated biolink pydantic model.
ASPECT_TO_PREDICATE = {
    "P": "biolink:participates_in",  # Biological Process
    "F": "biolink:enables",          # Molecular Function  
    "C": "biolink:located_in",       # Cellular Component
}

# All GO aspects use the same association class for consistency
# This allows predicate-based differentiation rather than association class differentiation
ASPECT_TO_ASSOCIATION = {
    "P": GeneToGoTermAssociation,
    "F": GeneToGoTermAssociation,
    "C": GeneToGoTermAssociation,
}

# GO evidence codes mapped to biolink knowledge levels and agent types
# Note: Using hardcoded mapping instead of JSON config for simplicity and performance
# The biolink model provides KnowledgeLevelEnum and AgentTypeEnum for validation
EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE = {
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
    Transforms a single GAF record into Biolink nodes and edges using the pydantic model directly.
    
    This function leverages the biolink pydantic model for validation and structure,
    keeping the code simple and biolink-centered while maintaining readability.
    """
    nodes: List[NamedThing] = []
    associations: List[Association] = []

    # Parse GAF record fields
    db_object_id = record["DB_Object_ID"]
    go_id = record["GO_ID"]
    aspect = record["Aspect"]
    db_object_symbol = record["DB_Object_Symbol"]
    qualifier = record["Qualifier"]
    publications = record["DB_Reference"].split("|")
    evidence_code = record["Evidence_Code"]
    taxon = record["Taxon"]

    # Create gene node using biolink pydantic model
    # Biolink pydantic model centric: Uses Gene class from biolink model for automatic validation of required fields,
    # proper type checking, and biolink-compliant structure
    gene = Gene(
        id=f"UniProtKB:{db_object_id}",
        name=db_object_symbol,
        category=["biolink:Gene"],
        in_taxon=[taxon.replace("taxon:", "NCBITaxon:")],
    )
    nodes.append(gene)

    # Create GO term node using biolink pydantic model
    # Biolink pydantic model centric: Uses NamedThing for GO terms due to Koza framework compatibility.
    
    # Biolink pydantic model centric: Uses OntologyClass for GO terms since they are ontology concepts,
    # not physical entities. This is semantically correct and I assume follows biolink model design principles.
    # go_term = OntologyClass(
    #     id=go_id
    # )

    # While OntologyClass would be semantically more appropriate, Koza's KGX converter only supports
    # NamedThing and Association entities for proper serialization. This is a limitation of the Koza framework.
    go_term = NamedThing(
        id=go_id,
        category=["biolink:NamedThing"]
    )
    nodes.append(go_term)

    # Get predicate and association class from mappings
    # Biolink pydantic model centric: Uses biolink predicate IRIs from hardcoded mapping since biolink model 
    # doesn't expose predicate constants from YAML slots section
    predicate = ASPECT_TO_PREDICATE.get(aspect)
    if not predicate:
        logging.warning(f"Unknown aspect '{aspect}' for record: {record}")
        return [], []

    # Biolink-centric: Uses biolink association class for proper structure and validation
    association_class = ASPECT_TO_ASSOCIATION.get(aspect, GeneToGoTermAssociation)

    # Get knowledge level and agent type from evidence code mapping
    # Biolink-centric: Uses biolink KnowledgeLevelEnum and AgentTypeEnum for type safety
    # and automatic validation of biolink-compliant knowledge metadata
    knowledge_level, agent_type = EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE.get(
        evidence_code, 
        (KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided)
    )

    # Format publications as CURIEs using biolink conventions
    # Biolink pydantic model centric: Formats publication IDs as proper CURIEs following biolink model conventions
    # for consistent identifier representation across the knowledge graph
    publications_list = []
    for p in publications:
        if p:
            if p.startswith("PMID:"):
                publications_list.append(p)
            else:
                publications_list.append(f"PMID:{p}")

    # Create association using biolink pydantic model directly
    # Biolink pydantic model centric: Uses biolink association class constructor for automatic validation,
    # proper field type checking, and biolink-compliant association structure
    # Note: in_taxon is not used on associations because GeneToGoTermAssociation 
    # doesn't include the 'thing with taxon' mixin in the biolink model.
    # The taxon information is captured on the gene node instead.
    association = association_class(
        id=str(uuid.uuid4()),
        subject=gene.id,
        predicate=predicate,
        object=go_term.id,
        negated="NOT" in qualifier,
        has_evidence=[f"ECO:{evidence_code}"],  # Biolink pydantic model centric: Formats evidence as ECO CURIE
        publications=publications_list,
        primary_knowledge_source=INFORES_GOA,
        aggregator_knowledge_source=[INFORES_BIOLINK],
        knowledge_level=knowledge_level,
        agent_type=agent_type,
    )
    associations.append(association)

    return nodes, associations