"""
TMKP (Text Mining Knowledge Provider) ingest.

This ingest processes text-mined assertions from the Translator Text Mining Provider.
Data comes as tar.gz archives containing:
- nodes.tsv: Entity information
- edges.tsv: Relationships between entities
- content_metadata.json: Biolink class and slot mappings

"""

import json
from typing import Any, Dict, List
from loguru import logger

import koza
from koza.model.graphs import KnowledgeGraph

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    Protein,
    SmallMolecule,
    Disease,
    MolecularMixture,
    PhenotypicFeature,
    ComplexMolecularMixture,
    NamedThing,
    Association,
    ChemicalAffectsGeneAssociation,
    GeneToDiseaseAssociation,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    GeneRegulatesGeneAssociation,
    Study,
    TextMiningStudyResult,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from translator_ingest.util.biolink import INFORES_TEXT_MINING_KP


# Map biolink classes from content_metadata.json
BIOLINK_CLASS_MAP = {
    "biolink:ChemicalEntity": ChemicalEntity,
    "biolink:Protein": Protein,
    "biolink:SmallMolecule": SmallMolecule,
    "biolink:Disease": Disease,
    "biolink:MolecularMixture": MolecularMixture,
    "biolink:PhenotypicFeature": PhenotypicFeature,
    "biolink:ComplexMolecularMixture": ComplexMolecularMixture,
    "biolink:NamedThing": NamedThing,
}

# Map edge types to association classes
ASSOCIATION_MAP = {
    "biolink:ChemicalToGeneAssociation": ChemicalAffectsGeneAssociation,
    "biolink:GeneToDiseaseAssociation": GeneToDiseaseAssociation,
    "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation": ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    "biolink:GeneRegulatoryRelationship": GeneRegulatesGeneAssociation,
}


def get_latest_version() -> str:
    """Return the latest version identifier for TMKP data."""
    return "tmkp-2023-03-05"


def parse_attributes(attributes: List[Dict[str, Any]], association: Association) -> List[TextMiningStudyResult]:
    """
    Parse attribute objects and extract supporting studies.

    Attributes can contain nested attributes representing TextMiningStudyResult objects.
    This function processes them and returns a list of TextMiningStudyResult objects.
    """
    text_mining_results = []

    for attr in attributes:
        attr_type = attr.get("attribute_type_id", "")
        value = attr.get("value")

        # Map simple attributes to association slots if they exist
        if hasattr(association, attr_type):
            setattr(association, attr_type, value)

        # Handle supporting study results
        if attr_type == "biolink:supporting_study_result":
            # Create TextMiningStudyResult object
            tm_result = TextMiningStudyResult(
                id=value,
                category=["biolink:TextMiningStudyResult"]
            )

            # Process nested attributes for this result
            nested_attrs = attr.get("attributes", [])
            for nested in nested_attrs:
                nested_type = nested.get("attribute_type_id", "")
                nested_value = nested.get("value")

                if nested_type == "biolink:supporting_text":
                    tm_result.supporting_text = [nested_value] if nested_value else []
                elif nested_type == "biolink:supporting_document":
                    # Store document ID in xref field for now
                    tm_result.xref = [nested_value] if nested_value else []

            text_mining_results.append(tm_result)

    return text_mining_results


@koza.transform_record(tag="nodes")
def transform_tmkp_node(koza_transform: koza.KozaTransform, record: Dict[str, Any]) -> KnowledgeGraph | None:
    """Transform TMKP node records."""
    try:
        node_id = record.get("id")
        name = record.get("name")
        category = record.get("category")

        if not all([node_id, category]):
            return None

        # Get appropriate class from mapping
        node_class = BIOLINK_CLASS_MAP.get(category, NamedThing)

        # Create node
        node = node_class(
            id=node_id,
            name=name,
            category=node_class.model_fields["category"].default
        )

        # Return node in graph
        return KnowledgeGraph(nodes=[node])

    except Exception as e:
        logger.error(f"Error processing node: {e}")
        return None


@koza.transform_record(tag="edges")
def transform_tmkp_edge(koza_transform: koza.KozaTransform, record: Dict[str, Any]) -> KnowledgeGraph | None:
    """Transform TMKP edge records with attribute parsing."""
    try:
        subject_id = record.get("subject")
        predicate = record.get("predicate")
        object_id = record.get("object")
        relation = record.get("relation")

        if not all([subject_id, predicate, object_id]):
            return None

        # Get association class
        assoc_class = ASSOCIATION_MAP.get(relation, Association)

        # Build association kwargs with all fields
        assoc_kwargs = {
            "id": record.get("id", entity_id()),
            "subject": subject_id,
            "predicate": predicate,
            "object": object_id,
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.text_mining_agent,
        }
        
        # Add all qualifiers to kwargs if present
        if qualified_pred := record.get("qualified_predicate"):
            assoc_kwargs["qualified_predicate"] = qualified_pred
        elif assoc_class == GeneRegulatesGeneAssociation:
            # For GeneRegulatesGeneAssociation, use predicate as qualified_predicate if not provided
            assoc_kwargs["qualified_predicate"] = predicate
            
        # Add all other qualifiers
        for qualifier in ["subject_aspect_qualifier", "subject_direction_qualifier",
                         "object_aspect_qualifier", "object_direction_qualifier"]:
            if value := record.get(qualifier):
                assoc_kwargs[qualifier] = value
        
        # For GeneRegulatesGeneAssociation, require object_aspect_qualifier and object_direction_qualifier.
        if assoc_class == GeneRegulatesGeneAssociation:
            # If either qualifier is missing, skip the edge to avoid semantic errors.
            if "object_aspect_qualifier" not in assoc_kwargs or "object_direction_qualifier" not in assoc_kwargs:
                logger.warning(
                    "Skipping GeneRegulatesGeneAssociation edge due to missing qualifiers: "
                    f"object_aspect_qualifier={assoc_kwargs.get('object_aspect_qualifier')}, "
                    f"object_direction_qualifier={assoc_kwargs.get('object_direction_qualifier')}. "
                    "These qualifiers are required for semantic correctness."
                )
                return None
        # Create association with all fields
        association = assoc_class(**assoc_kwargs)

        # Parse attributes JSON
        if attributes_json := record.get("_attributes"):
            attributes = json.loads(attributes_json)

            # Extract supporting studies
            text_mining_results = parse_attributes(attributes, association)

            # Create a Study object to contain the TextMiningStudyResult objects
            if text_mining_results:
                # Create a single Study that contains all the text mining results
                study = Study(
                    id=entity_id(),  # Generate unique ID for this study
                    category=["biolink:Study"],
                    has_study_results=text_mining_results
                )
                # Add the Study to the association
                studies_dict = {study.id: study}
                association.has_supporting_studies = studies_dict

        # Add knowledge sources
        association.sources = build_association_knowledge_sources(
            primary=INFORES_TEXT_MINING_KP,
            supporting=["infores:pubmed"]
        )

        # Create nodes for subject and object
        nodes = []

        # Create subject node (we don't have name info in edges, so minimal node)
        subject_node = NamedThing(id=subject_id)
        nodes.append(subject_node)

        # Create object node
        object_node = NamedThing(id=object_id)
        nodes.append(object_node)

        # Return graph with nodes and edges
        return KnowledgeGraph(nodes=nodes, edges=[association])

    except Exception as e:
        logger.error(f"Error processing edge: {e}")
        return None
