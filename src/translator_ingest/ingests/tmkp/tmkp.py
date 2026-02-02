"""
TMKP (Text Mining Knowledge Provider) ingest.

This ingest processes text-mined assertions from the Translator Text Mining Provider.
Data comes as tar.gz archives containing:
- nodes.tsv: Entity information
- edges.tsv: Relationships between entities
- content_metadata.json: Biolink class and slot mappings

"""

import json
from functools import lru_cache
from typing import Any, Dict, List, Set, Tuple
from loguru import logger
from bmt import Toolkit
from bmt.utils import parse_name
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
    CorrelatedGeneToDiseaseAssociation,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    GeneRegulatesGeneAssociation,
    Study,
    TextMiningStudyResult,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from translator_ingest.util.biolink import INFORES_TEXT_MINING_KP


# Map TMKP attribute names to Biolink slot names
# These handle cases where TMKP uses different naming than Biolink
# Biolink YAML uses space case ("has evidence count")
# JSON Schema uses snake_case with biolink: prefix ("biolink:has_evidence_count")
# Pydantic expects snake_case field names
TMKP_TO_BIOLINK_SLOT_MAP = {
    # Space case variations (from YAML serialization)
    "has evidence count": "evidence_count",
    "has confidence score": "has_confidence_score",
    # Snake_case variations that need mapping
    "has_evidence_count": "evidence_count",
    "supporting_publications": "publications",
    "supporting_document": "publications",
    "tmkp_confidence_score": "has_confidence_score",
    "semmed_agreement_count": "semmed_agreement_count",  # TMKP-specific, no Biolink equivalent
}

# Track which unmapped attributes we've already warned about (to avoid log spam)
_warned_unmapped_attrs: Set[str] = set()


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
# Note: Using CorrelatedGeneToDiseaseAssociation instead of GeneToDiseaseAssociation
# because text mining identifies correlations from literature, and the base
# GeneToDiseaseAssociation constrains predicates to only 'contributes_to' or
# 'associated_with', which doesn't include 'affects' used by TMKP.
ASSOCIATION_MAP = {
    "biolink:ChemicalToGeneAssociation": ChemicalAffectsGeneAssociation,
    "biolink:GeneToDiseaseAssociation": CorrelatedGeneToDiseaseAssociation,
    "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation": ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    "biolink:GeneRegulatoryRelationship": GeneRegulatesGeneAssociation,
}

# Track edges skipped due to invalid subject/object prefixes (for reporting)
_skipped_edges_by_prefix: Set[Tuple[str, str, str, str]] = set()

# Module-level BMT Toolkit instance (instantiated once)
_toolkit: Toolkit | None = None


def _get_toolkit() -> Toolkit:
    """Get the module-level BMT Toolkit instance."""
    global _toolkit
    if _toolkit is None:
        _toolkit = Toolkit()
    return _toolkit


def _get_id_prefix(curie: str) -> str:
    """
    Extract the prefix from a CURIE.

    >>> _get_id_prefix("MONDO:0008315")
    'MONDO'
    >>> _get_id_prefix("DRUGBANK:DB01248")
    'DRUGBANK'
    >>> _get_id_prefix("invalid")
    ''
    """
    if ":" in curie:
        return curie.split(":")[0]
    return ""


@lru_cache(maxsize=128)
def _get_valid_prefixes_for_class(class_name: str) -> frozenset[str]:
    """
    Get all valid ID prefixes for a Biolink class and its descendants.

    Uses BMT to collect id_prefixes from the class and all its subclasses,
    providing comprehensive prefix coverage for domain/range validation.

    Args:
        class_name: Biolink class name (e.g., 'ChemicalEntity', 'DiseaseOrPhenotypicFeature')

    Returns:
        Frozen set of valid ID prefixes for the class hierarchy.

    >>> 'DRUGBANK' in _get_valid_prefixes_for_class('ChemicalEntity')
    True
    >>> 'MONDO' in _get_valid_prefixes_for_class('DiseaseOrPhenotypicFeature')
    True
    """
    tk = _get_toolkit()
    prefixes: set[str] = set()

    # Get the class and all its descendants
    descendants = tk.get_descendants(class_name, formatted=True) or []
    classes_to_check = [class_name] + [parse_name(d) for d in descendants]

    for cls in classes_to_check:
        elem = tk.get_element(cls)
        if elem and hasattr(elem, "id_prefixes") and elem.id_prefixes:
            prefixes.update(elem.id_prefixes)

    return frozenset(prefixes)


@lru_cache(maxsize=64)
def _get_predicate_domain_range_prefixes(predicate: str) -> tuple[frozenset[str], frozenset[str]] | None:
    """
    Get valid subject (domain) and object (range) prefixes for a predicate.

    Extracts domain and range constraints from the Biolink predicate definition
    and returns the valid ID prefixes for each.

    Args:
        predicate: Biolink predicate CURIE (e.g., 'biolink:treats')

    Returns:
        Tuple of (domain_prefixes, range_prefixes) or None if predicate has no constraints.

    >>> domain_prefixes, range_prefixes = _get_predicate_domain_range_prefixes('biolink:treats')
    >>> 'DRUGBANK' in domain_prefixes  # chemicals can treat
    True
    >>> 'MONDO' in range_prefixes  # diseases can be treated
    True
    """
    tk = _get_toolkit()
    predicate_name = parse_name(predicate)
    elem = tk.get_element(predicate_name)

    if not elem:
        return None

    domain_element = getattr(elem, "domain", None)
    range_element = getattr(elem, "range", None)

    # If no domain/range constraints, we can't validate
    if not domain_element and not range_element:
        return None

    domain_prefixes = _get_valid_prefixes_for_class(domain_element) if domain_element else frozenset()
    range_prefixes = _get_valid_prefixes_for_class(range_element) if range_element else frozenset()

    return domain_prefixes, range_prefixes


def _validate_edge_prefixes(subject_id: str, object_id: str, predicate: str) -> bool:
    """
    Validate that subject and object ID prefixes match the predicate's domain/range.

    Use the Biolink Model's predicate definitions to determine valid subject (domain)
    and object (range) types, then check if the given CURIEs have prefixes that
    belong to those types. This catches semantically invalid edges like
    "MONDO:disease biolink:treats DRUGBANK:drug" (a disease can't treat a chemical).

    Args:
        subject_id: Subject CURIE (e.g., 'DRUGBANK:DB01248')
        object_id: Object CURIE (e.g., 'MONDO:0008315')
        predicate: Biolink predicate CURIE (e.g., 'biolink:treats')

    Returns:
        True if the edge is valid (or no constraints exist), False if it should be skipped.

    >>> _validate_edge_prefixes('DRUGBANK:DB01248', 'MONDO:0008315', 'biolink:treats')
    True
    >>> _validate_edge_prefixes('MONDO:0008315', 'DRUGBANK:DB01248', 'biolink:treats')
    False
    """
    constraints = _get_predicate_domain_range_prefixes(predicate)
    if constraints is None:
        return True

    domain_prefixes, range_prefixes = constraints

    # Only validate if we have constraints to check against
    subject_prefix = _get_id_prefix(subject_id)
    object_prefix = _get_id_prefix(object_id)

    # If domain is constrained, validate subject
    if domain_prefixes and subject_prefix not in domain_prefixes:
        return False

    # If range is constrained, validate object
    if range_prefixes and object_prefix not in range_prefixes:
        return False

    return True


def get_skipped_edges_summary() -> Dict[str, int]:
    """
    Return a summary of edges skipped due to invalid subject/object prefixes.

    Returns:
        Dict mapping pattern strings to counts.
    """
    summary: Dict[str, int] = {}
    for subject_id, predicate, object_id, relation in _skipped_edges_by_prefix:
        key = f"{_get_id_prefix(subject_id)} {predicate} {_get_id_prefix(object_id)} ({relation})"
        summary[key] = summary.get(key, 0) + 1
    return summary


def get_latest_version() -> str:
    """Return the latest version identifier for TMKP data."""
    return "tmkp-2023-03-05"


def parse_attributes(attributes: List[Dict[str, Any]], association: Association) -> None:
    """
    Parse attribute objects and populate the association with supporting studies and knowledge sources.

    Attributes can contain nested attributes representing TextMiningStudyResult objects,
    as well as knowledge source information that should be used to build the sources collection.

    This function mutates the association in place, setting:
    - has_supporting_studies: Dict of Study objects containing TextMiningStudyResult objects
    - sources: Knowledge source attribution built from attribute data or defaults
    - Any other direct attributes found in the attribute list
    """
    text_mining_results: List[TextMiningStudyResult] = []
    primary_source = None
    supporting_sources: List[str] = []

    for attr in attributes:
        attr_type = attr.get("attribute_type_id", "")
        value = attr.get("value")

        # Strip "biolink:" prefix if present to get the actual slot name
        slot_name = attr_type.replace("biolink:", "") if attr_type.startswith("biolink:") else attr_type

        if slot_name == "supporting_study_result":
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
                    tm_result.xref = [nested_value] if nested_value else []
                elif nested_type == "biolink:supporting_text_located_in":
                    tm_result.supporting_text_section_type = nested_value
                elif nested_type == "biolink:extraction_confidence_score":
                    tm_result.extraction_confidence_score = float(nested_value) if nested_value else None
                elif nested_type == "biolink:subject_location_in_text":
                    # Field expects list[int], TMKP sends pipe-delimited string like "42|50"
                    tm_result.subject_location_in_text = (
                        [int(x) for x in nested_value.split("|")]
                        if isinstance(nested_value, str) else (nested_value or [])
                    )
                elif nested_type == "biolink:object_location_in_text":
                    # Field expects list[int], TMKP sends pipe-delimited string like "42|50"
                    tm_result.object_location_in_text = (
                        [int(x) for x in nested_value.split("|")]
                        if isinstance(nested_value, str) else (nested_value or [])
                    )
                elif nested_type == "biolink:supporting_document_year":
                    tm_result.supporting_document_year = int(nested_value) if nested_value else None

            text_mining_results.append(tm_result)

        elif slot_name == "primary_knowledge_source":
            primary_source = value

        elif slot_name == "supporting_data_source":
            if isinstance(value, list):
                supporting_sources.extend(value)
            else:
                supporting_sources.append(value)

        elif hasattr(association, slot_name):
            setattr(association, slot_name, value)

        elif slot_name in TMKP_TO_BIOLINK_SLOT_MAP:
            biolink_slot = TMKP_TO_BIOLINK_SLOT_MAP[slot_name]
            if hasattr(association, biolink_slot):
                # publications field expects a list, but TMKP sends pipe-separated strings
                # Multiple attributes (supporting_publications, supporting_document) may map here
                if biolink_slot == "publications":
                    new_pubs = value.split("|") if isinstance(value, str) else (value or [])
                    existing = getattr(association, biolink_slot) or []
                    setattr(association, biolink_slot, existing + new_pubs)
                else:
                    setattr(association, biolink_slot, value)

        elif attr_type and attr_type not in _warned_unmapped_attrs:
            # Log warning for truly unrecognized attributes (only once per attribute type)
            logger.warning(

                f"Skipping unknown TMKP edge attribute: '{attr_type}' "
                f"(no mapping defined and not a slot on {type(association).__name__})"
            )
            _warned_unmapped_attrs.add(attr_type)

    # Build has_supporting_studies from collected TextMiningStudyResult objects
    if text_mining_results:
        study = Study(
            id=entity_id(),
            category=["biolink:Study"],
            has_study_results=text_mining_results
        )
        association.has_supporting_studies = {study.id: study}

    # Build knowledge sources with defaults if not provided in attributes
    association.sources = build_association_knowledge_sources(
        primary=primary_source or INFORES_TEXT_MINING_KP,
        supporting=supporting_sources or ["infores:pubmed"]
    )


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
    """Transform TMKP-edge records with attribute parsing."""
    try:
        subject_id = record.get("subject")
        predicate = record.get("predicate")
        object_id = record.get("object")
        relation = record.get("relation")

        if not all([subject_id, predicate, object_id]):
            return None

        # Validate subject/object prefixes match predicate domain/range constraints
        if not _validate_edge_prefixes(subject_id, object_id, predicate):
            _skipped_edges_by_prefix.add((subject_id, predicate, object_id, relation))
            return None

        # Get association class
        assoc_class = ASSOCIATION_MAP.get(relation, Association)

        # Build association kwargs with all fields
        assoc_kwargs = {
            "id": entity_id(),
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

        # Parse attributes JSON - this populates has_supporting_studies and sources on the association
        if attributes_json := record.get("_attributes"):
            attributes = json.loads(attributes_json)
            parse_attributes(attributes, association)
        else:
            # No attributes - set default sources
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
