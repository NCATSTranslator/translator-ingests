"""
TMKP (Text Mining Knowledge Provider) ingest.

This ingest processes text-mined assertions from the Translator Text Mining Provider.
Data is provided as individual gzipped KGX files:
- nodes.tsv.gz: Entity information
- edges.tsv.gz: Relationships between entities

"""

import gzip
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
from loguru import logger
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
    GeneToDiseaseAssociation,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    GeneToGeneAssociation,
    GeneRegulatesGeneAssociation,
    Study,
    TextMiningStudyResult,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    ChemicalOrGeneOrGeneProductFormOrVariantEnum,
)
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id
from translator_ingest.util.biolink import INFORES_TEXT_MINING_KP, get_biolink_model_toolkit

TMKP_DEFAULT_SOURCES = build_association_knowledge_sources(
    primary=INFORES_TEXT_MINING_KP,
    supporting=["infores:pubmed"],
)


# Remap predicates from source to canonical Biolink form.
# Text-mined 'treats' edges should use the broader 'treats_or_applied_or_studied_to_treat'
# because NLP cannot distinguish between studied, applied, and actual treatment relationships.
PREDICATE_REMAP = {
    "biolink:treats": "biolink:treats_or_applied_or_studied_to_treat",
}

MODIFIED_FORM = ChemicalOrGeneOrGeneProductFormOrVariantEnum.modified_form

# Source edge ids of text-mined treats edges that none of their supporting sentences support, as judged by a
# typed-decision model (Jev) over every supporting sentence. How the list was made:
# https://github.com/cbizon/tmkp-cleanup/tree/v1/treats
JEV_DROPPED_EDGES_FILE = "tmkp_treats_jev_v3_dropped_edges.txt.gz"
# TMKP edge ids aren't unique: a treats edge and a contributes_to edge for the same chemical and condition share an
# id. The list only judges treats edges, so it applies only to these predicates.
JEV_FILTERED_PREDICATES = frozenset({"biolink:treats_or_applied_or_studied_to_treat", "biolink:treats"})

# Chemical -> protein edges (https://github.com/cbizon/tmkp-cleanup/tree/v1/regulation). TMKP's edge id also ignores the direction
# qualifier (an increased and a decreased edge for the same pair share one), so these lists are keyed on
# (id, direction).
# - dropped: edges none of whose sentences support them as mapped
# - pruned: sentences removed from kept edges' evidence (dropped, or moved to a repaired edge)
# - repaired: new edges for sentences whose chemical or protein was mapped wrongly, read as their own reader
CHEM_GENE_RELATION = "biolink:ChemicalToGeneAssociation"
CHEM_GENE_DROPPED_EDGES_FILE = "tmkp_chem_gene_dropped_edges.txt.gz"
CHEM_GENE_PRUNED_SENTENCES_FILE = "tmkp_chem_gene_pruned_sentences.txt.gz"
REGULATES = "biolink:regulates"

# Map TMKP attribute names to Biolink slot names.
TMKP_TO_BIOLINK_SLOT_MAP = {
    "extraction_confidence_score": "has_confidence_score",
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
ASSOCIATION_MAP = {
    "biolink:ChemicalToGeneAssociation": ChemicalAffectsGeneAssociation,
    "biolink:GeneToDiseaseAssociation": GeneToDiseaseAssociation,
    "biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation": ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    "biolink:GeneRegulatoryRelationship": GeneToGeneAssociation,
}

# Track edges skipped due to invalid subject/object prefixes (for reporting)
_skipped_edges_by_prefix: Set[Tuple[str, str, str, str]] = set()


def _reset_module_state() -> None:
    """Reset module-level mutable state. Used by tests to ensure clean state between runs."""
    _skipped_edges_by_prefix.clear()
    _warned_unmapped_attrs.clear()


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


def _normalize_publication_id(pub_id: str) -> str:
    """
    Normalize a publication identifier to a Biolink-resolvable CURIE.

    TMKP source data emits bare PMC identifiers like ``PMC6211782`` without
    a CURIE prefix.

    >>> _normalize_publication_id("PMC6211782")
    'PMC:PMC6211782'
    >>> _normalize_publication_id("PMID:31388901")
    'PMID:31388901'
    >>> _normalize_publication_id("PMC:PMC6211782")
    'PMC:PMC6211782'
    >>> _normalize_publication_id("")
    ''
    """
    if ":" in pub_id:
        return pub_id
    if pub_id.startswith("PMC"):
        return f"PMC:{pub_id}"
    return pub_id


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
    tk = get_biolink_model_toolkit()
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
        predicate: Biolink predicate CURIE (e.g., 'biolink:treats' converted to "treats or applied or studied to treat"))

    Returns:
        Tuple of (domain_prefixes, range_prefixes) or None if predicate has no constraints.

    >>> domain_prefixes, range_prefixes = _get_predicate_domain_range_prefixes('biolink:treats')
    >>> 'DRUGBANK' in domain_prefixes  # chemicals can treat
    True
    >>> 'MONDO' in range_prefixes  # diseases can be treated
    True
    """
    tk = get_biolink_model_toolkit()
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
    return "tmkp-2024-09-07"


def load_dropped_edge_ids(path: Path) -> frozenset[str]:
    """
    Load the source edge ids to drop: a gzipped text file with one TMKP edge id per line.

    Args:
        path: Path to the gzipped id list.

    Returns:
        Frozen set of edge ids.
    """
    with gzip.open(path, "rt") as f:
        return frozenset(line.strip() for line in f if line.strip())


def load_id_direction_pairs(path: Path) -> frozenset[tuple[str, str]]:
    """
    Load chemical -> protein edges to drop: a gzipped file of "edge id<TAB>direction" lines.

    Args:
        path: Path to the gzipped list.

    Returns:
        Frozen set of (edge id, object direction qualifier) pairs.
    """
    with gzip.open(path, "rt") as f:
        return frozenset(tuple(line.rstrip("\n").split("\t")) for line in f if line.strip())


def load_pruned_sentences(path: Path) -> dict[tuple[str, str], frozenset[str]]:
    """
    Load the sentences to remove from kept chemical -> protein edges: a gzipped file of
    "edge id<TAB>direction<TAB>sentence id" lines.

    Args:
        path: Path to the gzipped list.

    Returns:
        (edge id, direction) -> the supporting study result ids to leave out.
    """
    pruned: dict[tuple[str, str], set[str]] = {}
    with gzip.open(path, "rt") as f:
        for line in f:
            if line.strip():
                edge_id, direction, sentence_id = line.rstrip("\n").split("\t")
                pruned.setdefault((edge_id, direction), set()).add(sentence_id)
    return {k: frozenset(v) for k, v in pruned.items()}


def parse_attributes(
    attributes: List[Dict[str, Any]],
    association: Association,
    protected_slots: frozenset[str] = frozenset(),
    excluded_results: frozenset[str] = frozenset(),
) -> None:
    """
    Parse attribute objects and populate the association with supporting studies and knowledge sources.

    Attributes can contain nested attributes representing TextMiningStudyResult objects,
    as well as knowledge source information that should be used to build the sources collection.

    This function mutates the association in place, setting:
    - has_supporting_studies: Dict of Study objects containing TextMiningStudyResult objects
    - sources: Knowledge source attribution built from attribute data or defaults
    - Any other direct attributes found in the attribute list

    :param attributes: Parsed '_attributes' objects from a TMKP edge record.
    :param association: The association to mutate in place.
    :param protected_slots: Slot names the caller has already determined. Source attributes
        of the same name are ignored rather than overwriting the computed value.
    :param excluded_results: Supporting study result ids to leave out (sentences that don't support the edge as
        mapped). When any are left out, the edge's publications are rebuilt from the results that remain.
    """
    text_mining_results: List[TextMiningStudyResult] = []
    primary_source = None
    supporting_sources: List[str] = []

    for attr in attributes:
        attr_type = attr.get("attribute_type_id", "")
        value = attr.get("value")

        # Strip "biolink:" prefix if present to get the actual slot name
        slot_name = attr_type.replace("biolink:", "") if attr_type.startswith("biolink:") else attr_type

        if slot_name == "has_supporting_study_result" and value in excluded_results:
            continue

        if slot_name == "has_supporting_study_result":
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
                elif nested_type == "biolink:publications":
                    # Per-evidence provenance: the document this supporting_text came from.
                    # Distinct from the edge-level 'biolink:publications' attribute, which
                    # aggregates these across all evidence items.
                    tm_result.xref = [_normalize_publication_id(nested_value)] if nested_value else []
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

        elif slot_name in protected_slots:
            # ingest already set this slot; the source value should not overwrite it.
            continue

        elif hasattr(association, slot_name):
            setattr(association, slot_name, value)

        elif slot_name in TMKP_TO_BIOLINK_SLOT_MAP:
            biolink_slot = TMKP_TO_BIOLINK_SLOT_MAP[slot_name]
            if hasattr(association, biolink_slot):
                setattr(association, biolink_slot, value)
            elif biolink_slot not in _warned_unmapped_attrs:
                logger.warning(
                    f"TMKP attribute '{attr_type}' is mapped to '{biolink_slot}', which is "
                    f"not a slot on {type(association).__name__} - value dropped"
                )
                _warned_unmapped_attrs.add(biolink_slot)

        elif attr_type and attr_type not in _warned_unmapped_attrs:
            # Log warning for truly unrecognized attributes (only once per attribute type)
            logger.warning(
                f"Skipping unknown TMKP edge attribute: '{attr_type}' "
                f"(no mapping defined and not a slot on {type(association).__name__})"
            )
            _warned_unmapped_attrs.add(attr_type)

    if excluded_results:
        association.publications = sorted({x for r in text_mining_results for x in (r.xref or [])}) or None

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


@koza.on_data_begin(tag="edges")
def on_data_begin_edges(koza_transform: koza.KozaTransform) -> None:
    """Load the Jev filter lists (treats ids to drop; chemical -> protein edges to drop and sentences to prune)."""
    input_dir = Path(koza_transform.input_files_dir)
    state = koza_transform.state
    state["jev_dropped_edge_ids"] = load_dropped_edge_ids(input_dir / JEV_DROPPED_EDGES_FILE)
    state["chem_gene_dropped_edges"] = load_id_direction_pairs(input_dir / CHEM_GENE_DROPPED_EDGES_FILE)
    state["chem_gene_pruned_sentences"] = load_pruned_sentences(input_dir / CHEM_GENE_PRUNED_SENTENCES_FILE)
    state["jev_dropped_edge_count"] = 0
    state["chem_gene_dropped_edge_count"] = 0
    state["chem_gene_pruned_sentence_count"] = 0
    logger.info(f"Loaded {len(state['jev_dropped_edge_ids'])} treats edge ids, "
                f"{len(state['chem_gene_dropped_edges'])} chemical -> protein edges to drop and "
                f"{sum(map(len, state['chem_gene_pruned_sentences'].values()))} sentences to prune from {input_dir}")


@koza.on_data_end(tag="edges")
def on_data_end_edges(koza_transform: koza.KozaTransform) -> None:
    """Report what the Jev filter lists removed."""
    state = koza_transform.state
    logger.info(f"Dropped {state['jev_dropped_edge_count']} treats edges and {state['chem_gene_dropped_edge_count']} "
                f"chemical -> protein edges; pruned {state['chem_gene_pruned_sentence_count']} sentences from kept "
                f"chemical -> protein edges")


@koza.transform_record(tag="edges")
def transform_tmkp_edge(koza_transform: koza.KozaTransform, record: Dict[str, Any]) -> KnowledgeGraph | None:
    """Transform TMKP-edge records with attribute parsing, after the Jev filter lists."""
    subject_id = record.get("subject")
    predicate = record.get("predicate")
    object_id = record.get("object")
    relation = record.get("relation")

    if not all([subject_id, predicate, object_id]):
        return None

    state = koza_transform.state
    # Drop treats edges that none of their supporting sentences support (see JEV_DROPPED_EDGES_FILE)
    if predicate in JEV_FILTERED_PREDICATES and record.get("id") in state["jev_dropped_edge_ids"]:
        state["jev_dropped_edge_count"] += 1
        return None

    # Chemical -> protein edges: drop unsupported ones, prune the sentences that don't support kept ones
    # (see https://github.com/cbizon/tmkp-cleanup/tree/v1/regulation)
    excluded_results: frozenset[str] = frozenset()
    if relation == CHEM_GENE_RELATION and predicate not in JEV_FILTERED_PREDICATES:
        key = (record.get("id"), record.get("object_direction_qualifier"))
        if key in state["chem_gene_dropped_edges"]:
            state["chem_gene_dropped_edge_count"] += 1
            return None
        excluded_results = state["chem_gene_pruned_sentences"].get(key, frozenset())
        state["chem_gene_pruned_sentence_count"] += len(excluded_results)

    # Validate subject/object prefixes match predicate domain/range constraints
    if not _validate_edge_prefixes(subject_id, object_id, predicate):
        _skipped_edges_by_prefix.add((subject_id, predicate, object_id, relation))
        return None

    return _edge_graph(record, excluded_results)


@koza.transform_record(tag="repaired_edges")
def transform_tmkp_repaired_edge(koza_transform: koza.KozaTransform, record: Dict[str, Any]) -> KnowledgeGraph:
    """
    Transform a repaired chemical -> protein edge: TMKP sentences whose chemical or protein was mapped to the
    wrong concept, regrouped under the right one (https://github.com/cbizon/tmkp-cleanup/tree/v1/regulation). A gene product as regulator makes a
    biolink:regulates gene -> gene edge; a chemical regulator stays biolink:affects.

    These edges are built offline, so a subject or object outside the association's Biolink range is a build error
    and fails the ingest rather than being skipped.
    """
    _validate_repaired_edge(record)
    return _edge_graph(record)


def _validate_repaired_edge(record: Dict[str, Any]) -> None:
    """
    Raise if a repaired edge's subject or object prefix is outside its association's range: a chemical or gene
    product as subject, a gene product as object (Protein Ontology ids are processed peptides).

    >>> _validate_repaired_edge({"subject": "UniProtKB:P05112", "object": "PR:000036009"})
    >>> _validate_repaired_edge({"subject": "MONDO:0005301", "object": "UniProtKB:P05112"})
    Traceback (most recent call last):
    ...
    ValueError: repaired TMKP edge MONDO:0005301 -> UniProtKB:P05112 has an id outside the association's range
    """
    subject_ok = _get_id_prefix(record["subject"]) in _get_valid_prefixes_for_class("ChemicalEntityOrGeneOrGeneProduct")
    object_ok = _get_id_prefix(record["object"]) in _get_valid_prefixes_for_class("GeneOrGeneProduct")
    if not (subject_ok and object_ok):
        raise ValueError(f"repaired TMKP edge {record['subject']} -> {record['object']} has an id outside the "
                         f"association's range")


def _edge_graph(record: Dict[str, Any], excluded_results: frozenset[str] = frozenset()) -> KnowledgeGraph:
    """Build the association and its two endpoint nodes for a TMKP edge record."""
    subject_id = record["subject"]
    object_id = record["object"]

    # Remap predicates from source to canonical Biolink form
    predicate = PREDICATE_REMAP.get(record["predicate"], record["predicate"])

    # Get association class: gene -> gene regulation (repaired edges) is the SIGNOR-style association
    if predicate == REGULATES:
        assoc_class = GeneRegulatesGeneAssociation
    else:
        assoc_class = ASSOCIATION_MAP.get(record.get("relation"), Association)

    # Knowledge level: 'treats_or_applied_or_studied_to_treat' is broad enough to warrant
    # 'knowledge_assertion' (the predicate semantics already account for uncertainty).
    # All other text-mined predicates use 'not_provided'.
    if predicate == "biolink:treats_or_applied_or_studied_to_treat":
        knowledge_level = KnowledgeLevelEnum.knowledge_assertion
    else:
        knowledge_level = KnowledgeLevelEnum.not_provided

    # Build association kwargs with all fields
    assoc_kwargs = {
        "id": entity_id(),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "knowledge_level": knowledge_level,
        "agent_type": AgentTypeEnum.text_mining_agent,
    }

    # Pass through all source record fields that are valid slots on the association class
    # and not already set in assoc_kwargs (preserving computed values like predicate).
    for key, value in record.items():
        if value and key not in assoc_kwargs and key in assoc_class.model_fields:
            assoc_kwargs[key] = value

    # For gene-disease 'contributes_to' edges, apply the canonical Biolink EPC pattern
    # as defaults: primary predicate 'affects', qualified predicate 'contributes_to', with
    # subject_form_or_variant_qualifier defaulting to 'modified_form' if not already set
    # by the source data (which may provide e.g. 'loss_of_function_variant_form').
    if assoc_class == GeneToDiseaseAssociation and predicate == "biolink:contributes_to":
        assoc_kwargs["predicate"] = "biolink:affects"
        assoc_kwargs.setdefault("qualified_predicate", "biolink:contributes_to")
        assoc_kwargs.setdefault("subject_form_or_variant_qualifier", MODIFIED_FORM)

    association = assoc_class(**assoc_kwargs)

    # Parse attributes JSON - this populates has_supporting_studies and sources on the association
    if attributes_json := record.get("_attributes"):
        attributes = json.loads(attributes_json)
        parse_attributes(attributes, association, frozenset(assoc_kwargs), excluded_results)
    else:
        # No attributes - set default sources
        association.sources = TMKP_DEFAULT_SOURCES

    # Endpoint nodes (edges carry no names, so minimal nodes)
    nodes = [NamedThing(id=subject_id), NamedThing(id=object_id)]

    # Return graph with nodes and edges
    return KnowledgeGraph(nodes=nodes, edges=[association])
