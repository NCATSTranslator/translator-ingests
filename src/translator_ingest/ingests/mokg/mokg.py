import json
import re
from pathlib import Path
from typing import Any

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Activity,
    AnatomicalEntity,
    Association,
    Behavior,
    BiologicalProcess,
    ChemicalEntity,
    Cohort,
    ComplexMolecularMixture,
    Device,
    Disease,
    DiseaseOrPhenotypicFeature,
    Drug,
    Gene,
    GeneFamily,
    GeographicLocation,
    GrossAnatomicalStructure,
    InformationContentEntity,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    MolecularActivity,
    MolecularMixture,
    NamedThing,
    OrganismTaxon,
    Pathway,
    Phenomenon,
    PhenotypicFeature,
    PopulationOfIndividualOrganisms,
    Procedure,
    Protein,
    SmallMolecule,
)
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.transform_utils import entity_id

INFORES_MOKG = "infores:multiomics-kg"
MOKG_SOURCES = build_association_knowledge_sources(primary=INFORES_MOKG)

logger = get_logger(__name__)

# A handful of records carry non-numeric artifacts in the stat columns (e.g. a
# leaked header "Adjusted P-value" or tissue labels like "Liver: Lactate"). Only
# values matching a real number are converted; everything else is dropped.
_NUMERIC_RE = re.compile(r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")


def parse_optional_float(value: Any) -> float | None:
    """Return float(value) only when value is a real number; otherwise None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or not _NUMERIC_RE.fullmatch(text):
        return None
    return float(text)


# Map node categories to concrete Pydantic classes. MOKG stores `category` as a
# scalar string. Every category present in the 3.0.0 release resolves to a real
# biolink class except: Publication and ClinicalAttribute (require extra fields),
# and GenomicEntity (a mixin) - those fall back to NamedThing.
CATEGORY_TO_CLASS: dict[str, type] = {
    "Activity": Activity,
    "AnatomicalEntity": AnatomicalEntity,
    "Behavior": Behavior,
    "BiologicalProcess": BiologicalProcess,
    "ChemicalEntity": ChemicalEntity,
    "Cohort": Cohort,
    "ComplexMolecularMixture": ComplexMolecularMixture,
    "Device": Device,
    "Disease": Disease,
    "DiseaseOrPhenotypicFeature": DiseaseOrPhenotypicFeature,
    "Drug": Drug,
    "Gene": Gene,
    "GeneFamily": GeneFamily,
    "GeographicLocation": GeographicLocation,
    "GrossAnatomicalStructure": GrossAnatomicalStructure,
    "InformationContentEntity": InformationContentEntity,
    "MolecularActivity": MolecularActivity,
    "MolecularMixture": MolecularMixture,
    "OrganismTaxon": OrganismTaxon,
    "Pathway": Pathway,
    "Phenomenon": Phenomenon,
    "PhenotypicFeature": PhenotypicFeature,
    "PopulationOfIndividualOrganisms": PopulationOfIndividualOrganisms,
    "Procedure": Procedure,
    "Protein": Protein,
    "SmallMolecule": SmallMolecule,
}


def normalize_category(raw_category: str | list[str] | None) -> list[str]:
    """Normalize a scalar (or list) node category into a one-item (or longer) list."""
    if raw_category is None:
        return ["biolink:NamedThing"]
    if isinstance(raw_category, list):
        return raw_category
    return [raw_category]


def create_node(node_data: dict[str, Any]) -> Any:
    """Build a typed node, normalizing scalar categories and falling back to NamedThing.

    NamedThing.category is a literal restricted to 'biolink:NamedThing', so the fallback
    forces that category rather than carrying an unmappable original value.
    """
    node_id = node_data.get("id")
    name = node_data.get("name")
    categories = normalize_category(node_data.get("category"))

    for category in categories:
        short_name = category.removeprefix("biolink:")
        node_class = CATEGORY_TO_CLASS.get(short_name)
        if node_class is not None:
            return node_class(id=node_id, name=name, category=categories)

    logger.debug(f"Unmappable categories {categories} for node {node_id}, using NamedThing")
    return NamedThing(id=node_id, name=name, category=["biolink:NamedThing"])


@koza.on_data_begin(tag="edges")
def on_data_begin_edges(koza: koza.KozaTransform) -> None:
    """Load all nodes into memory for edge lookup.

    The MOKG node file is plain NDJSON (not gzipped), unlike ctkp/dakp.
    """
    nodes_file_path = Path(koza.input_files_dir) / "MULTIOMICS_KG_3.0.0.nodes.ndjson"

    logger.info("Loading all nodes into memory...")
    nodes_lookup: dict[str, dict[str, Any]] = {}
    node_count = 0

    with nodes_file_path.open() as handle:
        for line in handle:
            if line.strip():
                node = json.loads(line)
                node_id = node.get("id")
                if node_id:
                    nodes_lookup[node_id] = node
                    node_count += 1

    logger.info(f"Loaded {node_count} nodes into memory")
    koza.state["nodes_lookup"] = nodes_lookup


@koza.transform_record(tag="edges")
def transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform an edge record into a KnowledgeGraph with both endpoints and the association."""
    subject_id = record.get("subject")
    object_id = record.get("object")
    predicate = record.get("predicate")

    if not all([subject_id, object_id, predicate]):
        logger.warning(f"Skipping edge missing required fields: {record}")
        return None

    nodes_lookup = koza.state.get("nodes_lookup", {})

    subject_node_data = nodes_lookup.get(subject_id)
    object_node_data = nodes_lookup.get(object_id)

    if not subject_node_data or not object_node_data:
        logger.warning(f"Skipping edge - missing node data for {subject_id} or {object_id}")
        return None

    # Disease / anatomical context CURIEs have no slot on the generic Association;
    # route them into the generic `qualifiers` list (dropping falsy values).
    qualifiers = [
        q
        for q in (
            record.get("biolink:disease_context_qualifier"),
            record.get("biolink:anatomical_context_qualifier"),
        )
        if q
    ]

    publication = record.get("publication")

    edge_props: dict[str, Any] = {
        "id": record.get("uuid", entity_id()),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "knowledge_level": record.get("knowledge_level", KnowledgeLevelEnum.knowledge_assertion),
        "agent_type": record.get("agent_type", AgentTypeEnum.manual_agent),
        "sources": MOKG_SOURCES,
        "publications": [publication] if publication else None,
        "qualifiers": qualifiers if qualifiers else None,
    }

    # Numeric statistics: map each source column to a biolink float slot.
    # Non-numeric values (leaked headers, tissue labels) are dropped by parse_optional_float.
    statistics: dict[str, float] = {
        slot: value
        for slot, column in (
            ("p_value", "p value"),
            ("adjusted_p_value", "adjusted p value"),
            ("has_confidence_score", "relationship strength"),
        )
        if (value := parse_optional_float(record.get(column))) is not None
    }
    edge_props.update(statistics)

    association = Association(**edge_props)

    return KnowledgeGraph(
        nodes=[create_node(subject_node_data), create_node(object_node_data)],
        edges=[association],
    )
