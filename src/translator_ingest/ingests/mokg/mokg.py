import json
import re
from pathlib import Path
from typing import Any

import koza
from pydantic import ValidationError

from biolink_model.datamodel.pydanticmodel_v2 import (
    Activity,
    AgentTypeEnum,
    AnatomicalEntity,
    Association,
    Behavior,
    BiologicalProcess,
    CausalGeneToDiseaseAssociation,
    ChemicalAffectsBiologicalEntityAssociation,
    ChemicalEntity,
    Cohort,
    ComplexMolecularMixture,
    CorrelatedGeneToDiseaseAssociation,
    Device,
    Disease,
    DiseaseAssociatedWithResponseToChemicalEntityAssociation,
    DiseaseOrPhenotypicFeature,
    DiseaseOrPhenotypicFeatureToLocationAssociation,
    Drug,
    Gene,
    GeneFamily,
    GeneRegulatesGeneAssociation,
    GeneToExpressionSiteAssociation,
    GeographicLocation,
    GrossAnatomicalStructure,
    InformationContentEntity,
    KnowledgeLevelEnum,
    MacromolecularMachineToBiologicalProcessAssociation,
    MolecularActivity,
    MolecularMixture,
    NamedThing,
    NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    OrganismTaxon,
    Pathway,
    Phenomenon,
    PhenotypicFeature,
    PopulationOfIndividualOrganisms,
    Procedure,
    Protein,
    Publication,
    RetrievalSource,
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
# biolink class except: GenomicEntity (a mixin) and ClinicalAttribute (no
# corresponding concrete class) - those fall back to NamedThing. Publication
# requires a default `publication_type` since the source data does not provide
# one.
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
    "Publication": Publication,
    "SmallMolecule": SmallMolecule,
}


# Maps every MOKG predicate to a biolink Association subclass that captures
# the strongest semantic fit. Each subclass may further restrict the predicate
# Literal; the transform falls back to the generic Association on any
# validation error (see _instantiate_association).
PREDICATE_TO_ASSOCIATION_CLASS: dict[str, type] = {
    "biolink:acts_upstream_of_or_within":             CausalGeneToDiseaseAssociation,
    "biolink:affects":                                 ChemicalAffectsBiologicalEntityAssociation,
    "biolink:associated_with":                         NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    "biolink:associated_with_increased_likelihood_of": NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    "biolink:associated_with_resistance_to":           DiseaseAssociatedWithResponseToChemicalEntityAssociation,
    "biolink:associated_with_sensitivity_to":          DiseaseAssociatedWithResponseToChemicalEntityAssociation,
    "biolink:biomarker_for":                           NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    "biolink:correlated_with":                         CorrelatedGeneToDiseaseAssociation,
    "biolink:disease_has_location":                    DiseaseOrPhenotypicFeatureToLocationAssociation,
    "biolink:expressed_in":                            GeneToExpressionSiteAssociation,
    "biolink:gene_associated_with_condition":          CausalGeneToDiseaseAssociation,
    "biolink:genetically_associated_with":             NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    "biolink:is_sequence_variant_of":                  CausalGeneToDiseaseAssociation,
    "biolink:negatively_correlated_with":              CorrelatedGeneToDiseaseAssociation,
    "biolink:participates_in":                         MacromolecularMachineToBiologicalProcessAssociation,
    "biolink:positively_correlated_with":              CorrelatedGeneToDiseaseAssociation,
    "biolink:regulates":                               GeneRegulatesGeneAssociation,
    "biolink:related_to":                              Association,
}


# Maps every MOKG source-column qualifier key to the corresponding biolink
# qualifier slot. When the chosen Association subclass lacks the typed slot the
# value is preserved in the generic `qualifiers: list[str]` list (see
# _build_qualifier_overlay) so no qualifier information is lost.
QUALIFIER_SOURCE_TO_SLOT: dict[str, str] = {
    "biolink:species_context_qualifier":         "species_context_qualifier",
    "biolink:anatomical_context_qualifier":      "anatomical_context_qualifier",
    "biolink:disease_context_qualifier":         "disease_context_qualifier",
    "biolink:subject_aspect_qualifier":          "subject_aspect_qualifier",
    "biolink:object_aspect_qualifier":           "object_aspect_qualifier",
    "biolink:subject_direction_qualifier":       "subject_direction_qualifier",
    "biolink:object_direction_qualifier":        "object_direction_qualifier",
    "biolink:subject_context_qualifier":         "subject_context_qualifier",
    "biolink:object_context_qualifier":          "object_context_qualifier",
    "biolink:subject_part_qualifier":            "subject_part_qualifier",
    "biolink:object_part_qualifier":             "object_part_qualifier",
    "biolink:part_qualifier":                    "part_qualifier",
    "biolink:subject_form_or_variant_qualifier": "subject_form_or_variant_qualifier",
    "biolink:population_context_qualifier":      "population_context_qualifier",
    "biolink:temporal_context_qualifier":        "temporal_context_qualifier",
}


# Maps raw edge-record column names to typed numeric biolink slots. Multiple
# case/spacing variants of the same logical field are listed so that all
# spellings the MOKG release actually uses route to the same destination slot.
TYPED_NUMERIC_COLUMN_TO_SLOT: dict[str, str] = {
    "p value":               "p_value",
    "P value":               "p_value",
    "P-value":               "p_value",
    "p-value":               "p_value",
    "pvalue":                "p_value",
    "adjusted p value":      "adjusted_p_value",
    "Adjusted p value":      "adjusted_p_value",
    "Adjusted P Value":      "adjusted_p_value",
    "Adjusted P-value":      "adjusted_p_value",
    "adj p value":           "adjusted_p_value",
    "adj.P-value(BH)":       "adjusted_p_value",
    "adj.P.Val":             "adjusted_p_value",
    "p.adj":                 "adjusted_p_value",
    "padj":                  "adjusted_p_value",
    "relationship strength": "has_confidence_score",
}


# Edge columns that we copy into `has_attribute: list[str]` as `key=value`
# strings. They have no dedicated biolink slot but are reusable pieces of edge
# metadata, so they belong on the open-ended has_attribute list rather than in
# the biolink-curated qualifiers slot.
HAS_ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    "sample size",
    "case sample size",
    "control sample size",
    "multiple testing correction method",
    "assertion method",
    "constellation",
    "domain",
    "nominal significance",
    "mtc significance",
    "bonferroni significance",
    "miscellaneous notes",
    "url",
)


# Columns to preserve verbatim (stringified JSON when list/dict) onto the
# `supporting_text` slot so that the ~200 study-specific numeric columns per
# predicate are not silently dropped from the ingest. Biolink has no typed
# slot for the dozens of per-study statistical measurements (odds ratio,
# hazard ratio, IVW, MR-Egger, etc.).
SUPPORTING_TEXT_COLUMNS: tuple[str, ...] = (
    "or",
    "lower ci",
    "upper ci",
    "lower confidence interval",
    "upper confidence interval",
    "odds ratio",
    "hazard ratio",
    "beta",
    "se",
    "fdr",
    "q value",
    "fold change",
    "regulation direction",
    "effect size",
    "log2fc",
    "correlation",
    "correlation coefficient",
    "fdr SMR",
    "ivw or",
    "ivw b",
    "ivw se",
    "ivw pval",
    "mregger or",
    "wmedian or",
    "pleiotropy p value",
    "h4 h3h4",
)


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

    taxon = node_data.get("taxon")
    provided_by = node_data.get("source")
    publication_year = node_data.get("year published")
    journal = node_data.get("journal")
    first_author = node_data.get("first author")

    for category in categories:
        short_name = category.removeprefix("biolink:")
        node_class = CATEGORY_TO_CLASS.get(short_name)
        if node_class is None:
            continue

        if short_name == "Publication":
            import datetime as _dt
            creation_date = None
            if publication_year:
                try:
                    creation_date = _dt.date(int(publication_year), 1, 1)
                except (TypeError, ValueError):
                    creation_date = None
            return Publication(
                id=node_id,
                name=name,
                category=categories,
                taxon=taxon,
                provided_by=[provided_by] if provided_by else None,
                publication_type=["JournalArticle"],
                authors=[first_author] if first_author else None,
                creation_date=creation_date,
                xref=[journal] if journal else None,
            )

        common_kwargs: dict[str, Any] = {
            "id": node_id,
            "name": name,
            "category": categories,
        }
        if taxon is not None:
            common_kwargs["taxon"] = taxon
        if provided_by is not None:
            common_kwargs["provided_by"] = [provided_by]
        return node_class(**common_kwargs)

    logger.debug(f"Unmappable categories {categories} for node {node_id}, using NamedThing")
    fallback_kwargs: dict[str, Any] = {
        "id": node_id,
        "name": name,
        "category": ["biolink:NamedThing"],
    }
    if taxon is not None:
        fallback_kwargs["taxon"] = taxon
    if provided_by is not None:
        fallback_kwargs["provided_by"] = [provided_by]
    return NamedThing(**fallback_kwargs)


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


def _value_to_attribute_string(key: str, value: Any) -> str:
    """Render any record value as a `key=value` string for the has_attribute list."""
    if isinstance(value, str):
        return f"{key}={value}"
    return f"{key}={json.dumps(value, sort_keys=True, ensure_ascii=False)}"


def _normalize_attribute_key(column: str) -> str:
    """Normalize a raw MOKG column name into a snake_case attribute key.

    Has_attribute values are surfaced into downstream tooling; matching the
    biolink snake_case convention makes them queryable.
    """
    return column.replace(" ", "_")


def _has_attribute_overlay(record: dict[str, Any]) -> list[str]:
    """Build a `has_attribute: list[str]` of `key=value` strings for the edge.

    Captures columns the typed-slot overlays don't claim: sample size, multiple
    testing correction method, assertion method, etc. We use this slot rather
    than `qualifiers` so that the strings are clearly ETL annotations rather
    than biolink concept identifiers.
    """
    out: list[str] = []
    for column in HAS_ATTRIBUTE_COLUMNS:
        value = record.get(column)
        if value is None or value == "":
            continue
        if isinstance(value, list) and not value:
            continue
        out.append(_value_to_attribute_string(_normalize_attribute_key(column), value))
    return out


def _supporting_text_overlay(record: dict[str, Any]) -> list[str]:
    """Render the study-specific numeric columns as JSON onto `supporting_text`.

    Biolink has no slot for the dozens of per-study statistical measurements
    (odds ratio, hazard ratio, IVW, MR-Egger, etc.). Rather than dropping them
    entirely we surface the full payload as a JSON string so downstream
    consumers can still recover the original values.
    """
    payload = {column: record[column] for column in SUPPORTING_TEXT_COLUMNS if column in record}
    if not payload:
        return []
    return [json.dumps(payload, sort_keys=True, ensure_ascii=False)]


def _build_qualifier_overlay(
    record: dict[str, Any], association_cls: type
) -> tuple[dict[str, Any], list[str]]:
    """Route every biolink-prefixed qualifier value to the right slot.

    The typed slot receives the value if `association_cls` declares it. Any
    qualifier whose typed slot is not on the chosen subclass is preserved in
    the generic `qualifiers: list[str]` so that no qualifier information is
    silently dropped.

    A few biolink qualifier slots (e.g. `anatomical_context_qualifier` on
    `ChemicalAffectsBiologicalEntityAssociation`) are typed as `list[str]`
    while the MOKG source data is a scalar CURIE. We wrap the value in a
    one-element list to satisfy those slots.
    """
    subclass_fields = association_cls.model_fields
    typed: dict[str, Any] = {}
    generic: list[str] = []
    for source_key, slot_name in QUALIFIER_SOURCE_TO_SLOT.items():
        value = record.get(source_key)
        if not value:
            continue
        if slot_name in subclass_fields:
            field = subclass_fields[slot_name]
            if "list" in str(field.annotation):
                typed[slot_name] = [value]
            else:
                typed[slot_name] = value
        else:
            generic.append(value)
    return typed, generic


def _typed_numeric_overlay(record: dict[str, Any]) -> dict[str, float]:
    """Populate p_value, adjusted_p_value, and has_confidence_score from the
    case-insensitive set of source columns. The destination slot name is taken
    from TYPED_NUMERIC_COLUMN_TO_SLOT, so 'Adjusted P Value' lands in
    `adjusted_p_value` exactly like 'adjusted p value' does.
    """
    out: dict[str, float] = {}
    for source_column, slot_name in TYPED_NUMERIC_COLUMN_TO_SLOT.items():
        value = parse_optional_float(record.get(source_column))
        if value is None:
            continue
        if slot_name not in out:
            out[slot_name] = value
    return out


def _prune_to_class_fields(
    edge_props: dict[str, Any], target_cls: type
) -> dict[str, Any]:
    """Drop keys not declared on `target_cls` so the model accepts the kwargs.

    Biolink Association and its subclasses are configured with
    `extra='forbid'`. After we accumulate qualifiers/numerics/has_attribute
    overlays, we need to strip any field the chosen subclass doesn't declare
    before construction - otherwise pydantic rejects valid biolink content.
    """
    allowed = set(target_cls.model_fields)
    return {k: v for k, v in edge_props.items() if k in allowed}


def _instantiate_association(
    predicate: str, edge_props: dict[str, Any]
) -> Association:
    """Build the Association subclass for `predicate`, falling back to the
    generic `Association` on any pydantic validation error (Literal mismatch,
    missing required slot, etc.). The fallback keeps every record in the
    ingest instead of silently dropping it.
    """
    target_cls = PREDICATE_TO_ASSOCIATION_CLASS.get(predicate, Association)
    pruned = _prune_to_class_fields(edge_props, target_cls)
    try:
        return target_cls(**pruned)
    except ValidationError as exc:
        logger.debug(
            f"Falling back to generic Association for {predicate} "
            f"({target_cls.__name__} rejected: {exc.errors()[0]['type']})"
        )
        return Association(**_prune_to_class_fields(edge_props, Association))


def _record_url_to_sources(record: dict[str, Any]) -> list[RetrievalSource] | None:
    """If the record carries a per-edge `url`, attach it to the default MOKG
    RetrievalSource via `source_record_urls`. The `url` is a record-level
    pointer to the source artifact in the publication repository.
    """
    url = record.get("url")
    if not url:
        return None
    primary = MOKG_SOURCES[0]
    return [
        RetrievalSource(
            id=primary.id,
            resource_id=primary.resource_id,
            resource_role=primary.resource_role,
            upstream_resource_ids=primary.upstream_resource_ids,
            source_record_urls=[url],
        )
    ]


@koza.transform_record(tag="edges")
def transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform an edge record into a KnowledgeGraph with both endpoints and the association."""
    subject_id = record.get("subject")
    object_id = record.get("object")
    predicate = record.get("predicate")

    if not all([subject_id, object_id, predicate]):
        logger.warning(f"Skipping edge missing required fields: {record}")
        return None

    # Drop edges flagged not significant. The `significant` column is a curated
    # ternary (YES/NO/UNSURE); keep YES and UNSURE, drop NO.
    if record.get("significant") == "NO":
        logger.debug("Dropping not-significant edge")
        return None

    nodes_lookup = koza.state.get("nodes_lookup", {})

    subject_node_data = nodes_lookup.get(subject_id)
    object_node_data = nodes_lookup.get(object_id)

    if not subject_node_data or not object_node_data:
        logger.warning(f"Skipping edge - missing node data for {subject_id} or {object_id}")
        return None

    association_cls = PREDICATE_TO_ASSOCIATION_CLASS.get(predicate, Association)
    typed_qualifiers, generic_qualifiers = _build_qualifier_overlay(record, association_cls)
    typed_numerics = _typed_numeric_overlay(record)
    has_attribute = _has_attribute_overlay(record)
    supporting_text = _supporting_text_overlay(record)
    sources = _record_url_to_sources(record) or MOKG_SOURCES

    publication = record.get("publication")

    edge_props: dict[str, Any] = {
        "id": record.get("uuid", entity_id()),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "knowledge_level": record.get("knowledge_level", KnowledgeLevelEnum.knowledge_assertion),
        "agent_type": record.get("agent_type", AgentTypeEnum.manual_agent),
        "sources": sources,
        "publications": [publication] if publication else None,
        "qualifiers": generic_qualifiers or None,
        "has_attribute": has_attribute or None,
        "supporting_text": supporting_text or None,
    }
    edge_props.update(typed_qualifiers)
    edge_props.update(typed_numerics)

    association = _instantiate_association(predicate, edge_props)

    return KnowledgeGraph(
        nodes=[create_node(subject_node_data), create_node(object_node_data)],
        edges=[association],
    )
