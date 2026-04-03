"""SemMedDB ingest: KG2 pre-processed edges -> Biolink Model associations."""

import os
from typing import Any

import koza
from koza.model.graphs import KnowledgeGraph

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    AnatomicalEntity,
    Association,
    CausalGeneToDiseaseAssociation,
    ChemicalAffectsBiologicalEntityAssociation,
    ChemicalAffectsGeneAssociation,
    ChemicalEntity,
    ChemicalOrGeneOrGeneProductFormOrVariantEnum,
    Disease,
    Gene,
    GeneAffectsChemicalAssociation,
    GeneToPhenotypicFeatureAssociation,
    GeneRegulatesGeneAssociation,
    KnowledgeLevelEnum,
    NamedThing,
    PhenotypicFeature,
    Protein,
    Study,
    TextMiningStudyResult,
)
from bmt.pydantic import build_association_knowledge_sources, entity_id
from translator_ingest.util.biolink import INFORES_SEMMEDDB

PREFIX_TO_CLASS: dict[str, type[NamedThing]] = {
    "NCBIGene": Gene,
    "HGNC": Gene,
    "ENSEMBL": Gene,
    "PR": Protein,
    "UniProtKB": Protein,
    "CHEBI": ChemicalEntity,
    "DRUGBANK": ChemicalEntity,
    "MONDO": Disease,
    "DOID": Disease,
    "HP": PhenotypicFeature,
    "UBERON": AnatomicalEntity,
}

PREDICATE_REMAP: dict[str, str] = {
    "biolink:preventative_for_condition": "biolink:treats_or_applied_or_studied_to_treat",
}

GENETIC_VARIANT_FORM = ChemicalOrGeneOrGeneProductFormOrVariantEnum.genetic_variant_form

PUBLICATIONS_CAP_THRESHOLD = 200
MAX_PUBLICATIONS_PER_STRATEGY = 100  # up to 2x this many total (score + recency)
PUBLICATIONS_CAP_ENABLED: bool = (
    os.environ.get("SEMMEDDB_UNCAPPED", "").lower() not in ("1", "true", "yes")
)

BTE_EXCLUDED_ORIGINAL_PREDICATES: frozenset[str] = frozenset({
    "compared_with",
    "isa",
    "measures",
    "higher_than",
    "lower_than",
})


def get_latest_version() -> str:
    """Return the current SemMedDB ingest version identifier."""
    return "semmeddb-2023-kg2.10.3"


def _get_node_class(curie: str) -> type[NamedThing]:
    """Return the Biolink class for a CURIE based on its prefix."""
    if ":" not in curie:
        return NamedThing
    return PREFIX_TO_CLASS.get(curie.split(":", 1)[0], NamedThing)


def _is_gene_or_protein(curie: str) -> bool:
    """Check whether a CURIE maps to Gene or Protein."""
    return _get_node_class(curie) in {Gene, Protein}


def _is_chemical(curie: str) -> bool:
    """Check whether a CURIE maps to ChemicalEntity."""
    return _get_node_class(curie) is ChemicalEntity


def _is_disease(curie: str) -> bool:
    """Check whether a CURIE maps to Disease."""
    return _get_node_class(curie) is Disease


def _is_phenotypic_feature(curie: str) -> bool:
    """Check whether a CURIE maps to PhenotypicFeature."""
    return _get_node_class(curie) is PhenotypicFeature


def _has_bte_excluded_predicate(kg2_ids: list[str]) -> bool:
    """Check whether any kg2_id encodes an original SemMedDB predicate that BTE removes.

    Each ``kg2_id`` string is formatted as ``SUBJECT---PREDICATE---OBJECT``.
    The predicate portion may carry a ``SEMMEDDB:`` prefix.

    >>> _has_bte_excluded_predicate(["UMLS:C0---SEMMEDDB:isa---UMLS:C1"])
    True
    >>> _has_bte_excluded_predicate(["UMLS:C0---SEMMEDDB:treats---UMLS:C1"])
    False
    >>> _has_bte_excluded_predicate([])
    False
    """
    for kid in kg2_ids:
        parts = kid.split("---")
        if len(parts) < 2:
            continue
        original_pred = parts[1].removeprefix("SEMMEDDB:").lower()
        if original_pred in BTE_EXCLUDED_ORIGINAL_PREDICATES:
            return True
    return False


def _pub_min_score(
    pmid: str,
    publications_info: dict[str, dict[str, str]],
) -> float:
    """Return the minimum of subject/object scores for a PMID (higher = more confident)."""
    info = publications_info.get(pmid, {})
    subj = float(info.get("subject score", 0) or 0)
    obj = float(info.get("object score", 0) or 0)
    return min(subj, obj)


def _pub_year(
    pmid: str,
    publications_info: dict[str, dict[str, str]],
) -> int:
    """Return the 4-digit publication year for a PMID, or 0 if unavailable."""
    date: str = publications_info.get(pmid, {}).get("publication date", "") or ""
    try:
        return int(date[:4]) if date else 0
    except ValueError:
        return 0


def _cap_publications(
    publications: list[str],
    publications_info: dict[str, dict[str, str]],
) -> tuple[list[str], dict[str, dict[str, str]]]:
    """Cap publications to avoid oversized edges (some SemMedDB edges have 60k+ PMIDs).

    Keeps the union of:
    - top MAX_PUBLICATIONS_PER_STRATEGY by confidence (min of subject/object score)
    - top MAX_PUBLICATIONS_PER_STRATEGY by recency (publication year)

    Returns trimmed (publications, publications_info).
    """
    if len(publications) <= PUBLICATIONS_CAP_THRESHOLD:
        return publications, publications_info

    top_by_score = set(
        sorted(
            publications,
            key=lambda p: _pub_min_score(p, publications_info),
            reverse=True,
        )[:MAX_PUBLICATIONS_PER_STRATEGY]
    )
    top_by_recency = set(
        sorted(
            publications,
            key=lambda p: _pub_year(p, publications_info),
            reverse=True,
        )[:MAX_PUBLICATIONS_PER_STRATEGY]
    )
    kept = top_by_score | top_by_recency
    return (
        [p for p in publications if p in kept],
        {k: v for k, v in publications_info.items() if k in kept},
    )


def _extract_supporting_studies(
    publications_info: dict[str, dict[str, str]],
) -> dict[str, Study] | None:
    """Extract supporting text from publications_info and create Study objects.

    ``publications_info`` maps PMIDs to dicts with keys like ``sentence``,
    ``publication date``, ``subject score``, and ``object score``.
    """
    if not publications_info:
        return None

    text_mining_results: list[TextMiningStudyResult] = []

    for pmid, info in publications_info.items():
        sentence = info.get("sentence")
        if not sentence:
            continue

        tm_result = TextMiningStudyResult(
            id=entity_id(),
            category=["biolink:TextMiningStudyResult"],
            supporting_text=[sentence],
        )
        if pmid:
            tm_result.xref = [pmid]

        text_mining_results.append(tm_result)

    if not text_mining_results:
        return None

    study = Study(
        id=entity_id(),
        category=["biolink:Study"],
        has_study_results=text_mining_results,
    )
    return {study.id: study}


def _make_node(curie: str, koza: koza.KozaTransform = None) -> NamedThing | None:
    # create a node from an identifier
    if ":" not in curie:
        # bad id format, count it for later reporting
        if koza and "bad_id_format" in koza.state:
            koza.state["bad_id_format"] += 1
        return None

    prefix = curie.split(":", 1)[0]
    cls = PREFIX_TO_CLASS.get(prefix, NamedThing)
    return cls(id=curie, category=cls.model_fields["category"].default)

_STATE_DEFAULTS: dict[str, int] = {
    "total_edges_processed": 0,
    "edges_with_publications": 0,
    "edges_with_qualifiers": 0,
    "bad_id_format": 0,
    "invalid_edges": 0,
    "invalid_nodes": 0,
    "domain_range_exclusion_skipped": 0,
    "low_publication_count_skipped": 0,
    "bte_excluded_predicate_skipped": 0,
    "publications_capped": 0,
}


@koza.on_data_begin(tag="filter_edges")
def on_begin_filter_edges(koza: koza.KozaTransform) -> None:
    """Initialize counters for processing statistics."""
    koza.state["seen_node_ids"] = set()
    for key, default in _STATE_DEFAULTS.items():
        koza.state[key] = default

@koza.on_data_end(tag="filter_edges")
def on_end_filter_edges(koza: koza.KozaTransform) -> None:  # noqa: PLR0912
    """Log processing summary with key metrics."""
    s = koza.state
    koza.log("semmeddb processing complete:", level="INFO")
    koza.log(f"  Total edges processed: {s['total_edges_processed']}", level="INFO")
    koza.log(
        f"  Edges emitted (>3 PMIDs each): {s['edges_with_publications']}",
        level="INFO",
    )
    koza.log(f"  Edges with qualifiers: {s['edges_with_qualifiers']}", level="INFO")
    koza.log(f"  Unique nodes extracted: {len(s['seen_node_ids'])}", level="INFO")

    _warn_if = [
        ("bad_id_format", "Bad ID format skipped", "WARNING"),
        ("invalid_edges", "Invalid edges skipped", "WARNING"),
        ("invalid_nodes", "Invalid nodes skipped", "WARNING"),
        ("domain_range_exclusion_skipped", "Domain/range exclusion skipped", "INFO"),
        ("low_publication_count_skipped", "Low publication count skipped", "INFO"),
        ("bte_excluded_predicate_skipped", "BTE-excluded predicate skipped", "INFO"),
        ("publications_capped", "Publications capped to top-N by score+recency", "INFO"),
    ]
    for key, label, level in _warn_if:
        if s[key] > 0:
            koza.log(f"  {label}: {s[key]}", level=level)

def _pick_affects_class(
    subject_id: str,
    object_id: str,
) -> type[Association]:
    """Choose the narrowest Association subclass for ``biolink:affects`` edges.

    All returned classes support ``qualified_predicate``,
    ``object_aspect_qualifier``, and ``object_direction_qualifier``.
    """
    sub_is_gene = _is_gene_or_protein(subject_id)
    sub_is_chem = _is_chemical(subject_id)
    obj_is_gene = _is_gene_or_protein(object_id)
    obj_is_chem = _is_chemical(object_id)

    if sub_is_chem and obj_is_gene:
        return ChemicalAffectsGeneAssociation
    if sub_is_gene and obj_is_chem:
        return GeneAffectsChemicalAssociation
    if sub_is_gene and obj_is_gene:
        return GeneRegulatesGeneAssociation
    return ChemicalAffectsBiologicalEntityAssociation


def _apply_filters(
    record: dict[str, Any],
    state: dict[str, Any],
) -> list[str] | None:
    """Run all record-level filters, returning publications on pass or None on reject."""
    if record.get("domain_range_exclusion"):
        state["domain_range_exclusion_skipped"] += 1
        return None

    publications: list[str] = record.get("publications", [])
    if len(publications) <= 3:
        state["low_publication_count_skipped"] += 1
        return None

    kg2_ids: list[str] = record.get("kg2_ids", [])
    if kg2_ids and _has_bte_excluded_predicate(kg2_ids):
        state["bte_excluded_predicate_skipped"] += 1
        return None

    return publications


def _collect_nodes(
    subject_id: str,
    object_id: str,
    seen_node_ids: set[str],
    koza: koza.KozaTransform,
) -> list[NamedThing] | None:
    """Create and deduplicate subject/object nodes, returning None on bad IDs."""
    nodes: list[NamedThing] = []
    for curie in (subject_id, object_id):
        if curie not in seen_node_ids:
            node = _make_node(curie, koza)
            if node is None:
                koza.state["invalid_nodes"] += 1
                return None
            nodes.append(node)
            seen_node_ids.add(curie)
    return nodes


def _build_association(
    association_kwargs: dict[str, Any],
    record: dict[str, Any],
    subject_id: str,
    object_id: str,
    predicate: str,
) -> Association:
    """Route to the correct Association subclass and attach qualifiers."""
    # KG2 field -> Biolink field mapping for qualifiers
    qualified_predicate: str | None = record.get("qualified_predicate")

    if qualified_predicate:
        qualifier_kwargs: dict[str, Any] = {
            "qualified_predicate": qualified_predicate,
        }
        aspect = record.get("qualified_object_aspect")
        if aspect is not None:
            qualifier_kwargs["object_aspect_qualifier"] = aspect
        direction = record.get("qualified_object_direction")
        if direction is not None:
            qualifier_kwargs["object_direction_qualifier"] = direction

        cls = _pick_affects_class(subject_id, object_id)
        return cls(**association_kwargs, **qualifier_kwargs)

    if predicate == "biolink:causes" and _is_gene_or_protein(subject_id):
        if _is_disease(object_id):
            return CausalGeneToDiseaseAssociation(
                **association_kwargs,
                subject_form_or_variant_qualifier=GENETIC_VARIANT_FORM,
            )
        if _is_phenotypic_feature(object_id):
            return GeneToPhenotypicFeatureAssociation(
                **association_kwargs,
                subject_form_or_variant_qualifier=GENETIC_VARIANT_FORM,
            )

    return Association(**association_kwargs)


@koza.transform_record(tag="filter_edges")
def transform_semmeddb_edge(
    koza: koza.KozaTransform,
    record: dict[str, Any],
) -> KnowledgeGraph | None:
    """Convert one KG2 edge record into Biolink nodes and associations."""
    if "total_edges_processed" not in koza.state:
        koza.state["seen_node_ids"] = set()
        for key, default in _STATE_DEFAULTS.items():
            koza.state[key] = default

    koza.state["total_edges_processed"] += 1

    publications = _apply_filters(record, koza.state)
    if publications is None:
        return None

    subject_id: str | None = record.get("subject")
    object_id: str | None = record.get("object")
    predicate: str | None = record.get("predicate")

    if not all([subject_id, object_id, predicate]):
        koza.state["invalid_edges"] += 1
        return None

    assert subject_id is not None
    assert object_id is not None
    assert predicate is not None

    nodes = _collect_nodes(
        subject_id, object_id, koza.state["seen_node_ids"], koza,
    )
    if nodes is None:
        return None

    # _apply_filters requires len(publications) > 3, so every emitted edge has publications.
    koza.state["edges_with_publications"] += 1

    publications_info: dict[str, dict[str, str]] = record.get(
        "publications_info", {},
    )

    if PUBLICATIONS_CAP_ENABLED and len(publications) > PUBLICATIONS_CAP_THRESHOLD:
        koza.state["publications_capped"] += 1
        publications, publications_info = _cap_publications(
            publications, publications_info,
        )

    predicate = PREDICATE_REMAP.get(predicate, predicate)

    association_kwargs: dict[str, Any] = {
        "id": entity_id(),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "publications": publications,
        "sources": build_association_knowledge_sources(
            primary=INFORES_SEMMEDDB,
        ),
        "knowledge_level": KnowledgeLevelEnum.not_provided,
        "agent_type": AgentTypeEnum.text_mining_agent,
    }

    if record.get("qualified_predicate"):
        koza.state["edges_with_qualifiers"] += 1

    association = _build_association(
        association_kwargs, record, subject_id, object_id, predicate,
    )

    supporting_studies = _extract_supporting_studies(publications_info)
    if supporting_studies:
        association.has_supporting_studies = supporting_studies

    return KnowledgeGraph(nodes=nodes, edges=[association])
