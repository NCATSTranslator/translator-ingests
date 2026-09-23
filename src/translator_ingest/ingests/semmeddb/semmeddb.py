"""SemMedDB ingest: KG2 pre-processed edges -> Biolink Model associations.

Publications are filtered by the LLM PMID-checker (RTXteam/LLM_PMID_Checker) verdicts during
the transform. A publication is dropped when its verdict is ``no`` or ``maybe``; ``yes``,
``no_abstract`` and any PMID with no verdict row at all are kept. The ``maybe`` bucket is dropped
for now and will be refined in a later second pass. An edge is dropped when no publication
remains, and edges that survive are capped to the most recent ``MAX_PUBLICATIONS_PER_EDGE``
publications. Rejected and capped-out PMIDs are removed from ``publications_info`` too, so no
supporting study is built for them, and nodes are only emitted for edges that survive.

The verdict artifact is keyed by the raw kg2.10.3 ids and the predicates this transform emits
(after ``PREDICATE_REMAP``), not by normalized ids. Normalized ids change with every Babel
release, and a key that does not match means "no verdict", which means keep, so a filter keyed on
them would silently decay as Babel moved on. Raw ids from the frozen kg2.10.3 source never
change. The artifact is re-keyed from the checker's normalized ids to raw ids by
``analysis/rekey_pmid_verdicts.py``.

Because a non-matching key silently means "keep", a coverage guard backstops the join: the
fraction of checked edges that have any verdict row must be at least ``MIN_EDGE_COVERAGE``, or
the transform fails instead of quietly passing everything through.
"""

import os
from pathlib import Path
from typing import Any

import koza
import polars as pl
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
    GeneToGeneAssociation,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    NamedThing,
    PhenotypicFeature,
    Protein,
    Study,
    TextMiningStudyResult,
)
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id
from translator_ingest.util.biolink import INFORES_SEMMEDDB

SEMMEDDB_SOURCES = build_association_knowledge_sources(primary=INFORES_SEMMEDDB)

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

# LLM PMID-checker results parquet downloaded into source_data/ (see download.yaml), re-keyed
# from normalized ids to raw kg2.10.3 ids.
VERDICT_ARTIFACT_FILENAME = "semmeddb_pmid_checker_results_raw_keyed.parquet"
VERDICT_ARTIFACT_COLUMNS = ["subject_curie", "predicate", "object_curie", "PMID", "support"]

# these verdicts remove a publication; "yes", "no_abstract" and any PMID absent from the results
# are kept. "maybe" is dropped now and refined in a later second pass.
DROP_SUPPORT_VALUES = frozenset({"no", "maybe"})

# minimum fraction of checked edges that must carry at least one verdict row. A key the artifact
# does not cover is treated as "keep", so a broken join would silently disable the filter rather
# than fail; this guard turns that into a loud error. Measured on kg2.10.3 with the re-keyed
# artifact: 99.7% of edges covered (the rest have only no-abstract publications the checker could
# not judge). The same edges joined on the original normalized keys reach only 89.3%, so this
# threshold catches a broken or drifted join with a wide margin on both sides.
MIN_EDGE_COVERAGE = 0.95

# edges left with more than this many publications after verdict filtering are trimmed to the
# most recent ones (highest PMID number, since PMIDs are assigned chronologically). This bounds
# the small tail of very large edges (some have 60k+ PMIDs) without touching the vast majority.
MAX_PUBLICATIONS_PER_EDGE = 1000

# the cap is on by default. Set SEMMEDDB_UNCAPPED=1 (or true/yes) to disable it and keep every
# surviving publication, for example to regenerate uncapped output for a fresh PMID-checker run.
# get_latest_version() reports a distinct source version when it is set, so capped and uncapped
# builds never share a directory or a build version.
PUBLICATIONS_CAP_ENABLED: bool = (
    os.environ.get("SEMMEDDB_UNCAPPED", "").lower() not in ("1", "true", "yes")
)

# the filter is on by default. Set SEMMEDDB_UNFILTERED=1 (or true/yes) to keep every publication
# the checker rejected, which is how the pre-filter edge set is regenerated for a new checker run
# (the planned second pass over the "maybe" bucket needs exactly that input). The verdict artifact
# is then not read at all and the coverage guard does not apply. get_latest_version() reports a
# distinct source version when it is set, so filtered and unfiltered builds never share a
# directory or a build version.
PMID_FILTER_ENABLED: bool = (
    os.environ.get("SEMMEDDB_UNFILTERED", "").lower() not in ("1", "true", "yes")
)

EdgeKey = tuple[str, str, str]

# Every edge key the verdict artifact mentions, mapped to the PMIDs rejected for that edge.
# A key with no rejected PMID maps to None rather than an empty set.
# Membership in this index is what "the checker covered this edge" means for the coverage guard,
# so it holds keys of every support value, not only the rejected ones.
VerdictIndex = dict[EdgeKey, set[str] | None]

BTE_EXCLUDED_ORIGINAL_PREDICATES: frozenset[str] = frozenset({
    "compared_with",
    "isa",
    "measures",
    "higher_than",
    "lower_than",
})

# Qualifier aspects that semantically require both endpoints to be activity-
# or abundance-bearing entities (i.e., Gene, Protein, or ChemicalEntity).
# Tied to NCATSTranslator/Feedback#1213: KG2 emits chemical -> phenotype/
# disease/anatomy edges qualified as "causes activity_or_abundance increased",
# which Aragorn then chains on to produce nonsensical "X has increased
# activity caused by Y" inferences.
ACTIVITY_LIKE_ASPECTS: frozenset[str] = frozenset({
    "activity",
    "activity_or_abundance",
    "abundance",
})

_ACTIVITY_BEARING_CLASSES: frozenset[type[NamedThing]] = frozenset({
    Gene,
    Protein,
    ChemicalEntity,
})


def get_latest_version() -> str:
    """Return the current SemMedDB ingest version identifier."""
    version = "semmeddb-2023-kg2.10.3"

    # Treat having publications uncapped like a different version.
    # Ideally this distinction would be part of the transform version and not
    # the source version, but the transform version is derived from code and
    # doesn't take env var settings like this into account. Doing it here has
    # the effect of distinguishing between capped and uncapped at the expense
    # of downloading the source twice.
    if not PUBLICATIONS_CAP_ENABLED:
        version += "-publications-uncapped"

    # Same treatment for an unfiltered build: its edges and publications differ from a filtered
    # one, so it must not land in the directory or the build version of a filtered build.
    if not PMID_FILTER_ENABLED:
        version += "-unfiltered"

    return version


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


def _aspect_target_is_valid(
    subject_id: str,
    object_id: str,
    aspect: str | None,
) -> bool:
    """Return True if the qualifier aspect is semantically applicable to both endpoints.

    Activity-like aspects (see ``ACTIVITY_LIKE_ASPECTS``) require both subject
    and object to be Gene, Protein, or ChemicalEntity. Other aspect values
    (or no aspect) are unconstrained here.

    Per NCATSTranslator/Feedback#1213, KG2 emits some chemical -> non-activity-
    bearing edges with activity qualifiers (e.g., glucose -> "injury"). Those
    qualifiers must be stripped so downstream rules cannot treat them as
    measurable activity changes.

    >>> _aspect_target_is_valid("CHEBI:1", "NCBIGene:1", "activity")
    True
    >>> _aspect_target_is_valid("CHEBI:1", "UMLS:C0012345", "activity_or_abundance")
    False
    >>> _aspect_target_is_valid("CHEBI:1", "MONDO:1", "activity")
    False
    >>> _aspect_target_is_valid("CHEBI:1", "MONDO:1", None)
    True
    """
    if aspect not in ACTIVITY_LIKE_ASPECTS:
        return True
    return (
        _get_node_class(subject_id) in _ACTIVITY_BEARING_CLASSES
        and _get_node_class(object_id) in _ACTIVITY_BEARING_CLASSES
    )


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


def load_verdicts(artifact_file: Path) -> VerdictIndex:
    """Read the verdict artifact into an index of edge key -> rejected PMIDs.

    Every row contributes its edge key, so the index's keys are the edges the checker covered.
    Only ``no``/``maybe`` rows contribute a PMID; a key whose verdicts are all kept maps to None.
    Support values are lowercased and stripped before they are compared, and a row with a null
    support counts as covered without rejecting anything.
    """
    verdicts = pl.read_parquet(artifact_file, columns=VERDICT_ARTIFACT_COLUMNS).with_columns(
        pl.col("support").cast(pl.Utf8).str.to_lowercase().str.strip_chars()
    )

    verdict_index: VerdictIndex = {}
    for subject, predicate, obj, pmid, support in verdicts.iter_rows():
        edge_key = (subject, predicate, obj)
        if support in DROP_SUPPORT_VALUES:
            rejected_pmids = verdict_index.get(edge_key)
            if rejected_pmids is None:
                verdict_index[edge_key] = {pmid}
            else:
                rejected_pmids.add(pmid)
        else:
            verdict_index.setdefault(edge_key, None)
    return verdict_index


def _pmid_number(pmid: str) -> int:
    """Return the numeric part of a PMID for recency ordering, or -1 if not numeric.

    PMIDs are assigned chronologically, so a higher number is a more recent paper.
    """
    digits = pmid.rsplit(":", 1)[-1]
    return int(digits) if digits.isdigit() else -1


def _cap_by_recency(publications: list[str], limit: int) -> list[str]:
    """Keep the ``limit`` most recent publications (highest PMID number), in original order.

    >>> _cap_by_recency(["PMID:5", "PMID:1", "PMID:9", "PMID:3"], 2)
    ['PMID:5', 'PMID:9']
    >>> _cap_by_recency(["PMID:1", "PMID:2"], 5)
    ['PMID:1', 'PMID:2']
    """
    if len(publications) <= limit:
        return publications
    kept = set(sorted(publications, key=_pmid_number, reverse=True)[:limit])
    return [pmid for pmid in publications if pmid in kept]


def _filter_publications(
    publications: list[str],
    publications_info: dict[str, dict[str, str]],
    rejected_pmids: set[str] | None,
    state: dict[str, Any],
) -> tuple[list[str], dict[str, dict[str, str]]]:
    """Drop rejected PMIDs, then cap to the most recent ``MAX_PUBLICATIONS_PER_EDGE``.

    Every removed PMID is dropped from ``publications_info`` too, so no supporting study is
    built for it. Returns an empty publication list when the checker rejected every publication.
    """
    kept_publications = (
        [pmid for pmid in publications if pmid not in rejected_pmids] if rejected_pmids else publications
    )
    removed_pmids = set(publications) - set(kept_publications)
    state["publications_rejected"] += len(removed_pmids)

    if PUBLICATIONS_CAP_ENABLED and len(kept_publications) > MAX_PUBLICATIONS_PER_EDGE:
        state["publications_capped"] += 1
        capped_publications = _cap_by_recency(kept_publications, MAX_PUBLICATIONS_PER_EDGE)
        removed_pmids |= set(kept_publications) - set(capped_publications)
        kept_publications = capped_publications

    if not removed_pmids:
        return publications, publications_info
    kept_info = {pmid: info for pmid, info in publications_info.items() if pmid not in removed_pmids}
    return kept_publications, kept_info


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
    "qualifier_stripped_invalid_domain_range": 0,
    "edges_verdict_checked": 0,
    "edges_with_verdicts": 0,
    "publications_rejected": 0,
    "edges_rejected_by_verdicts": 0,
}


@koza.on_data_begin(tag="filter_edges")
def on_begin_filter_edges(koza: koza.KozaTransform) -> None:
    """Initialize counters and load the PMID-checker verdicts."""
    koza.state["seen_node_ids"] = set()
    for key, default in _STATE_DEFAULTS.items():
        koza.state[key] = default

    if not PMID_FILTER_ENABLED:
        koza.log("SEMMEDDB_UNFILTERED is set, keeping every publication the checker rejected.", level="WARNING")
        koza.state["verdict_index"] = {}
        return

    if koza.input_files_dir is None:
        raise ValueError("No input_files_dir; the semmeddb transform needs the PMID-checker verdicts.")
    artifact_file = Path(koza.input_files_dir) / VERDICT_ARTIFACT_FILENAME
    if not artifact_file.exists():
        raise FileNotFoundError(f"PMID-checker verdict artifact not found: {artifact_file}")
    koza.log(f"Loading PMID-checker verdicts from {artifact_file}...", level="INFO")
    koza.state["verdict_index"] = load_verdicts(artifact_file)
    koza.log(f"  Edges covered by a verdict: {len(koza.state['verdict_index'])}", level="INFO")

@koza.on_data_end(tag="filter_edges")
def on_end_filter_edges(koza: koza.KozaTransform) -> None:  # noqa: PLR0912
    """Log processing summary with key metrics."""
    s = koza.state
    koza.log("semmeddb processing complete:", level="INFO")
    koza.log(f"  Total edges processed: {s['total_edges_processed']}", level="INFO")
    koza.log(
        f"  Edges emitted (at least one surviving publication): {s['edges_with_publications']}",
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
        ("publications_rejected", "Publications dropped by the PMID checker", "INFO"),
        ("edges_rejected_by_verdicts", "Edges dropped (every publication rejected)", "INFO"),
        ("publications_capped", f"Edges capped to the {MAX_PUBLICATIONS_PER_EDGE} most recent", "INFO"),
        (
            "qualifier_stripped_invalid_domain_range",
            "Qualifier stripped (aspect/endpoint mismatch, see Feedback#1213)",
            "INFO",
        ),
    ]
    for key, label, level in _warn_if:
        if s[key] > 0:
            koza.log(f"  {label}: {s[key]}", level=level)

    edge_coverage = s["edges_with_verdicts"] / s["edges_verdict_checked"] if s["edges_verdict_checked"] else 0.0
    if PMID_FILTER_ENABLED:
        koza.log(
            f"  Edges covered by a PMID-checker verdict: {s['edges_with_verdicts']} of "
            f"{s['edges_verdict_checked']} ({edge_coverage:.2%})",
            level="INFO",
        )
    koza.transform_metadata["pmid_checker_filter"] = {
        "edges_verdict_checked": s["edges_verdict_checked"],
        "edges_with_verdicts": s["edges_with_verdicts"],
        "edge_coverage": edge_coverage,
        "publications_rejected": s["publications_rejected"],
        "edges_rejected_by_verdicts": s["edges_rejected_by_verdicts"],
        "edges_capped": s["publications_capped"],
        "publications_cap_enabled": PUBLICATIONS_CAP_ENABLED,
        "max_publications_per_edge": MAX_PUBLICATIONS_PER_EDGE,
        "pmid_filter_enabled": PMID_FILTER_ENABLED,
    }

    if not PMID_FILTER_ENABLED:
        return

    # An edge key the verdicts do not cover keeps all of its publications, so a join that stops
    # matching would disable the filter silently rather than fail. Make that loud instead.
    if edge_coverage < MIN_EDGE_COVERAGE:
        raise RuntimeError(
            f"PMID-checker verdict coverage is {edge_coverage:.4f}, below the required "
            f"{MIN_EDGE_COVERAGE}: only {s['edges_with_verdicts']} of {s['edges_verdict_checked']} "
            f"checked edges have any verdict row. The verdict artifact "
            f"({VERDICT_ARTIFACT_FILENAME}) keys no longer match the edges this transform emits, "
            f"most likely an artifact mismatch or a source version change. Uncovered edges keep "
            f"every publication, so this would silently disable the filter."
        )

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
        return GeneToGeneAssociation
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
    state: dict[str, Any],
) -> Association:
    """Route to the correct Association subclass and attach qualifiers.

    For qualifier triples expressing activity- or abundance-like semantics
    (``aspect in ACTIVITY_LIKE_ASPECTS``), Biolink-style domain/range applies:
    both subject and object must be Gene, Protein, or ChemicalEntity. When the
    check fails, qualifiers are stripped and the edge is emitted as a plain
    ``biolink:affects`` Association (literature evidence is preserved). See
    NCATSTranslator/Feedback#1213.
    """
    qualified_predicate: str | None = record.get("qualified_predicate")

    if qualified_predicate:
        aspect = record.get("qualified_object_aspect")
        if not _aspect_target_is_valid(subject_id, object_id, aspect):
            state["qualifier_stripped_invalid_domain_range"] += 1
            return Association(**association_kwargs)

        qualifier_kwargs: dict[str, Any] = {
            "qualified_predicate": qualified_predicate,
        }
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

    # The verdicts are keyed by the ids and predicate this transform emits, so remap first.
    predicate = PREDICATE_REMAP.get(predicate, predicate)

    publications_info: dict[str, dict[str, str]] = record.get(
        "publications_info", {},
    )

    # Filter before collecting nodes, so nodes are only emitted for edges that survive.
    verdict_index: VerdictIndex = koza.state["verdict_index"]
    edge_key = (subject_id, predicate, object_id)
    if edge_key in verdict_index:
        koza.state["edges_with_verdicts"] += 1
    koza.state["edges_verdict_checked"] += 1

    publications, publications_info = _filter_publications(
        publications, publications_info, verdict_index.get(edge_key), koza.state,
    )
    if not publications:
        koza.state["edges_rejected_by_verdicts"] += 1
        return None

    nodes = _collect_nodes(
        subject_id, object_id, koza.state["seen_node_ids"], koza,
    )
    if nodes is None:
        return None

    koza.state["edges_with_publications"] += 1

    association_kwargs: dict[str, Any] = {
        "id": entity_id(),
        "subject": subject_id,
        "predicate": predicate,
        "object": object_id,
        "publications": publications,
        "sources": SEMMEDDB_SOURCES,
        "knowledge_level": KnowledgeLevelEnum.not_provided,
        "agent_type": AgentTypeEnum.text_mining_agent,
    }

    if record.get("qualified_predicate"):
        koza.state["edges_with_qualifiers"] += 1

    association = _build_association(
        association_kwargs, record, subject_id, object_id, predicate, koza.state,
    )

    supporting_studies = _extract_supporting_studies(publications_info)
    if supporting_studies:
        association.has_supporting_studies = supporting_studies

    return KnowledgeGraph(nodes=nodes, edges=[association])
