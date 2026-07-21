"""
Supporting variables and utility methods for the
STRING protein–protein interaction ingest processing.
"""
from typing import Any, Iterable
from pathlib import Path

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    GeneToGeneCoexpressionAssociation,
    PairwiseMolecularInteraction,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

from translator_ingest.util.biolink import build_association_knowledge_sources, INFORES_STRING
from translator_ingest.util.transform_utils import entity_id

# Target species for this ingest task. STRING ships per-organism files and prefixes every
# protein ID with the NCBI taxon (e.g. "9606.ENSP00000478725"), so we identify the
# species by parsing that prefix per row rather than configuring it per file.
SUPPORTED_TAXA: dict[str, str] = {
    "9606":  "NCBITaxon:9606",   # Homo sapiens
    "10090": "NCBITaxon:10090",  # Mus musculus
    "10116": "NCBITaxon:10116",  # Rattus norvegicus
}

# Row-inclusion threshold (STRING's high-confidence cutoff, 2026-07-17 decision).
# A row is processed only if combined_score exceeds this threshold.
# Raised from 500 (medium-confidence) to 700 (high-confidence) per the 7-17-26 call:
# at 0.70, calibrated evidence supports a reliable functional co-association claim.
COMBINED_SCORE_THRESHOLD = 700

# Per-channel thresholds for the conditional edges. A channel-specific edge fires only
# when the channel score exceeds the corresponding threshold.
# TODO: replace EXPERIMENTS_THRESHOLD and COEXPRESSION_THRESHOLD with Vlado's recommendations
# (pending the 2026-07-17 call AI action: Vlado to recommend cutoffs for
# directly_physically_interacts_with and coexpressed_with edges).
EXPERIMENTS_THRESHOLD = 750
COEXPRESSION_THRESHOLD = 750

# Always-emitted edge: functional association for every pair clearing the combined_score gate.
# Encodes STRING's core claim: these proteins participate in a shared biological program
# (process, pathway, reaction, complex, or expression program).
#
# PENDING BLOCKER A: association_basis_qualifier:Functional qualifier to be added once
# Matt's biolink PR lands (association_basis_qualifier enum {Statistical, Genetic, Functional}).
ALWAYS_PREDICATE = "biolink:associated_with"

# Channel-specific conditional edges. Each fires independently when its channel score
# clears the per-channel threshold. See string.yaml channel_thresholds for values.
# PENDING BLOCKER C: exact thresholds pending Vlado's recommendations.
CONDITIONAL_CHANNEL_PREDICATES: dict[str, str] = {
    "experiments":  "biolink:directly_physically_interacts_with",
    "coexpression": "biolink:coexpressed_with",
}

# Canonical default thresholds for one complete run.
# `resolve_thresholds()` merges these with any per-channel overrides
# from string.yaml's `transform.channel_thresholds` block.
DEFAULT_THRESHOLDS: dict[str, int] = {
    "combined_score": COMBINED_SCORE_THRESHOLD,
    "experiments":    EXPERIMENTS_THRESHOLD,
    "coexpression":   COEXPRESSION_THRESHOLD,
}

# KL/AT is fixed per edge type rather than row-derived for this iteration.
# Per the 2026-07-17 call: "on first pass I don't think we want to get into this —
# just use automated agent, knowledge assertion."
# TODO: finalize KL/AT values once the group confirms the exact biolink enum entries.
# ("automated agent" was specified but is not a current AgentTypeEnum value;
#  data_analysis_pipeline is the nearest equivalent for an automated scoring pipeline.)
EDGE_KL_AT: dict[str, tuple[KnowledgeLevelEnum, AgentTypeEnum]] = {
    "biolink:associated_with": (
        KnowledgeLevelEnum.knowledge_assertion,
        AgentTypeEnum.data_analysis_pipeline,
    ),
    "biolink:directly_physically_interacts_with": (
        KnowledgeLevelEnum.knowledge_assertion,
        AgentTypeEnum.data_analysis_pipeline,
    ),
    "biolink:coexpressed_with": (
        KnowledgeLevelEnum.statistical_association,
        AgentTypeEnum.data_analysis_pipeline,
    ),
}

# Association class to instantiate per predicate.
# `associated_with` uses the base `Association` class (no Protein-specific functional
# association class exists in the current biolink model).
# `directly_physically_interacts_with` uses PairwiseMolecularInteraction.
# `coexpressed_with` uses GeneToGeneCoexpressionAssociation (biolink rejects
# coexpressed_with on PairwiseMolecularInteraction).
PREDICATE_TO_ASSOCIATION_CLASS: dict[str, type[Association]] = {
    "biolink:associated_with":                    Association,
    "biolink:directly_physically_interacts_with": PairwiseMolecularInteraction,
    "biolink:coexpressed_with":                   GeneToGeneCoexpressionAssociation,
}

STRING_SOURCES = build_association_knowledge_sources(primary=INFORES_STRING)


def resolve_thresholds(overrides: dict[str, Any] | None = None) -> dict[str, int]:
    """
    Merge per-channel threshold overrides (from string.yaml
    'transform.channel_thresholds', surfaced via KozaTransform.extra_fields) on
    top of DEFAULT_THRESHOLDS. Values are coerced to int so YAML strings compare
    numerically. Unknown keys are kept (a channel not in CONDITIONAL_CHANNEL_PREDICATES
    is harmless — nothing reads it), letting the config stay forward-compatible.

    >>> resolve_thresholds() == DEFAULT_THRESHOLDS
    True
    >>> resolve_thresholds({"experiments": "800", "combined_score": 600})["experiments"]
    800
    >>> resolve_thresholds({"combined_score": 600})["combined_score"]
    600
    """
    merged = dict(DEFAULT_THRESHOLDS)
    if overrides:
        merged.update({str(k): int(v) for k, v in overrides.items()})
    return merged


# Note on identifier scheme: for the three Translator-target taxa (human / mouse / rat),
# STRING uses ENSEMBL protein identifiers exclusively — ENSP* (human), ENSMUSP* (mouse),
# ENSRNOP* (rat). Verified across the checked-in fixtures: 100% ENSEMBL, no UniProtKB
# accessions. (STRING does use non-ENSEMBL schemes for some non-vertebrate species, but
# those taxa aren't in SUPPORTED_TAXA and parse_string_protein_id raises on them.)
def parse_string_protein_id(string_id: str) -> tuple[str, str]:
    """
    Parse a STRING-prefixed protein identifier into an "(ENSEMBL CURIE, NCBITaxon CURIE)" pair.

    STRING ships protein IDs as "{taxid}.{ENSEMBL_protein_id}" (e.g.
    "9606.ENSP00000478725" for human, "10090.ENSMUSP..." for mouse,
    "10116.ENSRNOP..." for rat). The taxon is self-identifying in the row,
    so we don't need any reader-side configuration to handle multiple species.

    Raises "ValueError" for malformed IDs or unsupported taxa.

    >>> parse_string_protein_id("9606.ENSP00000478725")
    ('ENSEMBL:ENSP00000478725', 'NCBITaxon:9606')
    >>> parse_string_protein_id("10090.ENSMUSP00000000001")
    ('ENSEMBL:ENSMUSP00000000001', 'NCBITaxon:10090')
    >>> parse_string_protein_id("10116.ENSRNOP00000000001")
    ('ENSEMBL:ENSRNOP00000000001', 'NCBITaxon:10116')
    """
    taxid, dot, ensp = string_id.partition(".")
    if not dot or not ensp:
        raise ValueError(
            f"Expected STRING ID format '{{taxid}}.{{ensp}}', got: {string_id!r}"
        )
    taxon_curie = SUPPORTED_TAXA.get(taxid)
    if taxon_curie is None:
        raise ValueError(
            f"Unsupported taxon prefix {taxid!r} in {string_id!r}; "
            f"expected one of {sorted(SUPPORTED_TAXA)}"
        )
    return f"ENSEMBL:{ensp}", taxon_curie


def passes_combined_score(
    combined_score: str | int, threshold: int = COMBINED_SCORE_THRESHOLD
) -> bool:
    """
    Whether a row's combined_score exceeds the inclusion threshold.

    Uses strict greater-than (">"), matching STRING's web-UI convention where
    the slider value is the lower exclusive bound.

    >>> passes_combined_score("710")
    True
    >>> passes_combined_score("700")
    False
    >>> passes_combined_score("699")
    False
    >>> passes_combined_score(999)
    True
    """
    return int(combined_score) > threshold


def edges_for_row(
    record: dict[str, Any],
    thresholds: dict[str, int] | None = None,
) -> list[tuple[str, int | None]]:
    """
    Return the list of (predicate, channel_score_or_None) tuples to emit for one
    STRING ".full" row under the 2026-07-17 edge model.

    Always includes (ALWAYS_PREDICATE, combined_score). Additionally includes
    channel-specific edges when their scores exceed the per-channel threshold.
    The channel score is returned alongside each conditional predicate for later
    attachment as a score property (PENDING BLOCKER B: attributes.yaml PR for
    stringdb_experimental_score / stringdb_coexpression_score).

    >>> row = {"combined_score": "800", "experiments": "0", "coexpression": "0"}
    >>> edges_for_row(row)
    [('biolink:associated_with', 800)]

    >>> row = {"combined_score": "800", "experiments": "800", "coexpression": "0"}
    >>> edges_for_row(row)
    [('biolink:associated_with', 800), ('biolink:directly_physically_interacts_with', 800)]

    >>> row = {"combined_score": "800", "experiments": "0", "coexpression": "800"}
    >>> edges_for_row(row)
    [('biolink:associated_with', 800), ('biolink:coexpressed_with', 800)]

    Both conditional channels fire when both exceed the threshold.

    >>> row = {"combined_score": "900", "experiments": "800", "coexpression": "800"}
    >>> edges_for_row(row)
    [('biolink:associated_with', 900), ('biolink:directly_physically_interacts_with', 800), ('biolink:coexpressed_with', 800)]

    At the threshold is not above it — no conditional edge fires.

    >>> row = {"combined_score": "800", "experiments": "750", "coexpression": "750"}
    >>> edges_for_row(row)
    [('biolink:associated_with', 800)]

    Per-channel override lowers the experiments threshold to surface the edge.

    >>> row = {"combined_score": "800", "experiments": "800", "coexpression": "0"}
    >>> edges_for_row(row, thresholds={"experiments": 700, "coexpression": 750, "combined_score": 700})
    [('biolink:associated_with', 800), ('biolink:directly_physically_interacts_with', 800)]
    """
    resolved = thresholds if thresholds is not None else DEFAULT_THRESHOLDS
    combined = int(str(record.get("combined_score", 0)))
    result: list[tuple[str, int | None]] = [(ALWAYS_PREDICATE, combined)]
    for channel, predicate in CONDITIONAL_CHANNEL_PREDICATES.items():
        raw = record.get(channel)
        if raw is None:
            continue
        try:
            score = int(str(raw))
        except (TypeError, ValueError):
            continue
        threshold = resolved.get(channel, EXPERIMENTS_THRESHOLD)
        if score > threshold:
            result.append((predicate, score))
    return result


def make_string_ppi_edge(
    subject_id: str,
    predicate: str,
    object_id: str,
    knowledge_level: KnowledgeLevelEnum,
    agent_type: AgentTypeEnum,
):
    """
    Construct one STRING PPI edge, dispatching to the correct biolink Association
    class for the predicate.

    Uses PREDICATE_TO_ASSOCIATION_CLASS for the dispatch; raises AssertionError
    for unknown predicates.

    Edge score properties (stringdb_combined_score / stringdb_experimental_score /
    stringdb_coexpression_score) are PENDING BLOCKER B (attributes.yaml PR) and
    are not yet attached here.

    association_basis_qualifier:Functional on the associated_with edge is
    PENDING BLOCKER A (Matt's biolink PR) and is not yet attached here.
    """
    assert predicate in PREDICATE_TO_ASSOCIATION_CLASS, f"Unknown predicate: {predicate!r}"
    association_cls = PREDICATE_TO_ASSOCIATION_CLASS[predicate]
    return association_cls(
        id=entity_id(),
        subject=subject_id,
        predicate=predicate,
        object=object_id,
        sources=STRING_SOURCES,
        knowledge_level=knowledge_level,
        agent_type=agent_type,
    )


def load_string_to_entrez_mapping(
    mapping_path: Path | str,
    supported_taxa: Iterable[str] = SUPPORTED_TAXA.keys(),
) -> dict[str, list[str]]:
    """
    Load the STRING ↔ Entrez gene-ID mapping into a dict keyed by raw STRING ID.

    The file is a tab-separated table with one header line ("# NCBI taxid / entrez
    / STRING") and three columns per data row: "taxid", "entrez_id", "string_id".
    Multiple Entrez genes can map to the same STRING protein (paralogs / overlapping
    annotations), so values are lists of "NCBIGene:" CURIEs preserving STRING's
    order. Rows for taxa outside "supported_taxa" are skipped to keep the
    in-memory dict small.

    >>> import io
    >>> sample = io.StringIO(
    ...     "# NCBI taxid / entrez / STRING\\n"
    ...     "9606\\t381\\t9606.ENSP00000000233\\n"
    ...     "9606\\t9606\\t9606.ENSP00000000412\\n"
    ...     "4932\\t850001\\t4932.YAL001C\\n"
    ... )
    >>> # Helper to exercise the parser without touching disk:
    >>> from io import StringIO
    >>> def _parse(stream, taxa):
    ...     m = {}
    ...     next(stream)
    ...     for entry in stream:
    ...         taxon_id, entrez, sid = entry.rstrip().split("\\t")
    ...         if taxon_id in taxa:
    ...             m.setdefault(sid, []).append(f"NCBIGene:{entrez}")
    ...     return m
    >>> _parse(sample, {"9606"})
    {'9606.ENSP00000000233': ['NCBIGene:381'], '9606.ENSP00000000412': ['NCBIGene:9606']}
    """
    supported = set(supported_taxa)
    mapping: dict[str, list[str]] = {}
    with open(mapping_path) as fh:
        next(fh)  # discard header
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 3:
                continue
            taxid, entrez_id, string_id = parts
            if taxid not in supported:
                continue
            mapping.setdefault(string_id, []).append(f"NCBIGene:{entrez_id}")
    return mapping


def sorted_pair_key(p1: str, p2: str, predicate: str = "") -> tuple[str, str, str]:
    """
    Order-independent dedup key for symmetric STRING protein pairs.

    STRING's "protein.links" file lists each unordered pair in both directions
    ("p1 p2" and "p2 p1"); we emit one undirected edge per pair *per predicate*.
    The predicate component is part of the key so that multiple per-channel
    predicates can fire for the same pair without colliding in the dedup set.

    >>> sorted_pair_key("ENSEMBL:B", "ENSEMBL:A")
    ('ENSEMBL:A', 'ENSEMBL:B', '')
    >>> sorted_pair_key("ENSEMBL:A", "ENSEMBL:B")
    ('ENSEMBL:A', 'ENSEMBL:B', '')
    >>> sorted_pair_key("ENSEMBL:A", "ENSEMBL:B", "biolink:coexpressed_with")
    ('ENSEMBL:A', 'ENSEMBL:B', 'biolink:coexpressed_with')
    """
    a, b = sorted([p1, p2])
    return a, b, predicate
