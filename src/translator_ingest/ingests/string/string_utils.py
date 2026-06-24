"""
Supporting variables and utility methods for the
STRING protein–protein interaction ingest processing.
"""
from typing import Any, Iterable, Literal
from pathlib import Path

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    GeneToGeneCoexpressionAssociation,
    PairwiseMolecularInteraction,
    KnowledgeLevelEnum,
    AgentTypeEnum
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

STRING_CHANNELS = Literal[
    # Predicate-driving channels under Matt Brush's 2026-06-16 mapping. The other
    # STRING channels (textmining, fusion, cooccurence, homology, neighborhood*)
    # are read but do NOT mint their own predicate; their signal flows only
    # through combined_score (which drives the functionally_interacts_with
    # fallback at >0.8). See RIG.
    "experiments",
    "coexpression",
    "database",
]

# The STRING predicate set under Matt Brush's 2026-06-16 mapping.
#
# NOTE: "biolink:functionally_interacts_with" is NOT (yet) a predicate in the
# Biolink Model — it must be added by the modeling leads (Sierra) for the output
# to pass biolink/KGX validation. It is emitted here as a generic "Association"
# (the molecular-interaction classes reject unknown predicates), pending that
# addition. Semantics (Matt): "two proteins participate in one or more common
# biological processes, pathways, reactions, complexes, or cellular functions"
# (not necessarily contributing in the same direction to the process/function).
MI_PREDICATE = Literal[
  "biolink:physically_interacts_with",
  "biolink:coexpressed_with",
  "biolink:functionally_interacts_with",
]

# Coarse row-inclusion floor (STRING's medium-confidence cutoff). Used by the
# reader-level pre-filter in string.yaml and as the default for
# passes_combined_score; a row below this can fire neither a channel predicate
# (those need a channel > 750, hence combined >= ~750) nor the fallback (> 800).
COMBINED_SCORE_THRESHOLD = 500

# Per-channel high-confidence threshold. A channel's predicate is emitted for a
# row only if that channel's individual score exceeds this.
CHANNEL_HIGH_CONF_THRESHOLD = 750

# High overall-confidence threshold (STRING's 0.8 "high confidence" cutoff) for
# the functionally_interacts_with fallback: a row whose combined_score exceeds
# this mints a single functionally_interacts_with edge even when no individual
# channel clears CHANNEL_HIGH_CONF_THRESHOLD (Matt Brush, 2026-06-16).
COMBINED_HIGH_CONF_THRESHOLD = 800


# Channel → biolink predicate map (Matt Brush, 2026-06-16):
#   * experiments  → physically_interacts_with   (high experimental-evidence score)
#   * coexpression → coexpressed_with             (high co-expression score)
#   * database     → functionally_interacts_with  (high curated-database score)
#
# Channels read but deliberately NOT mapped to a predicate (their signal still
# feeds combined_score, which drives the functionally_interacts_with fallback):
#   * textmining — dropped as a standalone driver per the Translator convention
#     on text-mined co-occurrence (cf. the diseases ingest). Removing its
#     contribution from combined_score entirely is a separate longer-term step.
#   * fusion, cooccurence — gene-context channels; the gene-family predicates are
#     biolink domain/range = gene, invalid on our Protein nodes.
#   * neighborhood / homology — see RIG (native neighborhood is 0 in vertebrates;
#     homology means "inferred via orthologs", not "A is homologous to B").
CHANNEL_PREDICATES: dict[ STRING_CHANNELS, MI_PREDICATE ] = {
    "experiments":  "biolink:physically_interacts_with",
    "coexpression": "biolink:coexpressed_with",
    "database":     "biolink:functionally_interacts_with",
}


# Fallback predicate: when no individual channel clears CHANNEL_HIGH_CONF_THRESHOLD
# but the overall combined_score is high (> COMBINED_HIGH_CONF_THRESHOLD, STRING's
# 0.8 cutoff), emit a single functionally_interacts_with edge (Matt Brush,
# 2026-06-16). This replaces the earlier ORION-style physically_interacts_with
# fallback, which over-claimed physical interaction for aggregate-only evidence.
FALLBACK_PREDICATE: MI_PREDICATE = "biolink:functionally_interacts_with"


# Symmetry note (resolves "are these predicates reflexive?"): every STRING
# evidence channel describes a *symmetric* (undirected) relationship — if A
# physically interacts with / is coexpressed with / is a genetic neighbor of B,
# the converse holds. STRING reflects this by listing each pair in both
# directions ("p1 to p2" and "p2 to p1"), which is why we dedup on the sorted pair
# (see "sorted_pair_key"). The predicates are symmetric, not reflexive in the
# self-loop sense; STRING does not ship self-interactions (A, A).

# Biolink Association child class to instantiate per predicate. The three classes
# differ by what biolink permits:
#   * physically_interacts_with → PairwiseMolecularInteraction (permitted predicate)
#   * coexpressed_with → GeneToGeneCoexpressionAssociation (biolink rejects
#     coexpressed_with on PairwiseMolecularInteraction, and vice-versa)
#   * functionally_interacts_with → generic Association, because this predicate is
#     not (yet) in biolink's molecular-interaction enum (PairwiseMolecularInteraction
#     rejects it). Swap to a specific class once biolink adds the predicate.
# "make_string_ppi_edge" encapsulates the dispatch so the transform stays a flat loop.
PREDICATE_TO_ASSOCIATION_CLASS: dict[MI_PREDICATE, type[Association]] = {
    "biolink:physically_interacts_with":   PairwiseMolecularInteraction,
    "biolink:coexpressed_with":            GeneToGeneCoexpressionAssociation,
    "biolink:functionally_interacts_with": Association,
}

# PSI-MI interaction types
PSI_MI_PHYSICAL_ASSOCIATION = "MI:0915"
PSI_MI_FUNCTIONAL_INTERACTION = "MI:2286"
PSI_MI_COVALENT_BINDING = "MI:0195"

PREDICATE_TO_MI_TYPE = {
    "biolink:physically_interacts_with":   PSI_MI_PHYSICAL_ASSOCIATION,    # MI:0915
    "biolink:functionally_interacts_with": PSI_MI_FUNCTIONAL_INTERACTION,  # MI:2286

    # "biolink:coexpressed_with" — correlated expression only; the source of the
    # correlation may or may not be a direct molecular interaction. No PSI-MI type.
}

# Per-channel knowledge-level / agent-type assignment, mirroring ORION's STRING
# parser. Each STRING evidence channel implies a different epistemic status:
# EXPERIMENTS / DATABASE are curated assertions (manual_agent); COEXPRESSION /
# COOCCURENCE are statistical associations from a data pipeline; NEIGHBORHOOD /
# FUSION / HOMOLOGY are computational predictions; TEXTMINING is NLP-derived.
# This map keys on *all 8* STRING channels (including homology and database,
# which don't drive predicates) because KL/AT is a row-level property derived
# from the dominant-evidence channel, independent of which predicates fire.
CHANNEL_KL_AT: dict[str, tuple[KnowledgeLevelEnum, AgentTypeEnum]] = {
    "fusion":       (KnowledgeLevelEnum.prediction,              AgentTypeEnum.data_analysis_pipeline),
    "cooccurence":  (KnowledgeLevelEnum.statistical_association, AgentTypeEnum.data_analysis_pipeline),
    "homology":     (KnowledgeLevelEnum.prediction,              AgentTypeEnum.computational_model),
    "coexpression": (KnowledgeLevelEnum.statistical_association, AgentTypeEnum.data_analysis_pipeline),
    "experiments":  (KnowledgeLevelEnum.knowledge_assertion,     AgentTypeEnum.manual_agent),
    "database":     (KnowledgeLevelEnum.knowledge_assertion,     AgentTypeEnum.manual_agent),
    "textmining":   (KnowledgeLevelEnum.not_provided,            AgentTypeEnum.text_mining_agent),
}

STRING_SOURCES = build_association_knowledge_sources(primary=INFORES_STRING)

# Note on the identifier scheme (resolves "are they all ENSEMBL?"): for the three
# Translator-target taxa we support (human / mouse / rat), STRING uses ENSEMBL
# protein identifiers exclusively — ENSP* (human), ENSMUSP* (mouse), ENSRNOP*
# (rat). Verified across the checked-in fixtures: 100% ENSEMBL, no UniProtKB
# accessions. (STRING does use non-ENSEMBL schemes for some non-vertebrate
# species — e.g., yeast ORF names — but those taxa aren't in SUPPORTED_TAXA, and
# parse_string_protein_id raises an exception on them, so a more complex parser isn't needed.)
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

    >>> passes_combined_score("540")
    True
    >>> passes_combined_score("500")
    False
    >>> passes_combined_score("499")
    False
    >>> passes_combined_score(999)
    True
    """
    return int(combined_score) > threshold


def predicates_for_row(record: dict[str, Any]) -> list[MI_PREDICATE]:
    """
    Return the biolink predicates to emit for one STRING ".full" row, under Matt
    Brush's 2026-06-16 mapping:

      * experiments  > 750 → physically_interacts_with
      * coexpression > 750 → coexpressed_with
      * database     > 750 → functionally_interacts_with
      * else, if combined_score > 800 → functionally_interacts_with (fallback)
      * else → [] (no edge)

    The returned list follows "CHANNEL_PREDICATES" insertion order and is
    deduplicated. Channels with non-numeric or missing scores are skipped.

    >>> def _row(**ch):
    ...     base = {c: "0" for c in ("experiments", "coexpression", "database",
    ...             "textmining", "fusion", "cooccurence", "homology", "combined_score")}
    ...     base.update({k: str(v) for k, v in ch.items()})
    ...     return base

    Single channel above 750:

    >>> predicates_for_row(_row(experiments=800))
    ['biolink:physically_interacts_with']
    >>> predicates_for_row(_row(coexpression=800))
    ['biolink:coexpressed_with']
    >>> predicates_for_row(_row(database=800))
    ['biolink:functionally_interacts_with']

    Multiple channels fire independently (CHANNEL_PREDICATES order):

    >>> predicates_for_row(_row(experiments=800, coexpression=800))
    ['biolink:physically_interacts_with', 'biolink:coexpressed_with']

    No channel above 750 but a high overall combined score (>0.8) → functional fallback
    (text-mining contributes only through combined_score, never its own predicate):

    >>> predicates_for_row(_row(textmining=900, combined_score=850))
    ['biolink:functionally_interacts_with']

    No channel above 750 and combined below 0.8 → no edge:

    >>> predicates_for_row(_row(experiments=700, combined_score=750))
    []
    """
    fired: list[MI_PREDICATE] = []
    seen: set[MI_PREDICATE] = set()
    for channel, predicate in CHANNEL_PREDICATES.items():
        raw = record.get(channel)
        if raw is None:
            continue
        try:
            score = int(str(raw))
        except (TypeError, ValueError):
            continue
        if score > CHANNEL_HIGH_CONF_THRESHOLD and predicate not in seen:
            fired.append(predicate)
            seen.add(predicate)
    if fired:
        return fired
    # Fallback: a high overall combined score (>0.8) mints a single
    # functionally_interacts_with edge even when no single channel qualifies.
    if passes_combined_score(record.get("combined_score", 0), COMBINED_HIGH_CONF_THRESHOLD):
        return [FALLBACK_PREDICATE]
    return []


def molecular_interaction_type(predicate: MI_PREDICATE)-> list[str] | None:
    """
    This method attempts to assign the molecular interaction (MI)
    type code to a given STRING entry, based on the assigned predicate
    See https://ontobee.org/ontology/MI?iri=http://purl.obolibrary.org/obo/MI_0190
    """
    return \
        [PREDICATE_TO_MI_TYPE[predicate]] \
        if predicate in PREDICATE_TO_MI_TYPE \
        else None


def knowledge_level_and_agent_type_for_row(
    record: dict[str, Any],
) -> tuple[KnowledgeLevelEnum, AgentTypeEnum]:
    """
    Derive a single "(knowledge_level, agent_type)" for a STRING ".full" row
    from its evidence channels, mirroring ORION's STRING parser.

    Rule:
      1. Pick the channel with the highest score for the row; use its KL/AT from
         "CHANNEL_KL_AT".
      2. If two or more channels exceed "CHANNEL_HIGH_CONF_THRESHOLD" (750),
         upgrade to "knowledge_assertion" and prefer "manual_agent" when any
         high-confidence channel is curator-backed (EXPERIMENTS/DATABASE),
         otherwise "data_analysis_pipeline".
      3. If no channel has a positive score (only possible for synthetic rows;
         real rows passing combined_score > 500 always have ≥1 positive channel),
         fall back to "(not_provided, not_provided)".

    KL/AT is a row-level property: all edges emitted from a row share it,
    regardless of which per-channel predicates fired.

    >>> # Single dominant channel → that channel's KL/AT.
    >>> row = {"experiments": "800", "coexpression": "100", "textmining": "0",
    ...        "fusion": "0", "cooccurence": "0",
    ...        "homology": "0", "database": "0"}
    >>> kl, at = knowledge_level_and_agent_type_for_row(row)
    >>> kl.value, at.value
    ('knowledge_assertion', 'manual_agent')

    >>> # Text-mining dominant.
    >>> row = {"experiments": "0", "coexpression": "0", "textmining": "900",
    ...        "fusion": "0", "cooccurence": "0",
    ...        "homology": "0", "database": "0"}
    >>> kl, at = knowledge_level_and_agent_type_for_row(row)
    >>> kl.value, at.value
    ('not_provided', 'text_mining_agent')

    >>> # Two high-confidence channels, one curator-backed → upgrade.
    >>> row = {"experiments": "800", "coexpression": "800", "textmining": "0",
    ...        "fusion": "0", "cooccurence": "0",
    ...        "homology": "0", "database": "0"}
    >>> kl, at = knowledge_level_and_agent_type_for_row(row)
    >>> kl.value, at.value
    ('knowledge_assertion', 'manual_agent')

    >>> # All-zero (synthetic) row → not_provided.
    >>> row = {ch: "0" for ch in CHANNEL_KL_AT}
    >>> kl, at = knowledge_level_and_agent_type_for_row(row)
    >>> kl.value, at.value
    ('not_provided', 'not_provided')
    """
    max_score = 0
    dominant_channel: str | None = None
    high_conf_channels: list[str] = []
    for channel in CHANNEL_KL_AT:
        raw = record.get(channel)
        if raw is None:
            continue
        try:
            score = int(str(raw))
        except (TypeError, ValueError):
            continue
        if score > max_score:
            max_score = score
            dominant_channel = channel
        if score > CHANNEL_HIGH_CONF_THRESHOLD:
            high_conf_channels.append(channel)

    if dominant_channel is None:
        return KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided

    if len(high_conf_channels) > 1:
        # Multiple strong channels: treat as a curator-grade assertion. Prefer
        # manual_agent if any high-confidence channel is curator-backed.
        any_manual = any(
            CHANNEL_KL_AT[c][1] == AgentTypeEnum.manual_agent
            for c in high_conf_channels
        )
        agent = AgentTypeEnum.manual_agent if any_manual else AgentTypeEnum.data_analysis_pipeline
        return KnowledgeLevelEnum.knowledge_assertion, agent

    return CHANNEL_KL_AT[dominant_channel]


def make_string_ppi_edge(
    subject_id: str,
    predicate: MI_PREDICATE,
    object_id: str,
    knowledge_level: KnowledgeLevelEnum,
    agent_type: AgentTypeEnum,
):
    """
    Construct one STRING PPI edge, dispatching to the correct biolink Association
    class for the predicate and attaching the PSI-MI physical-association
    attribute only to "physically_interacts_with" edges.

    Centralizes the per-predicate class dispatch + has_attribute logic so the
    transform body stays a flat loop. Returns a "PairwiseMolecularInteraction"
    for most predicates and a "GeneToGeneCoexpressionAssociation" for
    "coexpressed_with" (see "PREDICATE_TO_ASSOCIATION_CLASS").
    """
    assert predicate in PREDICATE_TO_ASSOCIATION_CLASS, f"Unknown predicate: {predicate!r}"
    association_cls = PREDICATE_TO_ASSOCIATION_CLASS[predicate]
    return association_cls(
        id=entity_id(),
        subject=subject_id,
        predicate=predicate,
        object=object_id,
        sources=STRING_SOURCES,
        has_attribute=molecular_interaction_type(predicate),
        knowledge_level=knowledge_level,
        agent_type=agent_type
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
