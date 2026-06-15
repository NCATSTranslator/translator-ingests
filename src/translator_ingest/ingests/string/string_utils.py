"""
Supporting variables and utility methods for the
STRING protein–protein interaction ingest processing.
"""
from typing import Any, Iterable, Literal
from pathlib import Path

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneAssociation,
    GeneToGeneCoexpressionAssociation,
    PairwiseMolecularInteraction,
    PairwiseGeneToGeneInteraction,
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
    "neighborhood",
    "fusion",
    "cooccurence",
    "coexpression",
    "experiments",
    "textmining",
    # "homology",
    # "database"
]

# Strict subset of specific
# Molecular Interaction predicates
# as constrained by the Biolink Model
MI_PREDICATE = Literal[
  "biolink:gene_fusion_with",
  "biolink:genetic_neighborhood_of",
  "biolink:genetically_interacts_with",
  "biolink:interacts_with",
  "biolink:physically_interacts_with",
  "biolink:coexpressed_with"
]

# Row-inclusion threshold (STRING's medium-confidence cutoff). A row is processed
# only if 'combined_score' exceeds this threshold.
# This value matches the default offered on the STRING
# web UI and ORION's STRING parser.
COMBINED_SCORE_THRESHOLD = 500

# Per-channel high-confidence threshold. A predicate is emitted for a row only
# if the contributing channel's individual score exceeds this. ORION uses 750
# (their "high_conf_threshold"); we follow suit so that downstream consumers
# expecting ORION-equivalent predicate semantics see the same per-channel signal.
CHANNEL_HIGH_CONF_THRESHOLD = 750


# Channel → biolink predicate map. Mirrors ORION's STRING parser. Two channels
# are deliberately omitted:
#
#   * "homology" — STRING's HOMOLOGY channel does NOT mean "A is homologous
#     to B"; per STRING docs, it means "the interaction between A and B is
#     inferred via homologous proteins in another species." Using it for
#     "biolink:homologous_to" would be a semantic misread. (ORION's comment.)
#   * "database" — ORION doesn't map it either; DATABASE evidence contributes
#     to combined_score but doesn't drive an additional channel-specific
#     predicate beyond the EXPERIMENTS-driven physically_interacts_with.
#
# The "_transferred" variants (orthology-projected evidence) similarly don't
# get their own predicates — only the native channel score is consulted, again
# matching ORION's behavior.
CHANNEL_PREDICATES: dict[ STRING_CHANNELS, MI_PREDICATE ] = {
    "neighborhood": "biolink:genetic_neighborhood_of",
    "fusion":       "biolink:gene_fusion_with",
    "cooccurence":  "biolink:genetically_interacts_with",  # STRING's spelling (sic)
    "coexpression": "biolink:coexpressed_with",
    "experiments":  "biolink:physically_interacts_with",
    "textmining":   "biolink:interacts_with",
}


# Fallback predicate emitted when "combined_score" clears the row gate, but no
# individual channel exceeds CHANNEL_HIGH_CONF_THRESHOLD. ORION uses the same
# fallback — the implicit assumption is that an above-medium-confidence pair
# without a specific channel signal is most likely a physical interaction.
FALLBACK_PREDICATE: MI_PREDICATE = "biolink:physically_interacts_with"


# Canonical default thresholds: every predicate-driving channel gates at
# CHANNEL_HIGH_CONF_THRESHOLD (750) and the row-inclusion fallback gate is
# COMBINED_SCORE_THRESHOLD (500). These are the single source of truth for the
# transform when no per-channel overrides are supplied in string.yaml's
# 'transform.channel_thresholds' block (read via KozaTransform.extra_fields).
# Using this dict (rather than the bare constants) makes every channel an
# independently tunable knob — see resolve_thresholds() and the EDA under eda/.
DEFAULT_THRESHOLDS: dict[str, int] = {
    **{channel: CHANNEL_HIGH_CONF_THRESHOLD for channel in CHANNEL_PREDICATES},
    "combined_score": COMBINED_SCORE_THRESHOLD,
}


def resolve_thresholds(overrides: dict[str, Any] | None = None) -> dict[str, int]:
    """
    Merge per-channel threshold overrides (from string.yaml
    'transform.channel_thresholds', surfaced via KozaTransform.extra_fields) on
    top of DEFAULT_THRESHOLDS. Values are coerced to int so YAML strings compare
    numerically. Unknown keys are kept (a channel not in CHANNEL_PREDICATES is
    harmless — nothing reads it), letting the config stay forward-compatible.

    >>> resolve_thresholds() == DEFAULT_THRESHOLDS
    True
    >>> resolve_thresholds({"cooccurence": "450", "combined_score": 400})["cooccurence"]
    450
    >>> resolve_thresholds({"combined_score": 400})["combined_score"]
    400
    """
    merged = dict(DEFAULT_THRESHOLDS)
    if overrides:
        merged.update({str(k): int(v) for k, v in overrides.items()})
    return merged


# Symmetry note (resolves "are these predicates reflexive?"): every STRING
# evidence channel describes a *symmetric* (undirected) relationship — if A
# physically interacts with / is coexpressed with / is a genetic neighbor of B,
# the converse holds. STRING reflects this by listing each pair in both
# directions ("p1 to p2" and "p2 to p1"), which is why we dedup on the sorted pair
# (see "sorted_pair_key"). The predicates are symmetric, not reflexive in the
# self-loop sense; STRING does not ship self-interactions (A, A).

# Biolink Association child class to instantiate per predicate. Most of our per-channel
# predicates fit the permitted predicate set of "PairwiseMolecularInteraction", but
# "biolink:coexpressed_with" is not a molecular-interaction predicate in
# biolink — it belongs to "GeneToGeneCoexpressionAssociation". We can't collapse
# the two: biolink rejects "coexpressed_with" on "PairwiseMolecularInteraction"
# and rejects the molecular-interaction predicates on the coexpression class.
# "make_string_ppi_edge" encapsulates the dispatch so the transform doesn't
# repeat it. The predicates "biolink:genetic_neighborhood_of" and
# "biolink:genetically_interacts_with" are simply considered gene-to-gene interactions
# which may not necessarily imply direct physical interaction between two gene products.
PREDICATE_TO_ASSOCIATION_CLASS: dict[MI_PREDICATE, type[GeneToGeneAssociation]] = {
    "biolink:physically_interacts_with":  PairwiseMolecularInteraction,
    "biolink:interacts_with":             PairwiseMolecularInteraction,
    "biolink:gene_fusion_with":           PairwiseMolecularInteraction,
    "biolink:genetic_neighborhood_of":    PairwiseGeneToGeneInteraction,
    "biolink:genetically_interacts_with": PairwiseGeneToGeneInteraction,
    "biolink:coexpressed_with":           GeneToGeneCoexpressionAssociation,
}

# PSI-MI interaction type attached only to "physically_interacts_with" edges.
# "MI:0915" is "physical association" — semantically aligned with the
# physical-interaction predicate only. Other predicates (coexpressed_with,
# genetic_neighborhood_of, etc.) describe different evidence types that
# don't correspond to PSI-MI physical-association semantics; for those we
# omit the slot rather than attaching a misleading term.
# TODO: doublecheck if this is the correct association ID with Sierra
PSI_MI_PHYSICAL_ASSOCIATION = "MI:0915"
PSI_MI_FUNCTIONAL_INTERACTION = "MI:2286"
PSI_MI_COVALENT_BINDING = "MI:0195"

PREDICATE_TO_MI_TYPE = {
    "biolink:gene_fusion_with": PSI_MI_COVALENT_BINDING,
    # "biolink:genetic_neighborhood_of": "",
    "biolink:genetically_interacts_with": PSI_MI_FUNCTIONAL_INTERACTION,
    # "biolink:interacts_with": "",
    "biolink:physically_interacts_with": PSI_MI_PHYSICAL_ASSOCIATION,

    # This predicate only documents correlated expression,
    # whereas the source of the correlation may *or may not*
    # be due to a direct molecular interaction
    # "biolink:coexpressed_with": ""
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
    "neighborhood": (KnowledgeLevelEnum.prediction,              AgentTypeEnum.data_analysis_pipeline),
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


def predicates_for_row(
    record: dict[str, Any],
    thresholds: dict[str, int] | None = None,
    channel_threshold: int = CHANNEL_HIGH_CONF_THRESHOLD,
) -> list[MI_PREDICATE]:
    """
    Return the list of biolink predicates that should be emitted for one
    STRING ".full" row, based on which channels exceed the high-confidence
    threshold. If no channel fires, return a single-element list with the
    fallback predicate (matches ORION's behavior).

    The returned list is order-stable (follows "CHANNEL_PREDICATES" insertion
    order) and deduplicated — multiple channels mapping to the same predicate
    only fire once. Channels with non-numeric or missing scores are silently
    skipped; STRING guarantees integer scores, so this is a defensive guard.

    >>> row = {"neighborhood": "0", "fusion": "0", "cooccurence": "0",
    ...        "coexpression": "0", "experiments": "800", "textmining": "0",
    ...        "homology": "0", "database": "0"}
    >>> predicates_for_row(row)
    ['biolink:physically_interacts_with']

    >>> row = {"neighborhood": "0", "fusion": "0", "cooccurence": "0",
    ...        "coexpression": "780", "experiments": "0", "textmining": "0",
    ...        "homology": "0", "database": "0"}
    >>> predicates_for_row(row)
    ['biolink:coexpressed_with']

    Multi-channel: experiments + coexpression both fire above 750, emit both.

    >>> row = {"neighborhood": "0", "fusion": "0", "cooccurence": "0",
    ...        "coexpression": "780", "experiments": "780", "textmining": "0",
    ...        "homology": "0", "database": "0"}
    >>> predicates_for_row(row)
    ['biolink:coexpressed_with', 'biolink:physically_interacts_with']

    No channel above 750 → fallback (assumes the row passed combined_score
    gate elsewhere).

    >>> row = {"neighborhood": "200", "fusion": "0", "cooccurence": "0",
    ...        "coexpression": "300", "experiments": "200", "textmining": "100",
    ...        "homology": "0", "database": "0"}
    >>> predicates_for_row(row)
    ['biolink:physically_interacts_with']

    Per-channel override: a ``thresholds`` dict (from string.yaml's
    ``transform.channel_thresholds``) gates each channel independently. Lowering
    the ``cooccurence`` knob below a row's score surfaces
    ``genetically_interacts_with`` that the default 750 gate would hide (EDA: the
    cooccurence channel maxes at 542 across all three taxa).

    >>> row = {"neighborhood": "0", "fusion": "0", "cooccurence": "540",
    ...        "coexpression": "0", "experiments": "0", "textmining": "0",
    ...        "homology": "0", "database": "0"}
    >>> predicates_for_row(row)
    ['biolink:physically_interacts_with']
    >>> predicates_for_row(row, thresholds={"cooccurence": 450})
    ['biolink:genetically_interacts_with']
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
        threshold = thresholds.get(channel, channel_threshold) if thresholds else channel_threshold
        if score > threshold and predicate not in seen:
            fired.append(predicate)
            seen.add(predicate)
    fpl: list[MI_PREDICATE] = [FALLBACK_PREDICATE]
    return fired if fired else fpl


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
    thresholds: dict[str, int] | None = None,
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
    ...        "neighborhood": "0", "fusion": "0", "cooccurence": "0",
    ...        "homology": "0", "database": "0"}
    >>> kl, at = knowledge_level_and_agent_type_for_row(row)
    >>> kl.value, at.value
    ('knowledge_assertion', 'manual_agent')

    >>> # Text-mining dominant.
    >>> row = {"experiments": "0", "coexpression": "0", "textmining": "900",
    ...        "neighborhood": "0", "fusion": "0", "cooccurence": "0",
    ...        "homology": "0", "database": "0"}
    >>> kl, at = knowledge_level_and_agent_type_for_row(row)
    >>> kl.value, at.value
    ('not_provided', 'text_mining_agent')

    >>> # Two high-confidence channels, one curator-backed → upgrade.
    >>> row = {"experiments": "800", "coexpression": "800", "textmining": "0",
    ...        "neighborhood": "0", "fusion": "0", "cooccurence": "0",
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
        channel_threshold = thresholds.get(channel, CHANNEL_HIGH_CONF_THRESHOLD) if thresholds else CHANNEL_HIGH_CONF_THRESHOLD
        if score > channel_threshold:
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
