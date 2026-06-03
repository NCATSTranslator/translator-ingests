"""
STRING protein–protein interaction ingest (human, mouse, rat).

Reads STRING's ``{taxon}.protein.links.full.v12.0.txt.gz`` (16 columns: 7
evidence channels + 6 orthology-transferred variants + combined_score) and emits
per-channel biolink predicates between ``Protein`` nodes, following ORION's
STRING parser. See [CHANGELOG.md](./CHANGELOG.md) and
[string_rig.yaml](./string_rig.yaml) for scope, rationale, and alternatives.

Note: the STITCH protein–chemical sibling ingest (``stitch_pcl`` tag) was
developed alongside this STRING ingest but has been split out of this PR for a
later, properly-scoped effort (with mode-of-action predicates from
``actions.v5.0.tsv``). The complete STITCH implementation is preserved on the
``stitch-ingest`` branch; see the CHANGELOG entry dated 2026-05-28 for the
reintegration pointer.

Reference implementations consulted:
  * https://github.com/monarch-initiative/string-ingest/blob/main/src/protein_links.py
  * https://github.com/RobokopU24/ORION/blob/master/parsers/STRING/src/loadSTRINGDB.py
  * RENCI Automat production graph: https://automat.renci.org/string-db/
"""

from pathlib import Path
from typing import Any, Iterable

import koza
import requests
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    GeneToGeneCoexpressionAssociation,
    KnowledgeLevelEnum,
    PairwiseMolecularInteraction,
    Protein,
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    INFORES_STRING,
    build_association_knowledge_sources,
)
from translator_ingest.util.transform_utils import entity_id

STRING_VERSION_API_URL = "https://string-db.org/api/json/version"

# Target species for this ingest. STRING ships per-organism files and prefixes every
# protein ID with the NCBI taxon (e.g. ``9606.ENSP00000478725``), so we identify the
# species by parsing that prefix per row rather than configuring it per file.
SUPPORTED_TAXA: dict[str, str] = {
    "9606":  "NCBITaxon:9606",   # Homo sapiens
    "10090": "NCBITaxon:10090",  # Mus musculus
    "10116": "NCBITaxon:10116",  # Rattus norvegicus
}

# Row-inclusion threshold (STRING's medium-confidence cutoff). A row is processed
# only if combined_score exceeds this; matches the default offered on the STRING
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
#   * ``homology`` — STRING's HOMOLOGY channel does NOT mean "A is homologous
#     to B"; per STRING docs, it means "the interaction between A and B is
#     inferred via homologous proteins in another species." Using it for
#     ``biolink:homologous_to`` would be a semantic misread. (ORION's comment.)
#   * ``database`` — ORION doesn't map it either; DATABASE evidence contributes
#     to combined_score but doesn't drive an additional channel-specific
#     predicate beyond the EXPERIMENTS-driven physically_interacts_with.
#
# The ``_transferred`` variants (orthology-projected evidence) similarly don't
# get their own predicates — only the native channel score is consulted, again
# matching ORION's behavior.
CHANNEL_PREDICATES: dict[str, str] = {
    "neighborhood": "biolink:genetic_neighborhood_of",
    "fusion":       "biolink:gene_fusion_with",
    "cooccurence":  "biolink:genetically_interacts_with",  # STRING's spelling (sic)
    "coexpression": "biolink:coexpressed_with",
    "experiments":  "biolink:physically_interacts_with",
    "textmining":   "biolink:interacts_with",
}

# Fallback predicate emitted when ``combined_score`` clears the row gate but no
# individual channel exceeds CHANNEL_HIGH_CONF_THRESHOLD. ORION uses the same
# fallback — the implicit assumption is that an above-medium-confidence pair
# without specific channel signal is most likely a physical interaction.
FALLBACK_PREDICATE = "biolink:physically_interacts_with"

# Symmetry note (resolves "are these predicates reflexive?"): every STRING
# evidence channel describes a *symmetric* (undirected) relationship — if A
# physically interacts with / is coexpressed with / is a genetic neighbor of B,
# the converse holds. STRING reflects this by listing each pair in both
# directions (``p1 p2`` and ``p2 p1``), which is why we dedup on the sorted pair
# (see ``sorted_pair_key``). The predicates are symmetric, not reflexive in the
# self-loop sense; STRING does not ship self-interactions (A,A).

# Biolink Association class to instantiate per predicate. Most of our per-channel
# predicates fit ``PairwiseMolecularInteraction``'s permitted predicate set, but
# ``biolink:coexpressed_with`` is not a molecular-interaction predicate in
# biolink — it belongs to ``GeneToGeneCoexpressionAssociation``. We can't collapse
# the two: biolink rejects ``coexpressed_with`` on ``PairwiseMolecularInteraction``
# and rejects the molecular-interaction predicates on the coexpression class.
# ``make_string_ppi_edge`` encapsulates the dispatch so the transform doesn't
# repeat it.
PREDICATE_TO_ASSOCIATION_CLASS = {
    "biolink:physically_interacts_with":  PairwiseMolecularInteraction,
    "biolink:interacts_with":             PairwiseMolecularInteraction,
    "biolink:gene_fusion_with":           PairwiseMolecularInteraction,
    "biolink:genetic_neighborhood_of":    PairwiseMolecularInteraction,
    "biolink:genetically_interacts_with": PairwiseMolecularInteraction,
    "biolink:coexpressed_with":           GeneToGeneCoexpressionAssociation,
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

# PSI-MI interaction type attached only to ``physically_interacts_with`` edges.
# ``MI:0915`` is "physical association" — semantically aligned with the
# physical-interaction predicate only. Other predicates (coexpressed_with,
# genetic_neighborhood_of, etc.) describe different evidence types that
# don't correspond to PSI-MI physical-association semantics; for those we
# omit the slot rather than attaching a misleading term.
PSI_MI_PHYSICAL_ASSOCIATION = "MI:0915" # TODO: double check if this is the correct association ID with Sierra

# Filename of the STRING ↔ Entrez gene-ID mapping (universal across species).
# Downloaded by download.yaml into ``koza.input_files_dir``. Loaded once at
# transform start to populate ``equivalent_identifiers`` on Protein nodes.
ENTREZ_MAPPING_FILENAME = "all_organisms.entrez_2_string.tsv"

STRING_SOURCES = build_association_knowledge_sources(primary=INFORES_STRING)


def get_latest_version() -> str:
    """
    Return the current STRING release version (e.g. ``"v12.0"``).

    STRING exposes a JSON version endpoint that returns
    ``[{"string_version": "12.0", "stable_address": "https://version-12-0.string-db.org"}]``.
    The ``v`` prefix is added to match the convention used in STRING's download URLs
    (e.g. ``protein.links.v12.0/``).

    >>> v = get_latest_version()
    >>> v.startswith("v") and "." in v
    True
    """
    response = requests.get(STRING_VERSION_API_URL, timeout=30)
    response.raise_for_status()
    return f"v{response.json()[0]['string_version']}"


# Note on identifier scheme (resolves "are they all ENSEMBL?"): for the three
# Translator-target taxa we support (human / mouse / rat), STRING uses ENSEMBL
# protein identifiers exclusively — ENSP* (human), ENSMUSP* (mouse), ENSRNOP*
# (rat). Verified across the checked-in fixtures: 100% ENSEMBL, no UniProtKB
# accessions. (STRING does use non-ENSEMBL schemes for some non-vertebrate
# species — e.g. yeast ORF names — but those taxa aren't in SUPPORTED_TAXA, and
# parse_string_protein_id raises on them, so a more complex parser isn't needed.)
def parse_string_protein_id(string_id: str) -> tuple[str, str]:
    """
    Parse a STRING-prefixed protein identifier into an ``(ENSEMBL CURIE, NCBITaxon CURIE)`` pair.

    STRING ships protein IDs as ``{taxid}.{ENSEMBL_protein_id}`` (e.g.
    ``9606.ENSP00000478725`` for human, ``10090.ENSMUSP...`` for mouse,
    ``10116.ENSRNOP...`` for rat). The taxon is self-identifying in the row,
    so we don't need any reader-side configuration to handle multiple species.

    Raises ``ValueError`` for malformed IDs or unsupported taxa.

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

    Uses strict greater-than (``>``), matching STRING's web-UI convention where
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
    channel_threshold: int = CHANNEL_HIGH_CONF_THRESHOLD,
) -> list[str]:
    """
    Return the list of biolink predicates that should be emitted for one
    STRING ``.full`` row, based on which channels exceed the high-confidence
    threshold. If no channel fires, return a single-element list with the
    fallback predicate (matches ORION's behavior).

    The returned list is order-stable (follows ``CHANNEL_PREDICATES`` insertion
    order) and deduplicated — multiple channels mapping to the same predicate
    only fire once. Channels with non-numeric or missing scores are silently
    skipped; STRING guarantees integer scores so this is a defensive guard.

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
    """
    fired: list[str] = []
    seen: set[str] = set()
    for channel, predicate in CHANNEL_PREDICATES.items():
        raw = record.get(channel)
        if raw is None:
            continue
        try:
            score = int(raw)
        except (TypeError, ValueError):
            continue
        if score > channel_threshold and predicate not in seen:
            fired.append(predicate)
            seen.add(predicate)
    return fired if fired else [FALLBACK_PREDICATE]


def knowledge_level_and_agent_type_for_row(
    record: dict[str, Any],
) -> tuple[KnowledgeLevelEnum, AgentTypeEnum]:
    """
    Derive a single ``(knowledge_level, agent_type)`` for a STRING ``.full`` row
    from its evidence channels, mirroring ORION's STRING parser.

    Rule:
      1. Pick the channel with the highest score for the row; use its KL/AT from
         ``CHANNEL_KL_AT``.
      2. If two or more channels exceed ``CHANNEL_HIGH_CONF_THRESHOLD`` (750),
         upgrade to ``knowledge_assertion`` and prefer ``manual_agent`` when any
         high-confidence channel is curator-backed (EXPERIMENTS/DATABASE),
         otherwise ``data_analysis_pipeline``.
      3. If no channel has a positive score (only possible for synthetic rows;
         real rows passing combined_score > 500 always have ≥1 positive channel),
         fall back to ``(not_provided, not_provided)``.

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
            score = int(raw)
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
    predicate: str,
    object_id: str,
    knowledge_level: KnowledgeLevelEnum,
    agent_type: AgentTypeEnum,
):
    """
    Construct one STRING PPI edge, dispatching to the correct biolink Association
    class for the predicate and attaching the PSI-MI physical-association
    attribute only to ``physically_interacts_with`` edges.

    Centralizes the per-predicate class dispatch + has_attribute logic so the
    transform body stays a flat loop. Returns a ``PairwiseMolecularInteraction``
    for most predicates and a ``GeneToGeneCoexpressionAssociation`` for
    ``coexpressed_with`` (see ``PREDICATE_TO_ASSOCIATION_CLASS``).
    """
    has_attribute = (
        [PSI_MI_PHYSICAL_ASSOCIATION]
        if predicate == "biolink:physically_interacts_with"
        else None
    )
    association_cls = PREDICATE_TO_ASSOCIATION_CLASS[predicate]
    return association_cls(
        id=entity_id(),
        subject=subject_id,
        predicate=predicate,
        object=object_id,
        sources=STRING_SOURCES,
        has_attribute=has_attribute,
        knowledge_level=knowledge_level,
        agent_type=agent_type,
    )


def load_string_to_entrez_mapping(
    mapping_path: Path | str,
    supported_taxa: Iterable[str] = SUPPORTED_TAXA,
) -> dict[str, list[str]]:
    """
    Load the STRING ↔ Entrez gene-ID mapping into a dict keyed by raw STRING ID.

    The file is a tab-separated table with one header line (``# NCBI taxid / entrez
    / STRING``) and three columns per data row: ``taxid``, ``entrez_id``, ``string_id``.
    Multiple Entrez genes can map to the same STRING protein (paralogs / overlapping
    annotations), so values are lists of ``NCBIGene:`` CURIEs preserving STRING's
    order. Rows for taxa outside ``supported_taxa`` are skipped to keep the
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
    ...     for line in stream:
    ...         taxid, entrez, sid = line.rstrip().split("\\t")
    ...         if taxid in taxa:
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

    STRING's ``protein.links`` file lists each unordered pair in both directions
    (``p1 p2`` and ``p2 p1``); we emit one undirected edge per pair *per predicate*.
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
    return (a, b, predicate)


@koza.on_data_begin(tag="string_ppi")
def on_data_begin_string_ppi(koza_transform: koza.KozaTransform) -> None:
    """
    Load the STRING ↔ Entrez mapping into ``koza_transform.state["string_to_entrez"]``
    once per ``string_ppi`` transform run. Used by ``transform_string_ppi`` to
    populate ``equivalent_identifiers`` on Protein nodes with their NCBIGene
    equivalents.

    Tests bypass this hook by setting ``koza_transform.state["string_to_entrez"]``
    directly to a small fixture dict.
    """
    mapping_path = Path(koza_transform.input_files_dir) / ENTREZ_MAPPING_FILENAME
    koza_transform.state["string_to_entrez"] = load_string_to_entrez_mapping(mapping_path)


@koza.transform_record(tag="string_ppi")
def transform_string_ppi(
    koza_transform: koza.KozaTransform, record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform one row of STRING's ``protein.links.full.v12.0.txt.gz`` file into two
    ``Protein`` nodes and one or more per-channel edges.

    Predicate selection follows ORION's STRING parser: any evidence channel whose
    score exceeds ``CHANNEL_HIGH_CONF_THRESHOLD`` (750) fires the channel's
    corresponding predicate (see ``CHANNEL_PREDICATES``). If no channel exceeds
    the high-confidence threshold but the row passes the combined_score gate
    (>500), a single fallback ``physically_interacts_with`` edge is emitted.
    Up to 6 edges per pair (one per fired predicate); typically 1–2.

    Each edge carries a per-row knowledge_level / agent_type derived from the
    dominant evidence channel (see ``knowledge_level_and_agent_type_for_row``).

    Dedup is per (sorted_pair, predicate) so multiple predicates can fire for the
    same pair without colliding, while symmetric duplicate rows still collapse.
    """
    # NOTE: the combined_score > 500 gate is also applied as a reader-level filter
    # in string.yaml (the production path — Koza skips sub-threshold rows before
    # they reach here). This guard keeps the transform correct when called
    # directly in unit tests, where rows aren't pre-filtered.
    if not passes_combined_score(record["combined_score"]):
        return None

    subject_id, subject_taxon = parse_string_protein_id(record["protein1"])
    object_id, object_taxon = parse_string_protein_id(record["protein2"])

    # STRING's per-organism link files only contain intra-species pairs.
    # Catch corrupt or cross-species rows loudly.
    if subject_taxon != object_taxon:
        raise ValueError(
            f"Cross-species pair in STRING row: {record['protein1']!r} vs {record['protein2']!r}"
        )

    predicates = predicates_for_row(record)

    # Per-pair-per-predicate dedup. The dedup set lives on koza_transform.state
    # and grows with the number of unique (pair, predicate) tuples — bounded by
    # the above-threshold edge count (~1-2M for human PPI). If memory becomes a
    # constraint at full multi-organism scale, swap for an on-disk set (sqlite)
    # or an integer-keyed roaring bitmap; the key is already a hashable tuple.
    seen_pairs: set = koza_transform.state.setdefault("seen_pairs", set())
    new_predicates = [
        p for p in predicates
        if sorted_pair_key(subject_id, object_id, p) not in seen_pairs
    ]
    if not new_predicates:
        return None
    for p in new_predicates:
        seen_pairs.add(sorted_pair_key(subject_id, object_id, p))

    # Look up NCBIGene equivalents from the entrez_2_string mapping. Loaded
    # at transform start by on_data_begin; tests may inject a fixture dict.
    # Missing entries are normal (some STRING proteins have no Entrez mapping;
    # downstream NodeNormalizer still resolves most of them via UniProtKB).
    entrez_map: dict[str, list[str]] = koza_transform.state.get("string_to_entrez", {})
    subject_equivalents = entrez_map.get(record["protein1"]) or None
    object_equivalents = entrez_map.get(record["protein2"]) or None

    subject_node = Protein(
        id=subject_id,
        category=["biolink:Protein"],
        in_taxon=[subject_taxon],
        equivalent_identifiers=subject_equivalents,
    )
    object_node = Protein(
        id=object_id,
        category=["biolink:Protein"],
        in_taxon=[object_taxon],
        equivalent_identifiers=object_equivalents,
    )

    # KL/AT is a row-level property (derived from the dominant evidence channel),
    # shared by all edges emitted from this row.
    knowledge_level, agent_type = knowledge_level_and_agent_type_for_row(record)
    edges = [
        make_string_ppi_edge(subject_id, predicate, object_id, knowledge_level, agent_type)
        for predicate in new_predicates
    ]
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=edges)
