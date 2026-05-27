"""
STRING + STITCH ingest (human, mouse, rat).

This module hosts two sibling ingests under one yaml config:

  * ``string_ppi`` — protein–protein interactions from STRING
    (``{taxon}.protein.links.v12.0.txt.gz``)
  * ``stitch_pcl`` — protein–chemical interactions from STITCH
    (``{taxon}.protein_chemical.links.v5.0.tsv.gz``)

Both files share STRING's 3-column shape (subject, object, combined_score),
identifier scheme, and >500 medium-confidence cutoff. The ``stitch_pcl``
tag adds a ``CIDm``/``CIDs`` → ``PUBCHEM.COMPOUND`` parser and emits
``ChemicalEntity → interacts_with → Protein`` edges sourced from
``infores:stitch``.

Scope, rationale, and design alternatives are documented in
[CHANGELOG.md](./CHANGELOG.md) and [string_rig.yaml](./string_rig.yaml).

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
    Association,
    ChemicalEntity,
    KnowledgeLevelEnum,
    PairwiseMolecularInteraction,
    Protein,
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    INFORES_STITCH,
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

# STRING's medium-confidence cutoff; matches the default offered on the STRING web UI.
COMBINED_SCORE_THRESHOLD = 500

# PSI-MI interaction type asserted by every STRING edge in this ingest. ``MI:0915``
# is "physical association" — the same claim made by our biolink predicate. Attached
# via ``has_attribute`` so downstream consumers that key off PSI-MI terms can pick
# it up alongside the biolink predicate. See CHANGELOG.md for the slot/term rationale.
STRING_INTERACTION_TYPE_PSI_MI = "MI:0915"

# Filename of the STRING ↔ Entrez gene-ID mapping (universal across species).
# Downloaded by download.yaml into ``koza.input_files_dir``. Loaded once at
# transform start to populate ``equivalent_identifiers`` on Protein nodes.
ENTREZ_MAPPING_FILENAME = "all_organisms.entrez_2_string.tsv"

STRING_SOURCES = build_association_knowledge_sources(primary=INFORES_STRING)

# PSI-MI interaction type for STITCH protein-chemical edges. STITCH aggregates
# multiple evidence channels (binding assays, manual curation, text mining,
# co-occurrence in databases). We use ``MI:0190`` (interaction — root term)
# rather than a more specific binding term because STITCH doesn't characterize
# the *kind* of protein-chemical interaction on a per-row basis.
STITCH_INTERACTION_TYPE_PSI_MI = "MI:0190"

STITCH_SOURCES = build_association_knowledge_sources(primary=INFORES_STITCH)


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


def parse_stitch_chemical_id(stitch_id: str) -> str:
    """
    Convert a STITCH-prefixed chemical identifier into a ``PUBCHEM.COMPOUND`` CURIE.

    STITCH ships chemical IDs in two forms, both encoding a PubChem CID with leading
    zero padding:

      * ``CIDm{8-digit-int}`` — the "merged" (flat / non-stereo) compound
      * ``CIDs{8-digit-int}`` — a specific stereoisomer

    Both map to ``PUBCHEM.COMPOUND:{int}`` after stripping the 4-character prefix
    and any leading zeros. Raises ``ValueError`` for unrecognized formats.

    >>> parse_stitch_chemical_id("CIDm00000234")
    'PUBCHEM.COMPOUND:234'
    >>> parse_stitch_chemical_id("CIDm91758680")
    'PUBCHEM.COMPOUND:91758680'
    >>> parse_stitch_chemical_id("CIDs00012345")
    'PUBCHEM.COMPOUND:12345'
    """
    if not (stitch_id.startswith("CIDm") or stitch_id.startswith("CIDs")):
        raise ValueError(
            f"Expected STITCH chemical ID prefixed with 'CIDm' or 'CIDs', got: {stitch_id!r}"
        )
    digits = stitch_id[4:]
    if not digits.isdigit():
        raise ValueError(
            f"STITCH chemical ID has non-numeric body: {stitch_id!r}"
        )
    return f"PUBCHEM.COMPOUND:{int(digits)}"


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


def sorted_pair_key(p1: str, p2: str) -> tuple[str, str]:
    """
    Order-independent key for deduping symmetric protein pairs.

    STRING's ``protein.links`` file lists each unordered pair in both directions
    (``p1 p2`` and ``p2 p1``). We emit one undirected edge per pair, so we need
    a key that collapses the two rows together.

    >>> sorted_pair_key("ENSEMBL:B", "ENSEMBL:A")
    ('ENSEMBL:A', 'ENSEMBL:B')
    >>> sorted_pair_key("ENSEMBL:A", "ENSEMBL:B")
    ('ENSEMBL:A', 'ENSEMBL:B')
    """
    return tuple(sorted([p1, p2]))


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
    Transform one row of STRING's ``protein.links`` file into two ``Protein`` nodes
    and one ``physically_interacts_with`` edge.

    Rows with ``combined_score`` at or below ``COMBINED_SCORE_THRESHOLD`` are
    dropped. Each unordered pair is emitted at most once: STRING lists ``p1 p2``
    and ``p2 p1`` as separate rows, and the second occurrence is suppressed via
    a ``seen_pairs`` set on ``koza_transform.state``.
    """
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

    seen_pairs: set = koza_transform.state.setdefault("seen_pairs", set())
    key = sorted_pair_key(subject_id, object_id)
    if key in seen_pairs:
        return None
    seen_pairs.add(key)

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
    edge = PairwiseMolecularInteraction(
        id=entity_id(),
        subject=subject_id,
        predicate="biolink:physically_interacts_with",
        object=object_id,
        sources=STRING_SOURCES,
        has_attribute=[STRING_INTERACTION_TYPE_PSI_MI],
        # STRING's combined_score is computationally aggregated across heterogeneous
        # channels (experiments, databases, text mining, co-expression, neighborhood,
        # fusion, co-occurrence). It isn't an explicit curator assertion of any single
        # claim, so ``not_provided`` is honest. Matches the Automat STRING-DB KP.
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
    )
    return KnowledgeGraph(nodes=[subject_node, object_node], edges=[edge])


@koza.transform_record(tag="stitch_pcl")
def transform_stitch_pcl(
    koza_transform: koza.KozaTransform, record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform one row of STITCH's ``protein_chemical.links`` file into one
    ``ChemicalEntity`` node, one ``Protein`` node, and one ``biolink:interacts_with``
    edge directed from chemical to protein.

    Rows with ``combined_score`` at or below ``COMBINED_SCORE_THRESHOLD`` are
    dropped (same medium-confidence cutoff used for STRING). STITCH rows are
    directed (chemical → protein) and not symmetric, so no dedup is needed.
    The edge carries:

      * ``predicate``: ``biolink:interacts_with`` — STITCH doesn't characterize
        the kind of protein-chemical interaction (binding, modulation, substrate),
        so the loose predicate is honest
      * ``has_attribute``: ``[MI:0190]`` (PSI-MI "interaction", root term)
      * ``primary_knowledge_source``: ``infores:stitch``
      * ``knowledge_level=not_provided`` and ``agent_type=not_provided``,
        matching the STRING ingest's rationale (aggregate computational score
        across heterogeneous channels)
    """
    if not passes_combined_score(record["combined_score"]):
        return None

    chemical_id = parse_stitch_chemical_id(record["chemical"])
    protein_id, protein_taxon = parse_string_protein_id(record["protein"])

    chemical_node = ChemicalEntity(
        id=chemical_id,
        category=["biolink:ChemicalEntity"],
    )
    protein_node = Protein(
        id=protein_id,
        category=["biolink:Protein"],
        in_taxon=[protein_taxon],
    )
    edge = Association(
        id=entity_id(),
        subject=chemical_id,
        predicate="biolink:interacts_with",
        object=protein_id,
        sources=STITCH_SOURCES,
        has_attribute=[STITCH_INTERACTION_TYPE_PSI_MI],
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
    )
    return KnowledgeGraph(nodes=[chemical_node, protein_node], edges=[edge])
