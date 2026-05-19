"""Unit tests for STRING ingest helpers and transform."""

import pytest

import koza
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    KnowledgeLevelEnum,
)

from tests.unit.ingests import MockKozaTransform, MockKozaWriter

from translator_ingest.ingests.string.string import (
    get_latest_version,
    parse_string_protein_id,
    passes_combined_score,
    sorted_pair_key,
    transform_record,
)


@pytest.fixture
def mock_koza() -> koza.KozaTransform:
    """Fresh MockKozaTransform with empty state for each test."""
    mk = MockKozaTransform(extra_fields={}, writer=MockKozaWriter(), mappings={})
    mk.state = {}
    return mk


@pytest.mark.parametrize(
    "string_id,taxon_prefix,expected",
    [
        ("9606.ENSP00000478725", "9606", "ENSEMBL:ENSP00000478725"),
        ("9606.ENSP00000000001", "9606", "ENSEMBL:ENSP00000000001"),
        # Yeast ORF-style identifier (no leading ENSP); the prefix-strip is
        # independent of identifier format, so this works too.
        ("4932.YAL001C", "4932", "ENSEMBL:YAL001C"),
        # Default taxon_prefix is human, so an explicit arg isn't required.
        ("9606.ENSP00000481152", None, "ENSEMBL:ENSP00000481152"),
    ],
)
def test_parse_string_protein_id(string_id, taxon_prefix, expected):
    if taxon_prefix is None:
        assert parse_string_protein_id(string_id) == expected
    else:
        assert parse_string_protein_id(string_id, taxon_prefix=taxon_prefix) == expected


@pytest.mark.parametrize(
    "string_id,taxon_prefix",
    [
        # Wrong taxon prefix
        ("10090.ENSP00000478725", "9606"),
        # Missing taxon prefix entirely
        ("ENSP00000478725", "9606"),
        # Empty string
        ("", "9606"),
    ],
)
def test_parse_string_protein_id_rejects_bad_prefix(string_id, taxon_prefix):
    with pytest.raises(ValueError, match="Expected STRING ID prefixed"):
        parse_string_protein_id(string_id, taxon_prefix=taxon_prefix)


@pytest.mark.parametrize(
    "score,expected",
    [
        ("952", True),   # high confidence
        ("540", True),   # just above the boundary
        ("501", True),   # one above the boundary
        ("500", False),  # at the boundary; strict greater-than
        ("499", False),  # just below
        ("0", False),    # zero
        (952, True),     # accepts int too
    ],
)
def test_passes_combined_score(score, expected):
    assert passes_combined_score(score) is expected


def test_passes_combined_score_custom_threshold():
    assert passes_combined_score("400", threshold=300) is True
    assert passes_combined_score("400", threshold=400) is False
    assert passes_combined_score("400", threshold=500) is False


@pytest.mark.parametrize(
    "p1,p2,expected",
    [
        # Already sorted
        ("ENSEMBL:A", "ENSEMBL:B", ("ENSEMBL:A", "ENSEMBL:B")),
        # Reverse order should normalize
        ("ENSEMBL:B", "ENSEMBL:A", ("ENSEMBL:A", "ENSEMBL:B")),
        # Realistic ENSP IDs from Automat
        (
            "ENSEMBL:ENSP00000481152",
            "ENSEMBL:ENSP00000478289",
            ("ENSEMBL:ENSP00000478289", "ENSEMBL:ENSP00000481152"),
        ),
    ],
)
def test_sorted_pair_key(p1, p2, expected):
    assert sorted_pair_key(p1, p2) == expected


def test_sorted_pair_key_collapses_symmetric_rows():
    """The whole point: ``p1 p2`` and ``p2 p1`` produce the same key."""
    a, b = "ENSEMBL:ENSP00000481152", "ENSEMBL:ENSP00000478289"
    assert sorted_pair_key(a, b) == sorted_pair_key(b, a)


# Network-dependent: hits https://string-db.org/api/json/version.
# Skipped to keep CI hermetic, matching the convention in test_panther.py.
@pytest.mark.skip(reason="hits string-db.org; run manually to verify the version endpoint")
def test_get_latest_version_live():
    version = get_latest_version()
    assert version.startswith("v")
    major, _, minor = version[1:].partition(".")
    assert major.isdigit() and minor.isdigit()


# Realistic ENSP IDs sampled from RENCI's Automat STRING graph (human, v12.0).
P1 = "9606.ENSP00000478725"
P2 = "9606.ENSP00000478289"
P3 = "9606.ENSP00000481152"


def test_transform_emits_protein_pair_above_threshold(mock_koza):
    result = transform_record(
        mock_koza,
        {"protein1": P1, "protein2": P2, "combined_score": "540"},
    )
    assert result is not None
    assert len(result.nodes) == 2
    assert len(result.edges) == 1

    node_ids = {n.id for n in result.nodes}
    assert node_ids == {"ENSEMBL:ENSP00000478725", "ENSEMBL:ENSP00000478289"}
    for node in result.nodes:
        assert node.category == ["biolink:Protein"]
        assert node.in_taxon == ["NCBITaxon:9606"]

    edge = result.edges[0]
    assert edge.subject == "ENSEMBL:ENSP00000478725"
    assert edge.object == "ENSEMBL:ENSP00000478289"
    assert edge.predicate == "biolink:physically_interacts_with"
    assert edge.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
    assert edge.agent_type == AgentTypeEnum.not_provided
    # Sources is built via build_association_knowledge_sources(primary=INFORES_STRING)
    primary = next(
        s for s in edge.sources if s.resource_role == "primary_knowledge_source"
    )
    assert primary.resource_id == "infores:string"


@pytest.mark.parametrize("score", ["500", "499", "0"])
def test_transform_drops_rows_at_or_below_threshold(mock_koza, score):
    result = transform_record(
        mock_koza,
        {"protein1": P1, "protein2": P2, "combined_score": score},
    )
    assert result is None


def test_transform_dedupes_symmetric_duplicate(mock_koza):
    """STRING lists each pair twice (p1→p2 and p2→p1). Emit only once."""
    first = transform_record(
        mock_koza,
        {"protein1": P1, "protein2": P2, "combined_score": "952"},
    )
    second = transform_record(
        mock_koza,
        {"protein1": P2, "protein2": P1, "combined_score": "952"},
    )
    assert first is not None
    assert second is None


def test_transform_keeps_distinct_pairs(mock_koza):
    """Dedup is per-pair, not global."""
    first = transform_record(
        mock_koza,
        {"protein1": P1, "protein2": P2, "combined_score": "952"},
    )
    second = transform_record(
        mock_koza,
        {"protein1": P1, "protein2": P3, "combined_score": "952"},
    )
    assert first is not None
    assert second is not None
    assert first.edges[0].subject != second.edges[0].object or first.edges[0].object != second.edges[0].subject


def test_transform_rejects_non_human_taxon(mock_koza):
    """A non-9606-prefixed protein ID should raise — defensive check."""
    with pytest.raises(ValueError, match="Expected STRING ID prefixed"):
        transform_record(
            mock_koza,
            {"protein1": "10090.ENSMUSP00000000001", "protein2": P2, "combined_score": "952"},
        )
