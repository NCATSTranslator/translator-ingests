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
    "string_id,expected_curie,expected_taxon",
    [
        ("9606.ENSP00000478725",     "ENSEMBL:ENSP00000478725",    "NCBITaxon:9606"),
        ("9606.ENSP00000000001",     "ENSEMBL:ENSP00000000001",    "NCBITaxon:9606"),
        ("10090.ENSMUSP00000000001", "ENSEMBL:ENSMUSP00000000001", "NCBITaxon:10090"),
        ("10116.ENSRNOP00000000001", "ENSEMBL:ENSRNOP00000000001", "NCBITaxon:10116"),
    ],
)
def test_parse_string_protein_id(string_id, expected_curie, expected_taxon):
    assert parse_string_protein_id(string_id) == (expected_curie, expected_taxon)


def test_parse_string_protein_id_rejects_unsupported_taxon():
    with pytest.raises(ValueError, match="Unsupported taxon prefix"):
        parse_string_protein_id("4932.YAL001C")  # yeast, not in our target set


@pytest.mark.parametrize(
    "string_id",
    [
        "ENSP00000478725",  # No taxon prefix
        "",                 # Empty
        "9606.",            # Empty ENSP part
        ".ENSP00000478725", # Empty taxon part
    ],
)
def test_parse_string_protein_id_rejects_malformed(string_id):
    with pytest.raises(ValueError):
        parse_string_protein_id(string_id)


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
H1 = "9606.ENSP00000478725"
H2 = "9606.ENSP00000478289"
H3 = "9606.ENSP00000481152"
# Plausible mouse and rat IDs (real ENSEMBL identifier formats).
M1 = "10090.ENSMUSP00000000001"
M2 = "10090.ENSMUSP00000000002"
R1 = "10116.ENSRNOP00000000001"
R2 = "10116.ENSRNOP00000000002"


@pytest.mark.parametrize(
    "p1,p2,expected_taxon",
    [
        (H1, H2, "NCBITaxon:9606"),
        (M1, M2, "NCBITaxon:10090"),
        (R1, R2, "NCBITaxon:10116"),
    ],
)
def test_transform_emits_protein_pair_above_threshold(mock_koza, p1, p2, expected_taxon):
    result = transform_record(
        mock_koza,
        {"protein1": p1, "protein2": p2, "combined_score": "540"},
    )
    assert result is not None
    assert len(result.nodes) == 2
    assert len(result.edges) == 1

    for node in result.nodes:
        assert node.category == ["biolink:Protein"]
        assert node.in_taxon == [expected_taxon]

    edge = result.edges[0]
    assert edge.predicate == "biolink:physically_interacts_with"
    assert edge.knowledge_level == KnowledgeLevelEnum.not_provided
    assert edge.agent_type == AgentTypeEnum.not_provided
    # PSI-MI interaction-type CURIE (MI:0915 physical association) attached via has_attribute.
    assert edge.has_attribute == ["MI:0915"]
    primary = next(
        s for s in edge.sources if s.resource_role == "primary_knowledge_source"
    )
    assert primary.resource_id == "infores:string"


@pytest.mark.parametrize("score", ["500", "499", "0"])
def test_transform_drops_rows_at_or_below_threshold(mock_koza, score):
    result = transform_record(
        mock_koza,
        {"protein1": H1, "protein2": H2, "combined_score": score},
    )
    assert result is None


def test_transform_dedupes_symmetric_duplicate(mock_koza):
    """STRING lists each pair twice (p1→p2 and p2→p1). Emit only once."""
    first = transform_record(
        mock_koza,
        {"protein1": H1, "protein2": H2, "combined_score": "952"},
    )
    second = transform_record(
        mock_koza,
        {"protein1": H2, "protein2": H1, "combined_score": "952"},
    )
    assert first is not None
    assert second is None


def test_transform_keeps_distinct_pairs(mock_koza):
    """Dedup is per-pair, not global."""
    first = transform_record(
        mock_koza,
        {"protein1": H1, "protein2": H2, "combined_score": "952"},
    )
    second = transform_record(
        mock_koza,
        {"protein1": H1, "protein2": H3, "combined_score": "952"},
    )
    assert first is not None
    assert second is not None


def test_transform_rejects_unsupported_taxon(mock_koza):
    """A row from a non-target species (e.g. yeast) should raise loudly."""
    with pytest.raises(ValueError, match="Unsupported taxon prefix"):
        transform_record(
            mock_koza,
            {"protein1": "4932.YAL001C", "protein2": "4932.YAL002W", "combined_score": "952"},
        )


def test_transform_rejects_cross_species_pair(mock_koza):
    """Per-organism STRING files only contain intra-species rows; defend against corruption."""
    with pytest.raises(ValueError, match="Cross-species pair"):
        transform_record(
            mock_koza,
            {"protein1": H1, "protein2": M1, "combined_score": "952"},
        )
