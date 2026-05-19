"""Unit tests for STRING ingest pure helpers."""

import pytest

from translator_ingest.ingests.string.string import (
    get_latest_version,
    parse_string_protein_id,
    passes_combined_score,
    sorted_pair_key,
)


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
