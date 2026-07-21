"""Unit tests for STRING ingest helpers and transform."""

import pytest

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Protein,
    AgentTypeEnum,
    KnowledgeLevelEnum,
)

from tests.unit.ingests import MockKozaTransform, MockKozaWriter

from translator_ingest.ingests.string.string import (
    get_latest_version,
    transform_string_ppi,
)
from translator_ingest.ingests.string.string_utils import (
    ALWAYS_PREDICATE,
    COMBINED_SCORE_THRESHOLD,
    CONDITIONAL_CHANNEL_PREDICATES,
    DEFAULT_THRESHOLDS,
    EDGE_KL_AT,
    edges_for_row,
    load_string_to_entrez_mapping,
    parse_string_protein_id,
    passes_combined_score,
    resolve_thresholds,
    sorted_pair_key,
)


def _full_row(
    protein1: str,
    protein2: str,
    combined_score: str | int,
    **channel_scores: int | str,
) -> dict:
    """Build a STRING ".full" record dict. Channels default to 0 unless
    overridden by the keyword arg (named after the channel column, e.g.
    "experiments=800"). Saves lots of dict-spelling-outs in transform tests.
    """
    row = {"protein1": protein1, "protein2": protein2, "combined_score": str(combined_score)}
    for ch in [
        "neighborhood_transferred",
        "fusion", "cooccurence", "homology",
        "coexpression", "coexpression_transferred",
        "experiments", "experiments_transferred",
        "database", "database_transferred",
        "textmining", "textmining_transferred",
    ]:
        row[ch] = str(channel_scores.get(ch, 0))
    return row


# Fixture mapping for tests: covers the canonical pairs used in the transform
# tests below. Real ingest loads this via on_data_begin from the 285 MB
# all_organisms.entrez_2_string.tsv file; tests inject this small dict directly.
FIXTURE_STRING_TO_ENTREZ: dict[str, list[str]] = {
    "9606.ENSP00000478725":     ["NCBIGene:7157"],   # plausible TP53-like
    "9606.ENSP00000478289":     ["NCBIGene:4193"],   # plausible MDM2-like
    "9606.ENSP00000481152":     ["NCBIGene:1234", "NCBIGene:5678"],  # multi-mapping example
    "10090.ENSMUSP00000000001": ["NCBIGene:11428"],
    "10090.ENSMUSP00000000002": ["NCBIGene:11429"],
    "10116.ENSRNOP00000000001": ["NCBIGene:24152"],
    "10116.ENSRNOP00000000002": ["NCBIGene:24153"],
    # NOTE: H3 ("9606.ENSP00000000001" used in some tests) deliberately absent
    # to exercise the no-mapping code path.
}


@pytest.fixture
def mock_koza() -> koza.KozaTransform:
    """Fresh MockKozaTransform with empty state and fixture mapping for each test."""
    mk = MockKozaTransform(extra_fields={}, writer=MockKozaWriter(), mappings={})
    mk.state = {"string_to_entrez": FIXTURE_STRING_TO_ENTREZ}
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
        ("710", True),   # just above the boundary
        ("701", True),   # one above the boundary
        ("700", False),  # at the boundary; strict greater-than
        ("699", False),  # just below
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
        # Already sorted; default predicate "" component
        ("ENSEMBL:A", "ENSEMBL:B", ("ENSEMBL:A", "ENSEMBL:B", "")),
        # Reverse order should normalize
        ("ENSEMBL:B", "ENSEMBL:A", ("ENSEMBL:A", "ENSEMBL:B", "")),
        # Realistic ENSP IDs from Automat
        (
            "ENSEMBL:ENSP00000481152",
            "ENSEMBL:ENSP00000478289",
            ("ENSEMBL:ENSP00000478289", "ENSEMBL:ENSP00000481152", ""),
        ),
    ],
)
def test_sorted_pair_key(p1, p2, expected):
    assert sorted_pair_key(p1, p2) == expected


def test_sorted_pair_key_collapses_symmetric_rows():
    """"p1 p2" and "p2 p1" produce the same key for the same predicate."""
    a, b = "ENSEMBL:ENSP00000481152", "ENSEMBL:ENSP00000478289"
    assert sorted_pair_key(a, b, "biolink:coexpressed_with") == \
           sorted_pair_key(b, a, "biolink:coexpressed_with")


def test_sorted_pair_key_distinguishes_predicates():
    """Different predicates produce different keys for the same pair."""
    a, b = "ENSEMBL:A", "ENSEMBL:B"
    assert sorted_pair_key(a, b, "biolink:coexpressed_with") != \
           sorted_pair_key(a, b, "biolink:associated_with")


# Network-dependent: hits https://string-db.org/api/json/version.
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


# ──── edges_for_row tests ─────────────────────────────────────────────────────


def test_edges_for_row_always_emits_associated_with():
    """Every row above the combined_score gate yields an associated_with edge."""
    row = {"combined_score": "800", "experiments": "0", "coexpression": "0"}
    result = edges_for_row(row)
    assert len(result) >= 1
    assert result[0] == (ALWAYS_PREDICATE, 800)


def test_edges_for_row_experiments_above_threshold_fires_physical():
    """experiments > threshold adds directly_physically_interacts_with."""
    row = {"combined_score": "800", "experiments": "800", "coexpression": "0"}
    predicates = [p for p, _ in edges_for_row(row)]
    assert "biolink:directly_physically_interacts_with" in predicates


def test_edges_for_row_coexpression_above_threshold_fires_coexpressed():
    """coexpression > threshold adds coexpressed_with."""
    row = {"combined_score": "800", "experiments": "0", "coexpression": "800"}
    predicates = [p for p, _ in edges_for_row(row)]
    assert "biolink:coexpressed_with" in predicates


def test_edges_for_row_at_threshold_does_not_fire():
    """At exactly the threshold is not above it — no conditional edge fires."""
    row = {"combined_score": "800", "experiments": "750", "coexpression": "750"}
    result = edges_for_row(row)
    assert len(result) == 1  # associated_with only
    assert result[0][0] == ALWAYS_PREDICATE


def test_edges_for_row_both_channels_fire():
    """Both conditional channels firing yields three total edges."""
    row = {"combined_score": "900", "experiments": "800", "coexpression": "800"}
    predicates = [p for p, _ in edges_for_row(row)]
    assert set(predicates) == {
        "biolink:associated_with",
        "biolink:directly_physically_interacts_with",
        "biolink:coexpressed_with",
    }


def test_edges_for_row_carries_channel_score():
    """The score component of each tuple matches the raw channel value."""
    row = {"combined_score": "850", "experiments": "820", "coexpression": "0"}
    result = edges_for_row(row)
    by_pred = {p: s for p, s in result}
    assert by_pred["biolink:associated_with"] == 850
    assert by_pred["biolink:directly_physically_interacts_with"] == 820


def test_edges_for_row_per_channel_threshold_override():
    """A thresholds dict overrides individual channels independently."""
    row = {"combined_score": "800", "experiments": "760", "coexpression": "0"}
    # Default (750): 760 > 750 → fires.
    assert "biolink:directly_physically_interacts_with" in [
        p for p, _ in edges_for_row(row)
    ]
    # Raised to 800: 760 is not > 800 → does not fire.
    lowered = edges_for_row(row, thresholds={"experiments": 800, "coexpression": 750, "combined_score": 700})
    assert "biolink:directly_physically_interacts_with" not in [p for p, _ in lowered]


def test_edges_for_row_non_predicate_channels_never_fire():
    """Channels that have no entry in CONDITIONAL_CHANNEL_PREDICATES never
    add edges, even with very high scores."""
    row = {
        "combined_score": "900",
        "experiments": "0", "coexpression": "0",
        "textmining": "999", "fusion": "999", "cooccurence": "999",
        "homology": "999", "database": "999",
    }
    predicates = [p for p, _ in edges_for_row(row)]
    # Only always-predicate; the other channels are not in CONDITIONAL_CHANNEL_PREDICATES.
    assert predicates == [ALWAYS_PREDICATE]
    for ch in ["textmining", "fusion", "cooccurence", "homology", "database"]:
        assert ch not in CONDITIONAL_CHANNEL_PREDICATES


# ──── Transform tests ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "p1,p2,expected_taxon",
    [
        (H1, H2, "NCBITaxon:9606"),
        (M1, M2, "NCBITaxon:10090"),
        (R1, R2, "NCBITaxon:10116"),
    ],
)
def test_transform_emits_associated_with_above_threshold(mock_koza, p1, p2, expected_taxon):
    """Every above-threshold row emits at least one associated_with edge.
    Nodes are Protein with correct taxon."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(p1, p2, combined_score=750),
    )
    assert result is not None
    assert result.nodes is not None and result.edges is not None
    assert len(list(result.nodes)) == 2
    edges = list(result.edges)
    assert len(edges) >= 1
    assert edges[0].predicate == "biolink:associated_with"

    for node in result.nodes:
        assert node.category == ["biolink:Protein"]
        assert isinstance(node, Protein) and node.in_taxon == [expected_taxon]
        assert node.equivalent_identifiers


def test_transform_associated_with_kl_at(mock_koza):
    """The always-emitted associated_with edge carries the fixed KL/AT."""
    result = transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=750))
    assert result is not None and result.edges is not None
    edges = list(result.edges)
    assoc = next(e for e in edges if e.predicate == "biolink:associated_with")
    expected_kl, expected_at = EDGE_KL_AT["biolink:associated_with"]
    assert assoc.knowledge_level == expected_kl
    assert assoc.agent_type == expected_at


def test_transform_experiments_fires_directly_physically_interacts_with(mock_koza):
    """experiments > threshold adds directly_physically_interacts_with."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=900, experiments=800),
    )
    assert result is not None and result.edges is not None
    predicates = {e.predicate for e in result.edges}
    assert "biolink:directly_physically_interacts_with" in predicates
    assert "biolink:associated_with" in predicates


def test_transform_coexpression_fires_coexpressed_with(mock_koza):
    """coexpression > threshold adds coexpressed_with."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=900, coexpression=800),
    )
    assert result is not None and result.edges is not None
    predicates = {e.predicate for e in result.edges}
    assert "biolink:coexpressed_with" in predicates
    assert "biolink:associated_with" in predicates


def test_transform_both_channels_fire(mock_koza):
    """Both channels above threshold yields all three edge types."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=900, experiments=800, coexpression=800),
    )
    assert result is not None and result.edges is not None
    predicates = {e.predicate for e in result.edges}
    assert predicates == {
        "biolink:associated_with",
        "biolink:directly_physically_interacts_with",
        "biolink:coexpressed_with",
    }


def test_transform_conditional_edges_carry_correct_kl_at(mock_koza):
    """Each edge type carries its fixed KL/AT from EDGE_KL_AT."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=900, experiments=800, coexpression=800),
    )
    assert result is not None and result.edges is not None
    for edge in result.edges:
        expected_kl, expected_at = EDGE_KL_AT[edge.predicate]
        assert edge.knowledge_level == expected_kl, f"KL mismatch on {edge.predicate}"
        assert edge.agent_type == expected_at, f"AT mismatch on {edge.predicate}"


def test_transform_non_predicate_channels_do_not_add_edges(mock_koza):
    """textmining, fusion, cooccurence, homology at high scores yield only associated_with."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=900,
                  textmining=999, fusion=999, cooccurence=999, homology=999),
    )
    assert result is not None and result.edges is not None
    predicates = [e.predicate for e in result.edges]
    assert predicates == ["biolink:associated_with"]


def test_transform_dedupes_per_pair_per_predicate(mock_koza):
    """Symmetric duplicate row for the SAME predicate is suppressed; a DIFFERENT
    predicate for the same pair is independent and still emits."""
    first = transform_string_ppi(
        mock_koza, _full_row(H1, H2, combined_score=900, experiments=800),
    )
    # Same pair, reversed — same predicates → fully suppressed
    second = transform_string_ppi(
        mock_koza, _full_row(H2, H1, combined_score=900, experiments=800),
    )
    # Same pair, reversed — coexpression didn't fire in first → emits
    third = transform_string_ppi(
        mock_koza, _full_row(H2, H1, combined_score=900, coexpression=800),
    )
    assert first is not None and first.edges is not None
    assert second is None  # full dup (associated_with and directly_physically already seen)
    assert third is not None and third.edges is not None
    assert list(third.edges)[0].predicate == "biolink:coexpressed_with"


def test_transform_populates_equivalent_identifiers_from_mapping(mock_koza):
    """Each Protein node carries its NCBIGene equivalents from the mapping dict."""
    result = transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=952))
    assert result is not None and result.nodes is not None
    by_id = {n.id: n for n in list(result.nodes)}
    assert by_id["ENSEMBL:ENSP00000478725"].equivalent_identifiers == ["NCBIGene:7157"]
    assert by_id["ENSEMBL:ENSP00000478289"].equivalent_identifiers == ["NCBIGene:4193"]


def test_transform_preserves_multimapping(mock_koza):
    """Proteins with multiple Entrez mappings carry the full list."""
    result = transform_string_ppi(mock_koza, _full_row(H1, H3, combined_score=952))
    assert result is not None and result.nodes is not None
    by_id = {n.id: n for n in list(result.nodes)}
    assert by_id["ENSEMBL:ENSP00000481152"].equivalent_identifiers == [
        "NCBIGene:1234",
        "NCBIGene:5678",
    ]


def test_transform_handles_missing_mapping(mock_koza):
    """A protein with no Entrez mapping yields 'equivalent_identifiers=None'."""
    result = transform_string_ppi(
        mock_koza, _full_row("9606.ENSP00000000001", H2, combined_score=952),
    )
    assert result is not None and result.nodes is not None
    by_id = {n.id: n for n in result.nodes}
    assert by_id["ENSEMBL:ENSP00000000001"].equivalent_identifiers is None
    assert by_id["ENSEMBL:ENSP00000478289"].equivalent_identifiers == ["NCBIGene:4193"]


@pytest.mark.parametrize("score", ["700", "699", "0"])
def test_transform_drops_rows_at_or_below_threshold(mock_koza, score):
    assert transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=score)) is None


def test_transform_keeps_distinct_pairs(mock_koza):
    """Dedup is per-pair, not global."""
    first = transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=952))
    second = transform_string_ppi(mock_koza, _full_row(H1, H3, combined_score=952))
    assert first is not None
    assert second is not None


def test_transform_rejects_unsupported_taxon(mock_koza):
    """A row from a non-target species (e.g., yeast) should raise loudly."""
    with pytest.raises(ValueError, match="Unsupported taxon prefix"):
        transform_string_ppi(
            mock_koza,
            _full_row("4932.YAL001C", "4932.YAL002W", combined_score=952),
        )


def test_transform_rejects_cross_species_pair(mock_koza):
    """Per-organism STRING files only contain intra-species rows; defend against corruption."""
    with pytest.raises(ValueError, match="Cross-species pair"):
        transform_string_ppi(mock_koza, _full_row(H1, M1, combined_score=952))


# ──── Configurable per-channel threshold tests ───────────────────────────────


def test_resolve_thresholds_defaults_and_overrides():
    """No overrides → the canonical defaults; overrides coerced to int and merged."""
    assert resolve_thresholds() == DEFAULT_THRESHOLDS
    assert resolve_thresholds(None) == DEFAULT_THRESHOLDS
    merged = resolve_thresholds({"experiments": "800", "combined_score": 600})
    assert merged["experiments"] == 800          # string coerced to int
    assert merged["combined_score"] == 600
    assert merged["coexpression"] == DEFAULT_THRESHOLDS["coexpression"]  # untouched


def test_transform_default_thresholds_no_channel_fire(mock_koza):
    """With channel scores at 0 and combined_score just above 700, only
    associated_with is emitted (no conditional channel fires)."""
    result = transform_string_ppi(
        mock_koza, _full_row(H1, H2, combined_score=750)
    )
    assert result is not None and result.edges is not None
    assert [e.predicate for e in result.edges] == ["biolink:associated_with"]


def test_transform_respects_injected_per_channel_thresholds(mock_koza):
    """Lowering the experiments threshold via injected state surfaces the
    directly_physically_interacts_with edge."""
    mock_koza.state["thresholds"] = resolve_thresholds({"experiments": 700})
    result = transform_string_ppi(
        mock_koza, _full_row(H1, H2, combined_score=800, experiments=750)
    )
    assert result is not None and result.edges is not None
    predicates = {e.predicate for e in result.edges}
    assert "biolink:directly_physically_interacts_with" in predicates


def test_transform_respects_injected_combined_score_gate(mock_koza):
    """The combined_score gate is read from the resolved thresholds too: raising
    it drops a row that the default gate would have kept."""
    mock_koza.state["thresholds"] = resolve_thresholds({"combined_score": 900})
    assert transform_string_ppi(
        mock_koza, _full_row(H1, H2, combined_score=750, experiments=800)
    ) is None


# ──── Entrez mapping tests ────────────────────────────────────────────────────


def test_load_string_to_entrez_mapping(tmp_path):
    """Parser produces a dict[str, list[str]] of CURIEs, skips unsupported taxa,
    preserves order on multi-mapping rows, and tolerates blank/short lines."""
    p = tmp_path / "all_organisms.entrez_2_string.tsv"
    p.write_text(
        "# NCBI taxid / entrez / STRING\n"
        "9606\t381\t9606.ENSP00000000233\n"
        "9606\t9606\t9606.ENSP00000000412\n"
        "9606\t1234\t9606.ENSP00000481152\n"   # first of a multimap
        "9606\t5678\t9606.ENSP00000481152\n"   # second of the same protein
        "10090\t11428\t10090.ENSMUSP00000000001\n"
        "4932\t850001\t4932.YAL001C\n"          # unsupported taxon must be skipped
        "\n"                                     # blank line must be tolerated
        "9606\tmalformed\n"                      # short line must be skipped
    )
    mapping = load_string_to_entrez_mapping(p)
    assert mapping == {
        "9606.ENSP00000000233": ["NCBIGene:381"],
        "9606.ENSP00000000412": ["NCBIGene:9606"],
        "9606.ENSP00000481152": ["NCBIGene:1234", "NCBIGene:5678"],
        "10090.ENSMUSP00000000001": ["NCBIGene:11428"],
    }
    assert "4932.YAL001C" not in mapping
