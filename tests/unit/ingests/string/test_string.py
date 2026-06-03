"""Unit tests for STRING ingest helpers and transform."""

import pytest

import koza
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    KnowledgeLevelEnum,
)

from tests.unit.ingests import MockKozaTransform, MockKozaWriter

from translator_ingest.ingests.string.string import (
    CHANNEL_KL_AT,
    CHANNEL_PREDICATES,
    FALLBACK_PREDICATE,
    get_latest_version,
    knowledge_level_and_agent_type_for_row,
    load_string_to_entrez_mapping,
    parse_string_protein_id,
    passes_combined_score,
    predicates_for_row,
    sorted_pair_key,
    transform_string_ppi,
)


def _full_row(
    protein1: str,
    protein2: str,
    combined_score: str | int,
    **channel_scores: int | str,
) -> dict:
    """Build a STRING ``.full`` record dict. Channels default to 0 unless
    overridden by keyword arg (named after the channel column, e.g.
    ``experiments=800``). Saves a lot of dict-spelling-out in transform tests.
    """
    row = {"protein1": protein1, "protein2": protein2, "combined_score": str(combined_score)}
    for ch in [
        "neighborhood", "neighborhood_transferred",
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
    """Fresh MockKozaTransform with empty state + fixture mapping for each test."""
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
    """``p1 p2`` and ``p2 p1`` produce the same key for the same predicate."""
    a, b = "ENSEMBL:ENSP00000481152", "ENSEMBL:ENSP00000478289"
    assert sorted_pair_key(a, b, "biolink:coexpressed_with") == \
           sorted_pair_key(b, a, "biolink:coexpressed_with")


def test_sorted_pair_key_distinguishes_predicates():
    """Different predicates produce different keys for the same pair, so
    multiple per-channel predicates can coexist without colliding."""
    a, b = "ENSEMBL:A", "ENSEMBL:B"
    assert sorted_pair_key(a, b, "biolink:coexpressed_with") != \
           sorted_pair_key(a, b, "biolink:physically_interacts_with")


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
    """Above-combined-score row with no high-confidence channels emits a single
    fallback edge (``physically_interacts_with``). PSI-MI MI:0915 attached only
    to the physical-interaction predicate."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(p1, p2, combined_score=540),
    )
    assert result is not None
    assert len(result.nodes) == 2
    assert len(result.edges) == 1

    for node in result.nodes:
        assert node.category == ["biolink:Protein"]
        assert node.in_taxon == [expected_taxon]
        assert node.equivalent_identifiers
        for eq in node.equivalent_identifiers:
            assert eq.startswith("NCBIGene:")

    edge = result.edges[0]
    assert edge.predicate == "biolink:physically_interacts_with"
    assert edge.knowledge_level == KnowledgeLevelEnum.not_provided
    assert edge.agent_type == AgentTypeEnum.not_provided
    assert edge.has_attribute == ["MI:0915"]
    primary = next(s for s in edge.sources if s.resource_role == "primary_knowledge_source")
    assert primary.resource_id == "infores:string"


def test_transform_populates_equivalent_identifiers_from_mapping(mock_koza):
    """Each Protein node carries its NCBIGene equivalents from the mapping dict."""
    result = transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=952))
    by_id = {n.id: n for n in result.nodes}
    assert by_id["ENSEMBL:ENSP00000478725"].equivalent_identifiers == ["NCBIGene:7157"]
    assert by_id["ENSEMBL:ENSP00000478289"].equivalent_identifiers == ["NCBIGene:4193"]


def test_transform_preserves_multimapping(mock_koza):
    """Proteins with multiple Entrez mappings carry the full list."""
    result = transform_string_ppi(mock_koza, _full_row(H1, H3, combined_score=952))
    by_id = {n.id: n for n in result.nodes}
    # H3 (ENSP00000481152) maps to two genes in the fixture.
    assert by_id["ENSEMBL:ENSP00000481152"].equivalent_identifiers == [
        "NCBIGene:1234",
        "NCBIGene:5678",
    ]


def test_transform_handles_missing_mapping(mock_koza):
    """A protein with no Entrez mapping yields ``equivalent_identifiers=None``,
    not a crash or empty list."""
    # "9606.ENSP00000000001" is deliberately absent from FIXTURE_STRING_TO_ENTREZ.
    result = transform_string_ppi(
        mock_koza, _full_row("9606.ENSP00000000001", H2, combined_score=952),
    )
    by_id = {n.id: n for n in result.nodes}
    assert by_id["ENSEMBL:ENSP00000000001"].equivalent_identifiers is None
    # The other protein still gets its mapping populated.
    assert by_id["ENSEMBL:ENSP00000478289"].equivalent_identifiers == ["NCBIGene:4193"]


# ──── Per-channel predicate tests ────────────────────────────────────────────


def test_predicates_for_row_single_channel_above_threshold():
    row = {"experiments": "800", "coexpression": "0", "neighborhood": "0",
           "fusion": "0", "cooccurence": "0", "textmining": "0"}
    assert predicates_for_row(row) == ["biolink:physically_interacts_with"]


def test_predicates_for_row_multiple_channels_fire_independently():
    row = {"experiments": "780", "coexpression": "780", "neighborhood": "0",
           "fusion": "0", "cooccurence": "0", "textmining": "0"}
    preds = predicates_for_row(row)
    assert "biolink:physically_interacts_with" in preds
    assert "biolink:coexpressed_with" in preds
    assert len(preds) == 2


def test_predicates_for_row_at_threshold_does_not_fire():
    """Threshold is strict greater-than; equal does not fire."""
    row = {"experiments": "750", "coexpression": "0", "neighborhood": "0",
           "fusion": "0", "cooccurence": "0", "textmining": "0"}
    # No channel exceeds 750 → fallback only.
    assert predicates_for_row(row) == [FALLBACK_PREDICATE]


def test_predicates_for_row_no_channel_above_threshold_returns_fallback():
    row = {"experiments": "500", "coexpression": "400", "neighborhood": "200",
           "fusion": "0", "cooccurence": "0", "textmining": "300"}
    assert predicates_for_row(row) == [FALLBACK_PREDICATE]


def test_predicates_for_row_homology_never_emits_predicate():
    """HOMOLOGY channel is excluded from CHANNEL_PREDICATES because STRING's
    HOMOLOGY score means 'interaction inferred via orthologs in another
    species', NOT 'A is homologous to B'. So even with a maxed-out homology
    score, no predicate is added (and we fall back to physically_interacts_with)."""
    row = {"experiments": "0", "coexpression": "0", "neighborhood": "0",
           "fusion": "0", "cooccurence": "0", "textmining": "0",
           "homology": "999"}
    assert predicates_for_row(row) == [FALLBACK_PREDICATE]
    assert "homology" not in CHANNEL_PREDICATES


def test_predicates_for_row_database_never_emits_predicate():
    """DATABASE is similarly excluded from CHANNEL_PREDICATES (matches ORION).
    Database evidence still contributes to combined_score; it just doesn't
    drive an additional channel-specific predicate."""
    assert "database" not in CHANNEL_PREDICATES


def test_predicates_for_row_all_channels_fire():
    """When every mapped channel is above threshold, all 6 predicates fire."""
    row = {ch: "800" for ch in CHANNEL_PREDICATES}
    preds = predicates_for_row(row)
    assert set(preds) == set(CHANNEL_PREDICATES.values())


@pytest.mark.parametrize(
    "channel_scores,expected_predicates",
    [
        # ORION-style channels
        ({"experiments": 800},  ["biolink:physically_interacts_with"]),
        ({"coexpression": 800}, ["biolink:coexpressed_with"]),
        ({"textmining": 800},   ["biolink:interacts_with"]),
        ({"neighborhood": 800}, ["biolink:genetic_neighborhood_of"]),
        ({"fusion": 800},       ["biolink:gene_fusion_with"]),
        ({"cooccurence": 800},  ["biolink:genetically_interacts_with"]),
    ],
)
def test_transform_emits_predicate_per_high_confidence_channel(
    mock_koza, channel_scores, expected_predicates
):
    result = transform_string_ppi(
        mock_koza, _full_row(H1, H2, combined_score=952, **channel_scores),
    )
    assert result is not None
    predicates = [e.predicate for e in result.edges]
    assert predicates == expected_predicates


def test_transform_emits_multiple_edges_for_multi_channel_row(mock_koza):
    """A row with three high-confidence channels emits three distinct edges
    (one per fired predicate), sharing the same subject/object."""
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=952,
                  experiments=800, coexpression=800, textmining=800),
    )
    assert result is not None
    predicates = {e.predicate for e in result.edges}
    assert predicates == {
        "biolink:physically_interacts_with",
        "biolink:coexpressed_with",
        "biolink:interacts_with",
    }
    # MI:0915 only on the physical-interaction edge; others omit has_attribute.
    physical = next(e for e in result.edges if e.predicate == "biolink:physically_interacts_with")
    assert physical.has_attribute == ["MI:0915"]
    for e in result.edges:
        if e.predicate != "biolink:physically_interacts_with":
            assert e.has_attribute is None


def test_transform_dedupes_per_pair_per_predicate(mock_koza):
    """Symmetric duplicate row for the SAME predicate gets dropped; the same
    pair under a DIFFERENT predicate is independent and still emits."""
    first = transform_string_ppi(
        mock_koza, _full_row(H1, H2, combined_score=952, experiments=800),
    )
    # Same pair, reversed, same predicate → suppressed
    second = transform_string_ppi(
        mock_koza, _full_row(H2, H1, combined_score=952, experiments=800),
    )
    # Same pair, reversed, DIFFERENT predicate → emitted
    third = transform_string_ppi(
        mock_koza, _full_row(H2, H1, combined_score=952, coexpression=800),
    )
    assert first is not None and len(first.edges) == 1
    assert second is None  # full dup
    assert third is not None and len(third.edges) == 1
    assert third.edges[0].predicate == "biolink:coexpressed_with"


# ──── Per-channel knowledge-level / agent-type tests ─────────────────────────


@pytest.mark.parametrize(
    "channel_scores,expected_kl,expected_at",
    [
        # Single dominant channel → that channel's KL/AT
        ({"experiments": 800},  KnowledgeLevelEnum.knowledge_assertion,    AgentTypeEnum.manual_agent),
        ({"database": 800},     KnowledgeLevelEnum.knowledge_assertion,    AgentTypeEnum.manual_agent),
        ({"coexpression": 800}, KnowledgeLevelEnum.statistical_association, AgentTypeEnum.data_analysis_pipeline),
        ({"cooccurence": 800},  KnowledgeLevelEnum.statistical_association, AgentTypeEnum.data_analysis_pipeline),
        ({"neighborhood": 800}, KnowledgeLevelEnum.prediction,             AgentTypeEnum.data_analysis_pipeline),
        ({"fusion": 800},       KnowledgeLevelEnum.prediction,             AgentTypeEnum.data_analysis_pipeline),
        ({"homology": 800},     KnowledgeLevelEnum.prediction,             AgentTypeEnum.computational_model),
        ({"textmining": 800},   KnowledgeLevelEnum.not_provided,           AgentTypeEnum.text_mining_agent),
    ],
)
def test_knowledge_level_and_agent_type_single_channel(channel_scores, expected_kl, expected_at):
    row = {ch: 0 for ch in [
        "neighborhood", "fusion", "cooccurence", "homology",
        "coexpression", "experiments", "database", "textmining",
    ]}
    row.update(channel_scores)
    kl, at = knowledge_level_and_agent_type_for_row(row)
    assert kl == expected_kl
    assert at == expected_at


def test_knowledge_level_multi_high_conf_upgrades_to_manual():
    """Two high-conf channels including a curator-backed one → knowledge_assertion + manual_agent."""
    row = {ch: 0 for ch in CHANNEL_KL_AT}
    row.update({"experiments": 800, "coexpression": 800})  # one manual, one pipeline
    kl, at = knowledge_level_and_agent_type_for_row(row)
    assert kl == KnowledgeLevelEnum.knowledge_assertion
    assert at == AgentTypeEnum.manual_agent


def test_knowledge_level_multi_high_conf_without_manual_uses_pipeline():
    """Two high-conf channels, neither curator-backed → knowledge_assertion + data_analysis_pipeline."""
    row = {ch: 0 for ch in CHANNEL_KL_AT}
    row.update({"coexpression": 800, "neighborhood": 800})  # statistical + prediction, no manual
    kl, at = knowledge_level_and_agent_type_for_row(row)
    assert kl == KnowledgeLevelEnum.knowledge_assertion
    assert at == AgentTypeEnum.data_analysis_pipeline


def test_knowledge_level_all_zero_row_is_not_provided():
    row = {ch: 0 for ch in CHANNEL_KL_AT}
    kl, at = knowledge_level_and_agent_type_for_row(row)
    assert kl == KnowledgeLevelEnum.not_provided
    assert at == AgentTypeEnum.not_provided


def test_transform_propagates_channel_kl_at_to_edges(mock_koza):
    """Every edge from a row shares the row-level KL/AT derived from the dominant channel."""
    # experiments dominant → knowledge_assertion + manual_agent on all edges,
    # including the coexpression edge that also fires.
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=952, experiments=900, coexpression=800),
    )
    assert result is not None
    assert len(result.edges) == 2
    for edge in result.edges:
        assert edge.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
        assert edge.agent_type == AgentTypeEnum.manual_agent


def test_transform_textmining_only_edge_carries_textmining_kl_at(mock_koza):
    result = transform_string_ppi(
        mock_koza,
        _full_row(H1, H2, combined_score=952, textmining=900),
    )
    assert result is not None
    edge = result.edges[0]
    assert edge.predicate == "biolink:interacts_with"
    assert edge.knowledge_level == KnowledgeLevelEnum.not_provided
    assert edge.agent_type == AgentTypeEnum.text_mining_agent


def test_load_string_to_entrez_mapping(tmp_path):
    """Parser produces a dict[str, list[str]] of CURIEs, skips unsupported taxa,
    preserves order on multi-mapping rows, and tolerates blank/short lines."""
    p = tmp_path / "all_organisms.entrez_2_string.tsv"
    p.write_text(
        "# NCBI taxid / entrez / STRING\n"
        "9606\t381\t9606.ENSP00000000233\n"
        "9606\t9606\t9606.ENSP00000000412\n"
        "9606\t1234\t9606.ENSP00000481152\n"   # first of a multi-map
        "9606\t5678\t9606.ENSP00000481152\n"   # second of the same protein
        "10090\t11428\t10090.ENSMUSP00000000001\n"
        "4932\t850001\t4932.YAL001C\n"          # unsupported taxon, must be skipped
        "\n"                                     # blank line, must be tolerated
        "9606\tmalformed\n"                      # short line, must be skipped
    )
    mapping = load_string_to_entrez_mapping(p)
    assert mapping == {
        "9606.ENSP00000000233": ["NCBIGene:381"],
        "9606.ENSP00000000412": ["NCBIGene:9606"],
        "9606.ENSP00000481152": ["NCBIGene:1234", "NCBIGene:5678"],
        "10090.ENSMUSP00000000001": ["NCBIGene:11428"],
    }
    # Yeast (4932) row was filtered out.
    assert "4932.YAL001C" not in mapping


@pytest.mark.parametrize("score", ["500", "499", "0"])
def test_transform_drops_rows_at_or_below_threshold(mock_koza, score):
    assert transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=score)) is None


def test_transform_keeps_distinct_pairs(mock_koza):
    """Dedup is per-pair, not global."""
    first = transform_string_ppi(mock_koza, _full_row(H1, H2, combined_score=952))
    second = transform_string_ppi(mock_koza, _full_row(H1, H3, combined_score=952))
    assert first is not None
    assert second is not None


def test_transform_rejects_unsupported_taxon(mock_koza):
    """A row from a non-target species (e.g. yeast) should raise loudly."""
    with pytest.raises(ValueError, match="Unsupported taxon prefix"):
        transform_string_ppi(
            mock_koza,
            _full_row("4932.YAL001C", "4932.YAL002W", combined_score=952),
        )


def test_transform_rejects_cross_species_pair(mock_koza):
    """Per-organism STRING files only contain intra-species rows; defend against corruption."""
    with pytest.raises(ValueError, match="Cross-species pair"):
        transform_string_ppi(mock_koza, _full_row(H1, M1, combined_score=952))
