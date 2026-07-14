import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    CausalGeneToDiseaseAssociation,
    ChemicalAffectsBiologicalEntityAssociation,
    CorrelatedGeneToDiseaseAssociation,
    Disease,
    DiseaseAssociatedWithResponseToChemicalEntityAssociation,
    DiseaseOrPhenotypicFeatureToLocationAssociation,
    Gene,
    GeneRegulatesGeneAssociation,
    GeneToExpressionSiteAssociation,
    GeographicLocation,
    InformationContentEntity,
    KnowledgeLevelEnum,
    MacromolecularMachineToBiologicalProcessAssociation,
    NamedThing,
    NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    Protein,
    Publication,
    RetrievalSource,
    SmallMolecule,
)

from translator_ingest.ingests.mokg.mokg import (
    HAS_ATTRIBUTE_COLUMNS,
    MOKG_SOURCES,
    PREDICATE_TO_ASSOCIATION_CLASS,
    QUALIFIER_SOURCE_TO_SLOT,
    SUPPORTING_TEXT_COLUMNS,
    TYPED_NUMERIC_COLUMN_TO_SLOT,
    create_node,
    normalize_category,
    parse_optional_float,
    transform,
)


# ---------------------------------------------------------------------------
# Node construction
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("node_data", "expected_cls"),
    [
        ({"id": "NCBIGene:1", "name": "A1BG", "category": "biolink:Gene"}, Gene),
        ({"id": "UniProtKB:P04217", "name": "Protein X", "category": "biolink:Protein"}, Protein),
        ({"id": "CHEBI:17154", "name": "Nicotinamide", "category": "biolink:SmallMolecule"}, SmallMolecule),
        ({"id": "MONDO:0005575", "name": "Colorectal cancer", "category": "biolink:Disease"}, Disease),
        ({"id": "GEO:1", "name": "Loc", "category": "biolink:GeographicLocation"}, GeographicLocation),
        # GenomicEntity is a mixin that cannot be instantiated -> NamedThing fallback
        ({"id": "FOO:1", "name": "Genomic", "category": "biolink:GenomicEntity"}, NamedThing),
        # No category at all -> NamedThing fallback
        ({"id": "FOO:2", "name": "No category node"}, NamedThing),
    ],
)
def test_create_node_maps_category(node_data, expected_cls):
    node = create_node(node_data)
    assert isinstance(node, expected_cls)


def test_create_node_normalizes_scalar_category_to_list():
    node = create_node({"id": "NCBIGene:1", "name": "A1BG", "category": "biolink:Gene"})
    assert node.category == ["biolink:Gene"]


def test_create_node_fallback_forces_named_thing_category():
    """NamedThing.category is a literal, so the fallback must use biolink:NamedThing."""
    node = create_node({"id": "FOO:1", "name": "Genomic", "category": "biolink:GenomicEntity"})
    assert isinstance(node, NamedThing)
    assert node.category == ["biolink:NamedThing"]


def test_create_node_carries_taxon_when_present():
    node = create_node(
        {
            "id": "NCBIGene:1",
            "name": "A1BG",
            "category": "biolink:Gene",
            "taxon": "NCBITaxon:9606",
        }
    )
    assert node.taxon == "NCBITaxon:9606"


def test_create_node_publication_carries_authors_year_journal():
    import datetime as _dt
    node = create_node(
        {
            "id": "PMC:1",
            "name": "My Paper",
            "category": "biolink:Publication",
            "first author": "Doe J",
            "journal": "Nature",
            "year published": 2024,
            "source": "BABEL",
            "taxon": "NCBITaxon:9606",
        }
    )
    assert isinstance(node, Publication)
    assert node.publication_type == ["JournalArticle"]
    assert node.authors == ["Doe J"]
    assert node.creation_date == _dt.date(2024, 1, 1)
    assert node.xref == ["Nature"]
    assert node.taxon == "NCBITaxon:9606"
    assert node.provided_by == ["BABEL"]


def test_normalize_category_handles_none_list_and_scalar():
    assert normalize_category(None) == ["biolink:NamedThing"]
    assert normalize_category("biolink:Gene") == ["biolink:Gene"]
    assert normalize_category(["biolink:Gene", "biolink:NamedThing"]) == [
        "biolink:Gene",
        "biolink:NamedThing",
    ]


# ---------------------------------------------------------------------------
# Association construction and round-trip
# ---------------------------------------------------------------------------


def test_association_roundtrip_with_qualifiers_and_statistics():
    """The generic Association carries p_value, adjusted_p_value, has_confidence_score,
    publications, and the disease/anatomical context CURIEs via `qualifiers`."""
    association = Association(
        id="05017423-f0c6-3c34-9190-c1daf01915f0",
        subject="CHEBI:30772",
        predicate="biolink:positively_correlated_with",
        object="NCBITaxon:33033",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        sources=MOKG_SOURCES,
        publications=["PMC:PMC9431300"],
        qualifiers=["MONDO:0005575", "UBERON:0001555"],
        p_value=0.002,
        adjusted_p_value=0.86227902,
        has_confidence_score=0.019010875,
    )
    restored = Association.model_validate(association.model_dump())
    assert restored == association
    assert restored.p_value == pytest.approx(0.002)
    assert restored.adjusted_p_value == pytest.approx(0.86227902)
    assert restored.has_confidence_score == pytest.approx(0.019010875)
    assert restored.qualifiers == ["MONDO:0005575", "UBERON:0001555"]
    assert restored.publications == ["PMC:PMC9431300"]


def test_association_minimal_without_optional_fields():
    """Edges without statistics or qualifiers still validate."""
    association = Association(
        id="abc-123",
        subject="CHEBI:1",
        predicate="biolink:associated_with",
        object="NCBITaxon:1",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        sources=MOKG_SOURCES,
    )
    restored = Association.model_validate(association.model_dump())
    assert restored == association


# ---------------------------------------------------------------------------
# Predicates route to typed Association subclasses
# ---------------------------------------------------------------------------


def test_predicate_map_enumerates_all_eighteen_predicates():
    expected = {
        "biolink:acts_upstream_of_or_within",
        "biolink:affects",
        "biolink:associated_with",
        "biolink:associated_with_increased_likelihood_of",
        "biolink:associated_with_resistance_to",
        "biolink:associated_with_sensitivity_to",
        "biolink:biomarker_for",
        "biolink:correlated_with",
        "biolink:disease_has_location",
        "biolink:expressed_in",
        "biolink:gene_associated_with_condition",
        "biolink:genetically_associated_with",
        "biolink:is_sequence_variant_of",
        "biolink:negatively_correlated_with",
        "biolink:participates_in",
        "biolink:positively_correlated_with",
        "biolink:regulates",
        "biolink:related_to",
    }
    assert set(PREDICATE_TO_ASSOCIATION_CLASS) == expected
    assert len(PREDICATE_TO_ASSOCIATION_CLASS) == 18


@pytest.mark.parametrize(
    ("predicate", "expected_cls"),
    [
        ("biolink:acts_upstream_of_or_within",             CausalGeneToDiseaseAssociation),
        ("biolink:affects",                                 ChemicalAffectsBiologicalEntityAssociation),
        ("biolink:associated_with",                         NamedThingAssociatedWithLikelihoodOfNamedThingAssociation),
        ("biolink:associated_with_increased_likelihood_of", NamedThingAssociatedWithLikelihoodOfNamedThingAssociation),
        ("biolink:associated_with_resistance_to",           DiseaseAssociatedWithResponseToChemicalEntityAssociation),
        ("biolink:associated_with_sensitivity_to",          DiseaseAssociatedWithResponseToChemicalEntityAssociation),
        ("biolink:biomarker_for",                           NamedThingAssociatedWithLikelihoodOfNamedThingAssociation),
        ("biolink:correlated_with",                         CorrelatedGeneToDiseaseAssociation),
        ("biolink:disease_has_location",                    DiseaseOrPhenotypicFeatureToLocationAssociation),
        ("biolink:expressed_in",                            GeneToExpressionSiteAssociation),
        ("biolink:gene_associated_with_condition",          CausalGeneToDiseaseAssociation),
        ("biolink:genetically_associated_with",             NamedThingAssociatedWithLikelihoodOfNamedThingAssociation),
        ("biolink:is_sequence_variant_of",                  CausalGeneToDiseaseAssociation),
        ("biolink:negatively_correlated_with",              CorrelatedGeneToDiseaseAssociation),
        ("biolink:participates_in",                         MacromolecularMachineToBiologicalProcessAssociation),
        ("biolink:positively_correlated_with",              CorrelatedGeneToDiseaseAssociation),
        ("biolink:regulates",                               GeneRegulatesGeneAssociation),
        ("biolink:related_to",                              Association),
    ],
)
def test_predicate_routes_to_typed_subclass(predicate, expected_cls):
    assert PREDICATE_TO_ASSOCIATION_CLASS[predicate] is expected_cls


# ---------------------------------------------------------------------------
# Qualifier routing
# ---------------------------------------------------------------------------


def test_qualifier_map_enumerates_all_fifteen_keys():
    expected = {
        "biolink:species_context_qualifier",
        "biolink:anatomical_context_qualifier",
        "biolink:disease_context_qualifier",
        "biolink:subject_aspect_qualifier",
        "biolink:object_aspect_qualifier",
        "biolink:subject_direction_qualifier",
        "biolink:object_direction_qualifier",
        "biolink:subject_context_qualifier",
        "biolink:object_context_qualifier",
        "biolink:subject_part_qualifier",
        "biolink:object_part_qualifier",
        "biolink:part_qualifier",
        "biolink:subject_form_or_variant_qualifier",
        "biolink:population_context_qualifier",
        "biolink:temporal_context_qualifier",
    }
    assert set(QUALIFIER_SOURCE_TO_SLOT) == expected
    assert len(QUALIFIER_SOURCE_TO_SLOT) == 15


@pytest.mark.parametrize(
    ("source_key", "expected_slot"),
    [
        ("biolink:species_context_qualifier",         "species_context_qualifier"),
        ("biolink:anatomical_context_qualifier",      "anatomical_context_qualifier"),
        ("biolink:disease_context_qualifier",         "disease_context_qualifier"),
        ("biolink:subject_aspect_qualifier",          "subject_aspect_qualifier"),
        ("biolink:object_aspect_qualifier",           "object_aspect_qualifier"),
        ("biolink:subject_direction_qualifier",       "subject_direction_qualifier"),
        ("biolink:object_direction_qualifier",        "object_direction_qualifier"),
        ("biolink:subject_context_qualifier",         "subject_context_qualifier"),
        ("biolink:object_context_qualifier",          "object_context_qualifier"),
        ("biolink:subject_part_qualifier",            "subject_part_qualifier"),
        ("biolink:object_part_qualifier",             "object_part_qualifier"),
        ("biolink:part_qualifier",                    "part_qualifier"),
        ("biolink:subject_form_or_variant_qualifier", "subject_form_or_variant_qualifier"),
        ("biolink:population_context_qualifier",      "population_context_qualifier"),
        ("biolink:temporal_context_qualifier",        "temporal_context_qualifier"),
    ],
)
def test_typed_qualifier_routes_to_correct_slot(source_key, expected_slot):
    """All 15 biolink-prefixed qualifier columns map to a known biolink slot."""
    assert QUALIFIER_SOURCE_TO_SLOT[source_key] == expected_slot


def test_capital_p_adjusted_p_value_routes_to_adjusted_p_value():
    """Multiple case/spacing variants of 'adjusted p value' all land in the
    same biolink slot, so 'Adjusted P Value' is not silently dropped."""
    from translator_ingest.ingests.mokg.mokg import _typed_numeric_overlay

    record = {"Adjusted P Value": "0.05"}
    out = _typed_numeric_overlay(record)
    assert "adjusted_p_value" in out
    assert out["adjusted_p_value"] == pytest.approx(0.05)


@pytest.mark.parametrize(
    ("source_column", "expected_slot"),
    [
        ("p value", "p_value"),
        ("P-value", "p_value"),
        ("pvalue", "p_value"),
        ("adjusted p value", "adjusted_p_value"),
        ("Adjusted P Value", "adjusted_p_value"),
        ("adj p value", "adjusted_p_value"),
        ("adj.P.Val", "adjusted_p_value"),
        ("padj", "adjusted_p_value"),
        ("relationship strength", "has_confidence_score"),
    ],
)
def test_typed_numeric_overlay_covers_all_case_variants(source_column, expected_slot):
    from translator_ingest.ingests.mokg.mokg import _typed_numeric_overlay

    record = {source_column: "0.42"}
    out = _typed_numeric_overlay(record)
    assert out[expected_slot] == pytest.approx(0.42)


# ---------------------------------------------------------------------------
# Numeric parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0.86227902", 0.86227902),
        ("0.019010875", 0.019010875),
        ("-3.5", -3.5),
        ("1e-9", 1e-9),
        (179, 179.0),
        ("Adjusted P-value", None),
        ("Liver: Lactate", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_optional_float(value, expected):
    assert parse_optional_float(value) == expected


# ---------------------------------------------------------------------------
# Transform behavior
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("significant", "expected_none"),
    [
        ("NO", True),
        ("YES", False),
        ("UNSURE", False),
    ],
)
def test_transform_filters_not_significant_edges(significant, expected_none):
    """The transform drops edges flagged significant='NO' and keeps YES/UNSURE."""
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:Gene"},
        {"id": "B:1", "name": "b", "category": "biolink:Protein"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:associated_with",
        "significant": significant,
    }
    result = transform(koza, record)
    if expected_none:
        assert result is None
    else:
        assert isinstance(result, KnowledgeGraph)


def test_transform_routes_sample_size_into_has_attribute():
    """`sample size` has no biolink slot; the transform preserves it on
    has_attribute as a `key=value` string."""
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:SmallMolecule"},
        {"id": "B:1", "name": "b", "category": "biolink:Disease"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:associated_with",
        "significant": "YES",
        "uuid": "abc",
        "sample size": 179,
    }
    result = transform(koza, record)
    assert isinstance(result, KnowledgeGraph)
    association = result.edges[0]
    assert "sample_size=179" in (association.has_attribute or [])


def test_transform_routes_multiple_testing_correction_method_into_has_attribute():
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:SmallMolecule"},
        {"id": "B:1", "name": "b", "category": "biolink:Disease"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:associated_with",
        "significant": "YES",
        "uuid": "abc",
        "multiple testing correction method": "Benjamini Hochberg",
    }
    result = transform(koza, record)
    association = result.edges[0]
    assert "multiple_testing_correction_method=Benjamini Hochberg" in (association.has_attribute or [])


def test_transform_attaches_edge_url_to_sources():
    from types import SimpleNamespace

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:SmallMolecule"},
        {"id": "B:1", "name": "b", "category": "biolink:Disease"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:associated_with",
        "significant": "YES",
        "uuid": "abc",
        "url": "https://example.org/record/1",
    }
    result = transform(koza, record)
    sources = result.edges[0].sources
    assert isinstance(sources, list) and len(sources) == 1
    assert sources[0].source_record_urls == ["https://example.org/record/1"]


def test_transform_routes_disease_context_to_typed_slot_when_supported():
    """For subclasses with `disease_context_qualifier` (e.g. CorrelatedGeneToDisease),
    the value lands in the typed slot, not the generic qualifiers list."""
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:SmallMolecule"},
        {"id": "B:1", "name": "b", "category": "biolink:Disease"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:correlated_with",
        "significant": "YES",
        "uuid": "abc",
        "biolink:disease_context_qualifier": "MONDO:0005575",
    }
    result = transform(koza, record)
    assert isinstance(result, KnowledgeGraph)
    association = result.edges[0]
    assert association.disease_context_qualifier == "MONDO:0005575"
    assert (association.qualifiers or []) == []


def test_transform_falls_back_qualifier_to_generic_list_when_subclass_lacks_slot():
    """For subclasses WITHOUT a typed `temporal_context_qualifier` slot (e.g.
    Association), the value lands in the generic `qualifiers` list rather
    than being silently dropped."""
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:SmallMolecule"},
        {"id": "B:1", "name": "b", "category": "biolink:Disease"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:related_to",
        "significant": "YES",
        "uuid": "abc",
        "biolink:temporal_context_qualifier": "UBERON:0000118",
    }
    result = transform(koza, record)
    assert isinstance(result, KnowledgeGraph)
    association = result.edges[0]
    assert (association.qualifiers or []) == ["UBERON:0000118"]


# ---------------------------------------------------------------------------
# Coverage invariants
# ---------------------------------------------------------------------------


def test_has_attribute_columns_covers_user_feedback_fields():
    """The columns the user explicitly called out (`sample size` and
    `multiple testing correction method`) must appear in HAS_ATTRIBUTE_COLUMNS."""
    assert "sample size" in HAS_ATTRIBUTE_COLUMNS
    assert "multiple testing correction method" in HAS_ATTRIBUTE_COLUMNS


def test_typed_numeric_columns_covers_capital_p_adjusted_p_value():
    """The user flagged `Adjusted P Value` (capital P) - it must route to
    adjusted_p_value through TYPED_NUMERIC_COLUMN_TO_SLOT."""
    assert TYPED_NUMERIC_COLUMN_TO_SLOT.get("Adjusted P Value") == "adjusted_p_value"


def test_supporting_text_columns_covers_known_study_metrics():
    """At least the canonical study metrics should appear in SUPPORTING_TEXT_COLUMNS."""
    for column in ("odds ratio", "hazard ratio", "or", "beta", "fdr"):
        assert column in SUPPORTING_TEXT_COLUMNS, column
