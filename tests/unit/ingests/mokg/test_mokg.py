import json

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
    KnowledgeLevelEnum,
    MacromolecularMachineToBiologicalProcessAssociation,
    NamedThing,
    NamedThingAssociatedWithLikelihoodOfNamedThingAssociation,
    Protein,
    Publication,
    SmallMolecule,
)

from translator_ingest.ingests.mokg.mokg import (
    HAS_ATTRIBUTE_COLUMNS,
    MOKG_SOURCES,
    PREDICATE_TO_ASSOCIATION_CLASS,
    QUALIFIER_SOURCE_TO_SLOT,
    SUPPORTING_TEXT_COLUMNS,
    create_node,
    normalize_category,
    transform,
)
from translator_ingest.util.type_coercion import (
    EFFECT_TYPE_VALUES,
    coerce_record_types,
    custom_association_class,
    is_neglog10_column,
    map_effect_type_value,
    parse_optional_float,
    significance_qualifier,
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
    record = {"Adjusted P Value": "0.05"}
    slots, claimed = coerce_record_types(record)
    assert "adjusted_p_value" in slots
    assert slots["adjusted_p_value"] == pytest.approx(0.05)
    assert "Adjusted P Value" in claimed


@pytest.mark.parametrize(
    ("source_column", "expected_slot"),
    [
        ("p value", "p_value"),
        ("P-value", "p_value"),
        ("pvalue", "p_value"),
        ("P.Value", "p_value"),
        ("adjusted p value", "adjusted_p_value"),
        ("Adjusted P Value", "adjusted_p_value"),
        ("adj p value", "adjusted_p_value"),
        ("adj.P.Val", "adjusted_p_value"),
        ("padj", "adjusted_p_value"),
        ("q value", "adjusted_p_value"),
        ("false discovery rate", "adjusted_p_value"),
        ("FDR", "adjusted_p_value"),
        ("relationship strength", "effect_size"),
        ("effect size", "effect_size"),
        ("odds ratio", "effect_size"),
        ("correlation coefficient", "effect_size"),
    ],
)
def test_coercion_covers_all_column_spellings(source_column, expected_slot):
    slots, _claimed = coerce_record_types({source_column: "0.42"})
    assert slots[expected_slot] == pytest.approx(0.42)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1.234e-08", 1.234e-08),
        ("5E-3", 0.005),
        ("1.0000e-03", 1.0e-03),
        ("0.86227902", 0.86227902),
        (179, 179.0),
    ],
)
def test_scientific_notation_strings_coerce_to_float(value, expected):
    """Tablassert emits p-values as controlled scientific-notation strings;
    the coercion layer must accept them exactly like plain decimals."""
    assert parse_optional_float(value) == pytest.approx(expected)
    slots, _ = coerce_record_types({"p value": value})
    assert slots["p_value"] == pytest.approx(expected)


def test_non_numeric_artifacts_do_not_claim_a_slot():
    """A leaked header or tissue label parses to None and leaves the column
    unclaimed, so it can still surface in the untyped overlays."""
    slots, claimed = coerce_record_types({"p value": "Liver: Lactate", "adjusted p value": "Adjusted P-value"})
    assert slots == {}
    assert claimed == frozenset()


def test_effect_size_pairs_with_effect_type_from_assertion_method():
    """The custom effect_size/effect_type slots (biolink PR #1774) ride on the
    association even though biolink-model 4.4.2 does not declare them."""
    record = {
        "relationship strength": "0.019010875",
        "assertion method": "Spearman Correlation",
    }
    slots, claimed = coerce_record_types(record)
    assert slots["effect_size"] == pytest.approx(0.019010875)
    assert slots["effect_type"] == "spearmans_rho"
    assert "relationship strength" in claimed


def test_effect_type_never_travels_without_effect_size():
    """Biolink class rule: effect_type may only be populated when effect_size
    is populated."""
    slots, _ = coerce_record_types({"assertion method": "Spearman Correlation"})
    assert "effect_type" not in slots


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Spearman Correlation", "spearmans_rho"),
        ("Pearson correlation", "pearsons_r"),
        ("OR", "odds_ratio"),
        ("log2FC", "log2_fold_change"),
        ("Mann-Whitney U test", None),
        ("ANOVA", None),
        (None, None),
        ("", None),
    ],
)
def test_map_effect_type_value(raw, expected):
    assert map_effect_type_value(raw) == expected


def test_effect_type_values_match_biolink_pr_1774():
    """The local EffectTypes definition mirrors the custom biolink-model
    additions (PR #1774) until the pinned model declares them natively."""
    assert len(EFFECT_TYPE_VALUES) == 25
    assert "spearmans_rho" in EFFECT_TYPE_VALUES
    assert "root_mean_square_standardized_effect" in EFFECT_TYPE_VALUES


@pytest.mark.parametrize(
    ("p_value", "expected"),
    [
        (0.0005, "very_strongly_significant"),
        (0.001, "very_strongly_significant"),
        (0.005, "strongly_significant"),
        (0.01, "strongly_significant"),
        (0.03, "significant"),
        (0.05, "significant"),
        (0.08, "suggestive"),
        (0.10, "suggestive"),
        (0.5, "not_significant"),
        (None, None),
    ],
)
def test_significance_qualifier_bands(p_value, expected):
    assert significance_qualifier(p_value) == expected


def test_significance_qualifier_prefers_raw_p_value():
    slots, _ = coerce_record_types({"p value": "0.02", "adjusted p value": "0.9"})
    assert slots["statistical_significance_qualifier"] == "significant"


def test_significance_qualifier_absent_without_numeric_p_value():
    """Class rule (PR #1766): the qualifier is only set when a numeric
    significance slot is populated."""
    slots, _ = coerce_record_types({"relationship strength": "0.5"})
    assert "statistical_significance_qualifier" not in slots


def test_custom_association_class_extends_only_missing_slots():
    """The override self-retires: a base class already declaring a custom slot
    does not get a redundant redeclaration."""
    from biolink_model.datamodel.pydanticmodel_v2 import Association

    extended = custom_association_class(Association)
    assert "effect_size" in extended.model_fields
    assert "effect_type" in extended.model_fields
    assert "statistical_significance_qualifier" in extended.model_fields
    # Already-extended classes are idempotent.
    assert custom_association_class(extended) is extended


def test_validation_error_fallback_retains_custom_slots():
    """When the typed subclass rejects an edge (e.g. `biolink:regulates` maps
    to GeneRegulatesGeneAssociation, which rejects the record), the generic
    Association fallback must still carry the custom override slots."""
    from biolink_model.datamodel.pydanticmodel_v2 import Association

    from translator_ingest.ingests.mokg.mokg import _instantiate_association

    association = _instantiate_association(
        "biolink:regulates",
        {
            "id": "edge-1",
            "subject": "A:1",
            "predicate": "biolink:regulates",
            "object": "B:1",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": MOKG_SOURCES,
            "effect_size": 0.5,
            "effect_type": "spearmans_rho",
            "statistical_significance_qualifier": "significant",
        },
    )
    assert type(association).__name__ == "AssociationWithCustomSlots"
    assert isinstance(association, Association)
    assert association.effect_size == pytest.approx(0.5)
    assert association.effect_type == "spearmans_rho"
    assert association.statistical_significance_qualifier == "significant"


def test_study_size_columns_are_not_claimed_for_the_edge():
    """study_size lives on the inlined Study node in the current Biolink Model,
    not on Association, so study-size-like columns stay available to the
    untyped has_attribute overlay."""
    slots, claimed = coerce_record_types({"sample size": 179})
    assert slots == {}
    assert claimed == frozenset()


@pytest.mark.parametrize(
    ("column", "is_neglog"),
    [
        ("negative log10 p value", True),
        ("-log10(p)", True),
        ("neg log10 q value", True),
        ("p value", False),
        ("log10 p value", False),  # no negation marker -> ambiguous, not un-logged
    ],
)
def test_neglog10_column_detection(column, is_neglog):
    assert is_neglog10_column(column) is is_neglog


def test_negative_log10_p_value_column_is_unlogged():
    """-log10(p)=8 means p=1e-8: the slot must hold the recovered p-value, and
    the significance band must be very strongly significant (not inverted)."""
    slots, claimed = coerce_record_types({"negative log10 p value": "8.0"})
    assert slots["p_value"] == pytest.approx(1e-8)
    assert slots["statistical_significance_qualifier"] == "very_strongly_significant"
    assert "negative log10 p value" in claimed


def test_negative_log10_zero_maps_to_p_one():
    slots, _ = coerce_record_types({"negative log10 p value": "0"})
    assert slots["p_value"] == pytest.approx(1.0)
    assert slots["statistical_significance_qualifier"] == "not_significant"


def test_negative_log10_underflows_to_zero_without_error():
    """Observed max in the 3.0.0 release is ~864; 10**-864 underflows float64
    to 0.0, which is indistinguishable from p ~ 0 and must not raise."""
    slots, _ = coerce_record_types({"negative log10 p value": "864.066614351"})
    assert slots["p_value"] == 0.0
    assert slots["statistical_significance_qualifier"] == "very_strongly_significant"


def test_raw_p_value_column_beats_neglog10_alias():
    """When both a raw and a negative-log10 p-value column are present, the
    raw column wins and its value is carried untransformed."""
    slots, claimed = coerce_record_types({"p value": "0.03", "negative log10 p value": "8.0"})
    assert slots["p_value"] == pytest.approx(0.03)
    assert "negative log10 p value" not in claimed


def test_unparseable_candidate_does_not_shadow_parseable_alias():
    """A junk value in one spelling (leaked header, tissue label) must not
    block a parseable alias spelling of the same statistic."""
    slots, claimed = coerce_record_types({"p value": "Liver: Lactate", "P-value": "0.03"})
    assert slots["p_value"] == pytest.approx(0.03)
    assert "P-value" in claimed
    assert "p value" not in claimed


def test_unhashable_assertion_method_does_not_crash():
    """A leaked list/dict in the assertion-method column must not raise
    TypeError from the effect-type cache key."""
    slots, _ = coerce_record_types(
        {"relationship strength": "0.5", "assertion method": ["Spearman", "Correlation"]}
    )
    assert slots["effect_size"] == pytest.approx(0.5)
    # str() of the list still resolves the embedded alias.
    assert slots["effect_type"] == "spearmans_rho"


def test_effect_type_falls_back_to_assertion_method_when_metric_column_unmatched():
    """An effect-type column whose value is outside the enum (e.g. 'ANOVA')
    does not block the assertion-method fallback."""
    slots, claimed = coerce_record_types(
        {
            "relationship strength": "0.5",
            "effect type": "ANOVA",
            "assertion method": "Spearman Correlation",
        }
    )
    assert slots["effect_type"] == "spearmans_rho"
    assert "effect type" not in claimed


def test_unmatched_effect_type_column_is_not_claimed():
    slots, claimed = coerce_record_types({"effect type": "ANOVA"})
    assert slots == {}
    assert claimed == frozenset()


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
    adjusted_p_value through the coercion layer."""
    slots, _ = coerce_record_types({"Adjusted P Value": "0.05"})
    assert slots["adjusted_p_value"] == pytest.approx(0.05)


def test_supporting_text_columns_covers_known_study_metrics():
    """At least the canonical study metrics should appear in SUPPORTING_TEXT_COLUMNS."""
    for column in ("odds ratio", "hazard ratio", "or", "beta", "fdr"):
        assert column in SUPPORTING_TEXT_COLUMNS, column


# ---------------------------------------------------------------------------
# Typed coercion on the output association
# ---------------------------------------------------------------------------


def test_transform_emits_typed_custom_slots_on_the_edge():
    """End-to-end: string statistics from the NDJSON land on the association
    as typed values - floats for the numeric slots, enum tokens for
    effect_type and the significance qualifier - including the custom slots
    the pinned biolink-model does not declare."""
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "CHEBI:30772", "name": "Glycerol", "category": "biolink:SmallMolecule"},
        {"id": "NCBITaxon:33033", "name": "Parvimonas micra", "category": "biolink:OrganismTaxon"},
    ]
    koza = SimpleNamespace(
        state={"nodes_lookup": {n["id"]: n for n in nodes}},
        input_files_dir=".",
    )
    record = {
        "subject": "CHEBI:30772",
        "object": "NCBITaxon:33033",
        "predicate": "biolink:positively_correlated_with",
        "significant": "UNSURE",
        "uuid": "05017423-f0c6-3c34-9190-c1daf01915f0",
        "sample size": 179,
        "multiple testing correction method": "Benjamini Hochberg",
        "adjusted p value": "0.86227902",
        "relationship strength": "0.019010875",
        "assertion method": "Spearman Correlation",
        "odds ratio": "2.5",
    }
    result = transform(koza, record)
    assert isinstance(result, KnowledgeGraph)
    association = result.edges[0]

    assert association.adjusted_p_value == pytest.approx(0.86227902)
    assert association.effect_size == pytest.approx(0.019010875)
    assert association.effect_type == "spearmans_rho"
    assert association.statistical_significance_qualifier == "not_significant"

    serialized = json.loads(association.model_dump_json(exclude_none=True))
    assert isinstance(serialized["adjusted_p_value"], float)
    assert isinstance(serialized["effect_size"], float)
    assert serialized["effect_type"] == "spearmans_rho"
    # sample size keeps its has_attribute string (study_size has no
    # Association home in the pinned model).
    assert "sample_size=179" in (association.has_attribute or [])


def test_transform_does_not_duplicate_claimed_columns_in_supporting_text():
    """Columns consumed by the typed coercion layer must not reappear in the
    supporting_text payload."""
    from types import SimpleNamespace

    from koza.model.graphs import KnowledgeGraph

    nodes = [
        {"id": "A:1", "name": "a", "category": "biolink:Gene"},
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
        "fdr": "0.9",
        "or": "2.5",
        "h4 h3h4": "0.42",
    }
    result = transform(koza, record)
    assert isinstance(result, KnowledgeGraph)
    association = result.edges[0]

    # 'fdr' and 'or' are claimed by the typed slots (adjusted_p_value /
    # effect_size) so they must not repeat inside supporting_text; 'h4 h3h4'
    # has no typed slot and stays.
    supporting = "\n".join(association.supporting_text or [])
    assert '"fdr"' not in supporting
    assert '"or"' not in supporting
    assert '"h4 h3h4"' in supporting
