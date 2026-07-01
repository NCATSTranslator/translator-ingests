import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    Disease,
    Gene,
    GeographicLocation,
    KnowledgeLevelEnum,
    NamedThing,
    Protein,
    SmallMolecule,
)

from translator_ingest.ingests.mokg.mokg import (
    MOKG_SOURCES,
    create_node,
    normalize_category,
    parse_optional_float,
)


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


def test_normalize_category_handles_none_list_and_scalar():
    assert normalize_category(None) == ["biolink:NamedThing"]
    assert normalize_category("biolink:Gene") == ["biolink:Gene"]
    assert normalize_category(["biolink:Gene", "biolink:NamedThing"]) == [
        "biolink:Gene",
        "biolink:NamedThing",
    ]


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


def test_significant_and_sample_size_have_no_slot():
    """`significant` and `sample size` have no usable slot on the generic Association:
    `supporting_study_size` is not applied to any association class and `number_of_cases`
    is subclass-only, so both are rejected as extra_forbidden. This is why the transform
    drops them rather than misusing the ontology-class-typed `qualifiers` list."""
    from pydantic import ValidationError

    common = {
        "id": "abc",
        "subject": "CHEBI:1",
        "predicate": "biolink:associated_with",
        "object": "NCBITaxon:1",
        "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
        "agent_type": AgentTypeEnum.manual_agent,
        "sources": MOKG_SOURCES,
    }
    with pytest.raises(ValidationError):
        Association(**common, significant="YES")
    with pytest.raises(ValidationError):
        Association(**common, sample_size=179)


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
