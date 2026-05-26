import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsBiologicalEntityAssociation,
    DirectionQualifierEnum,
    GeneAffectsChemicalAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    GeneRegulatesGeneAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)

from translator_ingest.ingests.pathbank.interaction_mapping import map_interaction_edge


@pytest.mark.parametrize(
    "interaction_type,left_type,right_type,expected_predicate,expected_class,expected_direction",
    [
        (
            "Activation",
            "Protein",
            "Protein",
            "biolink:regulates",
            GeneRegulatesGeneAssociation,
            DirectionQualifierEnum.upregulated,
        ),
        (
            "Inhibition",
            "Compound",
            "Protein",
            "biolink:affects",
            ChemicalAffectsBiologicalEntityAssociation,
            DirectionQualifierEnum.downregulated,
        ),
        (
            "Activation",
            "Protein",
            "Compound",
            "biolink:regulates",
            GeneAffectsChemicalAssociation,
            DirectionQualifierEnum.upregulated,
        ),
    ],
)
def test_interaction_causal_mapping(
    interaction_type: str,
    left_type: str,
    right_type: str,
    expected_predicate: str,
    expected_class: type[Association],
    expected_direction: DirectionQualifierEnum,
) -> None:
    interaction_mapping = map_interaction_edge(
        interaction_type=interaction_type,
        left_type=left_type,
        right_type=right_type,
    )

    assert interaction_mapping.association_class is expected_class
    assert interaction_mapping.predicate == expected_predicate
    assert interaction_mapping.qualified_predicate == "biolink:causes"
    assert (
        interaction_mapping.object_aspect_qualifier
        == GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
    )
    assert interaction_mapping.object_direction_qualifier == expected_direction


@pytest.mark.parametrize(
    "interaction_type,expected_predicate",
    [
        ("Physical association", "biolink:physically_interacts_with"),
        ("Unknown interaction type", "biolink:interacts_with"),
    ],
)
def test_interaction_non_causal_mapping(
    interaction_type: str,
    expected_predicate: str,
) -> None:
    interaction_mapping = map_interaction_edge(
        interaction_type=interaction_type,
        left_type="Protein",
        right_type="Protein",
    )

    assert interaction_mapping.association_class is Association
    assert interaction_mapping.predicate == expected_predicate
    assert interaction_mapping.qualified_predicate is None


# ── Pydantic round-trip fixtures & test ──────────────────────────────

_PATHBANK_SOURCES = [
    RetrievalSource(
        id="infores:pathbank",
        resource_id="infores:pathbank",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": Association,
        "params": {
            "id": "uuid:pathbank-test-1",
            "subject": "SMPDB:SMP0000001",
            "predicate": "biolink:has_participant",
            "object": "CHEBI:62370",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
    {
        "association_class": GeneRegulatesGeneAssociation,
        "params": {
            "id": "uuid:pathbank-test-2",
            "subject": "UniProtKB:P12345",
            "predicate": "biolink:regulates",
            "object": "UniProtKB:Q67890",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance,
            "object_direction_qualifier": DirectionQualifierEnum.upregulated,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
    {
        "association_class": ChemicalAffectsBiologicalEntityAssociation,
        "params": {
            "id": "uuid:pathbank-test-3",
            "subject": "CHEBI:15378",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P11387",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance,
            "object_direction_qualifier": DirectionQualifierEnum.downregulated,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
    {
        "association_class": GeneAffectsChemicalAssociation,
        "params": {
            "id": "uuid:pathbank-test-4",
            "subject": "UniProtKB:P12345",
            "predicate": "biolink:regulates",
            "object": "CHEBI:62370",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance,
            "object_direction_qualifier": DirectionQualifierEnum.upregulated,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f["association_class"].__name__,
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
