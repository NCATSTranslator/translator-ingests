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
            "id": "8557038b-fa94-47b6-a39d-bba1c5f50d6a",
            "subject": "SMPDB:SMP0037185",
            "predicate": "biolink:has_participant",
            "object": "CHEBI:15533",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "6a4a9e3a-269d-49bb-bfaf-663eb825ed9c",
            "subject": "SMPDB:SMP0058319",
            "predicate": "biolink:occurs_in",
            "object": "GO:0043202",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
    {
        "association_class": ChemicalAffectsBiologicalEntityAssociation,
        "params": {
            "id": "42685296-b480-5dcd-8a46-09b9aab11eee",
            "subject": "CHEBI:17234",
            "predicate": "biolink:affects",
            "object": "CHEBI:17489",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _PATHBANK_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f"{f['association_class'].__name__}_{f['params']['predicate'].split(':')[-1]}",
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
