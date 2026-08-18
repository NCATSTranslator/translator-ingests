import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalAffectsGeneAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    ChemicalGeneInteractionAssociation,
    GeneAffectsChemicalAssociation,
    GeneRegulatesGeneAssociation,
    PairwiseGeneToGeneInteraction,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


SIGNOR_SOURCES = [
    RetrievalSource(
        id="infores:signor",
        resource_id="infores:signor",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# -- Fixtures: one per distinct (category, predicate, agent_type, knowledge_level) tuple --
EDGE_FIXTURES = [
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "6d9d6e8c-b9b0-47a6-829f-988ac073bbeb",
            "subject": "CHEBI:47519",
            "predicate": "biolink:affects",
            "object": "NCBIGene:54658",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": ChemicalEntityToChemicalEntityAssociation,
        "params": {
            "id": "5f837816-b0ce-4c06-89b0-0abeb47d1ddd",
            "subject": "CHEBI:17368",
            "predicate": "biolink:affects",
            "object": "CHEBI:28997",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": ChemicalEntityToChemicalEntityAssociation,
        "params": {
            "id": "002ee7e5-d4e1-500a-9bb3-ea92e3e1ff7d",
            "subject": "CHEBI:17172",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "CHEBI:57673",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": ChemicalGeneInteractionAssociation,
        "params": {
            "id": "764b64a1-da2c-4d1f-a8a4-8550514e0cb0",
            "subject": "CHEBI:9453",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:3269",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": GeneAffectsChemicalAssociation,
        "params": {
            "id": "3c0bb9cb-83a8-4453-9aa0-ede7bd123c19",
            "subject": "NCBIGene:123041",
            "predicate": "biolink:affects",
            "object": "CHEBI:29101",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": GeneRegulatesGeneAssociation,
        "params": {
            "id": "d30c7d77-b166-492b-84d0-bdf94af9f523",
            "subject": "NCBIGene:6272",
            "predicate": "biolink:regulates",
            "object": "NCBIGene:348",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.abundance,
            "object_direction_qualifier": DirectionQualifierEnum.upregulated,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": PairwiseGeneToGeneInteraction,
        "params": {
            "id": "8dc21c7d-aab7-419f-a678-7ba1b28d0597",
            "subject": "NCBIGene:136",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:2775",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
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
