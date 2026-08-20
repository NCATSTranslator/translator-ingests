import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsGeneAssociation,
    PairwiseMolecularInteraction,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    CausalMechanismQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


GTOPDB_SOURCES = [
    RetrievalSource(
        id="infores:gtopdb",
        resource_id="infores:gtopdb",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in gtopdb_rig.yaml / gtopdb.py ────
EDGE_FIXTURES = [
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "uuid:gtopdb-chem-affects-gene",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P08588",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.agonism,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
            "publications": ["PMID:12345678"],
        },
    },
    {
        "association_class": PairwiseMolecularInteraction,
        "params": {
            "id": "uuid:gtopdb-pairwise-interaction",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "UniProtKB:P08588",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:gtopdb-generic-related",
            "subject": "PUBCHEM.COMPOUND:5311",
            "predicate": "biolink:related_to",
            "object": "UniProtKB:Q14416",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
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
