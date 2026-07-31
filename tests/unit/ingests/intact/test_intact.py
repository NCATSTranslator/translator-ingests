import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    PairwiseMolecularInteraction,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


INTACT_SOURCES = [
    RetrievalSource(
        id="infores:intact",
        resource_id="infores:intact",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in intact_rig.yaml / intact.py ────
EDGE_FIXTURES = [
    {
        "association_class": PairwiseMolecularInteraction,
        "params": {
            "id": "uuid:intact-ppi",
            "subject": "UniProtKB:P04637",
            "predicate": "biolink:physically_interacts_with",
            "object": "UniProtKB:Q00987",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": INTACT_SOURCES,
            "publications": ["PMID:9029145"],
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
