import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


BGEE_SOURCES = [
    RetrievalSource(
        id="infores:bgee",
        resource_id="infores:bgee",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in bgee_rig.yaml / bgee.py ────────
EDGE_FIXTURES = [
    {
        "association_class": Association,
        "params": {
            "id": "uuid:bgee-test-expressed-in",
            "subject": "Ensembl:ENSG00000139618",
            "predicate": "biolink:expressed_in",
            "object": "UBERON:0000955",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": BGEE_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:bgee-test-expressed-in-cell",
            "subject": "Ensembl:ENSG00000141510",
            "predicate": "biolink:expressed_in",
            "object": "CL:0000540",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": BGEE_SOURCES,
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
