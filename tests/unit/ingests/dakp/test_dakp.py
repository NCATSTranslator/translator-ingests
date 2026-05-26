import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    EntityToDiseaseAssociation,
    EntityToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


DAKP_SOURCES = [
    RetrievalSource(
        id="infores:multiomics-drugapprovals",
        resource_id="infores:multiomics-drugapprovals",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in dakp_rig.yaml / dakp.py ────────
EDGE_FIXTURES = [
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "uuid:dakp-test-entity-disease",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:treats",
            "object": "MONDO:0005148",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": DAKP_SOURCES,
        },
    },
    {
        "association_class": EntityToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:dakp-test-entity-phenotype",
            "subject": "PUBCHEM.COMPOUND:5311",
            "predicate": "biolink:treats",
            "object": "HP:0001945",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": DAKP_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:dakp-test-generic",
            "subject": "PUBCHEM.COMPOUND:3672",
            "predicate": "biolink:related_to",
            "object": "MONDO:0005015",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": DAKP_SOURCES,
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
