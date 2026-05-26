import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    GeneToDiseaseAssociation,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


GENETICSKP_SOURCES = [
    RetrievalSource(
        id="infores:geneticskp",
        resource_id="infores:geneticskp",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in geneticskp_rig.yaml / geneticskp.py
EDGE_FIXTURES = [
    {
        "association_class": GeneToDiseaseAssociation,
        "params": {
            "id": "uuid:geneticskp-gene-disease",
            "subject": "NCBIGene:672",
            "predicate": "biolink:associated_with",
            "object": "MONDO:0007254",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.computational_model,
            "sources": GENETICSKP_SOURCES,
        },
    },
    {
        "association_class": GeneToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:geneticskp-gene-phenotype",
            "subject": "NCBIGene:672",
            "predicate": "biolink:has_phenotype",
            "object": "HP:0000729",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.computational_model,
            "sources": GENETICSKP_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:geneticskp-generic",
            "subject": "NCBIGene:672",
            "predicate": "biolink:genetically_associated_with",
            "object": "GO:0006915",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.computational_model,
            "sources": GENETICSKP_SOURCES,
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
