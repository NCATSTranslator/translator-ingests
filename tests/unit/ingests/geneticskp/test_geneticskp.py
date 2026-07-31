import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
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

# ── Fixtures: one per predicate from merged edges output.
# All edges are generic Associations with agent_type data_analysis_pipeline.
EDGE_FIXTURES = [
    {
        "association_class": Association,
        "params": {
            "id": "magma_NCBIGene:8726_MONDO:0005300NCBIGene:8726MONDO:0005300",
            "subject": "NCBIGene:8726",
            "predicate": "biolink:gene_associated_with_condition",
            "object": "MONDO:0005300",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.data_analysis_pipeline,
            "sources": GENETICSKP_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "magma_EFO:0004980_NCBIGene:152485EFO:0004980NCBIGene:152485",
            "subject": "EFO:0004980",
            "predicate": "biolink:condition_associated_with_gene",
            "object": "NCBIGene:152485",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.data_analysis_pipeline,
            "sources": GENETICSKP_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "magma_GO:0035878_EFO:0004995GO:0035878EFO:0004995",
            "subject": "GO:0035878",
            "predicate": "biolink:genetic_association",
            "object": "EFO:0004995",
            "knowledge_level": KnowledgeLevelEnum.statistical_association,
            "agent_type": AgentTypeEnum.data_analysis_pipeline,
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
