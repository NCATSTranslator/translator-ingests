import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    EntityToDiseaseAssociation,
    EntityToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


CTKP_SOURCES = [
    RetrievalSource(
        id="infores:multiomics-clinicaltrials",
        resource_id="infores:multiomics-clinicaltrials",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
        upstream_resource_ids=["infores:aact"],
    ),
    RetrievalSource(
        id="infores:aact",
        resource_id="infores:aact",
        resource_role=ResourceRoleEnum.supporting_data_source,
        upstream_resource_ids=["infores:clinicaltrials"],
    ),
    RetrievalSource(
        id="infores:clinicaltrials",
        resource_id="infores:clinicaltrials",
        resource_role=ResourceRoleEnum.supporting_data_source,
    ),
]

# -- Fixtures: one per (category, predicate, agent_type, knowledge_level) tuple
#    found in merged_edges.jsonl
EDGE_FIXTURES = [
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "urn:uuid:f0dd8d82-6aa5-35ad-9a49-6d656347d45b",
            "subject": "PUBCHEM.COMPOUND:164532637",
            "predicate": "biolink:in_clinical_trials_for",
            "object": "UMLS:C0860197",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CTKP_SOURCES,
        },
    },
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "85f0ee1f-6779-3db5-a4bf-7f506c7fb5b2",
            "subject": "UNII:0K5743G68X",
            "predicate": "biolink:treats",
            "object": "MONDO:0010936",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CTKP_SOURCES,
        },
    },
    {
        "association_class": EntityToPhenotypicFeatureAssociation,
        "params": {
            "id": "urn:uuid:38783536-f66a-3ac2-8252-dd2ec3fc972e",
            "subject": "DRUGBANK:DB19139",
            "predicate": "biolink:in_clinical_trials_for",
            "object": "NCIT:C146780",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CTKP_SOURCES,
        },
    },
    {
        "association_class": EntityToPhenotypicFeatureAssociation,
        "params": {
            "id": "aa5e2f3e-981a-3b30-b413-309c9ebdf092",
            "subject": "CHEBI:5118",
            "predicate": "biolink:treats",
            "object": "UMLS:C5977286",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CTKP_SOURCES,
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
