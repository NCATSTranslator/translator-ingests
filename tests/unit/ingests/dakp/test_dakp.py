import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    EntityToDiseaseAssociation,
    EntityToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


# -- Sources helpers built from real merged-edge data --
_SOURCES_PRIMARY = [
    RetrievalSource(
        id="infores:multiomics-drugapprovals",
        resource_id="infores:multiomics-drugapprovals",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
        upstream_resource_ids=["infores:dailymed", "infores:faers"],
        source_record_urls=[
            "https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id=1f540160-47b7-3bec-8269-d0c817e400f0"
        ],
    ),
    RetrievalSource(
        id="infores:faers",
        resource_id="infores:faers",
        resource_role=ResourceRoleEnum.supporting_data_source,
    ),
    RetrievalSource(
        id="infores:dailymed",
        resource_id="infores:dailymed",
        resource_role=ResourceRoleEnum.supporting_data_source,
    ),
]

_SOURCES_AGGREGATOR = [
    RetrievalSource(
        id="infores:multiomics-drugapprovals",
        resource_id="infores:multiomics-drugapprovals",
        resource_role=ResourceRoleEnum.aggregator_knowledge_source,
        upstream_resource_ids=["infores:dailymed", "infores:faers"],
        source_record_urls=[
            "https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id=6859e49d-c934-3823-8a79-4c10bf4bb8b8"
        ],
    ),
    RetrievalSource(
        id="infores:faers",
        resource_id="infores:faers",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    ),
    RetrievalSource(
        id="infores:dailymed",
        resource_id="infores:dailymed",
        resource_role=ResourceRoleEnum.supporting_data_source,
    ),
]

_SOURCES_CONTRAINDICATED_AGG = [
    RetrievalSource(
        id="infores:multiomics-drugapprovals",
        resource_id="infores:multiomics-drugapprovals",
        resource_role=ResourceRoleEnum.aggregator_knowledge_source,
        upstream_resource_ids=["infores:dailymed", "infores:medi"],
        source_record_urls=[
            "https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id=065237e0-d8b4-3a4b-80c1-0aa58f4d8253"
        ],
    ),
    RetrievalSource(
        id="infores:medi",
        resource_id="infores:medi",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
        upstream_resource_ids=["infores:dailymed"],
    ),
    RetrievalSource(
        id="infores:dailymed",
        resource_id="infores:dailymed",
        resource_role=ResourceRoleEnum.supporting_data_source,
    ),
]


# -- Fixtures: one per distinct (category, predicate, agent_type, knowledge_level)
#    tuple found in the merged_edges.jsonl --
EDGE_FIXTURES = [
    # 1) EntityToPhenotypicFeatureAssociation / applied_to_treat / observation
    {
        "association_class": EntityToPhenotypicFeatureAssociation,
        "params": {
            "id": "6859e49d-c934-3823-8a79-4c10bf4bb8b8",
            "subject": "CHEBI:5551",
            "predicate": "biolink:applied_to_treat",
            "object": "HP:0002783",
            "knowledge_level": KnowledgeLevelEnum.observation,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_AGGREGATOR,
        },
    },
    # 2) EntityToDiseaseAssociation / applied_to_treat / observation
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "2fb678bf-eb9b-322f-aa09-5e78fd1e8716",
            "subject": "CHEBI:31236",
            "predicate": "biolink:applied_to_treat",
            "object": "MONDO:0011918",
            "knowledge_level": KnowledgeLevelEnum.observation,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_AGGREGATOR,
        },
    },
    # 3) EntityToDiseaseAssociation / treats / knowledge_assertion
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "1f540160-47b7-3bec-8269-d0c817e400f0",
            "subject": "CHEBI:27786",
            "predicate": "biolink:treats",
            "object": "MONDO:0020696",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_PRIMARY,
        },
    },
    # 4) EntityToPhenotypicFeatureAssociation / contraindicated_in / knowledge_assertion
    {
        "association_class": EntityToPhenotypicFeatureAssociation,
        "params": {
            "id": "065237e0-d8b4-3a4b-80c1-0aa58f4d8253",
            "subject": "CHEBI:135935",
            "predicate": "biolink:contraindicated_in",
            "object": "HP:0004796",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_CONTRAINDICATED_AGG,
        },
    },
    # 5) EntityToDiseaseAssociation / contraindicated_in / knowledge_assertion
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "f2b69c3a-6337-33e3-9dc0-7995f3bee4a2",
            "subject": "CHEBI:6904",
            "predicate": "biolink:contraindicated_in",
            "object": "MONDO:0005068",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_CONTRAINDICATED_AGG,
        },
    },
    # 6) EntityToPhenotypicFeatureAssociation / treats / knowledge_assertion
    {
        "association_class": EntityToPhenotypicFeatureAssociation,
        "params": {
            "id": "00365965-0f56-3d3c-8bc8-6db397e1c5a6",
            "subject": "CHEBI:3756",
            "predicate": "biolink:treats",
            "object": "HP:0100749",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_PRIMARY,
        },
    },
    # 7) Dual-category (EntityToDiseaseAssociation first) / applied_to_treat / observation
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "014205ae-7cd4-5d5c-b7b4-b563514fe7e1",
            "subject": "CHEBI:231594",
            "predicate": "biolink:applied_to_treat",
            "object": "MONDO:0005133",
            "knowledge_level": KnowledgeLevelEnum.observation,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_AGGREGATOR,
        },
    },
    # 8) Dual-category (EntityToDiseaseAssociation first) / treats / knowledge_assertion
    {
        "association_class": EntityToDiseaseAssociation,
        "params": {
            "id": "07d55aab-ff0d-572d-b07e-17417c941b27",
            "subject": "CHEBI:9667",
            "predicate": "biolink:treats",
            "object": "MONDO:0005011",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _SOURCES_PRIMARY,
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
