import logging
import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneAssociation,
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)
from translator_ingest.ingests.go_cam.go_cam import transform_go_cam_models

from tests.unit.ingests import MockKozaWriter

logger = logging.getLogger(__name__)


@pytest.fixture
def gocam_output():
    writer = MockKozaWriter()

    # Mock the koza transform object
    from koza.transform import KozaTransform

    mock_koza = KozaTransform(writer=writer, extra_fields={}, mappings={})

    # Create test data - this will be passed as an iterable to the transform function
    data = [
        {
            "graph": {
                "model_info": {
                    "id": "gomodel:0000000300000001",
                    "taxon": "NCBITaxon:9606",
                    "title": "Test GO-CAM Model",
                    "state": "production",
                    "date": "2023-01-01",
                    "contributor": "http://orcid.org/0000-0000-0000-0000",
                }
            },
            "nodes": [
                {"id": "UniProtKB:P12345", "label": "Test Gene 1"},
                {"id": "UniProtKB:Q67890", "label": "Test Gene 2"},
            ],
            "edges": [
                {
                    "source": "UniProtKB:P12345",
                    "target": "UniProtKB:Q67890",
                    "source_gene": "UniProtKB:P12345",
                    "target_gene": "UniProtKB:Q67890",
                    "model_id": "gomodel:GO:0001234",
                    "causal_predicate": "RO:0002629",
                    "causal_predicate_has_reference": ["PMID:12345678"],
                    "source_gene_molecular_function": "GO:0005515",
                    "source_gene_biological_process": "GO:0003700",
                    "source_gene_occurs_in": "GO:0005634",
                    "source_gene_product": "UniProtKB:P12345",
                    "target_gene_molecular_function": "GO:0043565",
                    "target_gene_biological_process": "GO:0003700",
                    "target_gene_occurs_in": "GO:0005634",
                    "target_gene_product": "UniProtKB:Q67890",
                }
            ],
            "_file_path": "test_model.json",
        }
    ]

    # Call the transform function directly
    results = transform_go_cam_models(mock_koza, data)
    for result in results:
        writer.write([result])

    return writer.items


def test_gocam_entities(gocam_output):
    entities = gocam_output
    assert entities
    assert len(entities) == 1

    # Extract nodes and edges from KnowledgeGraph
    kg = entities[0]
    from koza.model.graphs import KnowledgeGraph

    assert isinstance(kg, KnowledgeGraph)

    all_entities = list()
    all_entities.extend(kg.nodes)
    all_entities.extend(kg.edges)

    genes = [e for e in all_entities if isinstance(e, Gene)]
    assert len(genes) == 2

    gene1 = [g for g in genes if g.id == "UniProtKB:P12345"][0]
    assert gene1.name == "Test Gene 1"
    assert gene1.category == ["biolink:Gene"]
    assert gene1.in_taxon == ["NCBITaxon:9606"]

    gene2 = [g for g in genes if g.id == "UniProtKB:Q67890"][0]
    assert gene2.name == "Test Gene 2"
    assert gene2.category == ["biolink:Gene"]
    assert gene2.in_taxon == ["NCBITaxon:9606"]

    associations = [e for e in all_entities if isinstance(e, GeneToGeneAssociation)]
    assert len(associations) == 1

    association = associations[0]
    assert association.subject == "UniProtKB:P12345"
    assert association.subject_activity_qualifier == "GO:0005515"
    assert association.subject_process_qualifier == "GO:0003700"
    assert association.subject_context_qualifier == "GO:0005634"
    assert association.predicate == "biolink:regulates"
    assert association.object == "UniProtKB:Q67890"
    assert association.object_activity_qualifier == "GO:0043565"
    assert association.object_process_qualifier == "GO:0003700"
    assert association.object_context_qualifier == "GO:0005634"
    assert association.original_predicate == "RO:0002629"
    assert association.publications == ["PMID:12345678"]
    # Check that sources are properly set
    assert association.sources is not None
    assert len(association.sources) >= 1
    primary_source = [s for s in association.sources if s.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == "infores:go-cam"


# -- Pydantic round-trip fixtures & test --

_GO_CAM_SOURCES = [
    RetrievalSource(
        id="infores:go-cam",
        resource_id="infores:go-cam",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "00007103-672d-5079-8d8c-6a4b1dc880f6",
            "subject": "NCBIGene:16171",
            "predicate": "biolink:acts_upstream_of",
            "object": "NCBIGene:19695",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "a06e6f4a-e1fd-48e2-8a20-211210adcde9",
            "subject": "NCBIGene:10919",
            "predicate": "biolink:related_to",
            "object": "NCBIGene:23468",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "79badbdf-e594-4d5d-a4d6-3d8fd7e69f84",
            "subject": "NCBIGene:14526",
            "predicate": "biolink:regulates",
            "object": "NCBIGene:14527",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "f3a0ad16-fb39-4dd6-9223-79c01edef663",
            "subject": "NCBIGene:81545",
            "predicate": "biolink:is_input_of",
            "object": "NCBIGene:5595",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "006d851a-04d7-50c9-84f4-ebd652ed5a22",
            "subject": "NCBIGene:652968",
            "predicate": "biolink:causes",
            "object": "NCBIGene:84219",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "70b69544-7ac5-47fe-aa76-c5c9d8690136",
            "subject": "NCBIGene:65960",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "NCBIGene:65960",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "3b6e144c-f380-4292-97bc-57d0f6b69733",
            "subject": "NCBIGene:7421",
            "predicate": "biolink:acts_upstream_of_negative_effect",
            "object": "NCBIGene:1365",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "cbdb38b8-4594-40aa-ad81-9a66f0e0376a",
            "subject": "NCBIGene:619665",
            "predicate": "biolink:has_part",
            "object": "NCBIGene:619665",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "709f5611-515c-418f-8c21-a4ecbe9c761b",
            "subject": "NCBIGene:29110",
            "predicate": "biolink:has_input",
            "object": "UniProtKB:M0R3E9",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "2e10150d-f59d-48fa-a4a4-49cb2d9e041b",
            "subject": "NCBIGene:11684",
            "predicate": "biolink:precedes",
            "object": "NCBIGene:15446",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "aeaf7444-16ef-4951-8cd1-cc6beccb259a",
            "subject": "NCBIGene:60391",
            "predicate": "biolink:part_of",
            "object": "NCBIGene:60391",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "bdc24992-5eb9-4df4-aa84-775c6326a977",
            "subject": "NCBIGene:12578",
            "predicate": "biolink:acts_upstream_of_or_within_negative_effect",
            "object": "NCBIGene:104394",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "750b3403-193b-4c8d-bbe8-3b5a6e9e9750",
            "subject": "NCBIGene:51548",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:22800",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "9ec3fd8d-8e85-4c74-8d2b-c22bf7d2e723",
            "subject": "UniProtKB:Q6ZSJ9-1",
            "predicate": "biolink:enabled_by",
            "object": "NCBIGene:1742",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
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
