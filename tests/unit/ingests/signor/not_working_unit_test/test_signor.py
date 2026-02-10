import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests._ingest_template._ingest_template import (
    on_begin_ingest_by_record,
    transform_ingest_by_record,
    transform_ingest_all,
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "publications",
    "sources",
    "knowledge_level",
    "agent_type",
)


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - resource_id - returns infores id
            {
                "GeneName": "CDK2",
                "GeneID": "UniProtKB:P24941",
                "GeneName": "NPAT",
                "GeneID": "UniProtKB:Q14207",
                "predicate": "biolink:regulates",
                "resource_id": "infores:signor",  # expected field is signor as source
            },
            # Captured node contents
            [
                {"name": "CDK2", "id": "UniProtKB:P24941", "category": ["biolink:Protein"]},
                {"name": "NPAT", "id": "UniProtKB:Q14207", "category": ["biolink:Protein"]}
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneRegulatesGeneAssociation"],
                "subject": "UniProtKB:P24941",
                "predicate": "biolink:regulates",
                "object": "UniProtKB:Q14207",
                "sources": [{"id": "urn:uuid:90b1f606-edec-4b09-a990-bf83e93ac70b", "category": ["biolink:RetrievalSource"], "resource_id": "infores:signor", "resource_role": "primary_knowledge_source"}],
                "knowledge_level": "knowledge_assertion",
                "agent_type": "manual_agent",
                "object_aspect_qualifier": "activity_or_abundance",
                "object_direction_qualifier": "upregulated",
                "qualified_predicate": "biolink:causes"}
        ),
        (  # Query 1 - Another record with signorID for complex node
            {
                "ChemicalName": "midostaurin",
                "ChemicalID": "CHEBI:63452",
                "GeneName": "PRKCA",
                "GeneID": "UniProtKB:P17252",
                "predicate": "biolink:affects",
                "resource_id": "infores:signor",
            },
            # Captured node contents
            [
                {"id": "CHEBI:63452", "category": ["biolink:ChemicalEntity"], "name": "midostaurin"},
                {"name": "PRKCA", "id": "UniProtKB:P17252", "category": ["biolink:Protein"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:GeneRegulatesGeneAssociation"],
                "subject": "CHEBI:63452",
                "predicate": "biolink:affects",
                "object": "UniProtKB:P17252",
                "sources": [{"id": "urn:uuid:c6dc9031-4f69-41bb-bc10-ef8bd279bd44", "category": ["biolink:RetrievalSource"], "resource_id": "infores:signor", "resource_role": "primary_knowledge_source"}],
                "knowledge_level": "knowledge_assertion",
                "agent_type": "manual_agent",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": "downregulated",
                "qualified_predicate": "biolink:causes"
            },
        ),
    ],
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):
    on_begin_ingest_by_record(mock_koza_transform)
    validate_transform_result(
        ## should use transform_ingest_all?
        # result=transform_ingest_by_record(mock_koza_transform, test_record),
        result=transform_ingest_all(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )
