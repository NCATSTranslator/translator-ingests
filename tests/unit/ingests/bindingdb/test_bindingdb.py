import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests._ingest_template._ingest_template import (
    on_begin_ingest_by_record,
    transform_ingest_by_record,
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
        (  # Query 0 - Missing PubMedIDs - returns None
            {
                "ChemicalName": "10074-G5",
                "ChemicalID": "C534883",
                "CasRN": "",
                "DiseaseName": "Burkitt Lymphoma",
                "DiseaseID": "MESH:D002051",
                "DirectEvidence": "",
                "InferenceGeneSymbol": "MYC",
                "InferenceScore": "",
                "OmimIDs": "113970",
                "PubMedIDs": "",  # empty expected field, hence, parse doesn't return a knowledge graph
            },
            None,
            None,
        ),
        (  # Query 1 - Another record complete with PubMedIDs
            {
                # 199,
                # CN(Cc1ccc(s1)C(=O)N[C@@H](CC(O)=O)C(=O)CSCc1ccccc1Cl)Cc1ccc(O)c(c1)C(O)=O |r|,
                # "InChI=1S/C27H27ClN2O7S2/c1-30(12-16-6-8-22(31)19(10-16)27(36)37)13-18-7-9-24(39-18)26(35)29-21(11-25(33)34)23(32)15-38-14-17-4-2-3-5-20(17)28/h2-10,21,31H,11-15H2,1H3,(H,29,35)(H,33,34)(H,36,37)",
                # FIEQQFOHZKVJLV-UHFFFAOYSA-N,
                # 219,
                # 5-({[(5-{[(2S)-1-carboxy-4-{[(2-chlorophenyl)methyl]sulfanyl}-3-oxobutan-2-yl]carbamoyl}thiophen-2-yl)methyl](methyl)amino}methyl)-2-hydroxybenzoic acid::Thiophene Scaffold 47c::Inhibitor 47c,
                # Caspase-3,Homo sapiens, 90,,,,,,7.4000,25.00 C,Curated from the literature by BindingDB,10.1021/jm020230j,10.7270/Q2B56GW5,12408711,aid1795219
            },
            # Captured node contents
            [
                {"id": "MESH:C534883", "name": "10074-G5", "category": ["biolink:ChemicalEntity"]},
                {"id": "MESH:D013734", "name": "Androgen-Insensitivity Syndrome", "category": ["biolink:Disease"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation"],
                "subject": "MESH:C534883",
                "predicate": "biolink:related_to",
                "object": "MESH:D013734",
                "publications": ["PMID:1303262", "PMID:8281139"],
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:ctd"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
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
        result=transform_ingest_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )
