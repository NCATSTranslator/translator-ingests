import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import  Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.panther.panther import (
    # on_begin_ingest_by_record,
    transform_gene_to_gene_orthology
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    extra_fields = dict()
    extra_fields["tx_gmap"] = dict()
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=extra_fields, writer=writer, mappings=mappings)

# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = [
    "id",
    "in_taxon",
    "category"
]

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "predicate",
    "object",
    "has_evidence",
    "sources",
    "knowledge_level",
    "agent_type"
]


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (   # Query 0 - Missing a record field column (Gene key as an example) - returns None
            {
                # "Gene": "HUMAN|HGNC=11477|UniProtKB=Q6GZX4",
                "Ortholog": "RAT|RGD=1564893|UniProtKB=Q6GZX2",
                "Type of ortholog": "LDO",
                "Common ancestor for the orthologs": "Euarchontoglires",
                "Panther Ortholog ID": "PTHR12434"
            },
            None,
            None
        ),
        (   # Query 1 - Empty record field (Gene key as an example) - returns None
            {
                "Gene": "",
                "Ortholog": "RAT|RGD=1564893|UniProtKB=Q6GZX2",
                "Type of ortholog": "LDO",
                "Common ancestor for the orthologs": "Euarchontoglires",
                "Panther Ortholog ID": "PTHR12434"
            },
            None,
            None
        ),
        (   # Query 2 - This data includes Genes from a currently excluded species, S. Pombe - returns None
            {
                "Gene": "MOUSE|MGI=MGI=2147627|UniProtKB=Q91WQ3",
                "Ortholog": "SCHPO|PomBase=SPAC30C2.04|UniProtKB=Q9P6K7",
                "Type of ortholog": "LDO",
                "Common ancestor for the orthologs": "Opisthokonts",
                "Panther Ortholog ID": "PTHR11586"
            },
            None,
            None
        ),
        (   # Query 3 - Regular record, HUMAN (HGNC identified gene) to RAT ortholog row test
            {
                 "Gene": "HUMAN|HGNC=11477|UniProtKB=Q6GZX4",              # species1|DB=id1|protdb=pdbid1
                 "Ortholog": "RAT|RGD=1564893|UniProtKB=Q6GZX2",           # species2|DB=id2|protdb=pdbid2
                 "Type of ortholog": "LDO",                                # [LDO, O, P, X ,LDX]  see: localtt
                 "Common ancestor for the orthologs": "Euarchontoglires",  # unused
                 "Panther Ortholog ID": "PTHR12434"
            },

            # Captured node contents
            [
                {
                    "id": "HGNC:11477",
                    "in_taxon": ["NCBITaxon:9606"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "RGD:1564893",
                    "in_taxon": ["NCBITaxon:10116"],
                    "category": ["biolink:Gene"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneToGeneHomologyAssociation"],
                "subject": "HGNC:11477",
                "object": "RGD:1564893",
                "predicate": "biolink:orthologous_to",
                "has_evidence": ["PANTHER.FAMILY:PTHR12434"],
                "aggregator_knowledge_source": ["infores:translator-panther-kgx"],
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    },
                    # {
                    #     "resource_role": "aggregator_knowledge_source",
                    #     "resource_id": "infores:translator-panther-kgx",
                    #     "upstream_resource_ids": ["infores:panther"]
                    #
                    # }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
            }
        ),
        (   # Query 4 - Regular record, HUMAN (HGNC identified gene) to RAT ortholog row test
            {
                "Gene": "HUMAN|Ensembl=ENSG00000275949.5|UniProtKB=A0A0G2JMH3",
                "Ortholog": "MOUSE|MGI=MGI=99431|UniProtKB=P84078",
                "Type of ortholog": "O",
                "Common ancestor for the orthologs": "Euarchontoglires",
                "Panther Ortholog ID": "PTHR11711"
            },

            # Captured node contents
            [
                {
                    "id": "ENSEMBL:ENSG00000275949",
                    "in_taxon": ["NCBITaxon:9606"],
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "MGI:99431",
                    "in_taxon": ["NCBITaxon:10090"],
                    "category": ["biolink:Gene"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneToGeneHomologyAssociation"],
                "subject": "ENSEMBL:ENSG00000275949",
                "object": "MGI:99431",
                "predicate": "biolink:orthologous_to",
                "has_evidence": ["PANTHER.FAMILY:PTHR11711"],
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    },
                    # {
                    #     "resource_role": "aggregator_knowledge_source",
                    #     "resource_id": "infores:translator-panther-kgx",
                    #     "upstream_resource_ids": ["infores:panther"]
                    #
                    # }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided
             },
        )
    ]
)
def test_ingest_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    # on_begin_ingest_by_record(mock_koza_transform)
    validate_transform_result(
        result=transform_gene_to_gene_orthology(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edge=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        association_test_slots=ASSOCIATION_TEST_SLOTS
    )
