import re

import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import  Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.panther.panther import (
    get_latest_version,
    transform_gene_orthology,
    transform_gene_classification
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform

# Whatever the version number is, it should not be "Not found"
def test_get_latest_version():
    version: str = get_latest_version()
    assert version != "Not found"
    assert re.match(r"\d{1,2}\.\d", version)  # something like "19.0"

@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    extra_fields = dict()
    # extra_fields["ntg_map"] = dict()
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=extra_fields, writer=writer, mappings=mappings)

# list of slots whose values are
# to be checked in a result node
ORTHOLOG_NODE_TEST_SLOTS = [
    "id",
    "in_taxon",
    "category"
]

# list of slots whose values are
# to be checked in a result edge
ORTHOLOGY_ASSOCIATION_TEST_SLOTS = [
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
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
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
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             },
        ),
    ]
)
def test_transform_gene_orthology(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    # on_begin_ingest_by_record(mock_koza_transform)
    validate_transform_result(
        result=transform_gene_orthology(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edge=result_edge,
        node_test_slots=ORTHOLOG_NODE_TEST_SLOTS,
        association_test_slots=ORTHOLOGY_ASSOCIATION_TEST_SLOTS
    )


# list of slots whose values are
# to be checked in a result node
ANNOTATION_NODE_TEST_SLOTS = [
    "id",
    "category"
]

# list of slots whose values are
# to be checked in a result edge
ANNOTATION_ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "predicate",
    "object",
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
# ORION edge cases test data to add to unit tests(?)
# {
#   "source_type": "primary",
#   "edges": [
#     {
#       "subject_category": "biolink:GeneFamily",
#       "object_category": "biolink:GeneFamily",
#       "predicate": "biolink:has_part",
#       "subject_id": "PANTHER.FAMILY:PTHR23158",
#       "object_id": "PANTHER.FAMILY:PTHR23158:SF57"
#     },
#     {
#       "subject_category": "biolink:GeneFamily",
#       "object_category": "biolink:Gene",
#       "predicate": "biolink:has_part",
#       "subject_id": "PANTHER.FAMILY:PTHR23158",
#       "object_id": "NCBIGene:375056"
#     },
#     {
#       "subject_category": "biolink:GeneFamily",
#       "object_category": "biolink:CellularComponent",
#       "predicate": "biolink:located_in",
#       "subject_id": "PANTHER.FAMILY:PTHR23158",
#       "object_id": "GO:0070971"
#     },
#     {
#       "subject_category": "biolink:GeneFamily",
#       "object_category": "biolink:BiologicalProcess",
#       "predicate": "biolink:actively_involved_in",
#       "subject_id": "PANTHER.FAMILY:PTHR23158",
#       "object_id": "GO:0006888"
#     },
#     {
#       "subject_category": "biolink:GeneFamily",
#       "object_category": "biolink:MolecularActivity",
#       "predicate": "biolink:catalyzes",
#       "subject_id": "PANTHER.FAMILY:PTHR10489",
#       "object_id": "GO:0038023"
#     },
#     {
#       "subject_category": "biolink:GeneFamily",
#       "object_category": "biolink:Pathway",
#       "predicate": "biolink:actively_involved_in",
#       "subject_id": "PANTHER.FAMILY:PTHR10489",
#       "object_id": "GO:0007165"
#     },
#     {
#       "subject_category": "biolink:Pathway",
#       "object_category": "biolink:GeneFamily",
#       "predicate": "biolink:has_participant",
#       "subject_id": "PANTHER.PATHWAY:P00044",
#       "object_id": "PANTHER.FAMILY:PTHR23158"
#     },
#     {
#       "subject_category": "biolink:Pathway",
#       "object_category": "biolink:BiologicalProcess",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:0030845",
#       "object_id": "GO:0065007"
#     },
#     {
#       "subject_category": "biolink:Pathway",
#       "object_category": "biolink:Pathway",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:0010476",
#       "object_id": "GO:0007165"
#     },
#     {
#       "subject_category": "biolink:MolecularActivity",
#       "object_category": "biolink:MolecularActivity",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:0031829",
#       "object_id": "GO:0005515"
#     },
#     {
#       "subject_category": "biolink:BiologicalProcess",
#       "object_category": "biolink:BiologicalProcess",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:0090317",
#       "object_id": "GO:0032879"
#     },
#     {
#       "subject_category": "biolink:CellularComponent",
#       "object_category": "biolink:CellularComponent",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:1990005",
#       "object_id": "GO:0043226"
#     },
#     {
#       "subject_category": "biolink:CellularComponent",
#       "object_category": "biolink:AnatomicalEntity",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:0005634",
#       "object_id": "UBERON:0001062"
#     },
#     {
#       "subject_category": "biolink:CellularComponent",
#       "object_category": "biolink:GrossAnatomicalStructure",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "GO:0005604",
#       "object_id": "UBERON:0000475"
#     },
#     {
#       "subject_category": "biolink:AnatomicalEntity",
#       "object_category": "biolink:CellularComponent",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "UBERON:0008877",
#       "object_id": "GO:0005604"
#     },
#     {
#       "subject_category": "biolink:GrossAnatomicalStructure",
#       "object_category": "biolink:CellularComponent",
#       "predicate": "biolink:subclass_of",
#       "subject_id": "UBERON:4000020",
#       "object_id": "GO:0030312"
#     }
#   ]
# }
    ]
)
def test_transform_gene_classification(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    # on_begin_ingest_by_record(mock_koza_transform)
    validate_transform_result(
        result=transform_gene_classification(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edge=result_edge,
        node_test_slots=ANNOTATION_NODE_TEST_SLOTS,
        association_test_slots=ANNOTATION_ASSOCIATION_TEST_SLOTS
    )
