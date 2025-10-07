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
             }
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
        (   # Query 0 - Missing data (gene_identifier key as an example) - returns None
            {
                # "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },
            None,
            None
        ),
        (   # Query 1 - GeneFamily--has_part->GeneFamily
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.FAMILY:PTHR23158",
                    "category": ["biolink:GeneFamily"]
                },
                {
                    "id": "PANTHER.FAMILY:PTHR23158:SF57",
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneFamilyToGeneOrGeneProductOrGeneFamilyAssociation"],
                "subject": "PANTHER.FAMILY:PTHR23158",
                "object": "PANTHER.FAMILY:PTHR23158:SF57",
                "predicate": "biolink:has_part",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 2 - GeneFamily--has_part->Gene
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.FAMILY:PTHR23158",
                    "category": ["biolink:GeneFamily"]
                },
                {
                    "id": "NCBIGene:375056",
                    "category": ["biolink:Gene"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneFamilyToGeneOrGeneProductOrGeneFamilyAssociation"],
                "subject": "PANTHER.FAMILY:PTHR23158",
                "object": "NCBIGene:375056",
                "predicate": "biolink:has_part",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 3 - GeneFamily--located_in->CellularComponent
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.FAMILY:PTHR23158",
                    "category": ["biolink:GeneFamily"]
                },
                {
                    "id": "GO:0070971",
                    "category": ["biolink:CellularComponent"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneOrGeneProductOrGeneFamilyToAnatomicalEntityAssociation"],
                "subject": "PANTHER.FAMILY:PTHR23158",
                "object": "GO:0070971",
                "predicate": "biolink:located_in",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 4 - GeneFamily--actively_involved_in->BiologicalProcess
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.FAMILY:PTHR23158",
                    "category": ["biolink:GeneFamily"]
                },
                {
                    "id": "GO:0006888",
                    "category": ["biolink:BiologicalProcess"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneOrGeneProductOrGeneFamilyToBiologicalProcessOrActivityAssociation"],
                "subject": "PANTHER.FAMILY:PTHR23158",
                "object": "GO:0006888",
                "predicate": "biolink:actively_involved_in",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 5 - GeneFamily--catalyzes->MolecularActivity
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.FAMILY:PTHR10489",
                    "category": ["biolink:GeneFamily"]
                },
                {
                    "id": "GO:0038023",
                    "category": ["biolink:MolecularActivity"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneOrGeneProductOrGeneFamilyToBiologicalProcessOrActivityAssociation"],
                "subject": "PANTHER.FAMILY:PTHR10489",
                "object": "GO:0038023",
                "predicate": "biolink:catalyzes",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 6 - GeneFamily--actively_involved_in->Pathway
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.FAMILY:PTHR10489",
                    "category": ["biolink:GeneFamily"]
                },
                {
                    "id": "GO:0007165",
                    "category": ["biolink:Pathway"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:GeneOrGeneProductOrGeneFamilyToBiologicalProcessOrActivityAssociation"],
                "subject": "PANTHER.FAMILY:PTHR10489",
                "object": "GO:0007165",
                "predicate": "biolink:actively_involved_in",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 7 - Pathway--has_participant->GeneFamily
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "PANTHER.PATHWAY:P00044",
                    "category": ["biolink:Pathway"]
                },
                {
                    "id": "PANTHER.FAMILY:PTHR23158",
                    "category": ["biolink:GeneFamily"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:BiologicalProcessOrActivityToGeneOrGeneProductOrGeneFamilyAssociation"],
                "subject": "PANTHER.PATHWAY:P00044",
                "object": "PANTHER.FAMILY:PTHR23158",
                "predicate": "biolink:has_participant",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 8 - Pathway--subclass_of->BiologicalProcess
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:0030845",
                    "category": ["biolink:Pathway"]
                },
                {
                    "id": "GO:0065007",
                    "category": ["biolink:BiologicalProcess"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:BiologicalProcessOrActivityToBiologicalProcessOrActivityAssociation"],
                "subject": "GO:0030845",
                "object": "GO:0065007",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 9 - Pathway--subclass_of->Pathway
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:0010476",
                    "category": ["biolink:Pathway"]
                },
                {
                    "id": "GO:0007165",
                    "category": ["biolink:Pathway"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:BiologicalProcessOrActivityToBiologicalProcessOrActivityAssociation"],
                "subject": "GO:0010476",
                "object": "GO:0007165",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 10 - MolecularActivity--subclass_of->MolecularActivity
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:0031829",
                    "category": ["biolink:MolecularActivity"]
                },
                {
                    "id": "GO:0005515",
                    "category": ["biolink:MolecularActivity"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:BiologicalProcessOrActivityToBiologicalProcessOrActivityAssociation"],
                "subject": "GO:0031829",
                "object": "GO:0005515",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 11 - BiologicalProcess--subclass_of->BiologicalProcess
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:0090317",
                    "category": ["biolink:BiologicalProcess"]
                },
                {
                    "id": "GO:0032879",
                    "category": ["biolink:BiologicalProcess"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:BiologicalProcessOrActivityToBiologicalProcessOrActivityAssociation"],
                "subject": "GO:0090317",
                "object": "GO:0032879",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 12 - CellularComponent--subclass_of->CellularComponent
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:1990005",
                    "category": ["biolink:CellularComponent"]
                },
                {
                    "id": "GO:0043226",
                    "category": ["biolink:CellularComponent"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityToAnatomicalEntityAssociation"],
                "subject": "GO:1990005",
                "object": "GO:0043226",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 13 - CellularComponent--subclass_of->AnatomicalEntity
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:0005634",
                    "category": ["biolink:CellularComponent"]
                },
                {
                    "id": "UBERON:0001062",
                    "category": ["biolink:AnatomicalEntity"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityToAnatomicalEntityAssociation"],
                "subject": "GO:0005634",
                "object": "UBERON:0001062",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
             }
        ),
        (   # Query 14 - CellularComponent--subclass_of->GrossAnatomicalStructure
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "GO:0005604",
                    "category": ["biolink:CellularComponent"]
                },
                {
                    "id": "UBERON:0000475",
                    "category": ["biolink:GrossAnatomicalStructure"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityToAnatomicalEntityAssociation"],
                "subject": "GO:0005604",
                "object": "UBERON:0000475",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
            }
        ),
        (   # Query 15 - AnatomicalEntity--subclass_of->CellularComponent
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "UBERON:0008877",
                    "category": ["biolink:AnatomicalEntity"]
                },
                {
                    "id": "GO:0005604",
                    "category": ["biolink:CellularComponent"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityToAnatomicalEntityAssociation"],
                "subject": "UBERON:0008877",
                "object": "GO:0005604",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
            }
        ),
        (   # Query 16 - GrossAnatomicalStructure--subclass_of->CellularComponent
            {
                "gene_identifier": "",
                "protein_id": "",
                "gene_name": "",
                "panther_sf_id": "",
                "panther_family_name": "",
                "panther_subfamily_name": "",
                "panther_molecular_func": "",
                "panther_biological_process": "",
                "cellular_components": "",
                "protein_class": "",
                "pathway": ""
            },

            # Captured node contents
            [
                {
                    "id": "UBERON:4000020",
                    "category": ["biolink:GrossAnatomicalStructure"]
                },
                {
                    "id": "GO:0030312",
                    "category": ["biolink:CellularComponent"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityToAnatomicalEntityAssociation"],
                "subject": "UBERON:4000020",
                "object": "GO:0030312",
                "predicate": "biolink:subclass_of",
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:panther"
                    }
                ],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_validation_of_automated_agent
            }
        )
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
