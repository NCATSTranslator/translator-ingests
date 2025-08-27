import pytest

from typing import Optional, Iterator, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.transform import Record, Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests._ingest_template._ingest_template import transform_ingest_by_record

from tests.unit.ingests import transform_test_runner


class MockKozaWriter(KozaWriter):
    """
    Mock "do nothing" implementation of a KozaWriter
    """
    def write(self, entities: Iterable):
        pass

    def finalize(self):
        pass

    def write_edges(self, edges: Iterable):
        pass

    def write_nodes(self, nodes: Iterable):
        pass


class MockKozaTransform(koza.KozaTransform):
    """
    Mock "do nothing" implementation of a KozaTransform
    """
    @property
    def current_reader(self) -> str:
        return ""

    @property
    def data(self) -> Iterator[Record]:
        record: Record = dict()
        yield record

# test mondo_map for the gene_to_phenotype
# disease_id to disease_context_qualifier mappings
mock_mondo_sssom_map: dict[str, dict[str, str]] = {
    "OMIM:231550": {"subject_id": "MONDO:0009279"},
    "Orphanet:442835": {"subject_id": "MONDO:0018614"},
    "OMIM:614129": {"subject_id": "MONDO:0013588"},
    "OMIM:613013": {"subject_id": "MONDO:0700041"}
}

@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = {"mondo_map": mock_mondo_sssom_map}
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)

# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = [
    "id",
    "name",
    "category",
    "provided_by",
    "inheritance"
]

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "publications",
    "has_evidence",
    "sex_qualifier",
    "onset_qualifier",
    "has_percentage",
    "has_quotient",
    "frequency_qualifier",
    "disease_context_qualifier",
    "sources",
    "knowledge_level",
    "agent_type"
]

#
# CTD record fields:
#
# - ChemicalName
# - ChemicalID
# - CasRN
# - DiseaseName
# - DiseaseID
# - DirectEvidence
# - InferenceGeneSymbol
# - InferenceScore
# - OmimIDs
# - PubMedIDs
#
# def transform_ingest_by_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
#
#     # here is an example of skipping a record based off of some condition
#     publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
#     if not publications:
#         koza.state['example_counter'] += 1
#         return None
#
#     chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
#     disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
#     association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
#         id=str(uuid.uuid4()),
#         subject=chemical.id,
#         predicate=BIOLINK_RELATED_TO,
#         object=disease.id,
#         publications=publications,
#         primary_knowledge_source=INFORES_CTD,
#         knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
#         agent_type=AgentTypeEnum.manual_agent,
#     )
#     return KnowledgeGraph(nodes=[chemical, disease], edges=[association])

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - missing data (empty record, hence, missing fields)
            {},
            None,
            None
        ),
        (  # Query 1 - Sample Mendelian disease
            {
                "association_type": "MENDELIAN",
                "disease_id": "OMIM:212050",
                "gene_symbol": "CARD9",
                "ncbi_gene_id": "NCBIGene:64170",
                "source": "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen",
            },

            # Captured node contents
            [
                {
                    "id": "NCBIGene:64170",
                    "name": "CARD9",
                    "category": ["biolink:Gene"]
                },
                {
                    "id": "OMIM:212050",
                    "category": ["biolink:Disease"]
                }
            ],

            # Captured edge contents
            {
                "category": ["biolink:CausalGeneToDiseaseAssociation"],
                "subject": "NCBIGene:64170",
                "predicate": "biolink:causes",
                "object": "OMIM:212050",

                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:hpo-annotations"
                    },
                    {
                        "resource_role": "supporting_data_source",
                        "resource_id": "infores:medgen"
                    },
                    {
                        "resource_role": "supporting_data_source",
                        "resource_id": "infores:omim"
                    }
                ],

                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent
            }
        )
    ]
)
def test_ingest_transform(
        mock_koza_transform: koza.KozaTransform,
        test_record: dict,
        result_nodes: Optional[list],
        result_edge: Optional[dict]
):
    transform_test_runner(
        result=transform_ingest_by_record(mock_koza_transform, test_record),
        expected_nodes=result_nodes,
        expected_edge=result_edge,
        node_test_slots=NODE_TEST_SLOTS,
        association_test_slots=ASSOCIATION_TEST_SLOTS
    )
