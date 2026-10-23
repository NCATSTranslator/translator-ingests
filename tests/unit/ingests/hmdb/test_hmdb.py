import pytest

from pathlib import Path
from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

import koza
from koza.utils.exceptions import MapItemException
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.hmdb.hmdb import (
    on_begin_hmdb_ingest,
    transform_hmdb_ingest,
)
from translator_ingest.ingests.hmdb.hmdb_ingest_utils import (
	get_term_mapping,
	get_anatomy_term,
	get_go_cellular_term
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform

@pytest.fixture(scope="package")
def basic_mappings() -> Mappings:
    # Test Koza map
    return Mappings(
        {
            "go_lookup": {
                "cytoplasm": {
                    "go_id": "GO:0005737",
                    "go_label": "cytoplasm",
                    "match_type": "label"
                }
            },
            "uberon_lookup": {
                "neck of uterus": {
                    "uberon_id": "UBERON:0000002",
                    "uberon_label": "uterine cervix",
                    "match_type": "label"
                }
            },
            "cl_lookup": {
                "fibroblast": {
                "cl_id": "CL:0000057",
                "cl_label": "fibroblast",
                "match_type": "label"
            }
    }
        }
    ) 

@pytest.fixture(scope="package")
def basic_mock_koza_transform(basic_mappings: Mappings) -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    return MockKozaTransform(
        extra_fields={},
        writer=writer,
        mappings=basic_mappings,
        transform_metadata={},
        input_files_dir = None
    )

def test_get_term_mapping(basic_mock_koza_transform: koza.KozaTransform):
    term_id, term_name = get_term_mapping(basic_mock_koza_transform, ontology="go", name="cytoplasm")
    assert term_id == "GO:0005737" and term_name == "cytoplasm"
    term_id, term_name = get_term_mapping(basic_mock_koza_transform, ontology="uberon", name="neck of uterus")
    assert term_id == "UBERON:0000002" and term_name == "uterine cervix"


def test_get_term_mapping_failure(basic_mock_koza_transform: koza.KozaTransform):
    term_id, term_name = get_term_mapping(basic_mock_koza_transform, ontology="unknown", name="fake name")
    assert term_id == "HMDB:fake_name" and term_name == "fake name"


def test_get_go_cellular_term(basic_mock_koza_transform: koza.KozaTransform):
    term_id, term_name = get_go_cellular_term(basic_mock_koza_transform, name="Cytoplasm")
    assert term_id == "GO:0005737" and term_name == "cytoplasm"


def test_get_anatomy_term(basic_mock_koza_transform: koza.KozaTransform):
    term_id, term_name = get_anatomy_term(basic_mock_koza_transform, name="neck of uterus")
    assert term_id == "UBERON:0000002" and term_name == "uterine cervix"
    term_id, term_name = get_anatomy_term(basic_mock_koza_transform, name="Cellular Cytoplasm")
    assert term_id == term_id == "GO:0005737" and term_name == "cytoplasm"
    term_id, term_name = get_anatomy_term(basic_mock_koza_transform, name="Fibroblasts")
    assert term_id == term_id == "CL:0000057" and term_name == "fibroblast"


@pytest.fixture(scope="package")
def full_mappings() -> Mappings:
    # Test Koza map
    return Mappings(
        {
            "go_lookup": {
                "cytoplasm": {
                    "go_id": "GO:0005737",
                    "go_label": "cytoplasm",
                    "match_type": "label"
                }
            },
            "uberon_lookup": {
                "blood": {
                    "uberon_id": "UBERON:0000178",
                    "uberon_label": "blood",
                    "match_type": "label"
                },
                "cerebrospinal fluid": {
                    "uberon_id": "UBERON:0001359",
                    "uberon_label": "cerebrospinal fluid",
                    "match_type": "label"
                },
                "feces": {
                    "uberon_id": "UBERON:0001988",
                    "uberon_label": "feces",
                    "match_type": "label"
                },
                "saliva": {
                    "uberon_id": "UBERON:0001836",
                    "uberon_label": "saliva",
                    "match_type": "label"
                },
                "urine": {
                    "uberon_id": "UBERON:0001088",
                    "uberon_label": "urine",
                    "match_type": "label"
                },
                "placenta": {
                    "uberon_id": "UBERON:0001987",
                    "uberon_label": "placenta",
                    "match_type": "label"
                },
                "skeletal muscle": {
                    "uberon_id": "UBERON:0001134",
                    "uberon_label": "skeletal muscle tissue",
                    "match_type": "label"
                }
            }
        }
    )


@pytest.fixture(scope="package")
def full_mapping_mock_koza_transform(full_mappings: Mappings) -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    return MockKozaTransform(
        extra_fields={},
        writer=writer,
        mappings=full_mappings,
        transform_metadata={},
        # Swap in the following code for temporary debugging using the real data file
        # input_files_dir=INGESTS_DATA_PATH / "hmdb_metabolites.zip" # Path(__file__).resolve().parent
        input_files_dir = Path(__file__).resolve().parent
    )


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = (
    "category",
    "subject",
    "predicate",
    "object",
    "object_aspect_qualifier",
    "publications",
    "sources",
    "knowledge_level",
    "agent_type"
)

@pytest.mark.parametrize(
    "result_nodes,result_edge",
    [
        (  # Query 0 -
            # Contents of some nodes expected in the results
            [
                {
                    "id": "SMPDB:SMP0124716",
                    "name": "1-Methylhistidine Metabolism",
                    "category": ["biolink:Pathway"]
                },
                {
                    "id": "OMIM:125853",
                    "name": "Diabetes mellitus type 2",
                    "category": ["biolink:Disease"]
                },
                {
                    "id": "UniProtKB:Q96KN2",
                    "name": "Beta-Ala-His dipeptidase",
                    "category": ["biolink:Protein"]
                },
                {
                    "id": "HMDB:HMDB0000001",
                    "name": "1-Methylhistidine",
                    "category": ["biolink:MolecularEntity"]
                },
                {
                    "id": "GO:0005737",
                    "name": "cytoplasm",
                    "category": ["biolink:CellularComponent"]
                },
                {
                    "id": "UBERON:0000178",
                    "name": "blood",
                    "category": ["biolink:NamedThing"]
                },
                {
                    "id": "UBERON:0001987",
                    "name": "placenta",
                    "category": ["biolink:GrossAnatomicalStructure"]
                }
            ],
            # Captured edge contents
            [
                {
                    "category": ["biolink:ChemicalEntityToPathwayAssociation"],
                    "subject": "SMPDB:SMP0124716",
                    "predicate": "biolink:participates_in",
                    "object": "HMDB:HMDB0000001",
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hmdb"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_agent,
                },
                {
                    "category": ["biolink:DiseaseAssociatedWithResponseToChemicalEntityAssociation"],
                    "subject": "OMIM:104300",
                    "predicate": "biolink:associated_with_response_to",
                    "object": "HMDB:HMDB0000001",
                    "publications": ["PMID:17031479", "PMID:11959400"],
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hmdb"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_agent,
                },
                {
                    "category": ["biolink:GeneAffectsChemicalAssociation"],
                    "subject": "UniProtKB:Q96KN2",
                    "predicate": "biolink:regulates",
                    "object": "HMDB:HMDB0000001",
                    "object_aspect_qualifier": "abundance",
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hmdb"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_agent,
                },
                {
                    "category": ["biolink:Association"],
                    "subject": "HMDB:HMDB0000001",
                    "predicate": "biolink:located_in",
                    "object": "GO:0005737",
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hmdb"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_agent,
                },
                {
                    "category": ["biolink:Association"],
                    "subject": "HMDB:HMDB0000001",
                    "predicate": "biolink:located_in",
                    "object": "UBERON:0000178",
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hmdb"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_agent,
                },
                {
                    "category": ["biolink:Association"],
                    "subject": "HMDB:HMDB0000001",
                    "predicate": "biolink:located_in",
                    "object": "UBERON:0001987",
                    "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:hmdb"}],
                    "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                    "agent_type": AgentTypeEnum.manual_agent,
                }
            ],
        ),
    ],
)
def test_hmdb_ingest_transform(
    full_mapping_mock_koza_transform: koza.KozaTransform,
    result_nodes: list,
    result_edge: dict
):
    on_begin_hmdb_ingest(full_mapping_mock_koza_transform)

    # the data argument is ignored internally (i.e., the HMDB data file is directly read.)
    # hence we pass an empty list as data. The mock KozaTransform object points to the
    # sample HMDB data file located in the tests/unit/ingests/hmdb folder itself.
    # An iterable derived list of KnowledgeGraph objects is returned (a bit challenging to test but...)
    knowledge_graphs = list(transform_hmdb_ingest(full_mapping_mock_koza_transform, []))

    assert len(knowledge_graphs) == 1

    validate_transform_result(
        result=knowledge_graphs[0],
        expected_nodes=result_nodes,
        expected_edges=result_edge,
        expected_no_of_edges = 19,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS
    )
