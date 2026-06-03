import io
import tarfile

import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from koza.model.graphs import KnowledgeGraph

from tests.unit.ingests import MockKozaWriter, MockKozaTransform

from translator_ingest.ingests.ubergraph.ubergraph import (
    on_begin_redundant_graph,
    on_end_redundant_graph,
    prepare_ontology_data,
    transform_redundant_graph,
    INFORES_UBERGRAPH,
    EXTRACTED_ONTOLOGY_PREFIXES,
    OBO_MISSING_MAPPINGS,
    BIOLINK_MAPPING_CHANGES,
)

SUBCLASS_OF_IRI = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
PART_OF_IRI = "http://purl.obolibrary.org/obo/BFO_0000050"


def test_extracted_ontology_prefixes():
    assert "UBERON" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "GO" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "MONDO" in EXTRACTED_ONTOLOGY_PREFIXES
    assert "CHEBI" in EXTRACTED_ONTOLOGY_PREFIXES
    assert len(EXTRACTED_ONTOLOGY_PREFIXES) > 0


def test_obo_missing_mappings():
    assert OBO_MISSING_MAPPINGS["NCBIGene"] == "http://purl.obolibrary.org/obo/NCBIGene_"
    assert OBO_MISSING_MAPPINGS["HGNC"] == "http://purl.obolibrary.org/obo/HGNC_"
    assert OBO_MISSING_MAPPINGS["SGD"] == "http://purl.obolibrary.org/obo/SGD_"


def test_biolink_mapping_changes():
    assert BIOLINK_MAPPING_CHANGES["KEGG"] == "http://identifiers.org/kegg/"
    assert BIOLINK_MAPPING_CHANGES["NCBIGene"] == "https://identifiers.org/ncbigene/"


def test_infores_ubergraph():
    assert INFORES_UBERGRAPH == "infores:ubergraph"


class MockKozaState:
    def __init__(self):
        self.state = {}
        self.transform_metadata = {}

    def __getitem__(self, key):
        return self.state[key]

    def __setitem__(self, key, value):
        self.state[key] = value

    def log(self, message, level="INFO"):
        pass


def test_on_begin_redundant_graph():
    mock_koza = MockKozaState()

    on_begin_redundant_graph(mock_koza)

    assert mock_koza.state["record_counter"] == 0
    assert mock_koza.state["skipped_record_counter"] == 0
    assert mock_koza.transform_metadata["redundant_graph"]["num_source_lines"] == 0
    assert mock_koza.transform_metadata["redundant_graph"]["unusable_source_lines"] == 0


def test_on_end_redundant_graph():
    mock_koza = MockKozaState()
    mock_koza.state = {
        "record_counter": 100,
        "skipped_record_counter": 10
    }
    mock_koza.transform_metadata = {"redundant_graph": {}}

    on_end_redundant_graph(mock_koza)

    assert mock_koza.transform_metadata["redundant_graph"]["num_source_lines"] == 100
    assert mock_koza.transform_metadata["redundant_graph"]["unusable_source_lines"] == 10


def _write_redundant_graph_tgz(directory, node_labels, edge_labels, edges):
    """Build a redundant-graph-table.tgz fixture matching the Ubergraph layout.

    Each argument is a list of tab-separated-value rows (without trailing newlines).
    """
    tar_path = directory / "redundant-graph-table.tgz"
    members = {
        "redundant-graph-table/node-labels.tsv": node_labels,
        "redundant-graph-table/edge-labels.tsv": edge_labels,
        "redundant-graph-table/edges.tsv": edges,
    }
    with tarfile.open(tar_path, "w:gz") as tar:
        for name, rows in members.items():
            payload = ("\n".join(rows) + "\n").encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    return tar_path


def test_prepare_ontology_data_filters_and_converts(tmp_path):
    # Node ids: GO/UBERON/HP are retained ontologies; "unmapped" has no CURIE;
    # BFO maps to a CURIE but is not in the extraction allow-list.
    node_labels = [
        "1\thttp://purl.obolibrary.org/obo/GO_0008150",
        "2\thttp://purl.obolibrary.org/obo/UBERON_0001062",
        "3\thttp://purl.obolibrary.org/obo/HP_0000118",
        "4\thttp://example.org/unmapped_thing",
        "5\thttp://purl.obolibrary.org/obo/BFO_0000001",
    ]
    edge_labels = [
        f"10\t{SUBCLASS_OF_IRI}",
        f"11\t{PART_OF_IRI}",
    ]
    edges = [
        "1\t10\t2",  # GO subClassOf UBERON -> kept
        "3\t10\t2",  # HP subClassOf UBERON -> kept (verifies the HP prefix fix)
        "1\t11\t2",  # GO part_of UBERON   -> skipped (predicate not mapped)
        "1\t10\t4",  # GO subClassOf unmapped -> skipped (object has no CURIE)
        "1\t10\t5",  # GO subClassOf BFO   -> skipped (object not in allow-list)
    ]
    _write_redundant_graph_tgz(tmp_path, node_labels, edge_labels, edges)

    mock_koza = MockKozaTransform(
        extra_fields={}, writer=MockKozaWriter(), mappings={}, input_files_dir=tmp_path
    )

    records = list(prepare_ontology_data(mock_koza, iter([])))

    assert records == [
        {"subject": "GO:0008150", "predicate": "biolink:subclass_of", "object": "UBERON:0001062"},
        {"subject": "HP:0000118", "predicate": "biolink:subclass_of", "object": "UBERON:0001062"},
    ]
    assert mock_koza.state["record_counter"] == 5
    assert mock_koza.state["skipped_record_counter"] == 3


@pytest.fixture
def mock_koza():
    return MockKozaTransform(extra_fields={}, writer=MockKozaWriter(), mappings={})


def test_transform_creates_nodes_and_edges(mock_koza):
    data = [
        {"subject": "GO:0008150", "predicate": "biolink:subclass_of", "object": "UBERON:0001062"},
    ]

    results = list(transform_redundant_graph(mock_koza, iter(data)))

    assert len(results) == 1
    kg = results[0]
    assert isinstance(kg, KnowledgeGraph)
    nodes = list(kg.nodes)
    edges = list(kg.edges)
    assert len(nodes) == 2
    assert len(edges) == 1

    node_ids = [node.id for node in kg.nodes]
    assert "GO:0008150" in node_ids
    assert "UBERON:0001062" in node_ids

    edge = edges[0]
    assert isinstance(edge, Association)
    assert edge.subject == "GO:0008150"
    assert edge.object == "UBERON:0001062"
    assert edge.predicate == "biolink:subclass_of"
    assert edge.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert len(edge.sources) == 1
    assert edge.sources[0].resource_id == INFORES_UBERGRAPH


def test_transform_deduplicates_nodes(mock_koza):
    data = [
        {"subject": "GO:0008150", "predicate": "biolink:subclass_of", "object": "UBERON:0001062"},
        {"subject": "GO:0008150", "predicate": "biolink:subclass_of", "object": "MONDO:0000001"},
    ]

    results = list(transform_redundant_graph(mock_koza, iter(data)))

    assert len(results) == 1
    kg = results[0]
    assert len(kg.edges) == 2
    assert len(kg.nodes) == 3

    node_ids = [node.id for node in kg.nodes]
    assert node_ids.count("GO:0008150") == 1