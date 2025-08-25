from typing import Iterable
import json

import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneAssociation,
    Gene
)
from koza.io.writer.writer import KozaWriter
from koza.runner import KozaRunner, KozaTransformHooks
from translator_ingest.ingests.go_cam.go_cam import transform_go_cam_models
from pathlib import Path
from unittest.mock import MagicMock


class MockWriter(KozaWriter):
    def __init__(self):
        self.items = []

    def write(self, entities):
        if isinstance(entities, list):
            self.items.extend(entities)
        else:
            for entity in entities:
                self.items.append(entity)

    def write_nodes(self, nodes: Iterable):
        self.items.extend(nodes)

    def write_edges(self, edges: Iterable):
        self.items.extend(edges)

    def finalize(self):
        pass


@pytest.fixture
def gocam_output():
    writer = MockWriter()
    
    # Mock the koza transform object
    from koza.transform import KozaTransform
    mock_koza = KozaTransform(writer=writer, extra_fields={}, mappings={})
    
    # Create test data - this will be passed as an iterable to the transform function
    data = [{
        "graph": {
            "model_info": {
                "id": "gomodel:0000000300000001",
                "taxon": "NCBITaxon:9606",
                "title": "Test GO-CAM Model",
                "state": "production",
                "date": "2023-01-01",
                "contributor": "http://orcid.org/0000-0000-0000-0000"
            }
        },
        "nodes": [
            {
                "id": "UniProtKB:P12345",
                "label": "Test Gene 1"
            },
            {
                "id": "UniProtKB:Q67890",
                "label": "Test Gene 2"
            }
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
                "target_gene_product": "UniProtKB:Q67890"
            }
        ],
        "_file_path": "test_model.json"
    }]
    
    # Call the transform function directly
    results = transform_go_cam_models(mock_koza, data)
    for result in results:
        writer.write([result])
    
    return writer.items


def test_gocam_entities(gocam_output):
    entities = gocam_output
    assert entities
    assert len(entities) == 3

    genes = [e for e in entities if isinstance(e, Gene)]
    assert len(genes) == 2

    gene1 = [g for g in genes if g.id == "UniProtKB:P12345"][0]
    assert gene1.name == "Test Gene 1"
    assert gene1.category == ["biolink:Gene"]
    assert gene1.in_taxon == ["NCBITaxon:9606"]

    gene2 = [g for g in genes if g.id == "UniProtKB:Q67890"][0]
    assert gene2.name == "Test Gene 2"
    assert gene2.category == ["biolink:Gene"]
    assert gene2.in_taxon == ["NCBITaxon:9606"]

    associations = [e for e in entities if isinstance(e, GeneToGeneAssociation)]
    assert len(associations) == 1

    association = associations[0]
    assert association.subject == "UniProtKB:P12345"
    assert association.object == "UniProtKB:Q67890"
    assert association.predicate == "biolink:directly_positively_regulates"
    assert association.original_predicate == "RO:0002629"
    assert association.publications == ["PMID:12345678"]
    # Check that sources are properly set
    assert association.sources is not None
    assert len(association.sources) >= 1
    primary_source = [s for s in association.sources if s.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == "infores:go-cam"


def test_node_edge_consistency():
    """Test that all nodes referenced in edges are yielded as nodes using real test data."""
    # Load test data
    test_file = Path(__file__).parent / "input" / "67b1629100002092_networkx.json"
    with open(test_file) as f:
        model_data = json.load(f)
    
    # Add file path for processing
    model_data['_file_path'] = str(test_file)
    
    # Mock koza transform
    mock_koza = MagicMock()
    
    # Collect all yielded items
    yielded_items = list(transform_go_cam_models(mock_koza, [model_data]))
    
    # Separate nodes and edges
    nodes = [item for item in yielded_items if isinstance(item, Gene)]
    edges = [item for item in yielded_items if isinstance(item, GeneToGeneAssociation)]
    
    # Extract node IDs
    node_ids = {node.id for node in nodes}
    
    # Extract node IDs referenced in edges
    edge_node_refs = set()
    for edge in edges:
        edge_node_refs.add(edge.subject)
        edge_node_refs.add(edge.object)
    
    # Find missing nodes
    missing_nodes = edge_node_refs - node_ids
    
    print(f"Nodes yielded: {len(nodes)}")
    print(f"Edges yielded: {len(edges)}")
    print(f"Unique node IDs in nodes: {len(node_ids)}")
    print(f"Unique node IDs referenced by edges: {len(edge_node_refs)}")
    
    if missing_nodes:
        print(f"Missing nodes: {missing_nodes}")
        for node_id in missing_nodes:
            print(f"Missing node {node_id} found in source nodes: {node_id in [n['id'] for n in model_data.get('nodes', [])]}")
    
    # Assert no missing nodes
    assert len(missing_nodes) == 0, f"Missing nodes referenced in edges: {missing_nodes}"
