import logging
import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneAssociation,
    Gene
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
    assert association.object == "UniProtKB:Q67890"
    assert association.predicate == "biolink:directly_positively_regulates"
    assert association.original_predicate == "RO:0002629"
    assert association.publications == ["PMID:12345678"]
    # Check that sources are properly set
    assert association.sources is not None
    assert len(association.sources) >= 1
    primary_source = [s for s in association.sources if s.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == "infores:go-cam"
