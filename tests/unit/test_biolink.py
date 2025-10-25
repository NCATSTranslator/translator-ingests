from bmt import Toolkit

from translator_ingest.util.biolink import get_node_class
import biolink_model.datamodel.pydanticmodel_v2 as pyd


def test_get_node_class_by_category_curie():
    # Biolink CURIE category
    disease_node_id: str = "DOID:0111266"
    node_class = get_node_class(node_id=disease_node_id, categories=["biolink:Disease"])
    assert node_class is not None
    disease_node = node_class(id=disease_node_id,**{})
    assert isinstance(disease_node,pyd.Disease)

def test_get_node_class_simple_name():
    # Naked name of category
    gene_node_id: str = "HGNC:12791"
    node_class = get_node_class(node_id=gene_node_id, categories=["Gene"])
    assert node_class is not None
    gene_node = node_class(id=gene_node_id,**{})
    assert isinstance(gene_node,pyd.Gene)

def test_get_node_class_unknown_category():
    # Unknown category
    nonsense_node_id: str = "foo:bar"
    node_class = get_node_class(node_id=nonsense_node_id, categories=["biolink:Nonsense"])
    assert node_class == pyd.NamedThing

def test_get_node_class_empty_categories():
    # Empty categories
    empty_node_id: str = "empty:node"
    node_class = get_node_class(node_id=empty_node_id, categories=[])
    assert node_class is pyd.NamedThing

def test_get_node_class_from_most_specific_category():
    # List of categories - want the most specific one
    disease_node_id: str = "DOID:0111266"
    node_class = get_node_class(
        node_id=disease_node_id,
        categories=[
            "biolink:Disease",
            "biolink:DiseaseOrPhenotypicFeature"
            "biolink:ThingWithTaxon",
            "biolink:BiologicalEntity",
            "biolink:NamedThing",
        ]
    )
    assert node_class is not None
    disease_node = node_class(id=disease_node_id,**{})
    assert isinstance(disease_node,pyd.Disease)
