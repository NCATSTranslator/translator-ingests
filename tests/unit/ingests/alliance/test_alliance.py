"""
Test suite for Alliance translator ingest.

Tests phenotype and expression transforms with focus on mouse and rat data filtering.
Verifies that nodes are created on both sides of associations.
"""

import pytest
from unittest.mock import patch
from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    PhenotypicFeature,
    AnatomicalEntity,
    CellularComponent,
    GeneToPhenotypicFeatureAssociation,
    GeneToExpressionSiteAssociation,
)
from translator_ingest.ingests.alliance.alliance import (
    transform_phenotype,
    transform_expression,
)

# Test entity lookup dictionary - maps entity IDs to their biolink categories
# Used to mock the DuckDB lookup without requiring actual data files
TEST_ENTITY_LOOKUP = {
    "MGI:98834": "biolink:Gene",  # Mouse gene used in tests
    "RGD:1234567": "biolink:Gene",  # Example rat gene
}

# ===== PHENOTYPE TESTS =====


@pytest.fixture
def mgi_phenotype_row():
    """Mouse phenotype association"""
    return {
        "objectId": "MGI:98834",
        "phenotypeTermIdentifiers": [{"termId": "MP:0001262", "termOrder": 1}],
        "evidence": {"publicationId": "PMID:12345678"},
    }


@pytest.fixture
def mgi_phenotype_multi_term_row():
    """Mouse phenotype with multiple terms - should NOT be filtered in translator ingest"""
    return {
        "objectId": "MGI:98834",
        "phenotypeTermIdentifiers": [
            {"termId": "MP:0001262", "termOrder": 1},
            {"termId": "MP:0002169", "termOrder": 2},
        ],
        "evidence": {"publicationId": "PMID:12345678"},
    }


def test_mgi_phenotype(mgi_phenotype_row):
    """Test mouse phenotype is processed and nodes are created"""
    # Mock the entity lookup to return values from our test dictionary
    with patch("translator_ingest.ingests.alliance.alliance.lookup_entity_category") as mock_lookup:
        mock_lookup.side_effect = lambda entity_id: TEST_ENTITY_LOOKUP.get(entity_id)

        result = transform_phenotype(None, mgi_phenotype_row)

        # Should return: Gene node + PhenotypicFeature node + Association
        assert len(result) == 3

        # Separate results by type (order-independent)
        genes = [r for r in result if isinstance(r, Gene)]
        phenotypes = [r for r in result if isinstance(r, PhenotypicFeature)]
        assocs = [r for r in result if isinstance(r, GeneToPhenotypicFeatureAssociation)]

        # Check counts
        assert len(genes) == 1
        assert len(phenotypes) == 1
        assert len(assocs) == 1

        # Check gene node
        gene = genes[0]
        assert gene.id == "MGI:98834"
        assert "biolink:Gene" in gene.category

        # Check phenotypic feature node
        phenotype = phenotypes[0]
        assert phenotype.id == "MP:0001262"
        assert "biolink:PhenotypicFeature" in phenotype.category

        # Check association
        assoc = assocs[0]
        assert assoc.subject == "MGI:98834"
        assert assoc.predicate == "biolink:has_phenotype"
        assert assoc.object == "MP:0001262"
        assert assoc.publications == ["PMID:12345678"]
        assert assoc.primary_knowledge_source == "infores:mgi"


def test_mgi_phenotype_multi_term(mgi_phenotype_multi_term_row):
    """Test mouse phenotype with multiple terms creates multiple associations and nodes"""
    # Mock the entity lookup to return values from our test dictionary
    with patch("translator_ingest.ingests.alliance.alliance.lookup_entity_category") as mock_lookup:
        mock_lookup.side_effect = lambda entity_id: TEST_ENTITY_LOOKUP.get(entity_id)

        result = transform_phenotype(None, mgi_phenotype_multi_term_row)

        # Should return: Gene node + 2 PhenotypicFeature nodes + 2 Associations
        assert len(result) == 5

        # Separate results by type (order-independent)
        genes = [r for r in result if isinstance(r, Gene)]
        phenotypes = [r for r in result if isinstance(r, PhenotypicFeature)]
        assocs = [r for r in result if isinstance(r, GeneToPhenotypicFeatureAssociation)]

        # Check counts
        assert len(genes) == 1
        assert len(phenotypes) == 2
        assert len(assocs) == 2

        # Check gene node
        assert genes[0].id == "MGI:98834"

        # Check phenotypic feature nodes
        phenotype_ids = {p.id for p in phenotypes}
        assert phenotype_ids == {"MP:0001262", "MP:0002169"}

        # Check associations
        assoc_objects = {a.object for a in assocs}
        assert assoc_objects == {"MP:0001262", "MP:0002169"}

        # Verify all associations point to the same gene
        for assoc in assocs:
            assert assoc.subject == "MGI:98834"


# ===== EXPRESSION TESTS =====


@pytest.fixture
def mgi_expression_anatomy_row():
    """Mouse expression association with anatomical structure"""
    return {
        "geneId": "MGI:98834",
        "assay": "MMO:0000655",
        "whereExpressed": {"anatomicalStructureTermId": "EMAPA:17524"},
        "whenExpressed": {"stageTermId": "MmusDv:0000003"},
        "evidence": {"publicationId": "PMID:12345678"},
        "crossReference": {"id": "MGI:5555555"},
    }


@pytest.fixture
def mgi_expression_cellular_component_row():
    """Mouse expression association with cellular component"""
    return {
        "geneId": "MGI:98834",
        "assay": "MMO:0000655",
        "whereExpressed": {"cellularComponentTermId": "GO:0005737"},
        "whenExpressed": {"stageTermId": "MmusDv:0000003"},
        "evidence": {"publicationId": "PMID:12345678"},
        "crossReference": {"id": "MGI:5555555"},
    }


def test_mgi_expression_anatomy(mgi_expression_anatomy_row):
    """Test mouse expression with anatomical structure creates nodes"""
    result = transform_expression(None, mgi_expression_anatomy_row)

    # Should return: Gene node + AnatomicalEntity node + Association
    assert len(result) == 3

    # Check gene node
    gene = result[0]
    assert isinstance(gene, Gene)
    assert gene.id == "MGI:98834"
    assert "biolink:Gene" in gene.category

    # Check anatomical entity node
    anatomy = result[1]
    assert isinstance(anatomy, AnatomicalEntity)
    assert anatomy.id == "EMAPA:17524"
    assert "biolink:AnatomicalEntity" in anatomy.category

    # Check association
    assoc = result[2]
    assert isinstance(assoc, GeneToExpressionSiteAssociation)
    assert assoc.subject == "MGI:98834"
    assert assoc.predicate == "biolink:expressed_in"
    assert assoc.object == "EMAPA:17524"
    assert assoc.stage_qualifier == "MmusDv:0000003"
    assert assoc.qualifiers == ["MMO:0000655"]
    assert "PMID:12345678" in assoc.publications
    assert "MGI:5555555" in assoc.publications
    assert assoc.primary_knowledge_source == "infores:mgi"


def test_mgi_expression_cellular_component(mgi_expression_cellular_component_row):
    """Test mouse expression with cellular component creates nodes"""
    result = transform_expression(None, mgi_expression_cellular_component_row)

    # Should return: Gene node + CellularComponent node + Association
    assert len(result) == 3

    # Check gene node
    gene = result[0]
    assert isinstance(gene, Gene)
    assert gene.id == "MGI:98834"
    assert "biolink:Gene" in gene.category

    # Check cellular component node
    cell_component = result[1]
    assert isinstance(cell_component, CellularComponent)
    assert cell_component.id == "GO:0005737"
    assert "biolink:CellularComponent" in cell_component.category

    # Check association
    assoc = result[2]
    assert isinstance(assoc, GeneToExpressionSiteAssociation)
    assert assoc.subject == "MGI:98834"
    assert assoc.predicate == "biolink:expressed_in"
    assert assoc.object == "GO:0005737"
    assert assoc.stage_qualifier == "MmusDv:0000003"
    assert assoc.qualifiers == ["MMO:0000655"]
    assert "PMID:12345678" in assoc.publications
    assert "MGI:5555555" in assoc.publications
    assert assoc.primary_knowledge_source == "infores:mgi"
