"""
Test suite for Alliance translator ingest.

Tests all transforms (gene, disease, phenotype, expression, genotype, allele)
with focus on mouse and rat data filtering.
"""

import pytest
from translator_ingest.ingests.alliance.alliance import (
    transform_gene,
    transform_disease,
    transform_phenotype,
    transform_expression,
)


# ===== GENE TESTS =====

@pytest.fixture
def mgi_gene_row():
    """Mouse gene from MGI"""
    return {
        "symbol": "Pax2",
        "name": "paired box 2",
        "soTermId": "SO:0001217",
        "basicGeneticEntity": {
            "primaryId": "MGI:97486",
            "taxonId": "NCBITaxon:10090",
            "synonyms": ["pax-2", "Pax-2"],
            "crossReferences": [
                {"id": "NCBI_Gene:18504"},
                {"id": "ENSEMBL:ENSMUSG00000004231"},
            ],
        },
    }


@pytest.fixture
def rgd_gene_row():
    """Rat gene from RGD"""
    return {
        "symbol": "Pax2",
        "name": "paired box 2",
        "soTermId": "SO:0001217",
        "basicGeneticEntity": {
            "primaryId": "RGD:3291",
            "taxonId": "NCBITaxon:10116",
            "synonyms": ["Pax-2"],
            "crossReferences": [
                {"id": "NCBI_Gene:25509"},
            ],
        },
    }


@pytest.fixture
def zfin_gene_row():
    """Zebrafish gene - should be filtered out"""
    return {
        "symbol": "pax2a",
        "name": "paired box 2a",
        "soTermId": "SO:0001217",
        "basicGeneticEntity": {
            "primaryId": "ZFIN:ZDB-GENE-990415-8",
            "taxonId": "NCBITaxon:7955",
        },
    }


def test_mgi_gene(mgi_gene_row):
    """Test mouse gene is processed"""
    genes = transform_gene(None, mgi_gene_row)
    assert len(genes) == 1
    gene = genes[0]
    assert gene.id == "MGI:97486"
    assert gene.symbol == "Pax2"
    assert "NCBITaxon:10090" in gene.in_taxon
    assert gene.in_taxon_label == "Mus musculus"
    assert gene.provided_by == ["infores:mgi"]


def test_rgd_gene(rgd_gene_row):
    """Test rat gene is processed"""
    genes = transform_gene(None, rgd_gene_row)
    assert len(genes) == 1
    gene = genes[0]
    assert gene.id == "RGD:3291"
    assert gene.symbol == "Pax2"
    assert "NCBITaxon:10116" in gene.in_taxon
    assert gene.in_taxon_label == "Rattus norvegicus"
    assert gene.provided_by == ["infores:rgd"]


def test_zfin_gene_filtered(zfin_gene_row):
    """Test non-mouse/rat gene is filtered out"""
    genes = transform_gene(None, zfin_gene_row)
    assert len(genes) == 0


# ===== DISEASE TESTS =====

@pytest.fixture
def mgi_disease_row():
    """Mouse disease association"""
    return {
        'Taxon': 'NCBITaxon:10090',
        'SpeciesName': 'Mus musculus',
        'DBobjectType': 'affected_genomic_model',
        'DBObjectID': 'MGI:3799157',
        'DBObjectSymbol': 'None [background:] C58/J',
        'AssociationType': 'is_model_of',
        'DOID': 'DOID:0060041',
        'DOtermName': 'autism spectrum disorder',
        'WithOrtholog': '',
        'InferredFromID': '',
        'InferredFromSymbol': '',
        'ExperimentalCondition': '',
        'Modifier': '',
        'EvidenceCode': 'ECO:0000033',
        'EvidenceCodeName': 'author statement supported by traceable reference',
        'Reference': 'PMID:29885454',
        'Date': '20181028',
        'Source': 'MGI'
    }


@pytest.fixture
def mgi_disease_with_condition_row():
    """Mouse disease with experimental condition - should NOT be filtered in translator ingest"""
    return {
        'Taxon': 'NCBITaxon:10090',
        'SpeciesName': 'Mus musculus',
        'DBobjectType': 'gene',
        'DBObjectID': 'MGI:98834',
        'DBObjectSymbol': 'Tg',
        'AssociationType': 'is_implicated_in',
        'DOID': 'DOID:12252',
        'DOtermName': 'goiter',
        'WithOrtholog': '',
        'InferredFromID': '',
        'InferredFromSymbol': '',
        'ExperimentalCondition': 'Has Condition: iodine-deficient diet',
        'Modifier': '',
        'EvidenceCode': 'ECO:0000305',
        'EvidenceCodeName': 'curator inference',
        'Reference': 'PMID:12345678',
        'Date': '20200101',
        'Source': 'MGI'
    }


@pytest.fixture
def zfin_disease_row():
    """Zebrafish disease - should be filtered out"""
    return {
        'Taxon': 'NCBITaxon:7955',
        'SpeciesName': 'Danio rerio',
        'DBobjectType': 'affected_genomic_model',
        'DBObjectID': 'ZFIN:ZDB-FISH-160908-10',
        'DBObjectSymbol': 'TU + MO2-mybpc2b',
        'AssociationType': 'is_model_of',
        'DOID': 'DOID:423',
        'DOtermName': 'myopathy',
        'WithOrtholog': '',
        'InferredFromID': '',
        'InferredFromSymbol': '',
        'ExperimentalCondition': '',
        'Modifier': '',
        'EvidenceCode': 'ECO:0000305',
        'EvidenceCodeName': 'curator inference',
        'Reference': 'PMID:27022191',
        'Date': '20240314',
        'Source': 'ZFIN'
    }


def test_mgi_disease(mgi_disease_row):
    """Test mouse disease association is processed"""
    associations = transform_disease(None, mgi_disease_row)
    assert len(associations) == 1
    assoc = associations[0]
    assert assoc.subject == 'MGI:3799157'
    assert assoc.predicate == 'biolink:model_of'
    assert assoc.object == 'DOID:0060041'
    assert assoc.has_evidence == ['ECO:0000033']
    assert assoc.publications == ['PMID:29885454']
    assert assoc.primary_knowledge_source == 'infores:mgi'


def test_mgi_disease_with_condition(mgi_disease_with_condition_row):
    """Test mouse disease with experimental condition is NOT filtered (unlike alliance-ingest)"""
    associations = transform_disease(None, mgi_disease_with_condition_row)
    # In translator-ingest, we keep all mouse/rat data, so this should be processed
    assert len(associations) == 1
    assoc = associations[0]
    assert assoc.subject == 'MGI:98834'
    assert assoc.object == 'DOID:12252'


def test_zfin_disease_filtered(zfin_disease_row):
    """Test non-mouse/rat disease is filtered out"""
    associations = transform_disease(None, zfin_disease_row)
    assert len(associations) == 0


# ===== PHENOTYPE TESTS =====

@pytest.fixture
def mgi_phenotype_row():
    """Mouse phenotype association"""
    return {
        "objectId": "MGI:98834",
        "phenotypeTermIdentifiers": [
            {"termId": "MP:0001262", "termOrder": 1}
        ],
        "evidence": {
            "publicationId": "PMID:12345678"
        }
    }


@pytest.fixture
def mgi_phenotype_multi_term_row():
    """Mouse phenotype with multiple terms - should NOT be filtered in translator ingest"""
    return {
        "objectId": "MGI:98834",
        "phenotypeTermIdentifiers": [
            {"termId": "MP:0001262", "termOrder": 1},
            {"termId": "MP:0002169", "termOrder": 2}
        ],
        "evidence": {
            "publicationId": "PMID:12345678"
        }
    }


def test_mgi_phenotype(mgi_phenotype_row):
    """Test mouse phenotype is processed"""
    associations = transform_phenotype(None, mgi_phenotype_row)
    assert len(associations) == 1
    assoc = associations[0]
    assert assoc.subject == "MGI:98834"
    assert assoc.predicate == "biolink:has_phenotype"
    assert assoc.object == "MP:0001262"
    assert assoc.publications == ["PMID:12345678"]
    assert assoc.primary_knowledge_source == "infores:mgi"


def test_mgi_phenotype_multi_term(mgi_phenotype_multi_term_row):
    """Test mouse phenotype with multiple terms is NOT filtered (unlike alliance-ingest)"""
    associations = transform_phenotype(None, mgi_phenotype_multi_term_row)
    # In translator-ingest, we keep all mouse/rat data including multi-phenotype records
    assert len(associations) == 2
    assert associations[0].object == "MP:0001262"
    assert associations[1].object == "MP:0002169"


# ===== EXPRESSION TESTS =====

@pytest.fixture
def mgi_expression_row():
    """Mouse expression association"""
    return {
        "geneId": "MGI:98834",
        "assay": "MMO:0000655",
        "whereExpressed": {
            "anatomicalStructureTermId": "EMAPA:17524"
        },
        "whenExpressed": {
            "stageTermId": "MmusDv:0000003"
        },
        "evidence": {
            "publicationId": "PMID:12345678"
        },
        "crossReference": {
            "id": "MGI:5555555"
        }
    }


def test_mgi_expression(mgi_expression_row):
    """Test mouse expression is processed"""
    associations = transform_expression(None, mgi_expression_row)
    assert len(associations) == 1
    assoc = associations[0]
    assert assoc.subject == "MGI:98834"
    assert assoc.predicate == "biolink:expressed_in"
    assert assoc.object == "EMAPA:17524"
    assert assoc.stage_qualifier == "MmusDv:0000003"
    assert assoc.qualifiers == ["MMO:0000655"]
    assert "PMID:12345678" in assoc.publications
    assert "MGI:5555555" in assoc.publications
    assert assoc.primary_knowledge_source == "infores:mgi"
