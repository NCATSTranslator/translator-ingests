"""Test file for the Alliance allele transform script."""
import pytest
from biolink_model.datamodel.pydanticmodel_v2 import SequenceVariant, VariantToGeneAssociation
from koza.utils.testing_utils import mock_koza  # noqa: F401

# Define the ingest name and transform script path
INGEST_NAME = "alliance_allele"
TRANSFORM_SCRIPT = "./src/alliance_genotype/allele.py"


@pytest.fixture
def zfin_allele(mock_koza):
    row = {
        "AlleleId": "ZFIN:ZDB-ALT-123456-7",
        "AlleleSymbol": "tyr<b1>",
        "AlleleSynonyms": "tyrosinase b1,tyr-b1",
        "Taxon": "NCBITaxon:7955",
        "SpeciesName": "Danio rerio",
        "AlleleAssociatedGeneId": "ZFIN:ZDB-GENE-000508-1",
        "AlleleAssociatedGeneSymbol": "tyr",
        "VariantsTypeId": "SO:0001059",  # sequence_alteration
    }

    return mock_koza(INGEST_NAME, row, TRANSFORM_SCRIPT)


@pytest.fixture
def mgi_allele(mock_koza):
    row = {
        "AlleleId": "MGI:5569116",
        "AlleleSymbol": "Tyr<c-2J>",
        "AlleleSynonyms": "tyrosinase<cocoa-2J>,c-2J",
        "Taxon": "NCBITaxon:10090",
        "SpeciesName": "Mus musculus",
        "AlleleAssociatedGeneId": "MGI:98880",
        "AlleleAssociatedGeneSymbol": "Tyr",
        "VariantsTypeId": "SO:1000008",  # point_mutation
    }

    return mock_koza(INGEST_NAME, row, TRANSFORM_SCRIPT)


@pytest.fixture
def allele_without_gene(mock_koza):
    row = {
        "AlleleId": "MGI:6543210",
        "AlleleSymbol": "Unknown<tm1>",
        "AlleleSynonyms": "unknown allele",
        "Taxon": "NCBITaxon:10090",
        "SpeciesName": "Mus musculus",
        "AlleleAssociatedGeneId": "-",
        "VariantsTypeId": "SO:0000667",  # insertion
    }

    return mock_koza(INGEST_NAME, row, TRANSFORM_SCRIPT)


def test_zfin_allele_transform(zfin_allele):
    entities = zfin_allele
    variants = [entity for entity in entities if isinstance(entity, SequenceVariant)]
    assert len(variants) == 1

    variant = variants[0]
    assert variant.id == "ZFIN:ZDB-ALT-123456-7"
    assert variant.name == "tyr<b1>"
    assert variant.in_taxon == ["NCBITaxon:7955"]
    assert variant.in_taxon_label == "Danio rerio"
    assert variant.synonym == ["tyrosinase b1", "tyr-b1"]


def test_mgi_allele_transform(mgi_allele):
    entities = mgi_allele
    variants = [entity for entity in entities if isinstance(entity, SequenceVariant)]
    assert len(variants) == 1

    variant = variants[0]
    assert variant.id == "MGI:5569116"
    assert variant.name == "Tyr<c-2J>"
    assert variant.in_taxon == ["NCBITaxon:10090"]
    assert variant.in_taxon_label == "Mus musculus"
    assert variant.synonym == ["tyrosinase<cocoa-2J>", "c-2J"]


def test_zfin_gene_association(zfin_allele):
    entities = zfin_allele
    associations = [entity for entity in entities if isinstance(entity, VariantToGeneAssociation)]
    assert len(associations) == 1

    association = associations[0]
    assert association.subject == "ZFIN:ZDB-ALT-123456-7"
    assert association.predicate == "biolink:is_sequence_variant_of"
    assert association.original_predicate == "SO:0001059"  # Verify original predicate is preserved
    assert association.object == "ZFIN:ZDB-GENE-000508-1"
    assert association.primary_knowledge_source == "infores:zfin"
    assert set(association.aggregator_knowledge_source) == {"infores:monarchinitiative", "infores:agrkb"}
    assert association.knowledge_level == "knowledge_assertion"
    assert association.agent_type == "manual_agent"


def test_mgi_gene_association(mgi_allele):
    entities = mgi_allele
    associations = [entity for entity in entities if isinstance(entity, VariantToGeneAssociation)]
    assert len(associations) == 1

    association = associations[0]
    assert association.subject == "MGI:5569116"
    assert association.predicate == "biolink:is_sequence_variant_of"
    assert association.original_predicate == "SO:1000008"  # Verify original predicate is preserved
    assert association.object == "MGI:98880"
    assert association.primary_knowledge_source == "infores:mgi"
    assert set(association.aggregator_knowledge_source) == {"infores:monarchinitiative", "infores:agrkb"}


def test_allele_without_gene(allele_without_gene):
    entities = allele_without_gene
    variants = [entity for entity in entities if isinstance(entity, SequenceVariant)]
    associations = [entity for entity in entities if isinstance(entity, VariantToGeneAssociation)]

    # Verify variant is created
    assert len(variants) == 1
    assert variants[0].id == "MGI:6543210"

    # Verify no association is created when gene ID is missing
    assert len(associations) == 0
